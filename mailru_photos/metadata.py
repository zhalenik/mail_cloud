"""Small, network-free encoders for the APK's metadata protocol.

Grounding: Cloud Mail 5.117.0.10079902, AddBlobRequest (operation 103),
CreateFolderRequest (operation 106), QuotaRequest (operation 23), DataEncoder,
C3250v0 and MPR_NONE. Strings use
NFC-normalized UTF-8 *byte* lengths including a terminating NUL. Integer
encoding is unsigned base-128 varint, least-significant group first.
"""

from __future__ import annotations

import re
import unicodedata

MAX_U64 = (1 << 64) - 1
MAX_U32 = (1 << 32) - 1


class MetadataProtocolError(ValueError):
    """Malformed response or a failed metadata operation (without secrets)."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


def encode_varint(value: int) -> bytes:
    """Encode an unsigned 64-bit integer using the APK's PU encoding."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("varint value must be an integer")
    if not 0 <= value <= MAX_U64:
        raise ValueError("varint value must be in the uint64 range")
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def read_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Return ``(value, next_offset)``; reject truncation and uint64 overflow.

    At most ten bytes are inspected. Remaining bytes belong to the next field
    and are intentionally left unread. Overlong but in-range encodings are
    accepted, matching the app's decoder.
    """
    if isinstance(offset, bool) or not isinstance(offset, int):
        raise TypeError("offset must be an integer")
    if not 0 <= offset <= len(data):
        raise ValueError("offset is outside the buffer")
    value = 0
    for group in range(10):
        position = offset + group
        if position >= len(data):
            raise MetadataProtocolError("Truncated metadata varint")
        byte = data[position]
        if group == 9 and byte > 1:
            raise MetadataProtocolError("Metadata varint exceeds uint64")
        value |= (byte & 0x7F) << (group * 7)
        if not byte & 0x80:
            return value, position + 1
    raise MetadataProtocolError("Metadata varint exceeds uint64")


def encode_mkdir(path: str) -> bytes:
    """Encode operation 106 for an absolute remote directory path.

    Encoding: opcode, MPR_NONE revision, length-prefixed NUL-terminated path,
    then zero flags. This function does not create a directory by itself.
    """
    if not isinstance(path, str):
        raise TypeError("remote directory path must be text")
    if not path.startswith("/") or "\x00" in path:
        raise ValueError("remote directory path must be absolute and contain no NUL")
    encoded = unicodedata.normalize("NFC", path).encode("utf-8")
    if len(encoded) + 1 > MAX_U32:
        raise ValueError("remote directory path exceeds the protocol string limit")
    return (
        encode_varint(106)
        + b"\x00"  # MPR_NONE writes one zero byte.
        + encode_varint(len(encoded) + 1)
        + encoded
        + b"\x00"  # String terminator.
        + b"\x00"  # Zero flags.
    )


def parse_mkdir_response(raw: bytes) -> int:
    """Return the unsigned status byte; subsequent revision bytes are opaque.

    The caller must interpret the status: 0 is success; 4 is already exists
    and needs a directory-type check; 9 is a case conflict. Other values are
    errors, including 1 (missing parent) and 254 (revision conflict).
    """
    if not raw:
        raise MetadataProtocolError("Empty mkdir response")
    return raw[0]


def encode_add_file(path: str, size: int, mtime_seconds: int, digest: str) -> bytes:
    """Encode an ordinary, create-only file commit (operation 103).

    AddBlobRequest's default flags are 3. Bit 2 prevents replacement; the
    separate autorename/autoupload flags (55) are deliberately not enabled.
    The digest is the raw 20-byte Mail Cloud hash, not its ASCII spelling.
    This operation consumes the account's ordinary storage quota.
    """
    if not isinstance(path, str):
        raise TypeError("remote file path must be text")
    if (not path.startswith("/") or "\x00" in path
            or any(part in ("", ".", "..") for part in path.split("/")[1:])):
        raise ValueError("remote file path must be canonical, absolute and contain no NUL")
    encoded = unicodedata.normalize("NFC", path).encode("utf-8")
    if len(encoded) + 1 > MAX_U32:
        raise ValueError("remote file path exceeds the protocol string limit")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", digest):
        raise ValueError("file hash must contain exactly 40 hexadecimal characters")
    return (
        encode_varint(103)
        + b"\x00"  # MPR_NONE: no revision precondition.
        + encode_varint(len(encoded) + 1) + encoded + b"\x00"
        + encode_varint(size)
        + encode_varint(mtime_seconds)
        + b"\x00"
        + bytes.fromhex(digest)
        + b"\x03"  # Create only; no overwrite and no automatic renaming.
    )


def parse_add_file_response(raw: bytes) -> int:
    """Read a create-only commit status and validate a positive revision.

    Both the status and revision type are single bytes, including statuses
    253/254. A success without the complete BaseRevision is not confirmation.
    Flags 3 request neither an auto-renamed name nor a node-id section, so no
    other trailing fields are expected. Nonzero statuses are left to the
    caller; even NOT_MODIFIED (12) is not positive commit evidence here.
    """
    if not raw:
        raise MetadataProtocolError("Empty add-file response")
    status = raw[0]
    if status != 0:
        return status
    if len(raw) < 2:
        raise MetadataProtocolError("Missing add-file revision")
    revision_type, offset = raw[1], 2
    if revision_type not in range(6):
        raise MetadataProtocolError("Unknown add-file revision type")
    revision_count = 2 if revision_type in (3, 4) else 1 if revision_type in (1, 2, 5) else 0
    for _ in range(revision_count):
        # TreeID is always 12 bytes, followed by one unsigned revision value.
        if len(raw) < offset + 12:
            raise MetadataProtocolError("Truncated add-file tree id")
        _, offset = read_varint(raw, offset + 12)
    if revision_type == 5:
        if len(raw) < offset + 12:
            raise MetadataProtocolError("Truncated add-file redirect tree id")
        offset += 12
    if offset != len(raw):
        raise MetadataProtocolError("Unexpected trailing add-file fields")
    return status


def encode_quota() -> bytes:
    """Encode operation 23 (read used and total space)."""
    return b"\x17"


def parse_quota_response(raw: bytes) -> tuple[int, int]:
    """Return ``(used_bytes, total_bytes)`` after a successful status byte.

    A used value larger than total is valid for an over-quota account.
    Trailing extension bytes are ignored, as in the APK's quota parser.
    """
    if not raw:
        raise MetadataProtocolError("Empty quota response")
    status = raw[0]
    if status != 0:
        raise MetadataProtocolError(
            f"Metadata quota request failed with status {status}", status=status
        )
    used, offset = read_varint(raw, 1)
    total, _ = read_varint(raw, offset)
    return used, total

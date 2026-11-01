"""Small, network-free encoders for the APK's metadata protocol.

Grounding: Cloud Mail 5.117.0.10079902, CreateFolderRequest (operation 106),
QuotaRequest (operation 23), DataEncoder, C3250v0 and MPR_NONE. Strings use
NFC-normalized UTF-8 *byte* lengths including a terminating NUL. Integer
encoding is unsigned base-128 varint, least-significant group first.
"""

from __future__ import annotations

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

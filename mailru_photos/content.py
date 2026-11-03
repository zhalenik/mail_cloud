"""Authenticated task content: pinned descriptors, bounded reads, no path input.

The controller supplies the source root and the identity stored in its queue.
Only the HTTP layer decides who may invoke this module. Filesystem traversal
never follows symlinks, including intermediate directories and the root path.
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
import re
import stat
from urllib.parse import quote

from .api import FileIdentity


CHUNK_SIZE = 64 * 1024
_OPENAT_SUPPORTED = (os.name == "posix" and os.open in os.supports_dir_fd
                     and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY"))
CONTENT_SECURITY = (
    ("Cache-Control", "no-store"),
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "no-referrer"),
    ("X-Frame-Options", "DENY"),
    ("Cross-Origin-Resource-Policy", "same-origin"),
    ("Content-Security-Policy", "default-src 'none'; sandbox; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"),
)


class ContentError(Exception):
    def __init__(self, status: int, message: str, code: str, *, headers=()):
        super().__init__(message)
        self.status, self.message, self.code = status, message, code
        self.headers = tuple(headers)


def _filesystem_error(error: OSError) -> ContentError:
    if error.errno == errno.ENOENT:
        return ContentError(404, "Исходный файл больше не найден на сервере.", "source_missing")
    if error.errno in {errno.EACCES, errno.EPERM, errno.ELOOP, errno.ENOTDIR, errno.ENXIO, errno.ENODEV}:
        return ContentError(403, "Нет безопасного доступа к исходному файлу.", "source_forbidden")
    return ContentError(503, "Не удалось открыть исходный файл на сервере.", "source_unavailable")


def _open_source(root: Path, path: Path, expected: FileIdentity) -> int:
    if not _OPENAT_SUPPORTED:
        raise ContentError(501, "Этот сервер не поддерживает безопасное открытие исходных файлов.", "source_not_supported")
    root, path = Path(root), Path(path)
    if not root.is_absolute() or not path.is_absolute() or ".." in root.parts or ".." in path.parts:
        raise ContentError(403, "Недопустимый путь к исходному файлу.", "source_forbidden")
    try:
        relative = path.relative_to(root)
    except ValueError:
        raise ContentError(403, "Файл находится вне исходной папки.", "source_forbidden") from None
    if not relative.parts:
        raise ContentError(403, "Открыть можно только отдельный обычный файл.", "source_forbidden")
    if not isinstance(expected, FileIdentity):
        raise ContentError(409, "Не удалось проверить состояние исходного файла.", "source_changed")
    directory = file_fd = None
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        directory = os.open(root.anchor, flags)
        # Open every component relative to an already pinned parent. A rename
        # or symlink replacement cannot redirect later opens outside that tree.
        for part in root.parts[1:] + relative.parts[:-1]:
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        file_fd = os.open(relative.parts[-1], os.O_RDONLY | os.O_NOFOLLOW
                          | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0), dir_fd=directory)
        info = os.fstat(file_fd)
        if not stat.S_ISREG(info.st_mode):
            raise ContentError(403, "Открыть можно только отдельный обычный файл.", "source_forbidden")
        if FileIdentity.from_stat(info) != expected:
            raise ContentError(409, "Исходный файл изменился после сканирования. Пересканируйте папку.", "source_changed")
        result, file_fd = file_fd, None
        return result
    except OSError as error:
        raise _filesystem_error(error) from None
    finally:
        if directory is not None:
            os.close(directory)
        if file_fd is not None:
            os.close(file_fd)


def _range(value: str | None, size: int) -> tuple[int, int, int]:
    if value is None:
        return 200, 0, size
    failure = ContentError(416, "Запрошенный диапазон файла недоступен.", "invalid_range",
                           headers=(("Content-Range", f"bytes */{size}"), ("Accept-Ranges", "bytes")))
    if not isinstance(value, str) or len(value) > 256:
        raise failure
    unit, separator, span = value.strip().partition("=")
    if unit.casefold() != "bytes":
        # RFC 9110 §14.2 requires ignoring unrecognized range units.
        return 200, 0, size
    match = re.fullmatch(r"([0-9]*)-([0-9]*)", span) if separator else None
    if match is None or not any(match.groups()) or size == 0:
        raise failure
    first, last = match.groups()
    if not first:
        suffix = int(last)
        if suffix == 0:
            raise failure
        start, end = max(0, size - suffix), size - 1
    else:
        start = int(first)
        end = min(int(last), size - 1) if last else size - 1
        if start >= size or end < start:
            raise failure
    return 206, start, end - start + 1


def _inline_type(fd: int, filename: str) -> str | None:
    signature = os.pread(fd, 16, 0)
    extension = Path(filename).suffix.casefold()
    if extension in {".jpg", ".jpeg"} and signature.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if extension == ".png" and signature.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if extension == ".gif" and signature.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if extension == ".webp" and signature.startswith(b"RIFF") and signature[8:12] == b"WEBP":
        return "image/webp"
    return None


def _disposition(filename: str, inline: bool) -> str:
    fallback = re.sub(r"[^A-Za-z0-9._ -]", "_", filename)[:150].strip(" .") or "download"
    encoded = quote(filename, safe="", encoding="utf-8", errors="replace")
    return f"{'inline' if inline else 'attachment'}; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


class FileResponse:
    """A WSGI iterable that owns one descriptor until completion or close()."""
    def __init__(self, fd: int, expected: FileIdentity, *, status: int, start: int,
                 length: int, content_type: str, disposition: str, head: bool):
        self._fd: int | None = fd
        self._expected = expected
        self._remaining = 0 if head else length
        self.status = status
        self.headers = [("Content-Type", content_type), ("Content-Length", str(length)),
                        ("Content-Disposition", disposition), ("Accept-Ranges", "bytes"), *CONTENT_SECURITY]
        if status == 206:
            self.headers.append(("Content-Range", f"bytes {start}-{start + length - 1}/{expected.size}"))
        try:
            os.lseek(fd, start, os.SEEK_SET)
            if head or length == 0:
                self.close()
        except BaseException:
            self.close()
            raise

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self._fd is None or self._remaining == 0:
            self.close()
            raise StopIteration
        try:
            if FileIdentity.from_stat(os.fstat(self._fd)) != self._expected:
                raise OSError("Исходный файл изменился во время чтения.")
            value = os.read(self._fd, min(CHUNK_SIZE, self._remaining))
            if not value or FileIdentity.from_stat(os.fstat(self._fd)) != self._expected:
                raise OSError("Исходный файл изменился во время чтения.")
            self._remaining -= len(value)
            if self._remaining == 0:
                self.close()
            return value
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)

    def __del__(self):
        # WSGI servers call close() on completion/disconnect. This is only a
        # fallback for callers that abandon the iterable without following it.
        try:
            self.close()
        except OSError:
            pass


def open_task_content(root: Path, path: Path, expected: FileIdentity, *, download: bool = False,
                      range_header: str | None = None, head: bool = False) -> FileResponse:
    """Open a queue-selected file, preserving its original bytes and name."""
    if type(download) is not bool or type(head) is not bool:
        raise ValueError("download and head must be booleans")
    fd = _open_source(root, path, expected)
    try:
        # Range only has semantics for GET (RFC 9110 §14.2).
        status, start, length = _range(None if head else range_header, expected.size)
        media_type = _inline_type(fd, Path(path).name)
        disposition = _disposition(Path(path).name, bool(media_type) and not download)
        response_fd, fd = fd, None
        return FileResponse(response_fd, expected, status=status, start=start, length=length,
                            content_type=media_type or "application/octet-stream",
                            disposition=disposition, head=head)
    except BaseException:
        # FileResponse owns the descriptor once constructed, including HEAD.
        if fd is not None:
            os.close(fd)
        raise

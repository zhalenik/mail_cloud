"""Conservative upload planning; only the server can confirm actual billing.

The published auto-upload formats are listed at
https://help.mail.ru/cloud/files/autosave/. They describe supported mobile
formats, not a guarantee that every file with an extension is unlimited.
MOV additionally has a confirmed successful account test by this client user.
Unlisted formats stay discoverable, but use the explicitly permitted regular
route instead of silently gambling with storage quota.
"""

from __future__ import annotations

from pathlib import Path

# Cloud Mail Android RemoteParams: unlimited_upload_max_file_size default.
# Keep the exact binary limit so files that still qualify are never billed just
# because a rounded decimal interpretation of "2 GB" was used by the client.
UNLIMITED_MAX_BYTES = 2 ** 31

MEDIA_POLICY_VERSION = 1

LEGACY_PHOTO_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".gif"})
PHOTO_EXTENSIONS = LEGACY_PHOTO_EXTENSIONS | {".raw", ".dng", ".tif", ".tiff"}
VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".3gp", ".3g2", ".3gpp", ".3gpp2", ".avi", ".vob", ".mov",
    ".mkv", ".m2t", ".m2ts", ".ts", ".mts", ".f4v", ".flv", ".wmv", ".m4v",
    ".mpg", ".ogv", ".webm", ".hevc",
})
LEGACY_MEDIA_EXTENSIONS = LEGACY_PHOTO_EXTENSIONS | VIDEO_EXTENSIONS
MEDIA_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS

DOCUMENTED_PHOTO_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif", ".raw", ".dng", ".tif", ".tiff",
})
DOCUMENTED_VIDEO_EXTENSIONS = frozenset({".mp4", ".3gp", ".mkv", ".webm", ".avi"})
ACCOUNT_CONFIRMED_VIDEO_EXTENSIONS = frozenset({".mov"})


def classify_media(path: str | Path, size: int) -> dict[str, str]:
    """Return a routing plan, never an assertion about a remote object.

    Files outside the documented/observed set are sent through the ordinary
    route only if the user explicitly enables it. Absence from that set is
    not presented as an official prohibition on the file's extension.
    """
    if type(size) is not int or size < 0:
        raise ValueError("Media size must be a nonnegative integer")
    extension = Path(path).suffix.casefold()
    if size > UNLIMITED_MAX_BYTES:
        return {"mode": "regular", "reason": "size_limit",
                "description": "Размер больше 2 ГБ: запланирована обычная загрузка в квоту."}
    if extension in DOCUMENTED_PHOTO_EXTENSIONS | DOCUMENTED_VIDEO_EXTENSIONS:
        return {"mode": "unlimited", "reason": "documented_format",
                "description": "Формат указан в справке автозагрузки Mail; режим подтвердит сервер."}
    if extension in ACCOUNT_CONFIRMED_VIDEO_EXTENSIONS:
        return {"mode": "unlimited", "reason": "account_confirmed_mov",
                "description": "MOV проверен в аккаунте пользователя; режим каждого файла подтвердит сервер."}
    return {"mode": "regular", "reason": "outside_documented_formats",
            "description": "Формат вне подтверждённого списка: по политике клиента запланирована загрузка в квоту."}

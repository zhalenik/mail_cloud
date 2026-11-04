"""Original mobile uploads with explicit storage modes and per-file evidence."""

from __future__ import annotations

import hashlib
import contextlib
import json
import math
import os
import posixpath
import re
import threading
import time
import uuid
import unicodedata
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlencode, urlsplit

from .auth import AuthError, TokenManager
from .media import UNLIMITED_MAX_BYTES
from .metadata import (MetadataProtocolError, encode_add_file, encode_mkdir, encode_quota,
                       parse_add_file_response, parse_mkdir_response, parse_quota_response)
from .transport import (Cancelled, HTTP, Response, SourceReadError, TransportError,
                        check_cancelled, validate_url)


API_BASE = "https://android-api.cloud.mail.ru"
QUOTA_BASE = "https://cloud.mail.ru"
UPLOAD_BASE = "https://uploader.cloud.mail.ru"
DISPATCHER = "https://dispatcher.cloud.mail.ru/m"
# The Android client uses the same 2 GiB limit for photo and video autoupload.
MAX_MEDIA_BYTES = UNLIMITED_MAX_BYTES
MAX_PHOTO_BYTES = MAX_MEDIA_BYTES  # Compatibility with previous Python callers.
PROTOCOL_VERSION = "cloud-mail-5.117.0.10079902-unlimited-v2-confirmed"


class ProtocolError(RuntimeError):
    """A failure to interpret the protocol; do not keep uploading blindly."""


class RemoteError(ProtocolError):
    def __init__(self, status: int, code: str = "REMOTE_ERROR", *, retry_after: float | None = None,
                 authenticated: bool = True):
        self.status, self.code, self.retry_after = status, code, retry_after
        self.auth_required = authenticated and status in (401, 403)
        self.retryable = status == 429 or status >= 500 or code == "CONDITION/BADBLOB"
        self.unsupported = code in {"NOT/SUPPORTED", "CONDITION/BODYTOOBIG"}
        self.fatal = status in {401, 403} or code in {
            "CONDITION/OVERQUOTA", "CONDITION/NOT_ACCEPTABLE", "CONDITION/NOT_ACCEPTABLE/UNLIM_UPL_TOTAL"
        }
        super().__init__(f"Mail.ru: HTTP {status}, {code}")


def is_auth_failure(error: BaseException) -> bool:
    """Recognize rejected Cloud credentials without parsing messages or URLs.

    Authenticated Cloud responses have already had their token-refresh
    attempt. A public discovery response, an unrelated URL or an exception
    carrying a ``status`` attribute is not evidence of a rejected account.
    """
    return isinstance(error, AuthError) or (
        isinstance(error, RemoteError) and error.auth_required
    )


class RemoteConflict(ProtocolError):
    pass


class UnlimitedNotConfirmed(ProtocolError):
    """The server has not supplied a durable, positive per-file billing proof."""

    def __init__(self, message: str, *, observation: dict[str, Any] | None = None):
        # The caller chooses what to do with a known quota-consuming commit.
        # Unknown billing is retained separately, never silently treated as false.
        self.observation = dict(observation) if observation is not None else None
        self.result = self.observation
        super().__init__(message)


class FileVerificationError(ProtocolError):
    """One remote object failed verification, without changing protocol rules."""


class SourceChanged(ProtocolError):
    pass


class UnsupportedPhoto(ProtocolError):
    pass


@dataclass(frozen=True)
class AccountInfo:
    email: str
    unlimited_enabled: bool
    upload_limit: int | None


@dataclass(frozen=True)
class FileIdentity:
    size: int
    mtime_ns: int
    device: int
    inode: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> "FileIdentity":
        return cls(value.st_size, value.st_mtime_ns, value.st_dev, value.st_ino)


def cloud_hash(path: str | Path, *, expected: FileIdentity | None = None,
               stop: threading.Event | None = None) -> tuple[str, FileIdentity]:
    """Streaming Mail Cloud content hash; names and EXIF are left untouched."""
    check_cancelled(stop)
    try:
        return _cloud_hash(path, expected=expected, stop=stop)
    except OSError:
        raise SourceReadError("Не удалось прочитать исходный файл для вычисления хеша.") from None


def _cloud_hash(path: str | Path, *, expected: FileIdentity | None = None,
                stop: threading.Event | None = None) -> tuple[str, FileIdentity]:
    with open(path, "rb") as stream:
        identity = FileIdentity.from_stat(os.fstat(stream.fileno()))
        if expected is not None and identity != expected:
            raise SourceChanged("Файл изменился после сканирования. Выполните scan ещё раз.")
        if identity.size <= 20:
            digest = stream.read().ljust(20, b"\0").hex().upper()
        else:
            calculator = hashlib.sha1(b"mrCloud")
            count = 0
            while block := stream.read(1024 * 1024):
                check_cancelled(stop)
                calculator.update(block)
                count += len(block)
            if count != identity.size:
                raise SourceChanged("Размер файла изменился при вычислении хеша.")
            calculator.update(str(count).encode("ascii"))
            digest = calculator.hexdigest().upper()
        after = FileIdentity.from_stat(os.fstat(stream.fileno()))
    if identity != after or identity != FileIdentity.from_stat(os.stat(path, follow_symlinks=False)):
        raise SourceChanged("Файл изменился при вычислении хеша.")
    return digest, identity


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ProtocolError(f"Некорректное числовое поле: {label}.")
    if isinstance(value, int) and 0 <= value <= (1 << 63) - 1:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        # The APK parses these headers as signed Java Longs. Reject absurd
        # integers without entering Python's potentially huge int conversion.
        if len(value) <= 19:
            converted = int(value)
            if converted <= (1 << 63) - 1:
                return converted
    raise ProtocolError(f"Неожиданный формат числового поля: {label}.")


def _positive_integer(value: Any, label: str) -> int:
    converted = _integer(value, label)
    if converted == 0:
        raise ProtocolError(f"Ожидалось положительное числовое поле: {label}.")
    return converted


def _remote_path(path: str) -> str:
    """Use the same normalized path for JSON, URI and binary metadata calls."""
    if (not isinstance(path, str) or not path.startswith("/") or "\x00" in path
            or any(part in ("", ".", "..") for part in path.split("/")[1:])):
        raise ValueError("Remote file path must be canonical and absolute")
    return unicodedata.normalize("NFC", path)


def _node_id(value: Any) -> str:
    if isinstance(value, str) and len(value) <= 256 and not any(ord(c) < 32 for c in value):
        return value
    if type(value) is int and value >= 0:
        return str(value)
    return ""


def _commit_observation(digest: str, size: int, path: str, mode: str, *,
                        nodeid: Any = None, flag: Any = None, verified: bool = False,
                        committed: bool = True) -> dict[str, Any]:
    result: dict[str, Any] = {
        "mode": mode, "storage_mode": mode, "hash": digest, "size": size,
        "path": path, "nodeid": _node_id(nodeid), "verified": verified,
        "stat_matched": verified, "commit_accepted": committed,
    }
    if type(flag) is bool:
        result["is_unlim_upload"] = flag
    return result


def _json(response: Response) -> dict[str, Any]:
    try:
        result = json.loads(response.body)
    except (ValueError, UnicodeError):
        raise ProtocolError("Сервер вернул ответ, который не является JSON.") from None
    if not isinstance(result, dict):
        raise ProtocolError("Неожиданная структура ответа сервера.")
    return result


def _error_code(response: Response) -> str:
    try:
        value = json.loads(response.body)
        code = value.get("error", "REMOTE_ERROR") if isinstance(value, dict) else "REMOTE_ERROR"
        if isinstance(code, dict):
            code = code.get("code", "REMOTE_ERROR")
        if isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_/]{0,100}", code):
            return code
    except (ValueError, UnicodeError):
        pass
    return "REMOTE_ERROR"


def _retry_after(response: Response) -> float | None:
    value = response.headers.get("retry-after")
    if not value:
        return None
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = parsedate_to_datetime(value).timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            return None
    if not math.isfinite(delay) or delay > threading.TIMEOUT_MAX:
        return None
    return max(0, delay)


class CloudAPI:
    _task_local_lock = threading.Lock()

    def __init__(self, tokens: TokenManager, *, stop: threading.Event | None = None,
                 transport_factory: Callable[[], HTTP] | None = None):
        self.tokens = tokens
        self.stop = stop or threading.Event()
        self._factory = transport_factory or (lambda: HTTP(stop=self.stop))
        self._local = threading.local()
        self._metadata_url: str | None = None
        self._metadata_until = 0.0
        self._directory_lock = threading.RLock()
        self._directories: set[str] = {"/"}
        self._rate_lock = threading.Lock()
        self._not_before = 0.0
        self._session_id = str(uuid.uuid4())

    @property
    def transport(self) -> HTTP:
        if not hasattr(self._local, "http"):
            self._local.http = self._factory()
        # Persistent per-thread transports must never retain a previous
        # task's cancellation signal when the same worker takes another task.
        self._local.http.stop = self.request_stop
        return self._local.http

    @property
    def request_stop(self):
        return getattr(getattr(self, "_local", None), "request_stop", self.stop)

    @contextlib.contextmanager
    def task_scope(self, stop):
        # Some offline adapters intentionally omit CloudAPI.__init__. Keep
        # that supported without sharing task signals between their threads.
        if not hasattr(self, "_local"):
            with self._task_local_lock:
                if not hasattr(self, "_local"):
                    self._local = threading.local()
        previous = getattr(self._local, "request_stop", None)
        self._local.request_stop = stop
        if hasattr(self._local, "http"):
            self._local.http.stop = stop
        try:
            yield
        finally:
            if previous is None:
                del self._local.request_stop
            else:
                self._local.request_stop = previous
            if hasattr(self._local, "http"):
                self._local.http.stop = self.request_stop

    def close_thread(self) -> None:
        if hasattr(self._local, "http"):
            self._local.http.close()
            del self._local.http

    def _wait(self, seconds: float) -> None:
        stop = self.request_stop
        if stop.wait(max(0, seconds)):
            check_cancelled(stop)
            raise Cancelled("Загрузка остановлена.")

    def _request(self, method: str, url: str, *, headers: dict[str, str] | None = None,
                 body: bytes = b"", stream=None, length=None, authenticated: bool = True,
                 token_query: bool = False, token_parameter: str = "token", progress=None,
                 folder_read: bool = False) -> Response:
        check_cancelled(self.request_stop)
        if token_parameter not in {"token", "access_token"}:
            raise ValueError("Unsupported OAuth query parameter")
        # Retry only reads. M1's folder read happens to use POST, with no cloud
        # mutation. No other POST may opt into retries through this flag.
        if folder_read and (method != "POST" or urlsplit(url)._replace(query="", fragment="").geturl()
                            != QUOTA_BASE + "/api/m1/folder" or stream is not None):
            raise ValueError("Only the private folder listing can be retried as a POST read")
        # A failed PUT/POST mutation may already have been accepted;
        # retrying those belongs to the durable upload/reconciliation workflow.
        read_only = (method == "GET" and not body and stream is None) or folder_read
        network_attempts = 3 if read_only else 1
        for attempt in range(2):
            actual_headers = dict(headers or {})
            token = self.tokens.access_token() if authenticated else ""
            actual_url = url
            if authenticated and token_query:
                auth_query = {token_parameter: token}
                if token_parameter == "token":
                    auth_query["client_id"] = self.tokens.tokens.client_id
                actual_url += ("&" if "?" in actual_url else "?") + urlencode(auth_query)
            elif authenticated:
                actual_headers["Authorization"] = "Bearer " + token
            start = stream.tell() if stream is not None else None
            for network_attempt in range(network_attempts):
                with self._rate_lock:
                    delay = self._not_before - time.monotonic()
                if delay > 0:
                    self._wait(delay)
                try:
                    response = self.transport.request(method, actual_url, headers=actual_headers, body=body,
                                                      stream=stream, length=length, progress=progress)
                except TransportError as error:
                    if (network_attempt + 1 >= network_attempts or error.category not in
                            {None, "timeout", "connection_reset", "dns_error", "http_protocol", "network_error"}):
                        raise
                    self._wait(network_attempt + 1)
                    continue
                if read_only and response.status in (502, 503, 504) and network_attempt + 1 < network_attempts:
                    retry_after = _retry_after(response)
                    # A long (or unparseable) explicit server delay is returned
                    # to the caller instead of sleeping/retrying prematurely.
                    if response.headers.get("retry-after") and (retry_after is None or retry_after > 30):
                        break
                    self._wait(max(network_attempt + 1, retry_after if retry_after is not None else 0))
                    continue
                break
            if response.status in (401, 403) and authenticated and attempt == 0:
                self.tokens.access_token(force_refresh=True, rejected_token=token)
                if stream is not None:
                    stream.seek(start)
                continue
            if response.status == 429:
                delay = _retry_after(response)
                with self._rate_lock:
                    self._not_before = max(self._not_before, time.monotonic() + (delay if delay is not None else 30))
            return response
        raise AuthError("Сервер не принял авторизацию после обновления токена.")

    @staticmethod
    def _require(response: Response, *statuses: int, authenticated: bool = True) -> Response:
        if response.status not in statuses:
            raise RemoteError(response.status, _error_code(response), retry_after=_retry_after(response),
                              authenticated=authenticated)
        return response

    def account_info(self) -> AccountInfo:
        response = self._require(self._request("GET", API_BASE + "/api/v1/user/info"), 200)
        data = _json(response)
        email = data.get("email")
        flag = data.get("unlimited_upload_enabled")
        if not isinstance(email, str) or not email or not isinstance(flag, bool):
            raise ProtocolError("В user/info отсутствуют email или unlimited_upload_enabled; массовая загрузка остановлена.")
        limits = [_integer(data[field], field) for field in ("upload_limit", "max_file_size")
                  if data.get(field) is not None]
        return AccountInfo(email.strip().lower(), flag, min(limits) if limits else None)

    def require_account(self, expected_email: str | None = None) -> AccountInfo:
        account = self.account_info()
        if expected_email and account.email != expected_email.strip().lower():
            raise ProtocolError("Авторизован другой аккаунт, чем привязанный к этой очереди.")
        if not account.unlimited_enabled:
            raise ProtocolError("Сервер вернул unlimited_upload_enabled=false. Загрузка остановлена.")
        return account

    def _metadata_endpoint(self) -> str:
        with self._directory_lock:
            if self._metadata_url and time.monotonic() < self._metadata_until:
                return self._metadata_url
            response = self._require(self._request("GET", DISPATCHER, authenticated=False), 200,
                                     authenticated=False)
            try:
                endpoint = response.body.decode("ascii").split()[0]
            except (UnicodeError, IndexError):
                raise ProtocolError("Не удалось прочитать адрес сервера метаданных.") from None
            validate_url(endpoint)
            if urlsplit(endpoint).query:
                raise ProtocolError("Неожиданные параметры в адресе сервера метаданных.")
            self._metadata_url, self._metadata_until = endpoint, time.monotonic() + 600
            return endpoint

    def quota(self) -> dict[str, int]:
        response = self._require(self._request("GET", QUOTA_BASE + "/api/v4/user/quota"), 200)
        data = _json(response)
        used = data.get("used")
        if not isinstance(used, dict):
            raise ProtocolError("В ответе квоты отсутствует раздел used.")
        return {"used_bytes": _integer(used.get("cloud"), "used.cloud"),
                "total_bytes": _integer(data.get("quota"), "quota"),
                "mail_bytes": _integer(used.get("mail"), "used.mail"),
                "combined_used_bytes": _integer(used.get("total"), "used.total")}

    def legacy_quota(self) -> dict[str, int]:
        """Optional diagnostic only; bulk guard uses v4 used.cloud explicitly."""
        response = self._require(self._request("POST", self._metadata_endpoint(),
                                               headers={"Content-Type": "application/octet-stream"},
                                               body=encode_quota(), token_query=True), 200)
        used, total = parse_quota_response(response.body)
        return {"used_bytes": used, "total_bytes": total}

    def ensure_parent(self, path: str) -> None:
        path = _remote_path(path)
        parent = posixpath.dirname(path)
        if not path.startswith("/") or any(part in (".", "..", "") for part in path.split("/")[1:]):
            raise ValueError("Remote path must be canonical and absolute")
        with self._directory_lock:
            current = ""
            for component in parent.split("/"):
                if not component:
                    continue
                current += "/" + component
                if current in self._directories:
                    continue
                response = self._require(self._request("POST", self._metadata_endpoint(),
                                                       headers={"Content-Type": "application/octet-stream"},
                                                       body=encode_mkdir(current), token_query=True), 200)
                status = parse_mkdir_response(response.body)
                if status not in (0, 4):
                    raise ProtocolError(f"Не удалось создать папку: код метаданных {status}.")
                if len(self._directories) > 20000:
                    self._directories = {"/"}
                self._directories.add(current)

    def stat(self, path: str) -> dict[str, Any] | None:
        path = _remote_path(path)
        response = self._request("GET", API_BASE + "/api/v1/private/stat" + quote(path, safe="/"))
        if response.status == 404:
            return None
        self._require(response, 200)
        data = _json(response)
        size = _integer(data.get("size"), "size")
        digest = data.get("hash")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", digest):
            raise ProtocolError("Сервер не вернул ожидаемый хеш файла.")
        # The read URL supplies the identity when older servers omit a path.
        # An explicit conflicting path must never be silently ignored.
        reported_path = path
        for key in ("path", "home"):
            if key in data:
                try:
                    checked = _remote_path(data[key])
                    checked.encode("utf-8")
                except (ValueError, UnicodeError):
                    raise FileVerificationError("Сервер вернул некорректный путь проверяемого файла.") from None
                if checked.casefold() != path.casefold():
                    raise FileVerificationError("Сервер вернул другой путь проверяемого файла.")
                reported_path = checked
        result = {"size": size, "hash": digest.upper(), "path": reported_path}
        if "nodeid" in data:
            result["nodeid"] = _node_id(data["nodeid"])
        if "type" in data:
            result["type"] = data["type"]
        if type(data.get("is_unlim_upload")) is bool:
            result["is_unlim_upload"] = data["is_unlim_upload"]
        return result

    def list_folder(self, path: str = "/", *, offset: int = 0, limit: int = 500) -> dict[str, Any]:
        """Read one bounded page of the authenticated account's own tree.

        Wire grounding: rclone's Mail backend listM1 uses POST /api/m1/folder
        with form field home and query parameters offset/limit/access_token.
        Its FolderInfoResponse supplies direct-child count, home, kind and
        list, and may include tree/rev/grev. Those revisions are returned for
        per-directory consistency checks; they are NOT a documented global
        snapshot token. Callers must discard incomplete inventories.

        total/next_offset and listed_paths describe the raw page, including
        excluded mounted shares and virtual collections. The latter permits
        cross-page overlap detection without retaining excluded objects.
        File hash, size, and canonical home
        are required. Optional billing is copied only when an exact boolean
        is_unlim_upload was returned; a missing flag remains unknown.
        No public URL, recycle-bin endpoint, or foreign mount is followed.
        """
        path = "/" if path == "/" else _remote_path(path)
        if type(offset) is not int or not 0 <= offset <= (1 << 63) - 1:
            raise ValueError("Folder offset must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Folder page size must be between 1 and 1000")
        response = self._require(self._request(
            "POST", QUOTA_BASE + "/api/m1/folder?" + urlencode({"offset": offset, "limit": limit}),
            token_query=True, token_parameter="access_token", folder_read=True,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode({"home": path}).encode("ascii"),
        ), 200)
        payload = _json(response)
        status = _integer(payload.get("status"), "status")
        if status != 200:
            raise RemoteError(status, "REMOTE_ERROR")
        body = payload.get("body")
        own_folders = {"folder", "camera-upload", "shared"}
        excluded_kinds = {"mounted", "public", "trash", "trashbin", "album", "collection", "recent", "search"}
        if (not isinstance(body, dict) or not isinstance(body.get("kind"), str)
                or body["kind"] not in own_folders):
            raise ProtocolError("Облако не подтвердило чтение собственной папки; сверка не завершена.")

        def checked_path(value, *, root=False):
            try:
                result = "/" if root and value == "/" else _remote_path(value)
                result.encode("utf-8")
                return result
            except (ValueError, UnicodeError):
                raise ProtocolError("В списке Облака получен некорректный путь; сверка не завершена.") from None

        home = checked_path(body.get("home"), root=True)
        if home.casefold() != path.casefold():
            raise ProtocolError("Список Облака относится к другой папке; сверка не завершена.")
        count = body.get("count")
        if not isinstance(count, dict):
            raise ProtocolError("Облако не сообщило число элементов папки; сверка не завершена.")
        total = _integer(count.get("files"), "count.files") + _integer(count.get("folders"), "count.folders")
        if total > (1 << 63) - 1:
            raise ProtocolError("Некорректное число элементов папки Облака.")
        raw_items = body.get("list")
        # Some empty-folder responses omit the optional list field. A missing
        # list for a nonempty folder must never imply that its files vanished.
        if raw_items is None and "list" not in body and total == 0:
            raw_items = []
        if not isinstance(raw_items, list):
            raise ProtocolError("Облако не вернуло список элементов папки; сверка не завершена.")
        if (len(raw_items) > limit or offset > total or offset + len(raw_items) > total
                or (not raw_items and offset < total)):
            raise ProtocolError("Пагинация списка Облака неполна или изменилась; повторите сверку.")
        items, listed_paths, seen, excluded = [], [], set(), 0
        for raw in raw_items:
            check_cancelled(self.request_stop)
            if not isinstance(raw, dict):
                raise ProtocolError("Некорректная запись списка Облака; сверка не завершена.")
            child = checked_path(raw.get("home"))
            if posixpath.dirname(child).casefold() != home.casefold() or child.casefold() in seen:
                raise ProtocolError("Список Облака содержит повторный или посторонний путь; сверка не завершена.")
            seen.add(child.casefold())
            listed_paths.append(child)
            kind = raw.get("kind")
            if not isinstance(kind, str):
                raise ProtocolError("В списке Облака неизвестный тип объекта; сверка не завершена.")
            if kind in excluded_kinds:
                excluded += 1
                continue
            if kind in own_folders:
                item = {"kind": "folder", "path": child}
            elif kind == "file":
                digest = raw.get("hash")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", digest):
                    raise ProtocolError("В списке Облака нет полного хеша файла; сверка не завершена.")
                item = {"kind": "file", "path": child, "hash": digest.upper(),
                        "size": _integer(raw.get("size"), "size")}
                if type(raw.get("is_unlim_upload")) is bool:
                    item["is_unlim_upload"] = raw["is_unlim_upload"]
            else:
                raise ProtocolError("В списке Облака неизвестный тип объекта; сверка не завершена.")
            node = _node_id(raw.get("nodeid"))
            if node:
                item["nodeid"] = node
            items.append(item)
        revision = {}
        if "tree" in body:
            if not isinstance(body["tree"], str) or not body["tree"] or len(body["tree"]) > 256:
                raise ProtocolError("Облако вернуло некорректный идентификатор дерева.")
            revision["tree"] = body["tree"]
        for key in ("rev", "grev"):
            if key in body:
                revision[key] = _integer(body[key], key)
        next_offset = offset + len(raw_items)
        return {"path": home, "items": items, "total": total,
                "next_offset": next_offset if next_offset < total else None,
                "revision": revision or None, "listed": len(raw_items), "excluded": excluded,
                "listed_paths": listed_paths}

    def _trash_stat(self, path: str) -> dict[str, Any] | None:
        """Read M1's explicit file kind before a file-only trash operation."""
        response = self._request(
            "GET", QUOTA_BASE + "/api/m1/file?" + urlencode({"home": path, "offset": 0, "limit": 1}),
            token_query=True, token_parameter="access_token",
        )
        if response.status == 404:
            return None
        self._require(response, 200)
        data = _json(response)
        status = _integer(data.get("status"), "status")
        if status == 404:
            return None
        if status != 200:
            raise RemoteError(status, "REMOTE_ERROR")
        item = data.get("body")
        if not isinstance(item, dict) or item.get("kind") != "file":
            raise FileVerificationError("Удаление отменено: Облако не подтвердило, что объект является файлом.")
        if not isinstance(item.get("home"), str) or unicodedata.normalize("NFC", item["home"]) != path:
            raise FileVerificationError("Удаление отменено: Облако вернуло другой путь объекта.")
        digest = item.get("hash")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", digest):
            raise FileVerificationError("Удаление отменено: хеш файла в Облаке не подтверждён.")
        return {"hash": digest.upper(), "size": _integer(item.get("size"), "size"),
                "nodeid": _node_id(item.get("nodeid"))}

    def trash_file(self, path: str, *, expected_hash: str, expected_size: int,
                   expected_node_id: str | None = None) -> dict[str, Any]:
        """Move one identity-checked file to Cloud's recycle bin.

        Wire grounding: rclone's Mail.ru backend ``readItemMetaData``,
        ``Object.Remove`` and ``delete(..., false)`` use M1 /file and
        /file/remove. Its permanent cleanup uses a different /trashbin/empty
        endpoint, intentionally absent here. Source:
        https://github.com/rclone/rclone/blob/master/backend/mailru/mailru.go
        Mail documents the recycle-bin semantics at
        https://help.mail.ru/cloud/files/basket/ . Trashed bytes can still
        consume quota until Mail clears its recycle bin.

        The service offers no documented atomic hash precondition on this
        operation; callers must not modify the same cloud path concurrently.
        A lost response never triggers a blind second delete. A retry starts
        with a new identity check, and success requires a follow-up 404.
        """
        path = _remote_path(path)
        if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", expected_hash):
            raise ValueError("Expected cloud hash is required for deletion")
        expected_hash = expected_hash.upper()
        expected_size = _integer(expected_size, "expected_size")
        if expected_node_id is not None and not _node_id(expected_node_id):
            raise ValueError("Expected node id must be a nonempty identifier")
        expected_node_id = _node_id(expected_node_id)

        def require_identity(item: dict[str, Any]) -> None:
            if item["hash"] != expected_hash or item["size"] != expected_size:
                raise FileVerificationError("Удаление отменено: файл в Облаке изменился, хеш или размер не совпадает.")

        # Check optional immutable node identity as well as content. M1 does
        # not always return nodeid, so the mobile stat can supply that field.
        node_confirmed = not expected_node_id
        if expected_node_id:
            mobile = self.stat(path)
            if mobile is not None:
                require_identity(mobile)
                if mobile.get("nodeid") != expected_node_id:
                    raise FileVerificationError("Удаление отменено: идентификатор файла в Облаке не совпадает.")
                node_confirmed = True
        existing = self._trash_stat(path)
        if existing is None:
            return {"path": path, "trashed": False, "already_missing": True, "absent": True}
        require_identity(existing)
        if not node_confirmed:
            raise FileVerificationError("Удаление отменено: идентификатор существующего файла в Облаке не подтверждён.")
        if expected_node_id and existing.get("nodeid") and existing["nodeid"] != expected_node_id:
            raise FileVerificationError("Удаление отменено: идентификатор файла в Облаке изменился.")
        check_cancelled(self.request_stop, force=True)
        response = self._require(self._request(
            "POST", QUOTA_BASE + "/api/m1/file/remove", token_query=True, token_parameter="access_token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode({"home": path}).encode("ascii"),
        ), 200)
        status = _integer(_json(response).get("status"), "status")
        if status != 200:
            raise RemoteError(status, "REMOTE_ERROR")
        for retry in range(4):
            remaining = self._trash_stat(path)
            if remaining is None:
                return {"path": path, "trashed": True, "already_missing": False, "absent": True}
            require_identity(remaining)
            if retry < 3:
                self._wait(1 + retry)
        raise FileVerificationError("Облако приняло удаление, но отсутствие файла по прежнему пути не подтверждено.")

    def _upload_url(self, url: str, *, is_retried: bool = False, upload_type: str = "AUTO") -> str:
        """Append the APK's upload query fields, preserving a shard's own query."""
        validate_url(url)
        if not isinstance(is_retried, bool):
            raise ValueError("is_retried must be a boolean")
        if upload_type not in ("AUTO", "MANUAL"):
            raise ValueError("upload_type must be AUTO or MANUAL")
        query = urlencode({
            "platform": "cloud-android", "gaid": "", "is_background": "true",
            "upload_type": upload_type, "session_id": self._session_id,
            "is_retried": "true" if is_retried else "false", "network": "wi-fi",
        })
        return url + ("&" if "?" in url else "?") + query

    @staticmethod
    def _check_blob_limits(response: Response, size: int) -> None:
        """Apply APK upload limits before sending a new or resumed body.

        X-Max-Upload-Limit is a file-size cap. X-Upload-Limit can instead be
        the remaining space; the APK distinguishes that from an oversized
        file when the two limits differ. A 404 must supply X-Upload-Limit;
        partially available blobs may omit it.
        """
        maximum = response.headers.get("x-max-upload-limit")
        maximum = _integer(maximum, "X-Max-Upload-Limit") if maximum is not None else None
        if maximum is not None and size > maximum:
            raise UnsupportedPhoto("Файл превышает максимальный размер загрузки, возвращённый сервером.")
        limit = response.headers.get("x-upload-limit")
        if limit is None and response.status != 404:
            return
        if limit == "-1":
            return  # APK LimitChecker treats this specific value as unknown.
        limit = _integer(limit, "X-Upload-Limit")
        if size > limit:
            if maximum is not None and limit == maximum:
                raise UnsupportedPhoto("Файл превышает максимальный размер загрузки, возвращённый сервером.")
            raise RemoteError(400, "CONDITION/OVERQUOTA")

    def _upload_blob(self, path: str, digest: str, identity: FileIdentity, *, progress=None,
                     is_retried: bool = False, upload_type: str = "AUTO") -> None:
        url = UPLOAD_BASE + "/resumable/" + digest
        deadline = time.monotonic() + 600
        reported = 0

        def report_until(position: int) -> None:
            nonlocal reported
            position = min(identity.size, max(reported, position))
            delta = position - reported
            reported = position
            if progress is not None and delta:
                progress(delta)

        def sent(size: int) -> None:
            report_until(reported + max(0, size))

        while True:
            status = self._request("PUT", self._upload_url(url, is_retried=is_retried, upload_type=upload_type), headers={"Content-Range": f"bytes */{identity.size}",
                                                        "Content-Type": "application/json"})
            if status.status == 201:
                if upload_type == "MANUAL":
                    self._check_blob_limits(status, identity.size)
                # The APK requires a parseable Long, not a positive lifetime.
                # In particular, a confirmed blob may return X-Valid-for: 0.
                _integer(status.headers.get("x-valid-for"), "X-Valid-for")
                report_until(identity.size)
                return
            if status.status == 202:
                if time.monotonic() > deadline:
                    raise TransportError("Сервер ещё обрабатывает файл; повтор будет выполнен из очереди.")
                delay = _integer(status.headers.get("x-wait-for"), "X-Wait-for")
                self._wait(max(1, delay))
                continue
            if status.status == 404:
                if upload_type == "MANUAL":
                    self._check_blob_limits(status, identity.size)
                else:
                    limit = _integer(status.headers.get("x-upload-limit"), "X-Upload-Limit")
                    if identity.size > limit:
                        raise UnsupportedPhoto("Файл превышает лимит загрузки, возвращённый сервером.")
                offset, target = 0, url
            elif status.status == 200:
                if upload_type == "MANUAL":
                    self._check_blob_limits(status, identity.size)
                _integer(status.headers.get("x-valid-for"), "X-Valid-for")
                offset = _integer(status.headers.get("x-received"), "X-Received")
                target = status.headers.get("x-location")
                if offset > identity.size or not isinstance(target, str):
                    raise ProtocolError("Сервер вернул неверное смещение незавершённой загрузки.")
                validate_url(target)
                report_until(offset)
                if offset == identity.size:
                    if time.monotonic() > deadline:
                        raise TransportError("Сервер не завершил обработку загруженных байтов.")
                    self._wait(2)
                    continue
            else:
                self._require(status, 200, 201, 202, 404)
                raise AssertionError("unreachable")
            headers = {"Content-Type": "application/octet-stream"}
            if offset:
                headers["Content-Range"] = f"bytes {offset}-{identity.size - 1}/{identity.size}"
            try:
                with open(path, "rb") as stream:
                    if FileIdentity.from_stat(os.fstat(stream.fileno())) != identity:
                        raise SourceChanged("Файл изменился перед передачей.")
                    stream.seek(offset)
                    uploaded = self._request("PUT", self._upload_url(target, is_retried=is_retried, upload_type=upload_type), headers=headers, stream=stream,
                                             length=identity.size - offset, progress=sent if progress is not None else None)
                    if FileIdentity.from_stat(os.fstat(stream.fileno())) != identity:
                        raise SourceChanged("Файл изменился во время передачи.")
            except OSError:
                raise SourceReadError("Не удалось прочитать исходный файл для передачи.") from None
            self._require(uploaded, 201, 202)
            if uploaded.status == 201:
                if upload_type == "MANUAL":
                    self._check_blob_limits(uploaded, identity.size)
                _integer(uploaded.headers.get("x-valid-for"), "X-Valid-for")
                report_until(identity.size)
                return
            self._wait(max(1, _integer(uploaded.headers.get("x-wait-for"), "X-Wait-for")))

    def upload_photo(self, path: str, remote_path: str, *, expected: FileIdentity,
                     prior_intent: dict[str, Any] | None = None, save_intent: Callable[[str], None],
                     save_confirmation: Callable[[str], None] | None = None,
                     save_observation: Callable[[dict[str, Any]], None] | None = None,
                     progress=None, is_retried: bool = False) -> dict[str, Any]:
        """Send original media bytes through the working photo upload flow.

        The method name remains compatible. This operation never falls back
        to an ordinary upload; the caller explicitly selects that mode.
        """
        remote_path = _remote_path(remote_path)
        if expected.size <= 20:
            raise UnsupportedPhoto("Файл слишком мал для поддерживаемого формата фото или видео.")
        if expected.size > MAX_MEDIA_BYTES:
            raise UnsupportedPhoto("Файл больше 2 ГБ (2 147 483 648 байт) — лимита автозагрузки фото и видео.")
        digest, identity = cloud_hash(path, expected=expected, stop=self.request_stop)
        existing = self.stat(remote_path)
        if existing is not None:
            if (existing["hash"] == digest and existing["size"] == identity.size and prior_intent
                    and prior_intent.get("cloud_hash") == digest and prior_intent.get("remote_path") == remote_path
                    and prior_intent.get("storage_mode", "unlimited") == "unlimited"):
                saved = prior_intent.get("storage_observation")
                known_regular = (isinstance(saved, dict) and saved.get("hash") == digest
                                 and saved.get("size") == identity.size and saved.get("path") == remote_path
                                 and saved.get("mode", saved.get("storage_mode")) == "regular"
                                 and saved.get("is_unlim_upload") is False)
                if known_regular or prior_intent.get("unlimited_confirmed") is not True:
                    observation = _commit_observation(
                        digest, identity.size, remote_path, "regular" if known_regular else "unknown",
                        nodeid=existing.get("nodeid") or (saved.get("nodeid") if known_regular else None),
                        flag=False if known_regular else None, verified=True, committed=False,
                    )
                    if save_observation is not None:
                        save_observation(observation)
                    raise UnlimitedNotConfirmed("Файл уже есть в Облаке и совпадает по хешу, но подтверждение "
                                                "is_unlim_upload=true для него не сохранено. Автоматическое "
                                                "признание загрузки безлимитной остановлено; проверьте отчёт.",
                                                observation=observation)
                result = {"hash": digest, "size": identity.size, "path": remote_path,
                        "commit_accepted": False, "recovered_commit": True, "stat_matched": True,
                        "mode": "unlimited", "verified": True, "nodeid": _node_id(existing.get("nodeid")),
                        "storage_mode": "unlimited", "is_unlim_upload": True, "unlimited_evidence": "saved_commit_response",
                        "quota_status": "server_confirmed_unlimited"}
                if save_observation is not None:
                    save_observation(result)
                return result
            raise RemoteConflict("Файл с этим путём уже есть в Облаке. Перезапись отключена.")
        self.ensure_parent(remote_path)
        self._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried)
        try:
            current = FileIdentity.from_stat(os.stat(path, follow_symlinks=False))
        except OSError:
            raise SourceReadError("Исходный файл стал недоступен до подтверждения загрузки.") from None
        if identity != current:
            raise SourceChanged("Файл изменился до подтверждения загрузки.")
        # A durable pause may come from another process with no in-memory
        # notification. Re-read it at the boundary before starting a commit.
        check_cancelled(self.request_stop, force=True)
        save_intent(digest)
        payload = {
            "hash": digest, "last_modified": max(0, identity.mtime_ns // 1_000_000),
            "path": remote_path, "size": identity.size, "unlimited_upload": True,
            "options": {"autorename": False, "excl_by_hash": False, "overwrite": False},
            "overwrite": False, "platform": "cloud-android", "is_background": True,
            "gaid": "", "upload_type": "AUTO",
        }
        response = self._require(self._request("POST", API_BASE + "/api/v1/private/add",
                                               headers={"Content-Type": "application/json"},
                                               body=json.dumps(payload, ensure_ascii=False).encode("utf-8")), 200)
        added = _json(response)
        if added.get("name") != posixpath.basename(remote_path):
            raise FileVerificationError("Сервер вернул другое имя файла; задача не отмечена загруженной.")
        if added.get("path") not in (None, "", remote_path):
            raise FileVerificationError("Сервер вернул другой путь файла; задача не отмечена загруженной.")
        flag = added.get("is_unlim_upload")
        mode = "unlimited" if flag is True else "regular" if flag is False else "unknown"
        observation = _commit_observation(digest, identity.size, remote_path, mode,
                                          nodeid=added.get("nodeid"), flag=flag)
        # This callback happens before verification, cancellation checks and
        # policy exceptions. A later stat failure cannot erase accepted bytes.
        if save_observation is not None:
            save_observation(observation)
        if added.get("is_unlim_upload") is not True:
            detail = ("false" if added.get("is_unlim_upload") is False else
                      "отсутствует" if "is_unlim_upload" not in added else "некорректного типа")
            raise UnlimitedNotConfirmed("Сервер добавил файл, но не подтвердил безлимит: is_unlim_upload="
                                        + detail + ". Задача не отмечена загруженной; проверьте отчёт и квоту.",
                                        observation=observation)
        # Persist the server's positive per-file proof before the follow-up
        # stat. A matching hash alone cannot establish unlimited billing after
        # an ambiguous/lost commit response.
        if save_confirmation is not None:
            save_confirmation(digest)
        result = None
        for retry in range(4):
            result = self.stat(remote_path)
            if result is not None:
                break
            self._wait(1 + retry)
        if result is None or result["hash"] != digest or result["size"] != identity.size:
            raise FileVerificationError("После добавления не подтверждены хеш и размер файла в Облаке.")
        if observation["nodeid"] and result.get("nodeid") and observation["nodeid"] != result["nodeid"]:
            raise FileVerificationError("После добавления изменился идентификатор файла в Облаке.")
        verified = {**observation, "verified": True,
                "recovered_commit": False, "stat_matched": True,
                "storage_mode": "unlimited", "is_unlim_upload": True, "unlimited_evidence": "commit_response",
                "quota_status": "server_confirmed_unlimited"}
        if save_observation is not None:
            save_observation(verified)
        return verified

    def upload_regular(self, path: str, remote_path: str, *, expected: FileIdentity,
                       prior_intent: dict[str, Any] | None = None, save_intent: Callable[[str], None],
                       save_confirmation: Callable[[str], None] | None = None,
                       save_observation: Callable[[dict[str, Any]], None] | None = None,
                       reserve_quota: Callable[[int, int], None] | None = None,
                       progress=None, is_retried: bool = False,
                       account: AccountInfo | None = None) -> dict[str, Any]:
        """Explicit create-only upload into the account storage quota.

        Callers must obtain the user's opt-in before choosing this operation.
        It is never called as a fallback from upload_photo. The native APK's
        create-only metadata operation is used, with no overwrite/rename.
        A positive operation status is saved before checking the remote file;
        hash equality alone cannot recover an ambiguous earlier commit.
        """
        remote_path = _remote_path(remote_path)
        existing = self.stat(remote_path)
        if existing is None:
            # Check caps before hashing a multi-gigabyte source or sending its
            # body. A saved, already-committed file needs no free space again.
            info = account if account is not None else self.account_info()
            if info.upload_limit is None:
                raise ProtocolError("Сервер не сообщил максимальный размер обычной загрузки для аккаунта.")
            limit = _positive_integer(info.upload_limit, "upload_limit")
            if expected.size > limit:
                raise UnsupportedPhoto("Файл превышает максимальный размер обычной загрузки для этого аккаунта.")
            quota = self.quota()
            available = max(0, quota["total_bytes"] - quota["combined_used_bytes"])
            if expected.size > available:
                raise RemoteError(400, "CONDITION/OVERQUOTA")
        digest, identity = cloud_hash(path, expected=expected, stop=self.request_stop)
        if existing is not None:
            if (existing["hash"] == digest and existing["size"] == identity.size and prior_intent
                    and prior_intent.get("cloud_hash") == digest and prior_intent.get("remote_path") == remote_path
                    and prior_intent.get("storage_mode") == "regular"):
                if prior_intent.get("regular_confirmed") is not True:
                    raise RemoteConflict("Файл уже есть в Облаке и совпадает по хешу, но подтверждение обычной "
                                         "загрузки для него не сохранено. Перезапись и автоматическое признание "
                                         "неподтверждённой загрузки завершённой отключены.")
                result = {"hash": digest, "size": identity.size, "path": remote_path,
                        "commit_accepted": False, "recovered_commit": True, "stat_matched": True,
                        "mode": "regular", "verified": True, "nodeid": _node_id(existing.get("nodeid")),
                        "storage_mode": "regular", "regular_evidence": "saved_commit_response",
                        "quota_status": "uses_cloud_quota"}
                if save_observation is not None:
                    save_observation(result)
                return result
            raise RemoteConflict("Файл с этим путём уже есть в Облаке. Перезапись отключена.")
        body = encode_add_file(remote_path, identity.size, max(0, identity.mtime_ns // 1_000_000_000), digest)
        # Batch workers share a reservation budget. Already committed objects
        # above returned before reaching this point and need no new space.
        if reserve_quota is not None:
            reserve_quota(identity.size, available)
        self.ensure_parent(remote_path)
        self._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried, upload_type="MANUAL")
        try:
            current = FileIdentity.from_stat(os.stat(path, follow_symlinks=False))
        except OSError:
            raise SourceReadError("Исходный файл стал недоступен до подтверждения загрузки.") from None
        if identity != current:
            raise SourceChanged("Файл изменился до подтверждения загрузки.")
        check_cancelled(self.request_stop, force=True)
        save_intent(digest)
        response = self._require(self._request("POST", self._metadata_endpoint(),
                                               headers={"Content-Type": "application/octet-stream"},
                                               body=body, token_query=True), 200)
        try:
            status = parse_add_file_response(response.body)
        except MetadataProtocolError:
            raise ProtocolError("Сервер не вернул статус принятия обычной загрузки; подтвердить создание файла нельзя.") from None
        if status in (4, 9, 12):
            raise RemoteConflict("Облако не подтвердило создание нового файла: путь уже существует или конфликтует. "
                                 "Перезапись и автоматическое переименование отключены.")
        if status == 7:
            raise RemoteError(400, "CONDITION/OVERQUOTA")
        if status == 253:
            raise RemoteError(400, "CONDITION/BADBLOB")
        if status != 0:
            raise ProtocolError(f"Облако не подтвердило обычную загрузку: код метаданных {status}.")
        observation = _commit_observation(digest, identity.size, remote_path, "regular")
        observation["regular_evidence"] = "metadata_commit_response"
        if save_observation is not None:
            save_observation(observation)
        if save_confirmation is not None:
            save_confirmation(digest)
        result = None
        for retry in range(4):
            result = self.stat(remote_path)
            if result is not None:
                break
            self._wait(1 + retry)
        if result is None or result["hash"] != digest or result["size"] != identity.size:
            raise FileVerificationError("После добавления не подтверждены хеш и размер файла в Облаке.")
        verified = {**observation, "verified": True, "nodeid": _node_id(result.get("nodeid")),
                "commit_accepted": True, "recovered_commit": False, "stat_matched": True,
                "storage_mode": "regular", "regular_evidence": "metadata_commit_response",
                "quota_status": "uses_cloud_quota"}
        if save_observation is not None:
            save_observation(verified)
        return verified

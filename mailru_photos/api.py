"""Mobile media uploads and a separate, explicit ordinary upload operation.

See docs/PROTOCOL_RU.md for APK evidence and limits of offline verification.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import posixpath
import re
import threading
import time
import uuid
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlencode, urlsplit

from .auth import AuthError, TokenManager
from .media import UNLIMITED_MAX_BYTES
from .metadata import (
    MetadataProtocolError, encode_add_file, encode_mkdir, encode_quota,
    parse_add_file_response, parse_mkdir_response, parse_quota_response,
)
from .transport import Cancelled, HTTP, Response, TransportError, validate_url


API_BASE = "https://android-api.cloud.mail.ru"
QUOTA_BASE = "https://cloud.mail.ru"
UPLOAD_BASE = "https://uploader.cloud.mail.ru"
DISPATCHER = "https://dispatcher.cloud.mail.ru/m"
# Exact APK RemoteParams unlimited_upload_max_file_size default. An ordinary
# account upload limit can be higher and is checked separately for that mode.
MAX_MEDIA_BYTES = UNLIMITED_MAX_BYTES
MAX_PHOTO_BYTES = MAX_MEDIA_BYTES  # Compatibility for existing Python callers.
PROTOCOL_VERSION = "cloud-mail-5.117.0.10079902-unlimited-v2-confirmed"


class ProtocolError(RuntimeError):
    """A failure to interpret the protocol; do not keep uploading blindly."""


class RemoteError(ProtocolError):
    def __init__(self, status: int, code: str = "REMOTE_ERROR", *, retry_after: float | None = None):
        self.status, self.code, self.retry_after = status, code, retry_after
        self.retryable = status == 429 or status >= 500 or code == "CONDITION/BADBLOB"
        self.unsupported = code in {"NOT/SUPPORTED", "CONDITION/BODYTOOBIG"}
        self.fatal = status in {401, 403} or code in {
            "CONDITION/OVERQUOTA", "CONDITION/NOT_ACCEPTABLE", "CONDITION/NOT_ACCEPTABLE/UNLIM_UPL_TOTAL"
        }
        super().__init__(f"Mail.ru: HTTP {status}, {code}")


class RemoteConflict(ProtocolError):
    pass


class UnlimitedNotConfirmed(ProtocolError):
    """The server has not supplied a durable, positive per-file billing proof."""


class SourceChanged(ProtocolError):
    pass


class UnsupportedPhoto(ProtocolError):
    """Unsupported media; the original exception name remains compatible."""


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
                if stop is not None and stop.is_set():
                    raise Cancelled("Загрузка остановлена.")
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
        return self._local.http

    def close_thread(self) -> None:
        if hasattr(self._local, "http"):
            self._local.http.close()
            del self._local.http

    def _wait(self, seconds: float) -> None:
        if self.stop.wait(max(0, seconds)):
            raise Cancelled("Загрузка остановлена.")

    def _request(self, method: str, url: str, *, headers: dict[str, str] | None = None,
                 body: bytes = b"", stream=None, length=None, authenticated: bool = True,
                 token_query: bool = False, progress=None) -> Response:
        # Retry only reads. A failed PUT/POST may already have been accepted;
        # retrying those belongs to the durable upload/reconciliation workflow.
        read_only = method == "GET" and not body and stream is None
        network_attempts = 3 if read_only else 1
        for attempt in range(2):
            actual_headers = dict(headers or {})
            token = self.tokens.access_token() if authenticated else ""
            actual_url = url
            if authenticated and token_query:
                actual_url += ("&" if "?" in actual_url else "?") + urlencode({
                    "token": token, "client_id": self.tokens.tokens.client_id
                })
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
    def _require(response: Response, *statuses: int) -> Response:
        if response.status not in statuses:
            raise RemoteError(response.status, _error_code(response), retry_after=_retry_after(response))
        return response

    def account_info(self) -> AccountInfo:
        response = self._require(self._request("GET", API_BASE + "/api/v1/user/info"), 200)
        data = _json(response)
        email = data.get("email")
        flag = data.get("unlimited_upload_enabled")
        if not isinstance(email, str) or not email or not isinstance(flag, bool):
            raise ProtocolError("В user/info отсутствуют email или unlimited_upload_enabled; массовая загрузка остановлена.")
        # The Android DTO names this upload_limit. Also honor max_file_size
        # if a server version supplies it; use the smaller reported value.
        limits = [_integer(data[name], name) for name in ("upload_limit", "max_file_size")
                  if data.get(name) is not None]
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
            response = self._require(self._request("GET", DISPATCHER, authenticated=False), 200)
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
        response = self._request("GET", API_BASE + "/api/v1/private/stat" + quote(path, safe="/"))
        if response.status in (404, 405):
            return None
        self._require(response, 200)
        data = _json(response)
        size = _integer(data.get("size"), "size")
        digest = data.get("hash")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", digest):
            raise ProtocolError("Сервер не вернул ожидаемый хеш файла.")
        return {"size": size, "hash": digest.upper()}

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
        while True:
            status = self._request("PUT", self._upload_url(url, is_retried=is_retried, upload_type=upload_type), headers={"Content-Range": f"bytes */{identity.size}",
                                                        "Content-Type": "application/json"})
            if status.status == 201:
                if upload_type == "MANUAL":
                    self._check_blob_limits(status, identity.size)
                # The APK requires a parseable Long, not a positive lifetime.
                # In particular, a confirmed blob may return X-Valid-for: 0.
                _integer(status.headers.get("x-valid-for"), "X-Valid-for")
                return
            if status.status == 202:
                if time.monotonic() > deadline:
                    raise TransportError("Сервер ещё обрабатывает файл; повтор будет выполнен из очереди.")
                delay = _integer(status.headers.get("x-wait-for"), "X-Wait-for")
                self._wait(max(1, delay))
                continue
            if status.status == 404:
                self._check_blob_limits(status, identity.size)
                offset, target = 0, url
            elif status.status == 200:
                self._check_blob_limits(status, identity.size)
                _integer(status.headers.get("x-valid-for"), "X-Valid-for")
                offset = _integer(status.headers.get("x-received"), "X-Received")
                target = status.headers.get("x-location")
                if offset > identity.size or not isinstance(target, str):
                    raise ProtocolError("Сервер вернул неверное смещение незавершённой загрузки.")
                validate_url(target)
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
            with open(path, "rb") as stream:
                if FileIdentity.from_stat(os.fstat(stream.fileno())) != identity:
                    raise SourceChanged("Файл изменился перед передачей.")
                stream.seek(offset)
                uploaded = self._request("PUT", self._upload_url(target, is_retried=is_retried, upload_type=upload_type), headers=headers, stream=stream,
                                         length=identity.size - offset, progress=progress)
                if FileIdentity.from_stat(os.fstat(stream.fileno())) != identity:
                    raise SourceChanged("Файл изменился во время передачи.")
            self._require(uploaded, 201, 202)
            if uploaded.status == 201:
                if upload_type == "MANUAL":
                    self._check_blob_limits(uploaded, identity.size)
                _integer(uploaded.headers.get("x-valid-for"), "X-Valid-for")
                return
            self._wait(max(1, _integer(uploaded.headers.get("x-wait-for"), "X-Wait-for")))

    def upload_photo(self, path: str, remote_path: str, *, expected: FileIdentity,
                     prior_intent: dict[str, Any] | None = None, save_intent: Callable[[str], None],
                     save_confirmation: Callable[[str], None] | None = None,
                     progress=None, is_retried: bool = False) -> dict[str, Any]:
        """Upload original photo or video bytes through the same mobile flow.

        The legacy method name is retained for runner and Python compatibility.
        The server, rather than a file extension, confirms media eligibility.
        """
        if expected.size <= 20:
            raise UnsupportedPhoto("Файл слишком мал для поддерживаемого формата фото или видео.")
        if expected.size > MAX_MEDIA_BYTES:
            raise UnsupportedPhoto("Файл больше 2 ГБ (2 147 483 648 байт) — лимита автозагрузки фото и видео.")
        digest, identity = cloud_hash(path, expected=expected, stop=self.stop)
        existing = self.stat(remote_path)
        if existing is not None:
            if (existing["hash"] == digest and existing["size"] == identity.size and prior_intent
                    and prior_intent.get("cloud_hash") == digest and prior_intent.get("remote_path") == remote_path
                    and prior_intent.get("storage_mode", "unlimited") == "unlimited"):
                if prior_intent.get("unlimited_confirmed") is not True:
                    raise UnlimitedNotConfirmed("Файл уже есть в Облаке и совпадает по хешу, но подтверждение "
                                                "is_unlim_upload=true для него не сохранено. Автоматическое "
                                                "признание загрузки безлимитной остановлено; проверьте отчёт.")
                return {"hash": digest, "size": identity.size, "path": remote_path,
                        "commit_accepted": False, "recovered_commit": True, "stat_matched": True,
                        "storage_mode": "unlimited",
                        "is_unlim_upload": True, "unlimited_evidence": "saved_commit_response",
                        "quota_status": "server_confirmed_unlimited"}
            raise RemoteConflict("Файл с этим путём уже есть в Облаке. Перезапись отключена.")
        self.ensure_parent(remote_path)
        self._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried)
        if identity != FileIdentity.from_stat(os.stat(path, follow_symlinks=False)):
            raise SourceChanged("Файл изменился до подтверждения загрузки.")
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
            raise ProtocolError("Сервер вернул другое имя файла; массовая загрузка остановлена.")
        if added.get("path") not in (None, "", remote_path):
            raise ProtocolError("Сервер вернул другой путь файла; массовая загрузка остановлена.")
        if added.get("is_unlim_upload") is not True:
            detail = ("false" if added.get("is_unlim_upload") is False else
                      "отсутствует" if "is_unlim_upload" not in added else "некорректного типа")
            raise UnlimitedNotConfirmed("Сервер добавил файл, но не подтвердил безлимит: is_unlim_upload="
                                        + detail + ". Массовая загрузка остановлена; проверьте отчёт и квоту.")
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
            raise ProtocolError("После добавления не подтверждены хеш и размер файла в Облаке.")
        return {"hash": digest, "size": identity.size, "path": remote_path,
                "nodeid": str(added.get("nodeid", "")), "commit_accepted": True,
                "recovered_commit": False, "stat_matched": True,
                "storage_mode": "unlimited",
                "is_unlim_upload": True, "unlimited_evidence": "commit_response",
                "quota_status": "server_confirmed_unlimited"}

    def upload_regular(self, path: str, remote_path: str, *, expected: FileIdentity,
                       prior_intent: dict[str, Any] | None = None, save_intent: Callable[[str], None],
                       save_confirmation: Callable[[str], None] | None = None,
                       progress=None, is_retried: bool = False,
                       account: AccountInfo | None = None) -> dict[str, Any]:
        """Explicit ordinary upload for media above the autoupload size limit.

        Callers must obtain the user's opt-in before choosing this operation.
        It is never called as a fallback from upload_photo. The native APK's
        create-only metadata operation is used, with no overwrite/rename.
        A complete success response is saved before checking the remote file;
        hash equality alone cannot recover an ambiguous earlier commit.
        """
        if expected.size <= MAX_MEDIA_BYTES:
            raise UnsupportedPhoto("Обычная загрузка разрешена только для файлов больше 2 ГБ (2 147 483 648 байт).")
        existing = self.stat(remote_path)
        if existing is None:
            # Check caps before hashing a multi-gigabyte source or sending its
            # body. A saved, already-committed file needs no free space again.
            info = account if account is not None else self.require_account()
            if info.upload_limit is None:
                raise ProtocolError("Сервер не сообщил максимальный размер обычной загрузки для аккаунта.")
            limit = _positive_integer(info.upload_limit, "upload_limit")
            if expected.size > limit:
                raise UnsupportedPhoto("Файл превышает максимальный размер обычной загрузки для этого аккаунта.")
            quota = self.quota()
            available = max(0, quota["total_bytes"] - quota["combined_used_bytes"])
            if expected.size > available:
                raise RemoteError(400, "CONDITION/OVERQUOTA")
        digest, identity = cloud_hash(path, expected=expected, stop=self.stop)
        if existing is not None:
            if (existing["hash"] == digest and existing["size"] == identity.size and prior_intent
                    and prior_intent.get("cloud_hash") == digest and prior_intent.get("remote_path") == remote_path
                    and prior_intent.get("storage_mode") == "regular"):
                if prior_intent.get("regular_confirmed") is not True:
                    raise RemoteConflict("Файл уже есть в Облаке и совпадает по хешу, но подтверждение обычной "
                                         "загрузки для него не сохранено. Перезапись и автоматическое признание "
                                         "неподтверждённой загрузки завершённой отключены.")
                return {"hash": digest, "size": identity.size, "path": remote_path,
                        "commit_accepted": False, "recovered_commit": True, "stat_matched": True,
                        "storage_mode": "regular", "regular_evidence": "saved_commit_response",
                        "quota_status": "uses_cloud_quota"}
            raise RemoteConflict("Файл с этим путём уже есть в Облаке. Перезапись отключена.")
        body = encode_add_file(remote_path, identity.size, max(0, identity.mtime_ns // 1_000_000_000), digest)
        self.ensure_parent(remote_path)
        self._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried, upload_type="MANUAL")
        if identity != FileIdentity.from_stat(os.stat(path, follow_symlinks=False)):
            raise SourceChanged("Файл изменился до подтверждения загрузки.")
        save_intent(digest)
        response = self._require(self._request("POST", self._metadata_endpoint(),
                                               headers={"Content-Type": "application/octet-stream"},
                                               body=body, token_query=True), 200)
        try:
            status = parse_add_file_response(response.body)
        except MetadataProtocolError:
            raise ProtocolError("Сервер не вернул полное подтверждение обычной загрузки в ответе метаданных.") from None
        if status in (4, 9, 12):
            raise RemoteConflict("Облако не подтвердило создание нового файла: путь уже существует или конфликтует. "
                                 "Перезапись и автоматическое переименование отключены.")
        if status == 7:
            raise RemoteError(400, "CONDITION/OVERQUOTA")
        if status == 253:
            raise RemoteError(400, "CONDITION/BADBLOB")
        if status != 0:
            raise ProtocolError(f"Облако не подтвердило обычную загрузку: код метаданных {status}.")
        if save_confirmation is not None:
            save_confirmation(digest)
        result = None
        for retry in range(4):
            result = self.stat(remote_path)
            if result is not None:
                break
            self._wait(1 + retry)
        if result is None or result["hash"] != digest or result["size"] != identity.size:
            raise ProtocolError("После добавления не подтверждены хеш и размер файла в Облаке.")
        return {"hash": digest, "size": identity.size, "path": remote_path,
                "commit_accepted": True, "recovered_commit": False, "stat_matched": True,
                "storage_mode": "regular", "regular_evidence": "commit_response",
                "quota_status": "uses_cloud_quota"}

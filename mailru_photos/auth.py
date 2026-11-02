"""Mail.ru OAuth for the account owner; no embedded application secrets.

The password and refresh request shapes are evidenced by Cloud Mail APK
5.117.0.10079902: OAuthAuthRequest and OAuthAccessTokenRefreshRequest.
New app-password logins use cloud-win, as in rclone's Mailru backend. This
client ID accepted the owner's app password and its token was accepted by the
Android user/info API; cloud-android rejected the same password with code 3.
HostType.AUTH is resolved through dispatcher.cloud.mail.ru/o; the public
dispatcher returned https://o2.mail.ru/token during this investigation.

Pass a dedicated application password interactively, never on a command line.
Tokens remain on the local machine. Windows stores them with user-scoped DPAPI;
POSIX uses a mode-0600 file. Passwords are never persisted.
"""

from __future__ import annotations

import base64
import ctypes
import json
import math
import os
import re
import stat
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


TOKEN_ENDPOINT = "https://o2.mail.ru/token"
DEFAULT_CLIENT_ID = "cloud-win"
USER_AGENT = "mailru-photo-client/0.3 (personal photo uploader)"
MAX_RESPONSE_BYTES = 1024 * 1024

# Names from the APK's named OAuthError enum, not free-form server text.
SERVER_ERROR_NAMES = {
    1: "INVALID_CLIENT", 2: "INVALID_REQUEST", 3: "INVALID_USERNAME_OR_PASSWORD",
    6: "TOKEN_NOT_FOUND", 11: "EXTERNAL_OAUTH_LOGIN", 13: "FORBIDDEN",
    14: "FIRST_STEP_REQUIRED", 15: "SECOND_STEP_REQUIRED",
    16: "SECOND_STEP_RATE_LIMIT", 17: "INVALID_AUTH_CODE", 21: "BLOCKED_USER",
    26: "RE_CAPTCHA",
}
OAUTH_ERROR_NAMES = frozenset({
    "invalid_grant", "invalid_client", "invalid_request", "unauthorized_client",
    "unsupported_grant_type", "invalid_scope", "access_denied", "need_2fa",
    "temporarily_unavailable", "server_error", "unknown",
})


def _error_number(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value):
        value = int(value)
    if isinstance(value, int) and 0 <= value <= 2**31 - 1:
        return value
    return None


class AuthError(RuntimeError):
    """An intentionally sanitized error; never contains HTTP bodies or tokens."""

    def __init__(self, message: str, *, code: str = "auth_error", status: int | None = None,
                 server_error_code: int | str | None = None, client_id: str | None = None,
                 grant_type: str | None = None, stage: str | None = None,
                 oauth_error: str | None = None, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.server_error_code = _error_number(server_error_code)
        self.client_id = client_id
        self.grant_type = grant_type
        self.stage = stage
        self.oauth_error = oauth_error if isinstance(oauth_error, str) and oauth_error in OAUTH_ERROR_NAMES else None
        delay = _error_number(retry_after_seconds)
        self.retry_after_seconds = delay if delay is not None and delay <= 86400 else None

    def safe_diagnostics(self) -> dict[str, str | int]:
        """Only local enums and bounded numbers; never serialize server bodies."""
        result: dict[str, str | int] = {"code": self.code}
        if self.stage in ("token", "user_info"):
            result["stage"] = self.stage
        if self.client_id in ("cloud-win", "cloud-android"):
            result["client_id"] = self.client_id
        elif self.client_id is not None:
            result["client_id"] = "custom"
        if self.grant_type in ("password", "refresh_token"):
            result["grant_type"] = self.grant_type
        if type(self.status) is int and 100 <= self.status <= 599:
            result["http_status"] = self.status
        if self.server_error_code is not None:
            result["server_error_code"] = self.server_error_code
            result["server_error_name"] = SERVER_ERROR_NAMES.get(self.server_error_code, "UNKNOWN")
        if self.oauth_error is not None:
            result["oauth_error"] = self.oauth_error
        if self.retry_after_seconds is not None:
            result["retry_after_seconds"] = self.retry_after_seconds
        return result


@dataclass(frozen=True)
class Tokens:
    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    username: str
    expires_at: float | None = None
    client_id: str = DEFAULT_CLIENT_ID

    def expired(self, *, margin: float = 60, now: float | None = None) -> bool:
        return self.expires_at is not None and (time.time() if now is None else now) + margin >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        """For the private token store only. Do not log this mapping."""
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "username": self.username,
            "expires_at": self.expires_at,
            "client_id": self.client_id,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "Tokens":
        if not isinstance(data, dict):
            raise AuthError("Некорректный файл авторизации.", code="invalid_token_file")
        for key in ("access_token", "refresh_token", "username", "client_id"):
            value = data.get(key)
            if not isinstance(value, str) or not value or "\x00" in value or "\r" in value or "\n" in value:
                raise AuthError("Некорректный файл авторизации.", code="invalid_token_file")
        expires_at = data.get("expires_at")
        if expires_at is not None and (
            isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at)
        ):
            raise AuthError("Некорректный срок действия токена.", code="invalid_token_file")
        return cls(data["access_token"], data["refresh_token"], data["username"], expires_at, data["client_id"])


def _dpapi(data: bytes, *, decrypt: bool) -> bytes:
    """Windows user-bound encryption; the secret is not portable to another user."""
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]

    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    buffer = ctypes.create_string_buffer(data)
    incoming = DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    outgoing = DATA_BLOB()
    flags = 0x1  # CRYPTPROTECT_UI_FORBIDDEN; user scope is the default.
    if decrypt:
        operation = crypt32.CryptUnprotectData
        operation.argtypes = [ctypes.POINTER(DATA_BLOB), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
                              ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(DATA_BLOB)]
        result = operation(ctypes.byref(incoming), None, None, None, None, flags, ctypes.byref(outgoing))
    else:
        operation = crypt32.CryptProtectData
        operation.argtypes = [ctypes.POINTER(DATA_BLOB), wintypes.LPCWSTR, ctypes.c_void_p, ctypes.c_void_p,
                              ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(DATA_BLOB)]
        result = operation(ctypes.byref(incoming), "Mail.ru photo client", None, None, None, flags, ctypes.byref(outgoing))
    operation.restype = wintypes.BOOL
    if not result:
        raise AuthError("Windows не смог защитить или прочитать локальные токены.", code="token_storage_error")
    try:
        return ctypes.string_at(outgoing.pbData, outgoing.cbData)
    finally:
        kernel32.LocalFree(ctypes.cast(outgoing.pbData, ctypes.c_void_p))


class TokenStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)

    def clear(self) -> None:
        """Forget this client's saved session after all token users have stopped."""
        try:
            self.path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            raise AuthError("Не удалось удалить сохранённый вход в Mail.ru.", code="token_storage_error") from None
        if os.name != "nt":
            try:
                fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError:
                raise AuthError("Не удалось сохранить выход из Mail.ru на диске.", code="token_storage_error") from None

    def save(self, tokens: Tokens) -> None:
        # Re-validate before writing so an invalid server response cannot be saved.
        tokens = Tokens.from_dict(tokens.to_dict())
        private_data = json.dumps(tokens.to_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if os.name == "nt":
            payload = {"version": 1, "storage": "windows-dpapi", "data": base64.b64encode(_dpapi(private_data, decrypt=False)).decode("ascii")}
        else:
            payload = {"version": 1, "storage": "posix-0600", "tokens": tokens.to_dict()}
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temp_path: str | None = None
        try:
            fd, temp_path = tempfile.mkstemp(prefix=".auth-", suffix=".tmp", dir=self.path.parent)
            with os.fdopen(fd, "wb") as stream:
                if os.name != "nt":
                    os.fchmod(stream.fileno(), 0o600)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
        except OSError:
            raise AuthError("Не удалось сохранить локальный файл авторизации.", code="token_storage_error") from None
        finally:
            if temp_path is not None:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    def load(self) -> Tokens:
        try:
            fd = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb") as stream:
                metadata = os.fstat(stream.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise AuthError("Файл авторизации должен быть обычным файлом.", code="invalid_token_file")
                if os.name != "nt" and stat.S_IMODE(metadata.st_mode) & 0o077:
                    raise AuthError("Файл токенов доступен другим пользователям. Установите права 600.", code="unsafe_token_permissions")
                raw = stream.read(MAX_RESPONSE_BYTES + 1)
        except FileNotFoundError:
            raise AuthError("Сначала выполните login.", code="not_logged_in") from None
        except OSError:
            raise AuthError("Не удалось прочитать локальный файл авторизации.", code="token_storage_error") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise AuthError("Слишком большой файл авторизации.", code="invalid_token_file")
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict) or payload.get("version") != 1:
                raise ValueError()
            storage = payload.get("storage")
            if storage == "windows-dpapi":
                if os.name != "nt":
                    raise AuthError("Этот файл токенов защищён Windows DPAPI. Выполните login на этом компьютере.", code="token_storage_error")
                secret = base64.b64decode(payload["data"], validate=True)
                return Tokens.from_dict(json.loads(_dpapi(secret, decrypt=True)))
            if storage != "posix-0600" or os.name == "nt":
                raise ValueError()
            return Tokens.from_dict(payload["tokens"])
        except (ValueError, KeyError, TypeError):
            raise AuthError("Некорректный файл авторизации.", code="invalid_token_file") from None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise AuthError("Сервер авторизации вернул перенаправление; передача пароля остановлена.", code="auth_redirect", status=code)


def _server_error(data: Any, status: int | None) -> AuthError:
    body = data if isinstance(data, dict) else {}
    numeric = _error_number(body.get("error_code"))
    remote_error = body.get("error")
    oauth_error = (remote_error if isinstance(remote_error, str) and remote_error in OAUTH_ERROR_NAMES
                   else "unknown" if remote_error else None)
    context = {"status": status, "server_error_code": numeric, "oauth_error": oauth_error}
    if status == 429:
        return AuthError("Mail.ru ограничил частоту входа. Повторите попытку позже.", code="auth_rate_limited", **context)
    if status is not None and 500 <= status <= 599:
        return AuthError("Сервер авторизации Mail.ru временно недоступен. Повторите попытку позже.", code="auth_server_unavailable", **context)
    if numeric == 16:
        return AuthError("Mail.ru ограничил повторное подтверждение второго фактора. Дождитесь окончания ограничения; обычный пароль аккаунта здесь не используйте.",
                         code="second_factor_rate_limited", retry_after_seconds=body.get("timeout"), **context)
    if numeric in (15, 26):
        return AuthError("Mail.ru запросил второй фактор или CAPTCHA. Для этого способа входа нужен пароль внешнего приложения с правами «Почта, Облако, Календарь — все протоколы». Отключать двухфакторную защиту не требуется; интерактивное подтверждение в клиенте не реализовано.",
                         code="second_factor_required", **context)
    if numeric == 21:
        return AuthError("Mail.ru сообщил, что доступ к аккаунту заблокирован.", code="account_blocked", **context)
    if numeric == 11:
        return AuthError("Mail.ru требует вход через внешний поставщик авторизации. Обычный пароль в этом режиме не принимается; используйте пароль внешнего приложения.",
                         code="external_login_required", **context)
    if numeric == 1 or oauth_error in ("invalid_client", "unauthorized_client"):
        return AuthError("Mail.ru отклонил OAuth-клиент. Проверьте версию программы; повторять ввод того же пароля не требуется.",
                         code="oauth_client_rejected", **context)
    return AuthError("Mail.ru отклонил авторизацию для выбранного OAuth-клиента. Проверьте полный адрес и пароль внешнего приложения с правами «Почта, Облако, Календарь — все протоколы». Числовой код указан в диагностике.",
                     code="auth_rejected", **context)


class AuthClient:
    def __init__(self, *, timeout: float = 60, client_id: str = DEFAULT_CLIENT_ID,
                 opener: Any | None = None, clock: Callable[[], float] = time.time):
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be positive and finite")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", client_id):
            raise ValueError("Invalid OAuth client_id")
        self.timeout = timeout
        self.client_id = client_id
        self.opener = opener if opener is not None else urllib.request.build_opener(_NoRedirect())
        self.clock = clock

    def _request(self, fields: dict[str, str]) -> dict[str, Any]:
        request = urllib.request.Request(
            TOKEN_ENDPOINT,
            data=urllib.parse.urlencode(fields).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json", "User-Agent": USER_AGENT},
            method="POST",
        )
        status: int | None = None
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                status = response.status
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            status = exc.code
            try:
                raw = exc.read(MAX_RESPONSE_BYTES + 1)
            finally:
                exc.close()
        except (urllib.error.URLError, OSError, TimeoutError):
            raise AuthError("Не удалось связаться с сервером авторизации Mail.ru.", code="auth_network_error") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise AuthError("Слишком большой ответ сервера авторизации.", code="auth_protocol_error", status=status)
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            if status == 429 or (status is not None and 500 <= status <= 599):
                raise _server_error(None, status) from None
            raise AuthError("Сервер авторизации вернул некорректный JSON.", code="auth_protocol_error", status=status) from None
        if status != 200 or not isinstance(data, dict) or data.get("error") or data.get("error_code") not in (None, 0, "0"):
            raise _server_error(data, status)
        return data

    def _tokens(self, response: dict[str, Any], username: str, old_refresh: str | None = None,
                *, client_id: str | None = None) -> Tokens:
        expires_at = None
        expires_in = response.get("expires_in")
        if expires_in is not None:
            try:
                duration = float(expires_in)
                if isinstance(expires_in, bool) or not math.isfinite(duration) or duration <= 0:
                    raise ValueError()
                expires_at = self.clock() + duration
            except (TypeError, ValueError):
                raise AuthError("Некорректный срок действия токена от Mail.ru.", code="auth_protocol_error") from None
        try:
            return Tokens.from_dict({
                "access_token": response.get("access_token"),
                "refresh_token": response.get("refresh_token", old_refresh),
                "username": username,
                "expires_at": expires_at,
                "client_id": self.client_id if client_id is None else client_id,
            })
        except AuthError:
            raise AuthError("В ответе Mail.ru нет корректной пары токенов.", code="auth_protocol_error") from None

    def login(self, username: str, password: str) -> Tokens:
        username = username.strip()
        if not username or "@" not in username or not password:
            raise AuthError("Укажите полный адрес аккаунта и непустой пароль приложения.", code="invalid_credentials")
        try:
            data = self._request({"client_id": self.client_id, "grant_type": "password", "username": username, "password": password})
            return self._tokens(data, username)
        except AuthError as error:
            error.client_id, error.grant_type, error.stage = self.client_id, "password", "token"
            raise

    def refresh(self, tokens: Tokens) -> Tokens:
        tokens = Tokens.from_dict(tokens.to_dict())
        try:
            data = self._request({"client_id": tokens.client_id, "grant_type": "refresh_token", "refresh_token": tokens.refresh_token})
            return self._tokens(data, tokens.username, tokens.refresh_token, client_id=tokens.client_id)
        except AuthError as error:
            error.client_id, error.grant_type, error.stage = tokens.client_id, "refresh_token", "token"
            raise


def login(username: str, password: str, **kwargs: Any) -> Tokens:
    return AuthClient(**kwargs).login(username, password)


def refresh(tokens: Tokens, **kwargs: Any) -> Tokens:
    return AuthClient(**kwargs).refresh(tokens)


class TokenManager:
    """Serialize refresh so parallel upload workers do not rotate tokens at once."""

    def __init__(self, store: TokenStore, client: AuthClient | None = None):
        self.store = store
        self.client = client or AuthClient()
        # Load on first authenticated request, after the runner owns its OS
        # lock. Constructing the CLI must not cache another process's old
        # rotating refresh token before that process exits.
        self._tokens: Tokens | None = None
        self._lock = threading.RLock()

    @property
    def tokens(self) -> Tokens:
        with self._lock:
            if self._tokens is None:
                self._tokens = self.store.load()
            return self._tokens

    def access_token(self, *, force_refresh: bool = False, rejected_token: str | None = None) -> str:
        with self._lock:
            if self._tokens is None:
                self._tokens = self.store.load()
            refresh_needed = self._tokens.expired() or force_refresh
            if rejected_token is not None and rejected_token != self._tokens.access_token:
                refresh_needed = self._tokens.expired()
            if refresh_needed:
                updated = self.client.refresh(self._tokens)
                self.store.save(updated)
                self._tokens = updated
            return self._tokens.access_token

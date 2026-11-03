"""Small TLS-only, streaming HTTP transport. No automatic redirects or URL logs."""

from __future__ import annotations

import http.client
import base64
import math
import re
import socket
import ssl
import threading
from dataclasses import dataclass, field
from typing import BinaryIO, Callable
from urllib.parse import unquote, urlsplit
from urllib.request import getproxies, proxy_bypass

from .auth import USER_AGENT


class TransportError(RuntimeError):
    """A local message with optional bounded, non-secret request diagnostics."""

    _CATEGORIES = frozenset({"timeout", "tls_certificate", "tls_error", "dns_error",
                             "connection_reset", "http_protocol", "encoding_error", "network_error",
                             "proxy_configuration", "invalid_url"})
    _STAGES = frozenset({"user_info", "quota", "metadata", "stat", "blob_status",
                         "blob_upload", "unlimited_add", "cloud_request"})
    _METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})

    def __init__(self, message: str, *, category: str | None = None, method: str | None = None,
                 host: str | None = None, stage: str | None = None, errno: int | None = None):
        self.category = category if isinstance(category, str) and category in self._CATEGORIES else None
        self.method = method if isinstance(method, str) and method in self._METHODS else None
        self.host = host if isinstance(host, str) and re.fullmatch(r"[a-z0-9.-]{1,253}", host) else None
        self.stage = stage if isinstance(stage, str) and stage in self._STAGES else None
        self.errno = errno if type(errno) is int and -(2**31) <= errno <= 2**31 - 1 else None
        diagnostic = self.safe_diagnostics()
        suffix = " [" + "; ".join(f"{key}={value}" for key, value in diagnostic.items()) + "]" if diagnostic else ""
        super().__init__(message + suffix)

    def safe_diagnostics(self) -> dict[str, str | int]:
        return {key: value for key, value in (("stage", self.stage), ("method", self.method),
                ("host", self.host), ("category", self.category), ("errno", self.errno)) if value is not None}


def _request_stage(method: str, host: str, path: str, *, has_stream: bool) -> str:
    if path == "/api/v1/user/info":
        return "user_info"
    if path == "/api/v4/user/quota":
        return "quota"
    if path == "/api/v1/private/stat" or path.startswith("/api/v1/private/stat/"):
        return "stat"
    if method == "POST" and path == "/api/v1/private/add":
        return "unlimited_add"
    if method == "PUT":
        return "blob_upload" if has_stream else "blob_status"
    if (path == "/m" or path.rstrip("/") == "/meta" or path.startswith("/meta/")
            or (method == "POST" and path == "/" and host.endswith(".datacloudmail.ru"))):
        return "metadata"
    return "cloud_request"


def _network_error(error: BaseException, method: str, host: str, path: str, *, has_stream: bool) -> TransportError:
    # Do not interpolate str(error): TLS, proxy and HTTP exceptions may contain
    # full URLs, response fragments or credentials. Class and errno are enough.
    if isinstance(error, (TimeoutError, socket.timeout)):
        category, message = "timeout", "Превышено время ожидания сети; очередь сохранена."
    elif isinstance(error, ssl.SSLCertVerificationError):
        category, message = "tls_certificate", "Не удалось проверить TLS-сертификат сервера; очередь сохранена."
    elif isinstance(error, ssl.SSLError):
        category, message = "tls_error", "Ошибка TLS-соединения; очередь сохранена."
    elif isinstance(error, socket.gaierror):
        category, message = "dns_error", "Не удалось определить адрес сервера (DNS); очередь сохранена."
    elif isinstance(error, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        category, message = "connection_reset", "Сетевое соединение сброшено или прервано; очередь сохранена."
    elif isinstance(error, http.client.HTTPException):
        category, message = "http_protocol", "Ошибка HTTP-соединения; очередь сохранена."
    elif isinstance(error, UnicodeError):
        category, message = "encoding_error", "Ошибка кодировки HTTP-запроса; очередь сохранена."
    else:
        category, message = "network_error", "Ошибка сетевого ввода-вывода; очередь сохранена."
    return TransportError(message, category=category, method=method, host=host,
                          stage=_request_stage(method, host, path, has_stream=has_stream),
                          errno=getattr(error, "errno", None))


class Cancelled(RuntimeError):
    pass


class TaskCancelled(Cancelled):
    """Only the current claimed task was paused or removed."""


class SourceReadError(RuntimeError):
    """The local input could not be read; unrelated tasks may continue."""


def check_cancelled(stop, *, force: bool = False) -> None:
    """Keep task cancellation distinct while accepting ordinary Events."""
    check = getattr(stop, "checkpoint", None) if force else None
    if not callable(check):
        check = getattr(stop, "check", None)
    if callable(check):
        check()
    elif stop is not None and stop.is_set():
        raise Cancelled("Загрузка остановлена.")


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str] = field(repr=False)
    body: bytes = field(repr=False)


def validate_url(url: str) -> None:
    """Keep bearer tokens on known Mail Cloud service domain families."""
    if not isinstance(url, str) or not url.isascii() or url != url.strip():
        raise TransportError("Некорректный адрес сервера.", category="invalid_url")
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except (TypeError, ValueError):
        raise TransportError("Некорректный адрес сервера.", category="invalid_url") from None
    allowed = host in {"cloud.mail.ru", "o2.mail.ru", "alt-cloud.mail.ru", "alt-o2.mail.ru"} or any(
        host.endswith(suffix) for suffix in (".cloud.mail.ru", ".datacloudmail.ru", ".cldmail.ru")
    )
    if (parsed.scheme != "https" or not allowed or parsed.username is not None
            or parsed.password is not None or port not in (None, 443) or parsed.fragment
            or any(ord(char) < 32 or ord(char) == 127 for char in url)):
        raise TransportError("Адрес сервера не относится к разрешённым HTTPS-узлам Облака.", category="invalid_url")


class HTTP:
    """One instance per worker; persistent connections, fixed-size upload buffer."""

    def __init__(self, *, timeout: float = 90, stop: threading.Event | None = None):
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be positive and finite")
        self.timeout = timeout
        self.stop = stop or threading.Event()
        self._context = ssl.create_default_context()
        self._connections: dict[str, http.client.HTTPSConnection] = {}

    def close(self) -> None:
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def _new_connection(self, host: str) -> http.client.HTTPSConnection:
        """Tunnel HTTPS through a configured HTTP proxy, keeping TLS end to end.

        Match urllib's HTTPS_PROXY/NO_PROXY discovery used by OAuth.  Proxy
        credentials belong exclusively to CONNECT, never to the origin request.
        HTTPSConnection uses the tunnel host for certificate validation and SNI.
        """
        proxy = getproxies().get("https")
        if not proxy or proxy_bypass(host):
            return http.client.HTTPSConnection(host, timeout=self.timeout, context=self._context)
        try:
            if not isinstance(proxy, str) or not proxy.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in proxy):
                raise ValueError()
            parsed = urlsplit(proxy)
            if (parsed.scheme != "http" or not parsed.hostname or parsed.path not in ("", "/")
                    or parsed.query or parsed.fragment):
                raise ValueError()
            port = parsed.port if parsed.port is not None else 80
            if not 1 <= port <= 65535:
                raise ValueError()
            tunnel_headers = {}
            if parsed.username is not None:
                username = unquote(parsed.username, errors="strict")
                password = unquote(parsed.password or "", errors="strict")
                if ":" in username or any(c in username + password for c in ("\r", "\n", "\x00")):
                    raise ValueError()
                credentials = base64.b64encode((username + ":" + password).encode("utf-8")).decode("ascii")
                tunnel_headers["Proxy-Authorization"] = "Basic " + credentials
            conn = http.client.HTTPSConnection(parsed.hostname, port, timeout=self.timeout, context=self._context)
            conn.set_tunnel(host, 443, headers=tunnel_headers)
            return conn
        except (ValueError, UnicodeError, http.client.HTTPException, OSError):
            raise TransportError("Некорректная настройка HTTPS-прокси. Поддерживается HTTP-прокси "
                                 "в формате http://host:port с туннелем CONNECT.", category="proxy_configuration") from None

    def request(self, method: str, url: str, *, headers: dict[str, str] | None = None,
                body: bytes = b"", stream: BinaryIO | None = None, length: int | None = None,
                progress: Callable[[int], None] | None = None) -> Response:
        validate_url(url)
        parsed = urlsplit(url)
        host = parsed.hostname
        assert host is not None
        check_cancelled(self.stop)
        if stream is not None and (length is None or length < 0):
            raise ValueError("A stream requires an explicit non-negative length")
        if stream is not None and body:
            raise ValueError("Use either body or stream")
        content_length = len(body) if stream is None else length
        fields = {"User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Encoding": "identity"}
        fields.update(headers or {})
        if any(name.lower() == "proxy-authorization" for name in fields):
            raise TransportError("Учётные данные прокси должны передаваться только в настройке прокси, не серверу Облака.",
                                 category="proxy_configuration")
        if method != "GET" or content_length:
            fields["Content-Length"] = str(content_length)
        target = parsed.path or "/"
        if parsed.query:
            target += "?" + parsed.query
        conn = None
        try:
            if host not in self._connections:
                if len(self._connections) >= 8:
                    self.close()
                self._connections[host] = self._new_connection(host)
            conn = self._connections[host]
            conn.putrequest(method, target, skip_accept_encoding=True)
            for key, value in fields.items():
                conn.putheader(key, value)
            conn.endheaders()
            if stream is None:
                if body:
                    conn.send(body)
            else:
                remaining = int(length)
                while remaining:
                    check_cancelled(self.stop)
                    try:
                        block = stream.read(min(1024 * 1024, remaining))
                    except OSError:
                        raise SourceReadError("Не удалось прочитать исходный файл во время передачи.") from None
                    if not block:
                        raise SourceReadError("Исходный файл закончился во время передачи.")
                    conn.send(block)
                    remaining -= len(block)
                    if progress is not None:
                        progress(len(block))
            incoming = conn.getresponse()
            payload = incoming.read(2 * 1024 * 1024 + 1)
            if len(payload) > 2 * 1024 * 1024:
                raise TransportError("Слишком большой служебный ответ сервера.")
            result = Response(incoming.status, {k.lower(): v for k, v in incoming.getheaders()}, payload)
            # Redirects are returned to the protocol layer, never followed with credentials.
            if incoming.will_close:
                conn.close()
                self._connections.pop(host, None)
            return result
        except (OSError, http.client.HTTPException, UnicodeError) as error:
            if conn is not None:
                try:
                    conn.close()
                except (OSError, http.client.HTTPException):
                    pass
            self._connections.pop(host, None)
            raise _network_error(error, method, host, parsed.path or "/", has_stream=stream is not None) from None
        except BaseException:
            if conn is not None:
                try:
                    conn.close()
                except (OSError, http.client.HTTPException):
                    pass
            self._connections.pop(host, None)
            raise

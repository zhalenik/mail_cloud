"""Private, authenticated browser interface over the existing upload engine.

Only the optional web entry point depends on Waitress. The CLI and the verified
Mail.ru wire protocol remain independent of this HTTP application.
"""
from __future__ import annotations

import argparse
import base64
from collections import OrderedDict
import getpass
import hashlib
import hmac
from http import HTTPStatus
from http.cookies import SimpleCookie
import json
import logging
import math
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit

from . import __version__
from .auth import AuthError
from .content import ContentError, FileResponse, open_task_content
from .export import QueueExportResponse, options as export_options
from .queue import QueueBusyError, QueueConfigurationError, QueueError, _file_lock

MAX_BODY = 128 * 1024
COOKIE = "mailru_panel"
PASSWORD_ROUNDS = 600000
STATIC = Path(__file__).with_name("web_static")


class WebError(Exception):
    def __init__(self, status: int, message: str, code: str = "request_error"):
        self.status, self.message, self.code = status, message, code
        super().__init__(message)


def _private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".panel-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            if os.name != "nt":
                os.fchmod(stream.fileno(), 0o600)
            json.dump(value, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode(raw: str) -> bytes:
    return base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True)


class PanelAccess:
    """Persist a password verifier and cookie key, never a cleartext password."""
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.failures: OrderedDict[str, list[float]] = OrderedDict()
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            self.data = {"version": 1, "secret": secrets.token_hex(32),
                         "epoch": secrets.token_hex(12), "password": None}
            _private_json(path, self.data)
        else:
            with os.fdopen(fd, "r", encoding="utf-8") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or (os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o077):
                    raise ValueError("Файл доступа к панели должен быть обычным файлом с правами 600.")
                self.data = json.loads(stream.read(MAX_BODY))
            if (not isinstance(self.data, dict) or self.data.get("version") != 1
                    or not isinstance(self.data.get("secret"), str) or len(self.data["secret"]) != 64
                    or not isinstance(self.data.get("epoch"), str)):
                raise ValueError("Повреждены настройки доступа к панели.")
        self.key = bytes.fromhex(self.data["secret"])

    @property
    def configured(self) -> bool:
        return isinstance(self.data.get("password"), dict)

    def read_session(self, cookie_header: str) -> dict | None:
        if len(cookie_header) > 8192:
            return None
        try:
            cookies = SimpleCookie(cookie_header)
            token = cookies[COOKIE].value
            if len(token) > 4096:
                return None
            encoded, signature = token.split(".")
            expected = hmac.new(self.key, encoded.encode("ascii"), hashlib.sha256).digest()
            if not hmac.compare_digest(_decode(signature), expected):
                return None
            value = json.loads(_decode(encoded))
            if (not isinstance(value, dict) or value.get("epoch") != self.data["epoch"]
                    or not isinstance(value.get("csrf"), str) or len(value["csrf"]) != 43
                    or type(value.get("authenticated")) is not bool
                    or not isinstance(value.get("expires"), (int, float))
                    or not math.isfinite(value["expires"]) or value["expires"] <= time.time()):
                return None
            return value
        except (KeyError, ValueError, TypeError, UnicodeError):
            return None

    def new_session(self, authenticated: bool, *, secure: bool) -> tuple[dict, str]:
        lifetime = 30 * 86400 if authenticated else 3600
        value = {"epoch": self.data["epoch"], "csrf": secrets.token_urlsafe(32),
                 "authenticated": authenticated, "expires": int(time.time()) + lifetime}
        encoded = _encode(json.dumps(value, separators=(",", ":")).encode("utf-8"))
        signature = _encode(hmac.new(self.key, encoded.encode("ascii"), hashlib.sha256).digest())
        cookie = f"{COOKIE}={encoded}.{signature}; Path=/; HttpOnly; SameSite=Strict; Max-Age={lifetime}"
        if secure:
            cookie += "; Secure"
        return value, cookie

    def setup(self, password: str) -> None:
        self._validate_password(password)
        with self.lock:
            if self.configured:
                raise WebError(409, "Пароль панели уже настроен. Выполните вход.", "already_configured")
            self._store_password(password)

    @staticmethod
    def _validate_password(password: str) -> None:
        if not isinstance(password, str) or not 12 <= len(password) <= 1024 or "\x00" in password:
            raise WebError(400, "Для панели задайте пароль длиной от 12 до 1024 символов.")

    def _store_password(self, password: str) -> None:
        salt = secrets.token_bytes(24)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ROUNDS)
        updated = {**self.data, "epoch": secrets.token_hex(12), "password": {
            "salt": salt.hex(), "digest": digest.hex(), "rounds": PASSWORD_ROUNDS}}
        # A failed write must leave both the active verifier/session epoch and
        # the previous on-disk file usable. Publish memory state after replace.
        _private_json(self.path, updated)
        self.data = updated
        self.failures.clear()

    def change_password(self, current_password: str, new_password: str, peer: str) -> None:
        self._validate_password(new_password)
        with self.lock:
            try:
                self.login(current_password, peer)
            except WebError as error:
                if error.status == 401:
                    # The panel session is still valid; a mistyped current
                    # password must not log this browser out of the panel.
                    raise WebError(400, "Текущий пароль панели указан неверно.", "invalid_current_password") from None
                raise
            self._store_password(new_password)

    def recover_password(self, new_password: str) -> None:
        """Offline owner recovery only; never exposed as an HTTP operation."""
        self._validate_password(new_password)
        with self.lock:
            self._store_password(new_password)

    def login(self, password: str, peer: str) -> None:
        if not isinstance(password, str) or len(password) > 1024:
            raise WebError(400, "Некорректный пароль панели.")
        now = time.monotonic()
        with self.lock:
            recent = [when for when in self.failures.get(peer, []) if now - when < 60]
            if len(recent) >= 8:
                raise WebError(429, "Слишком много попыток входа. Подождите минуту.", "rate_limited")
            if not self.configured:
                raise WebError(409, "Сначала задайте пароль панели.", "setup_required")
            saved = self.data["password"]
            digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                         bytes.fromhex(saved["salt"]), saved["rounds"])
            if not hmac.compare_digest(digest.hex(), saved["digest"]):
                self.failures[peer] = recent + [now]
                self.failures.move_to_end(peer)
                while len(self.failures) > 256:
                    self.failures.popitem(last=False)
                raise WebError(401, "Неверный пароль панели.", "invalid_password")
            self.failures.pop(peer, None)


def _text(data: dict, key: str, *, limit: int = 4096) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value or len(value) > limit or "\x00" in value:
        raise WebError(400, f"Заполните поле {key} корректным текстом.")
    return value


def _integer(value, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise WebError(400, "Ожидалось целое число.")
    try:
        result = int(value)
        if str(result) != str(value):
            raise ValueError()
    except (ValueError, TypeError):
        raise WebError(400, "Ожидалось целое число.") from None
    if not minimum <= result <= maximum:
        raise WebError(400, "Число вне допустимого диапазона.")
    return result


class WebApplication:
    def __init__(self, service, state_dir: Path, *, allowed_hosts=None, public_origin: str | None = None):
        self.service = service
        self.access = PanelAccess(Path(state_dir) / "web_access.json")
        self.allowed_hosts = {item.strip().casefold().strip("[]") for item in
                              (allowed_hosts or ("localhost", "127.0.0.1", "::1")) if item.strip()}
        if "*" in self.allowed_hosts:
            raise ValueError("Укажите конкретные имена/IP панели вместо '*'.")
        self.public_origin = None
        if public_origin:
            parsed = urlsplit(public_origin)
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
                    or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
                raise ValueError("MAILRU_PUBLIC_ORIGIN должен иметь вид https://photos.example.com.")
            self.public_origin = f"{parsed.scheme}://{parsed.netloc}"
            self.allowed_hosts.add(parsed.hostname.casefold())

    def _origin(self, environ: dict) -> str:
        host = environ.get("HTTP_HOST", "")
        try:
            parsed = urlsplit("http://" + host)
            if (not host or any(c in host for c in "/\\\r\n\t@ ") or not parsed.hostname
                    or parsed.hostname.casefold() not in self.allowed_hosts):
                raise ValueError()
            _ = parsed.port
        except ValueError:
            raise WebError(403, "Это имя сервера не разрешено настройками панели.", "invalid_host") from None
        return self.public_origin or f"{environ.get('wsgi.url_scheme', 'http')}://{host}"

    @staticmethod
    def _body(environ: dict) -> dict:
        if environ.get("CONTENT_TYPE", "").split(";", 1)[0].strip().lower() != "application/json":
            raise WebError(415, "Ожидается application/json.")
        try:
            length = int(environ.get("CONTENT_LENGTH") or "0")
        except ValueError:
            raise WebError(400, "Некорректная длина запроса.") from None
        if not 0 <= length <= MAX_BODY:
            raise WebError(413, "Слишком большой запрос.")
        raw = environ["wsgi.input"].read(length)
        if len(raw) != length:
            raise WebError(400, "Запрос получен не полностью.")
        def reject_constant(_):
            raise ValueError()
        try:
            data = json.loads(raw, parse_constant=reject_constant)
        except (ValueError, UnicodeError):
            raise WebError(400, "Некорректный JSON.") from None
        if not isinstance(data, dict):
            raise WebError(400, "Ожидается объект JSON.")
        return data

    def __call__(self, environ, start_response):
        cookie = None
        extra_headers = []
        try:
            origin = self._origin(environ)
            path = environ.get("PATH_INFO", "/")
            method = environ.get("REQUEST_METHOD", "GET")
            if method not in {"GET", "HEAD", "POST"}:
                raise WebError(405, "Метод не поддерживается.")
            session = self.access.read_session(environ.get("HTTP_COOKIE", ""))
            if method == "POST":
                if environ.get("HTTP_ORIGIN") != origin:
                    raise WebError(403, "Запрос должен исходить из этой панели.", "invalid_origin")
                supplied = environ.get("HTTP_X_CSRF_TOKEN", "")
                if not session or not hmac.compare_digest(supplied, session["csrf"]):
                    raise WebError(403, "Обновите страницу и повторите действие.", "invalid_csrf")
                data = self._body(environ)
            else:
                data = {}
            if path == "/healthz" and method in {"GET", "HEAD"}:
                value, content_type = {"ok": True, "version": __version__}, "application/json"
            elif path in {"/", "/static/app.js", "/static/style.css"} and method in {"GET", "HEAD"}:
                filename = {"/": "index.html", "/static/app.js": "app.js", "/static/style.css": "style.css"}[path]
                value = (STATIC / filename).read_bytes()
                content_type = {"index.html": "text/html", "app.js": "application/javascript", "style.css": "text/css"}[filename]
            elif path == "/api/session" and method == "GET":
                if not session:
                    session, cookie = self.access.new_session(False, secure=origin.startswith("https:"))
                value = {"authenticated": session["authenticated"] and self.access.configured,
                         "setup_required": not self.access.configured, "csrf": session["csrf"]}
                content_type = "application/json"
            elif path in {"/api/session/setup", "/api/session/login", "/api/session/logout", "/api/session/password"} and method == "POST":
                if path.endswith("/setup"):
                    self.access.setup(_text(data, "password", limit=1024))
                    authenticated = True
                elif path.endswith("/login"):
                    self.access.login(_text(data, "password", limit=1024), environ.get("REMOTE_ADDR", "local"))
                    authenticated = True
                elif path.endswith("/password"):
                    if not session or session.get("authenticated") is not True or not self.access.configured:
                        raise WebError(401, "Войдите в панель управления.", "authentication_required")
                    self.access.change_password(_text(data, "current_password", limit=1024),
                                                _text(data, "new_password", limit=1024),
                                                environ.get("REMOTE_ADDR", "local"))
                    authenticated = True
                else:
                    authenticated = False
                session, cookie = self.access.new_session(authenticated, secure=origin.startswith("https:"))
                value = {"authenticated": authenticated, "setup_required": not self.access.configured,
                         "csrf": session["csrf"]}
                content_type = "application/json"
            elif path.startswith("/api/"):
                if not session or session.get("authenticated") is not True or not self.access.configured:
                    raise WebError(401, "Войдите в панель управления.", "authentication_required")
                environ["mailru.session_owner"] = session["csrf"]
                value = self._api(path, method, data, environ)
                content_type = "application/json"
            else:
                raise WebError(404, "Страница не найдена.", "not_found")
            status = 200
        except WebError as error:
            status, value, content_type = error.status, {"error": {"message": error.message, "code": error.code}}, "application/json"
        except ContentError as error:
            status, value, content_type = error.status, {"error": {"message": error.message, "code": error.code}}, "application/json"
            extra_headers.extend(error.headers)
        except QueueBusyError as error:
            status, value, content_type = 409, {"error": {"message": str(error), "code": "busy"}}, "application/json"
        except (QueueError, AuthError, ValueError) as error:
            missing_source = getattr(error, "code", None) == "source_not_found"
            status, value, content_type = (404 if missing_source else 400), {"error": {"message": str(error), "code": "source_not_found" if missing_source else "operation_error"}}, "application/json"
        except Exception:
            # Do not expose arbitrary exception strings, credentials or traces.
            status, value, content_type = 500, {"error": {"message": "Не удалось выполнить запрос. Очередь сохранена.", "code": "internal_error"}}, "application/json"
        if isinstance(value, (FileResponse, QueueExportResponse)):
            try:
                start_response(f"{value.status} {HTTPStatus(value.status).phrase}", value.headers)
            except BaseException:
                value.close()
                raise
            return value
        if content_type == "application/json":
            value = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        headers = [("Content-Type", content_type + "; charset=utf-8"), ("Content-Length", str(len(value))),
                   ("Cache-Control", "no-store"), ("X-Content-Type-Options", "nosniff"),
                   ("Referrer-Policy", "no-referrer"), ("X-Frame-Options", "DENY"),
                   ("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")]
        headers.extend(extra_headers)
        if cookie:
            headers.append(("Set-Cookie", cookie))
        start_response(f"{status} {HTTPStatus(status).phrase}", headers)
        return [b"" if environ.get("REQUEST_METHOD") == "HEAD" else value]

    def _api(self, path: str, method: str, data: dict, environ: dict):
        query = parse_qs(environ.get("QUERY_STRING", ""), max_num_fields=16)
        if path == "/api/jobs/export" and method in {"GET", "HEAD"}:
            parameters = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True, max_num_fields=16)
            return self.service.export_jobs(export_options(parameters), head=method == "HEAD")
        content = re.fullmatch(r"/api/tasks/([0-9]+)/content", path)
        if content is not None and method in {"GET", "HEAD"}:
            options = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True, max_num_fields=4)
            choice = options.get("download", ["0"])
            if set(options) - {"download"} or len(choice) != 1 or choice[0] not in {"0", "1"}:
                raise WebError(400, "Параметр download должен быть 0 или 1.")
            try:
                source = self.service.task_source(_integer(content[1], 1, 2**63 - 1))
            except (QueueConfigurationError, FileNotFoundError):
                raise WebError(404, "Задача не найдена. Обновите список.", "task_missing") from None
            return open_task_content(*source, download=choice[0] == "1", head=method == "HEAD",
                                     range_header=None if environ.get("HTTP_IF_RANGE") else environ.get("HTTP_RANGE"))
        if method == "GET":
            if path == "/api/sources":
                return self.service.sources(limit=_integer(query.get("limit", ["50"])[0], 1, 200),
                    offset=_integer(query.get("offset", ["0"])[0], 0, 100000000),
                    kind=query.get("kind", ["all"])[0], q=query.get("q", [""])[0])
            if path == "/api/sources/members":
                return self.service.source_members(_integer(query.get("id", [None])[0], 1, 2**63 - 1),
                    limit=_integer(query.get("limit", ["200"])[0], 1, 200),
                    offset=_integer(query.get("offset", ["0"])[0], 0, 100000000))
            if path == "/api/schedules":
                return self.service.schedules()
            if path == "/api/state":
                return self.service.snapshot(**({"source_id": _integer(query["source_id"][0], 1, 2**63 - 1)} if "source_id" in query else {}))
            if path == "/api/jobs":
                status = query.get("status", ["all"])[0]
                if status not in {"all", "pending", "running", "uploaded", "failed", "skipped", "paused", "existing",
                                   "uploaded_unlimited", "uploaded_regular", "uploaded_unknown"}:
                    raise WebError(400, "Неизвестный фильтр очереди.")
                return self.service.jobs(status=None if status == "all" else status,
                                         limit=_integer(query.get("limit", ["50"])[0], 1, 200),
                                         offset=_integer(query.get("offset", ["0"])[0], 0, 100000000),
                                         sort=query.get("sort", ["asc"])[0],
                                         sort_by=query.get("sort_by", ["created_at"])[0],
                                         **({"source_id": _integer(query["source_id"][0], 1, 2**63 - 1)} if "source_id" in query else {}))
            if path == "/api/folders":
                return self.service.folders(query.get("path", [None])[0])
            if path == "/api/report":
                return {"client_version": __version__, "state": self.service.snapshot(),
                        "errors": self.service.jobs(status="failed", limit=100, offset=0)}
        if path.startswith("/api/plans") or path.startswith("/api/notifications"):
            return self.service.planning_request(method, path, query, data,
                                                 owner=environ.get("mailru.session_owner", ""))
        if method != "POST":
            raise WebError(404, "Неизвестная команда.")
        if path == "/api/schedules/save":
            return self.service.save_schedule(data)
        elif path == "/api/schedules/toggle":
            return self.service.toggle_schedule(_integer(data.get("id"), 1, 2**63 - 1), data.get("enabled"))
        elif path == "/api/schedules/remove":
            return self.service.remove_schedule(_integer(data.get("id"), 1, 2**63 - 1))
        elif path == "/api/setup":
            self.service.setup(_text(data, "destination"))
        elif path == "/api/login":
            self.service.login(_text(data, "email", limit=320), _text(data, "password", limit=1024))
        elif path == "/api/logout":
            self.service.logout()
        elif path == "/api/sources/add":
            self.service.add_folder(_text(data, "path") if "paths" not in data else None, _text(data, "alias", limit=255),
                                    remote_prefix=_text(data, "remote_prefix", limit=2048),
                                    **({"paths": data["paths"]} if "paths" in data else {}))
        elif path in {"/api/sources", "/api/add-folder"}:
            # Older panels and scripted clients derive their default from the
            # existing queue configuration; new forms always send a full path.
            self.service.add_folder(_text(data, "path"), _text(data, "alias", limit=255),
                                    remote_prefix=_text(data, "remote_prefix", limit=2048)
                                    if "remote_prefix" in data else None)
        elif path == "/api/sources/update":
            self.service.update_source(_integer(data.get("id"), 1, 2**63 - 1),
                                       remote_prefix=_text(data, "remote_prefix", limit=2048),
                                       alias=_text(data, "alias", limit=255) if "alias" in data else None,
                                       **({"paths": data["paths"]} if "paths" in data else {}))
        elif path == "/api/sources/remove":
            self.service.remove_source(_integer(data.get("id"), 1, 2**63 - 1))
        elif path == "/api/tasks/delete/preview":
            return self.service.preview_deletion(
                task_id=_integer(data["id"], 1, 2**63 - 1) if "id" in data else None,
                scope=_text(data, "scope", limit=16) if "scope" in data else "all",
                status=_text(data, "status", limit=32) if "status" in data else None,
                delete_cloud=data.get("delete_cloud", False), delete_local=data.get("delete_local", False),
                owner=environ.get("mailru.session_owner", ""),
                **({"source_id": _integer(data["source_id"], 1, 2**63 - 1)} if "source_id" in data else {}))
        elif path == "/api/tasks/delete":
            self.service.confirm_deletion(_text(data, "token", limit=200),
                local_confirmation=_text(data, "local_confirmation", limit=100) if "local_confirmation" in data else "",
                owner=environ.get("mailru.session_owner", ""))
        elif path in {"/api/tasks/pause", "/api/tasks/resume", "/api/tasks/start", "/api/tasks/remove", "/api/tasks/skip", "/api/tasks/return"}:
            self.service.control_task(_integer(data.get("id"), 1, 2**63 - 1), path.rsplit("/", 1)[1])
        elif path == "/api/tasks/clear":
            scope = _text(data, "scope", limit=16)
            if scope not in {"all", "finished"}:
                raise WebError(400, "Выберите все задачи или обработанные задачи.")
            self.service.clear_tasks(scope, **({"source_id": _integer(data["source_id"], 1, 2**63 - 1)} if "source_id" in data else {}))
        elif path == "/api/scan":
            self.service.scan(**({"source_id": _integer(data["source_id"], 1, 2**63 - 1)} if "source_id" in data else {}))
        elif path == "/api/start":
            self.service.start(**({"source_id": _integer(data["source_id"], 1, 2**63 - 1)} if "source_id" in data else {}))
        elif path == "/api/pause":
            self.service.pause()
        elif path == "/api/retry":
            include = data.get("include_permanent", False)
            if type(include) is not bool:
                raise WebError(400, "Некорректный параметр повторной попытки.")
            self.service.retry_failed(include_permanent=include,
                **({"source_id": _integer(data["source_id"], 1, 2**63 - 1)} if "source_id" in data else {}))
        elif path == "/api/quota/refresh":
            self.service.refresh_quota()
        elif path == "/api/settings":
            workers = _integer(data.get("workers"), 1, 16)
            if type(data.get("auto_resume")) is not bool:
                raise WebError(400, "Укажите настройку продолжения после перезапуска.")
            if "preflight_check" in data:
                raise WebError(400, "Предварительная проверка удалена. Обновите страницу панели.")
            if "remote_reconcile" in data and type(data["remote_reconcile"]) is not bool:
                raise WebError(400, "Сверка с Облаком должна быть включена или выключена.")
            if "upload_large_files" in data:
                raise WebError(400, "Настройка обычной загрузки больших файлов удалена. Обновите страницу панели.")
            self.service.settings(workers=workers, auto_resume=data["auto_resume"],
                                  remote_reconcile=data.get("remote_reconcile"),
                                  regular_policy=data.get("regular_policy"),
                                  unexpected_unlimited_action=data.get("unexpected_unlimited_action"),
                                  timezone=data.get("timezone"))
        else:
            raise WebError(404, "Неизвестная команда.")
        return self.service.snapshot()


def reset_panel_password(state_dir: Path, *, read_password=None) -> None:
    """Recover panel access while its controller is stopped.

    No account token, queue, settings or source file is changed. The controller
    lock is acquired before prompting so a running server cannot overwrite or
    continue using a different in-memory verifier during recovery.
    """
    state_dir = Path(state_dir).expanduser().resolve()
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    read_password = read_password or getpass.getpass
    try:
        with _file_lock(state_dir / "web.run.lock"):
            password = confirmation = ""
            try:
                password = read_password("Новый пароль панели (не менее 12 символов): ")
                confirmation = read_password("Повторите новый пароль панели: ")
                PanelAccess._validate_password(password)
                if password != confirmation:
                    raise WebError(400, "Пароли не совпали. Прежний пароль панели сохранён.")
                PanelAccess(state_dir / "web_access.json").recover_password(password)
            finally:
                password = confirmation = ""
    except QueueBusyError:
        raise QueueBusyError("Панель сейчас работает. Остановите приложение или контейнер перед восстановлением пароля.") from None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Браузерная панель загрузки фото и видео в Облако Mail")
    parser.add_argument("--state", type=Path, default=Path(os.environ.get("MAILRU_STATE", "state")))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--allow-root", action="append", type=Path, help="корень фотографий на сервере; можно указать несколько")
    parser.add_argument("--allowed-host", action="append", help="дополнительное имя/IP панели")
    parser.add_argument("--public-origin", default=os.environ.get("MAILRU_PUBLIC_ORIGIN") or None)
    parser.add_argument("--reset-panel-password", action="store_true",
                        help="задать новый пароль панели через скрытый ввод; приложение должно быть остановлено")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    state = args.state.expanduser().resolve()
    if args.reset_panel_password:
        try:
            reset_panel_password(state)
        except (KeyboardInterrupt, EOFError):
            print("Восстановление пароля отменено.", flush=True)
            return 130
        except (ValueError, QueueError, WebError) as error:
            print(f"Не удалось изменить пароль панели: {error}", flush=True)
            return 2
        except OSError:
            print("Не удалось сохранить пароль панели. Проверьте доступ к каталогу состояния.", flush=True)
            return 2
        print("Новый пароль панели сохранён. Прежние сеансы завершены; аккаунт Mail и очередь сохранены.", flush=True)
        return 0
    try:
        from waitress import create_server
    except ImportError:
        parser.error("Для GUI установите requirements-web.txt или запустите готовую Docker-конфигурацию.")
    from .web_service import WebService
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    hosts = ["localhost", "127.0.0.1", "::1"] + (args.allowed_host or [])
    hosts.extend(item.strip() for item in os.environ.get("MAILRU_ALLOWED_HOSTS", "").split(",") if item.strip())
    roots = args.allow_root or [Path("/photos")]
    service = server = None
    def stop_signal(*_):
        raise KeyboardInterrupt()
    try:
        with _file_lock(state / "web.lock"):
            service = WebService(state, roots)
            app = WebApplication(service, state, allowed_hosts=hosts, public_origin=args.public_origin)
            server = create_server(app, host=args.host, port=args.port, threads=6,
                                   ident="MailruPhotoClient", max_request_body_size=MAX_BODY,
                                   max_request_header_size=16384, channel_timeout=30,
                                   expose_tracebacks=False, log_socket_errors=False)
            signal.signal(signal.SIGTERM, stop_signal)
            print(f"Mailru Photo Client {__version__}: http://{args.host}:{args.port}", flush=True)
            service.resume_if_requested()
            try:
                server.run()
            except KeyboardInterrupt:
                pass
            finally:
                service.close(timeout=105)
                server.close()
    except (OSError, ValueError, QueueError) as error:
        print(f"Не удалось запустить панель: {error}", flush=True)
        if service is not None:
            service.close()
        if server is not None:
            server.close()
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Local web controller around the verified CLI engine; no new cloud protocol.

Each task owns its SQLite connection. Browser requests see bounded summaries and
never receive authentication tokens. A durable user intention controls restart;
manual pause clears that intention, process shutdown deliberately preserves it.
"""
from __future__ import annotations

import copy
import dataclasses
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable

from . import __version__
from .api import CloudAPI, PROTOCOL_VERSION, ProtocolError
from .auth import AuthClient, AuthError, TokenManager, TokenStore
from .cli import write_json
from .queue import PHOTO_EXTENSIONS, FileChangedError, Queue, QueueBusyError, QueueError, STATES, _file_lock
from .runner import ProbeRequired, probe, process_job, run
from .transport import Cancelled, TransportError

LOGGER = logging.getLogger("mailru_photos.service")
LOGGER.addHandler(logging.NullHandler())


class WebService:
    def __init__(self, state_dir: Path, allowed_roots: list[Path], *,
                 api_factory: Callable[..., CloudAPI] = CloudAPI,
                 auth_client_factory: Callable[..., AuthClient] = AuthClient,
                 event_factory: Callable[[], threading.Event] = threading.Event,
                 probe_settle_seconds: float = 60):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.allowed_roots = tuple(dict.fromkeys(Path(p).expanduser().resolve() for p in allowed_roots))
        if not self.allowed_roots:
            raise ValueError("Укажите хотя бы одну доступную папку с фотографиями.")
        self._api_factory = api_factory
        self._auth_client_factory = auth_client_factory
        self._event_factory = event_factory
        self._settle_seconds = probe_settle_seconds
        self._lock = threading.RLock()
        self._stats_lock = threading.Lock()
        self._stats_cache: tuple[float, dict] | None = None
        self._thread: threading.Thread | None = None
        self._operation: str | None = None
        self._stop = event_factory()
        self._pause_requested = False
        self._shutting_down = False
        self._closed = False
        self._files: dict[int, dict] = {}
        self._uploaded_in_run = 0
        self._settings = {"workers": 2, "auto_resume": True, "preflight_check": True}
        stored = self._read_json("web_settings.json", {})
        if isinstance(stored, dict):
            if type(stored.get("workers")) is int and 1 <= stored["workers"] <= 16:
                self._settings["workers"] = stored["workers"]
            if type(stored.get("auto_resume")) is bool:
                self._settings["auto_resume"] = stored["auto_resume"]
            if type(stored.get("preflight_check")) is bool:
                self._settings["preflight_check"] = stored["preflight_check"]
        previous = self._read_json("web_activity.json", {})
        self._resume_upload = isinstance(previous, dict) and previous.get("resume_upload") is True
        self._auto_pending = self._resume_upload and self._settings["auto_resume"]
        self._activity = {"phase": "paused" if self._resume_upload else "idle",
                          "message": "Очередь восстановлена после остановки." if self._resume_upload else "Готово к работе.",
                          "last_error": None, "started_at": None, "scan": None,
                          "current_files": [], "probe_wait_until": None}
        old_events = self._read_json("web_events.json", [])
        self._events = [e for e in old_events[-100:] if isinstance(e, dict) and
                        isinstance(e.get("message"), str)][:100] if isinstance(old_events, list) else []
        self._event_id = max((e.get("id", 0) for e in self._events if type(e.get("id")) is int), default=0)
        self._server_lock = _file_lock(str(self.state_dir / "web.run.lock"))
        self._server_lock.__enter__()
        self._server_lock_released = False
        try:
            self._configuration = self._read_configuration()
            if self._configuration is not None:
                with self._queue() as queue, queue.exclusive_run():
                    recovered = queue.recover_running()
                    if recovered:
                        self._activity["message"] = f"Восстановлено незавершённых заданий: {recovered}."
            self._authenticated = self._has_auth()
        except BaseException:
            self._server_lock.__exit__(None, None, None)
            raise

    def _read_json(self, name: str, default):
        path = self.state_dir / name
        try:
            if path.is_symlink() or path.stat().st_size > 1024 * 1024:
                raise QueueError("Недопустимый служебный файл состояния.")
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return default
        except (OSError, ValueError):
            raise QueueError(f"Не удалось прочитать {name}; исходное состояние сохранено.") from None

    def _read_configuration(self):
        value = self._read_json("project.json", None)
        if value is None:
            return None
        if not isinstance(value, dict) or value.get("version") != 1 or not isinstance(value.get("destination"), str):
            raise QueueError("Некорректный project.json; исходная очередь не изменена.")
        return value

    def _queue(self) -> Queue:
        if self._configuration is None:
            raise ValueError("Сначала выберите папку назначения в Облаке.")
        return Queue(self.state_dir / "queue.sqlite3", destination=self._configuration["destination"])

    def _has_auth(self) -> bool:
        try:
            TokenStore(self.state_dir / "auth.dat").load()
            return True
        except AuthError:
            return False

    def _display_stats(self, queue: Queue) -> dict:
        """Share a bounded-lived display summary between dashboard requests.

        Upload decisions always use Queue.stats() directly. Serializing cache
        refreshes also avoids one full scan per browser tab at the same instant.
        """
        with self._stats_lock:
            if self._stats_cache is None or time.monotonic() - self._stats_cache[0] >= 5:
                stats = queue.stats()
                self._stats_cache = (time.monotonic(), stats)
            return copy.deepcopy(self._stats_cache[1])

    def _invalidate_stats(self) -> None:
        with self._stats_lock:
            self._stats_cache = None

    def _persist_activity(self) -> None:
        write_json(self.state_dir / "web_activity.json", {
            "version": 1, "resume_upload": self._resume_upload,
            "phase": self._activity["phase"], "updated_at": time.time(),
        })

    def _event(self, message: str, level: str = "info") -> None:
        with self._lock:
            self._event_id += 1
            self._events.append({"id": self._event_id, "at": time.time(), "level": level, "message": message[:2000]})
            del self._events[:-100]
            write_json(self.state_dir / "web_events.json", self._events)

    def _phase(self, phase: str, message: str, *, error: str | None = None) -> None:
        self._invalidate_stats()
        with self._lock:
            # A worker must not overwrite a visible pause request while it drains.
            if self._pause_requested and phase in {"checking", "probing", "uploading", "scanning"}:
                return
            self._activity.update(phase=phase, message=message, last_error=error)
            if phase != "probing":
                self._activity["probe_wait_until"] = None
            self._persist_activity()
        self._event(message, "error" if error else "info")
        # Container logs carry fixed lifecycle fields only. User paths, email,
        # request bodies, token values and free-form error messages stay out.
        LOGGER.log(logging.ERROR if error else logging.INFO,
                   "phase=%s error=%s", phase, bool(error))

    def _ensure_idle(self) -> None:
        if self._closed:
            raise QueueError("Приложение останавливается.")
        if self._operation is not None:
            raise QueueBusyError("Дождитесь текущей операции или нажмите «Пауза».")

    def _allowed_path(self, value: str | Path, *, exists: bool = True) -> Path:
        if not isinstance(value, (str, Path)) or not str(value) or "\x00" in str(value):
            raise ValueError("Укажите папку с фотографиями.")
        path = Path(value).expanduser().resolve()
        if not any(path == root or path.is_relative_to(root) for root in self.allowed_roots):
            raise ValueError("Эта папка находится за пределами подключённых каталогов с фотографиями.")
        if exists and not path.is_dir():
            raise ValueError("Папка недоступна. Проверьте подключение диска и путь на сервере.")
        return path

    def setup(self, destination: str) -> dict:
        if not isinstance(destination, str) or len(destination) > 2048:
            raise ValueError("Укажите корректную папку назначения в Облаке.")
        with self._lock:
            self._ensure_idle()
            if self._configuration and destination != self._configuration["destination"]:
                raise ValueError("Эта очередь уже связана с другой папкой Облака. Существующие задания не изменены.")
            with Queue(self.state_dir / "queue.sqlite3", destination=destination) as queue, queue.exclusive_run():
                self._configuration = {"version": 1, "destination": queue.destination}
                write_json(self.state_dir / "project.json", self._configuration)
            self._invalidate_stats()
            self._event("Папка назначения сохранена.")
        return {"ok": True}

    def add_folder(self, path: str, alias: str) -> dict:
        if not isinstance(alias, str) or len(alias) > 255:
            raise ValueError("Укажите короткое имя источника.")
        with self._lock:
            self._ensure_idle()
            source = self._allowed_path(path)
            with self._queue() as queue, queue.exclusive_run():
                root = queue.add_root(source, alias)
                queue.set_metadata("last_scan", {"complete": False, "reason": "source_added"})
            self._invalidate_stats()
            self._event(f"Добавлена папка: {root.alias}.")
        return {"ok": True, "source": dataclasses.asdict(root)}

    def settings(self, workers: int | None = None, auto_resume: bool | None = None,
                 preflight_check: bool | None = None) -> dict:
        with self._lock:
            self._ensure_idle()
            updated = dict(self._settings)
            if workers is not None:
                if type(workers) is not int or not 1 <= workers <= 16:
                    raise ValueError("Число параллельных загрузок должно быть от 1 до 16.")
                updated["workers"] = workers
            if auto_resume is not None:
                if type(auto_resume) is not bool:
                    raise ValueError("Автоматическое продолжение должно быть включено или выключено.")
                updated["auto_resume"] = auto_resume
            if preflight_check is not None:
                if type(preflight_check) is not bool:
                    raise ValueError("Предварительная проверка должна быть включена или выключена.")
                updated["preflight_check"] = preflight_check
            write_json(self.state_dir / "web_settings.json", updated)
            self._settings = updated
            return {"ok": True, "settings": dict(self._settings)}

    def folders(self, path: str | None = None) -> dict:
        roots = [{"name": p.name or str(p), "path": str(p)} for p in self.allowed_roots]
        if path is None or path == "":
            return {"path": None, "parent": None, "roots": roots, "directories": roots, "truncated": False}
        current = self._allowed_path(path)
        directories = []
        truncated = False
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                        continue
                    directories.append({"name": entry.name, "path": str(current / entry.name)})
                    if len(directories) >= 1000:
                        truncated = True
                        break
        except OSError:
            raise ValueError("Не удалось прочитать список папок. Проверьте права доступа.") from None
        directories.sort(key=lambda e: e["name"].casefold())
        parent = str(current.parent) if any(current.parent == r or current.parent.is_relative_to(r) for r in self.allowed_roots) else None
        return {"path": str(current), "parent": parent, "roots": roots,
                "directories": directories, "truncated": truncated}

    def jobs(self, status: str | None = None, limit: int = 50, offset: int = 0) -> dict:
        if status in ("", "all"):
            status = None
        if status is not None and status not in STATES:
            raise ValueError("Неизвестный фильтр заданий.")
        if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
            raise ValueError("Некорректная страница списка заданий.")
        if self._configuration is None:
            return {"jobs": [], "total": 0, "offset": offset, "limit": limit}
        fields = {"id", "relative_path", "local_path", "remote_path", "status", "size", "attempts",
                  "last_error", "skip_reason", "next_attempt", "uploaded_at"}
        with self._queue() as queue:
            rows = queue.list_jobs(status, limit=limit, offset=offset, summary=True)
            stats = self._display_stats(queue)
        return {"jobs": [{k: v for k, v in row.items() if k in fields} for row in rows],
                "total": stats[status] if status else stats["total"], "offset": offset, "limit": limit}

    def snapshot(self) -> dict:
        with self._lock:
            activity = copy.deepcopy(self._activity)
            activity["current_files"] = copy.deepcopy(list(self._files.values()))
            result = {"version": __version__, "configured": self._configuration is not None,
                      "authenticated": self._authenticated, "destination": None, "account": None,
                      "sources": [], "stats": {**{s: 0 for s in STATES}, "total": 0, "total_bytes": 0,
                                                "uploaded_bytes": 0, "ready": 0, "permanent_failures": 0},
                      "activity": activity, "settings": dict(self._settings),
                      "last_scan": None, "probe": None, "events": copy.deepcopy(self._events)}
        if self._configuration is not None:
            with self._queue() as queue:
                report = queue.get_metadata("probe", {})
                result.update(destination=queue.destination, account=queue.account_id,
                              sources=[dataclasses.asdict(r) for r in queue.roots()], stats=self._display_stats(queue),
                              last_scan=queue.get_metadata("last_scan"))
                if isinstance(report, dict) and report:
                    result["probe"] = {k: report[k] for k in ("result", "started_at", "finished_at", "explanation", "error") if k in report}
                    before = report.get("before", {}).get("used_bytes") if isinstance(report.get("before"), dict) else None
                    if type(before) is int:
                        result["probe"]["quota_deltas"] = [m["used_bytes"] - before for m in report.get("measurements", [])
                                                            if isinstance(m, dict) and type(m.get("used_bytes")) is int]
        return result

    def _launch(self, name: str, function: Callable[[], tuple[str, str]], *, upload_intent: bool = False) -> dict:
        with self._lock:
            self._ensure_idle()
            self._stop = self._event_factory()
            self._pause_requested = False
            self._shutting_down = False
            self._operation = name
            self._files.clear()
            self._uploaded_in_run = 0
            self._invalidate_stats()
            self._resume_upload = upload_intent
            self._auto_pending = False
            self._activity.update(phase=name if name in {"login", "scanning"} else "checking",
                                  message="Запускаем операцию…", last_error=None, started_at=time.time(),
                                  scan=None, probe_wait_until=None)
            self._persist_activity()
            self._thread = threading.Thread(target=self._execute, args=(function,), name="mailru-web-task", daemon=True)
            self._thread.start()
        return {"ok": True, "started": name}

    def _execute(self, function: Callable[[], tuple[str, str]]) -> None:
        try:
            phase, message = function()
            with self._lock:
                if self._pause_requested:
                    raise Cancelled("Приостановлено.")
                if self._shutting_down and self._resume_upload:
                    with self._queue() as queue:
                        stats = queue.stats()
                    remaining = stats["pending"] + stats["running"] + stats["failed"] - stats["permanent_failures"]
                    if remaining:
                        raise Cancelled("Остановка процесса; сохранено намерение продолжить очередь.")
                self._resume_upload = False
            self._phase(phase, message)
        except Cancelled:
            with self._lock:
                if not self._shutting_down:
                    self._resume_upload = False
            self._phase("paused", "Приостановлено. Очередь и завершённые файлы сохранены.")
        except Exception as error:
            interrupted_network = (isinstance(error, TransportError) and self._stop.is_set()
                                   and (self._pause_requested or self._shutting_down))
            if interrupted_network:
                with self._lock:
                    if not self._shutting_down:
                        self._resume_upload = False
                self._phase("paused", "Приостановлено. Очередь сохранена после завершения сетевого запроса.")
            else:
                safe = str(error) if isinstance(error, (QueueError, AuthError, ProtocolError, TransportError, ProbeRequired, ValueError)) else f"Ошибка {type(error).__name__}; очередь сохранена."
                with self._lock:
                    self._resume_upload = False
                    if isinstance(error, AuthError):
                        self._authenticated = False
                self._phase("error", safe, error=safe)
        finally:
            with self._lock:
                self._invalidate_stats()
                self._files.clear()
                self._activity["probe_wait_until"] = None
                self._operation = None
                self._persist_activity()
                if self._closed:
                    self._release_server_lock()

    def _check_stop(self) -> None:
        if self._stop.is_set():
            raise Cancelled("Приостановлено.")

    def _new_api(self):
        return self._api_factory(TokenManager(TokenStore(self.state_dir / "auth.dat")), stop=self._stop)

    def login(self, email: str, password: str) -> dict:
        if not isinstance(email, str) or not email.strip() or len(email) > 320:
            raise ValueError("Укажите адрес аккаунта Mail.")
        if not isinstance(password, str) or not password or len(password) > 1024:
            raise ValueError("Введите пароль внешнего приложения.")
        if self._configuration is None:
            raise ValueError("Сначала выберите папку назначения в Облаке.")
        email = email.strip().casefold()
        def perform():
            nonlocal password
            api = None
            with self._queue() as queue, queue.exclusive_run():
                try:
                    self._phase("login", "Входим в аккаунт…")
                    tokens = self._auth_client_factory().login(email, password)
                    self._check_stop()
                    class MemoryStore:
                        def __init__(self, value): self.value = value
                        def load(self): return self.value
                        def save(self, value): self.value = value
                    memory = MemoryStore(tokens)
                    api = self._api_factory(TokenManager(memory), stop=self._stop)
                    account = api.account_info()
                    if account.email.strip().casefold() != email:
                        raise AuthError("Сервер вернул другой аккаунт; вход не сохранён.")
                    self._check_stop()
                    queue.bind_account(account.email)
                    TokenStore(self.state_dir / "auth.dat").save(memory.value)
                    previous = queue.get_metadata("probe")
                    if isinstance(previous, dict) and previous.get("result") == "provisional_pass":
                        queue.set_metadata("probe", {**previous, "result": "invalidated", "reason": "new_login"})
                    queue.set_metadata("last_auth", {"result": "success", "at": time.time(), "stage": "user_info", "client_id": tokens.client_id})
                    with self._lock:
                        self._authenticated = True
                except AuthError as error:
                    queue.set_metadata("last_auth", {"result": "failed", "at": time.time(), **error.safe_diagnostics()})
                    raise
                finally:
                    password = ""
                    if api is not None:
                        api.close_thread()
            return "idle", "Вход сохранён. Повторно вводить пароль при обычном запуске не потребуется."
        return self._launch("login", perform)

    def _scan_queue(self, queue: Queue) -> None:
        self._phase("scanning", "Сканируем папки с фотографиями…")
        def on_progress(result):
            with self._lock:
                self._activity["scan"] = dataclasses.asdict(result)
                self._activity["message"] = f"Найдено фотографий: {result.seen:,}."
        with queue.exclusive_run():
            queue.recover_running()
            for source in queue.roots():
                self._allowed_path(source.path)
            extensions = queue.get_metadata("extensions", sorted(PHOTO_EXTENSIONS))
            queue.set_metadata("last_scan", {"complete": False, "reason": "scan_started"})
            result = queue.scan(extensions, stop=self._stop, progress=on_progress)
            queue.set_metadata("last_scan", dataclasses.asdict(result))
            queue.set_metadata("extensions", sorted(extensions))
        self._check_stop()
        if not result.complete:
            raise QueueError(f"Сканирование завершилось с ошибками: {result.error_count}. Проверьте доступ к папкам.")
        LOGGER.info("scan_complete seen=%d added=%d changed=%d",
                    result.seen, result.added, result.changed)

    def scan(self) -> dict:
        if self._configuration is None:
            raise ValueError("Сначала выберите папку назначения.")
        def perform():
            with self._queue() as queue:
                self._scan_queue(queue)
                total = queue.stats()["total"]
            return "idle", f"Сканирование завершено. Файлов в очереди: {total:,}."
        return self._launch("scanning", perform)

    def _emit(self, event: str, data: dict) -> None:
        with self._lock:
            if event == "progress":
                # The runner already computed this summary for CLI progress.
                with self._stats_lock:
                    self._stats_cache = (time.monotonic(), copy.deepcopy(data))
            elif event == "file_started":
                self._files[data["job_id"]] = {"id": data["job_id"], "path": data["path"], "size": data["size"], "sent": 0, "stage": "preparing"}
            elif event == "upload_progress":
                item = self._files.get(data["job_id"])
                if item:
                    item["sent"] = min(item["size"], item["sent"] + max(0, data["bytes"]))
                    item["stage"] = "uploading"
            elif event == "file_finished":
                self._files.pop(data["job_id"], None)
            elif event == "probe_wait":
                self._activity["probe_wait_until"] = time.time() + data["seconds"]
                self._activity["message"] = "Проверяем, что фотографии не расходуют квоту…"
            elif event == "uploaded":
                self._uploaded_in_run += 1
                if self._uploaded_in_run % 100 == 0:
                    self._event(f"За текущий запуск загружено файлов: {self._uploaded_in_run:,}.")
                    LOGGER.info("uploaded_in_run=%d", self._uploaded_in_run)
            elif event in {"file_error", "file_changed", "skipped"}:
                self._event(str(data.get("reason", "Ошибка файла.")), "warning")
            elif event == "remote_error":
                self._event(f"Ошибка сервера: HTTP {data.get('status')}, {data.get('code')}.", "warning")
            elif event == "retry":
                self._event(f"Файл {data.get('job_id')}: соединение прервано, повторная попытка отложена.", "warning")

    def _valid_probe(self, report: Any, account: str, destination: str) -> bool:
        if not isinstance(report, dict):
            return False
        finished = report.get("finished_at")
        before = report.get("before")
        return (report.get("result") == "provisional_pass" and report.get("protocol") == PROTOCOL_VERSION
                and report.get("account") == account and report.get("destination") == destination
                and type(finished) in (int, float) and math.isfinite(finished) and 0 <= time.time() - finished <= 86400
                and isinstance(before, dict) and type(before.get("used_bytes")) is int)

    def _can_resume_probe(self, report: Any, account: str, destination: str) -> bool:
        if not isinstance(report, dict) or report.get("result") not in {"incomplete", "interrupted"}:
            return False
        started = report.get("started_at")
        before = report.get("before")
        samples = report.get("samples")
        return (report.get("protocol") == PROTOCOL_VERSION and report.get("account") == account
                and report.get("destination") == destination and type(started) in (int, float)
                and math.isfinite(started) and 0 <= time.time() - started <= 86400
                and isinstance(before, dict) and type(before.get("used_bytes")) is int
                and isinstance(samples, list) and len(samples) <= 10
                and (samples or type(report.get("active_job_id")) is int)
                and all(isinstance(s, dict) and s.get("is_unlim_upload") is True
                        and s.get("stat_matched") is True
                        and ((s.get("commit_accepted") is True and s.get("unlimited_evidence") == "commit_response")
                             or (s.get("recovered_commit") is True and s.get("unlimited_evidence") == "saved_commit_response"))
                        and type(s.get("job_id")) is int for s in samples))

    @staticmethod
    def _proof_belongs_to_probe(metadata: dict, started: float) -> bool:
        if metadata.get("unlimited_evidence") == "commit_response":
            return metadata.get("commit_accepted") is True
        if metadata.get("unlimited_evidence") != "saved_commit_response":
            return False
        created = metadata.get("commit_intent_created_at")
        confirmed = metadata.get("unlimited_confirmed_at")
        return (metadata.get("recovered_commit") is True and type(created) in (int, float)
                and type(confirmed) in (int, float) and started <= created <= confirmed <= time.time())

    def _resume_probe(self, queue: Queue, api: CloudAPI, previous: dict) -> dict:
        """Resume only a recent interrupted probe, keeping its original baseline.

        File history/intents must prove that this probe performed the confirmed
        commit. Both quota measurements restart; no unrelated new sample is used.
        """
        report = copy.deepcopy(previous)
        with queue.exclusive_run():
            report.update(result="incomplete", measurements=[], resumed_at=time.time())
            report.pop("error", None)
            queue.set_metadata("probe", report)
            try:
                active_id = report.get("active_job_id")
                if type(active_id) is int:
                    # mark_uploaded and report.samples are separate transactions.
                    # History covers a power loss between those two commits.
                    history = [row for row in queue.upload_history(active_id)
                               if row.get("uploaded_at", 0) >= report["started_at"]
                               and row["verified_metadata"].get("protocol") == PROTOCOL_VERSION
                               and row["verified_metadata"].get("is_unlim_upload") is True
                               and row["verified_metadata"].get("stat_matched") is True
                               and self._proof_belongs_to_probe(row["verified_metadata"], report["started_at"])]
                    if history:
                        result = {"job_id": active_id, **history[-1]["verified_metadata"]}
                    else:
                        job = queue.claim(max_attempts=5)
                        if job is None or job.id != active_id:
                            if job is not None:
                                queue.release(job, "Waiting for the original interrupted probe job")
                            raise ProtocolError("Не найдено исходное задание прерванной проверки; очередь сохранена.")
                        try:
                            intent = queue.get_commit_intent(job)
                            if intent and intent.get("unlimited_confirmed") is True:
                                confirmed = intent.get("unlimited_confirmed_at")
                                created = intent.get("created_at")
                                if not (type(confirmed) in (int, float) and type(created) in (int, float)
                                        and report["started_at"] <= created <= confirmed <= time.time()):
                                    raise ProtocolError("Подтверждение файла старше этой проверки; его нельзя использовать как новый образец.")
                            result = process_job(queue, api, job, emit=self._emit)
                        except Cancelled:
                            queue.release(job, "Interrupted automatic probe; retain saved confirmation")
                            raise
                        except Exception as error:
                            if isinstance(error, TransportError) and api.stop.is_set():
                                queue.release(job, "Network request interrupted during resumed probe")
                                raise Cancelled("Контрольная проверка приостановлена.") from None
                            if not isinstance(error, FileChangedError):
                                safe = str(error) if isinstance(error, (ProtocolError, AuthError, TransportError)) else type(error).__name__
                                queue.mark_failed(job, safe, retry_delay=None)
                            raise
                    report["samples"].append(result)
                    report.pop("active_job_id", None)
                    queue.set_metadata("probe", report)
                for sample in report["samples"]:
                    evidence = [row for row in queue.upload_history(sample["job_id"])
                                if row.get("uploaded_at", 0) >= report["started_at"]
                                and row.get("remote_hash") == sample.get("hash")
                                and row.get("source_size") == sample.get("size")
                                and row["verified_metadata"].get("protocol") == PROTOCOL_VERSION
                                and row["verified_metadata"].get("is_unlim_upload") is True
                                and row["verified_metadata"].get("unlimited_evidence") in {"commit_response", "saved_commit_response"}
                                and row["verified_metadata"].get("path") == sample.get("path")
                                and self._proof_belongs_to_probe(row["verified_metadata"], report["started_at"])]
                    if not evidence or api.stat(sample["path"]) != {"hash": sample["hash"], "size": sample["size"]}:
                        raise ProtocolError("Не удалось подтвердить файл прерванной контрольной проверки.")
                for step in range(2):
                    self._emit("probe_wait", {"seconds": self._settle_seconds / 2, "measurement": step + 1})
                    if api.stop.wait(self._settle_seconds / 2):
                        raise Cancelled("Контрольная проверка приостановлена.")
                    api.require_account(report["account"])
                    quota = api.quota()
                    report["measurements"].append({"at": time.time(), **quota})
                    queue.set_metadata("probe", report)
                unchanged = all(q["used_bytes"] == report["before"]["used_bytes"] for q in report["measurements"])
                report["result"] = "provisional_pass" if unchanged else "quota_changed"
                report["explanation"] = "Проверка безлимита завершена, квота не изменилась." if unchanged else "Квота изменилась; массовая загрузка не начата."
            except Cancelled:
                report["result"] = "interrupted"
                raise
            except TransportError:
                if api.stop.is_set():
                    report["result"] = "interrupted"
                    raise Cancelled("Контрольная проверка приостановлена.") from None
                report["result"] = "failed"
                raise
            except Exception:
                report["result"] = "failed"
                raise
            finally:
                report["finished_at"] = time.time()
                queue.set_metadata("probe", report)
        return report

    def _pipeline(self) -> tuple[str, str]:
        api = None
        with self._queue() as queue, queue.exclusive_run():
            with queue.exclusive_run():
                queue.recover_running()
                if not queue.roots():
                    raise ValueError("Добавьте хотя бы одну папку с фотографиями.")
                for source in queue.roots():
                    self._allowed_path(source.path)
            last_scan = queue.get_metadata("last_scan", {})
            if not isinstance(last_scan, dict) or last_scan.get("complete") is not True:
                self._scan_queue(queue)
            self._check_stop()
            stats = queue.stats()
            remaining = stats["pending"] + stats["running"] + stats["failed"] - stats["permanent_failures"]
            old_probe = queue.get_metadata("probe", {})
            preflight_check = self._settings["preflight_check"]
            resume_control = preflight_check and self._can_resume_probe(old_probe, queue.account_id, queue.destination)
            if not remaining and not resume_control:
                if stats["failed"]:
                    return "error", "Есть файлы с ошибками. Исправьте причину и нажмите «Повторить ошибки»."
                return "completed", "Все найденные файлы обработаны." if stats["skipped"] else "Все найденные фотографии загружены."
            api = self._new_api()
            try:
                self._phase("checking", "Проверяем аккаунт и доступность безлимита…")
                with queue.exclusive_run():
                    account = api.require_account(queue.account_id)
                    queue.bind_account(account.email)
                    report = queue.get_metadata("probe", {})
                    valid = preflight_check and self._valid_probe(report, account.email, queue.destination)
                    if valid:
                        valid = api.quota()["used_bytes"] == report["before"]["used_bytes"]
                if preflight_check and not valid:
                    self._phase("probing", "Проверяем безлимит на одном файле перед загрузкой архива…")
                    if self._can_resume_probe(report, account.email, queue.destination):
                        report = self._resume_probe(queue, api, report)
                    else:
                        stats = queue.stats()
                        retry_at = stats.get("next_retry_at")
                        if stats["ready"] == 0 and retry_at is not None:
                            self._phase("probing", "Ожидаем повторной попытки после временного сбоя…")
                            if api.stop.wait(max(0, retry_at - time.time())):
                                raise Cancelled("Приостановлено.")
                        report = probe(queue, api, count=1, settle_seconds=self._settle_seconds,
                                       emit=self._emit, file_progress=True)
                    if report.get("result") != "provisional_pass":
                        raise ProbeRequired("Квота изменилась во время проверки. Загрузка приостановлена; повторите запуск при стабильной квоте.")
                self._check_stop()
                self._phase("uploading", "Загружаем фотографии в безлимит…")
                result = run(queue, api, workers=self._settings["workers"], emit=self._emit,
                             file_progress=True, require_probe=preflight_check)
                if self._pause_requested or self._shutting_down:
                    raise Cancelled("Приостановлено.")
                if result.get("stopped"):
                    raise ProtocolError((result.get("errors") or ["Загрузка остановлена; очередь сохранена."])[0])
                if result.get("outcome") == "finished_with_errors":
                    return "error", "Загрузка завершилась с ошибками; подробности в списке заданий."
                if result.get("outcome") == "finished_with_skips":
                    return "completed", "Загрузка завершена; есть пропущенные файлы."
                return "completed", "Все фотографии загружены и проверены."
            finally:
                api.close_thread()

    def start(self) -> dict:
        if self._configuration is None:
            raise ValueError("Сначала выберите папку назначения в Облаке.")
        if not self._authenticated:
            raise ValueError("Сначала войдите в аккаунт Mail.")
        return self._launch("upload", self._pipeline, upload_intent=True)

    def pause(self) -> dict:
        with self._lock:
            self._resume_upload = False
            self._auto_pending = False
            self._pause_requested = True
            self._stop.set()
            self._activity.update(phase="pausing" if self._operation else "paused",
                                  message="Приостанавливаем операцию, сохраняем очередь…" if self._operation else "Приостановлено. Очередь сохранена.")
            self._persist_activity()
        return {"ok": True}

    def retry_failed(self, include_permanent: bool = False) -> dict:
        if type(include_permanent) is not bool:
            raise ValueError("Некорректный параметр повторной попытки.")
        with self._lock:
            self._ensure_idle()
            with self._queue() as queue, queue.exclusive_run():
                count = queue.retry_failed(include_permanent=include_permanent)
            self._invalidate_stats()
            self._event(f"Возвращено в очередь: {count:,}.")
            return {"ok": True, "queued_again": count}

    def resume_if_requested(self) -> bool:
        """Call after the HTTP listener is ready; no spontaneous fresh uploads."""
        with self._lock:
            requested = self._auto_pending and self._settings["auto_resume"] and not self._closed
        if requested:
            try:
                self.start()
            except (ValueError, QueueError) as error:
                with self._lock:
                    self._resume_upload = False
                    self._auto_pending = False
                self._phase("error", str(error), error=str(error))
                return False
            return True
        return False

    def _release_server_lock(self) -> None:
        with self._lock:
            if not self._server_lock_released:
                self._server_lock_released = True
                self._server_lock.__exit__(None, None, None)

    def close(self, timeout: float = 15) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._shutting_down = True
            self._stop.set()
            self._persist_activity()
            task = self._thread
        if task is not None and task.is_alive() and task is not threading.current_thread():
            task.join(timeout=timeout)
        # If a socket is still draining, keep the process lock until it exits.
        # A second controller must not reuse the same state concurrently.
        if task is None or not task.is_alive():
            self._release_server_lock()

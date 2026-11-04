"""Local web controller around the verified CLI engine; no new cloud protocol.

Each task owns its SQLite connection. Browser requests see bounded summaries and
never receive authentication tokens. A durable user intention controls restart;
manual pause clears that intention, process shutdown deliberately preserves it.
"""
from __future__ import annotations

import copy
from collections import OrderedDict, deque
import contextlib
import dataclasses
import json
import logging
import os
import secrets
import hmac
import sqlite3
from pathlib import Path
import threading
import time
from typing import Any, Callable
from urllib.parse import quote

from . import __version__
from .api import CloudAPI, FileIdentity, PROTOCOL_VERSION, ProtocolError, is_auth_failure
from .auth import AuthClient, AuthError, TokenManager, TokenStore
from .cli import write_json
from .queue import FileChangedError, Queue, QueueBusyError, QueueError, STATES, _file_lock
from .runner import QuotaBudget, run
from .scheduler import Scheduler, checked_timezone
from .deletion import (DeletionJournal, LocalDeletionRecoveryRequired, LOCAL_CONFIRMATION, deletion_preview,
                       delete_local_file, local_delete_supported, selected_tasks)
from .transport import Cancelled, TransportError

LOGGER = logging.getLogger("mailru_photos.service")
LOGGER.addHandler(logging.NullHandler())


class _QuotaObservedAPI:
    """Observe genuine quota responses without extra requests or token managers."""
    def __init__(self, api, callback):
        self._api, self._callback = api, callback

    def __getattr__(self, name):
        return getattr(self._api, name)

    def quota(self):
        value = self._api.quota()
        self._callback(value)
        return value


class WebService:
    def __init__(self, state_dir: Path, allowed_roots: list[Path], *,
                 api_factory: Callable[..., CloudAPI] = CloudAPI,
                 auth_client_factory: Callable[..., AuthClient] = AuthClient,
                 event_factory: Callable[[], threading.Event] = threading.Event):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.allowed_roots = tuple(dict.fromkeys(Path(p).expanduser().resolve() for p in allowed_roots))
        if not self.allowed_roots:
            raise ValueError("Укажите хотя бы одну доступную папку с фото и видео.")
        self._api_factory = api_factory
        self._auth_client_factory = auth_client_factory
        self._event_factory = event_factory
        self._lock = threading.RLock()
        self._stats_lock = threading.Lock()
        self._stats_cache: tuple[float, dict] | None = None
        self._database_cache: tuple[float, dict] | None = None
        self._jobs_cache = OrderedDict()
        self._priority_jobs = deque()
        self._task_starts_closed = True
        self._deletion_previews = {}
        self.scheduler = None
        self._deletion = None
        self._deletion_pending = False
        self._auto_deletion = False
        self._thread: threading.Thread | None = None
        self._operation: str | None = None
        self._stop = event_factory()
        self._pause_requested = False
        self._shutting_down = False
        self._closed = False
        self._files: dict[int, dict] = {}
        self._task_signals: dict[int, tuple[str, threading.Event]] = {}
        self._quota_cache: dict | None = None
        self._quota_error: str | None = None
        self._uploaded_in_run = 0
        self._settings = {"workers": 2, "auto_resume": True, "remote_reconcile": False,
                          "regular_policy": "skip", "unexpected_unlimited_action": "trash",
                          "timezone": checked_timezone(os.environ.get("MAILRU_TIMEZONE", "Europe/Moscow"))}
        stored = self._read_json("web_settings.json", {})
        if isinstance(stored, dict):
            if isinstance(stored.get("timezone"), str):
                self._settings["timezone"] = checked_timezone(stored["timezone"])
            if type(stored.get("workers")) is int and 1 <= stored["workers"] <= 16:
                self._settings["workers"] = stored["workers"]
            if type(stored.get("auto_resume")) is bool:
                self._settings["auto_resume"] = stored["auto_resume"]
            if stored.get("regular_policy") in {"skip", "upload"}:
                self._settings["regular_policy"] = stored["regular_policy"]
            if stored.get("unexpected_unlimited_action") in {"trash", "keep"}:
                self._settings["unexpected_unlimited_action"] = stored["unexpected_unlimited_action"]
            if type(stored.get("remote_reconcile")) is bool:
                self._settings["remote_reconcile"] = stored["remote_reconcile"]
        previous = self._read_json("web_activity.json", {})
        self._auth_required = isinstance(previous, dict) and previous.get("auth_required") is True
        self._logout_requested = isinstance(previous, dict) and previous.get("logout_requested") is True
        self._resume_upload = (isinstance(previous, dict) and previous.get("resume_upload") is True
                               and not self._logout_requested and not self._auth_required)
        selected = previous.get("job_id") if isinstance(previous, dict) else None
        self._resume_job_id = selected if type(selected) is int and selected > 0 else None
        scope = previous.get("root_ids") if isinstance(previous, dict) else None
        self._resume_root_ids = sorted(set(scope)) if isinstance(scope, list) and scope and all(type(i) is int and i > 0 for i in scope) else None
        occurrence = previous.get("occurrence_id") if isinstance(previous, dict) else None
        self._resume_occurrence_id = occurrence if isinstance(occurrence, str) and len(occurrence) == 32 else None
        self._auto_pending = self._resume_upload and self._settings["auto_resume"]
        self._activity = {"phase": "paused" if self._resume_upload else "idle",
                          "message": "Очередь восстановлена после остановки." if self._resume_upload else "Готово к работе.",
                          "last_error": None, "started_at": None, "scan": None,
                          "current_files": []}
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
                    queue.upgrade_media_discovery()
                    queue.apply_regular_policy(upload=self._settings["regular_policy"] == "upload")
                    if recovered:
                        self._activity["message"] = f"Восстановлено незавершённых заданий: {recovered}."
            journal_path = self.state_dir / "deletions.sqlite3"
            if journal_path.exists():
                with DeletionJournal(journal_path) as journal:
                    operation = journal.operation()
                    if operation and operation.get("state") in {"confirmed", "running", "paused"}:
                        self._deletion_pending = True
                        self._auto_deletion = operation.get("state") in {"confirmed", "running"} and self._settings["auto_resume"]
                        self._deletion = {**journal.summary(), "resume_available": True}
                        self._activity.update(phase="paused", message="Незавершённое удаление сохранено. Нажмите «Продолжить».")
            self._authenticated = False
            if self._logout_requested:
                self._complete_logout()
            else:
                self._authenticated = self._has_auth()
                if self._auth_required:
                    self._quota_error = "Для обновления данных требуется повторный вход в Mail.ru."
                    self._activity.update(phase="paused", message="Для продолжения требуется повторный вход в Mail.ru.")
            self.scheduler = Scheduler(self.state_dir / "schedules.sqlite3", timezone_name=self._settings["timezone"],
                                       launch=self._start_scheduled, resume_interrupted=self._settings["auto_resume"])
            if self._resume_occurrence_id:
                saved_occurrence = self.scheduler.occurrence(self._resume_occurrence_id)
                if saved_occurrence and saved_occurrence["status"] in {"complete", "error", "cancelled"}:
                    self._resume_upload = self._auto_pending = False
                    self._resume_occurrence_id = self._resume_root_ids = None
                    if not self._deletion_pending:
                        self._activity.update(phase="idle", message="Предыдущий запуск планировщика завершён.")
                    self._persist_activity()
                elif (saved_occurrence and saved_occurrence["status"] in {"queued", "running"}
                      and isinstance(previous, dict) and previous.get("resume_upload") is False
                      and previous.get("phase") in {"pausing", "paused", "logout"}):
                    # pause() can be durable before the worker checkpoints its
                    # occurrence. That manual decision wins after a crash.
                    self.scheduler.finish(saved_occurrence["id"], "paused", "Выполнение приостановлено пользователем.")
                    self._resume_upload = self._auto_pending = False
                    self._activity.update(phase="paused", message="Запуск планировщика приостановлен. Нажмите «Продолжить».")
                    self._persist_activity()
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
            raise ValueError("Сначала добавьте исходную папку или войдите в аккаунт Mail.")
        return Queue(self.state_dir / "queue.sqlite3", destination=self._configuration["destination"])

    def _ensure_configuration(self) -> None:
        """Initialize new panels without asking for a global cloud folder.

        Existing queue destinations remain the compatibility default for CLI
        and older clients. New source forms supply their complete destination.
        Call with the controller lock held and after checking it is idle.
        """
        if self._configuration is not None:
            return
        with Queue(self.state_dir / "queue.sqlite3", destination="/") as queue, queue.exclusive_run():
            configuration = {"version": 1, "destination": queue.destination}
            write_json(self.state_dir / "project.json", configuration)
            self._configuration = configuration
        self._invalidate_stats()

    @staticmethod
    def _source_id(value: int) -> int:
        if type(value) is not int or not 1 <= value <= 2**63 - 1:
            raise ValueError("Укажите корректный номер исходной папки.")
        return value

    @staticmethod
    def _source_alias(value: str) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > 255 or "\x00" in value:
            raise ValueError("Укажите короткое имя источника.")
        return value

    @staticmethod
    def _source_destination(value: str) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > 2048 or "\x00" in value:
            raise ValueError("Укажите корректную папку назначения для источника.")
        return value

    def _has_auth(self) -> bool:
        if self._auth_required:
            return False
        try:
            TokenStore(self.state_dir / "auth.dat").load()
            return True
        except AuthError:
            return False

    def _database_snapshot(self) -> dict:
        """No SQLite opens or file stats for repeated idle dashboard polling."""
        with self._stats_lock:
            active = self._operation is not None
            cached = self._database_cache
            if cached is None or (active and time.monotonic() - cached[0] >= 5):
                with self._queue() as queue:
                    value = {"destination": queue.destination, "account": queue.account_id,
                             "sources": [dataclasses.asdict(r) for r in queue.roots()],
                             "stats": queue.stats(), "last_scan": queue.get_metadata("last_scan")}
                self._database_cache = (time.monotonic(), value)
            return copy.deepcopy(self._database_cache[1])

    def _invalidate_stats(self) -> None:
        with self._stats_lock:
            self._stats_cache = None
            self._database_cache = None
            self._jobs_cache.clear()

    def _persist_activity(self) -> None:
        write_json(self.state_dir / "web_activity.json", {
            "version": 1, "resume_upload": self._resume_upload, "job_id": self._resume_job_id,
            "root_ids": self._resume_root_ids, "occurrence_id": self._resume_occurrence_id,
            "logout_requested": self._logout_requested,
            "auth_required": self._auth_required,
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
            if self._logout_requested and phase != "logout":
                return
            # A worker must not overwrite a visible pause request while it drains.
            if self._pause_requested and phase in {"checking", "uploading", "scanning", "deleting"}:
                return
            self._activity.update(phase=phase, message=message, last_error=error)
            self._persist_activity()
        self._event(message, "error" if error else "info")
        # Container logs carry fixed lifecycle fields only. User paths, email,
        # request bodies, token values and free-form error messages stay out.
        LOGGER.log(logging.ERROR if error else logging.INFO,
                   "phase=%s error=%s", phase, bool(error))

    def _ensure_idle(self) -> None:
        if self._closed:
            raise QueueError("Приложение останавливается.")
        if self._operation is not None or self._logout_requested:
            raise QueueBusyError("Дождитесь текущей операции или нажмите «Пауза».")

    def _allowed_path(self, value: str | Path, *, exists: bool = True) -> Path:
        if not isinstance(value, (str, Path)) or not str(value) or "\x00" in str(value):
            raise ValueError("Укажите папку с фото и видео.")
        path = Path(value).expanduser().resolve()
        if not any(path == root or path.is_relative_to(root) for root in self.allowed_roots):
            raise ValueError("Эта папка находится за пределами подключённых каталогов с фото и видео.")
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

    def add_folder(self, path: str, alias: str, remote_prefix: str | None = None) -> dict:
        alias = self._source_alias(alias)
        if remote_prefix is not None:
            remote_prefix = self._source_destination(remote_prefix)
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            source = self._allowed_path(path)
            self._ensure_configuration()
            with self._queue() as queue, queue.exclusive_run():
                previous = queue.roots()
                root = queue.add_root(source, alias, remote_prefix=remote_prefix)
                changed = root not in previous
            self._invalidate_stats()
            if changed:
                self._event(f"Добавлена папка: {root.alias}.")
        return {"ok": True, "source": dataclasses.asdict(root)}

    def update_source(self, source_id: int, *, remote_prefix: str, alias: str | None = None) -> dict:
        source_id = self._source_id(source_id)
        remote_prefix = self._source_destination(remote_prefix)
        if alias is not None:
            alias = self._source_alias(alias)
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            with self._queue() as queue, queue.exclusive_run():
                previous = next((root for root in queue.roots() if root.id == source_id), None)
                if previous is None:
                    raise ValueError("Исходная папка не найдена. Обновите страницу.")
                self._allowed_path(previous.path, exists=False)
                root = queue.update_root(source_id, remote_prefix=remote_prefix, alias=alias)
                changed = root != previous
            self._invalidate_stats()
            if changed:
                self._event(f"Изменён источник: {root.alias}. Уже загруженные файлы остаются на прежнем месте.")
        return {"ok": True, "source": dataclasses.asdict(root)}

    def remove_source(self, source_id: int) -> dict:
        source_id = self._source_id(source_id)
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            with self._queue() as queue, queue.exclusive_run():
                root = next((root for root in queue.roots() if root.id == source_id), None)
                if root is None:
                    raise ValueError("Исходная папка не найдена. Обновите страницу.")
                if self.scheduler is not None:
                    self.scheduler.remove_source(source_id)
                removed = queue.remove_root(source_id)
            self._invalidate_stats()
            self._event(f"Убран источник: {root.alias}. Удалено задач: {removed:,}. Файлы на диске и в Облаке сохранены.")
        return {"ok": True, "removed_tasks": removed}

    def settings(self, workers: int | None = None, auto_resume: bool | None = None,
                 remote_reconcile: bool | None = None, regular_policy: str | None = None,
                 unexpected_unlimited_action: str | None = None, timezone: str | None = None) -> dict:
        with self._lock:
            self._ensure_idle()
            updated = dict(self._settings)
            if workers is not None:
                if type(workers) is not int or not 1 <= workers <= 16:
                    raise ValueError("Число параллельных загрузок должно быть от 1 до 16.")
                updated["workers"] = workers
            for key, value in (("auto_resume", auto_resume), ("remote_reconcile", remote_reconcile)):
                if value is not None:
                    if type(value) is not bool:
                        raise ValueError("Настройка должна быть включена или выключена.")
                    updated[key] = value
            for key, value, allowed in (("regular_policy", regular_policy, {"skip", "upload"}),
                                       ("unexpected_unlimited_action", unexpected_unlimited_action, {"trash", "keep"})):
                if value is not None:
                    if not isinstance(value, str) or value not in allowed:
                        raise ValueError("Некорректная политика хранения.")
                    updated[key] = value
            if timezone is not None:
                updated["timezone"] = checked_timezone(timezone)
            if self._configuration and updated["regular_policy"] != self._settings["regular_policy"]:
                with self._queue() as queue, queue.exclusive_run():
                    queue.apply_regular_policy(upload=updated["regular_policy"] == "upload")
            try:
                write_json(self.state_dir / "web_settings.json", updated)
            except OSError:
                if self._configuration and updated["regular_policy"] != self._settings["regular_policy"]:
                    with self._queue() as queue, queue.exclusive_run():
                        queue.apply_regular_policy(upload=self._settings["regular_policy"] == "upload")
                self._invalidate_stats()
                raise
            self._settings = updated
            if self.scheduler is not None:
                self.scheduler.set_timezone(updated["timezone"])
            self._invalidate_stats()
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

    def task_source(self, task_id: int) -> tuple[Path, Path, FileIdentity]:
        task_id = self._source_id(task_id)
        with self._queue() as queue:
            row = queue.get_job(task_id)
            if row is None:
                raise FileNotFoundError("Задача больше не существует в текущей очереди.")
            source = next((root for root in queue.roots() if root.id == row["root_id"]), None)
        if source is None:
            raise FileNotFoundError("Исходная папка задачи больше не подключена.")
        self._allowed_path(source.path, exists=False)
        root, path = Path(source.path), Path(row["local_path"])
        # Do not resolve the file here: the content reader opens each path
        # component without following symlinks, then validates its identity.
        if not path.is_absolute() or not path.is_relative_to(root) or ".." in path.parts:
            raise ValueError("Исходный файл находится за пределами подключённой папки.")
        return root, path, FileIdentity(row["size"], row["mtime_ns"], row["device"], int(row["inode"]))

    def _remember_quota(self, value: dict) -> None:
        keys = ("used_bytes", "mail_bytes", "combined_used_bytes", "total_bytes")
        if not isinstance(value, dict) or any(type(value.get(key)) is not int or value[key] < 0 for key in keys):
            return  # Display telemetry cannot replace the protocol validator.
        with self._lock:
            self._quota_cache = {"used_bytes": value["combined_used_bytes"],
                                 "cloud_bytes": value["used_bytes"], "mail_bytes": value["mail_bytes"],
                                 "total_bytes": value["total_bytes"],
                                 "free_bytes": max(0, value["total_bytes"] - value["combined_used_bytes"]),
                                 "updated_at": time.time()}
            self._quota_error = None

    def _invalidate_auth(self) -> None:
        """Retain the last measured quota, but stop presenting it as current."""
        with self._lock:
            self._authenticated = False
            self._auth_required = True
            self._resume_upload = False
            self._auto_pending = False
            self._quota_error = "Для обновления данных требуется повторный вход в Mail.ru."
            # Keep the explicit rejection across a controller restart even
            # though its saved tokens are retained for a subsequent login.
            self._persist_activity()

    def refresh_quota(self) -> dict:
        with self._lock:
            self._ensure_idle()
            if not self._authenticated:
                raise ValueError("Сначала войдите в аккаунт Mail.")
            self._ensure_configuration()
            previous = (self._activity["phase"], self._activity["message"])
        def perform():
            api = None
            try:
                with self._queue() as queue, queue.exclusive_run():
                    api = self._new_api()
                    account = api.require_account(queue.account_id)
                    queue.bind_account(account.email)
                    api.quota()
            except Cancelled:
                raise
            except (AuthError, ProtocolError, TransportError) as error:
                with self._lock:
                    self._quota_error = str(error)
                    if is_auth_failure(error):
                        self._invalidate_auth()
                self._event("Не удалось обновить квоту. Последние полученные значения сохранены.", "warning")
            finally:
                if api is not None:
                    api.close_thread()
            return previous
        return self._launch("quota", perform)

    def _task_controls_available(self) -> bool:
        return (not self._closed and not self._shutting_down and not self._logout_requested
                and self._configuration is not None
                and self._operation in (None, "upload") and not self._deletion_pending
                and self._activity["phase"] not in {"scanning", "reconciling"}
                and not (self._pause_requested and self._operation is not None))

    @contextlib.contextmanager
    def _task_scope(self, job):
        """A signal belongs to one claim, never to a reusable worker thread."""
        signal = threading.Event()
        with self._lock:
            self._task_signals[job.id] = (job.claim_token, signal)
        try:
            yield signal
        finally:
            with self._lock:
                current = self._task_signals.get(job.id)
                if current is not None and current[0] == job.claim_token:
                    self._task_signals.pop(job.id, None)

    def _next_priority_job(self):
        with self._lock:
            return self._priority_jobs.popleft() if self._priority_jobs else None

    def control_task(self, task_id: int, action: str) -> dict:
        task_id = self._source_id(task_id)
        if action not in {"pause", "start", "resume", "remove"}:
            raise ValueError("Неизвестное действие с задачей.")
        if action == "resume":
            action = "start"
        with self._lock:
            if not self._task_controls_available():
                raise QueueBusyError("Дождитесь завершения текущей операции перед изменением задачи.")
            if action == "start" and not self._authenticated:
                raise ValueError("Сначала войдите в аккаунт Mail.")
            if action == "start" and self._operation == "upload" and self._task_starts_closed:
                raise QueueBusyError("Текущий запуск уже завершается. Повторите запуск задачи после его завершения.")
            with self._queue() as queue:
                guard = queue.exclusive_run() if self._operation is None else contextlib.nullcontext()
                with guard:
                    row = queue.get_job(task_id)
                    if row is None:
                        raise ValueError("Задача не найдена. Обновите список.")
                    if action == "start" and self._operation == "upload" and (
                            (self._resume_job_id is not None and self._resume_job_id != task_id)
                            or (self._resume_root_ids is not None and row["root_id"] not in self._resume_root_ids)):
                        raise QueueBusyError("Завершите текущий выборочный запуск перед запуском задачи из другого источника.")
                    if action == "pause" and row["status"] != "running":
                        raise ValueError("Остановить можно только выполняющуюся задачу.")
                    if action == "start" and row.get("planned_mode") == "regular" and self._settings["regular_policy"] == "skip":
                        raise ValueError("Для этой задачи требуется квота. Сначала разрешите обычную загрузку в настройках.")
                    queue.request_control(task_id, action)
                current = self._task_signals.get(task_id)
                if current is not None and action in {"pause", "remove"}:
                    current[1].set()
            self._invalidate_stats()
            if action == "start":
                if self._operation is None:
                    self._resume_job_id = task_id
                    self._resume_root_ids = None
                    self._resume_occurrence_id = None
                    return self._launch("upload", lambda: self._pipeline(job_id=task_id), upload_intent=True)
                if task_id not in self._priority_jobs:
                    self._priority_jobs.append(task_id)
            messages = {"pause": "Запрошена остановка задачи", "start": "Запрошен запуск задачи",
                        "remove": "Задача убрана из очереди; исходный файл сохранён"}
            self._event(f"{messages[action]}: {task_id}.")
        return {"ok": True, "task_id": task_id, "action": action}

    def clear_tasks(self, scope: str = "all") -> dict:
        if scope not in {"all", "finished"}:
            raise ValueError("Выберите все задачи или обработанные задачи.")
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            with self._queue() as queue, queue.exclusive_run():
                removed = queue.clear_tasks(scope=scope)
            self._invalidate_stats()
            self._event(f"Из очереди убрано задач: {removed:,}. Файлы на диске и в Облаке сохранены.")
        return {"ok": True, "removed_tasks": removed}

    def _ensure_no_deletion(self):
        if self._deletion_pending:
            raise QueueBusyError("Сначала продолжите подтверждённое удаление. Его состояние сохранено.")

    @staticmethod
    def _delete_selection(task_id=None, scope="all", status=None):
        if task_id is not None:
            return {"id": WebService._source_id(task_id)}
        if scope not in {"all", "finished", "filter"}:
            raise ValueError("Неизвестный набор задач для удаления.")
        if status in ("", "all"):
            status = None
        if status is not None and status not in (*STATES, "paused", "uploaded_unlimited", "uploaded_regular", "uploaded_unknown"):
            raise ValueError("Неизвестный фильтр задач.")
        return {"scope": scope, "status": status if scope == "filter" else None}

    def preview_deletion(self, *, task_id=None, scope="all", status=None,
                         delete_cloud=False, delete_local=False, owner=""):
        if type(delete_cloud) is not bool or type(delete_local) is not bool:
            raise ValueError("Выберите действия с помощью галочек.")
        selection = self._delete_selection(task_id, scope, status)
        with self._lock:
            self._ensure_no_deletion()
            if not self._task_controls_available():
                raise QueueBusyError("Дождитесь завершения текущей операции.")
            if delete_cloud or delete_local or task_id is None:
                self._ensure_idle()
            with self._queue() as queue:
                guard = queue.exclusive_run() if self._operation is None else contextlib.nullcontext()
                with guard:
                    preview = deletion_preview(queue, selection, delete_cloud=delete_cloud, delete_local=delete_local)
            if not preview["task_count"]:
                raise ValueError("В выбранном списке нет задач.")
            if delete_cloud and not self._authenticated:
                raise ValueError("Для удаления из Облака сначала войдите в Mail.")
            token = secrets.token_urlsafe(32)
            expires_at = time.time() + 300
            self._deletion_previews = {key: value for key, value in self._deletion_previews.items()
                                       if value["expires_at"] > time.time()}
            if len(self._deletion_previews) >= 32:
                self._deletion_previews.pop(next(iter(self._deletion_previews)))
            self._deletion_previews[token] = {**preview, "selection": selection, "delete_cloud": delete_cloud,
                                             "delete_local": delete_local, "owner": owner, "expires_at": expires_at}
            return {key: value for key, value in {**preview, "token": token, "expires_at": expires_at}.items()
                    if key != "fingerprint"}

    def confirm_deletion(self, token, *, local_confirmation="", owner=""):
        if not isinstance(token, str) or len(token) > 200:
            raise ValueError("Откройте окно удаления заново.")
        with self._lock:
            preview = self._deletion_previews.get(token)
            if (not preview or preview["expires_at"] < time.time()
                    or not hmac.compare_digest(str(preview["owner"]), str(owner))):
                raise ValueError("Подтверждение удаления истекло. Откройте окно заново.")
            self._ensure_no_deletion()
            if preview["delete_local"]:
                if local_confirmation != LOCAL_CONFIRMATION:
                    raise ValueError("Для удаления исходников введите: " + LOCAL_CONFIRMATION)
                if not preview["local_delete_allowed"]:
                    raise ValueError("Источник доступен только для чтения или нет прав на удаление.")
            physical = preview["delete_local"] or preview["delete_cloud"]
            if physical or preview["selection"].get("id") is None:
                self._ensure_idle()
            if preview["delete_cloud"] and not self._authenticated:
                raise ValueError("Сначала войдите в аккаунт Mail.")
            self._deletion_previews.pop(token)
            if not physical:
                selected = preview["selection"]
                if selected.get("id") is not None:
                    return self.control_task(selected["id"], "remove")
                with self._queue() as queue, queue.exclusive_run():
                    current = deletion_preview(queue, selected)
                    if current["fingerprint"] != preview["fingerprint"]:
                        raise ValueError("Список задач изменился. Подтвердите новый список.")
                    if selected["scope"] in {"all", "finished"}:
                        removed = queue.clear_tasks(selected["scope"])
                    else:
                        removed = 0
                        for row in selected_tasks(queue, selected):
                            queue.delete_task_final(row["id"])
                            removed += 1
                self._invalidate_stats()
                self._event(f"Удалено задач: {removed:,}. Файлы сохранены.")
                return {"ok": True, "removed_tasks": removed}
            # Freeze a durable, bounded snapshot before any remote/local deletion.
            with self._queue() as queue, queue.exclusive_run(), DeletionJournal(self.state_dir / "deletions.sqlite3") as journal:
                operation = journal.prepare(queue, preview)
                self._deletion = {**journal.summary(), "resume_available": False}
            self._deletion_pending = True
            self._resume_upload = False
            self._resume_job_id = None
            self._resume_root_ids = None
            self._resume_occurrence_id = None
            return self._launch("deleting", self._perform_deletion)

    def _perform_deletion(self):
        api = None
        with self._queue() as queue, queue.exclusive_run(), DeletionJournal(self.state_dir / "deletions.sqlite3") as journal:
            operation = journal.operation()
            if not operation or operation.get("state") not in {"confirmed", "running", "paused"}:
                raise QueueError("Подтверждённое удаление не найдено.")
            operation["state"] = "running"
            journal.set_operation(operation)
            try:
                if operation["delete_cloud"]:
                    api = self._new_api()
                    api.require_account(queue.account_id)
                self._phase("deleting", "Удаляем только выбранные задачи и подтверждённые файлы…")
                after = 0
                counters = journal.summary()
                roots_by_id = {root.id: root for root in queue.roots()}
                while True:
                    entries = journal.pending(after_id=after)
                    if not entries:
                        break
                    for entry in entries:
                        self._check_stop()
                        after = entry["id"]
                        record = json.loads(entry["record"])
                        current = queue.get_job(entry["id"], include_missing=True)
                        try:
                            if current is None:
                                if ((not operation["delete_cloud"] or entry["cloud_done"])
                                        and (not operation["delete_local"] or entry["local_done"])):
                                    journal.done(entry["id"], "task_done")
                                    counters["done"] += 1
                                    continue
                                raise ValueError("Задача была изменена или удалена; файлы не затронуты.")
                            if current["status"] == "running":
                                raise ValueError("Выполняющуюся задачу нельзя физически удалить.")
                            identity_keys = ("root_id", "local_path", "size", "mtime_ns", "device", "inode")
                            if any(str(current.get(key)) != str(record.get(key)) for key in identity_keys):
                                raise ValueError("Исходник задачи изменился после подтверждения; удаление отменено.")
                            if operation["delete_cloud"] and not entry["cloud_done"]:
                                if record.get("cloud_present") and record.get("cloud_path") and record.get("cloud_hash"):
                                    if any(current.get(key) != record.get(key) for key in ("cloud_path", "cloud_hash", "cloud_size")):
                                        raise ValueError("Облачный объект задачи изменился после подтверждения.")
                                    api.trash_file(record["cloud_path"], expected_hash=record["cloud_hash"],
                                                   expected_size=record["cloud_size"])
                                    if not queue.mark_cloud_deleted(entry["id"], expected_hash=record["cloud_hash"], expected_path=record["cloud_path"]):
                                        raise QueueError("Не удалось сохранить подтверждение удаления облачного объекта.")
                                journal.done(entry["id"], "cloud_done")
                            if operation["delete_local"] and not entry["local_done"]:
                                root = roots_by_id.get(record["root_id"])
                                if root is None or str(root.path) != record["root"]:
                                    raise ValueError("Подключение исходной папки изменилось; файл сохранён.")
                                self._allowed_path(root.path, exists=False)
                                expected = FileIdentity(record["size"], record["mtime_ns"], record["device"], int(record["inode"]))
                                delete_local_file(Path(record["root"]), Path(record["local_path"]), expected, entry["stage"])
                                journal.done(entry["id"], "local_done")
                            queue.delete_task_final(entry["id"])
                            journal.done(entry["id"], "task_done")
                            counters["done"] += 1
                        except (Cancelled, LocalDeletionRecoveryRequired):
                            raise
                        except (sqlite3.Error,):
                            raise
                        except Exception as error:
                            if is_auth_failure(error):
                                raise
                            if (isinstance(error, TransportError) and self._stop.is_set()
                                    and (self._pause_requested or self._shutting_down)):
                                raise Cancelled("Подтверждённое удаление приостановлено; его можно продолжить.") from error
                            message = str(error) if isinstance(error, (ValueError, QueueError, ProtocolError, TransportError)) else type(error).__name__
                            journal.error(entry["id"], message)
                            counters["errors"] += 1
                            if current is not None:
                                queue.record_task_error(entry["id"], "Удаление не завершено: " + message)
                            self._event(f"Задача {entry['id']}: удаление не завершено. " + message, "warning")
                        with self._lock:
                            self._deletion = {**counters, "processed": counters["done"] + counters["errors"],
                                              "resume_available": False}
                        self._invalidate_stats()
                operation["state"] = "complete"
                operation["finished_at"] = time.time()
                journal.set_operation(operation)
                self._deletion_pending = False
                summary = journal.summary()
                with self._lock:
                    self._deletion = {**summary, "resume_available": False}
                if api is not None:
                    try:
                        api.quota()
                    except (AuthError, ProtocolError, TransportError) as error:
                        if is_auth_failure(error):
                            self._invalidate_auth()
                return "idle", f"Удалено задач: {summary['done']:,}. Ошибок удаления: {summary['errors']:,}."
            except BaseException:
                # Shutdown preserves a confirmed operation. A manual pause must
                # wait for an explicit Continue instead of restarting silently.
                operation["state"] = "running" if self._shutting_down else "paused"
                journal.set_operation(operation)
                self._deletion_pending = True
                with self._lock:
                    self._deletion = {**journal.summary(), "resume_available": True}
                raise
            finally:
                if api is not None:
                    api.close_thread()
                self._invalidate_stats()

    def jobs(self, status: str | None = None, limit: int = 50, offset: int = 0, sort: str = "asc") -> dict:
        if status in ("", "all"):
            status = None
        if status is not None and status not in (*STATES, "paused", "uploaded_unlimited", "uploaded_regular", "uploaded_unknown"):
            raise ValueError("Неизвестный фильтр задач.")
        if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0 or sort not in {"asc", "desc"}:
            raise ValueError("Некорректная страница списка задач.")
        if self._configuration is None:
            return {"jobs": [], "total": 0, "total_bytes": 0, "offset": offset, "limit": limit, "sort": sort}
        key = (status, limit, offset, sort)
        with self._stats_lock:
            cached = self._jobs_cache.get(key)
            if cached is None or (self._operation is not None and time.monotonic() - cached[0] >= 5):
                with self._queue() as queue:
                    rows = queue.list_jobs(status, limit=limit, offset=offset, summary=True, sort=sort)
                    totals = queue.jobs_summary(status)
                self._jobs_cache[key] = (time.monotonic(), rows, totals)
                self._jobs_cache.move_to_end(key)
                while len(self._jobs_cache) > 32:
                    self._jobs_cache.popitem(last=False)
            _, rows, totals = copy.deepcopy(self._jobs_cache[key])
        with self._lock:
            active = copy.deepcopy(self._files)
        fields = {"id", "root_id", "relative_path", "local_path", "remote_path", "status", "size", "attempts",
                  "last_error", "skip_reason", "next_attempt", "uploaded_at", "storage_mode", "created_at",
                  "paused", "remove_requested", "planned_mode", "planned_reason", "observed_mode",
                  "cloud_present", "cloud_verified", "cloud_path", "cloud_size", "cloud_observed_at", "cloud_deleted_at"}
        items = []
        for row in rows:
            item = {k: v for k, v in row.items() if k in fields}
            for name in ("paused", "remove_requested", "cloud_present", "cloud_verified"):
                item[name] = bool(row.get(name))
            content_url = f"/api/tasks/{row['id']}/content"
            item["download_url"] = content_url + "?download=1"
            item["preview_url"] = content_url + "?download=0"
            item["cloud_url"] = ("https://cloud.mail.ru/home" + quote(row["cloud_path"], safe="/")
                                 if row.get("cloud_present") and row.get("cloud_path") else None)
            item["progress"] = None
            current = active.get(row["id"])
            if row["status"] == "running" and current:
                total = max(0, row["size"])
                sent = min(total, max(0, current.get("sent", 0)))
                item["progress"] = {"sent": sent, "total": total,
                                    "percent": sent / total * 100 if total else 0,
                                    "stage": current.get("stage", "preparing")}
            items.append(item)
        return {"jobs": items, "total": totals["total"], "total_bytes": totals["total_bytes"],
                "offset": offset, "limit": limit, "sort": sort}

    def snapshot(self) -> dict:
        with self._lock:
            activity = copy.deepcopy(self._activity)
            quota = copy.deepcopy(self._quota_cache)
            if quota is not None:
                quota.update(stale=bool(self._quota_error) or time.time() - quota["updated_at"] > 120,
                             error=self._quota_error, refreshing=self._operation == "quota")
            activity["current_files"] = copy.deepcopy(list(self._files.values()))
            result = {"version": __version__, "configured": self._configuration is not None,
                      "authenticated": self._authenticated, "auth_required": self._auth_required,
                      "destination": None, "account": None, "sources": [],
                      "stats": {**{s: 0 for s in STATES}, "total": 0, "total_bytes": 0,
                                "uploaded_bytes": 0, "ready": 0, "permanent_failures": 0},
                      "activity": activity, "settings": dict(self._settings),
                      "quota": quota, "quota_error": self._quota_error,
                      "quota_refreshing": self._operation == "quota",
                      "quota_refresh_available": self._authenticated and self._operation is None
                          and not self._closed and not self._logout_requested and not self._auto_pending,
                      "task_controls_available": self._task_controls_available(),
                      "queue_clear_available": self._configuration is not None and self._operation is None
                          and not self._closed and not self._logout_requested and not self._deletion_pending,
                      "local_delete_available": local_delete_supported(),
                      "deletion": copy.deepcopy(self._deletion),
                      "last_scan": None, "events": copy.deepcopy(self._events),
                      "scheduler": self.scheduler.summary() if self.scheduler is not None else None}
            if self._configuration is not None:
                result.update(self._database_snapshot())
        stats = result["stats"]
        result["storage_plan"] = {key: {"count": stats.get(prefix + "_count", 0), "bytes": stats.get(prefix + "_bytes", 0)}
            for key, prefix in (("unlimited", "planned_unlimited"), ("regular", "planned_regular"),
                                ("uploaded", "uploaded_total"), ("uploaded_unlimited", "uploaded_unlimited"),
                                ("uploaded_regular", "uploaded_regular"), ("uploaded_unknown", "uploaded_unknown"))}
        return result

    def _launch(self, name: str, function: Callable[[], tuple[str, str]], *, upload_intent: bool = False) -> dict:
        with self._lock:
            self._ensure_idle()
            if name in {"upload", "scanning"}:
                self._ensure_no_deletion()
            self._stop = self._event_factory()
            self._pause_requested = False
            self._shutting_down = False
            self._operation = name
            self._files.clear()
            self._task_signals.clear()
            self._priority_jobs.clear()
            self._task_starts_closed = name != "upload"
            self._deletion_previews.clear()
            self._uploaded_in_run = 0
            self._invalidate_stats()
            self._resume_upload = upload_intent
            self._auto_pending = False
            self._activity.update(phase=name if name in {"login", "scanning", "quota", "deleting"} else "checking",
                                  message="Запускаем операцию…", last_error=None, started_at=time.time(),
                                  scan=None)
            try:
                self._persist_activity()
                self._thread = threading.Thread(target=self._execute, args=(function,), name="mailru-web-task", daemon=True)
                self._thread.start()
            except Exception:
                # Do not strand a controller as busy if no worker was started.
                self._operation = None
                self._thread = None
                self._task_starts_closed = True
                self._resume_upload = self._auto_pending = False
                self._activity.update(phase="paused", message="Не удалось начать операцию. Проверьте доступ к папке состояния и повторите запуск.")
                with contextlib.suppress(OSError, QueueError):
                    self._persist_activity()
                raise
        return {"ok": True, "started": name}

    def _execute(self, function: Callable[[], tuple[str, str]]) -> None:
        try:
            phase, message = function()
            with self._lock:
                if self._pause_requested:
                    raise Cancelled("Приостановлено.")
                if self._shutting_down and self._resume_upload:
                    with self._queue() as queue:
                        if self._resume_job_id is not None:
                            row = queue.get_job(self._resume_job_id, include_missing=True)
                            remaining = bool(row and row["present"] and not row["paused"] and not row["remove_requested"] and
                                             (row["status"] in {"pending", "running"} or
                                              (row["status"] == "failed" and row["next_attempt"] is not None)))
                        else:
                            remaining = queue.stats(root_ids=self._resume_root_ids)["remaining"]
                    if remaining:
                        raise Cancelled("Остановка процесса; сохранено намерение продолжить очередь.")
                self._resume_upload = False
                self._resume_job_id = None
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
                safe = str(error) if isinstance(error, (QueueError, AuthError, ProtocolError, TransportError, ValueError)) else f"Ошибка {type(error).__name__}; очередь сохранена."
                with self._lock:
                    self._resume_upload = False
                    if is_auth_failure(error):
                        self._invalidate_auth()
                self._phase("error", safe, error=safe)
        finally:
            with self._lock:
                self._invalidate_stats()
                self._files.clear()
                self._task_signals.clear()
                self._task_starts_closed = True
                if self._logout_requested:
                    try:
                        self._complete_logout()
                    except Exception:
                        safe = "Не удалось завершить выход из Mail.ru. Проверьте доступ к папке состояния и повторите выход."
                        self._activity.update(phase="error", message=safe, last_error=safe)
                        self._event(safe, "error")
                        LOGGER.error("phase=logout error=True")
                self._operation = None
                self._persist_activity()
                if self.scheduler is not None:
                    self.scheduler.wake()
                if self._closed:
                    self._release_server_lock()

    def _check_stop(self) -> None:
        if self._stop.is_set():
            raise Cancelled("Приостановлено.")

    def _new_api(self):
        api = self._api_factory(TokenManager(TokenStore(self.state_dir / "auth.dat")), stop=self._stop)
        return _QuotaObservedAPI(api, self._remember_quota)

    def _complete_logout(self) -> None:
        """Run only after the active task and its workers have drained.

        The durable request survives a crash. The queue's run lock prevents a
        separate CLI process from refreshing credentials while they are removed.
        Queue/account binding and completed jobs deliberately survive logout.
        """
        if self._configuration is not None:
            with self._queue() as queue, queue.exclusive_run():
                queue.set_metadata("last_auth", {"result": "logged_out", "at": time.time()})
                TokenStore(self.state_dir / "auth.dat").clear()
        else:
            TokenStore(self.state_dir / "auth.dat").clear()
        self._authenticated = False
        self._auth_required = False
        self._quota_cache = None
        self._quota_error = None
        self._logout_requested = False
        self._resume_upload = False
        self._auto_pending = False
        self._phase("paused", "Вы вышли из Mail.ru. Очередь сохранена; для продолжения войдите в тот же аккаунт.")

    def logout(self) -> dict:
        with self._lock:
            if self._closed:
                raise QueueError("Приложение останавливается.")
            self._logout_requested = True
            self._resume_upload = False
            self._auto_pending = False
            self._pause_requested = True
            self._stop.set()
            self._activity.update(phase="logout", message="Завершаем текущую операцию и выходим из Mail.ru…",
                                  last_error=None)
            # Persist before signalling completion, including when a login or
            # token refresh is currently in flight and may still save tokens.
            self._persist_activity()
            if self._operation is None:
                self._operation = "logout"
                self._thread = threading.Thread(target=self._execute,
                    args=(lambda: ("paused", "Выход из Mail.ru."),), name="mailru-web-logout", daemon=True)
                self._thread.start()
        return {"ok": True, "started": "logout"}

    def login(self, email: str, password: str) -> dict:
        if not isinstance(email, str) or not email.strip() or len(email) > 320:
            raise ValueError("Укажите адрес аккаунта Mail.")
        if not isinstance(password, str) or not password or len(password) > 1024:
            raise ValueError("Введите пароль внешнего приложения.")
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
                    try:
                        self._remember_quota(api.quota())
                    except (ProtocolError, TransportError) as error:
                        if is_auth_failure(error):
                            # New credentials remain in memory until all
                            # authenticated checks accept them. A transient
                            # quota outage may be ignored; rejection may not.
                            raise AuthError("Сервер отклонил авторизацию при проверке квоты; вход не сохранён.",
                                            code="auth_rejected") from None
                        with self._lock:
                            self._quota_error = str(error)
                    self._check_stop()
                    # A quota read may refresh the in-memory token too.
                    TokenStore(self.state_dir / "auth.dat").save(memory.value)
                    queue.set_metadata("last_auth", {"result": "success", "at": time.time(), "stage": "user_info", "client_id": tokens.client_id})
                    with self._lock:
                        self._authenticated = True
                        self._auth_required = False
                except AuthError as error:
                    queue.set_metadata("last_auth", {"result": "failed", "at": time.time(), **error.safe_diagnostics()})
                    raise
                finally:
                    password = ""
                    if api is not None:
                        api.close_thread()
            return "idle", "Вход сохранён. Повторно вводить пароль при обычном запуске не потребуется."
        with self._lock:
            self._ensure_idle()
            self._ensure_configuration()
            return self._launch("login", perform)

    def _scan_queue(self, queue: Queue, root_ids=None) -> None:
        self._phase("scanning", "Сканируем папки с фото и видео…")
        def on_progress(result):
            with self._lock:
                self._activity["scan"] = dataclasses.asdict(result)
                self._activity["message"] = f"Найдено файлов: {result.seen:,}."
        with queue.exclusive_run():
            queue.recover_running()
            for source in queue.roots():
                if root_ids is None or source.id in root_ids:
                    self._allowed_path(source.path)
            extensions = queue.discovery_extensions()
            queue.set_metadata("last_scan", {"complete": False, "reason": "scan_started", "root_ids": root_ids})
            result = queue.scan(extensions, stop=self._stop, progress=on_progress, root_ids=root_ids)
            queue.set_metadata("last_scan", {**dataclasses.asdict(result), "root_ids": root_ids,
                                             "occurrence_id": self._resume_occurrence_id})
            queue.set_metadata("extensions", sorted(extensions))
            queue.apply_regular_policy(upload=self._settings["regular_policy"] == "upload", root_ids=root_ids)
        self._check_stop()
        if not result.complete:
            raise QueueError(f"Сканирование завершилось с ошибками: {result.error_count}. Проверьте доступ к папкам.")
        LOGGER.info("scan_complete seen=%d added=%d changed=%d",
                    result.seen, result.added, result.changed)
        self._reconcile_queue(queue, root_ids=root_ids)
        if self._authenticated:
            api = None
            try:
                api = self._new_api()
                api.require_account(queue.account_id)
                api.quota()
            except (ProtocolError, AuthError, TransportError) as error:
                if is_auth_failure(error):
                    self._invalidate_auth()
                self._event("Сканирование завершено, но свежая квота недоступна. Обновите её перед обычной загрузкой.", "warning")
            finally:
                if api is not None:
                    api.close_thread()
        self._invalidate_stats()

    def _reconcile_queue(self, queue: Queue, root_ids=None) -> None:
        if not self._settings["remote_reconcile"]:
            return
        if not self._authenticated:
            self._event("Локальное сканирование завершено. Для сверки с Облаком войдите в Mail и пересканируйте папки.", "warning")
            return
        self._phase("reconciling", "Сверяем найденные задачи с файлами в Облаке…")
        api = None
        def progress(report):
            with self._lock:
                self._activity["reconcile"] = {key: report.get(key) for key in
                    ("checked", "matched", "errors", "cancelled", "auth_required")}
                self._activity["message"] = (f"Проверено задач: {report.get('checked', 0):,}. "
                                              f"Уже в Облаке: {report.get('matched', 0):,}.")
        try:
            with queue.exclusive_run():
                api = self._new_api()
                account = api.require_account(queue.account_id)
                queue.bind_account(account.email)
                # Optional by construction: disabled installations do not even
                # import the reconciliation implementation.
                from .reconcile import reconcile
                report = reconcile(queue, api, stop=self._stop, progress=progress, root_ids=root_ids)
                queue.set_metadata("last_reconcile", {"at": time.time(), **report})
                progress(report)
                if report.get("auth_required"):
                    self._invalidate_auth()
                if report.get("errors"):
                    self._event("Сверка с Облаком завершена с замечаниями. Локальная очередь сохранена.", "warning")
                else:
                    self._event(f"Сверка завершена. Уже в Облаке: {report.get('matched', 0):,}.")
        except Cancelled:
            raise
        except (QueueError, sqlite3.Error):
            raise
        except Exception as error:
            if is_auth_failure(error):
                self._invalidate_auth()
            self._event(f"Дополнительная сверка не завершена ({type(error).__name__}). Локальное сканирование сохранено.", "warning")
            queue.set_metadata("last_reconcile", {"at": time.time(), "errors": 1, "aborted": type(error).__name__})
        finally:
            if api is not None:
                api.close_thread()
            self._invalidate_stats()
        self._check_stop()

    def scan(self) -> dict:
        if self._configuration is None:
            raise ValueError("Добавьте хотя бы одну исходную папку с фото и видео.")
        def perform():
            with self._queue() as queue:
                self._scan_queue(queue)
                total = queue.stats()["total"]
            return "idle", f"Сканирование завершено. Задач в очереди: {total:,}."
        return self._launch("scanning", perform)

    def _emit(self, event: str, data: dict) -> None:
        with self._lock:
            if event == "progress":
                # The runner already computed this summary for CLI progress.
                with self._stats_lock:
                    self._stats_cache = (time.monotonic(), copy.deepcopy(data))
            elif event == "file_started":
                self._files[data["job_id"]] = {"id": data["job_id"], "path": data["path"], "size": data["size"],
                                               "sent": 0, "stage": "preparing", "storage_mode": data.get("storage_mode")}
            elif event == "regular_files_skipped":
                self._event(f"Пропущено задач, требующих квоты: {data['count']:,}. Изменить правило можно в настройках.")
            elif event == "upload_progress":
                item = self._files.get(data["job_id"])
                if item:
                    item["sent"] = min(item["size"], item["sent"] + max(0, data["bytes"]))
                    item["stage"] = "uploading"
            elif event == "file_finished":
                self._files.pop(data["job_id"], None)
                self._invalidate_stats()
            elif event == "uploaded":
                self._uploaded_in_run += 1
                if self._uploaded_in_run % 100 == 0:
                    self._event(f"За текущий запуск загружено файлов: {self._uploaded_in_run:,}.")
                    LOGGER.info("uploaded_in_run=%d", self._uploaded_in_run)
            elif event in {"file_error", "file_changed", "skipped", "quota_warning"}:
                self._event(str(data.get("reason", "Ошибка файла.")), "warning")
            elif event == "remote_error":
                self._event(f"Ошибка сервера: HTTP {data.get('status')}, {data.get('code')}.", "warning")
            elif event == "retry":
                self._event(f"Файл {data.get('job_id')}: соединение прервано, повторная попытка отложена.", "warning")

    @staticmethod
    def _completion(stats: dict) -> tuple[str, str]:
        if not stats.get("total"):
            return "idle", "Очередь пуста. Чтобы найти файлы снова, нажмите «Пересканировать папки»."
        if stats.get("paused"):
            return "paused", (f"Доступные задачи обработаны. На индивидуальной паузе: {stats['paused']:,}. "
                              "Продолжите нужные задачи в очереди.")
        failed, skipped, existing = stats.get("failed", 0), stats.get("skipped", 0), stats.get("existing", 0)
        if failed or skipped:
            return "completed", (f"Обработка завершена. Уже в Облаке: {existing:,}; ошибок: {failed:,}; пропущено: {skipped - existing:,}. "
                                 "Подробности и повтор — в очереди задач.")
        return "completed", "Все фото и видео загружены и проверены."

    def _pipeline(self, job_id: int | None = None, *, root_ids=None, scan_first=False) -> tuple[str, str]:
        api = None
        with self._queue() as queue, queue.exclusive_run():
            queue.recover_running()
            if not queue.roots():
                raise ValueError("Добавьте хотя бы одну папку с фото и видео.")
            last_scan = queue.get_metadata("last_scan", {})
            scan_scope = last_scan.get("root_ids") if isinstance(last_scan, dict) else None
            scope_covered = scan_scope is None or (root_ids is not None and set(root_ids).issubset(set(scan_scope)))
            scheduled_scan_missing = (self._resume_occurrence_id is not None and
                                      (not isinstance(last_scan, dict) or last_scan.get("occurrence_id") != self._resume_occurrence_id))
            if scan_first or scheduled_scan_missing or not isinstance(last_scan, dict) or last_scan.get("complete") is not True or not scope_covered:
                self._scan_queue(queue, root_ids=root_ids)
            self._check_stop()
            if not self._authenticated:
                raise AuthError("Для продолжения требуется вход в Mail.ru.", code="auth_rejected")
            queue.apply_regular_policy(upload=self._settings["regular_policy"] == "upload", root_ids=root_ids)
            with self._lock:
                stats = queue.stats(root_ids=root_ids)
                if not stats["remaining"] and not self._priority_jobs:
                    self._task_starts_closed = True
                    return self._completion(stats)
            api = self._new_api()
            try:
                self._phase("uploading", "Выполняем выбранную задачу…" if job_id else "Загружаем фото и видео…")
                selected = job_id
                budget = QuotaBudget()
                while True:
                    result = run(queue, api, workers=self._settings["workers"], emit=self._emit,
                                 file_progress=True, task_scope=self._task_scope, job_id=selected,
                                 priority_job=self._next_priority_job, root_ids=root_ids, quota_budget=budget,
                                 regular_policy=self._settings["regular_policy"],
                                 unexpected_unlimited_action=self._settings["unexpected_unlimited_action"])
                    if result.get("auth_required"):
                        self._invalidate_auth()
                    if result.get("stopped") and result.get("errors"):
                        if result.get("auth_required"):
                            raise AuthError("Для продолжения требуется вход в Mail.ru.", code="auth_rejected")
                        raise ProtocolError(result["errors"][0])
                    with self._lock:
                        if self._pause_requested or self._shutting_down or result.get("stopped"):
                            raise Cancelled("Приостановлено; очередь сохранена.")
                        # Workers can finish before the final quota request.
                        # Every Start accepted in that interval must be drained
                        # before atomically closing admission to this operation.
                        if self._priority_jobs:
                            selected = self._priority_jobs.popleft()
                        else:
                            self._task_starts_closed = True
                            break
                if job_id is not None:
                    return "idle", "Выбранная задача обработана. Остальные задачи ожидают общего запуска."
                return self._completion(queue.stats(root_ids=root_ids))
            finally:
                with self._lock:
                    self._task_starts_closed = True
                api.close_thread()

    def start(self, *, job_id: int | None = None, root_ids=None, occurrence_id=None) -> dict:
        if self._deletion_pending:
            with DeletionJournal(self.state_dir / "deletions.sqlite3") as journal:
                operation = journal.operation()
            if operation and operation.get("delete_cloud") and not self._authenticated:
                raise ValueError("Для продолжения удаления из Облака войдите в Mail.")
            return self._launch("deleting", self._perform_deletion)
        if occurrence_id is None and job_id is None and root_ids is None and self._resume_occurrence_id:
            prior = self.scheduler.occurrence(self._resume_occurrence_id)
            if prior and prior["status"] in {"paused", "running"}:
                occurrence_id = prior["id"]
        if occurrence_id is not None:
            if not self._authenticated:
                raise ValueError("Для продолжения планировщика войдите в аккаунт Mail.")
            item = self.scheduler.occurrence(occurrence_id)
            if item is None:
                raise ValueError("Сохранённый запуск планировщика не найден.")
            with self._lock:
                if item["status"] == "running" and self._operation is None:
                    # A failed disk write before thread.start may also prevent
                    # the paused checkpoint. Explicit Continue retries it once
                    # storage is writable again; it never replaces a live run.
                    self.scheduler.finish(item["id"], "paused", "Повторный запуск после незавершённого старта.")
                    item = self.scheduler.occurrence(item["id"])
            if not self._start_scheduled(item, scan_first=False):
                raise QueueBusyError("Дождитесь завершения текущей операции.")
            return {"ok": True, "started": "scheduled_upload"}
        if self._configuration is None:
            raise ValueError("Добавьте хотя бы одну исходную папку с фото и видео.")
        if not self._authenticated:
            raise ValueError("Сначала войдите в аккаунт Mail.")
        with self._lock:
            self._ensure_idle()
            self._resume_job_id = job_id
            self._resume_root_ids = root_ids
            self._resume_occurrence_id = None
            return self._launch("upload", lambda: self._pipeline(job_id=job_id, root_ids=root_ids), upload_intent=True)

    def pause(self) -> dict:
        with self._lock:
            if self._logout_requested:
                return {"ok": True}
            self._resume_upload = False
            self._auto_pending = False
            self._pause_requested = True
            self._stop.set()
            if self.scheduler is not None:
                self.scheduler.cancel_queued("Общая пауза: ожидавший запуск отменён. Будущие расписания сохранены.")
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
        try:
            return self._resume_if_requested()
        finally:
            if self.scheduler is not None:
                self.scheduler.start()

    def _resume_if_requested(self) -> bool:
        """Call after the HTTP listener is ready; no spontaneous fresh uploads."""
        with self._lock:
            requested = self._auto_pending and self._settings["auto_resume"] and not self._closed
        if self._auto_deletion:
            self._auto_deletion = False
            try:
                self.start()
            except (ValueError, QueueError) as error:
                self._phase("paused", str(error), error=str(error))
                return False
            return True
        if requested:
            try:
                self.start(job_id=self._resume_job_id, root_ids=self._resume_root_ids, occurrence_id=self._resume_occurrence_id)
            except (ValueError, QueueError) as error:
                with self._lock:
                    self._resume_upload = False
                    self._auto_pending = False
                self._phase("error", str(error), error=str(error))
                return False
            return True
        return False

    def schedules(self):
        return self.scheduler.list()

    def save_schedule(self, data):
        with self._lock:
            if self._closed:
                raise QueueBusyError("Приложение останавливается.")
            roots = self._database_snapshot()["sources"] if self._configuration is not None else []
            result = self.scheduler.save(data, [root["id"] for root in roots])
            self._event("Сохранён планировщик: " + result["name"] + ".")
            return self.scheduler.list()

    def toggle_schedule(self, identifier, enabled):
        with self._lock:
            self.scheduler.toggle(self._source_id(identifier), enabled)
            return self.scheduler.list()

    def remove_schedule(self, identifier):
        with self._lock:
            self.scheduler.remove(self._source_id(identifier))
            return self.scheduler.list()

    def _start_scheduled(self, occurrence, *, scan_first=True):
        with self._lock:
            if (self._closed or self._shutting_down or self._operation is not None
                    or self._deletion_pending or self._logout_requested or not self._authenticated):
                return False
            current = self.scheduler.occurrence(occurrence["id"])
            if current is None or current["status"] not in {"queued", "paused"}:
                return False
            item = self.scheduler.begin(occurrence["id"])
            self._resume_job_id = None
            self._resume_root_ids = list(item["source_ids"])
            self._resume_occurrence_id = item["id"]
            try:
                self._event("Запущен планировщик: " + item["name"] + ".")
                self._launch("upload", lambda: self._scheduled_pipeline(item, scan_first=scan_first), upload_intent=True)
            except Exception:
                # begin() is durable before any work. A launch failure must be
                # resumable now, without needing a process restart to recover it.
                self._resume_upload = self._auto_pending = False
                self._activity.update(phase="paused", message="Планировщик не смог начать работу. Проверьте папку состояния и нажмите «Продолжить».")
                with contextlib.suppress(Exception):
                    self.scheduler.finish(item["id"], "paused", "Не удалось начать работу. Запуск можно продолжить.")
                with contextlib.suppress(OSError, QueueError):
                    self._persist_activity()
                raise
            return True

    def _scheduled_pipeline(self, occurrence, *, scan_first):
        try:
            result = self._pipeline(root_ids=occurrence["source_ids"], scan_first=scan_first)
        except Cancelled:
            status = "running" if self._shutting_down else "paused"
            self.scheduler.finish(occurrence["id"], status, "Выполнение приостановлено; очередь сохранена.")
            raise
        except Exception as error:
            safe = str(error) if isinstance(error, (ValueError, QueueError, ProtocolError, AuthError, TransportError)) else type(error).__name__
            self.scheduler.finish(occurrence["id"], "error", safe)
            with self._lock:
                self._resume_upload = False
                self._resume_occurrence_id = None
                self._resume_root_ids = None
            raise
        else:
            self.scheduler.finish(occurrence["id"], "complete", result[1])
            with self._lock:
                self._resume_upload = False
                self._resume_occurrence_id = None
                self._resume_root_ids = None
            return result

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
            if self.scheduler is not None:
                self.scheduler.close(wait=False)
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
        if self.scheduler is not None:
            self.scheduler.close()

"""Persistent, independently controlled uploads with durable storage receipts."""

from __future__ import annotations

import concurrent.futures
import contextlib
import random
import re
import sqlite3
import threading
import time
from typing import Any, Callable, ContextManager

from .api import (PROTOCOL_VERSION, CloudAPI, FileIdentity, FileVerificationError,
                  ProtocolError, RemoteConflict, RemoteError, SourceChanged,
                  UnlimitedNotConfirmed, UnsupportedPhoto, is_auth_failure)
from .auth import AuthError
from .queue import FileChangedError, Job, Queue, QueueError, StaleClaimError
from .transport import Cancelled, SourceReadError, TaskCancelled, TransportError


TaskScope = Callable[[Job], ContextManager[threading.Event | None]]


class _TaskStop:
    """A monotonic cancellation signal for one claim, never a global stop.

    The UI event provides prompt in-process notifications. A bounded-frequency
    durable read also covers restarts, another controller and registration races.
    Only the owning worker reads its SQLite connection.
    """
    def __init__(self, global_stop, signal, read_control):
        self.global_stop, self.signal, self.read_control = global_stop, signal, read_control
        self._next_read = 0.0
        self._cancelled = False

    def _task_cancelled(self, *, force: bool = False) -> bool:
        if self._cancelled:
            return True
        if self.signal is not None and self.signal.is_set():
            self._cancelled = True
        now = time.monotonic()
        if not self._cancelled and (force or now >= self._next_read):
            self._next_read = now + 0.25
            self._cancelled = self.read_control() in {"pause", "remove"}
        return self._cancelled

    def is_set(self) -> bool:
        return self.global_stop.is_set() or self._task_cancelled()

    def check(self, *, force: bool = False) -> None:
        if self.global_stop.is_set():
            raise Cancelled("Загрузка остановлена.")
        if self._task_cancelled(force=force):
            raise TaskCancelled("Задача остановлена по запросу.")

    def checkpoint(self) -> None:
        self.check(force=True)

    def wait(self, timeout: float | None = None) -> bool:
        remaining = timeout
        while remaining is None or remaining > 0:
            if self.is_set():
                return True
            interval = 0.25 if remaining is None else min(0.25, remaining)
            if self.global_stop.wait(interval):
                return True
            if remaining is not None:
                remaining -= interval
        return self.is_set()


@contextlib.contextmanager
def _job_scope(queue: Queue, api: CloudAPI, job: Job, task_scope: TaskScope | None):
    registration = task_scope(job) if task_scope is not None else contextlib.nullcontext(None)
    with registration as signal:
        read_control = getattr(queue, "task_control", None)
        stop = _TaskStop(api.stop, signal, lambda: read_control(job) if callable(read_control) else None)
        # The action might have arrived before registration exposed the event.
        stop.check(force=True)
        scope = getattr(api, "task_scope", None)
        with scope(stop) if callable(scope) else contextlib.nullcontext():
            try:
                yield
            except (Cancelled, TransportError):
                # A pending network call may finish with a timeout/reset after
                # cancellation. Keep this local unless the global stop is set.
                stop.check(force=True)
                raise


def process_job(queue: Queue, api: CloudAPI, job: Job, *, emit=None,
                task_scope: TaskScope | None = None,
                unexpected_unlimited_action: str = "trash", quota_budget=None) -> dict[str, Any]:
    previous = max(0, getattr(job, "progress_bytes", 0))
    transferred = 0
    saved = previous
    last_save = time.monotonic()

    def progress(size):
        nonlocal transferred, saved, last_save
        transferred += max(0, size)
        known = min(job.size, max(previous, transferred))
        now = time.monotonic()
        if known > saved and now - last_save >= 2:
            saved = queue.record_progress(job, known)
            last_save = now
        if emit is not None:
            emit("upload_progress", {"job_id": job.id, "bytes": size, "sent": known})

    if emit is not None:
        emit("file_started", {"job_id": job.id, "path": job.remote_path,
                              "size": job.size, "storage_mode": job.planned_mode, "sent": previous})
    try:
        with _job_scope(queue, api, job, task_scope):
            return _process_job(queue, api, job, progress=progress,
                unexpected_unlimited_action=unexpected_unlimited_action, quota_budget=quota_budget)
    finally:
        try:
            known = min(job.size, max(previous, transferred))
            if known > saved:
                # Success and source replacement already finalized this claim;
                # a late progress write must never resurrect it.
                with contextlib.suppress(StaleClaimError):
                    queue.record_progress(job, known)
        finally:
            if quota_budget is not None and quota_budget.has_reservation(job.claim_token):
                try:
                    uncertain = bool(queue.get_commit_intent(job))
                except (QueueError, sqlite3.Error):
                    uncertain = True
                quota_budget.finish(job.claim_token, uncertain=uncertain)
            if quota_budget is not None:
                quota_budget.close_claim(job.claim_token)
            if emit is not None:
                emit("file_finished", {"job_id": job.id})


def _process_job(queue: Queue, api: CloudAPI, job: Job, *, progress=None,
                 unexpected_unlimited_action: str = "trash", quota_budget=None) -> dict[str, Any]:
    mode = job.planned_mode
    if mode not in {"unlimited", "regular"}:
        raise ProtocolError("Неизвестный план хранения задачи. Пересканируйте источник.")
    expected = FileIdentity(job.size, job.mtime_ns, job.device, job.inode)
    prior = queue.get_commit_intent(job)
    if quota_budget is not None and mode == "regular" and prior is None:
        quota_budget.precheck(job.size)
    if prior and prior.get("storage_mode", "unlimited") != mode:
        raise ProtocolError("Сохранённая попытка относится к другому режиму хранения; проверьте задачу.")

    def observe(value):
        if not isinstance(value, dict) or value.get("mode") not in {"unlimited", "regular", "unknown"}:
            raise ProtocolError("Некорректное подтверждение результата хранения.")
        queue.record_cloud_observation(job, mode=value["mode"], remote_hash=value["hash"],
                                       remote_id=value.get("nodeid"), metadata=value)
        if quota_budget is not None and value["mode"] == "regular" and value.get("commit_accepted") is True:
            quota_budget.observed(job.claim_token, value["size"])

    upload = api.upload_photo if mode == "unlimited" else api.upload_regular
    confirm = queue.confirm_unlimited_commit if mode == "unlimited" else queue.confirm_regular_commit
    try:
        result = upload(job.local_path, job.remote_path, expected=expected, prior_intent=prior,
                        save_intent=lambda digest: queue.save_commit_intent(job, digest, storage_mode=mode),
                        save_confirmation=lambda digest: confirm(job, digest), save_observation=observe,
                        is_retried=job.attempts > 1,
                        **({"reserve_quota": lambda size, available: quota_budget.reserve(job.claim_token, size, available)}
                           if mode == "regular" and quota_budget is not None else {}),
                        **({"progress": progress} if progress is not None else {}))
    except UnlimitedNotConfirmed as error:
        # The observation was committed before the API's final stat. A crash or
        # failed cleanup must never erase a file that is known to consume quota.
        observation = getattr(error, "observation", None)
        row = queue.get_job(job.id, include_missing=True)
        if (unexpected_unlimited_action == "trash" and row and row.get("cloud_present")
                and row.get("observed_mode") == "regular"):
            try:
                api.trash_file(row["cloud_path"], expected_hash=row["cloud_hash"],
                               expected_size=row["cloud_size"])
                if not queue.mark_cloud_deleted(job.id, expected_hash=row["cloud_hash"],
                                                expected_path=row["cloud_path"]):
                    raise QueueError("Подтверждение удаления не совпало с сохранённым объектом.")
            except (Cancelled, TaskCancelled, QueueError, sqlite3.Error):
                raise
            except Exception as cleanup:
                if is_auth_failure(cleanup):
                    raise
                detail = str(cleanup) if isinstance(cleanup, (ProtocolError, TransportError)) else type(cleanup).__name__
                raise UnlimitedNotConfirmed(
                    "Сервер не подтвердил безлимит. Файл оставлен в Облаке: перенос в корзину не завершён. " + detail,
                    observation=observation) from None
            raise UnlimitedNotConfirmed(
                "Сервер не подтвердил безлимит. Файл перемещён в корзину Облака; квота может обновиться позже.",
                observation=observation) from None
        raise
    if (not isinstance(result, dict) or result.get("stat_matched") is not True
            or not isinstance(result.get("hash"), str)
            or not re.fullmatch(r"[0-9A-Fa-f]{40}", result["hash"])
            or type(result.get("size")) is not int or result["size"] != job.size
            or result.get("path") != job.remote_path
            or result.get("storage_mode", "unlimited") != mode):
        raise FileVerificationError("Не подтверждены путь, хеш, размер и режим хранения файла.")
    fresh = result.get("commit_accepted") is True and result.get("recovered_commit") is False
    recovered = result.get("commit_accepted") is False and result.get("recovered_commit") is True
    if mode == "unlimited":
        proven = (result.get("is_unlim_upload") is True
                  and ((fresh and result.get("unlimited_evidence") == "commit_response")
                       or (recovered and result.get("unlimited_evidence") == "saved_commit_response")))
    else:
        proven = ((fresh and result.get("regular_evidence") == "metadata_commit_response")
                  or (recovered and result.get("regular_evidence") == "saved_commit_response"))
    if not proven:
        raise FileVerificationError("Сервер не подтвердил создание файла в выбранном режиме хранения.")
    confirmation = queue.get_commit_intent(job)
    if (not confirmation or confirmation.get(mode + "_confirmed") is not True
            or confirmation.get("storage_mode", "unlimited") != mode
            or not isinstance(confirmation.get("cloud_hash"), str)
            or confirmation["cloud_hash"].casefold() != result["hash"].casefold()):
        raise ProtocolError("Подтверждение создания не сохранено для этой задачи; завершение не отмечено.")
    result = {**result, "storage_mode": mode}
    queue.mark_uploaded(job, remote_hash=result["hash"], remote_id=result.get("nodeid"),
                        verified_metadata={"protocol": PROTOCOL_VERSION, **result,
                            mode + "_confirmed_at": confirmation.get(mode + "_confirmed_at"),
                            "commit_intent_created_at": confirmation.get("created_at")})
    return {"job_id": job.id, **result}


def require_scanned(queue: Queue, root_ids=None) -> None:
    scan = queue.get_metadata("last_scan", {})
    scope = scan.get("root_ids") if isinstance(scan, dict) else None
    covered = scope is None or (root_ids is not None and set(root_ids).issubset(set(scope)))
    if not isinstance(scan, dict) or scan.get("complete") is not True or not covered:
        raise QueueError("Сначала завершите сканирование выбранных исходных папок.")


class QuotaBudget:
    """Conservative byte reservations shared by all ordinary-upload workers.

    Per-file account/quota checks remain in CloudAPI. Reservations prevent two
    workers from each spending the same free bytes. A fresh known-quota AUTO
    commit consumes this budget too, even if moved to the quota-bearing trash.
    """
    def __init__(self, quota=None):
        self._lock = threading.Lock()
        self._initial = (max(0, quota["total_bytes"] - quota["combined_used_bytes"])
                         if isinstance(quota, dict) else None)
        self._spent = 0
        self._reserved = {}
        self._observed = set()

    def prime(self, quota):
        """Initialize once; extra explicitly started tasks share earlier spending."""
        if isinstance(quota, dict):
            with self._lock:
                if self._initial is None:
                    self._initial = max(0, quota["total_bytes"] - quota["combined_used_bytes"])

    def precheck(self, size):
        with self._lock:
            if self._initial is None:
                return
            free = max(0, self._initial - self._spent - sum(self._reserved.values()))
            if size > free:
                raise ProtocolError(f"Недостаточно места в квоте: файл {size:,} байт, доступно с учётом текущих загрузок {free:,} байт. Передача не начата.")

    def exhausted(self):
        with self._lock:
            self._initial = self._spent

    def reserve(self, token, size, available):
        with self._lock:
            if token in self._reserved:
                return
            if self._initial is None:
                self._initial = max(0, available)
            free = max(0, min(available, self._initial - self._spent) - sum(self._reserved.values()))
            if size > free:
                raise ProtocolError(f"Недостаточно места в квоте: файл {size:,} байт, доступно с учётом текущих загрузок {free:,} байт. Передача не начата.")
            self._reserved[token] = size

    def observed(self, token, size):
        with self._lock:
            if token not in self._observed:
                self._spent += size
                self._observed.add(token)
            self._reserved.pop(token, None)

    def has_reservation(self, token):
        with self._lock:
            return token in self._reserved

    def close_claim(self, token):
        with self._lock:
            self._observed.discard(token)

    def finish(self, token, *, uncertain=False):
        with self._lock:
            size = self._reserved.pop(token, 0)
            if uncertain and token not in self._observed:
                self._spent += size
                self._observed.add(token)


class QuotaTelemetry:
    """Refresh real quota during work; a change is not a reason to stop a batch."""
    def __init__(self, api, account, emit, *, seconds=60, every_files=100):
        self.api, self.account, self.emit = api, account, emit
        self.seconds, self.every_files = seconds, every_files
        self._lock = threading.Lock()
        self._last, self._completed = time.monotonic(), 0
        self.last_quota = None

    def completed(self):
        with self._lock:
            self._completed += 1

    def check(self, *, force=False):
        with self._lock:
            if not force and self._completed < self.every_files and time.monotonic() - self._last < self.seconds:
                return
            self._last, self._completed = time.monotonic(), 0
            try:
                self.last_quota = self.api.quota()
            except (AuthError, ProtocolError, TransportError) as error:
                if is_auth_failure(error):
                    raise
                self.emit("quota_warning", {"reason": "Не удалось обновить квоту; задачи продолжаются."})


def run(queue: Queue, api: CloudAPI, *, workers: int = 4, max_attempts: int = 5,
        emit: Callable[[str, dict], None] | None = None, file_progress: bool = False,
        regular_policy: str = "skip", unexpected_unlimited_action: str = "trash",
        job_id: int | None = None, root_ids=None, task_scope: TaskScope | None = None,
        priority_job: Callable[[], int | None] | None = None,
        quota_budget: QuotaBudget | None = None) -> dict[str, Any]:
    if not 1 <= workers <= 16 or not 1 <= max_attempts <= 20:
        raise ValueError("workers must be 1..16, max_attempts 1..20")
    if regular_policy not in {"skip", "upload"} or unexpected_unlimited_action not in {"trash", "keep"}:
        raise ValueError("Некорректная политика хранения.")
    if job_id is not None and (type(job_id) is not int or job_id <= 0):
        raise ValueError("Некорректный номер задачи.")
    emit = emit or (lambda event, data: None)
    started = time.time()
    errors: list[str] = []
    error_lock = threading.Lock()
    auth_required = False

    def stop_with(error):
        nonlocal auth_required
        with error_lock:
            auth_required = auth_required or is_auth_failure(error)
            if len(errors) < 100:
                errors.append(str(error))
        api.stop.set()

    with queue.exclusive_run():
        queue.recover_running()
        require_scanned(queue, root_ids=root_ids)
        policy = queue.apply_regular_policy(upload=regular_policy == "upload", root_ids=root_ids)
        if policy.get("skipped"):
            emit("regular_files_skipped", {"count": policy["skipped"]})
        account = api.require_account(queue.account_id)
        queue.bind_account(account.email)
        telemetry = QuotaTelemetry(api, account.email, emit)
        telemetry.check(force=True)
        budget = quota_budget if quota_budget is not None else QuotaBudget()
        budget.prime(telemetry.last_quota)

        def worker():
            local = Queue(queue.path, destination=queue.destination, account_id=account.email)
            try:
                while not api.stop.is_set():
                    try:
                        telemetry.check()
                    except Cancelled:
                        api.stop.set()
                        return
                    except (AuthError, ProtocolError) as error:
                        stop_with(error)
                        return
                    selected = job_id
                    if selected is None and priority_job is not None:
                        selected = priority_job()
                    mode = "unlimited" if regular_policy == "skip" else None
                    job = local.claim(max_attempts=max_attempts, storage_mode=mode,
                                      stop=api.stop, job_id=selected, root_ids=root_ids)
                    if job is None:
                        if selected is not None and job_id is None:
                            continue  # An already claimed/paused priority ID does not stop other tasks.
                        if api.stop.is_set() or job_id is not None:
                            return
                        next_retry = local.stats(storage_mode=mode, root_ids=root_ids).get("next_retry_at")
                        if next_retry is None:
                            return
                        api.stop.wait(max(0.1, min(10, next_retry - time.time())))
                        continue
                    try:
                        result = process_job(local, api, job, emit=emit if file_progress else None,
                            task_scope=task_scope, unexpected_unlimited_action=unexpected_unlimited_action, quota_budget=budget)
                        telemetry.completed()
                        emit("uploaded", {"job_id": job.id, "path": job.remote_path, "size": job.size,
                            "storage_mode": result["storage_mode"], "recovered_commit": result["recovered_commit"]})
                    except TaskCancelled:
                        local.release(job, "Task paused or removed; preserve any remote commit")
                        emit("task_cancelled", {"job_id": job.id})
                    except Cancelled:
                        local.release(job, "Interrupted; safe to resume")
                        api.stop.set()
                        return
                    except FileChangedError as error:
                        emit("file_changed", {"job_id": job.id, "reason": str(error)})
                    except UnsupportedPhoto as error:
                        local.mark_skipped(job, str(error))
                        emit("skipped", {"job_id": job.id, "reason": str(error)})
                    except RemoteError as error:
                        if error.code == "CONDITION/OVERQUOTA":
                            budget.exhausted()
                        if is_auth_failure(error):
                            stop_with(error)
                        delay = (max(error.retry_after or 0, min(900, 2 ** job.attempts + random.random() * 3))
                                 if error.retryable and job.attempts < max_attempts and not is_auth_failure(error) else None)
                        local.mark_failed(job, str(error), retry_delay=delay)
                        emit("remote_error", {"job_id": job.id, "status": error.status, "code": error.code})
                    except TransportError as error:
                        if api.stop.is_set():
                            local.release(job, "Network request interrupted by pause/shutdown")
                            return
                        delay = min(900, 2 ** job.attempts + random.random() * 3) if job.attempts < max_attempts else None
                        local.mark_failed(job, str(error), retry_delay=delay)
                        emit("retry", {"job_id": job.id, "retry_in": delay})
                    except (ProtocolError, AuthError, SourceReadError) as error:
                        if is_auth_failure(error):
                            stop_with(error)
                        local.mark_failed(job, str(error), retry_delay=None)
                        emit("file_error", {"job_id": job.id, "reason": str(error)})
                    except (QueueError, sqlite3.Error) as error:
                        stop_with(ProtocolError(f"Ошибка сохранения очереди: {type(error).__name__}. Загрузка остановлена."))
                    except Exception as error:
                        # Unknown file failures are isolated too. Never log arbitrary
                        # exception payloads, network bodies or credentials.
                        message = f"Локальная ошибка задачи: {type(error).__name__}."
                        local.mark_failed(job, message, retry_delay=None)
                        emit("file_error", {"job_id": job.id, "reason": message})
                    if job_id is not None:
                        return  # Exactly one selected task; never drain unrelated work.
            finally:
                local.close()
                api.close_thread()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1 if job_id else workers,
                                                          thread_name_prefix="media-upload")
        futures = [executor.submit(worker) for _ in range(1 if job_id else workers)]
        try:
            while True:
                _, pending = concurrent.futures.wait(futures, timeout=10)
                emit("progress", queue.stats())
                if not pending:
                    break
            for future in futures:
                try:
                    future.result()
                except Cancelled:
                    api.stop.set()
                except Exception as error:
                    stop_with(ProtocolError(f"Ошибка сохранения/исполнения очереди: {type(error).__name__}."))
        except KeyboardInterrupt:
            api.stop.set()
            errors.append("Остановлено пользователем. Повторный запуск продолжит очередь.")
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        if not api.stop.is_set():
            try:
                telemetry.check(force=True)
            except Cancelled:
                api.stop.set()
            except (AuthError, ProtocolError) as error:
                stop_with(error)
        final_stats = queue.stats(root_ids=root_ids)
        outcome = ("stopped" if api.stop.is_set() else "single_finished" if job_id else
                   "paused_tasks" if final_stats.get("paused", 0) and final_stats.get("remaining") == 0 else
                   "finished_with_errors" if final_stats["failed"] or final_stats["pending"] or final_stats["running"] else
                   "finished_with_skips" if final_stats["skipped"] else "complete")
        result = {"started_at": started, "finished_at": time.time(), "stopped": api.stop.is_set(),
                  "outcome": outcome, "errors": errors, "stats": final_stats,
                  "last_quota": telemetry.last_quota, "protocol": PROTOCOL_VERSION,
                  "auth_required": auth_required, "job_id": job_id}
        queue.set_metadata("last_run", result)
        return result

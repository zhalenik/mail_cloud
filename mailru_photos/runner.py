"""Persistent queue execution, a small live probe, and ongoing quota monitoring."""

from __future__ import annotations

import concurrent.futures
import contextlib
import math
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
from .queue import FileChangedError, Job, Queue, QueueError
from .transport import Cancelled, SourceReadError, TaskCancelled, TransportError


TaskScope = Callable[[Job], ContextManager[threading.Event | None]]


class ProbeRequired(RuntimeError):
    pass


class QuotaChanged(ProtocolError):
    pass


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
                task_scope: TaskScope | None = None) -> dict[str, Any]:
    """Upload one claim; UI observation and cancellation do not relax proof."""
    if emit is not None:
        emit("file_started", {"job_id": job.id, "path": job.remote_path, "size": job.size, "storage_mode": "unlimited"})
    try:
        with _job_scope(queue, api, job, task_scope):
            return _process_job(queue, api, job, progress=(lambda size: emit(
                "upload_progress", {"job_id": job.id, "bytes": size})) if emit is not None else None)
    finally:
        if emit is not None:
            emit("file_finished", {"job_id": job.id})


def _process_job(queue: Queue, api: CloudAPI, job: Job, *, progress=None) -> dict[str, Any]:
    expected = FileIdentity(job.size, job.mtime_ns, job.device, job.inode)
    prior = queue.get_commit_intent(job)
    if prior and prior.get("storage_mode", "unlimited") != "unlimited":
        raise ProtocolError("Сохранённое подтверждение относится к обычному хранению; файл не изменён.")
    result = api.upload_photo(job.local_path, job.remote_path, expected=expected,
                              prior_intent=prior,
                              save_intent=lambda digest: queue.save_commit_intent(job, digest),
                              save_confirmation=lambda digest: queue.confirm_unlimited_commit(job, digest),
                              is_retried=job.attempts > 1,
                              **({"progress": progress} if progress is not None else {}))
    if (not isinstance(result, dict) or result.get("stat_matched") is not True
            or not isinstance(result.get("hash"), str)
            or not re.fullmatch(r"[0-9A-Fa-f]{40}", result["hash"])
            or isinstance(result.get("size"), bool) or result.get("size") != job.size
            or result.get("path") != job.remote_path
            or result.get("storage_mode", "unlimited") != "unlimited"
            or result.get("is_unlim_upload") is not True
            or not ((result.get("commit_accepted") is True and result.get("recovered_commit") is False
                     and result.get("unlimited_evidence") == "commit_response")
                    or (result.get("commit_accepted") is False and result.get("recovered_commit") is True
                        and result.get("unlimited_evidence") == "saved_commit_response"))):
        raise FileVerificationError("Не получено полное подтверждение безлимита, commit, пути, хеша и размера файла.")
    confirmation = queue.get_commit_intent(job)
    if (not confirmation or confirmation.get("unlimited_confirmed") is not True
            or confirmation.get("storage_mode", "unlimited") != "unlimited"
            or not isinstance(confirmation.get("cloud_hash"), str)
            or confirmation["cloud_hash"].casefold() != result["hash"].casefold()):
        raise ProtocolError("Подтверждение безлимита от сервера не сохранено для этого файла; завершение не отмечено.")
    result = {**result, "storage_mode": "unlimited"}
    queue.mark_uploaded(job, remote_hash=result["hash"], remote_id=result.get("nodeid"),
                        verified_metadata={"protocol": PROTOCOL_VERSION, **result,
                                           "unlimited_confirmed_at": confirmation.get("unlimited_confirmed_at"),
                                           "commit_intent_created_at": confirmation.get("created_at")})
    return {"job_id": job.id, **result}


def require_scanned(queue: Queue) -> None:
    scan = queue.get_metadata("last_scan", {})
    if not isinstance(scan, dict) or scan.get("complete") is not True:
        raise ProbeRequired("Сначала выполните scan. Ошибки обхода папок нужно устранить до загрузки.")


def process_probe_job(queue: Queue, api: CloudAPI, job: Job, *, baseline: int,
                      emit: Callable[[str, dict], None] | None = None,
                      file_progress: bool = False, task_scope: TaskScope | None = None) -> dict[str, Any] | None:
    """Process one probe candidate, preserving global safeguards.

    None means this task was paused, removed, unsupported or failed locally;
    the caller may take another candidate. Auth/protocol/state failures and
    global cancellation still propagate. This helper is also safe for a probe
    resumed by the web controller after a process restart.
    """
    emit = emit or (lambda event, data: None)
    try:
        emit("probe_upload", {"job_id": job.id, "path": job.remote_path})
        return process_job(queue, api, job, emit=emit if file_progress else None, task_scope=task_scope)
    except TaskCancelled:
        queue.release(job, "Task paused or removed during probe; preserve any remote commit")
        emit("task_cancelled", {"job_id": job.id})
    except (Cancelled, KeyboardInterrupt):
        queue.release(job, "Interrupted probe; reconcile any remote commit before retry")
        raise
    except FileChangedError as error:
        emit("file_changed", {"job_id": job.id, "reason": str(error)})
    except UnsupportedPhoto as error:
        queue.mark_skipped(job, str(error))
        emit("skipped", {"job_id": job.id, "reason": str(error)})
    except (SourceChanged, SourceReadError, RemoteConflict, FileVerificationError, UnlimitedNotConfirmed) as error:
        queue.mark_failed(job, str(error), retry_delay=None)
        emit("file_error", {"job_id": job.id, "reason": str(error)})
        if isinstance(error, UnlimitedNotConfirmed):
            api.require_account(queue.account_id)
            if api.quota()["used_bytes"] != baseline:
                raise QuotaChanged("Использование квоты изменилось после неподтверждённой задачи. Загрузка приостановлена.")
    except RemoteError as error:
        if error.unsupported:
            queue.mark_skipped(job, str(error))
        else:
            delay = min(900, 2 ** job.attempts) if error.retryable and job.attempts < 5 else None
            queue.mark_failed(job, str(error), retry_delay=delay)
        emit("remote_error", {"job_id": job.id, "status": error.status, "code": error.code})
        if error.fatal:
            raise
    except TransportError as error:
        if api.stop.is_set():
            queue.release(job, "Probe request interrupted by global pause/shutdown")
            raise Cancelled("Проверка приостановлена; сетевой запрос завершился после остановки.") from None
        queue.mark_failed(job, str(error), retry_delay=min(900, 2 ** job.attempts) if job.attempts < 5 else None)
        emit("file_error", {"job_id": job.id, "reason": str(error)})
    except (ProtocolError, AuthError) as error:
        queue.mark_failed(job, str(error), retry_delay=None)
        raise
    return None


def probe(queue: Queue, api: CloudAPI, *, count: int = 3, settle_seconds: float = 60,
          emit: Callable[[str, dict], None] | None = None, file_progress: bool = False,
          task_scope: TaskScope | None = None) -> dict[str, Any]:
    """Upload a few queued photos, then measure cloud usage twice.

    Each file requires an explicit is_unlim_upload=True add response, persisted
    before stat, plus matching hash/size. Equal quota measurements also guard
    the batch, while delayed quota changes still remain possible.
    """
    if not 1 <= count <= 10 or not math.isfinite(settle_seconds) or not 30 <= settle_seconds <= 86400:
        raise ValueError("Probe count must be 1..10 and settling period at least 30 seconds")
    emit = emit or (lambda event, data: None)
    with queue.exclusive_run():
        queue.recover_running()
        require_scanned(queue)
        account = api.require_account(queue.account_id)
        queue.bind_account(account.email)
        before = api.quota()
        report: dict[str, Any] = {
            "protocol": PROTOCOL_VERSION, "account": account.email,
            "destination": queue.destination, "started_at": time.time(),
            "settle_seconds": settle_seconds, "before": before, "samples": [],
            "requested_count": count,
            "measurements": [], "result": "incomplete", "live_test": True,
            "per_file_unlimited_flag": "is_unlim_upload_in_add_response",
        }
        queue.set_metadata("probe", report)
        try:
            accepted_count = 0
            while accepted_count < count:
                if api.stop.is_set():
                    raise Cancelled("Проверка приостановлена.")
                job = queue.claim(max_attempts=5, storage_mode="unlimited", stop=api.stop)
                if job is None:
                    if api.stop.is_set():
                        raise Cancelled("Проверка приостановлена.")
                    retry_at = queue.stats(storage_mode="unlimited").get("next_retry_at")
                    if retry_at is None:
                        break
                    if api.stop.wait(max(0.1, min(10, retry_at - time.time()))):
                        raise Cancelled("Проверка приостановлена во время ожидания повторной попытки.")
                    continue
                report["active_job_id"] = job.id
                queue.set_metadata("probe", report)
                result = process_probe_job(queue, api, job, baseline=before["used_bytes"],
                                           emit=emit, file_progress=file_progress, task_scope=task_scope)
                if result is not None and result.get("commit_accepted") is True:
                    report["samples"].append(result)
                    accepted_count += 1
                else:
                    # Keep report size bounded even when a large queue has
                    # many failed candidates or recovered older commits.
                    report["other_candidates"] = report.get("other_candidates", 0) + 1
                report.pop("active_job_id", None)
                # An interrupt during settling must retain already verified
                # uploads in the report, as well as their queue states.
                queue.set_metadata("probe", report)
            accepted = [sample for sample in report["samples"] if sample.get("commit_accepted") is True]
            if len(accepted) < count:
                report["result"] = "insufficient_new_samples"
                raise ProbeRequired("Для проверки не хватило новых подтверждённых загрузок. Проверьте задачи с ошибками и на паузе, добавьте файлы или уменьшите --count.")
            for step in range(2):
                emit("probe_wait", {"seconds": settle_seconds / 2, "measurement": step + 1})
                if api.stop.wait(settle_seconds / 2):
                    raise Cancelled("Проверка остановлена; загруженные файлы сохранены в очереди.")
                api.require_account(account.email)
                quota = api.quota()
                report["measurements"].append({"at": time.time(), **quota})
                queue.set_metadata("probe", report)
                emit("probe_quota", {"delta_bytes": quota["used_bytes"] - before["used_bytes"]})
        except BaseException as error:
            interrupted_network = isinstance(error, TransportError) and api.stop.is_set()
            if isinstance(error, (Cancelled, KeyboardInterrupt)) or interrupted_network:
                report["result"] = "interrupted"
                report["error"] = "Проверка прервана; для разрешения массовой загрузки требуется завершённая probe."
            else:
                if report["result"] == "incomplete":
                    report["result"] = "failed"
                report["error"] = str(error) if isinstance(error, (ProtocolError, AuthError, TransportError, ProbeRequired)) else type(error).__name__
            report["finished_at"] = time.time()
            queue.set_metadata("probe", report)
            if interrupted_network:
                raise Cancelled("Проверка приостановлена; контроль квоты можно продолжить.") from None
            raise
        unchanged = all(item["used_bytes"] == before["used_bytes"] for item in report["measurements"])
        report["result"] = "provisional_pass" if unchanged else "quota_changed"
        report["finished_at"] = time.time()
        report["explanation"] = (
            "Сервер подтвердил is_unlim_upload=true для каждого файла, hash/size совпали, "
            "две проверки used.cloud не показали изменения. "
            "Это предварительное подтверждение; отложенный пересчёт квоты не исключён."
            if unchanged else "Квота изменилась. Массовая загрузка не разрешена; проверьте другие операции аккаунта и ответы сервера."
        )
        queue.set_metadata("probe", report)
        return report


class QuotaGuard:
    def __init__(self, api: CloudAPI, account: str, baseline: int, *, seconds: float = 30, every_files: int = 50):
        self.api, self.account, self.baseline = api, account, baseline
        self.seconds, self.every_files = seconds, every_files
        self._lock = threading.Lock()
        self._last = time.monotonic()
        self._completed = 0
        self.last_quota: dict[str, int] | None = None

    def completed(self) -> None:
        with self._lock:
            self._completed += 1

    def check(self, *, force: bool = False) -> None:
        with self._lock:
            if not force and self._completed < self.every_files and time.monotonic() - self._last < self.seconds:
                return
            self.api.require_account(self.account)
            quota = self.api.quota()
            self.last_quota = quota
            if quota["used_bytes"] != self.baseline:
                raise QuotaChanged("Использование квоты Облака изменилось. Загрузка приостановлена; очередь сохранена.")
            self._last, self._completed = time.monotonic(), 0


def run(queue: Queue, api: CloudAPI, *, workers: int = 4, max_attempts: int = 5,
        emit: Callable[[str, dict], None] | None = None, file_progress: bool = False,
        require_probe: bool = True, task_scope: TaskScope | None = None) -> dict[str, Any]:
    if not 1 <= workers <= 16 or not 1 <= max_attempts <= 20:
        raise ValueError("workers must be 1..16, max_attempts 1..20")
    if type(require_probe) is not bool:
        raise ValueError("require_probe must be a boolean")
    emit = emit or (lambda event, data: None)
    started = time.time()
    errors: list[str] = []
    error_lock = threading.Lock()
    needs_recheck = False
    recheck_reason = ""
    auth_required = False

    def stop_with(error: Exception, *, invalidate_probe: bool = False) -> None:
        nonlocal needs_recheck, recheck_reason, auth_required
        with error_lock:
            # Another worker may already have recorded a different fatal
            # failure. Never lose a later, explicit authentication rejection.
            auth_required = auth_required or is_auth_failure(error)
            needs_recheck = needs_recheck or invalidate_probe
            if invalidate_probe and not recheck_reason:
                recheck_reason = str(error)
            if not errors:
                errors.append(str(error))
        api.stop.set()

    with queue.exclusive_run():
        queue.recover_running()
        require_scanned(queue)
        skipped = queue.skip_large_disabled()
        if skipped:
            emit("large_files_skipped", {"count": skipped})
        require_probe = require_probe and queue.stats(storage_mode="unlimited")["total"] > 0
        account = api.require_account(queue.account_id)
        queue.bind_account(account.email)
        evidence = queue.get_metadata("probe", {})
        evidence = evidence if isinstance(evidence, dict) else {}
        if require_probe:
            finished_at = evidence.get("finished_at")
            valid_timestamp = (isinstance(finished_at, (int, float)) and not isinstance(finished_at, bool)
                               and math.isfinite(finished_at) and 0 <= time.time() - finished_at <= 86400)
            if (evidence.get("result") != "provisional_pass"
                    or evidence.get("protocol") != PROTOCOL_VERSION or evidence.get("account") != account.email
                    or evidence.get("destination") != queue.destination
                    or not valid_timestamp or not isinstance(evidence.get("before"), dict)
                    or not isinstance(evidence["before"].get("used_bytes"), int)
                    or isinstance(evidence["before"].get("used_bytes"), bool)):
                raise ProbeRequired("Сначала выполните probe: требуется успешная предварительная проверка в этом аккаунте за последние 24 часа.")
        # Skipping the settling probe does not disable per-file confirmation or
        # quota monitoring. Use a fresh baseline without inventing probe proof.
        baseline = api.quota()["used_bytes"]
        if require_probe and baseline != evidence["before"]["used_bytes"]:
            raise ProbeRequired("После probe квота изменилась. Нужна новая проверка перед массовой загрузкой.")
        guard = QuotaGuard(api, account.email, baseline)

        def worker() -> None:
            local = Queue(queue.path, destination=queue.destination, account_id=account.email)
            try:
                while not api.stop.is_set():
                    try:
                        guard.check()
                    except Cancelled:
                        api.stop.set()
                        return
                    except (ProtocolError, AuthError, TransportError) as error:
                        stop_with(error, invalidate_probe=isinstance(error, (ProtocolError, AuthError)))
                        return
                    job = local.claim(max_attempts=max_attempts, storage_mode="unlimited", stop=api.stop)
                    if job is None:
                        if api.stop.is_set():
                            return
                        state = local.stats(storage_mode="unlimited")
                        next_retry = state.get("next_retry_at")
                        if next_retry is None:
                            return
                        api.stop.wait(max(0.1, min(10, next_retry - time.time())))
                        continue
                    try:
                        result = process_job(local, api, job, emit=emit if file_progress else None, task_scope=task_scope)
                        guard.completed()
                        emit("uploaded", {"job_id": job.id, "path": job.remote_path,
                                          "size": job.size, "recovered_commit": result["recovered_commit"]})
                    except (UnsupportedPhoto,) as error:
                        local.mark_skipped(job, str(error))
                        emit("skipped", {"job_id": job.id, "reason": str(error)})
                    except RemoteError as error:
                        if error.fatal:
                            # Record the global cause before a database write
                            # or UI observer could itself fail. In particular,
                            # that secondary error must not erase auth_required.
                            stop_with(error, invalidate_probe=True)
                        if error.unsupported:
                            local.mark_skipped(job, str(error))
                        elif error.retryable and job.attempts < max_attempts:
                            delay = max(error.retry_after or 0, min(900, 2 ** job.attempts + random.random() * 3))
                            local.mark_failed(job, str(error), retry_delay=delay)
                        else:
                            local.mark_failed(job, str(error), retry_delay=None)
                        emit("remote_error", {"job_id": job.id, "status": error.status, "code": error.code})
                    except TransportError as error:
                        if api.stop.is_set():
                            local.release(job, "Network request interrupted by pause/shutdown")
                            return
                        delay = min(900, 2 ** job.attempts + random.random() * 3) if job.attempts < max_attempts else None
                        local.mark_failed(job, str(error), retry_delay=delay)
                        emit("retry", {"job_id": job.id, "retry_in": delay})
                    except (SourceChanged, SourceReadError, RemoteConflict, FileVerificationError, UnlimitedNotConfirmed) as error:
                        local.mark_failed(job, str(error), retry_delay=None)
                        emit("file_error", {"job_id": job.id, "reason": str(error)})
                        if isinstance(error, UnlimitedNotConfirmed):
                            try:
                                guard.check(force=True)
                            except Cancelled:
                                api.stop.set()
                                return
                            except (ProtocolError, AuthError, TransportError) as guard_error:
                                stop_with(guard_error, invalidate_probe=isinstance(guard_error, (ProtocolError, AuthError)))
                                return
                    except FileChangedError as error:
                        # Queue already invalidated this claim and preserved history.
                        emit("file_changed", {"job_id": job.id, "reason": str(error)})
                    except TaskCancelled:
                        local.release(job, "Task paused or removed; preserve any remote commit")
                        emit("task_cancelled", {"job_id": job.id})
                    except Cancelled:
                        local.release(job, "Interrupted; safe to resume")
                        api.stop.set()
                        return
                    except (ProtocolError, AuthError) as error:
                        stop_with(error, invalidate_probe=True)
                        local.mark_failed(job, str(error), retry_delay=None)
                    except (QueueError, sqlite3.Error) as error:
                        stop_with(ProtocolError(f"Ошибка сохранения очереди: {type(error).__name__}. Загрузка остановлена."))
                    except Exception as error:
                        # Never put arbitrary exception reprs/HTTP payloads in logs.
                        stop_with(ProtocolError(f"Локальная ошибка {type(error).__name__}. Очередь сохранена."))
            finally:
                local.close()
                api.close_thread()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="photo-upload")
        futures = [executor.submit(worker) for _ in range(workers)]
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
                    stop_with(ProtocolError(f"Локальная ошибка worker: {type(error).__name__}. Очередь сохранена."))
        except KeyboardInterrupt:
            api.stop.set()
            errors.append("Остановлено пользователем. Повторный run продолжит очередь.")
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        if not api.stop.is_set():
            try:
                guard.check(force=True)
            except Cancelled:
                api.stop.set()
            except (ProtocolError, AuthError, TransportError) as error:
                stop_with(error, invalidate_probe=isinstance(error, (ProtocolError, AuthError)))
        final_stats = queue.stats()
        outcome = ("stopped" if api.stop.is_set() else
                   "paused_tasks" if final_stats.get("paused", 0) and final_stats.get("remaining") == 0 else
                   "finished_with_errors" if final_stats["failed"] or final_stats["pending"] or final_stats["running"] else
                   "finished_with_skips" if final_stats["skipped"] else "complete")
        result = {"started_at": started, "finished_at": time.time(), "stopped": api.stop.is_set(),
                  "outcome": outcome, "errors": errors, "stats": final_stats, "baseline_cloud_bytes": baseline,
                  "last_quota": guard.last_quota, "protocol": PROTOCOL_VERSION,
                  "preflight_check": require_probe, "auth_required": auth_required}
        queue.set_metadata("last_run", result)
        if needs_recheck:
            invalid = dict(evidence)
            invalid["result"] = "needs_recheck"
            invalid["reason"] = recheck_reason
            queue.set_metadata("probe", invalid)
        return result

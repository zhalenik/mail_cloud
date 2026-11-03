"""Optional exact-path cloud comparison, without uploads or deletions.

The controller calls this only after a complete scan and after authenticating
and binding the same account used by ``api``. It is disabled unless explicitly
requested in settings. A matching hash is an observation of existing content;
it is never evidence that this client uploaded it or that storage is unlimited.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import re
import threading
import time
from typing import Any

from .api import FileIdentity, ProtocolError, RemoteError, SourceChanged, cloud_hash
from .auth import AuthError
from .queue import Queue, QueueConfigurationError
from .transport import Cancelled, SourceReadError, TransportError


_BATCH_SIZE = 500
_MAX_ERROR_SAMPLES = 100
_MAX_CONSECUTIVE_INVALID_RESPONSES = 3


class _InvalidObservation(ValueError):
    """One returned object cannot establish an exact-path content match."""


def _observation(value: Mapping[str, Any], path: str) -> tuple[int, str]:
    if not isinstance(value, Mapping):
        raise _InvalidObservation("Сервер вернул некорректные сведения о файле.")
    size = value.get("size")
    digest = value.get("hash")
    if type(size) is not int or size <= 0:
        raise _InvalidObservation("Сервер не вернул положительный размер файла.")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9A-Fa-f]{40}", digest):
        raise _InvalidObservation("Сервер не вернул корректный хеш файла.")
    if "path" in value and value["path"] != path:
        raise _InvalidObservation("Ответ сервера относится к другому облачному пути.")
    return size, digest.upper()


def reconcile(queue: Queue, api: Any, *, stop: threading.Event,
              progress: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
    """Compare eligible tasks in bounded pages and mark exact existing matches.

    Only ``api.stat(path)`` performs network operations. File reads are streamed
    by the existing Mail Cloud hash implementation. Each candidate and final
    state transition rechecks eligibility so a concurrent pause/remove wins.
    Global network/protocol/auth failures end this optional pass early; local
    file problems are reported individually. Queue/database failures propagate.
    """
    summary: dict[str, Any] = {
        "checked": 0, "matched": 0, "errors": 0, "error_samples": [],
        "cancelled": False, "auth_required": False, "aborted": None,
    }
    last_progress = 0.0

    def snapshot() -> dict[str, Any]:
        return {**summary, "error_samples": [dict(item) for item in summary["error_samples"]]}

    def report(*, force: bool = False) -> None:
        nonlocal last_progress
        if progress is not None and (force or time.monotonic() - last_progress >= 0.25):
            last_progress = time.monotonic()
            progress(snapshot())

    def failed(candidate: Mapping[str, Any], error: Exception, message: str | None = None) -> None:
        summary["errors"] += 1
        if len(summary["error_samples"]) < _MAX_ERROR_SAMPLES:
            summary["error_samples"].append({
                "job_id": candidate["id"], "path": candidate["remote_path"],
                "kind": type(error).__name__, "message": (message or str(error))[:500],
            })

    if stop.is_set():
        summary["cancelled"] = True
        report(force=True)
        return snapshot()
    with queue.exclusive_run():
        if not queue.account_id:
            raise QueueConfigurationError("Bind the authenticated account before comparing cloud files.")
        report(force=True)
        after_id = 0
        invalid_responses = 0
        finish = False
        while not finish:
            if stop.is_set():
                summary["cancelled"] = True
                break
            candidates = queue.reconcile_candidates(after_id=after_id, limit=_BATCH_SIZE)
            if not candidates:
                break
            for candidate in candidates:
                after_id = candidate["id"]
                if stop.is_set():
                    summary["cancelled"] = True
                    finish = True
                    break
                summary["checked"] += 1
                try:
                    remote = api.stat(candidate["remote_path"])
                    if stop.is_set():
                        raise Cancelled("Сверка остановлена.")
                    if remote is None:
                        invalid_responses = 0
                        continue
                    size, digest = _observation(remote, candidate["remote_path"])
                    if size != candidate["size"]:
                        invalid_responses = 0
                        continue
                    expected = FileIdentity(candidate["size"], candidate["mtime_ns"], candidate["device"],
                                            int(candidate["inode"]))
                    local_hash, identity = cloud_hash(candidate["local_path"], expected=expected, stop=stop)
                    if stop.is_set():
                        raise Cancelled("Сверка остановлена.")
                    if identity != expected:
                        raise SourceChanged("Исходный файл изменился во время сверки.")
                    if local_hash.upper() != digest:
                        invalid_responses = 0
                        continue
                    # Hashing a large source can take time. Recheck the exact
                    # remote object before persisting a terminal observation.
                    remote = api.stat(candidate["remote_path"])
                    if stop.is_set():
                        raise Cancelled("Сверка остановлена.")
                    if remote is None or _observation(remote, candidate["remote_path"]) != (size, digest):
                        invalid_responses = 0
                        continue
                    if queue.mark_remote_existing(candidate, remote_hash=digest):
                        summary["matched"] += 1
                    invalid_responses = 0
                except Cancelled:
                    summary["cancelled"] = True
                    finish = True
                except (SourceChanged, SourceReadError, OSError) as error:
                    failed(candidate, error, "Исходный файл изменился или недоступен для чтения.")
                    invalid_responses = 0
                except _InvalidObservation as error:
                    failed(candidate, error)
                    invalid_responses += 1
                    if invalid_responses >= _MAX_CONSECUTIVE_INVALID_RESPONSES:
                        summary["aborted"] = "invalid_responses"
                        finish = True
                except AuthError as error:
                    failed(candidate, error, "Для сверки требуется повторная авторизация Mail.ru.")
                    summary["auth_required"] = True
                    summary["aborted"] = "auth"
                    finish = True
                except RemoteError as error:
                    failed(candidate, error)
                    summary["auth_required"] = error.status in (401, 403)
                    summary["aborted"] = "auth" if summary["auth_required"] else "remote"
                    finish = True
                except TransportError as error:
                    if stop.is_set():
                        summary["cancelled"] = True
                    else:
                        failed(candidate, error, "Сетевое соединение прервано; сверка завершена досрочно.")
                        summary["aborted"] = "network"
                    finish = True
                except ProtocolError as error:
                    failed(candidate, error)
                    summary["aborted"] = "protocol"
                    finish = True
                finally:
                    report()
                if finish:
                    break
        report(force=True)
    return snapshot()

"""Fresh cloud-tree reconciliation by content, independent of destination path.

A scan-local SQLite inventory avoids one network request per source. It is not
an atomic server snapshot: another client can change a file after it was listed.
No previous inventory or upload history is accepted as proof of current presence.
"""
from __future__ import annotations

from collections.abc import Callable
import json
from pathlib import Path
import threading
import time
from typing import Any

from .api import ProtocolError, RemoteError, SourceChanged, is_auth_failure
from .auth import AuthError
from .inventory import CloudInventory
from .queue import Queue, QueueConfigurationError
from .transport import Cancelled, SourceReadError, TransportError


def reconcile(queue: Queue, api: Any, *, stop: threading.Event,
              progress: Callable[[dict[str, Any]], None] | None = None, root_ids=None,
              inventory_path=None) -> dict[str, Any]:
    summary = {"checked": 0, "matched": 0, "matched_elsewhere": 0, "errors": 0,
               "error_samples": [], "cancelled": False, "auth_required": False,
               "aborted": None, "complete": False, "inventory_complete": False,
               "cloud_files": 0, "cloud_folders": 0, "cloud_pages": 0,
               "stage": "listing"}
    last_progress = 0.0

    def snapshot():
        return {**summary, "error_samples": [dict(value) for value in summary["error_samples"]]}

    def report(*, force=False):
        nonlocal last_progress
        if progress is not None and (force or time.monotonic() - last_progress >= 0.25):
            last_progress = time.monotonic()
            progress(snapshot())

    def listing_progress(data):
        summary.update(data)
        report()

    def failed(error, candidate=None, message=None):
        summary["errors"] += 1
        if len(summary["error_samples"]) < 100:
            summary["error_samples"].append({
                "job_id": candidate["id"] if candidate else None,
                "path": candidate["remote_path"] if candidate else None,
                "kind": type(error).__name__, "message": (message or str(error))[:500]})

    if stop.is_set():
        summary["cancelled"] = True
        report(force=True)
        return snapshot()
    with queue.exclusive_run():
        if not queue.account_id:
            raise QueueConfigurationError("Bind the authenticated account before comparing cloud files.")
        # Reject a conflicting live upload before making a potentially long
        # remote traversal. Matching still reads fresh keyset pages afterwards.
        queue.reconcile_candidates(after_id=0, limit=1, root_ids=root_ids)
        report(force=True)
        try:
            with CloudInventory(inventory_path or Path(queue.path).with_name("cloud-index.sqlite3")) as inventory:
                summary.update(inventory.refresh(api, stop=stop, progress=listing_progress))
                summary.update(inventory_complete=True, stage="matching")
                report(force=True)
                after_id = 0
                while True:
                    if stop.is_set():
                        raise Cancelled("Сверка остановлена.")
                    candidates = queue.reconcile_candidates(after_id=after_id, limit=500, root_ids=root_ids)
                    if not candidates:
                        break
                    for candidate in candidates:
                        after_id = candidate["id"]
                        if stop.is_set():
                            raise Cancelled("Сверка остановлена.")
                        summary["checked"] += 1
                        try:
                            # A size absent from this complete inventory cannot
                            # have a matching hash; avoid reading its local bytes.
                            if not inventory.has_size(candidate["size"]):
                                continue
                            digest, ctime_ns = inventory.local_hash(candidate, stop=stop)
                            found = inventory.find(digest, candidate["size"], candidate["remote_path"])
                            if found is None:
                                continue
                            if stop.is_set():
                                raise Cancelled("Сверка остановлена.")
                            adopted = queue.mark_remote_existing(
                                {**candidate, "ctime_ns": ctime_ns}, remote_hash=digest,
                                remote_path=found["path"], storage_mode=found["storage_mode"],
                                remote_id=found["nodeid"], evidence=json.loads(found["evidence"]))
                            if adopted:
                                summary["matched"] += 1
                                summary["matched_elsewhere"] += int(
                                    found["path"].casefold() != candidate["remote_path"].casefold())
                        except (SourceChanged, SourceReadError, OSError) as error:
                            message = "Исходный файл изменился или недоступен для чтения. Пересканируйте папку после устранения причины."
                            failed(error, candidate, message)
                            queue.record_task_error(candidate["id"], message)
                        finally:
                            report()
                summary.update(complete=True, stage="complete")
        except Cancelled:
            summary["cancelled"] = True
        except AuthError as error:
            failed(error, message="Для сверки требуется повторный вход в Mail.ru.")
            summary.update(auth_required=is_auth_failure(error), aborted="auth")
        except RemoteError as error:
            failed(error)
            summary.update(auth_required=is_auth_failure(error), aborted="auth" if is_auth_failure(error) else "remote")
        except TransportError as error:
            if stop.is_set():
                summary["cancelled"] = True
            else:
                failed(error, message="Сеть недоступна; полный список Облака не получен.")
                summary["aborted"] = "network"
        except ProtocolError as error:
            failed(error)
            summary["aborted"] = "protocol"
        report(force=True)
        return snapshot()

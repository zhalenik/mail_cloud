"""Explicit deletion plans, bounded durable journals and guarded local unlink.

Cloud deletion belongs to CloudAPI.trash_file. This module never empties trash.
A private staging directory protects the source-name race: rename without
replacement, verify the moved inode, then unlink only inside that directory.
Interrupted confirmed plans retain identities and completed steps on disk.
"""
from __future__ import annotations

import contextlib
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from .api import FileIdentity
from .queue import QueueError

LOCAL_CONFIRMATION = "УДАЛИТЬ С ДИСКА"
_FIELDS = ("id", "root_id", "local_path", "size", "mtime_ns", "device", "inode",
           "cloud_present", "cloud_path", "cloud_hash", "cloud_size", "cloud_verified")


class LocalDeletionRecoveryRequired(QueueError):
    """Keep the confirmed journal until its staged source is resolved."""


def selected_tasks(queue, selection):
    """Yield one current selection using at most 500 queue rows in memory."""
    if selection.get("id") is not None:
        row = queue.get_job(selection["id"], include_missing=True)
        if row is not None:
            yield row
        return
    after = 0
    status = selection.get("status") if selection.get("scope") == "filter" else None
    while True:
        rows = queue.iter_tasks(after_id=after, limit=500, status=status, include_missing=selection.get("scope") != "filter",
                                root_ids=selection.get("root_ids"))
        if not rows:
            return
        for row in rows:
            if (selection.get("scope") != "finished" or row["status"] in {"uploaded", "skipped"}
                    or (row["status"] == "failed" and row.get("next_attempt") is None)):
                yield row
        after = rows[-1]["id"]


def task_record(row, roots):
    value = {key: row.get(key) for key in _FIELDS}
    value["root"] = roots.get(row["root_id"])
    return value


def encode_record(record):
    return json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _digest_update(digest, record):
    digest.update(encode_record(record).encode("utf-8"))
    digest.update(b"\n")


def deletion_preview(queue, selection, *, delete_cloud=False, delete_local=False):
    roots = {root.id: str(root.path) for root in queue.roots()}
    digest = hashlib.sha256()
    counts = {"task_count": 0, "total_bytes": 0, "cloud_count": 0, "local_count": 0}
    used_roots = set()
    for row in selected_tasks(queue, selection):
        record = task_record(row, roots)
        _digest_update(digest, record)
        counts["task_count"] += 1
        counts["total_bytes"] += row["size"]
        counts["cloud_count"] += bool(row.get("cloud_present") and row.get("cloud_path") and row.get("cloud_hash"))
        counts["local_count"] += 1
        if delete_local:
            used_roots.add(record["root"])
    warnings = []
    local_allowed = local_delete_supported()
    if delete_local:
        for root in used_roots:
            try:
                writable = bool(root) and os.access(root, os.W_OK | os.X_OK)
                readonly = bool(os.statvfs(root).f_flag & getattr(os, "ST_RDONLY", 1)) if root else True
            except OSError:
                writable, readonly = False, True
            if not writable or readonly:
                local_allowed = False
        if not local_allowed:
            warnings.append("Удаление с диска недоступно: источник подключён только для чтения или нет прав на запись.")
        warnings.append("Исходные файлы будут удалены с диска навсегда. Корзины для них нет.")
    if delete_cloud:
        warnings.append("Облачные файлы будут перенесены в корзину. Это может не сразу освободить квоту.")
        if counts["cloud_count"] < counts["task_count"]:
            warnings.append("В Облаке удаляются только объекты с сохранённым адресом и подтверждением принадлежности задаче.")
    return {**counts, "fingerprint": digest.hexdigest(), "local_delete_allowed": local_allowed, "warnings": warnings}


def _rename_function():
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW") or os.open not in os.supports_dir_fd:
        return None
    library = ctypes.CDLL(None, use_errno=True)
    function = getattr(library, "renameat2", None)
    if function is not None:
        function.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        function.restype = ctypes.c_int
    return function


_RENAME = _rename_function()


def local_delete_supported():
    return _RENAME is not None


def _rename_no_replace(source_fd, source, destination_fd, destination):
    if _RENAME(source_fd, os.fsencode(source), destination_fd, os.fsencode(destination), 1) != 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))


def _open_parent(root: Path, path: Path):
    if not local_delete_supported():
        raise QueueError("На этом сервере безопасное удаление исходников не поддерживается.")
    if not root.is_absolute() or not path.is_absolute() or ".." in root.parts or ".." in path.parts:
        raise QueueError("Недопустимый путь удаления.")
    try:
        relative = path.relative_to(root)
    except ValueError:
        raise QueueError("Исходник находится вне подключённой папки.") from None
    if not relative.parts:
        raise QueueError("Удалять можно только обычный файл, а не папку.")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(root.anchor, flags)
    try:
        for part in root.parts[1:] + relative.parts[:-1]:
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd, relative.parts[-1]
    except BaseException:
        os.close(fd)
        raise


def delete_local_file(root: Path, path: Path, expected: FileIdentity, stage_name: str) -> None:
    """Remove only the scanned inode; recover a previously staged same inode.

    The staging name is generated by the confirmed journal, never supplied by
    an HTTP client. Rename is atomic and refuses replacement. An unexpected
    inode is restored without overwriting any newly created original path.
    """
    if (not isinstance(stage_name, str) or not stage_name.startswith(".mail-cloud-delete-")
            or "/" in stage_name or "\\" in stage_name or "\x00" in stage_name or len(stage_name) > 100):
        raise QueueError("Некорректный идентификатор операции удаления.")

    def source_error(error):
        return ("Источник подключён только для чтения; файл не удалён." if error.errno == errno.EROFS else
                "Нет прав на удаление исходника; файл сохранён." if error.errno in {errno.EACCES, errno.EPERM} else
                "Не удалось безопасно удалить исходник (" + type(error).__name__ + ").")

    try:
        parent, name = _open_parent(Path(root), Path(path))
    except OSError as error:
        raise QueueError(source_error(error)) from None
    staging = None
    trusted_stage = False
    moved_here = False
    identified = False
    unlinked = False

    def recover_staged_source():
        """Restore without replacing a new original path, or retain the journal."""
        if staging is None or not trusted_stage:
            return
        try:
            os.stat("source", dir_fd=staging, follow_symlinks=False)
        except FileNotFoundError:
            return
        except OSError:
            raise LocalDeletionRecoveryRequired(
                "Не удалось проверить исходник в служебной папке " + stage_name + ". "
                "Он не удалён. Проверьте доступ к диску и нажмите «Продолжить».") from None
        if not (moved_here or identified):
            raise LocalDeletionRecoveryRequired(
                "В служебной папке " + stage_name + " находится другой файл. Он сохранён. "
                "Проверьте исходник и служебную папку, затем нажмите «Продолжить».")
        try:
            _rename_no_replace(staging, "source", parent, name)
        except OSError:
            raise LocalDeletionRecoveryRequired(
                "Исходник сохранён в служебной папке " + stage_name + ". "
                "Вернуть его на прежнее место без перезаписи не удалось. "
                "Проверьте права и исходную папку, затем нажмите «Продолжить»; "
                "служебную папку пока не удаляйте.") from None
        try:
            os.fsync(parent)
            os.fsync(staging)
        except OSError:
            raise LocalDeletionRecoveryRequired(
                "Исходник возвращён на прежнее место, но сохранение результата на диске не подтверждено. "
                "Проверьте диск и нажмите «Продолжить».") from None

    try:
        try:
            os.mkdir(stage_name, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
        staging = os.open(stage_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        directory = os.fstat(staging)
        if directory.st_uid != os.geteuid() or stat.S_IMODE(directory.st_mode) != 0o700:
            raise QueueError("Служебная папка удаления имеет неподходящие права; исходник сохранён.")
        trusted_stage = True
        try:
            staged = os.stat("source", dir_fd=staging, follow_symlinks=False)
        except FileNotFoundError:
            try:
                original = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return  # Already absent, including a crash after the verified unlink.
            if not stat.S_ISREG(original.st_mode) or FileIdentity.from_stat(original) != expected:
                raise QueueError("Исходный файл изменился после сканирования и не удалён. Пересканируйте источник.")
            _rename_no_replace(parent, name, staging, "source")
            moved_here = True
            os.fsync(parent)
            os.fsync(staging)
            staged = os.stat("source", dir_fd=staging, follow_symlinks=False)
        if not stat.S_ISREG(staged.st_mode) or FileIdentity.from_stat(staged) != expected:
            recover_staged_source()
            raise QueueError("Файл изменился во время удаления и был возвращён на место.")
        identified = True
        os.unlink("source", dir_fd=staging)
        unlinked = True
        os.fsync(staging)
    except OSError as error:
        if unlinked:
            raise LocalDeletionRecoveryRequired(
                "Файл удалён, но сохранение результата на диске не подтверждено. "
                "Нажмите «Продолжить», чтобы проверить и завершить эту же операцию.") from None
        recover_staged_source()
        raise QueueError(source_error(error)) from None
    except QueueError:
        raise
    except Exception:
        # An unexpected in-process error must not strand the source and let a
        # later operation discard the only journal containing its stage name.
        if moved_here or identified:
            if unlinked:
                raise LocalDeletionRecoveryRequired(
                    "Завершение удаления не подтверждено. Нажмите «Продолжить» для проверки.") from None
            recover_staged_source()
        raise
    finally:
        if staging is not None:
            os.close(staging)
        if trusted_stage:
            try:
                os.rmdir(stage_name, dir_fd=parent)
                os.fsync(parent)
            except OSError:
                pass  # A surviving staged inode is retained for safe recovery.
        os.close(parent)


class DeletionJournal:
    """One confirmed operation, persisted before the first destructive call."""
    def __init__(self, path: Path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS operation (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY, record TEXT NOT NULL, stage TEXT NOT NULL,
                cloud_done INTEGER NOT NULL DEFAULT 0, local_done INTEGER NOT NULL DEFAULT 0,
                task_done INTEGER NOT NULL DEFAULT 0, error TEXT);
        """)
        self.db.commit()
        with contextlib.suppress(OSError):
            self.path.chmod(0o600)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.db.close()

    def operation(self):
        row = self.db.execute("SELECT value FROM operation WHERE singleton=1").fetchone()
        return json.loads(row[0]) if row else None

    def set_operation(self, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO operation(singleton,value) VALUES(1,?)", (json.dumps(value),))

    def prepare(self, queue, preview):
        previous = self.operation()
        if previous and previous.get("state") in {"confirmed", "running", "paused"}:
            raise QueueError("Предыдущее подтверждённое удаление ещё не завершено. Продолжите его.")
        operation_id = uuid.uuid4().hex
        roots = {root.id: str(root.path) for root in queue.roots()}
        digest = hashlib.sha256()
        count = 0
        self.set_operation({"state": "preparing", "id": operation_id})
        with self.db:
            self.db.execute("DELETE FROM entries")
            for row in selected_tasks(queue, preview["selection"]):
                record = task_record(row, roots)
                _digest_update(digest, record)
                self.db.execute("INSERT INTO entries(id,record,stage) VALUES(?,?,?)", (
                    row["id"], encode_record(record), ".mail-cloud-delete-" + operation_id + "-" + str(row["id"])))
                count += 1
        if digest.hexdigest() != preview["fingerprint"] or count != preview["task_count"]:
            self.set_operation({"state": "aborted", "id": operation_id})
            raise QueueError("Список задач изменился. Откройте окно удаления и подтвердите новый список.")
        operation = {"state": "confirmed", "id": operation_id, "task_count": count,
                     "delete_cloud": preview["delete_cloud"], "delete_local": preview["delete_local"],
                     "confirmed_at": time.time()}
        self.set_operation(operation)
        return operation

    def pending(self, *, after_id=0):
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM entries WHERE task_done=0 AND error IS NULL AND id>? ORDER BY id LIMIT 100", (after_id,))]

    def done(self, task_id, field):
        if field not in {"cloud_done", "local_done", "task_done"}:
            raise ValueError("Invalid deletion checkpoint")
        with self.db:
            self.db.execute(f"UPDATE entries SET {field}=1 WHERE id=?", (task_id,))

    def error(self, task_id, message):
        with self.db:
            self.db.execute("UPDATE entries SET error=? WHERE id=?", (message[:2000], task_id))

    def summary(self):
        result = dict(self.db.execute("SELECT COUNT(*) AS total,COALESCE(SUM(task_done),0) AS done,"
                                     "COALESCE(SUM(error IS NOT NULL),0) AS errors FROM entries").fetchone())
        return {**result, "processed": result["done"] + result["errors"]}

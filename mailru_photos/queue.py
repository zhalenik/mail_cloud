"""Bounded-memory, restartable local upload queue (no network operations).

The transport owns server-side validation.  ``mark_uploaded`` may be called
only after a successful, verified remote commit; a successful HTTP upload of
bytes alone is not proof that a photograph belongs to an unlimited album.

Use ``exclusive_run()`` around a runner, then ``recover_running()`` once on
startup.  Individual claims are transactional and safe across connections;
claim tokens prevent a worker from finishing an already recovered claim.
"""

from __future__ import annotations

import contextlib
import dataclasses
import errno
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
import unicodedata
import uuid

from .media import (
    UNLIMITED_MAX_BYTES, MEDIA_POLICY_VERSION, MEDIA_EXTENSIONS, PHOTO_EXTENSIONS,
    VIDEO_EXTENSIONS, LEGACY_PHOTO_EXTENSIONS, LEGACY_MEDIA_EXTENSIONS, classify_media,
)


STATES = ("pending", "running", "uploaded", "failed", "skipped")
TASK_FILTERS = (*STATES, "paused", "existing", "uploaded_unlimited", "uploaded_regular", "uploaded_unknown")
SCHEMA_VERSION = 6
_MAX_SOURCE_MEMBERS = 50000
_PROCESS_NONCE = uuid.uuid4().hex
_PROGRESS_ORDER = (
    "CASE WHEN status='uploaded' OR (status='skipped' AND skip_reason='remote_exists') THEN 100.0 "
    "WHEN size>0 THEN MIN(100.0,100.0*progress_bytes/size) ELSE 0.0 END"
)
_STATUS_ORDER = (
    "CASE WHEN status='uploaded' THEN 'Выполнено' "
    "WHEN status='skipped' AND skip_reason='remote_exists' THEN 'Уже в облаке' "
    "WHEN paused=1 THEN 'На паузе' WHEN status='running' THEN 'Загружается' "
    "WHEN status='failed' THEN 'Ошибка' WHEN status='skipped' THEN 'Пропущено' ELSE 'Ожидает' END"
)
_FILTER_STATE = "CASE WHEN status='skipped' AND skip_reason='remote_exists' THEN 'existing' ELSE status END"
_SORT_EXPRESSIONS = {
    "name": "name_sort", "progress": _PROGRESS_ORDER, "status": _STATUS_ORDER,
    "size": "size", "created_at": "created_at", "completed_at": "completed_at",
}
_NEW_SORT_INDEX_NAMES = frozenset(
    f"jobs_{group}_{field}" for group in ("sort", "filter", "paused", "observed")
    for field in _SORT_EXPRESSIONS if field != "created_at" or group == "filter"
)
_GROUP_SORT_INDEX_NAMES = frozenset(
    f"jobs_group_{category}_{field}" for category in ("sort", "filter", "paused", "observed")
    for field in _SORT_EXPRESSIONS
)
_SCHEMA_OBJECTS = frozenset({
    ("table", "metadata"), ("table", "roots"), ("table", "source_groups"), ("table", "scans"),
    ("table", "jobs"), ("table", "upload_results"), ("table", "commit_intents"),
    ("table", "commit_evidence"), ("table", "cloud_observations"), ("table", "rescan_intents"),
    ("index", "jobs_ready"), ("index", "jobs_pending"),
    ("index", "jobs_pending_unlimited"), ("index", "jobs_pending_regular"),
    ("index", "jobs_retry_unlimited"), ("index", "jobs_retry_regular"),
    ("index", "jobs_retry"), ("index", "jobs_paused"),
    ("index", "jobs_root_scan"), ("index", "jobs_status"),
    ("index", "jobs_current"), ("index", "jobs_current_status"),
    ("index", "jobs_source_identity"),
    ("index", "upload_results_job"),
    ("index", "commit_evidence_job"),
    ("index", "upload_results_origin"), ("index", "commit_evidence_origin"),
    ("index", "jobs_reconcile"),
    ("index", "jobs_current_created"), ("index", "jobs_current_status_created"),
    ("index", "jobs_paused_created"), ("index", "jobs_observed_created"),
    ("index", "cloud_observations_job"), ("index", "cloud_observations_origin"),
    ("index", "jobs_pending_root"), ("index", "jobs_retry_root"), ("index", "jobs_reconcile_root"),
    ("index", "roots_group"), ("index", "jobs_group"),
    ("index", "jobs_pending_group"), ("index", "jobs_retry_group"), ("index", "jobs_reconcile_group"),
    ("index", "cloud_observations_batch"), ("index", "cloud_observations_path"),
    ("index", "upload_results_batch"),
    ("trigger", "jobs_source_group_insert"), ("trigger", "jobs_source_group_update"),
} | {("index", name) for name in _NEW_SORT_INDEX_NAMES | _GROUP_SORT_INDEX_NAMES})
_JOB_SUMMARY_COLUMNS = (
    "id,root_id,source_group_id,relative_path,local_path,remote_path,status,size,attempts,present,"
    "last_error,skip_reason,next_attempt,uploaded_at,storage_mode,paused,remove_requested,created_at,"
    "planned_mode,planned_reason,observed_mode,cloud_present,cloud_path,cloud_hash,cloud_size,"
    "cloud_verified,cloud_observed_at,cloud_deleted_at,completed_at,progress_bytes"
)
_TASK_COLUMNS = {
    "name_sort": "TEXT NOT NULL DEFAULT ''",
    "progress_bytes": "INTEGER NOT NULL DEFAULT 0 CHECK(progress_bytes>=0)",
    "completed_at": "REAL",
}
_POLICY_COLUMNS = {
    "planned_mode": "TEXT NOT NULL DEFAULT 'unlimited' CHECK(planned_mode IN ('unlimited','regular'))",
    "planned_reason": "TEXT NOT NULL DEFAULT 'documented_format'",
    "observed_mode": "TEXT CHECK(observed_mode IN ('unlimited','regular','unknown'))",
    "cloud_present": "INTEGER NOT NULL DEFAULT 0 CHECK(cloud_present IN (0,1))",
    "cloud_path": "TEXT", "cloud_hash": "TEXT", "cloud_size": "INTEGER",
    "cloud_verified": "INTEGER NOT NULL DEFAULT 0 CHECK(cloud_verified IN (0,1))",
    "cloud_observed_at": "REAL", "cloud_deleted_at": "REAL",
}
_UPLOAD_RESULT_COLUMNS = frozenset({
    "id", "job_id", "origin_job_id", "account_id", "local_path", "local_key", "remote_path",
    "source_size", "source_mtime_ns", "source_device", "source_inode",
    "remote_hash", "remote_id", "verified_metadata", "uploaded_at",
})
_STORAGE_MODES = ("unlimited", "regular")
_OBSERVED_MODES = (*_STORAGE_MODES, "unknown")
_SIZE_PARTITIONS = {
    "unlimited": f"size<={UNLIMITED_MAX_BYTES}",
    "regular": f"size>{UNLIMITED_MAX_BYTES}",
}
_LEGACY_LARGE_FILE_ERROR = "Файл больше 2 ГБ; он не отправлен обычной загрузкой."
_LARGE_FILES_DISABLED_REASON = "Файл больше 2 ГБ: этот размер не поддерживается автозагрузкой."
_REGULAR_SKIP_CODES = ("regular_size_disabled", "regular_format_disabled", "large_files_disabled")


class _RootScope(tuple):
    """Immutable validated IDs with a compact, reusable query plan.

    A scheduled party may contain thousands of physical directories. Keeping
    the complete-group compression with the immutable selection avoids both
    revalidating every ID and opening one SQL cursor per directory per file.
    Source mutations invalidate it through the persisted generation number.
    """

    def __new__(cls, values, *, database, generation, groups, partial):
        result = super().__new__(cls, values)
        result.database = database
        result.generation = generation
        result.groups = tuple(groups)
        result.partial = tuple(partial)
        return result


def _size_partition(storage_mode: str | None) -> str:
    """A planned route, separate from observed storage or an old intent."""
    if storage_mode is None:
        return "1"
    if storage_mode not in _STORAGE_MODES:
        raise ValueError("storage_mode must be 'unlimited', 'regular', or None")
    # Literal predicates match the partial indexes. A parameterized threshold
    # cannot let SQLite prove that a partial index covers the entire query.
    return "planned_mode='" + storage_mode + "'"


def _normalize_extensions(values: Iterable[str]) -> frozenset[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("Provide media extensions as a list, not a string.")
    try:
        selected = frozenset(
            (value.strip() if value.strip().startswith(".") else "." + value.strip()).casefold()
            for value in values
        )
    except (TypeError, AttributeError):
        raise ValueError("Media extensions must be strings.") from None
    if not selected or any(value in ("", ".") or "/" in value or "\\" in value or "\x00" in value
                           for value in selected):
        raise ValueError("At least one valid media extension is required.")
    return selected


class QueueError(RuntimeError):
    pass


class QueueConfigurationError(QueueError):
    pass


class QueueBusyError(QueueError):
    pass


class ScanCollisionError(QueueError):
    pass


class StaleClaimError(QueueError):
    pass


class FileChangedError(QueueError):
    """The claimed source changed; it was not marked successfully uploaded."""


class SourceUnavailableError(FileChangedError):
    """Source inspection failed; its task stays visible with evidence intact.

    A FileChangedError subclass keeps the runner's existing per-file recovery
    path: this claim is already finalized and other tasks may continue.
    """


@dataclasses.dataclass(frozen=True, slots=True)
class Root:
    id: int
    path: str
    alias: str
    remote_prefix: str


@dataclasses.dataclass(frozen=True, slots=True)
class Job:
    id: int
    root_id: int
    local_path: str
    remote_path: str
    relative_path: str
    size: int
    mtime_ns: int
    device: int
    inode: int
    attempts: int
    claim_token: str
    storage_mode: str | None = None
    planned_mode: str = "unlimited"
    planned_reason: str = "documented_format"
    progress_bytes: int = 0


@dataclasses.dataclass(slots=True)
class ScanResult:
    scan_id: int = 0
    seen: int = 0
    added: int = 0
    changed: int = 0
    moved: int = 0
    unchanged: int = 0
    missing: int = 0
    ignored_symlinks: int = 0
    error_count: int = 0
    errors: list[str] = dataclasses.field(default_factory=list)
    seconds: float = 0.0
    complete: bool = False
    cancelled: bool = False
    root_ids: list[int] | None = None

    def error(self, message: str) -> None:
        self.error_count += 1
        if len(self.errors) < 100:
            self.errors.append(message)


def _path_key(path: str | os.PathLike[str]) -> str:
    return os.path.normcase(os.path.realpath(os.fspath(path)))


def _remote_key(path: str) -> str:
    # Conservative: fail visibly instead of risking overwrite when a server
    # treats case/Unicode normalization variants as the same remote name.
    return unicodedata.normalize("NFC", path).casefold()


def _destination(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise QueueConfigurationError("Destination must be an absolute cloud path using '/' separators.")
    if any(part in (".", "..") for part in value.split("/")):
        raise QueueConfigurationError("Destination cannot contain '.' or '..' segments.")
    return "/" + "/".join(part for part in value.split("/") if part)


def _alias(value: str) -> str:
    if not isinstance(value, str) or not value or value in (".", "..") or any(ch in value for ch in ("/", "\\", "\x00")):
        raise QueueConfigurationError("Root alias must be a nonempty single cloud folder name.")
    if value != value.strip() or value.endswith("."):
        raise QueueConfigurationError("Root alias cannot have surrounding whitespace or a trailing dot.")
    return unicodedata.normalize("NFC", value)


def _snapshot(path: str) -> tuple[int, int, int, int]:
    value = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(value.st_mode):
        raise OSError(f"Not a regular, nonsymlink file: {path}")
    return value.st_size, value.st_mtime_ns, value.st_dev, value.st_ino


def _job_snapshot(job: Job) -> tuple[int, int, int, int]:
    return job.size, job.mtime_ns, job.device, job.inode


@contextlib.contextmanager
def _file_lock(path: str) -> Iterator[None]:
    """An OS lock, automatically released on process death; never deletes it."""
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    handle = os.fdopen(fd, "r+b", buffering=0)
    acquired = False
    try:
        if os.name == "nt":
            import msvcrt

            if os.fstat(fd).st_size == 0:
                handle.write(b"\x00")
            handle.seek(0)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise QueueBusyError(f"Queue is already in use: {path}") from exc
        else:
            import fcntl

            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise QueueBusyError(f"Queue is already in use: {path}") from exc
        acquired = True
        yield
    finally:
        if acquired:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
        handle.close()


class Queue:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        destination: str = "/Photos",
        account_id: str | None = None,
    ) -> None:
        self.path = str(Path(db_path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.destination = _destination(destination)
        self._mutex = threading.RLock()
        self._runner_lock_depth = 0
        self._runner_lock_owner: int | None = None
        self._scope_cache: dict[tuple[int, ...], _RootScope] = {}
        self._closed = False
        self._db = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        try:
            self._db.execute("PRAGMA busy_timeout=30000")
            if self._db.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                self._db.execute("PRAGMA journal_mode=WAL")
            # A committed intent must survive an OS/power failure before the
            # network commit is attempted. NORMAL is insufficient for that promise.
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")
            # JSON1 is bundled with supported Python/container SQLite builds.
            # It prevents large source selections from hitting bind limits.
            try:
                self._db.execute("SELECT value FROM json_each('[1]')").fetchone()
            except sqlite3.Error as exc:
                raise QueueConfigurationError("SQLite JSON support is required for large source groups.") from exc
            self._initialize(account_id)
        except BaseException:
            self._db.close()
            raise

    def _initialize(self, account_id: str | None) -> None:
        schema_version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if schema_version not in (0, 1, 2, 3, 4, 5, SCHEMA_VERSION):
            raise QueueConfigurationError(f"Unsupported queue schema {schema_version}; expected {SCHEMA_VERSION}.")
        # The hot path returns one SQLite result instead of iterating schema
        # rows in Python. sqlite_step releases the GIL for each row: several
        # dashboard readers otherwise amplify every constructor into hundreds
        # of handoffs and can starve concurrent upload workers.
        if schema_version == SCHEMA_VERSION and self._has_current_schema():
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            needs_account = account_id is not None and self._account_needs_binding(account_id)
            if (existing is not None and not needs_account
                    and self._get_meta("_media_policy_version") == MEDIA_POLICY_VERSION):
                return
        objects = {(row["type"], row["name"]) for row in self._db.execute(
            "SELECT type,name FROM sqlite_master WHERE type IN ('table','index','trigger')"
        )}
        job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
        has_schema = (_SCHEMA_OBJECTS.issubset(objects)
                      and {"source_group_id", "storage_mode", "paused", "remove_requested", *_POLICY_COLUMNS, *_TASK_COLUMNS}.issubset(job_columns)
                      and self._has_group_schema()
                      and self._has_detached_upload_results())
        if ("table", "metadata") in objects:
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            needs_account = account_id is not None and self._account_needs_binding(account_id)
            # Polling/read connections must neither take the writer lock nor
            # commit metadata. Schema inspection above is entirely read-only.
            if (has_schema and schema_version == SCHEMA_VERSION and existing is not None and not needs_account
                    and self._get_meta("_media_policy_version") == MEDIA_POLICY_VERSION):
                return
        with self._schema_upgrade_backup(schema_version, has_jobs=("table", "jobs") in objects), self._transaction():
            # A second first-opener may have migrated while this one waited.
            schema_version = self._db.execute("PRAGMA user_version").fetchone()[0]
            if schema_version not in (0, 1, 2, 3, 4, 5, SCHEMA_VERSION):
                raise QueueConfigurationError(f"Unsupported queue schema {schema_version}; expected {SCHEMA_VERSION}.")
            self._create_core_schema()
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            if existing is None:
                self._put_meta("_destination", self.destination)
            if account_id is not None:
                self._bind_account(account_id)
            job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
            if "storage_mode" not in job_columns:
                self._db.execute("ALTER TABLE jobs ADD COLUMN storage_mode TEXT "
                                 "CHECK(storage_mode IN ('unlimited','regular'))")
            for flag in ("paused", "remove_requested"):
                if flag not in job_columns:
                    self._db.execute(f"ALTER TABLE jobs ADD COLUMN {flag} INTEGER NOT NULL DEFAULT 0 CHECK({flag} IN (0,1))")
            added_policy_columns = False
            for column, definition in _POLICY_COLUMNS.items():
                if column not in job_columns:
                    self._db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
                    added_policy_columns = True
            added_task_columns = False
            for column, definition in _TASK_COLUMNS.items():
                if column not in job_columns:
                    self._db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
                    added_task_columns = True
            for table in ("upload_results", "commit_evidence"):
                columns = {row["name"] for row in self._db.execute(f"PRAGMA table_info({table})")}
                if "origin_job_id" not in columns:
                    self._db.execute(f"ALTER TABLE {table} ADD COLUMN origin_job_id INTEGER")
                    # Detached schema-2 records have lost their original task
                    # ID. Never guess an origin for those historical rows.
                    self._db.execute(f"UPDATE {table} SET origin_job_id=job_id WHERE job_id IS NOT NULL")
            if not self._has_detached_upload_results():
                self._migrate_upload_results()
            self._migrate_source_groups()
            if schema_version < 3:
                # Schema-2 claim indexes included paused rows. Rebuild once
                # so a large paused prefix does not make each claim linear.
                for name in ("jobs_pending", "jobs_pending_unlimited", "jobs_pending_regular",
                             "jobs_retry_unlimited", "jobs_retry_regular"):
                    self._db.execute(f"DROP INDEX IF EXISTS {name}")
                self._preserve_job_ids()
            if (added_policy_columns or self._get_meta("_media_policy_version") != MEDIA_POLICY_VERSION):
                self._migrate_storage_policy()
            if added_task_columns:
                self._migrate_task_details()
            if schema_version < 4 or added_policy_columns:
                # These same names previously partitioned only by size. The
                # new route also includes unsupported/unlisted extensions.
                for name in ("jobs_pending_unlimited", "jobs_pending_regular",
                             "jobs_retry_unlimited", "jobs_retry_regular", "jobs_reconcile"):
                    self._db.execute(f"DROP INDEX IF EXISTS {name}")
            self._create_queue_indexes()
            if schema_version != SCHEMA_VERSION:
                self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _has_current_schema(self) -> bool:
        """Read-only structural validation, with no process cache to go stale."""
        jobs = {"source_group_id", "storage_mode", "paused", "remove_requested", *_POLICY_COLUMNS, *_TASK_COLUMNS}
        roots = {"group_id", "recursive", "exclusions_json", "remote_subdir"}
        results = _UPLOAD_RESULT_COLUMNS | {"batch_id"}
        query = """SELECT
            (SELECT COUNT(*) FROM json_each(?) wanted JOIN sqlite_master actual
                ON actual.type=json_extract(wanted.value,'$[0]') AND actual.name=json_extract(wanted.value,'$[1]'))=?
            AND (SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name IN (SELECT value FROM json_each(?)))=?
            AND (SELECT COUNT(*) FROM pragma_table_info('roots') WHERE name IN (SELECT value FROM json_each(?)))=?
            AND (SELECT COUNT(*) FROM pragma_table_info('upload_results') WHERE name IN (SELECT value FROM json_each(?)))=?
            AND EXISTS(SELECT 1 FROM pragma_table_info('upload_results') WHERE name='job_id' AND "notnull"=0)
            AND EXISTS(SELECT 1 FROM pragma_table_info('cloud_observations') WHERE name='batch_id')
            AND EXISTS(SELECT 1 FROM pragma_foreign_key_list('upload_results')
                WHERE "table"='jobs' AND "from"='job_id' AND "to"='id' AND on_delete='SET NULL')
        """
        return bool(self._db.execute(query, (
            json.dumps(sorted(_SCHEMA_OBJECTS)), len(_SCHEMA_OBJECTS),
            json.dumps(sorted(jobs)), len(jobs), json.dumps(sorted(roots)), len(roots),
            json.dumps(sorted(results)), len(results),
        )).fetchone()[0])

    @contextlib.contextmanager
    def _schema_upgrade_backup(self, version: int, *, has_jobs: bool) -> Iterator[None]:
        """Keep a consistent old-schema queue before the first upgrade write.

        SQLite backup includes committed WAL pages and produces a standalone
        rollback file. It cannot run from inside our own write transaction.
        The advisory lock serializes upgrade openers and is released on death.
        Initialized current-schema read connections never enter this path.
        """
        if version >= SCHEMA_VERSION or not has_jobs:
            yield
            return
        with _file_lock(self.path + ".schema-upgrade.lock"):
            current = self._db.execute("PRAGMA user_version").fetchone()[0]
            if current < SCHEMA_VERSION:
                self._save_schema_backup(current)
            yield

    def _save_schema_backup(self, version: int) -> Path:
        folder = Path(self.path).parent / "backups"
        destination = folder / (f"{Path(self.path).stem}-schema{version}-before{SCHEMA_VERSION}-"
                                f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}.sqlite3")
        created = False
        try:
            folder.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            created = True
            os.close(descriptor)
            with contextlib.closing(sqlite3.connect(str(destination), timeout=30, isolation_level=None)) as backup:
                self._db.backup(backup, pages=1024)
                # A WAL-mode source must still yield one restorable file.
                backup.execute("PRAGMA journal_mode=DELETE")
                if backup.execute("PRAGMA user_version").fetchone()[0] != version:
                    raise sqlite3.DatabaseError("Source schema changed while its backup was being created")
            with destination.open("rb") as saved:
                os.fsync(saved.fileno())
            if os.name == "posix":
                directory = os.open(folder, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            return destination
        except (OSError, sqlite3.Error) as error:
            if created:
                # Only this newly reserved, incomplete copy may be removed.
                with contextlib.suppress(OSError):
                    destination.unlink()
            raise QueueConfigurationError(
                "Не удалось сохранить резервную копию очереди перед обновлением схемы. "
                "Обновление отменено; проверьте свободное место и права на каталог состояния."
            ) from error

    def _create_core_schema(self) -> None:
        # execute(), not executescript(): Python's executescript implicitly
        # commits an open transaction and would make this migration partial.
        statements = (
            """CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS source_groups (
                id INTEGER PRIMARY KEY, alias TEXT NOT NULL, alias_key TEXT NOT NULL UNIQUE,
                remote_prefix TEXT NOT NULL, owner_key TEXT UNIQUE, definition_hash TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS roots (
                id INTEGER PRIMARY KEY, path TEXT NOT NULL, path_key TEXT NOT NULL UNIQUE,
                alias TEXT NOT NULL, alias_key TEXT NOT NULL UNIQUE, remote_prefix TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY, started_at REAL NOT NULL, finished_at REAL,
                complete INTEGER NOT NULL DEFAULT 0, result_json TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY, root_id INTEGER NOT NULL REFERENCES roots(id),
                relative_path TEXT NOT NULL, local_path TEXT NOT NULL,
                local_key TEXT NOT NULL UNIQUE, remote_path TEXT NOT NULL,
                remote_key TEXT NOT NULL UNIQUE, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
                device INTEGER NOT NULL, inode TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','running','uploaded','failed','skipped')),
                attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL,
                claim_token TEXT, last_error TEXT, skip_reason TEXT,
                remote_hash TEXT, remote_id TEXT, verified_metadata TEXT,
                storage_mode TEXT CHECK(storage_mode IN ('unlimited','regular')),
                paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1)),
                remove_requested INTEGER NOT NULL DEFAULT 0 CHECK(remove_requested IN (0,1)),
                uploaded_at REAL, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                seen_scan INTEGER, present INTEGER NOT NULL DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS commit_intents (
                job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                device INTEGER NOT NULL, inode TEXT NOT NULL,
                remote_path TEXT NOT NULL, cloud_hash TEXT NOT NULL,
                created_at REAL NOT NULL, intent_json TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS commit_evidence (
                id INTEGER PRIMARY KEY, job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
                origin_job_id INTEGER,
                intent_id TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('attempted','accepted')),
                account_id TEXT, local_path TEXT, remote_path TEXT NOT NULL,
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                source_device INTEGER NOT NULL, source_inode TEXT NOT NULL,
                cloud_hash TEXT NOT NULL, created_at REAL NOT NULL, recorded_at REAL NOT NULL,
                intent_json TEXT NOT NULL, UNIQUE(intent_id,stage)
            )""",
            """CREATE TABLE IF NOT EXISTS cloud_observations (
                id INTEGER PRIMARY KEY,
                job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL, origin_job_id INTEGER,
                event TEXT NOT NULL CHECK(event IN ('stored','deleted')),
                account_id TEXT, local_path TEXT,
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                source_device INTEGER, source_inode TEXT,
                remote_path TEXT NOT NULL, cloud_hash TEXT, cloud_size INTEGER NOT NULL,
                remote_id TEXT, mode TEXT NOT NULL CHECK(mode IN ('unlimited','regular','unknown')),
                verified INTEGER NOT NULL CHECK(verified IN (0,1)),
                receipt_json TEXT NOT NULL, received_at REAL NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS rescan_intents (
                root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
                local_key TEXT PRIMARY KEY, local_path TEXT NOT NULL,
                account_id TEXT NOT NULL, destination TEXT NOT NULL,
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                device INTEGER NOT NULL, inode TEXT NOT NULL,
                remote_path TEXT NOT NULL, cloud_hash TEXT NOT NULL,
                created_at REAL NOT NULL, intent_json TEXT NOT NULL
            )""",
        )
        for statement in statements:
            self._db.execute(statement)
        if not self._db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='upload_results'").fetchone():
            self._create_upload_results("upload_results")

    def _has_group_schema(self) -> bool:
        required = {"group_id", "recursive", "exclusions_json", "remote_subdir"}
        return (required.issubset({row["name"] for row in self._db.execute("PRAGMA table_info(roots)")})
                and all("batch_id" in {row["name"] for row in self._db.execute(f"PRAGMA table_info({table})")}
                        for table in ("upload_results", "cloud_observations")))

    def _migrate_source_groups(self) -> None:
        """Preserve all legacy physical/source IDs and detached upload proof."""
        columns = {row["name"] for row in self._db.execute("PRAGMA table_info(roots)")}
        definitions = {
            "group_id": "INTEGER REFERENCES source_groups(id)",
            "recursive": "INTEGER NOT NULL DEFAULT 1 CHECK(recursive IN (0,1))",
            "exclusions_json": "TEXT NOT NULL DEFAULT '[]'",
            "remote_subdir": "TEXT NOT NULL DEFAULT ''",
        }
        for name, definition in definitions.items():
            if name not in columns:
                self._db.execute(f"ALTER TABLE roots ADD COLUMN {name} {definition}")
        now = time.time()
        cursor = self._db.execute("SELECT * FROM roots WHERE group_id IS NULL ORDER BY id")
        try:
            while rows := cursor.fetchmany(500):
                for row in rows:
                    member = {"path": row["path"], "remote_subdir": "", "recursive": True, "excluded_paths": []}
                    self._db.execute(
                        "INSERT INTO source_groups(id,alias,alias_key,remote_prefix,definition_hash,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (row["id"], row["alias"], row["alias_key"], row["remote_prefix"],
                         self._group_definition(row["alias"], row["remote_prefix"], [member]), now, now),
                    )
                    self._db.execute("UPDATE roots SET group_id=? WHERE id=?", (row["id"], row["id"]))
        finally:
            cursor.close()
        job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
        if "source_group_id" not in job_columns:
            self._db.execute("ALTER TABLE jobs ADD COLUMN source_group_id INTEGER REFERENCES source_groups(id)")
            self._db.execute("UPDATE jobs SET source_group_id=(SELECT group_id FROM roots WHERE id=jobs.root_id)")
        for table in ("upload_results", "cloud_observations"):
            if "batch_id" not in {row["name"] for row in self._db.execute(f"PRAGMA table_info({table})")}:
                self._db.execute(f"ALTER TABLE {table} ADD COLUMN batch_id TEXT")
        # Old importers and tests may still INSERT only the original columns.
        # Normal scanning supplies source_group_id and avoids this extra write.
        self._db.execute("CREATE TRIGGER IF NOT EXISTS jobs_source_group_insert AFTER INSERT ON jobs "
                         "WHEN NEW.source_group_id IS NULL BEGIN UPDATE jobs SET source_group_id="
                         "(SELECT group_id FROM roots WHERE id=NEW.root_id) WHERE id=NEW.id; END")
        self._db.execute("CREATE TRIGGER IF NOT EXISTS jobs_source_group_update AFTER UPDATE OF root_id ON jobs "
                         "WHEN NEW.root_id<>OLD.root_id BEGIN UPDATE jobs SET source_group_id="
                         "(SELECT group_id FROM roots WHERE id=NEW.root_id) WHERE id=NEW.id; END")

    def _create_upload_results(self, table: str) -> None:
        # Both callers pass internal literal table names, never user input.
        self._db.execute(f"""CREATE TABLE {table} (
            id INTEGER PRIMARY KEY, job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
            origin_job_id INTEGER,
            account_id TEXT, local_path TEXT, local_key TEXT, remote_path TEXT,
            source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
            source_device INTEGER, source_inode TEXT,
            remote_hash TEXT, remote_id TEXT, verified_metadata TEXT,
            uploaded_at REAL NOT NULL
        )""")

    def _has_detached_upload_results(self) -> bool:
        columns = {row["name"]: row for row in self._db.execute("PRAGMA table_info(upload_results)")}
        if not _UPLOAD_RESULT_COLUMNS.issubset(columns) or columns["job_id"]["notnull"]:
            return False
        return any(row["table"] == "jobs" and row["from"] == "job_id"
                   and row["to"] == "id" and row["on_delete"] == "SET NULL"
                   for row in self._db.execute("PRAGMA foreign_key_list(upload_results)"))

    def _migrate_upload_results(self) -> None:
        """Detach durable evidence from task lifetime, in bounded batches.

        Legacy evidence is retained verbatim. Its remote path is taken from
        that evidence when available; the old task mapping is a fallback.
        Legacy records did not persist filesystem identities; leave those
        fields unknown instead of attaching the task's newer inode to proof.
        """
        self._create_upload_results("upload_results_migration")
        account = self._get_meta("_account_id")
        cursor = self._db.execute(
            "SELECT r.*,j.local_path AS job_local_path,j.local_key AS job_local_key,"
            "j.remote_path AS job_remote_path FROM upload_results r "
            "LEFT JOIN jobs j ON j.id=r.job_id ORDER BY r.id"
        )
        while rows := cursor.fetchmany(500):
            records = []
            for row in rows:
                item = dict(row)
                try:
                    evidence = json.loads(item["verified_metadata"] or "{}")
                except (ValueError, TypeError):
                    evidence = {}
                if not isinstance(evidence, dict):
                    evidence = {}
                remote_path = item.get("remote_path")
                if not remote_path:
                    for key in ("remote_path", "path"):
                        candidate = evidence.get(key)
                        try:
                            remote_path = _destination(candidate)
                        except QueueConfigurationError:
                            continue
                        break
                if not remote_path:
                    remote_path = item["job_remote_path"]
                records.append((
                    item["id"], item["job_id"], item.get("origin_job_id", item["job_id"]), item.get("account_id", account),
                    item.get("local_path", item["job_local_path"]),
                    item.get("local_key", item["job_local_key"]), remote_path,
                    item["source_size"], item["source_mtime_ns"],
                    item.get("source_device"), item.get("source_inode"),
                    item["remote_hash"], item["remote_id"], item["verified_metadata"], item["uploaded_at"],
                ))
            self._db.executemany(
                "INSERT INTO upload_results_migration(id,job_id,origin_job_id,account_id,local_path,local_key,remote_path,"
                "source_size,source_mtime_ns,source_device,source_inode,remote_hash,remote_id,verified_metadata,uploaded_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", records,
            )
        cursor.close()
        self._db.execute("DROP TABLE upload_results")
        self._db.execute("ALTER TABLE upload_results_migration RENAME TO upload_results")

    def _migrate_storage_policy(self) -> None:
        """Classify legacy rows in bounded batches without inventing billing.

        Only completed uploads or exact-content observations establish a
        remote object's existence. A stored intent on a pending/failed task
        proves neither acceptance nor a storage mode.
        """
        cursor = self._db.execute("SELECT * FROM jobs ORDER BY id")
        while rows := cursor.fetchmany(500):
            plans = []
            for row in rows:
                plan = classify_media(row["local_path"], row["size"])
                plans.append((plan["mode"], plan["reason"], row["id"]))
                if row["cloud_present"] or row["observed_mode"] is not None:
                    continue
                completed = row["status"] == "uploaded"
                existing = row["status"] == "skipped" and row["skip_reason"] == "remote_exists"
                if not completed and not existing:
                    continue
                try:
                    evidence = json.loads(row["verified_metadata"] or "{}")
                except (TypeError, ValueError):
                    evidence = {}
                if not isinstance(evidence, dict):
                    evidence = {}
                mode = "unknown"
                if completed:
                    if evidence.get("is_unlim_upload") is True:
                        mode = "unlimited"
                    elif evidence.get("is_unlim_upload") is False:
                        mode = "regular"
                    elif row["storage_mode"] in _STORAGE_MODES:
                        mode = row["storage_mode"]
                    elif evidence.get("storage_mode") in _STORAGE_MODES:
                        mode = evidence["storage_mode"]
                receipt = {"migrated_from_completed_task": True, "verified": True,
                           "previous_evidence": evidence}
                when = row["uploaded_at"] if completed else evidence.get("observed_at")
                if type(when) not in (int, float) or not math.isfinite(when) or when < 0:
                    when = row["updated_at"]
                self._record_cloud_observation_row(
                    row, mode=mode, remote_hash=row["remote_hash"], remote_id=row["remote_id"],
                    metadata=receipt, received_at=when,
                )
            self._db.executemany("UPDATE jobs SET planned_mode=?,planned_reason=? WHERE id=?", plans)
        cursor.close()
        self._put_meta("_media_policy_version", MEDIA_POLICY_VERSION)

    def _migrate_task_details(self) -> None:
        """Add sort keys and only completion dates supported by old receipts."""
        cursor = self._db.execute("SELECT id,relative_path FROM jobs ORDER BY id")
        while rows := cursor.fetchmany(500):
            self._db.executemany("UPDATE jobs SET name_sort=? WHERE id=?", (
                (unicodedata.normalize("NFC", row["relative_path"].rsplit("/", 1)[-1]).casefold(), row["id"])
                for row in rows
            ))
        cursor.close()
        # Old rescans update updated_at; it is not a reliable completion date
        # for an old failed/skipped task. Leave those unknown rather than guess.
        self._db.execute(
            "UPDATE jobs SET progress_bytes=size,completed_at=CASE WHEN status='uploaded' "
            "THEN uploaded_at ELSE cloud_observed_at END WHERE status='uploaded' "
            "OR (status='skipped' AND skip_reason='remote_exists')"
        )

    def _create_queue_indexes(self) -> None:
        statements = (
            "CREATE INDEX IF NOT EXISTS roots_group ON roots(group_id,id)",
            "CREATE INDEX IF NOT EXISTS jobs_group ON jobs(source_group_id,id)",
            "CREATE INDEX IF NOT EXISTS jobs_pending_group ON jobs(source_group_id,planned_mode,id) "
            "WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0",
            "CREATE INDEX IF NOT EXISTS jobs_retry_group ON jobs(source_group_id,planned_mode,next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS jobs_reconcile_group ON jobs(source_group_id,id) WHERE present=1 AND paused=0 AND remove_requested=0 "
            "AND status IN ('pending','failed') AND cloud_present=0 AND size>0",
            "CREATE INDEX IF NOT EXISTS cloud_observations_batch ON cloud_observations(batch_id,id)",
            "CREATE INDEX IF NOT EXISTS cloud_observations_path ON cloud_observations(account_id,remote_path,id)",
            "CREATE INDEX IF NOT EXISTS upload_results_batch ON upload_results(batch_id,id)",
            "CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,present,next_attempt,id)",
            "CREATE INDEX IF NOT EXISTS jobs_pending ON jobs(id) WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0",
            "CREATE INDEX IF NOT EXISTS jobs_pending_unlimited ON jobs(id) "
            "WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0 AND planned_mode='unlimited'",
            "CREATE INDEX IF NOT EXISTS jobs_pending_regular ON jobs(id) "
            "WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0 AND planned_mode='regular'",
            "CREATE INDEX IF NOT EXISTS jobs_retry ON jobs(next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS jobs_retry_unlimited ON jobs(next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL AND planned_mode='unlimited'",
            "CREATE INDEX IF NOT EXISTS jobs_retry_regular ON jobs(next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL AND planned_mode='regular'",
            "CREATE INDEX IF NOT EXISTS jobs_pending_root ON jobs(root_id,planned_mode,id) "
            "WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0",
            "CREATE INDEX IF NOT EXISTS jobs_retry_root ON jobs(root_id,planned_mode,next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS jobs_root_scan ON jobs(root_id,seen_scan)",
            "CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status,id)",
            "CREATE INDEX IF NOT EXISTS jobs_current ON jobs(id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_current_status ON jobs(status,id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_current_created ON jobs(created_at,id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_current_status_created ON jobs(status,created_at,id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_paused_created ON jobs(created_at,id) WHERE present=1 AND paused=1",
            "CREATE INDEX IF NOT EXISTS jobs_observed_created ON jobs(observed_mode,created_at,id) WHERE present=1 AND cloud_present=1",
            "CREATE INDEX IF NOT EXISTS jobs_source_identity ON jobs(root_id,device,inode,size,mtime_ns)",
            "CREATE INDEX IF NOT EXISTS jobs_paused ON jobs(id) WHERE present=1 AND paused=1",
            "CREATE INDEX IF NOT EXISTS upload_results_job ON upload_results(job_id)",
            "CREATE INDEX IF NOT EXISTS commit_evidence_job ON commit_evidence(job_id)",
            "CREATE INDEX IF NOT EXISTS upload_results_origin ON upload_results(origin_job_id)",
            "CREATE INDEX IF NOT EXISTS commit_evidence_origin ON commit_evidence(origin_job_id)",
            "CREATE INDEX IF NOT EXISTS cloud_observations_job ON cloud_observations(job_id)",
            "CREATE INDEX IF NOT EXISTS cloud_observations_origin ON cloud_observations(origin_job_id)",
            "CREATE INDEX IF NOT EXISTS jobs_reconcile ON jobs(id) WHERE present=1 AND paused=0 AND remove_requested=0 "
            "AND status IN ('pending','failed') AND cloud_present=0 AND size>0",
            "CREATE INDEX IF NOT EXISTS jobs_reconcile_root ON jobs(root_id,id) WHERE present=1 AND paused=0 AND remove_requested=0 "
            "AND status IN ('pending','failed') AND cloud_present=0 AND size>0",
        )
        for statement in statements:
            self._db.execute(statement)
        # Match each allowed ORDER BY for the full queue and filtered tabs.
        # An uncommon filter must not scan a 300k-row unrelated sort prefix.
        for field, expression in _SORT_EXPRESSIONS.items():
            # A source-scoped UI must neither sort its entire 300k-row group
            # for each page nor walk another group's matching status prefix.
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_group_sort_{field} ON jobs(source_group_id,({expression}),id) WHERE present=1")
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_group_filter_{field} ON jobs(source_group_id,({_FILTER_STATE}),({expression}),id) WHERE present=1")
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_group_paused_{field} ON jobs(source_group_id,({expression}),id) WHERE present=1 AND paused=1")
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_group_observed_{field} ON jobs(source_group_id,observed_mode,({expression}),id) WHERE present=1 AND cloud_present=1")
            # Remote-existing tasks have their own category. A few manual
            # skips must not walk hundreds of thousands of adopted objects.
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_filter_{field} ON jobs(({_FILTER_STATE}),({expression}),id) WHERE present=1")
            if field == "created_at":
                continue  # Schema-4 indexes cover the other three categories.
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_sort_{field} ON jobs(({expression}),id) WHERE present=1")
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_paused_{field} ON jobs(({expression}),id) WHERE present=1 AND paused=1")
            self._db.execute(f"CREATE INDEX IF NOT EXISTS jobs_observed_{field} ON jobs(observed_mode,({expression}),id) WHERE present=1 AND cloud_present=1")

    def _check_destination(self, existing: str | None) -> None:
        if existing is not None and existing != self.destination:
            raise QueueConfigurationError(
                f"This database belongs to cloud destination {existing!r}, not {self.destination!r}. "
                "Use a separate database for another destination."
            )

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")

    def close(self) -> None:
        with self._mutex:
            if not self._closed:
                self._db.close()
                self._closed = True

    def __enter__(self) -> Queue:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @contextlib.contextmanager
    def exclusive_run(self) -> Iterator[None]:
        """Own the OS run lock; nesting is allowed only on this owning thread.

        The web controller keeps this lock over account checks, probe, and run,
        so a separate CLI cannot rotate credentials between those operations.
        """
        owner = threading.get_ident()
        with self._mutex:
            nested = self._runner_lock_depth > 0
            if nested:
                if self._runner_lock_owner != owner:
                    raise QueueBusyError("This queue is already owned by another thread.")
                self._runner_lock_depth += 1
        if nested:
            try:
                yield
            finally:
                with self._mutex:
                    self._runner_lock_depth -= 1
            return
        with _file_lock(self.path + ".run.lock"):
            with self._mutex:
                self._runner_lock_depth = 1
                self._runner_lock_owner = owner
            try:
                yield
            finally:
                with self._mutex:
                    self._runner_lock_depth = 0
                    self._runner_lock_owner = None

    def _get_meta(self, key: str, default: Any = None) -> Any:
        row = self._db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def _put_meta(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        self._db.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, encoded),
        )

    def set_metadata(self, key: str, value: Any) -> None:
        """Persist transport capability evidence/settings, never credentials."""
        if not key or key.startswith("_"):
            raise QueueConfigurationError("Metadata keys beginning with '_' are reserved.")
        with self._transaction():
            self._put_meta(key, value)

    def get_metadata(self, key: str, default: Any = None) -> Any:
        with self._mutex:
            return self._get_meta(key, default)

    def _stored_discovery_extensions(self) -> frozenset[str] | None:
        stored = self._get_meta("extensions")
        if stored is None:
            return None
        if not isinstance(stored, list):
            raise QueueConfigurationError("Invalid saved media extension settings.")
        try:
            return _normalize_extensions(stored)
        except ValueError:
            raise QueueConfigurationError("Invalid saved media extension settings.") from None

    def discovery_extensions(self) -> frozenset[str]:
        """Return the current default or the user's explicitly selected filter.

        Older versions persisted the photo-only default without distinguishing
        it from an explicit filter. A new custom marker preserves that choice
        when users deliberately select the exact former photo-only set.
        """
        with self._mutex:
            stored = self._stored_discovery_extensions()
            if stored is None or (stored in (PHOTO_EXTENSIONS, LEGACY_PHOTO_EXTENSIONS, LEGACY_MEDIA_EXTENSIONS)
                                  and self._get_meta("extensions_custom") is not True):
                return MEDIA_EXTENSIONS
            return stored

    def set_discovery_extensions(self, extensions: Iterable[str], *, custom: bool | None = None) -> frozenset[str]:
        """Save a normalized filter and its optional explicit-choice marker together."""
        selected = _normalize_extensions(extensions)
        if custom is not None and type(custom) is not bool:
            raise ValueError("custom must be a boolean")
        with self._transaction():
            self._put_meta("extensions", sorted(selected))
            if custom is not None:
                self._put_meta("extensions_custom", custom)
        return selected

    def upgrade_media_discovery(self) -> bool:
        """Expand the old default once, preserving every job and upload result.

        Call under exclusive_run(), before starting work. Unchanged settings
        take only the read path, so repeated startup checks do not write to WAL.
        """
        def needs_upgrade() -> bool:
            if self._get_meta("extensions_custom") is True:
                return False
            stored = self._stored_discovery_extensions()
            if stored is not None:
                return stored in (PHOTO_EXTENSIONS, LEGACY_PHOTO_EXTENSIONS, LEGACY_MEDIA_EXTENSIONS)
            last_scan = self._get_meta("last_scan")
            return isinstance(last_scan, dict) and last_scan.get("complete") is True

        with self._mutex:
            if not needs_upgrade():
                return False
            with self._transaction():
                # Recheck after acquiring the database writer lock.
                if not needs_upgrade():
                    return False
                self._put_meta("extensions", sorted(MEDIA_EXTENSIONS))
                last_scan = self._get_meta("last_scan")
                if isinstance(last_scan, dict) and last_scan.get("complete") is True:
                    self._put_meta("last_scan", {**last_scan, "complete": False,
                                                  "reason": "media_formats_added"})
            return True

    @property
    def account_id(self) -> str | None:
        return self.get_metadata("_account_id")

    def _account_needs_binding(self, account_id: str) -> bool:
        if not isinstance(account_id, str) or not account_id.strip():
            raise QueueConfigurationError("A stable, nonempty account identifier is required.")
        existing = self._get_meta("_account_id")
        if existing is not None and existing != account_id:
            raise QueueConfigurationError("This queue is bound to a different account; use a separate database.")
        return existing is None

    def _bind_account(self, account_id: str) -> None:
        if self._account_needs_binding(account_id):
            self._put_meta("_account_id", account_id)

    def bind_account(self, account_id: str) -> None:
        """Call with the authenticated account's stable ID before any network write."""
        with self._transaction():
            self._bind_account(account_id)

    def _require_idle_sources(self) -> None:
        if self._get_meta("_scan_token"):
            raise QueueBusyError("Cannot change source roots during a scan.")
        if self._db.execute("SELECT 1 FROM jobs WHERE status='running' LIMIT 1").fetchone():
            raise QueueBusyError("Pause/finish uploads or recover interrupted tasks before changing sources.")

    @staticmethod
    def _root(row: sqlite3.Row) -> Root:
        return Root(row["id"], row["path"], row["alias"], row["remote_prefix"])

    def _root_id_high_water(self) -> int:
        stored = self._get_meta("_root_id_high_water", 0)
        if type(stored) is not int or stored < 0:
            raise QueueConfigurationError("Invalid saved source identifier sequence.")
        existing = self._db.execute("SELECT COALESCE(MAX(id),0) FROM roots").fetchone()[0]
        return max(stored, existing)

    def _job_id_high_water(self) -> int:
        stored = self._get_meta("_job_id_high_water", 0)
        if type(stored) is not int or stored < 0:
            raise QueueConfigurationError("Invalid saved task identifier sequence.")
        existing = self._db.execute("SELECT COALESCE(MAX(id),0) FROM jobs").fetchone()[0]
        return max(stored, existing)

    def _preserve_job_ids(self) -> int:
        highest = self._job_id_high_water()
        if self._get_meta("_job_id_high_water") != highest:
            self._put_meta("_job_id_high_water", highest)
        return highest

    def _archive_intent(self, job_id: int) -> None:
        """Preserve attempted/accepted receipts separately from verified files."""
        row = self._db.execute(
            "SELECT c.*,j.local_path FROM commit_intents c JOIN jobs j ON j.id=c.job_id WHERE c.job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return
        try:
            intent = json.loads(row["intent_json"])
        except (ValueError, TypeError):
            intent = {}
        if not isinstance(intent, dict):
            intent = {}
        intent_id = intent.get("intent_id")
        if not isinstance(intent_id, str) or not intent_id:
            # Legacy attempts have no UUID. This fingerprint uses only fields
            # immutable within one attempt, never its later confirmation.
            identity = [self._get_meta("_account_id"), row["job_id"], row["created_at"], row["remote_path"],
                        row["cloud_hash"], row["source_size"], row["source_mtime_ns"], row["device"], row["inode"]]
            intent_id = "legacy-" + hashlib.sha256(json.dumps(identity).encode("utf-8")).hexdigest()
        mode = intent.get("storage_mode", "unlimited")
        stage = "accepted" if mode in _STORAGE_MODES and intent.get(mode + "_confirmed") is True else "attempted"
        self._db.execute(
            "INSERT INTO commit_evidence(job_id,origin_job_id,intent_id,stage,account_id,local_path,remote_path,"
            "source_size,source_mtime_ns,source_device,source_inode,cloud_hash,created_at,recorded_at,intent_json) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(intent_id,stage) DO NOTHING",
            (job_id, job_id, intent_id, stage, intent.get("account_id", self._get_meta("_account_id")), row["local_path"],
             row["remote_path"], row["source_size"], row["source_mtime_ns"], row["device"], row["inode"],
             row["cloud_hash"], row["created_at"], time.time(), row["intent_json"]),
        )

    def _drop_intent(self, job_id: int) -> None:
        self._archive_intent(job_id)
        self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (job_id,))

    def _delete_tasks(self, where: str, parameters: tuple = ()) -> int:
        """Internal transaction-only deletion, with immutable evidence kept."""
        self._preserve_job_ids()
        cursor = self._db.execute(
            f"SELECT c.job_id FROM commit_intents c JOIN jobs ON jobs.id=c.job_id WHERE {where}", parameters,
        )
        while rows := cursor.fetchmany(500):
            for row in rows:
                self._archive_intent(row["job_id"])
        cursor.close()
        return self._db.execute(f"DELETE FROM jobs WHERE {where}", parameters).rowcount

    def _finalize_control(self, job_id: int, *, uploaded: bool = False) -> bool:
        """Resolve a durable request only after the owning worker has drained."""
        row = self._db.execute("SELECT paused,remove_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is not None and row["remove_requested"]:
            self._delete_tasks("jobs.id=?", (job_id,))
            return True
        if row is not None and uploaded and row["paused"]:
            # The remote commit already finished; a late pause cannot undo it.
            self._db.execute("UPDATE jobs SET paused=0 WHERE id=?", (job_id,))
        return False

    @staticmethod
    def _task_dict(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        for field in ("paused", "remove_requested", "cloud_present", "cloud_verified"):
            item[field] = bool(item[field])
        item["name"] = item["relative_path"].rsplit("/", 1)[-1]
        completed_file = item["status"] == "uploaded" or (item["status"] == "skipped" and item["skip_reason"] == "remote_exists")
        item["progress_percent"] = (100.0 if completed_file else
                                    min(100.0, 100.0 * item["progress_bytes"] / item["size"]) if item["size"] > 0 else 0.0)
        item["cloud_path_matches"] = (unicodedata.normalize("NFC", item["cloud_path"]) ==
                                      unicodedata.normalize("NFC", item["remote_path"])) if item["cloud_present"] and item["cloud_path"] else None
        item["status_label"] = ("Выполнено" if item["status"] == "uploaded" else
                                "Уже в облаке" if item["status"] == "skipped" and item["skip_reason"] == "remote_exists" else
                                "На паузе" if item["paused"] else
                                {"running": "Загружается", "failed": "Ошибка", "skipped": "Пропущено"}.get(item["status"], "Ожидает"))
        return item

    def request_control(self, job_id: int, action: str) -> dict[str, Any] | None:
        """Request a task action without revoking a live worker's claim.

        The controller owns the run lock when idle, or its running pipeline.
        This short transaction can run from another thread/connection while
        the worker owns the run lock. Task removal never invalidates last_scan.
        """
        if action not in ("pause", "resume", "start", "remove", "skip", "return"):
            raise ValueError("Task action must be pause, resume, start, remove, skip, or return")
        with self._transaction():
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Wait for scanning to finish before changing tasks.")
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or (not row["present"] and action != "remove"):
                raise QueueConfigurationError(f"Unknown current task: {job_id}")
            if action == "remove":
                if row["status"] != "running":
                    self._delete_tasks("jobs.id=?", (job_id,))
                    return None
                self._db.execute("UPDATE jobs SET remove_requested=1,updated_at=? WHERE id=?", (time.time(), job_id))
            elif row["remove_requested"]:
                raise QueueBusyError("This task is already being removed.")
            elif action == "skip":
                if row["status"] not in ("pending", "failed"):
                    raise QueueBusyError("Only a waiting, paused, or failed task can be skipped.")
                now = time.time()
                # A skip parks work; it must not discard an uncertain add.
                self._db.execute(
                    "UPDATE jobs SET status='skipped',skip_reason='manual_skipped',paused=0,claim_token=NULL,"
                    "next_attempt=NULL,last_error=NULL,completed_at=?,updated_at=? WHERE id=?", (now, now, job_id),
                )
            elif action == "return":
                if row["status"] != "skipped" or row["skip_reason"] == "remote_exists":
                    raise QueueBusyError("Only a skipped transfer can be returned to the queue.")
                self._db.execute(
                    "UPDATE jobs SET paused=0,status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                    "last_error=NULL,skip_reason=NULL,completed_at=NULL,updated_at=? WHERE id=?", (time.time(), job_id),
                )
            elif row["status"] == "uploaded" or (row["status"] == "skipped" and row["skip_reason"] == "remote_exists"):
                return self._task_dict(row)
            elif action == "pause":
                self._db.execute("UPDATE jobs SET paused=1,updated_at=? WHERE id=?", (time.time(), job_id))
            elif row["status"] == "running":
                self._db.execute("UPDATE jobs SET paused=0,updated_at=? WHERE id=?", (time.time(), job_id))
            else:
                self._db.execute(
                    "UPDATE jobs SET paused=0,status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                    "last_error=NULL,skip_reason=NULL,completed_at=NULL,updated_at=? WHERE id=?", (time.time(), job_id),
                )
            return self._task_dict(self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def delete_task_final(self, job_id: int) -> bool:
        """Remove one drained task, preserving every remote receipt.

        File deletion belongs to the controller's explicit deletion operation.
        This final queue step never touches local or remote files.
        """
        if type(job_id) is not int or job_id < 1:
            raise ValueError("job_id must be a positive integer")
        with self._transaction():
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Wait for scanning to finish before deleting tasks.")
            row = self._db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return False
            if row["status"] == "running":
                raise QueueBusyError("Stop this task and wait for its worker before deleting it.")
            return bool(self._delete_tasks("jobs.id=?", (job_id,)))

    def record_task_error(self, job_id: int, message: str) -> dict[str, Any]:
        """Keep an idle task after an incomplete management/deletion action.

        This is not a worker completion and must not fabricate a claim. The
        cloud receipts, local presence, pause and source identity stay intact.
        """
        if type(job_id) is not int or job_id < 1:
            raise ValueError("job_id must be a positive integer")
        with self._transaction():
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Wait for scanning to finish before changing tasks.")
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise QueueConfigurationError(f"Unknown task: {job_id}")
            if row["status"] == "running":
                raise QueueBusyError("Stop this task and wait for its worker before recording an action error.")
            now = time.time()
            self._db.execute(
                "UPDATE jobs SET status='failed',next_attempt=NULL,claim_token=NULL,skip_reason=NULL,"
                "remove_requested=0,last_error=?,completed_at=?,updated_at=? WHERE id=?", (str(message)[:16384], now, now, job_id),
            )
            return self._task_dict(self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def record_progress(self, job: Job, transferred_bytes: int) -> int:
        """Persist a claim's byte high-water mark; callers throttle writes.

        A pause/restart must retain the last observed transfer position. A
        delayed callback from an obsolete claim cannot change a newer task.
        Source replacement and a fresh rescan deliberately start new progress.
        """
        if type(transferred_bytes) is not int or transferred_bytes < 0:
            raise ValueError("transferred_bytes must be a nonnegative integer")
        with self._transaction():
            row = self._require_claim(job)
            value = min(row["size"], max(row["progress_bytes"], transferred_bytes))
            if value != row["progress_bytes"]:
                self._db.execute("UPDATE jobs SET progress_bytes=? WHERE id=?", (value, job.id))
            return value

    def task_control(self, job: Job) -> str | None:
        """Read one live claim's cancellation request, without database writes."""
        with self._mutex:
            row = self._require_claim(job)
            return "remove" if row["remove_requested"] else "pause" if row["paused"] else None

    def clear_tasks(self, scope: str = "all", *, root_ids: Sequence[int] | None = None) -> int:
        """Clear idle queue tasks; source registrations and physical files stay."""
        if scope not in ("all", "finished"):
            raise ValueError("Task clear scope must be all or finished")
        where = "1" if scope == "all" else "(status IN ('uploaded','skipped') OR (status='failed' AND next_attempt IS NULL))"
        selected = self._selected_root_ids(root_ids)
        root_filter, parameters = self._root_filter(selected)
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            return self._delete_tasks(f"({where}) AND {root_filter}", parameters)

    def reset_for_scan(self, root_ids: Sequence[int] | None = None) -> int:
        """Start a new task snapshot without erasing uncertain upload evidence.

        Only the selected sources are reset. Existing history is detached by
        _delete_tasks; only currently unresolved intents enter the recovery
        ledger. The next scan creates new IDs and no historical presence flags.
        A later claim can restore a matching intent, while cloud reconciliation
        remains free to resolve every newly discovered task first.
        """
        selected_roots = self._selected_root_ids(root_ids)
        where, parameters = self._root_filter(selected_roots)
        with self.exclusive_run(), _file_lock(self.path + ".scan.lock"), self._transaction():
            self._require_idle_sources()
            selected_roots = self._selected_root_ids(selected_roots)
            if not self._db.execute("SELECT 1 FROM roots LIMIT 1").fetchone():
                raise QueueConfigurationError("Register at least one source root before scanning.")
            account = self._get_meta("_account_id")
            cursor = self._db.execute(
                "SELECT jobs.*,c.source_size AS intent_size,c.source_mtime_ns AS intent_mtime,"
                "c.device AS intent_device,c.inode AS intent_inode,c.remote_path AS intent_path,"
                "c.cloud_hash AS intent_hash,c.created_at AS intent_created,c.intent_json AS saved_intent "
                f"FROM jobs JOIN commit_intents c ON c.job_id=jobs.id WHERE {where} ORDER BY jobs.id", parameters,
            )
            try:
                while rows := cursor.fetchmany(500):
                    for row in rows:
                        try:
                            intent = json.loads(row["saved_intent"])
                        except (ValueError, TypeError):
                            raise QueueError("Saved commit intent is invalid; the previous task list was retained.") from None
                        if not isinstance(intent, dict) or intent.get("storage_mode", "unlimited") not in _STORAGE_MODES:
                            raise QueueError("Saved commit intent is invalid; the previous task list was retained.")
                        if not account or intent.get("account_id") != account or intent.get("destination") != self.destination:
                            continue
                        receipt = self._current_storage_observation(row, row["intent_hash"])
                        if receipt is not None:
                            intent["_rescan_storage_observation"] = receipt
                        elif row["cloud_deleted_at"] is not None:
                            intent.pop("_rescan_storage_observation", None)
                        self._db.execute(
                            "INSERT INTO rescan_intents(root_id,local_key,local_path,account_id,destination,"
                            "source_size,source_mtime_ns,device,inode,remote_path,cloud_hash,created_at,intent_json) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(local_key) DO UPDATE SET "
                            "root_id=excluded.root_id,local_path=excluded.local_path,account_id=excluded.account_id,"
                            "destination=excluded.destination,source_size=excluded.source_size,source_mtime_ns=excluded.source_mtime_ns,"
                            "device=excluded.device,inode=excluded.inode,remote_path=excluded.remote_path,cloud_hash=excluded.cloud_hash,"
                            "created_at=excluded.created_at,intent_json=excluded.intent_json",
                            (row["root_id"], row["local_key"], row["local_path"], account, self.destination,
                             row["intent_size"], row["intent_mtime"], row["intent_device"], row["intent_inode"],
                             row["intent_path"], row["intent_hash"], row["intent_created"],
                             json.dumps(intent, ensure_ascii=False, allow_nan=False)),
                        )
            finally:
                cursor.close()
            count = self._delete_tasks(where, parameters)
            self._put_meta("last_scan", {"complete": False, "reason": "fresh_scan",
                                         "root_ids": list(selected_roots) if selected_roots is not None else None})
            return count

    def _restore_rescan_intent(self, job: Job) -> Job:
        """Attach one unresolved attempt only after its new task's source check."""
        with self._mutex:
            saved = self._db.execute("SELECT * FROM rescan_intents WHERE local_key=?", (os.path.normcase(job.local_path),)).fetchone()
            if saved is None:
                return job
        with self._transaction():
            current = self._require_claim(job)
            saved = self._db.execute("SELECT * FROM rescan_intents WHERE local_key=?", (current["local_key"],)).fetchone()
            if saved is None or self._db.execute("SELECT 1 FROM commit_intents WHERE job_id=?", (job.id,)).fetchone():
                return job
            fingerprint = (saved["source_size"], saved["source_mtime_ns"], saved["device"], int(saved["inode"]))
            if (saved["root_id"] != job.root_id or saved["account_id"] != self._get_meta("_account_id")
                    or saved["destination"] != self.destination or saved["remote_path"] != job.remote_path
                    or fingerprint != _job_snapshot(job)):
                return job
            try:
                intent = json.loads(saved["intent_json"])
            except (ValueError, TypeError):
                raise QueueError("Saved rescan recovery intent is invalid.") from None
            if not isinstance(intent, dict) or intent.get("storage_mode", "unlimited") not in _STORAGE_MODES:
                raise QueueError("Saved rescan recovery intent is invalid.")
            self._db.execute(
                "INSERT INTO commit_intents(job_id,source_size,source_mtime_ns,device,inode,remote_path,cloud_hash,created_at,intent_json) "
                "VALUES(?,?,?,?,?,?,?,?,?)", (job.id, saved["source_size"], saved["source_mtime_ns"], saved["device"], saved["inode"],
                                               saved["remote_path"], saved["cloud_hash"], saved["created_at"], saved["intent_json"]),
            )
            self._db.execute("UPDATE jobs SET storage_mode=? WHERE id=?", (intent.get("storage_mode", "unlimited"), job.id))
            self._db.execute("DELETE FROM rescan_intents WHERE local_key=?", (current["local_key"],))
            return dataclasses.replace(job, storage_mode=intent.get("storage_mode", "unlimited"))

    @staticmethod
    def _group_definition(alias: str, remote_prefix: str, members: Sequence[Mapping[str, Any]]) -> str:
        canonical = [{"path": item["path"], "remote_subdir": item["remote_subdir"],
                      "recursive": bool(item["recursive"]), "excluded_paths": list(item["excluded_paths"])}
                     for item in members]
        canonical.sort(key=lambda item: item["path"])
        encoded = json.dumps([alias, remote_prefix, canonical], ensure_ascii=False,
                             sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _relative_directory(value: Any, *, allow_empty: bool = False) -> str:
        if (not isinstance(value, str) or "\x00" in value or "\\" in value or value.startswith("/")
                or any(part in (".", "..") for part in value.split("/"))):
            raise QueueConfigurationError("Directory selections must be relative POSIX paths without '.' or '..'.")
        result = "/".join(part for part in value.split("/") if part)
        if not result and not allow_empty:
            raise QueueConfigurationError("An excluded directory must be below its source.")
        return result

    def _normalize_members(self, members: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        if (not isinstance(members, Sequence) or isinstance(members, (str, bytes))
                or not 1 <= len(members) <= _MAX_SOURCE_MEMBERS):
            raise QueueConfigurationError(f"A source needs 1..{_MAX_SOURCE_MEMBERS} directories.")
        result = []
        seen = set()
        for supplied in members:
            if not isinstance(supplied, Mapping):
                raise QueueConfigurationError("Each source directory must be an object.")
            try:
                path = _path_key(Path(supplied["path"]).expanduser())
            except (KeyError, TypeError, ValueError, OSError):
                raise QueueConfigurationError("A valid local source path is required.") from None
            if not os.path.isdir(path):
                raise QueueConfigurationError(f"Source root is not an accessible directory: {path}")
            if path in seen:
                raise QueueConfigurationError(f"A directory appears twice in this source: {path}")
            seen.add(path)
            recursive = supplied.get("recursive", True)
            if type(recursive) is not bool:
                raise QueueConfigurationError("recursive must be true or false.")
            subdir = self._relative_directory(supplied.get("remote_subdir", ""), allow_empty=True)
            excluded = supplied.get("excluded_paths", [])
            if (not isinstance(excluded, Sequence) or isinstance(excluded, (str, bytes))
                    or len(excluded) > _MAX_SOURCE_MEMBERS):
                raise QueueConfigurationError("excluded_paths must be a bounded list of relative directory paths.")
            # Descendants of an already excluded directory add no new rule.
            compact = []
            ordered_exclusions = sorted(
                {os.path.normcase(self._relative_directory(item)).replace(os.sep, "/") for item in excluded},
                key=lambda value: value.split("/"),
            )
            for relative in ordered_exclusions:
                if not compact or not (relative == compact[-1] or relative.startswith(compact[-1] + "/")):
                    compact.append(relative)
            if not recursive and compact:
                raise QueueConfigurationError("A direct-files source cannot also contain excluded subdirectories.")
            result.append({"path": path, "remote_subdir": unicodedata.normalize("NFC", subdir),
                           "recursive": recursive, "excluded_paths": compact})
        return result

    @staticmethod
    def _member_from_row(row: sqlite3.Row) -> dict[str, Any]:
        return {"id": row["id"], "path": row["path"], "remote_subdir": row["remote_subdir"],
                "remote_prefix": row["remote_prefix"], "recursive": bool(row["recursive"]),
                "excluded_paths": json.loads(row["exclusions_json"])}

    @staticmethod
    def _member_summary(row: sqlite3.Row) -> dict[str, Any]:
        """Bounded presentation, even for a directory with 50k exclusions."""
        # SQL computes the count/preview; state reads never deserialize the
        # complete exclusion document for each displayed source directory.
        return {"id": row["id"], "path": row["path"], "remote_subdir": row["remote_subdir"],
                "remote_prefix": row["remote_prefix"], "recursive": bool(row["recursive"]),
                "excluded_count": row["excluded_count"],
                "excluded_preview": [value for value in (row["excluded_0"], row["excluded_1"], row["excluded_2"]) if value is not None]}

    @staticmethod
    def _member_summary_columns() -> str:
        return ("id,path,remote_subdir,remote_prefix,recursive,json_array_length(exclusions_json) AS excluded_count,"
                "json_extract(exclusions_json,'$[0]') AS excluded_0,json_extract(exclusions_json,'$[1]') AS excluded_1,"
                "json_extract(exclusions_json,'$[2]') AS excluded_2")

    @staticmethod
    def _covers_descendant(member: Mapping[str, Any], descendant: str) -> bool:
        if member["path"] == descendant:
            return True
        if not member["recursive"]:
            return False
        relative = Path(descendant).relative_to(member["path"]).as_posix()
        return not any(relative == path or relative.startswith(path + "/") for path in member["excluded_paths"])

    def _validate_member_overlaps(self, members: Sequence[Mapping[str, Any]], *, ignore_group: int | None = None) -> None:
        """Check ancestors/descendants via paths, without an all-pairs scan."""
        stack: list[Mapping[str, Any]] = []
        for item in sorted(members, key=lambda member: Path(member["path"]).parts):
            while stack:
                try:
                    inside = os.path.commonpath((stack[-1]["path"], item["path"])) == stack[-1]["path"]
                except ValueError:
                    inside = False
                if inside:
                    break
                stack.pop()
            if any(self._covers_descendant(parent, item["path"]) for parent in stack):
                raise QueueConfigurationError("Source directories overlap; exclude the child or choose direct files only.")
            stack.append(item)
            ancestors = [str(path) for path in (Path(item["path"]), *Path(item["path"]).parents)]
            ancestor_where = ",".join("?" for _ in ancestors)
            for row in self._db.execute(
                f"SELECT * FROM roots WHERE path_key IN ({ancestor_where}) AND (? IS NULL OR group_id<>?)",
                (*ancestors, ignore_group, ignore_group),
            ):
                if self._covers_descendant(self._member_from_row(row), item["path"]):
                    raise QueueConfigurationError(f"Source directory overlaps an existing source: {row['path']}")
            if not item["recursive"]:
                continue
            prefix = item["path"].rstrip(os.sep) + os.sep
            upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
            for row in self._db.execute(
                "SELECT path FROM roots WHERE path_key>=? AND path_key<? AND (? IS NULL OR group_id<>?)",
                (prefix, upper, ignore_group, ignore_group),
            ):
                if self._covers_descendant(item, row["path"]):
                    raise QueueConfigurationError(f"Source directory overlaps an existing source: {row['path']}")

    def _sources_changed(self, reason: str) -> None:
        self._put_meta("_sources_generation", self._get_meta("_sources_generation", 0) + 1)
        self._scope_cache.clear()
        self._put_meta("last_scan", {"complete": False, "reason": reason})

    def _source_id_high_water(self) -> int:
        return max(self._root_id_high_water(), self._db.execute("SELECT COALESCE(MAX(id),0) FROM source_groups").fetchone()[0])

    def _insert_group_member(self, group_id: int, root_id: int, alias: str, prefix: str,
                             member: Mapping[str, Any]) -> None:
        remote = prefix.rstrip("/") + "/" + member["remote_subdir"] if member["remote_subdir"] else prefix
        self._db.execute(
            "INSERT INTO roots(id,path,path_key,alias,alias_key,remote_prefix,group_id,recursive,exclusions_json,remote_subdir) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (root_id, member["path"], member["path"], alias, "\x00member:" + str(root_id), remote,
             group_id, int(member["recursive"]), json.dumps(member["excluded_paths"], ensure_ascii=False), member["remote_subdir"]),
        )

    def add_source_group(self, members: Sequence[Mapping[str, Any]], alias: str,
                         remote_prefix: str | None = None, owner_key: str | None = None) -> dict[str, Any]:
        """Atomically create a logical source containing selected directories.

        owner_key makes planner activation safe to repeat after a crash. Only
        the exact original normalized definition can reuse an existing key.
        """
        alias = _alias(alias)
        prefix = _destination(remote_prefix) if remote_prefix is not None else self.destination.rstrip("/") + "/" + alias
        normalized = self._normalize_members(members)
        if owner_key is not None and (not isinstance(owner_key, str) or not 1 <= len(owner_key) <= 200
                                      or any(ord(char) < 32 for char in owner_key)):
            raise QueueConfigurationError("Invalid source owner key.")
        definition = self._group_definition(alias, prefix, normalized)
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            if owner_key is not None:
                existing = self._db.execute("SELECT * FROM source_groups WHERE owner_key=?", (owner_key,)).fetchone()
                if existing is not None:
                    if existing["definition_hash"] != definition:
                        raise QueueConfigurationError("This source owner already has a different directory selection.")
                    return self.source_group(existing["id"])
            if self._db.execute("SELECT 1 FROM source_groups WHERE alias_key=?", (_remote_key(alias),)).fetchone():
                raise ScanCollisionError(f"Root alias {alias!r} is already used by another source directory.")
            self._validate_member_overlaps(normalized)
            group_id = self._source_id_high_water() + 1
            now = time.time()
            self._db.execute(
                "INSERT INTO source_groups(id,alias,alias_key,remote_prefix,owner_key,definition_hash,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)", (group_id, alias, _remote_key(alias), prefix, owner_key, definition, now, now),
            )
            for index, member in enumerate(normalized):
                self._insert_group_member(group_id, group_id + index, alias, prefix, member)
            self._put_meta("_root_id_high_water", group_id + len(normalized) - 1)
            self._sources_changed("source_added")
            return self.source_group(group_id)

    def source_groups(self, *, include_members: bool = False) -> list[dict[str, Any]]:
        with self._mutex:
            return [self._source_group_dict(row, include_members=include_members)
                    for row in self._db.execute("SELECT * FROM source_groups ORDER BY id")]

    def source_groups_page(self, *, limit: int = 50, offset: int = 0, kind: str = "all", q: str = "") -> dict[str, Any]:
        """Bound the number of logical sources in dashboard/management reads."""
        if (type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0
                or kind not in ("all", "manual") or not isinstance(q, str) or len(q) > 256 or "\x00" in q):
            raise ValueError("limit must be in 1..200, offset nonnegative, kind all or manual, and q at most 256 characters")
        condition = "1" if kind == "all" else "owner_key IS NULL"
        parameters: tuple[Any, ...] = ()
        if q:
            # instr() is literal: %, _, quotes and backslashes never become
            # wildcard/SQL syntax. Paths follow host filesystem case rules.
            condition += (" AND (instr(alias_key,?)>0 OR EXISTS (SELECT 1 FROM roots INDEXED BY roots_group "
                          "WHERE group_id=source_groups.id AND instr(path,?)>0))")
            parameters = (_remote_key(q), os.path.normcase(q))
        with self._mutex:
            total = self._db.execute(f"SELECT COUNT(*) FROM source_groups WHERE {condition}", parameters).fetchone()[0]
            rows = self._db.execute(
                f"SELECT * FROM source_groups WHERE {condition} ORDER BY id LIMIT ? OFFSET ?", (*parameters, limit, offset),
            )
            return {"sources": [self._source_group_dict(row, include_members=False) for row in rows],
                    "total": total, "offset": offset, "limit": limit}

    def source_group_counts(self) -> dict[str, int]:
        """Small counters for state; no source/member dictionaries are built."""
        with self._mutex:
            return dict(self._db.execute(
                "SELECT COUNT(*) AS total,COALESCE(SUM(owner_key IS NULL),0) AS manual FROM source_groups",
            ).fetchone())

    def source_group(self, group_id: int, *, include_members: bool = False) -> dict[str, Any]:
        with self._mutex:
            row = self._db.execute("SELECT * FROM source_groups WHERE id=?", (group_id,)).fetchone()
            if row is None:
                raise QueueConfigurationError(f"Unknown source group: {group_id}")
            return self._source_group_dict(row, include_members=include_members)

    def _source_group_dict(self, row: sqlite3.Row, *, include_members: bool) -> dict[str, Any]:
        result = {key: row[key] for key in ("id", "alias", "remote_prefix", "owner_key", "created_at", "updated_at")}
        result["member_count"] = self._db.execute("SELECT COUNT(*) FROM roots WHERE group_id=?", (row["id"],)).fetchone()[0]
        result["members_preview"] = [self._member_summary(member) for member in self._db.execute(
            "SELECT " + self._member_summary_columns() + " FROM roots WHERE group_id=? ORDER BY id LIMIT 3", (row["id"],),
        )]
        result["path"] = result["members_preview"][0]["path"] if result["member_count"] == 1 else None
        if include_members:
            result["members"] = [self._member_summary(member) for member in self._db.execute(
                "SELECT " + self._member_summary_columns() + " FROM roots WHERE group_id=? ORDER BY id", (row["id"],),
            )]
        return result

    def source_group_members(self, group_id: int, *, limit: int = 200, offset: int = 0) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
            raise ValueError("limit must be in 1..200 and offset nonnegative")
        with self._mutex:
            group = self.source_group(group_id)
            return {"members": [self._member_summary(row) for row in self._db.execute(
                "SELECT " + self._member_summary_columns() + " FROM roots WHERE group_id=? ORDER BY id LIMIT ? OFFSET ?", (group_id, limit, offset),
            )], "total": group["member_count"], "offset": offset, "limit": limit}

    def group_root_ids(self, group_ids: Sequence[int]) -> tuple[int, ...]:
        if (not isinstance(group_ids, Sequence) or isinstance(group_ids, (str, bytes)) or not group_ids
                or len(group_ids) > _MAX_SOURCE_MEMBERS
                or any(type(value) is not int or not 0 < value < 2**63 for value in group_ids)):
            raise ValueError("group_ids must be a bounded nonempty sequence of positive IDs")
        groups = tuple(sorted(set(group_ids)))
        with self._mutex:
            where, parameters = self._ids_filter("id", groups)
            if self._db.execute(f"SELECT COUNT(*) FROM source_groups WHERE {where}", parameters).fetchone()[0] != len(groups):
                raise QueueConfigurationError("Unknown source group in selection.")
            where, parameters = self._ids_filter("group_id", groups)
            ids = tuple(row[0] for row in self._db.execute(f"SELECT id FROM roots WHERE {where} ORDER BY id", parameters))
            return self._selected_root_ids(ids)

    def root_group_id(self, root_id: int) -> int:
        """Resolve a physical worker/CLI source ID to its logical source."""
        if type(root_id) is not int or not 0 < root_id < 2**63:
            raise ValueError("root_id must be a positive source ID")
        with self._mutex:
            row = self._db.execute("SELECT group_id FROM roots WHERE id=?", (root_id,)).fetchone()
            if row is None:
                raise QueueConfigurationError(f"Unknown source root: {root_id}")
            return row[0]

    def _retarget_root(self, root_id: int, prefix: str, *, now: float) -> None:
        mutable = ("root_id=? AND cloud_present=0 AND status<>'uploaded' "
                   "AND NOT (status='skipped' AND COALESCE(skip_reason,'')='remote_exists') "
                   "AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id)")
        cursor = self._db.execute(f"SELECT id,relative_path FROM jobs WHERE {mutable} ORDER BY id", (root_id,))
        try:
            while rows := cursor.fetchmany(500):
                updates = []
                for row in rows:
                    remote = prefix.rstrip("/") + "/" + row["relative_path"]
                    updates.append((remote, _remote_key(remote), now, row["id"]))
                self._db.executemany("UPDATE jobs SET remote_path=?,remote_key=?,updated_at=? WHERE id=?", updates)
        finally:
            cursor.close()

    def update_source_group(self, group_id: int, remote_prefix: str, alias: str | None = None,
                            members: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
        prefix = _destination(remote_prefix)
        normalized = None if members is None else self._normalize_members(members)
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            group = self.source_group(group_id)
            selected_alias = group["alias"] if alias is None else _alias(alias)
            if self._db.execute("SELECT 1 FROM source_groups WHERE alias_key=? AND id<>?", (_remote_key(selected_alias), group_id)).fetchone():
                raise ScanCollisionError(f"Root alias {selected_alias!r} is already used by another source directory.")
            previous = {row["path"]: row for row in self._db.execute("SELECT * FROM roots WHERE group_id=?", (group_id,))}
            if normalized is None:
                normalized = [self._member_from_row(row) for row in previous.values()]
            self._validate_member_overlaps(normalized, ignore_group=group_id)
            definition = self._group_definition(selected_alias, prefix, normalized)
            if self._db.execute("SELECT definition_hash FROM source_groups WHERE id=?", (group_id,)).fetchone()[0] == definition:
                return group
            now = time.time()
            highest = self._source_id_high_water()
            wanted = {item["path"] for item in normalized}
            for path, row in previous.items():
                if path not in wanted:
                    self._delete_tasks("root_id=?", (row["id"],))
                    self._db.execute("DELETE FROM roots WHERE id=?", (row["id"],))
            try:
                # Release every changing destination together, so swapping two
                # member mappings does not collide with a member not visited
                # yet. Frozen uploaded/accepted objects keep their real keys.
                changing = []
                for member in normalized:
                    row = previous.get(member["path"])
                    remote = prefix.rstrip("/") + "/" + member["remote_subdir"] if member["remote_subdir"] else prefix
                    if row is not None and remote != row["remote_prefix"]:
                        changing.append(row["id"])
                if changing:
                    selected, parameters = self._ids_filter("root_id", changing)
                    self._db.execute(
                        "UPDATE jobs SET remote_key=?||id WHERE " + selected + " AND cloud_present=0 AND status<>'uploaded' "
                        "AND NOT (status='skipped' AND COALESCE(skip_reason,'')='remote_exists') "
                        "AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id)",
                        ("remap:" + uuid.uuid4().hex + ":", *parameters),
                    )
                for member in normalized:
                    row = previous.get(member["path"])
                    if row is None:
                        highest += 1
                        self._insert_group_member(group_id, highest, selected_alias, prefix, member)
                        continue
                    remote = prefix.rstrip("/") + "/" + member["remote_subdir"] if member["remote_subdir"] else prefix
                    if bool(row["recursive"]) != member["recursive"] or json.loads(row["exclusions_json"]) != member["excluded_paths"]:
                        self._delete_tasks("root_id=?", (row["id"],))
                    elif remote != row["remote_prefix"]:
                        self._retarget_root(row["id"], remote, now=now)
                    self._db.execute(
                        "UPDATE roots SET alias=?,alias_key=?,remote_prefix=?,recursive=?,exclusions_json=?,remote_subdir=? WHERE id=?",
                        (selected_alias, "\x00member:" + str(row["id"]), remote, int(member["recursive"]),
                         json.dumps(member["excluded_paths"], ensure_ascii=False), member["remote_subdir"], row["id"]),
                    )
                self._db.execute("UPDATE source_groups SET alias=?,alias_key=?,remote_prefix=?,definition_hash=?,updated_at=? WHERE id=?",
                                 (selected_alias, _remote_key(selected_alias), prefix, definition, now, group_id))
            except sqlite3.IntegrityError as exc:
                raise ScanCollisionError("The selected cloud folder conflicts with another task's destination.") from exc
            self._put_meta("_root_id_high_water", highest)
            self._sources_changed("source_updated")
            return self.source_group(group_id)

    def remove_source_group(self, group_id: int) -> int:
        """Forget a logical source; never delete local or remote files."""
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            self.source_group(group_id)
            self._put_meta("_root_id_high_water", self._source_id_high_water())
            count = self._delete_tasks("source_group_id=?", (group_id,))
            self._db.execute("DELETE FROM roots WHERE group_id=?", (group_id,))
            self._db.execute("DELETE FROM source_groups WHERE id=?", (group_id,))
            self._sources_changed("source_removed")
            return count

    def source_group_stats(self, group_ids: Sequence[int] | None = None) -> list[dict[str, Any]]:
        """One grouped pass, without fetching tasks or physical members."""
        selected = None
        if group_ids is not None:
            if (not isinstance(group_ids, Sequence) or isinstance(group_ids, (str, bytes))
                    or len(group_ids) > _MAX_SOURCE_MEMBERS
                    or any(type(value) is not int or not 0 < value < 2**63 for value in group_ids)):
                raise ValueError("group_ids must be a bounded sequence of positive IDs")
            selected = tuple(sorted(set(group_ids)))
            if not selected:
                return []
        expressions = []
        for label, condition in (("total", "1"), ("cloud", "cloud_present=1"),
                                 ("existing", "status='skipped' AND skip_reason='remote_exists'"),
                                 ("uploaded", "status='uploaded'"), ("pending", "status='pending'"),
                                 ("failed", "status='failed'"), ("skipped", "status='skipped' AND COALESCE(skip_reason,'')<>'remote_exists'")):
            for suffix, amount in (("count", "1"), ("bytes", "size")):
                expressions.append(f"COALESCE(SUM(CASE WHEN {condition} THEN {amount} ELSE 0 END),0) AS {label}_{suffix}")
        with self._mutex:
            job_condition, job_parameters = ("1", ()) if selected is None else self._ids_filter("source_group_id", selected)
            group_condition, group_parameters = ("1", ()) if selected is None else self._ids_filter("id", selected)
            ids = [row[0] for row in self._db.execute(
                f"SELECT id FROM source_groups WHERE {group_condition} ORDER BY id", group_parameters,
            )]
            if selected is not None and len(ids) != len(selected):
                raise QueueConfigurationError("Unknown source group in statistics selection.")
            aggregated = {row["source_group_id"]: dict(row) for row in self._db.execute(
                "SELECT source_group_id," + ",".join(expressions) + " FROM jobs INDEXED BY jobs_group "
                f"WHERE present=1 AND {job_condition} GROUP BY source_group_id", job_parameters,
            )}
            empty = {f"{label}_{suffix}": 0 for label in ("total", "cloud", "existing", "uploaded", "pending", "failed", "skipped")
                     for suffix in ("count", "bytes")}
            return [{"id": identifier, **empty, **aggregated.get(identifier, {})} for identifier in ids]

    def add_root(self, path: str | os.PathLike[str], alias: str, remote_prefix: str | None = None) -> Root:
        """Legacy single-directory source API; physical IDs stay stable."""
        root_path = _path_key(Path(path).expanduser())
        alias = _alias(alias)
        with self.exclusive_run():
            with self._mutex:
                row = self._db.execute("SELECT * FROM roots WHERE path_key=?", (root_path,)).fetchone()
                if row is not None:
                    group = self.source_group(row["group_id"])
                    if (group["member_count"] != 1 or row["alias"] != alias
                            or (remote_prefix is not None and row["remote_prefix"] != _destination(remote_prefix))
                            or not row["recursive"] or row["remote_subdir"] or json.loads(row["exclusions_json"])):
                        raise QueueConfigurationError("Use update_source_group to change an existing source mapping.")
                    return self._root(row)
            group = self.add_source_group([{"path": root_path}], alias, remote_prefix)
            with self._mutex:
                return self._root(self._db.execute("SELECT * FROM roots WHERE group_id=?", (group["id"],)).fetchone())

    def update_root(self, root_id: int, remote_prefix: str, alias: str | None = None) -> Root:
        with self.exclusive_run():
            with self._mutex:
                row = self._db.execute("SELECT * FROM roots WHERE id=?", (root_id,)).fetchone()
                if row is None:
                    raise QueueConfigurationError(f"Unknown source root: {root_id}")
                if self.source_group(row["group_id"])["member_count"] != 1:
                    raise QueueConfigurationError("Update the logical source group containing this directory.")
            self.update_source_group(row["group_id"], remote_prefix, alias=alias)
            with self._mutex:
                return self._root(self._db.execute("SELECT * FROM roots WHERE id=?", (root_id,)).fetchone())

    def remove_root(self, root_id: int) -> int:
        with self.exclusive_run():
            with self._mutex:
                row = self._db.execute("SELECT group_id FROM roots WHERE id=?", (root_id,)).fetchone()
                if row is None:
                    raise QueueConfigurationError(f"Unknown source root: {root_id}")
                if self.source_group(row["group_id"])["member_count"] != 1:
                    raise QueueConfigurationError("Remove the logical source group containing this directory.")
            return self.remove_source_group(row["group_id"])

    def roots(self) -> list[Root]:
        with self._mutex:
            return [Root(r["id"], r["path"], r["alias"], r["remote_prefix"])
                    for r in self._db.execute("SELECT * FROM roots ORDER BY id")]

    @staticmethod
    def _ids_filter(column: str, values: Sequence[int]) -> tuple[str, tuple[Any, ...]]:
        # column is always an internal literal. Only the values are external.
        if not values:
            return "0", ()
        if len(values) <= 400:
            return column + " IN (" + ",".join("?" for _ in values) + ")", tuple(values)
        return column + " IN (SELECT value FROM json_each(?))", (json.dumps(list(values)),)

    def _selected_root_ids(self, root_ids: Sequence[int] | None) -> _RootScope | None:
        """Validate a scope once and compress complete logical sources."""
        if root_ids is None:
            return None
        with self._mutex:
            generation = self._get_meta("_sources_generation", 0)
            if (isinstance(root_ids, _RootScope) and root_ids.database == self.path
                    and root_ids.generation == generation):
                return root_ids
            if (not isinstance(root_ids, Sequence) or isinstance(root_ids, (str, bytes))
                    or not 1 <= len(root_ids) <= _MAX_SOURCE_MEMBERS
                    or any(type(value) is not int or not 0 < value < 2**63 for value in root_ids)):
                raise ValueError(f"root_ids must be a nonempty sequence of at most {_MAX_SOURCE_MEMBERS} positive source IDs")
            selected = tuple(sorted(set(root_ids)))
            cached = self._scope_cache.get(selected)
            if cached is not None and cached.generation == generation:
                return cached
            where, parameters = self._ids_filter("id", selected)
            found = {row["id"]: row["group_id"] for row in self._db.execute(
                f"SELECT id,group_id FROM roots WHERE {where}", parameters,
            )}
            if len(found) != len(selected):
                missing = [str(value) for value in selected if value not in found]
                raise QueueConfigurationError("Unknown source roots: " + ", ".join(missing[:30]))
            group_counts: dict[int, int] = {}
            for group_id in found.values():
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
            where, parameters = self._ids_filter("group_id", tuple(group_counts))
            complete = tuple(row["group_id"] for row in self._db.execute(
                f"SELECT group_id,COUNT(*) AS n FROM roots WHERE {where} GROUP BY group_id", parameters,
            ) if row["n"] == group_counts[row["group_id"]])
            complete_set = set(complete)
            partial = tuple(root_id for root_id in selected if found[root_id] not in complete_set)
            result = _RootScope(selected, database=self.path, generation=generation, groups=complete, partial=partial)
            if len(self._scope_cache) >= 8:
                self._scope_cache.clear()
            self._scope_cache[selected] = result
            return result

    def _root_filter(self, root_ids: tuple[int, ...] | None) -> tuple[str, tuple[Any, ...]]:
        if root_ids is None:
            return "1", ()
        selected = self._selected_root_ids(root_ids)
        clauses, parameters = [], ()
        for column, ids in (("source_group_id", selected.groups), ("root_id", selected.partial)):
            if ids:
                clause, values = self._ids_filter(column, ids)
                clauses.append(clause)
                parameters += values
        return "(" + " OR ".join(clauses) + ")", parameters

    def _walk_files(self, root: Root, extensions: frozenset[str], result: ScanResult,
                    stop: threading.Event | None = None) -> Iterator[tuple]:
        with self._mutex:
            rule = self._db.execute("SELECT recursive,exclusions_json FROM roots WHERE id=?", (root.id,)).fetchone()
        recursive = bool(rule["recursive"]) if rule else True
        excluded = {os.path.normcase(str(Path(root.path).joinpath(*relative.split("/"))))
                    for relative in json.loads(rule["exclusions_json"])} if rule else set()
        directories = [root.path]
        while directories:
            if stop is not None and stop.is_set():
                return
            directory = directories.pop()
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        if stop is not None and stop.is_set():
                            return
                        try:
                            if entry.is_symlink():
                                result.ignored_symlinks += 1
                                continue
                            if entry.is_dir(follow_symlinks=False):
                                if recursive and os.path.normcase(entry.path) not in excluded:
                                    directories.append(entry.path)
                                continue
                            if not entry.is_file(follow_symlinks=False):
                                continue
                            if Path(entry.name).suffix.casefold() not in extensions:
                                continue
                            value = entry.stat(follow_symlinks=False)
                            relative = Path(entry.path).relative_to(root.path).as_posix()
                            remote = root.remote_prefix.rstrip("/") + "/" + relative
                            yield (
                                root.id, relative, entry.path, os.path.normcase(entry.path), remote,
                                _remote_key(remote), value.st_size, value.st_mtime_ns, value.st_dev, str(value.st_ino),
                            )
                        except (OSError, ValueError, UnicodeError) as exc:
                            result.error(f"Cannot inspect {entry.path}: {exc}")
            except OSError as exc:
                result.error(f"Cannot scan {directory}: {exc}")

    def scan(self, extensions: Iterable[str] | None = None, *, batch_size: int = 500,
             stop: threading.Event | None = None,
             progress: Callable[[ScanResult], None] | None = None,
             root_ids: Sequence[int] | None = None) -> ScanResult:
        """Stream selected roots; never hold the complete file list in RAM.

        A successful root scan prunes disappeared/excluded pending work.  A
        partial or interrupted scan never prunes unseen rows.  No claims can be
        issued during a scan, and a scan cannot begin while jobs are running.
        """
        if batch_size < 1 or batch_size > 10000:
            raise ValueError("batch_size must be in 1..10000")
        selected = MEDIA_EXTENSIONS if extensions is None else _normalize_extensions(extensions)
        selected_roots = self._selected_root_ids(root_ids)
        result = ScanResult(root_ids=list(selected_roots) if selected_roots is not None else None)
        started = time.monotonic()
        last_progress = started
        token = uuid.uuid4().hex
        with _file_lock(self.path + ".scan.lock"):
            with self._transaction():
                if self._db.execute("SELECT 1 FROM jobs WHERE status='running' LIMIT 1").fetchone():
                    raise QueueBusyError("Pause/finish uploads or recover interrupted claims before scanning.")
                # Recheck under the writer lock before beginning this scope.
                selected_roots = self._selected_root_ids(selected_roots)
                source_selection = set(selected_roots) if selected_roots is not None else None
                sources = [root for root in self.roots() if source_selection is None or root.id in source_selection]
                if not sources:
                    raise QueueConfigurationError("Register at least one source root before scanning.")
                # We own the OS lock, so an old token left by a killed scanner
                # can safely be replaced.  No PID/time guessing is necessary.
                self._put_meta("_scan_token", token)
                result.scan_id = self._db.execute(
                    "INSERT INTO scans(started_at) VALUES(?)", (time.time(),)
                ).lastrowid
            try:
                for root in sources:
                    if stop is not None and stop.is_set():
                        result.cancelled = True
                        break
                    error_start = result.error_count
                    batch = []
                    entries = (self._walk_files(root, selected, result) if stop is None else
                               self._walk_files(root, selected, result, stop=stop))
                    for entry in entries:
                        batch.append(entry)
                        if len(batch) >= batch_size:
                            self._ingest(batch, result)
                            batch.clear()
                            if progress is not None and time.monotonic() - last_progress >= 0.1:
                                result.seconds = time.monotonic() - started
                                progress(result)
                                last_progress = time.monotonic()
                    if batch:
                        self._ingest(batch, result)
                    if stop is not None and stop.is_set():
                        result.cancelled = True
                        break
                    if progress is not None:
                        result.seconds = time.monotonic() - started
                        progress(result)
                    if result.error_count == error_start:
                        with self._transaction():
                            # Keep historical uploaded states/evidence even if
                            # the user's local original has subsequently moved.
                            result.missing += self._db.execute(
                                "SELECT COUNT(*) FROM jobs WHERE root_id=? AND present=1 "
                                "AND (seen_scan IS NULL OR seen_scan<>?)",
                                (root.id, result.scan_id),
                            ).fetchone()[0]
                            self._db.execute(
                                "UPDATE jobs SET present=0, updated_at=?, "
                                "status=CASE WHEN status='uploaded' THEN status ELSE 'skipped' END, "
                                "skip_reason=CASE WHEN status='uploaded' OR skip_reason='remote_exists' THEN skip_reason ELSE 'missing' END, "
                                "last_error=CASE WHEN status='uploaded' OR skip_reason='remote_exists' THEN last_error "
                                "ELSE 'Absent or excluded in the latest complete source scan' END "
                                "WHERE root_id=? AND present=1 AND (seen_scan IS NULL OR seen_scan<>?)",
                                (time.time(), root.id, result.scan_id),
                            )
                result.complete = result.error_count == 0 and not result.cancelled
            finally:
                result.seconds = time.monotonic() - started
                with self._transaction():
                    self._db.execute(
                        "UPDATE scans SET finished_at=?,complete=?,result_json=? WHERE id=?",
                        (time.time(), int(result.complete), json.dumps(dataclasses.asdict(result), ensure_ascii=False), result.scan_id),
                    )
                    if self._get_meta("_scan_token") == token:
                        self._db.execute("DELETE FROM metadata WHERE key='_scan_token'")
        if progress is not None:
            progress(result)
        return result

    def _move_candidate(self, values: tuple) -> sqlite3.Row | None:
        """Recognize one disappeared source identity, never guess by name/size.

        A zero inode, existing old path, hard link, ambiguous database identity,
        or a saved remote commit prevents adoption. These conservative cases
        remain independent tasks; absent history is excluded from the queue.
        """
        if not int(values[9]):
            return None
        candidates = self._db.execute(
            "SELECT * FROM jobs WHERE root_id=? AND device=? AND inode=? AND size=? AND mtime_ns=? LIMIT 2",
            (values[0], values[8], values[9], values[6], values[7]),
        ).fetchall()
        if len(candidates) != 1:
            return None
        row = candidates[0]
        if self._db.execute("SELECT 1 FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone():
            return None
        try:
            os.stat(row["local_path"], follow_symlinks=False)
        except (FileNotFoundError, NotADirectoryError):
            pass
        except OSError:
            return None
        else:
            return None
        try:
            current = os.stat(values[2], follow_symlinks=False)
        except OSError:
            return None
        if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                or (current.st_size, current.st_mtime_ns, current.st_dev, str(current.st_ino)) != values[6:10]):
            return None
        return row

    def _update_unchanged_source(self, row: sqlite3.Row, values: tuple, scan_id: int, now: float) -> None:
        frozen = (row["cloud_present"] or row["status"] == "uploaded" or (row["status"] == "skipped" and row["skip_reason"] == "remote_exists") or self._db.execute(
            "SELECT 1 FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone() is not None)
        remote_path = row["remote_path"] if frozen else values[4]
        remote_key = row["remote_key"] if frozen else values[5]
        plan = classify_media(values[2], values[6])
        self._db.execute(
            "UPDATE jobs SET relative_path=?,local_path=?,local_key=?,remote_path=?,remote_key=?,"
            "seen_scan=?,present=1,updated_at=?,planned_mode=?,planned_reason=?,name_sort=?,"
            "status=CASE WHEN (status='skipped' AND skip_reason='missing') OR "
            "(status='failed' AND skip_reason='source_changed') THEN 'pending' ELSE status END,"
            "next_attempt=CASE WHEN skip_reason IN ('missing','source_changed') THEN 0 ELSE next_attempt END,"
            "completed_at=CASE WHEN skip_reason IN ('missing','source_changed') THEN NULL ELSE completed_at END,"
            "last_error=CASE WHEN skip_reason IN ('missing','source_changed') THEN NULL ELSE last_error END,"
            "skip_reason=CASE WHEN skip_reason IN ('missing','source_changed') THEN NULL ELSE skip_reason END WHERE id=?",
            values[1:4] + (remote_path, remote_key, scan_id, now, plan["mode"], plan["reason"],
                           unicodedata.normalize("NFC", values[1].rsplit("/", 1)[-1]).casefold(), row["id"]),
        )

    def _ingest(self, batch: list[tuple], result: ScanResult) -> None:
        now = time.time()
        with self._transaction():
            highest_id = self._job_id_high_water()
            next_id = highest_id + 1
            groups = {root_id: self._db.execute("SELECT group_id FROM roots WHERE id=?", (root_id,)).fetchone()[0]
                      for root_id in {values[0] for values in batch}}
            for values in batch:
                result.seen += 1
                plan = classify_media(values[2], values[6])
                # A name collision must not leave half an update behind, such
                # as removing an intent before a later UNIQUE check fails.
                self._db.execute("SAVEPOINT ingest_task")
                try:
                    row = self._db.execute("SELECT * FROM jobs WHERE local_key=?", (values[3],)).fetchone()
                    if row is None:
                        moved = self._move_candidate(values)
                        if moved is not None:
                            self._update_unchanged_source(moved, values, result.scan_id, now)
                            result.moved += 1
                        else:
                            self._db.execute(
                                "INSERT INTO jobs(id,root_id,relative_path,local_path,local_key,remote_path,remote_key,"
                                "size,mtime_ns,device,inode,created_at,updated_at,seen_scan,planned_mode,planned_reason,name_sort,source_group_id,next_attempt) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                                (next_id,) + values + (now, now, result.scan_id, plan["mode"], plan["reason"],
                                                      unicodedata.normalize("NFC", values[1].rsplit("/", 1)[-1]).casefold(), groups[values[0]]),
                            )
                            next_id += 1
                            result.added += 1
                    else:
                        if row["root_id"] != values[0]:
                            raise ScanCollisionError(f"Source mapping changed for {values[2]}")
                        old_fingerprint = (row["size"], row["mtime_ns"], row["device"], str(row["inode"]))
                        if old_fingerprint != values[6:10]:
                            self._drop_intent(row["id"])
                            self._db.execute(
                                "UPDATE jobs SET relative_path=?,local_path=?,local_key=?,remote_path=?,remote_key=?,"
                                "size=?,mtime_ns=?,device=?,inode=?,status='pending',attempts=0,"
                                "next_attempt=0,claim_token=NULL,last_error='Source changed since previous scan',"
                                "skip_reason=NULL,remote_hash=NULL,remote_id=NULL,verified_metadata=NULL,uploaded_at=NULL,storage_mode=NULL,"
                                "observed_mode=NULL,cloud_present=0,cloud_path=NULL,cloud_hash=NULL,cloud_size=NULL,"
                                "cloud_verified=0,cloud_observed_at=NULL,cloud_deleted_at=NULL,planned_mode=?,planned_reason=?,"
                                "progress_bytes=0,completed_at=NULL,name_sort=?,seen_scan=?,present=1,updated_at=? WHERE id=?",
                                values[1:10] + (plan["mode"], plan["reason"],
                                               unicodedata.normalize("NFC", values[1].rsplit("/", 1)[-1]).casefold(),
                                               result.scan_id, now, row["id"]),
                            )
                            result.changed += 1
                        else:
                            self._update_unchanged_source(row, values, result.scan_id, now)
                            result.unchanged += 1
                except (sqlite3.IntegrityError, ScanCollisionError, UnicodeError) as exc:
                    self._db.execute("ROLLBACK TO SAVEPOINT ingest_task")
                    result.error(f"Cannot map {values[2]} to {values[4]} (name collision or invalid name): {exc}")
                finally:
                    self._db.execute("RELEASE SAVEPOINT ingest_task")
            if next_id > highest_id + 1:
                self._put_meta("_job_id_high_water", next_id - 1)

    @staticmethod
    def _job(row: sqlite3.Row, token: str, attempts: int) -> Job:
        return Job(
            row["id"], row["root_id"], row["local_path"], row["remote_path"], row["relative_path"],
            row["size"], row["mtime_ns"], row["device"], int(row["inode"]), attempts, token,
            row["storage_mode"], row["planned_mode"], row["planned_reason"], row["progress_bytes"],
        )

    def _scoped_ready(self, root_ids: tuple[int, ...], *, storage_mode: str | None,
                      now: float, max_attempts: int | None, stop: threading.Event | None) -> sqlite3.Row | None:
        """Merge at most two indexed candidates per selected source.

        Ordering an IN(root_ids) result by id would sort every eligible row
        on each claim. Separate equality seeks keep this independent of the
        number of tasks in both selected and unrelated source directories.
        """
        modes = _STORAGE_MODES if storage_mode is None else (storage_mode,)
        scope = self._selected_root_ids(root_ids)
        for state in ("failed", "pending"):
            retry = state == "failed"
            retry_clause = " AND next_attempt IS NOT NULL AND next_attempt<=?" if retry else ""
            order = "next_attempt,id" if retry else "id"
            best = best_key = None
            for kind, source_id in ((kind, value) for kind, values in (("group", scope.groups), ("root", scope.partial)) for value in values):
                index = "jobs_" + ("retry" if retry else "pending") + "_" + kind
                column = "source_group_id" if kind == "group" else "root_id"
                for mode in modes:
                    if stop is not None and stop.is_set():
                        return None
                    parameters = (source_id, mode) + ((now,) if retry else ()) + (max_attempts, max_attempts)
                    row = self._db.execute(
                        f"SELECT * FROM jobs INDEXED BY {index} WHERE status='{state}' AND present=1 "
                        f"AND paused=0 AND remove_requested=0 AND {column}=? AND planned_mode=?"
                        f"{retry_clause} AND (? IS NULL OR attempts<?) ORDER BY {order} LIMIT 1", parameters,
                    ).fetchone()
                    if row is not None:
                        key = (row["next_attempt"], row["id"]) if retry else (row["id"],)
                        if best is None or key < best_key:
                            best, best_key = row, key
            if best is not None:
                return best
        return None

    def claim(self, *, now: float | None = None, max_attempts: int | None = None,
              storage_mode: str | None = None, stop: threading.Event | None = None,
              job_id: int | None = None, root_ids: Sequence[int] | None = None) -> Job | None:
        """Claim one ready file and validate the local snapshot before returning.

        None means no work is *currently* ready or the supplied stop is set,
        not that all work succeeded. Check stop before interpreting the result,
        and stats() for deferred/permanent failures and an active scan.
        """
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if job_id is not None and (type(job_id) is not int or job_id < 1):
            raise ValueError("job_id must be a positive integer or None")
        selected_roots = self._selected_root_ids(root_ids)
        root_filter, root_parameters = self._root_filter(selected_roots)
        partition = _size_partition(storage_mode)
        pending_index = "jobs_pending" if storage_mode is None else f"jobs_pending_{storage_mode}"
        retry_index = " INDEXED BY jobs_retry" if storage_mode is None else f" INDEXED BY jobs_retry_{storage_mode}"
        refreshed: set[int] = set()
        while True:
            # A missing/inaccessible prefix can contain hundreds of thousands
            # of tasks. Yield to global pause between candidate inspections.
            if stop is not None and stop.is_set():
                return None
            current = time.time() if now is None else now
            with self._transaction():
                if self._get_meta("_scan_token"):
                    return None
                # Separate indexed LIMIT queries are intentional.  One OR
                # query plus ORDER BY id makes SQLite sort the entire ready
                # queue on *every* claim (quadratic at 300,000 photographs).
                if job_id is not None:
                    # The PRIMARY KEY lookup is independent of queue length.
                    # An invalid target must never fall through to neighbours.
                    row = self._db.execute(
                        "SELECT * FROM jobs WHERE id=? AND present=1 AND paused=0 AND remove_requested=0 "
                        "AND (status='pending' OR (status='failed' AND next_attempt IS NOT NULL AND next_attempt<=?)) "
                        f"AND {partition} AND {root_filter} AND (? IS NULL OR attempts<?)",
                        (job_id, current, *root_parameters, max_attempts, max_attempts),
                    ).fetchone()
                elif selected_roots is not None:
                    row = self._scoped_ready(selected_roots, storage_mode=storage_mode, now=current,
                                             max_attempts=max_attempts, stop=stop)
                else:
                    row = self._db.execute(
                        f"SELECT * FROM jobs{retry_index} WHERE status='failed' AND present=1 "
                        "AND paused=0 AND remove_requested=0 "
                        "AND next_attempt IS NOT NULL AND next_attempt<=? "
                        f"AND {partition} AND (? IS NULL OR attempts<?) ORDER BY next_attempt,id LIMIT 1",
                        (current, max_attempts, max_attempts),
                    ).fetchone()
                    if row is None:
                        row = self._db.execute(
                            f"SELECT * FROM jobs INDEXED BY {pending_index} WHERE status='pending' AND present=1 "
                            "AND paused=0 AND remove_requested=0 "
                            f"AND {partition} AND (? IS NULL OR attempts<?) ORDER BY id LIMIT 1",
                            (max_attempts, max_attempts),
                        ).fetchone()
                if row is None:
                    return None
                token = uuid.uuid4().hex
                attempts = row["attempts"] + 1
                self._db.execute(
                    "UPDATE jobs SET status='running',claim_token=?,attempts=?,updated_at=?,next_attempt=NULL,completed_at=NULL WHERE id=?",
                    (token, attempts, current, row["id"]),
                )
                job = self._job(row, token, attempts)
            try:
                snapshot = self._inspect_source(job)
            except SourceUnavailableError:
                if job_id is not None:
                    return None
                continue
            if snapshot is None:
                self._invalidate_claim(job, None)
                if job_id is not None:
                    return None
                continue
            if snapshot != _job_snapshot(job):
                self._invalidate_claim(job, snapshot, retry=job_id is None and job.id not in refreshed)
                if job_id is not None:
                    return None
                refreshed.add(job.id)
                continue
            return self._restore_rescan_intent(job)

    def source_matches(self, job: Job) -> bool:
        try:
            return _snapshot(job.local_path) == _job_snapshot(job)
        except OSError:
            return False

    def _inspect_source(self, job: Job) -> tuple[int, int, int, int] | None:
        """Distinguish an absent path from an inaccessible/unsafe source.

        A permission or I/O error cannot establish disappearance or a content
        change. Keep the source snapshot and any commit evidence, record one
        visible failure, and leave an explicit resume available to the user.
        """
        try:
            return _snapshot(job.local_path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        except OSError as error:
            code = errno.errorcode.get(error.errno, type(error).__name__)
            message = (f"Не удалось проверить исходный файл ({code}). "
                       f"Проверьте доступ к файлу и тип исходного объекта: {job.local_path}")
            self.mark_failed(job, message, retry_delay=None)
            raise SourceUnavailableError(message) from None

    def _require_claim(self, job: Job) -> sqlite3.Row:
        row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job.id,)).fetchone()
        if row is None or row["status"] != "running" or row["claim_token"] != job.claim_token:
            raise StaleClaimError(f"Claim for job {job.id} is no longer current.")
        return row

    def _invalidate_claim(self, job: Job, snapshot: tuple[int, int, int, int] | None, *, retry: bool = False) -> None:
        with self._transaction():
            self._require_claim(job)
            if snapshot is None:
                self._drop_intent(job.id)
                now = time.time()
                self._db.execute(
                    "UPDATE jobs SET status='skipped',skip_reason='missing',present=0,claim_token=NULL,"
                    "storage_mode=NULL,last_error='Source is missing, inaccessible, or no longer a regular file',completed_at=?,updated_at=? WHERE id=?",
                    (now, now, job.id),
                )
            else:
                root = self._db.execute("SELECT remote_prefix FROM roots WHERE id=?", (job.root_id,)).fetchone()
                remote = root["remote_prefix"].rstrip("/") + "/" + job.relative_path
                if self._db.execute("SELECT 1 FROM jobs WHERE remote_key=? AND id<>?",
                                    (_remote_key(remote), job.id)).fetchone():
                    # Keep the old source snapshot and its uncertain commit
                    # evidence. A corrected mapping/rescan can resolve this
                    # one task; other ready tasks can still be claimed.
                    now = time.time()
                    self._db.execute(
                        "UPDATE jobs SET status='failed',claim_token=NULL,next_attempt=NULL,completed_at=?,updated_at=?,"
                        "last_error='Source changed, but the current cloud destination conflicts with another task' "
                        "WHERE id=?", (now, now, job.id),
                    )
                    self._finalize_control(job.id)
                    return
                self._drop_intent(job.id)
                plan = classify_media(job.local_path, snapshot[0])
                now = time.time()
                self._db.execute(
                    "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status=?,attempts=0,next_attempt=?,"
                    "claim_token=NULL,last_error='Source changed; any remote partial result must be reconciled',"
                    "remote_hash=NULL,remote_id=NULL,verified_metadata=NULL,uploaded_at=NULL,storage_mode=NULL,skip_reason=?,updated_at=?,"
                    "remote_path=?,remote_key=?,planned_mode=?,planned_reason=?,"
                    "observed_mode=NULL,cloud_present=0,cloud_path=NULL,cloud_hash=NULL,cloud_size=NULL,"
                    "cloud_verified=0,cloud_observed_at=NULL,cloud_deleted_at=NULL,progress_bytes=0,completed_at=? "
                    "WHERE id=?",
                    snapshot[:3] + (str(snapshot[3]), "pending" if retry else "failed", 0 if retry else None,
                                    None if retry else "source_changed", now, remote, _remote_key(remote),
                                    plan["mode"], plan["reason"], None if retry else now, job.id),
                )
            self._finalize_control(job.id)

    def save_commit_intent(self, job: Job, cloud_hash: str, *, storage_mode: str = "unlimited") -> None:
        """Durably record intent BEFORE the server-side add/commit request.

        This is evidence that an ambiguous retry belongs to this particular
        upload, not permission to adopt an arbitrary pre-existing cloud file.
        It survives failure/recovery and is tied to the account and source.
        Each call starts a NEW add attempt, so even a matching old intent must
        lose every previous confirmation. Recovery of an existing file
        reads the saved intent directly and does not call this method.
        """
        if storage_mode not in _STORAGE_MODES:
            raise ValueError("storage_mode must be 'unlimited' or 'regular'")
        if not isinstance(cloud_hash, str) or not cloud_hash or any(c in cloud_hash for c in ("\x00", "\n", "\r")):
            raise ValueError("cloud_hash must be a nonempty hash string")
        snapshot = self._inspect_source(job)
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed before remote commit: {job.local_path}")
        with self._transaction():
            claimed = self._require_claim(job)
            account = self._get_meta("_account_id")
            if not account:
                raise QueueConfigurationError("Bind the authenticated account before recording a remote commit.")
            previous = self._db.execute("SELECT * FROM commit_intents WHERE job_id=?", (job.id,)).fetchone()
            if previous is not None:
                same_source = (
                    previous["source_size"], previous["source_mtime_ns"], previous["device"], int(previous["inode"])
                ) == _job_snapshot(job)
                if same_source and previous["remote_path"] == job.remote_path:
                    if previous["cloud_hash"].casefold() != cloud_hash.casefold():
                        raise QueueError("A different commit hash already exists for the unchanged source; investigate before retrying.")
                    # Do not retain confirmation of either storage mode.
                    # A fresh add needs a fresh affirmative server response.
                self._archive_intent(job.id)
            intent = {
                "cloud_hash": cloud_hash, "remote_path": job.remote_path,
                "source_size": job.size, "source_mtime_ns": job.mtime_ns,
                "device": job.device, "inode": job.inode,
                "account_id": account, "destination": self.destination, "created_at": time.time(),
                "storage_mode": storage_mode,
                "intent_id": uuid.uuid4().hex,
            }
            batch_id = self._active_batch_id(claimed)
            if batch_id is not None:
                intent["batch_id"] = batch_id
            self._db.execute(
                "INSERT INTO commit_intents(job_id,source_size,source_mtime_ns,device,inode,remote_path,cloud_hash,created_at,intent_json) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET "
                "source_size=excluded.source_size,source_mtime_ns=excluded.source_mtime_ns,device=excluded.device,"
                "inode=excluded.inode,remote_path=excluded.remote_path,cloud_hash=excluded.cloud_hash,"
                "created_at=excluded.created_at,intent_json=excluded.intent_json",
                (job.id, job.size, job.mtime_ns, job.device, str(job.inode), job.remote_path,
                 cloud_hash, intent["created_at"], json.dumps(intent, ensure_ascii=False, allow_nan=False)),
            )
            self._db.execute("UPDATE jobs SET storage_mode=?,updated_at=? WHERE id=?",
                             (storage_mode, intent["created_at"], job.id))

    def get_commit_intent(self, job: Job) -> dict[str, Any] | None:
        """Return only an intent matching this current claim's account/source."""
        with self._transaction():
            claimed = self._require_claim(job)
            row = self._db.execute("SELECT * FROM commit_intents WHERE job_id=?", (job.id,)).fetchone()
            if row is None:
                return None
            try:
                intent = json.loads(row["intent_json"])
            except (ValueError, TypeError):
                raise QueueError("Saved commit intent is invalid.") from None
            if not isinstance(intent, dict) or intent.get("storage_mode", "unlimited") not in _STORAGE_MODES:
                raise QueueError("Saved commit intent has an invalid storage mode.")
            fingerprint = (row["source_size"], row["source_mtime_ns"], row["device"], int(row["inode"]))
            if (fingerprint != _job_snapshot(job) or row["remote_path"] != job.remote_path
                    or intent.get("account_id") != self._get_meta("_account_id")
                    or intent.get("destination") != self.destination):
                self._drop_intent(job.id)
                return None
            # Schema-1 clients before regular uploads only wrote unlimited
            # intents. Normalize them in memory without making every read write.
            result = {"storage_mode": "unlimited", **intent}
            # A negative or missing server flag must survive retry/restart.
            # Do not expose receipts for an object already sent to the bin;
            # its next add attempt needs a fresh server response.
            result.pop("storage_observation", None)
            prior_receipt = result.pop("_rescan_storage_observation", None)
            current_receipt = self._current_storage_observation(claimed, row["cloud_hash"])
            if current_receipt is not None:
                result["storage_observation"] = current_receipt
            elif claimed["cloud_deleted_at"] is None and isinstance(prior_receipt, dict):
                # Recovery evidence is not a current presence flag. The API
                # still has to read and compare the present remote object.
                result["storage_observation"] = prior_receipt
            return result

    def _current_storage_observation(self, row: sqlite3.Row, cloud_hash: str) -> dict[str, Any] | None:
        if (not row["cloud_present"] or not row["cloud_hash"]
                or row["cloud_hash"].casefold() != cloud_hash.casefold()):
            return None
        result = {"mode": row["observed_mode"], "hash": row["cloud_hash"], "size": row["cloud_size"],
                  "path": row["cloud_path"], "verified": bool(row["cloud_verified"]), "received_at": row["cloud_observed_at"]}
        receipt_row = self._db.execute(
            "SELECT receipt_json,remote_id FROM cloud_observations WHERE job_id=? AND event='stored' "
            "AND cloud_hash=? AND remote_path=? ORDER BY id DESC LIMIT 1",
            (row["id"], row["cloud_hash"], row["cloud_path"]),
        ).fetchone()
        if receipt_row is not None:
            receipt = json.loads(receipt_row["receipt_json"])
            if type(receipt.get("is_unlim_upload")) is bool:
                result["is_unlim_upload"] = receipt["is_unlim_upload"]
            if receipt.get("commit_accepted") is True:
                result["commit_accepted"] = True
            node_id = receipt.get("nodeid", receipt_row["remote_id"])
            if isinstance(node_id, str) and node_id:
                result["nodeid"] = node_id
        return result

    def confirm_unlimited_commit(self, job: Job, cloud_hash: str) -> None:
        """Persist the server's explicit unlimited confirmation before stat.

        An intent alone proves only that a commit was attempted. This separate
        confirmation may be recorded only after the API has received a matching
        successful add response with is_unlim_upload exactly True. It survives
        interruption before stat, but is bound to the same claim/source/account.
        """
        self._confirm_commit(job, cloud_hash, storage_mode="unlimited")

    def confirm_regular_commit(self, job: Job, cloud_hash: str) -> None:
        """Record an acknowledged ordinary add before its final stat check.

        This is confirmation of the regular request, not an unlimited billing
        flag. The transport must first validate a successful create response.
        """
        self._confirm_commit(job, cloud_hash, storage_mode="regular")

    def _confirm_commit(self, job: Job, cloud_hash: str, *, storage_mode: str) -> None:
        if not isinstance(cloud_hash, str) or not cloud_hash or any(c in cloud_hash for c in ("\x00", "\n", "\r")):
            raise ValueError("cloud_hash must be a nonempty hash string")
        with self._transaction():
            claimed = self._require_claim(job)
            row = self._db.execute("SELECT * FROM commit_intents WHERE job_id=?", (job.id,)).fetchone()
            if row is None:
                raise QueueError(f"Cannot confirm {storage_mode} storage without a saved commit intent.")
            try:
                intent = json.loads(row["intent_json"])
            except (ValueError, TypeError):
                raise QueueError("Saved commit intent is invalid; confirmation was not recorded.") from None
            account = self._get_meta("_account_id")
            fingerprint = (row["source_size"], row["source_mtime_ns"], row["device"], int(row["inode"]))
            expected = {"remote_path": job.remote_path, "source_size": job.size,
                        "source_mtime_ns": job.mtime_ns, "device": job.device, "inode": job.inode,
                        "account_id": account, "destination": self.destination}
            if (not account or not isinstance(intent, dict)
                    or intent.get("storage_mode", "unlimited") != storage_mode
                    or claimed["storage_mode"] not in (None, storage_mode)
                    or fingerprint != _job_snapshot(job) or row["remote_path"] != job.remote_path
                    or row["cloud_hash"].casefold() != cloud_hash.casefold()
                    or not isinstance(intent.get("cloud_hash"), str)
                    or intent["cloud_hash"].casefold() != cloud_hash.casefold()
                    or any(intent.get(key) != value for key, value in expected.items())):
                raise QueueError("Saved commit intent does not match this upload; confirmation was not recorded.")
            intent["storage_mode"] = storage_mode
            intent[f"{storage_mode}_confirmed"] = True
            intent[f"{storage_mode}_confirmed_at"] = time.time()
            self._db.execute("UPDATE commit_intents SET intent_json=? WHERE job_id=?",
                             (json.dumps(intent, ensure_ascii=False, allow_nan=False), job.id))
            self._db.execute("UPDATE jobs SET storage_mode=? WHERE id=?", (storage_mode, job.id))

            self._archive_intent(job.id)
        # The accepted remote receipt is already durable before inspecting a
        # local file that may have been replaced, moved, or removed meanwhile.
        snapshot = self._inspect_source(job)
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed after saving {storage_mode} confirmation: {job.local_path}")

    def set_active_batch(self, batch_id: str | None, source_group_id: int | None = None) -> None:
        """Bind new commit attempts to this live controller process only.

        The intent keeps durable provenance after creation. The runtime
        context must not grant ownership to a later CLI run after a crash,
        including a reused process ID or a fork inheriting module globals.
        """
        if batch_id is None:
            if source_group_id is not None:
                raise ValueError("Clearing a batch must not include a source group")
        else:
            batch_id = self._validate_batch_id(batch_id)
            if type(source_group_id) is not int or not 0 < source_group_id < 2**63:
                raise ValueError("source_group_id must be a positive source group ID")
        with self.exclusive_run(), self._transaction():
            if batch_id is None:
                self._put_meta("active_batch", None)
                return
            owner = self._db.execute("SELECT owner_key FROM source_groups WHERE id=?", (source_group_id,)).fetchone()
            if owner is None or owner[0] != "plan-batch:" + batch_id:
                raise QueueConfigurationError("This source group does not belong to the selected batch.")
            self._put_meta("active_batch", {"id": batch_id, "source_group_id": source_group_id,
                                             "pid": os.getpid(), "process_nonce": _PROCESS_NONCE})

    def _active_batch_id(self, row: sqlite3.Row) -> str | None:
        active = self._get_meta("active_batch")
        if (not isinstance(active, dict) or active.get("source_group_id") != row["source_group_id"]
                or type(active.get("pid")) is not int or active["pid"] != os.getpid()
                or active.get("process_nonce") != _PROCESS_NONCE
                or not isinstance(active.get("id"), str) or not 1 <= len(active["id"]) <= 128
                or any(ord(char) < 32 for char in active["id"])):
            return None
        owner = self._db.execute("SELECT owner_key FROM source_groups WHERE id=?", (row["source_group_id"],)).fetchone()
        return active["id"] if owner is not None and owner[0] == "plan-batch:" + active["id"] else None

    def _observation_batch_id(self, row: sqlite3.Row, remote_hash: str | None,
                              remote_path: str, receipt: Mapping[str, Any]) -> str | None:
        """Attribute creation, never mere discovery, to its original party."""
        if (not remote_hash or row["source_group_id"] is None
                or (receipt.get("commit_accepted") is not True and receipt.get("recovered_commit") is not True)):
            return None
        saved = self._db.execute("SELECT * FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone()
        if saved is None:
            return None
        try:
            intent = json.loads(saved["intent_json"])
        except (ValueError, TypeError):
            return None
        if not isinstance(intent, dict) or not isinstance(intent.get("batch_id"), str):
            return None
        batch_id = intent["batch_id"]
        mode = intent.get("storage_mode", "unlimited")
        confirmed = mode in _STORAGE_MODES and intent.get(mode + "_confirmed") is True
        if receipt.get("commit_accepted") is not True and not (confirmed and receipt.get("recovered_commit") is True):
            return None
        owner = self._db.execute("SELECT owner_key FROM source_groups WHERE id=?", (row["source_group_id"],)).fetchone()
        if (owner is None or owner[0] != "plan-batch:" + batch_id
                or intent.get("account_id") != self._get_meta("_account_id")
                or intent.get("destination") != self.destination
                or saved["remote_path"] != remote_path or row["remote_path"] != remote_path
                or saved["cloud_hash"].casefold() != remote_hash.casefold()
                or (saved["source_size"], saved["source_mtime_ns"], saved["device"], saved["inode"])
                != (row["size"], row["mtime_ns"], row["device"], row["inode"])):
            return None
        return batch_id

    def _record_cloud_observation_row(
        self, row: sqlite3.Row, *, mode: str, remote_hash: str | None,
        remote_id: str | None = None, metadata: Mapping[str, Any] | None = None,
        received_at: float | None = None,
    ) -> str | None:
        """Append a receipt and update the current projection, in a transaction.

        This is separate from upload_results. The response may be durable
        before stat/hash validation and may contradict the planned route.
        The optional hash supports incomplete finished legacy records; new
        public observations require a real hash.
        """
        if mode not in _OBSERVED_MODES:
            raise ValueError("mode must be unlimited, regular, or unknown")
        receipt = dict(metadata or {})
        verified = receipt.get("verified") is True
        remote_path = _destination(receipt.get("path", receipt.get("remote_path", row["remote_path"])))
        cloud_size = receipt.get("size", row["size"])
        if type(cloud_size) is not int or cloud_size < 0:
            raise ValueError("Observed cloud size must be a nonnegative integer")
        now = time.time() if received_at is None else received_at
        encoded = json.dumps(receipt, ensure_ascii=False, allow_nan=False)
        cloud_hash = remote_hash.upper() if isinstance(remote_hash, str) else None
        batch_id = self._observation_batch_id(row, cloud_hash, remote_path, receipt)
        self._db.execute(
            "INSERT INTO cloud_observations(job_id,origin_job_id,event,account_id,local_path,"
            "source_size,source_mtime_ns,source_device,source_inode,remote_path,cloud_hash,cloud_size,"
            "remote_id,mode,verified,receipt_json,received_at,batch_id) VALUES(?,?,'stored',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (row["id"], row["id"], self._get_meta("_account_id"), row["local_path"],
             row["size"], row["mtime_ns"], row["device"], row["inode"], remote_path, cloud_hash,
             cloud_size, remote_id, mode, int(verified), encoded, now, batch_id),
        )
        self._db.execute(
            "UPDATE jobs SET observed_mode=?,cloud_present=1,cloud_path=?,cloud_hash=?,cloud_size=?,"
            "cloud_verified=?,cloud_observed_at=?,cloud_deleted_at=NULL WHERE id=?",
            (mode, remote_path, cloud_hash, cloud_size, int(verified), now, row["id"]),
        )
        return batch_id

    def record_cloud_observation(
        self, job: Job, *, mode: str, remote_hash: str,
        remote_id: str | None = None, metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist the server's add response immediately, before final stat.

        mode describes the actual response, not the route requested by the
        client. An absent boolean flag is unknown. This method neither marks
        the task successful nor alters its claim or commit confirmation.
        """
        if mode not in _OBSERVED_MODES:
            raise ValueError("mode must be unlimited, regular, or unknown")
        if (not isinstance(remote_hash, str) or not remote_hash
                or any(char in remote_hash for char in ("\x00", "\n", "\r"))):
            raise ValueError("remote_hash must be a nonempty hash string")
        receipt = dict(metadata or {})
        flag = receipt.get("is_unlim_upload")
        if type(flag) is bool and mode != ("unlimited" if flag else "regular"):
            raise QueueError("Observed storage mode contradicts the server's boolean flag.")
        with self._transaction():
            row = self._require_claim(job)
            if not self._get_meta("_account_id"):
                raise QueueConfigurationError("Bind the authenticated account before recording a cloud response.")
            self._record_cloud_observation_row(
                row, mode=mode, remote_hash=remote_hash, remote_id=remote_id, metadata=receipt,
            )

    def mark_cloud_deleted(self, job_id: int, *, expected_hash: str,
                           expected_path: str | None = None) -> bool:
        """Record confirmed removal of precisely the observed cloud object.

        A mismatched or superseded observation is not cleared. The caller
        performs and validates the remote recycle-bin operation first.
        """
        if not isinstance(expected_hash, str) or not expected_hash:
            raise ValueError("expected_hash must be a nonempty hash string")
        if expected_path is not None:
            expected_path = _destination(expected_path)
        with self._transaction():
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if (row is None or not row["cloud_hash"]
                    or row["cloud_hash"].casefold() != expected_hash.casefold()
                    or (expected_path is not None and row["cloud_path"] != expected_path)):
                return False
            if not row["cloud_present"]:
                return row["cloud_deleted_at"] is not None
            now = time.time()
            receipt = json.dumps({"recycle_bin_confirmed": True, "path": row["cloud_path"],
                                  "hash": row["cloud_hash"]}, ensure_ascii=False, allow_nan=False)
            previous = self._db.execute(
                "SELECT batch_id FROM cloud_observations WHERE account_id IS ? AND remote_path=? "
                "AND cloud_hash=? ORDER BY id DESC LIMIT 1",
                (self._get_meta("_account_id"), row["cloud_path"], row["cloud_hash"]),
            ).fetchone()
            self._db.execute(
                "INSERT INTO cloud_observations(job_id,origin_job_id,event,account_id,local_path,"
                "source_size,source_mtime_ns,source_device,source_inode,remote_path,cloud_hash,cloud_size,"
                "remote_id,mode,verified,receipt_json,received_at,batch_id) VALUES(?,?,'deleted',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, job_id, self._get_meta("_account_id"), row["local_path"], row["size"],
                 row["mtime_ns"], row["device"], row["inode"], row["cloud_path"], row["cloud_hash"],
                 row["cloud_size"], row["remote_id"], row["observed_mode"], row["cloud_verified"], receipt, now,
                 previous[0] if previous else None),
            )
            self._db.execute(
                "UPDATE jobs SET cloud_present=0,cloud_deleted_at=?,updated_at=? WHERE id=?", (now, now, job_id),
            )
            return True

    @staticmethod
    def _validate_batch_id(batch_id: str) -> str:
        if (not isinstance(batch_id, str) or not 1 <= len(batch_id) <= 128
                or any(ord(char) < 32 for char in batch_id)):
            raise ValueError("batch_id must be a nonempty bounded identifier")
        return batch_id

    @staticmethod
    def _batch_receipt_current() -> str:
        # A later discovery of the same object is harmless. A new commit,
        # deletion, different content or another party's receipt supersedes
        # this proof. Cleanup still verifies the actual object before trash.
        return ("o.event='stored' AND NOT EXISTS (SELECT 1 FROM cloud_observations n "
                "INDEXED BY cloud_observations_path WHERE n.account_id IS o.account_id "
                "AND n.remote_path=o.remote_path AND n.id>o.id AND (n.event='deleted' "
                "OR n.batch_id IS NOT NULL OR n.cloud_hash IS NOT o.cloud_hash OR n.cloud_size<>o.cloud_size "
                "OR json_extract(n.receipt_json,'$.commit_accepted')=1))")

    def batch_cloud_objects(self, batch_id: str, *, after_id: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        """Page durable creation receipts, independent of the live task list.

        Only creations attributed by a saved, matching commit attempt appear.
        Reconciled existing objects never acquire batch ownership. The caller
        must compare account, path, size, hash and node ID with fresh cloud
        metadata before moving any object to its recycle bin.
        """
        batch_id = self._validate_batch_id(batch_id)
        if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("after_id must be nonnegative and limit must be in 1..500")
        with self._mutex:
            return [dict(row) for row in self._db.execute(
                "SELECT o.* FROM cloud_observations o INDEXED BY cloud_observations_batch "
                "WHERE o.batch_id=? AND o.id>? AND " + self._batch_receipt_current() + " ORDER BY o.id LIMIT ?",
                (batch_id, after_id, limit),
            )]

    def batch_cloud_object_state(self, batch_id: str, observation_id: int) -> dict[str, Any]:
        """Revalidate one frozen cleanup receipt immediately before cloud I/O.

        A journal survives restarts and can outlive the creation it describes.
        Equal content at the same path is not sufficient proof of continued
        ownership. Only the original current receipt permits another trash
        request; an exact creation-ID tombstone permits idempotent completion.
        """
        batch_id = self._validate_batch_id(batch_id)
        if type(observation_id) is not int or observation_id < 1:
            raise ValueError("observation_id must be a positive integer")
        with self._mutex:
            row = self._db.execute(
                "SELECT * FROM cloud_observations WHERE id=? AND batch_id=? AND event='stored'",
                (observation_id, batch_id),
            ).fetchone()
            if row is None:
                return {"state": "missing", "object": None}
            item = dict(row)
            if row["account_id"] != self._get_meta("_account_id"):
                return {"state": "superseded", "object": item}
            deleted = self._db.execute(
                "SELECT 1 FROM cloud_observations INDEXED BY cloud_observations_path "
                "WHERE account_id IS ? AND remote_path=? AND id>? AND batch_id=? AND event='deleted' "
                "AND cloud_hash IS ? AND cloud_size=? AND json_extract(receipt_json,'$.creation_observation_id')=? LIMIT 1",
                (row["account_id"], row["remote_path"], observation_id, batch_id,
                 row["cloud_hash"], row["cloud_size"], observation_id),
            ).fetchone()
            if deleted is not None:
                return {"state": "deleted", "object": item}
            current = self._db.execute(
                "SELECT 1 FROM cloud_observations o WHERE o.id=? AND " + self._batch_receipt_current(),
                (observation_id,),
            ).fetchone()
            return {"state": "current" if current is not None else "superseded", "object": item}

    def mark_batch_cloud_deleted(self, batch_id: str, observation_id: int, *, expected_hash: str,
                                expected_path: str) -> bool:
        """Record an externally confirmed recycle operation, after rescan too.

        The observation ID freezes the exact original proof. A superseding
        receipt or a different object cannot be cleared by a delayed result.
        """
        batch_id = self._validate_batch_id(batch_id)
        if type(observation_id) is not int or observation_id < 1:
            raise ValueError("observation_id must be a positive integer")
        if not isinstance(expected_hash, str) or not expected_hash:
            raise ValueError("expected_hash must be nonempty")
        expected_path = _destination(expected_path)
        with self._transaction():
            row = self._db.execute(
                "SELECT o.* FROM cloud_observations o WHERE o.id=? AND o.batch_id=? AND " + self._batch_receipt_current(),
                (observation_id, batch_id),
            ).fetchone()
            if (row is None or not row["cloud_hash"] or row["cloud_hash"].casefold() != expected_hash.casefold()
                    or row["remote_path"] != expected_path or row["account_id"] != self._get_meta("_account_id")):
                return False
            now = time.time()
            receipt = json.dumps({"recycle_bin_confirmed": True, "path": expected_path, "hash": row["cloud_hash"],
                                  "creation_observation_id": observation_id}, ensure_ascii=False, allow_nan=False)
            self._db.execute(
                "INSERT INTO cloud_observations(job_id,origin_job_id,event,account_id,local_path,source_size,source_mtime_ns,"
                "source_device,source_inode,remote_path,cloud_hash,cloud_size,remote_id,mode,verified,receipt_json,received_at,batch_id) "
                "VALUES(?,?,'deleted',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row["job_id"], row["origin_job_id"], row["account_id"], row["local_path"], row["source_size"], row["source_mtime_ns"],
                 row["source_device"], row["source_inode"], row["remote_path"], row["cloud_hash"], row["cloud_size"], row["remote_id"],
                 row["mode"], row["verified"], receipt, now, batch_id),
            )
            self._db.execute(
                "UPDATE jobs SET cloud_present=0,cloud_deleted_at=?,updated_at=? WHERE cloud_path=? AND cloud_hash=? AND cloud_present=1",
                (now, now, expected_path, row["cloud_hash"]),
            )
            return True

    def mark_uploaded(
        self,
        job: Job,
        *,
        remote_hash: str | None = None,
        remote_id: str | None = None,
        verified_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist verified remote proof before checking the local source.

        The transport may call this only after verifying the committed object,
        its hash, size and storage mode. A subsequent local change does not
        revoke that proof; it prevents marking the newer local file uploaded.
        """
        metadata = dict(verified_metadata or {})
        storage_mode = metadata.setdefault("storage_mode", "unlimited")
        if storage_mode not in _STORAGE_MODES:
            raise ValueError("verified_metadata.storage_mode must be 'unlimited' or 'regular'")
        if storage_mode == "regular" and metadata.get("is_unlim_upload") is True:
            raise QueueError("A regular upload cannot carry an unlimited confirmation.")
        if storage_mode == "unlimited" and metadata.get("is_unlim_upload") is False:
            raise QueueError("An unlimited upload cannot carry a negative unlimited confirmation.")
        evidence = json.dumps(metadata, ensure_ascii=False, allow_nan=False)
        now = time.time()
        with self._transaction():
            claimed = self._require_claim(job)
            if claimed["storage_mode"] not in (None, storage_mode):
                raise QueueError("Verified storage mode does not match the saved commit intent.")
            batch_id = self._record_cloud_observation_row(
                claimed, mode=storage_mode, remote_hash=remote_hash, remote_id=remote_id,
                metadata={**metadata, "verified": True}, received_at=now,
            )
            self._db.execute(
                "INSERT INTO upload_results(job_id,origin_job_id,account_id,local_path,local_key,remote_path,"
                "source_size,source_mtime_ns,source_device,source_inode,remote_hash,remote_id,verified_metadata,uploaded_at,batch_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job.id, job.id, self._get_meta("_account_id"), job.local_path, claimed["local_key"], job.remote_path,
                 job.size, job.mtime_ns, job.device, str(job.inode), remote_hash, remote_id, evidence, now, batch_id),
            )
            self._archive_intent(job.id)
        snapshot = self._inspect_source(job)
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed during upload: {job.local_path}")
        with self._transaction():
            self._require_claim(job)
            self._db.execute(
                "UPDATE jobs SET status='uploaded',claim_token=NULL,last_error=NULL,skip_reason=NULL,"
                "remote_hash=?,remote_id=?,verified_metadata=?,storage_mode=?,uploaded_at=?,updated_at=?,"
                "completed_at=?,progress_bytes=size,next_attempt=NULL WHERE id=?",
                (remote_hash, remote_id, evidence, storage_mode, now, now, now, job.id),
            )
            self._drop_intent(job.id)
            self._finalize_control(job.id, uploaded=True)

    def mark_failed(self, job: Job, error: str, *, retry_delay: float | None = 60, now: float | None = None) -> None:
        """retry_delay=None makes a failure permanent until explicit retry."""
        if retry_delay is not None and retry_delay < 0:
            raise ValueError("retry_delay cannot be negative")
        current = time.time() if now is None else now
        with self._transaction():
            self._require_claim(job)
            self._db.execute(
                "UPDATE jobs SET status='failed',claim_token=NULL,last_error=?,next_attempt=?,completed_at=?,updated_at=? WHERE id=?",
                (str(error)[:16384], None if retry_delay is None else current + retry_delay,
                 current if retry_delay is None else None, current, job.id),
            )
            self._finalize_control(job.id)

    def mark_skipped(self, job: Job, reason: str, *, code: str = "manual") -> None:
        if not isinstance(code, str) or not code or "\x00" in code:
            raise ValueError("A nonempty skip code is required")
        with self._transaction():
            self._require_claim(job)
            if code not in _REGULAR_SKIP_CODES:
                self._drop_intent(job.id)
            now = time.time()
            self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason=?,claim_token=NULL,last_error=?,"
                "next_attempt=NULL,completed_at=?,updated_at=? WHERE id=?", (code, str(reason)[:16384], now, now, job.id),
            )
            self._finalize_control(job.id)

    def apply_regular_policy(self, upload: bool, *, root_ids: Sequence[int] | None = None) -> dict[str, int]:
        """Reversibly park files planned for quota storage when it is disabled.

        Completed work, live workers, permanent errors, existing cloud
        observations and their receipts are untouched. A policy change never
        needs a filesystem rescan and preserves individual pause flags.
        """
        if type(upload) is not bool:
            raise ValueError("upload must be a boolean")
        selected_roots = self._selected_root_ids(root_ids)
        root_filter, root_parameters = self._root_filter(selected_roots)
        codes = "('regular_size_disabled','regular_format_disabled','large_files_disabled')"
        now = time.time()
        with self._transaction():
            resumed = self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                "last_error=NULL,skip_reason=NULL,completed_at=NULL,updated_at=? WHERE present=1 AND remove_requested=0 "
                "AND cloud_present=0 AND status='skipped' "
                f"AND (skip_reason IN {codes} OR (skip_reason='manual' AND last_error=?)) "
                f"AND (? OR planned_mode='unlimited') AND {root_filter}",
                (now, _LEGACY_LARGE_FILE_ERROR, int(upload), *root_parameters),
            ).rowcount
            skipped = 0
            if not upload:
                skipped = self._db.execute(
                    "UPDATE jobs SET status='skipped',claim_token=NULL,next_attempt=NULL,completed_at=?,updated_at=?,"
                    "skip_reason=CASE WHEN planned_reason='size_limit' THEN 'regular_size_disabled' "
                    "ELSE 'regular_format_disabled' END,"
                    "last_error=CASE WHEN planned_reason='size_limit' THEN ? ELSE ? END "
                    "WHERE present=1 AND remove_requested=0 AND cloud_present=0 AND planned_mode='regular' "
                    f"AND (status='pending' OR (status='failed' AND next_attempt IS NOT NULL)) AND {root_filter}",
                    (now, now, "Файл больше 2 ГБ; загрузка в хранилище с квотой отключена.",
                     "Формат не входит в подтверждённый клиентом список; загрузка в хранилище с квотой отключена.",
                     *root_parameters),
                ).rowcount
            return {"skipped": skipped, "resumed": resumed}

    def skip_large_disabled(self) -> int:
        """Park oversized ready/retryable files without losing commit evidence.

        Recover interrupted claims first under exclusive_run(). Active claims,
        finished files, missing sources, and permanent failures are untouched.
        """
        with self._transaction():
            now = time.time()
            return self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason='large_files_disabled',claim_token=NULL,"
                "last_error=?,next_attempt=NULL,completed_at=?,updated_at=? "
                f"WHERE present=1 AND paused=0 AND remove_requested=0 AND cloud_present=0 AND {_SIZE_PARTITIONS['regular']} AND "
                "(status='pending' OR (status='failed' AND next_attempt IS NOT NULL))",
                (_LARGE_FILES_DISABLED_REASON, now, now),
            ).rowcount

    def retry_large_skipped(self) -> int:
        """Resume only files parked by the large-file setting or its old limit.

        Keep ambiguous/confirmed ordinary intents for remote reconciliation.
        Other unsupported files and deliberate skips stay skipped.
        """
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                "last_error=NULL,skip_reason=NULL,completed_at=NULL,updated_at=? "
                f"WHERE present=1 AND paused=0 AND remove_requested=0 AND cloud_present=0 AND {_SIZE_PARTITIONS['regular']} AND status='skipped' AND "
                "(skip_reason='large_files_disabled' OR (skip_reason='manual' AND last_error=?))",
                (time.time(), _LEGACY_LARGE_FILE_ERROR),
            ).rowcount

    def release(self, job: Job, reason: str = "Interrupted") -> None:
        """Return an unfinished attempt to pending without spending its retry.

        A pause is not a completed failed upload.  Keep any commit intent so a
        resumed worker can reconcile a possible remote commit before retrying.
        """
        with self._transaction():
            self._require_claim(job)
            self._db.execute(
                "UPDATE jobs SET status='pending',attempts=MAX(0,attempts-1),claim_token=NULL,"
                "next_attempt=0,last_error=?,completed_at=NULL,updated_at=? WHERE id=?",
                (str(reason)[:16384], time.time(), job.id),
            )
            self._finalize_control(job.id)

    def recover_running(self) -> int:
        """Recover interrupted claims without spending unfinished attempts.

        A retried claim may already exist remotely if the process died between
        remote commit and local acknowledgement; the transport must reconcile.
        """
        owns_lock = self._runner_lock_depth and self._runner_lock_owner == threading.get_ident()
        lock = contextlib.nullcontext() if owns_lock else self.exclusive_run()
        with lock, _file_lock(self.path + ".scan.lock"), self._transaction():
            self._db.execute("DELETE FROM metadata WHERE key='_scan_token'")
            count = self._db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0]
            self._delete_tasks("remove_requested=1")
            self._db.execute(
                "UPDATE jobs SET status='pending',attempts=MAX(0,attempts-1),claim_token=NULL,next_attempt=0,completed_at=NULL,updated_at=?,"
                "last_error='Interrupted upload; reconcile remote state before retry' WHERE status='running'",
                (time.time(),),
            )
            return count

    def retry_failed(self, *, include_permanent: bool = False, root_ids: Sequence[int] | None = None) -> int:
        root_filter, parameters = self._root_filter(self._selected_root_ids(root_ids))
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,last_error=NULL,completed_at=NULL,updated_at=? "
                "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND (? OR next_attempt IS NOT NULL) "
                f"AND {root_filter}", (time.time(), int(include_permanent), *parameters),
            ).rowcount

    def reconcile_candidates(self, *, after_id: int = 0, limit: int = 500,
                             root_ids: Sequence[int] | None = None) -> list[dict[str, Any]]:
        """One bounded keyset page for an optional, read-only cloud comparison.

        No upload claims are created. The reconciliation pass owns the run
        lock, and final adoption rechecks eligibility and the source snapshot.
        """
        if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("after_id must be nonnegative and limit must be in 1..500")
        selected_roots = self._selected_root_ids(root_ids)
        with self._mutex:
            self._require_idle_sources()
            if selected_roots is not None:
                # Merge streaming root cursors rather than fetching a full
                # page from every source. Memory is bounded by root count
                # plus the requested page, and unrelated rows are never read.
                cursors, heads, result = [], [], []
                try:
                    selections = [(kind, value) for kind, values in (("group", selected_roots.groups), ("root", selected_roots.partial))
                                  for value in values]
                    if len(selections) > 32:
                        where, parameters = self._root_filter(selected_roots)
                        return [dict(row) for row in self._db.execute(
                            "SELECT id,root_id,relative_path,local_path,remote_path,size,mtime_ns,device,inode "
                            "FROM jobs INDEXED BY jobs_reconcile WHERE id>? "
                            "AND present=1 AND paused=0 AND remove_requested=0 AND status IN ('pending','failed') "
                            "AND cloud_present=0 AND size>0 "
                            f"AND {where} AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id) ORDER BY id LIMIT ?",
                            (after_id, *parameters, limit),
                        )]
                    for kind, source_id in selections:
                        column = "source_group_id" if kind == "group" else "root_id"
                        cursor = self._db.execute(
                            "SELECT id,root_id,relative_path,local_path,remote_path,size,mtime_ns,device,inode "
                            f"FROM jobs INDEXED BY jobs_reconcile_{kind} WHERE {column}=? AND id>? "
                            "AND present=1 AND paused=0 AND remove_requested=0 AND status IN ('pending','failed') "
                            "AND cloud_present=0 AND size>0 "
                            "AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id) ORDER BY id LIMIT ?",
                            (source_id, after_id, limit),
                        )
                        cursors.append(cursor)
                        row = cursor.fetchone()
                        if row is not None:
                            heapq.heappush(heads, (row["id"], len(cursors) - 1, row))
                    while heads and len(result) < limit:
                        _, index, row = heapq.heappop(heads)
                        result.append(dict(row))
                        row = cursors[index].fetchone()
                        if row is not None:
                            heapq.heappush(heads, (row["id"], index, row))
                    return result
                finally:
                    for cursor in cursors:
                        cursor.close()
            return [dict(row) for row in self._db.execute(
                "SELECT id,root_id,relative_path,local_path,remote_path,size,mtime_ns,device,inode "
                "FROM jobs INDEXED BY jobs_reconcile WHERE id>? AND present=1 AND paused=0 AND remove_requested=0 "
                "AND status IN ('pending','failed') AND cloud_present=0 AND size>0 "
                "AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id) ORDER BY id LIMIT ?",
                (after_id, limit),
            )]

    def mark_remote_existing(self, candidate: Mapping[str, Any], *, remote_hash: str,
                             remote_path: str | None = None, storage_mode: str = "unknown",
                             remote_id: str | None = None, evidence: Mapping[str, Any] | None = None) -> bool:
        """Record a current exact-content observation at any cloud path.

        Return False when a concurrent pause/removal/source change makes this
        candidate stale. The caller has already compared the cloud hash and
        size; the local snapshot and current mapping are rechecked here. A
        discovered file is separate from one uploaded by the current task.
        """
        if (not isinstance(remote_hash, str) or len(remote_hash) != 40
                or any(char not in "0123456789abcdefABCDEF" for char in remote_hash)):
            raise ValueError("A valid Mail Cloud hash is required")
        if storage_mode not in _OBSERVED_MODES:
            raise ValueError("storage_mode must be unlimited, regular, or unknown")
        actual_path = _destination(remote_path if remote_path is not None else candidate["remote_path"])
        supplied = dict(evidence or {})
        flag = supplied.get("is_unlim_upload")
        if type(flag) is bool and storage_mode != ("unlimited" if flag else "regular"):
            raise QueueError("Observed storage mode contradicts the current server flag.")
        ctime_ns = candidate.get("ctime_ns")
        if ctime_ns is not None and (type(ctime_ns) is not int or ctime_ns < 0):
            raise ValueError("ctime_ns must be a nonnegative integer when supplied")
        expected = (candidate["size"], candidate["mtime_ns"], candidate["device"], int(candidate["inode"]))
        if expected[0] <= 0:
            return False
        with self._transaction():
            account = self._get_meta("_account_id")
            if not account:
                raise QueueConfigurationError("Bind the authenticated account before comparing cloud files.")
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Wait for scanning to finish before comparing cloud files.")
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (candidate["id"],)).fetchone()
            if (row is None or not row["present"] or row["paused"] or row["remove_requested"] or row["cloud_present"]
                    or row["status"] not in ("pending", "failed")
                    or any(row[key] != candidate[key] for key in ("root_id", "relative_path", "local_path", "remote_path"))
                    or (row["size"], row["mtime_ns"], row["device"], int(row["inode"])) != expected
                    or self._db.execute("SELECT 1 FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone()):
                return False
            try:
                current = os.stat(row["local_path"], follow_symlinks=False)
                if (not stat.S_ISREG(current.st_mode)
                        or (current.st_size, current.st_mtime_ns, current.st_dev, current.st_ino) != expected
                        or (ctime_ns is not None and getattr(current, "st_ctime_ns", None) != ctime_ns)):
                    return False
            except OSError:
                return False
            observation = {
                **supplied, "remote_exists": True, "account_id": account, "path": actual_path,
                "hash": remote_hash.upper(), "size": row["size"], "observed_at": time.time(),
                "quota_status": storage_mode, "verified": True,
            }
            self._record_cloud_observation_row(
                row, mode=storage_mode, remote_hash=remote_hash, remote_id=remote_id, metadata=observation,
                received_at=observation["observed_at"],
            )
            self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason='remote_exists',claim_token=NULL,"
                "next_attempt=NULL,last_error=NULL,remote_hash=?,remote_id=?,verified_metadata=?,"
                "storage_mode=NULL,uploaded_at=NULL,completed_at=?,progress_bytes=size,updated_at=? WHERE id=?",
                (remote_hash.upper(), remote_id, json.dumps(observation, ensure_ascii=False, allow_nan=False),
                 observation["observed_at"], observation["observed_at"], row["id"]),
            )
            # This new observation resolves a previous uncertain attempt for
            # the same source. Its archived evidence remains immutable.
            self._db.execute(
                "DELETE FROM rescan_intents WHERE local_key=? AND account_id=? AND source_size=? "
                "AND source_mtime_ns=? AND device=? AND inode=?",
                (row["local_key"], account, row["size"], row["mtime_ns"], row["device"], row["inode"]),
            )
            return True

    def stats(self, *, now: float | None = None, storage_mode: str | None = None,
              include_missing: bool = False, root_ids: Sequence[int] | None = None) -> dict[str, Any]:
        current = time.time() if now is None else now
        selected_roots = self._selected_root_ids(root_ids)
        root_filter, root_parameters = self._root_filter(selected_roots)
        partition = _size_partition(storage_mode) + " AND " + root_filter
        selected = partition if include_missing else partition + " AND present=1"
        # The generic status/retry index can look cheaper to SQLite even
        # when virtually every matching status belongs to another source.
        # A scheduled source summary must visit only its selected roots,
        # including permanent errors, future retries and adopted files.
        source = "jobs"
        if selected_roots is not None:
            if not selected_roots.partial:
                source = "jobs INDEXED BY jobs_group"
            elif not selected_roots.groups:
                source = "jobs INDEXED BY jobs_root_scan"
        with self._mutex:
            counts = {state: 0 for state in STATES}
            sizes = {state: 0 for state in STATES}
            for row in self._db.execute(
                f"SELECT status,COUNT(*) AS n,COALESCE(SUM(size),0) AS bytes FROM {source} WHERE {selected} GROUP BY status",
                root_parameters,
            ):
                counts[row["status"]] = row["n"]
                sizes[row["status"]] = row["bytes"]
            ready = self._db.execute(
                f"SELECT COUNT(*) FROM {source} WHERE {partition} AND present=1 AND paused=0 AND remove_requested=0 AND (status='pending' OR "
                "(status='failed' AND next_attempt IS NOT NULL AND next_attempt<=?))", (*root_parameters, current),
            ).fetchone()[0]
            next_retry = self._db.execute(
                f"SELECT MIN(next_attempt) FROM {source} WHERE {partition} AND status='failed' AND present=1 "
                "AND paused=0 AND remove_requested=0 AND next_attempt>?",
                (*root_parameters, current),
            ).fetchone()[0]
            permanent = self._db.execute(
                f"SELECT COUNT(*) FROM {source} WHERE {selected} AND status='failed' AND next_attempt IS NULL", root_parameters,
            ).fetchone()[0]
            existing = self._db.execute(
                f"SELECT COUNT(*) AS count,COALESCE(SUM(size),0) AS bytes FROM {source} "
                f"WHERE {selected} AND status='skipped' AND skip_reason='remote_exists'", root_parameters,
            ).fetchone()
            controls = self._db.execute(
                f"SELECT COALESCE(SUM(paused),0) AS paused,COALESCE(SUM(remove_requested),0) AS removing,"
                "COALESCE(SUM(CASE WHEN present=1 AND paused=0 AND remove_requested=0 AND (status IN ('pending','running') "
                "OR (status='failed' AND next_attempt IS NOT NULL)) THEN 1 ELSE 0 END),0) AS remaining,"
                "COALESCE(SUM(CASE WHEN status IN ('uploaded','skipped') OR "
                "(status='failed' AND next_attempt IS NULL) THEN 1 ELSE 0 END),0) AS processed,"
                "COALESCE(SUM(CASE WHEN status IN ('uploaded','skipped') OR "
                f"(status='failed' AND next_attempt IS NULL) THEN size ELSE 0 END),0) AS processed_bytes FROM {source} WHERE {selected}",
                root_parameters,
            ).fetchone()
            # Planned bytes represent transfers still to do. A failed task
            # with an accepted cloud object belongs only to observed totals,
            # including an explicit false flag that consumed quota.
            planned = ("cloud_present=0 AND (status IN ('pending','running','failed') OR "
                       "(status='skipped' AND skip_reason IN "
                       "('regular_size_disabled','regular_format_disabled','large_files_disabled')))")
            expressions = []
            for mode in _STORAGE_MODES:
                for suffix, amount in (("count", "1"), ("bytes", "size")):
                    expressions.append(
                        f"COALESCE(SUM(CASE WHEN planned_mode='{mode}' AND ({planned}) "
                        f"THEN {amount} ELSE 0 END),0) AS planned_{mode}_{suffix}"
                    )
            for mode in _OBSERVED_MODES:
                for suffix, amount in (("count", "1"), ("bytes", "COALESCE(cloud_size,size)")):
                    expressions.append(
                        f"COALESCE(SUM(CASE WHEN cloud_present=1 AND observed_mode='{mode}' "
                        "AND NOT (status='skipped' AND COALESCE(skip_reason,'')='remote_exists') "
                        f"THEN {amount} ELSE 0 END),0) AS uploaded_{mode}_{suffix}"
                    )
                    expressions.append(
                        f"COALESCE(SUM(CASE WHEN cloud_present=1 AND observed_mode='{mode}' "
                        "AND status='skipped' AND skip_reason='remote_exists' "
                        f"THEN {amount} ELSE 0 END),0) AS existing_{mode}_{suffix}"
                    )
            storage = dict(self._db.execute(
                "SELECT " + ",".join(expressions) + f" FROM {source} WHERE {selected}", root_parameters,
            ).fetchone())
            for mode in _OBSERVED_MODES:
                for suffix in ("count", "bytes"):
                    storage[f"transferred_{mode}_{suffix}"] = storage[f"uploaded_{mode}_{suffix}"]
            for category in ("uploaded", "existing", "transferred"):
                for suffix in ("count", "bytes"):
                    storage[f"{category}_total_{suffix}"] = sum(storage[f"{category}_{mode}_{suffix}"] for mode in _OBSERVED_MODES)
            return {
                **counts, "total": sum(counts.values()), "total_bytes": sum(sizes.values()),
                "uploaded_bytes": sizes["uploaded"], "bytes_by_status": sizes,
                "ready": ready, "permanent_failures": permanent, "next_retry_at": next_retry,
                "existing": existing["count"], "existing_bytes": existing["bytes"],
                "skipped_tasks_count": counts["skipped"] - existing["count"],
                "skipped_tasks_bytes": sizes["skipped"] - existing["bytes"],
                "scan_active": bool(self._get_meta("_scan_token")),
                "roots": (len(selected_roots) if selected_roots is not None
                          else self._db.execute("SELECT COUNT(*) FROM roots").fetchone()[0]),
                **dict(controls), **storage,
            }

    @staticmethod
    def _task_filter(status: str | None, *, include_missing: bool) -> tuple[str, list[Any]]:
        if status is not None and status not in TASK_FILTERS:
            raise ValueError(f"Unknown status: {status}")
        conditions = [] if include_missing else ["present=1"]
        parameters: list[Any] = []
        if status == "paused":
            conditions.append("paused=1")
        elif status in ("uploaded_unlimited", "uploaded_regular", "uploaded_unknown"):
            conditions.extend(("cloud_present=1", "observed_mode=?",
                               "NOT (status='skipped' AND COALESCE(skip_reason,'')='remote_exists')"))
            parameters.append(status.removeprefix("uploaded_"))
        elif status is not None:
            conditions.append(f"({_FILTER_STATE})=?")
            parameters.append(status)
        return " AND ".join(conditions) or "1", parameters

    @staticmethod
    def _task_source(scope: _RootScope | None, *, status: str | None = None,
                     sort_by: str | None = None, include_missing: bool = False) -> str:
        if scope is None:
            return "jobs"
        if scope.partial:
            return "jobs INDEXED BY jobs_root_scan" if not scope.groups else "jobs"
        if len(scope.groups) != 1 or sort_by is None or include_missing:
            return "jobs INDEXED BY jobs_group"
        category = ("sort" if status is None else "paused" if status == "paused"
                    else "observed" if status.startswith("uploaded_") else "filter")
        return f"jobs INDEXED BY jobs_group_{category}_{sort_by}"

    def list_jobs(self, status: str | None = None, *, limit: int = 100, offset: int = 0,
                  summary: bool = False, include_missing: bool = False,
                  sort: str = "asc", sort_by: str = "created_at",
                  root_ids: Sequence[int] | None = None) -> list[dict[str, Any]]:
        if type(limit) is not int or type(offset) is not int or not 1 <= limit <= 10000 or offset < 0:
            raise ValueError("limit must be in 1..10000 and offset nonnegative")
        order = self._task_order(sort_by, sort)
        # A fixed projection keeps persisted verification evidence out of UI
        # polling. The default retains the complete records used by the CLI.
        columns = _JOB_SUMMARY_COLUMNS if summary else "*"
        where, parameters = self._task_filter(status, include_missing=include_missing)
        selected_roots = self._selected_root_ids(root_ids)
        source = self._task_source(selected_roots, status=status, sort_by=sort_by, include_missing=include_missing)
        scope, scope_parameters = self._root_filter(selected_roots)
        where += " AND " + scope
        parameters.extend(scope_parameters)
        parameters.extend((limit, offset))
        with self._mutex:
            return [self._task_dict(row) for row in self._db.execute(
                f"SELECT {columns} FROM {source} WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
                parameters,
            )]

    @staticmethod
    def _task_order(sort_by: str, sort: str) -> str:
        if not isinstance(sort_by, str) or sort_by not in _SORT_EXPRESSIONS:
            raise ValueError("Unknown task sort field")
        if sort not in ("asc", "desc"):
            raise ValueError("sort must be asc or desc")
        return _SORT_EXPRESSIONS[sort_by] + " " + sort + ",id " + sort

    def iter_export(self, status: str | None = None, *, sort_by: str = "created_at",
                    sort: str = "asc", batch_size: int = 500,
                    root_ids: Sequence[int] | None = None) -> Iterator[dict[str, Any]]:
        """Stream one coherent SELECT snapshot; caller owns a separate Queue.

        Closing the iterator on a disconnected download closes the cursor and
        releases its SQLite read snapshot. Memory never scales with row count.
        """
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be in 1..1000")
        where, parameters = self._task_filter(status, include_missing=False)
        selected_roots = self._selected_root_ids(root_ids)
        source = self._task_source(selected_roots, status=status, sort_by=sort_by)
        scope, scope_parameters = self._root_filter(selected_roots)
        where += " AND " + scope
        parameters.extend(scope_parameters)
        order = self._task_order(sort_by, sort)
        with self._mutex:
            cursor = self._db.execute(f"SELECT {_JOB_SUMMARY_COLUMNS} FROM {source} WHERE {where} ORDER BY {order}", parameters)
            try:
                while rows := cursor.fetchmany(batch_size):
                    for row in rows:
                        yield self._task_dict(row)
            finally:
                cursor.close()

    def jobs_summary(self, status: str | None = None, *, include_missing: bool = False,
                     root_ids: Sequence[int] | None = None) -> dict[str, int]:
        """Totals for the complete selected tab, independent of pagination."""
        where, parameters = self._task_filter(status, include_missing=include_missing)
        selected_roots = self._selected_root_ids(root_ids)
        source = self._task_source(selected_roots)
        scope, scope_parameters = self._root_filter(selected_roots)
        where += " AND " + scope
        parameters.extend(scope_parameters)
        with self._mutex:
            return dict(self._db.execute(
                f"SELECT COUNT(*) AS total,COALESCE(SUM(size),0) AS total_bytes FROM {source} WHERE {where}", parameters,
            ).fetchone())

    def iter_tasks(self, *, after_id: int = 0, limit: int = 500, status: str | None = None,
                   include_missing: bool = True, root_ids: Sequence[int] | None = None) -> list[dict[str, Any]]:
        """One bounded keyset page for deletion/snapshot operations.

        Unlike UI sorting this always follows the stable primary key. Rows
        disappear safely while an operation advances through the snapshot;
        task IDs are never reused.
        """
        if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("after_id must be nonnegative and limit must be in 1..500")
        where, parameters = self._task_filter(status, include_missing=include_missing)
        selected_roots = self._selected_root_ids(root_ids)
        source = self._task_source(selected_roots)
        scope, scope_parameters = self._root_filter(selected_roots)
        where += " AND " + scope
        parameters.extend(scope_parameters)
        with self._mutex:
            return [self._task_dict(row) for row in self._db.execute(
                f"SELECT * FROM {source} WHERE id>? AND ({where}) ORDER BY id LIMIT ?", (after_id, *parameters, limit),
            )]

    def get_job(self, job_id: int, *, include_missing: bool = False) -> dict[str, Any] | None:
        """Return a task without loading or decoding unrelated queue rows."""
        where = "id=?" if include_missing else "id=? AND present=1"
        with self._mutex:
            row = self._db.execute(f"SELECT * FROM jobs WHERE {where}", (job_id,)).fetchone()
            return self._task_dict(row) if row is not None else None

    def find_current_job(self, root_id: int, local_path: str | os.PathLike[str]) -> dict[str, Any] | None:
        """Locate a fresh task after a rebuild without reviving an obsolete ID."""
        if type(root_id) is not int or not 0 < root_id < 2**63:
            raise ValueError("root_id must be a positive source ID")
        key = os.path.normcase(os.path.abspath(os.fspath(local_path)))
        with self._mutex:
            row = self._db.execute("SELECT * FROM jobs WHERE local_key=? AND root_id=? AND present=1", (key, root_id)).fetchone()
            return self._task_dict(row) if row is not None else None

    def commit_history(self, job_id: int | None, *, limit: int = 100) -> list[dict[str, Any]]:
        """Receipts are attempted/accepted commits, never upload success flags."""
        if not 1 <= limit <= 10000:
            raise ValueError("limit must be in 1..10000")
        with self._mutex:
            column = "job_id" if job_id is None else "origin_job_id"
            return [dict(row) for row in self._db.execute(
                f"SELECT * FROM commit_evidence WHERE {column} IS ? ORDER BY id DESC LIMIT ?", (job_id, limit),
            )]

    def cloud_history(self, job_id: int | None, *, limit: int = 100) -> list[dict[str, Any]]:
        """Immutable add/deletion observations, including detached task history."""
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValueError("limit must be in 1..10000")
        with self._mutex:
            column = "job_id" if job_id is None else "origin_job_id"
            result = []
            for row in self._db.execute(
                f"SELECT * FROM cloud_observations WHERE {column} IS ? ORDER BY id DESC LIMIT ?", (job_id, limit),
            ):
                item = dict(row)
                item["verified"] = bool(item["verified"])
                result.append(item)
            return result

    def upload_history(self, job_id: int | None) -> list[dict[str, Any]]:
        with self._mutex:
            result = []
            column = "job_id" if job_id is None else "origin_job_id"
            for row in self._db.execute(f"SELECT * FROM upload_results WHERE {column} IS ? ORDER BY id", (job_id,)):
                item = dict(row)
                item["verified_metadata"] = json.loads(item["verified_metadata"] or "{}")
                result.append(item)
            return result

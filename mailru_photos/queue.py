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
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping
import unicodedata
import uuid

from .media import UNLIMITED_MAX_BYTES


# Discovery, not an assertion that every extension is eligible for unlimited
# storage. A transport can pass a narrower, server-supported set to scan(). The
# video list follows the mobile client's videoExtensions/MIME classification;
# the server's per-file response remains the authority on storage eligibility.
PHOTO_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".gif"})
VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".3gp", ".3g2", ".3gpp", ".3gpp2", ".avi", ".vob", ".mov",
    ".mkv", ".m2t", ".m2ts", ".ts", ".mts", ".f4v", ".flv", ".wmv", ".m4v",
    ".mpg", ".ogv", ".webm", ".hevc",
})
MEDIA_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS
STATES = ("pending", "running", "uploaded", "failed", "skipped")
SCHEMA_VERSION = 3
_SCHEMA_OBJECTS = frozenset({
    ("table", "metadata"), ("table", "roots"), ("table", "scans"),
    ("table", "jobs"), ("table", "upload_results"), ("table", "commit_intents"),
    ("table", "commit_evidence"),
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
})
_JOB_SUMMARY_COLUMNS = (
    "id,root_id,relative_path,local_path,remote_path,status,size,attempts,present,"
    "last_error,skip_reason,next_attempt,uploaded_at,storage_mode,paused,remove_requested"
)
_UPLOAD_RESULT_COLUMNS = frozenset({
    "id", "job_id", "origin_job_id", "account_id", "local_path", "local_key", "remote_path",
    "source_size", "source_mtime_ns", "source_device", "source_inode",
    "remote_hash", "remote_id", "verified_metadata", "uploaded_at",
})
_STORAGE_MODES = ("unlimited", "regular")
_SIZE_PARTITIONS = {
    "unlimited": f"size<={UNLIMITED_MAX_BYTES}",
    "regular": f"size>{UNLIMITED_MAX_BYTES}",
}
_LEGACY_LARGE_FILE_ERROR = "Файл больше 2 ГБ; он не отправлен обычной загрузкой."
_LARGE_FILES_DISABLED_REASON = "Файл больше 2 ГБ: этот размер не поддерживается автозагрузкой."


def _size_partition(storage_mode: str | None) -> str:
    if storage_mode is None:
        return "1"
    if storage_mode not in _STORAGE_MODES:
        raise ValueError("storage_mode must be 'unlimited', 'regular', or None")
    # Literal predicates match the partial indexes. A parameterized threshold
    # cannot let SQLite prove that a partial index covers the entire query.
    return _SIZE_PARTITIONS[storage_mode]


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
            self._initialize(account_id)
        except BaseException:
            self._db.close()
            raise

    def _initialize(self, account_id: str | None) -> None:
        schema_version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if schema_version not in (0, 1, 2, SCHEMA_VERSION):
            raise QueueConfigurationError(f"Unsupported queue schema {schema_version}; expected {SCHEMA_VERSION}.")
        objects = {(row["type"], row["name"]) for row in self._db.execute(
            "SELECT type,name FROM sqlite_master WHERE type IN ('table','index')"
        )}
        job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
        has_schema = (_SCHEMA_OBJECTS.issubset(objects) and {"storage_mode", "paused", "remove_requested"}.issubset(job_columns)
                      and self._has_detached_upload_results())
        if ("table", "metadata") in objects:
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            needs_account = account_id is not None and self._account_needs_binding(account_id)
            # Polling/read connections must neither take the writer lock nor
            # commit metadata. Schema inspection above is entirely read-only.
            if has_schema and schema_version == SCHEMA_VERSION and existing is not None and not needs_account:
                return
        with self._transaction():
            # A second first-opener may have migrated while this one waited.
            schema_version = self._db.execute("PRAGMA user_version").fetchone()[0]
            if schema_version not in (0, 1, 2, SCHEMA_VERSION):
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
            for table in ("upload_results", "commit_evidence"):
                columns = {row["name"] for row in self._db.execute(f"PRAGMA table_info({table})")}
                if "origin_job_id" not in columns:
                    self._db.execute(f"ALTER TABLE {table} ADD COLUMN origin_job_id INTEGER")
                    # Detached schema-2 records have lost their original task
                    # ID. Never guess an origin for those historical rows.
                    self._db.execute(f"UPDATE {table} SET origin_job_id=job_id WHERE job_id IS NOT NULL")
            if not self._has_detached_upload_results():
                self._migrate_upload_results()
            if schema_version < 3:
                # Schema-2 claim indexes included paused rows. Rebuild once
                # so a large paused prefix does not make each claim linear.
                for name in ("jobs_pending", "jobs_pending_unlimited", "jobs_pending_regular",
                             "jobs_retry_unlimited", "jobs_retry_regular"):
                    self._db.execute(f"DROP INDEX IF EXISTS {name}")
                self._preserve_job_ids()
            self._create_queue_indexes()
            if schema_version != SCHEMA_VERSION:
                self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _create_core_schema(self) -> None:
        # execute(), not executescript(): Python's executescript implicitly
        # commits an open transaction and would make this migration partial.
        statements = (
            """CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
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
        )
        for statement in statements:
            self._db.execute(statement)
        if not self._db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='upload_results'").fetchone():
            self._create_upload_results("upload_results")

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

    def _create_queue_indexes(self) -> None:
        statements = (
            "CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,present,next_attempt,id)",
            "CREATE INDEX IF NOT EXISTS jobs_pending ON jobs(id) WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0",
            f"CREATE INDEX IF NOT EXISTS jobs_pending_unlimited ON jobs(id) "
            f"WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0 AND size<={UNLIMITED_MAX_BYTES}",
            f"CREATE INDEX IF NOT EXISTS jobs_pending_regular ON jobs(id) "
            f"WHERE status='pending' AND present=1 AND paused=0 AND remove_requested=0 AND size>{UNLIMITED_MAX_BYTES}",
            "CREATE INDEX IF NOT EXISTS jobs_retry ON jobs(next_attempt,id) "
            "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL",
            f"CREATE INDEX IF NOT EXISTS jobs_retry_unlimited ON jobs(next_attempt,id) "
            f"WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL AND size<={UNLIMITED_MAX_BYTES}",
            f"CREATE INDEX IF NOT EXISTS jobs_retry_regular ON jobs(next_attempt,id) "
            f"WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL AND size>{UNLIMITED_MAX_BYTES}",
            "CREATE INDEX IF NOT EXISTS jobs_root_scan ON jobs(root_id,seen_scan)",
            "CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status,id)",
            "CREATE INDEX IF NOT EXISTS jobs_current ON jobs(id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_current_status ON jobs(status,id) WHERE present=1",
            "CREATE INDEX IF NOT EXISTS jobs_source_identity ON jobs(root_id,device,inode,size,mtime_ns)",
            "CREATE INDEX IF NOT EXISTS jobs_paused ON jobs(id) WHERE present=1 AND paused=1",
            "CREATE INDEX IF NOT EXISTS upload_results_job ON upload_results(job_id)",
            "CREATE INDEX IF NOT EXISTS commit_evidence_job ON commit_evidence(job_id)",
            "CREATE INDEX IF NOT EXISTS upload_results_origin ON upload_results(origin_job_id)",
            "CREATE INDEX IF NOT EXISTS commit_evidence_origin ON commit_evidence(origin_job_id)",
            "CREATE INDEX IF NOT EXISTS jobs_reconcile ON jobs(id) WHERE present=1 AND paused=0 AND remove_requested=0 "
            f"AND status IN ('pending','failed') AND size>0 AND size<={UNLIMITED_MAX_BYTES}",
        )
        for statement in statements:
            self._db.execute(statement)

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
            if stored is None or (stored == PHOTO_EXTENSIONS and self._get_meta("extensions_custom") is not True):
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
                return stored == PHOTO_EXTENSIONS
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
        item["paused"] = bool(item["paused"])
        item["remove_requested"] = bool(item["remove_requested"])
        return item

    def request_control(self, job_id: int, action: str) -> dict[str, Any] | None:
        """Request a task action without revoking a live worker's claim.

        The controller owns the run lock when idle, or its running pipeline.
        This short transaction can run from another thread/connection while
        the worker owns the run lock. Task removal never invalidates last_scan.
        """
        if action not in ("pause", "resume", "remove"):
            raise ValueError("Task action must be pause, resume, or remove")
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
            elif row["status"] == "uploaded" or (row["status"] == "skipped" and row["skip_reason"] == "remote_exists"):
                return self._task_dict(row)
            elif action == "pause":
                self._db.execute("UPDATE jobs SET paused=1,updated_at=? WHERE id=?", (time.time(), job_id))
            elif row["status"] == "running":
                self._db.execute("UPDATE jobs SET paused=0,updated_at=? WHERE id=?", (time.time(), job_id))
            else:
                self._db.execute(
                    "UPDATE jobs SET paused=0,status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                    "last_error=NULL,skip_reason=NULL,updated_at=? WHERE id=?", (time.time(), job_id),
                )
            return self._task_dict(self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def task_control(self, job: Job) -> str | None:
        """Read one live claim's cancellation request, without database writes."""
        with self._mutex:
            row = self._require_claim(job)
            return "remove" if row["remove_requested"] else "pause" if row["paused"] else None

    def clear_tasks(self, scope: str = "all") -> int:
        """Clear idle queue tasks; source registrations and physical files stay."""
        if scope not in ("all", "finished"):
            raise ValueError("Task clear scope must be all or finished")
        where = "1" if scope == "all" else "(status IN ('uploaded','skipped') OR (status='failed' AND next_attempt IS NULL))"
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            return self._delete_tasks(where)

    def add_root(self, path: str | os.PathLike[str], alias: str, remote_prefix: str | None = None) -> Root:
        """Register a source with an exact optional cloud destination.

        Omitting remote_prefix preserves the legacy destination/alias mapping.
        Re-adding the same source never silently changes an existing mapping.
        """
        root_path = _path_key(Path(path).expanduser())
        if not os.path.isdir(root_path):
            raise QueueConfigurationError(f"Source root is not an accessible directory: {root_path}")
        alias = _alias(alias)
        selected_prefix = (_destination(remote_prefix) if remote_prefix is not None
                           else self.destination.rstrip("/") + "/" + alias)
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            for row in self._db.execute("SELECT * FROM roots"):
                if row["path_key"] == root_path:
                    if row["alias"] != alias or (remote_prefix is not None and row["remote_prefix"] != selected_prefix):
                        raise QueueConfigurationError("Use update_root to change an existing source mapping.")
                    return self._root(row)
                if row["alias_key"] == _remote_key(alias):
                    raise ScanCollisionError(f"Root alias {alias!r} is already used by another source directory.")
                try:
                    common = os.path.commonpath((row["path_key"], root_path))
                except ValueError:  # Separate drives on Windows.
                    common = ""
                if common in (row["path_key"], root_path):
                    raise QueueConfigurationError("Source roots cannot overlap; choose the parent root or separate children.")
            root_id = self._root_id_high_water() + 1
            self._db.execute(
                "INSERT INTO roots(id,path,path_key,alias,alias_key,remote_prefix) VALUES(?,?,?,?,?,?)",
                (root_id, root_path, root_path, alias, _remote_key(alias), selected_prefix),
            )
            self._put_meta("_root_id_high_water", root_id)
            self._put_meta("last_scan", {"complete": False, "reason": "source_added"})
            return Root(root_id, root_path, alias, selected_prefix)

    def update_root(self, root_id: int, remote_prefix: str, alias: str | None = None) -> Root:
        """Retarget work that has not been sent; keep actual uploaded paths.

        Already uploaded tasks and tasks with a durable commit intent retain
        their remote path. No server-side move, delete, or upload occurs here.
        """
        selected_prefix = _destination(remote_prefix)
        selected_alias = _alias(alias) if alias is not None else None
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            root = self._db.execute("SELECT * FROM roots WHERE id=?", (root_id,)).fetchone()
            if root is None:
                raise QueueConfigurationError(f"Unknown source root: {root_id}")
            selected_alias = root["alias"] if selected_alias is None else selected_alias
            if self._db.execute("SELECT 1 FROM roots WHERE alias_key=? AND id<>?",
                                (_remote_key(selected_alias), root_id)).fetchone():
                raise ScanCollisionError(f"Root alias {selected_alias!r} is already used by another source directory.")
            if selected_prefix == root["remote_prefix"] and selected_alias == root["alias"]:
                return self._root(root)
            now = time.time()
            try:
                if selected_prefix != root["remote_prefix"]:
                    mutable = ("root_id=? AND status<>'uploaded' AND NOT (status='skipped' AND COALESCE(skip_reason,'')='remote_exists') AND NOT EXISTS "
                               "(SELECT 1 FROM commit_intents WHERE job_id=jobs.id)")
                    # Temporarily free old keys inside this transaction. This
                    # permits a destination below the old prefix without false
                    # collisions against another task that is also moving.
                    # Valid cloud keys always begin with '/', unlike this key.
                    temporary_key = "remap:" + uuid.uuid4().hex + ":"
                    self._db.execute(f"UPDATE jobs SET remote_key=?||id WHERE {mutable}",
                                     (temporary_key, root_id))
                    cursor = self._db.execute(f"SELECT id,relative_path FROM jobs WHERE {mutable} ORDER BY id", (root_id,))
                    while rows := cursor.fetchmany(500):
                        updates = []
                        for row in rows:
                            remote = selected_prefix.rstrip("/") + "/" + row["relative_path"]
                            updates.append((remote, _remote_key(remote), now, row["id"]))
                        self._db.executemany("UPDATE jobs SET remote_path=?,remote_key=?,updated_at=? WHERE id=?", updates)
                    cursor.close()
                self._db.execute("UPDATE roots SET alias=?,alias_key=?,remote_prefix=? WHERE id=?",
                                 (selected_alias, _remote_key(selected_alias), selected_prefix, root_id))
            except sqlite3.IntegrityError as exc:
                raise ScanCollisionError("The selected cloud folder conflicts with another task's destination.") from exc
            self._put_meta("last_scan", {"complete": False, "reason": "source_updated"})
            return Root(root_id, root["path"], selected_alias, selected_prefix)

    def remove_root(self, root_id: int) -> int:
        """Forget a source and its tasks, retaining detached upload evidence.

        Neither local originals nor cloud objects are deleted. Returned count
        includes absent historical task rows; upload_results remain separate.
        """
        with self.exclusive_run(), self._transaction():
            self._require_idle_sources()
            if not self._db.execute("SELECT 1 FROM roots WHERE id=?", (root_id,)).fetchone():
                raise QueueConfigurationError(f"Unknown source root: {root_id}")
            # Legacy queues may not yet have a sequence. Save it before the
            # largest/current root disappears so a stale browser action can
            # never address a subsequently added source using the old ID.
            highest = self._root_id_high_water()
            if self._get_meta("_root_id_high_water", 0) < highest:
                self._put_meta("_root_id_high_water", highest)
            count = self._delete_tasks("root_id=?", (root_id,))
            self._db.execute("DELETE FROM roots WHERE id=?", (root_id,))
            self._put_meta("last_scan", {"complete": False, "reason": "source_removed"})
            return count

    def roots(self) -> list[Root]:
        with self._mutex:
            return [Root(r["id"], r["path"], r["alias"], r["remote_prefix"])
                    for r in self._db.execute("SELECT * FROM roots ORDER BY id")]

    def _walk_files(self, root: Root, extensions: frozenset[str], result: ScanResult,
                    stop: threading.Event | None = None) -> Iterator[tuple]:
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
             progress: Callable[[ScanResult], None] | None = None) -> ScanResult:
        """Stream all registered roots; never hold the complete file list in RAM.

        A successful root scan prunes disappeared/excluded pending work.  A
        partial or interrupted scan never prunes unseen rows.  No claims can be
        issued during a scan, and a scan cannot begin while jobs are running.
        """
        if batch_size < 1 or batch_size > 10000:
            raise ValueError("batch_size must be in 1..10000")
        selected = MEDIA_EXTENSIONS if extensions is None else _normalize_extensions(extensions)
        result = ScanResult()
        started = time.monotonic()
        last_progress = started
        token = uuid.uuid4().hex
        with _file_lock(self.path + ".scan.lock"):
            with self._transaction():
                if self._db.execute("SELECT 1 FROM jobs WHERE status='running' LIMIT 1").fetchone():
                    raise QueueBusyError("Pause/finish uploads or recover interrupted claims before scanning.")
                sources = self.roots()
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
        frozen = (row["status"] == "uploaded" or (row["status"] == "skipped" and row["skip_reason"] == "remote_exists") or self._db.execute(
            "SELECT 1 FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone() is not None)
        remote_path = row["remote_path"] if frozen else values[4]
        remote_key = row["remote_key"] if frozen else values[5]
        self._db.execute(
            "UPDATE jobs SET relative_path=?,local_path=?,local_key=?,remote_path=?,remote_key=?,"
            "seen_scan=?,present=1,updated_at=?,"
            "status=CASE WHEN (status='skipped' AND skip_reason='missing') OR "
            "(status='failed' AND skip_reason='source_changed') THEN 'pending' ELSE status END,"
            "next_attempt=CASE WHEN skip_reason IN ('missing','source_changed') THEN 0 ELSE next_attempt END,"
            "last_error=CASE WHEN skip_reason IN ('missing','source_changed') THEN NULL ELSE last_error END,"
            "skip_reason=CASE WHEN skip_reason IN ('missing','source_changed') THEN NULL ELSE skip_reason END WHERE id=?",
            values[1:4] + (remote_path, remote_key, scan_id, now, row["id"]),
        )

    def _ingest(self, batch: list[tuple], result: ScanResult) -> None:
        now = time.time()
        with self._transaction():
            highest_id = self._job_id_high_water()
            next_id = highest_id + 1
            for values in batch:
                result.seen += 1
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
                                "size,mtime_ns,device,inode,created_at,updated_at,seen_scan,next_attempt) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                                (next_id,) + values + (now, now, result.scan_id),
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
                                "seen_scan=?,present=1,updated_at=? WHERE id=?",
                                values[1:10] + (result.scan_id, now, row["id"]),
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
            row["storage_mode"],
        )

    def claim(self, *, now: float | None = None, max_attempts: int | None = None,
              storage_mode: str | None = None) -> Job | None:
        """Claim one ready file and validate the local snapshot before returning.

        None means no work is *currently* ready, not that all work succeeded.
        Check stats() for deferred/permanent failures and an active scan.
        """
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        partition = _size_partition(storage_mode)
        pending_index = "jobs_pending" if storage_mode is None else f"jobs_pending_{storage_mode}"
        retry_index = " INDEXED BY jobs_retry" if storage_mode is None else f" INDEXED BY jobs_retry_{storage_mode}"
        refreshed: set[int] = set()
        while True:
            current = time.time() if now is None else now
            with self._transaction():
                if self._get_meta("_scan_token"):
                    return None
                # Separate indexed LIMIT queries are intentional.  One OR
                # query plus ORDER BY id makes SQLite sort the entire ready
                # queue on *every* claim (quadratic at 300,000 photographs).
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
                    "UPDATE jobs SET status='running',claim_token=?,attempts=?,updated_at=?,next_attempt=NULL WHERE id=?",
                    (token, attempts, current, row["id"]),
                )
                job = self._job(row, token, attempts)
            try:
                snapshot = _snapshot(job.local_path)
            except OSError:
                self._invalidate_claim(job, None)
                continue
            if snapshot != _job_snapshot(job):
                self._invalidate_claim(job, snapshot, retry=job.id not in refreshed)
                refreshed.add(job.id)
                continue
            return job

    def source_matches(self, job: Job) -> bool:
        try:
            return _snapshot(job.local_path) == _job_snapshot(job)
        except OSError:
            return False

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
                self._db.execute(
                    "UPDATE jobs SET status='skipped',skip_reason='missing',present=0,claim_token=NULL,"
                    "storage_mode=NULL,last_error='Source is missing, inaccessible, or no longer a regular file',updated_at=? WHERE id=?",
                    (time.time(), job.id),
                )
            else:
                root = self._db.execute("SELECT remote_prefix FROM roots WHERE id=?", (job.root_id,)).fetchone()
                remote = root["remote_prefix"].rstrip("/") + "/" + job.relative_path
                if self._db.execute("SELECT 1 FROM jobs WHERE remote_key=? AND id<>?",
                                    (_remote_key(remote), job.id)).fetchone():
                    # Keep the old source snapshot and its uncertain commit
                    # evidence. A corrected mapping/rescan can resolve this
                    # one task; other ready tasks can still be claimed.
                    self._db.execute(
                        "UPDATE jobs SET status='failed',claim_token=NULL,next_attempt=NULL,updated_at=?,"
                        "last_error='Source changed, but the current cloud destination conflicts with another task' "
                        "WHERE id=?", (time.time(), job.id),
                    )
                    self._finalize_control(job.id)
                    return
                self._drop_intent(job.id)
                self._db.execute(
                    "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status=?,attempts=0,next_attempt=?,"
                    "claim_token=NULL,last_error='Source changed; any remote partial result must be reconciled',"
                    "remote_hash=NULL,remote_id=NULL,verified_metadata=NULL,uploaded_at=NULL,storage_mode=NULL,skip_reason=?,updated_at=?,"
                    "remote_path=?,remote_key=? "
                    "WHERE id=?",
                    snapshot[:3] + (str(snapshot[3]), "pending" if retry else "failed", 0 if retry else None,
                                    None if retry else "source_changed", time.time(), remote, _remote_key(remote), job.id),
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
        try:
            snapshot = _snapshot(job.local_path)
        except OSError:
            snapshot = None
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed before remote commit: {job.local_path}")
        with self._transaction():
            self._require_claim(job)
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
            self._require_claim(job)
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
            return {"storage_mode": "unlimited", **intent}

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
        try:
            snapshot = _snapshot(job.local_path)
        except OSError:
            snapshot = None
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed after saving {storage_mode} confirmation: {job.local_path}")

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
        evidence = json.dumps(metadata, ensure_ascii=False, allow_nan=False)
        now = time.time()
        with self._transaction():
            claimed = self._require_claim(job)
            if claimed["storage_mode"] not in (None, storage_mode):
                raise QueueError("Verified storage mode does not match the saved commit intent.")
            self._db.execute(
                "INSERT INTO upload_results(job_id,origin_job_id,account_id,local_path,local_key,remote_path,"
                "source_size,source_mtime_ns,source_device,source_inode,remote_hash,remote_id,verified_metadata,uploaded_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job.id, job.id, self._get_meta("_account_id"), job.local_path, claimed["local_key"], job.remote_path,
                 job.size, job.mtime_ns, job.device, str(job.inode), remote_hash, remote_id, evidence, now),
            )
            self._archive_intent(job.id)
        try:
            snapshot = _snapshot(job.local_path)
        except OSError:
            snapshot = None
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed during upload: {job.local_path}")
        with self._transaction():
            self._require_claim(job)
            self._db.execute(
                "UPDATE jobs SET status='uploaded',claim_token=NULL,last_error=NULL,skip_reason=NULL,"
                "remote_hash=?,remote_id=?,verified_metadata=?,storage_mode=?,uploaded_at=?,updated_at=?,next_attempt=NULL WHERE id=?",
                (remote_hash, remote_id, evidence, storage_mode, now, now, job.id),
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
                "UPDATE jobs SET status='failed',claim_token=NULL,last_error=?,next_attempt=?,updated_at=? WHERE id=?",
                (str(error)[:16384], None if retry_delay is None else current + retry_delay, current, job.id),
            )
            self._finalize_control(job.id)

    def mark_skipped(self, job: Job, reason: str, *, code: str = "manual") -> None:
        if not isinstance(code, str) or not code or "\x00" in code:
            raise ValueError("A nonempty skip code is required")
        with self._transaction():
            self._require_claim(job)
            if code != "large_files_disabled":
                self._drop_intent(job.id)
            self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason=?,claim_token=NULL,last_error=?,"
                "next_attempt=NULL,updated_at=? WHERE id=?", (code, str(reason)[:16384], time.time(), job.id),
            )
            self._finalize_control(job.id)

    def skip_large_disabled(self) -> int:
        """Park oversized ready/retryable files without losing commit evidence.

        Recover interrupted claims first under exclusive_run(). Active claims,
        finished files, missing sources, and permanent failures are untouched.
        """
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason='large_files_disabled',claim_token=NULL,"
                "last_error=?,next_attempt=NULL,updated_at=? "
                f"WHERE present=1 AND paused=0 AND remove_requested=0 AND {_SIZE_PARTITIONS['regular']} AND "
                "(status='pending' OR (status='failed' AND next_attempt IS NOT NULL))",
                (_LARGE_FILES_DISABLED_REASON, time.time()),
            ).rowcount

    def retry_large_skipped(self) -> int:
        """Resume only files parked by the large-file setting or its old limit.

        Keep ambiguous/confirmed ordinary intents for remote reconciliation.
        Other unsupported files and deliberate skips stay skipped.
        """
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,claim_token=NULL,"
                "last_error=NULL,skip_reason=NULL,updated_at=? "
                f"WHERE present=1 AND paused=0 AND remove_requested=0 AND {_SIZE_PARTITIONS['regular']} AND status='skipped' AND "
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
                "next_attempt=0,last_error=?,updated_at=? WHERE id=?",
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
                "UPDATE jobs SET status='pending',attempts=MAX(0,attempts-1),claim_token=NULL,next_attempt=0,updated_at=?,"
                "last_error='Interrupted upload; reconcile remote state before retry' WHERE status='running'",
                (time.time(),),
            )
            return count

    def retry_failed(self, *, include_permanent: bool = False) -> int:
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,last_error=NULL,updated_at=? "
                "WHERE status='failed' AND present=1 AND paused=0 AND remove_requested=0 AND (? OR next_attempt IS NOT NULL)",
                (time.time(), int(include_permanent)),
            ).rowcount

    def reconcile_candidates(self, *, after_id: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        """One bounded keyset page for an optional, read-only cloud comparison.

        No upload claims are created. The reconciliation pass owns the run
        lock, and final adoption rechecks eligibility and the source snapshot.
        """
        if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("after_id must be nonnegative and limit must be in 1..500")
        with self._mutex:
            self._require_idle_sources()
            return [dict(row) for row in self._db.execute(
                "SELECT id,root_id,relative_path,local_path,remote_path,size,mtime_ns,device,inode "
                "FROM jobs INDEXED BY jobs_reconcile WHERE id>? AND present=1 AND paused=0 AND remove_requested=0 "
                f"AND status IN ('pending','failed') AND size>0 AND size<={UNLIMITED_MAX_BYTES} "
                "AND NOT EXISTS (SELECT 1 FROM commit_intents WHERE job_id=jobs.id) ORDER BY id LIMIT ?",
                (after_id, limit),
            )]

    def mark_remote_existing(self, candidate: Mapping[str, Any], *, remote_hash: str) -> bool:
        """Record an exact-path content observation, never an upload/billing proof.

        Return False when a concurrent pause/removal/source change makes this
        candidate stale. The caller has already compared the cloud hash and
        size; the local snapshot and current mapping are rechecked here.
        """
        if (not isinstance(remote_hash, str) or len(remote_hash) != 40
                or any(char not in "0123456789abcdefABCDEF" for char in remote_hash)):
            raise ValueError("A valid Mail Cloud hash is required")
        expected = (candidate["size"], candidate["mtime_ns"], candidate["device"], int(candidate["inode"]))
        if not 0 < expected[0] <= UNLIMITED_MAX_BYTES:
            return False
        with self._transaction():
            account = self._get_meta("_account_id")
            if not account:
                raise QueueConfigurationError("Bind the authenticated account before comparing cloud files.")
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Wait for scanning to finish before comparing cloud files.")
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (candidate["id"],)).fetchone()
            if (row is None or not row["present"] or row["paused"] or row["remove_requested"]
                    or row["status"] not in ("pending", "failed")
                    or any(row[key] != candidate[key] for key in ("root_id", "relative_path", "local_path", "remote_path"))
                    or (row["size"], row["mtime_ns"], row["device"], int(row["inode"])) != expected
                    or self._db.execute("SELECT 1 FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone()):
                return False
            try:
                if _snapshot(row["local_path"]) != expected:
                    return False
            except OSError:
                return False
            observation = {
                "remote_exists": True, "account_id": account, "path": row["remote_path"],
                "hash": remote_hash.upper(), "size": row["size"], "observed_at": time.time(),
                "quota_status": "unknown",
            }
            self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason='remote_exists',claim_token=NULL,"
                "next_attempt=NULL,last_error=NULL,remote_hash=?,remote_id=NULL,verified_metadata=?,"
                "storage_mode=NULL,uploaded_at=NULL,updated_at=? WHERE id=?",
                (remote_hash.upper(), json.dumps(observation, ensure_ascii=False, allow_nan=False),
                 observation["observed_at"], row["id"]),
            )
            return True

    def stats(self, *, now: float | None = None, storage_mode: str | None = None,
              include_missing: bool = False) -> dict[str, Any]:
        current = time.time() if now is None else now
        partition = _size_partition(storage_mode)
        selected = partition if include_missing else partition + " AND present=1"
        with self._mutex:
            counts = {state: 0 for state in STATES}
            sizes = {state: 0 for state in STATES}
            for row in self._db.execute(
                f"SELECT status,COUNT(*) AS n,COALESCE(SUM(size),0) AS bytes FROM jobs WHERE {selected} GROUP BY status"
            ):
                counts[row["status"]] = row["n"]
                sizes[row["status"]] = row["bytes"]
            ready = self._db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {partition} AND present=1 AND paused=0 AND remove_requested=0 AND (status='pending' OR "
                "(status='failed' AND next_attempt IS NOT NULL AND next_attempt<=?))", (current,),
            ).fetchone()[0]
            next_retry = self._db.execute(
                f"SELECT MIN(next_attempt) FROM jobs WHERE {partition} AND status='failed' AND present=1 "
                "AND paused=0 AND remove_requested=0 AND next_attempt>?",
                (current,),
            ).fetchone()[0]
            permanent = self._db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {selected} AND status='failed' AND next_attempt IS NULL"
            ).fetchone()[0]
            existing = self._db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {selected} AND status='skipped' AND skip_reason='remote_exists'"
            ).fetchone()[0]
            controls = self._db.execute(
                f"SELECT COALESCE(SUM(paused),0) AS paused,COALESCE(SUM(remove_requested),0) AS removing,"
                "COALESCE(SUM(CASE WHEN present=1 AND paused=0 AND remove_requested=0 AND (status IN ('pending','running') "
                "OR (status='failed' AND next_attempt IS NOT NULL)) THEN 1 ELSE 0 END),0) AS remaining,"
                "COALESCE(SUM(CASE WHEN status IN ('uploaded','skipped') OR "
                "(status='failed' AND next_attempt IS NULL) THEN 1 ELSE 0 END),0) AS processed,"
                "COALESCE(SUM(CASE WHEN status IN ('uploaded','skipped') OR "
                f"(status='failed' AND next_attempt IS NULL) THEN size ELSE 0 END),0) AS processed_bytes FROM jobs WHERE {selected}"
            ).fetchone()
            return {
                **counts, "total": sum(counts.values()), "total_bytes": sum(sizes.values()),
                "uploaded_bytes": sizes["uploaded"], "bytes_by_status": sizes,
                "ready": ready, "permanent_failures": permanent, "next_retry_at": next_retry,
                "existing": existing,
                "scan_active": bool(self._get_meta("_scan_token")),
                "roots": self._db.execute("SELECT COUNT(*) FROM roots").fetchone()[0],
                **dict(controls),
            }

    def list_jobs(self, status: str | None = None, *, limit: int = 100, offset: int = 0,
                  summary: bool = False, include_missing: bool = False) -> list[dict[str, Any]]:
        if status is not None and status not in (*STATES, "paused"):
            raise ValueError(f"Unknown status: {status}")
        if not 1 <= limit <= 10000 or offset < 0:
            raise ValueError("limit must be in 1..10000 and offset nonnegative")
        # A fixed projection keeps persisted verification evidence out of UI
        # polling. The default retains the complete records used by the CLI.
        columns = _JOB_SUMMARY_COLUMNS if summary else "*"
        conditions = [] if include_missing else ["present=1"]
        parameters = []
        if status is not None:
            if status == "paused":
                conditions.append("paused=1")
            else:
                conditions.append("status=?")
                parameters.append(status)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        parameters.extend((limit, offset))
        with self._mutex:
            return [self._task_dict(row) for row in self._db.execute(
                f"SELECT {columns} FROM jobs{where} ORDER BY id LIMIT ? OFFSET ?",
                parameters,
            )]

    def get_job(self, job_id: int, *, include_missing: bool = False) -> dict[str, Any] | None:
        """Return a task without loading or decoding unrelated queue rows."""
        where = "id=?" if include_missing else "id=? AND present=1"
        with self._mutex:
            row = self._db.execute(f"SELECT * FROM jobs WHERE {where}", (job_id,)).fetchone()
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

    def upload_history(self, job_id: int | None) -> list[dict[str, Any]]:
        with self._mutex:
            result = []
            column = "job_id" if job_id is None else "origin_job_id"
            for row in self._db.execute(f"SELECT * FROM upload_results WHERE {column} IS ? ORDER BY id", (job_id,)):
                item = dict(row)
                item["verified_metadata"] = json.loads(item["verified_metadata"] or "{}")
                result.append(item)
            return result

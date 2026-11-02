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
SCHEMA_VERSION = 1
_SCHEMA_OBJECTS = frozenset({
    ("table", "metadata"), ("table", "roots"), ("table", "scans"),
    ("table", "jobs"), ("table", "upload_results"), ("table", "commit_intents"),
    ("index", "jobs_ready"), ("index", "jobs_pending"),
    ("index", "jobs_pending_unlimited"), ("index", "jobs_pending_regular"),
    ("index", "jobs_retry_unlimited"), ("index", "jobs_retry_regular"),
    ("index", "jobs_root_scan"), ("index", "jobs_status"),
    ("index", "upload_results_job"),
})
_JOB_SUMMARY_COLUMNS = (
    "id,relative_path,local_path,remote_path,status,size,attempts,"
    "last_error,skip_reason,next_attempt,uploaded_at,storage_mode"
)
_STORAGE_MODES = ("unlimited", "regular")
_SIZE_PARTITIONS = {
    "unlimited": f"size<={UNLIMITED_MAX_BYTES}",
    "regular": f"size>{UNLIMITED_MAX_BYTES}",
}
_LEGACY_LARGE_FILE_ERROR = "Файл больше 2 ГБ; он не отправлен обычной загрузкой."
_LARGE_FILES_DISABLED_REASON = "Файл больше 2 ГБ: обычная загрузка с расходованием места выключена."


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
    if not value.startswith("/") or "\x00" in value or "\\" in value:
        raise QueueConfigurationError("Destination must be an absolute cloud path using '/' separators.")
    if any(part in (".", "..") for part in value.split("/")):
        raise QueueConfigurationError("Destination cannot contain '.' or '..' segments.")
    return "/" + "/".join(part for part in value.split("/") if part)


def _alias(value: str) -> str:
    if not value or value in (".", "..") or any(ch in value for ch in ("/", "\\", "\x00")):
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
        if schema_version not in (0, SCHEMA_VERSION):
            raise QueueConfigurationError(f"Unsupported queue schema {schema_version}; expected {SCHEMA_VERSION}.")
        objects = {(row["type"], row["name"]) for row in self._db.execute(
            "SELECT type,name FROM sqlite_master WHERE type IN ('table','index')"
        )}
        job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
        has_schema = _SCHEMA_OBJECTS.issubset(objects) and "storage_mode" in job_columns
        if ("table", "metadata") in objects:
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            needs_account = account_id is not None and self._account_needs_binding(account_id)
            # Browser reads and worker connections must not take the writer lock
            # or checkpoint new metadata on every open. Older schema-1 queues
            # still get missing tables/indexes installed once below.
            if has_schema and schema_version == SCHEMA_VERSION and existing is not None and not needs_account:
                return
        if not has_schema:
            self._db.executescript(f"""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS roots (
                id INTEGER PRIMARY KEY, path TEXT NOT NULL, path_key TEXT NOT NULL UNIQUE,
                alias TEXT NOT NULL, alias_key TEXT NOT NULL UNIQUE, remote_prefix TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY, started_at REAL NOT NULL, finished_at REAL,
                complete INTEGER NOT NULL DEFAULT 0, result_json TEXT
            );
            CREATE TABLE IF NOT EXISTS jobs (
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
                uploaded_at REAL, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                seen_scan INTEGER, present INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, present, next_attempt, id);
            CREATE INDEX IF NOT EXISTS jobs_pending ON jobs(id) WHERE status='pending' AND present=1;
            CREATE INDEX IF NOT EXISTS jobs_pending_unlimited ON jobs(id)
                WHERE status='pending' AND present=1 AND size<={UNLIMITED_MAX_BYTES};
            CREATE INDEX IF NOT EXISTS jobs_pending_regular ON jobs(id)
                WHERE status='pending' AND present=1 AND size>{UNLIMITED_MAX_BYTES};
            CREATE INDEX IF NOT EXISTS jobs_retry_unlimited ON jobs(next_attempt,id)
                WHERE status='failed' AND present=1 AND next_attempt IS NOT NULL AND size<={UNLIMITED_MAX_BYTES};
            CREATE INDEX IF NOT EXISTS jobs_retry_regular ON jobs(next_attempt,id)
                WHERE status='failed' AND present=1 AND next_attempt IS NOT NULL AND size>{UNLIMITED_MAX_BYTES};
            CREATE INDEX IF NOT EXISTS jobs_root_scan ON jobs(root_id, seen_scan);
            CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status, id);
            CREATE TABLE IF NOT EXISTS upload_results (
                id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                remote_hash TEXT, remote_id TEXT, verified_metadata TEXT,
                uploaded_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS upload_results_job ON upload_results(job_id);
            CREATE TABLE IF NOT EXISTS commit_intents (
                job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
                source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
                device INTEGER NOT NULL, inode TEXT NOT NULL,
                remote_path TEXT NOT NULL, cloud_hash TEXT NOT NULL,
                created_at REAL NOT NULL, intent_json TEXT NOT NULL
            );
            """)
        with self._transaction():
            # Additive schema-1 migration; recheck after taking the writer lock
            # so simultaneous first opens cannot add the column twice.
            job_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(jobs)")}
            if "storage_mode" not in job_columns:
                self._db.execute("ALTER TABLE jobs ADD COLUMN storage_mode TEXT "
                                 "CHECK(storage_mode IN ('unlimited','regular'))")
            existing = self._get_meta("_destination")
            self._check_destination(existing)
            if existing is None:
                self._put_meta("_destination", self.destination)
            if schema_version != SCHEMA_VERSION:
                self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            if account_id is not None:
                self._bind_account(account_id)

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

    def add_root(self, path: str | os.PathLike[str], alias: str) -> Root:
        root_path = _path_key(Path(path).expanduser())
        if not os.path.isdir(root_path):
            raise QueueConfigurationError(f"Source root is not an accessible directory: {root_path}")
        alias = _alias(alias)
        remote_prefix = self.destination.rstrip("/") + "/" + alias
        with self._transaction():
            if self._get_meta("_scan_token"):
                raise QueueBusyError("Cannot change source roots during a scan.")
            for row in self._db.execute("SELECT * FROM roots"):
                if row["path_key"] == root_path:
                    if row["alias"] != alias:
                        raise QueueConfigurationError("An existing source root cannot be assigned a different alias.")
                    return Root(row["id"], row["path"], row["alias"], row["remote_prefix"])
                if row["alias_key"] == _remote_key(alias):
                    raise ScanCollisionError(f"Root alias {alias!r} is already used by another source directory.")
                try:
                    common = os.path.commonpath((row["path_key"], root_path))
                except ValueError:  # Separate drives on Windows.
                    common = ""
                if common in (row["path_key"], root_path):
                    raise QueueConfigurationError("Source roots cannot overlap; choose the parent root or separate children.")
            cursor = self._db.execute(
                "INSERT INTO roots(path,path_key,alias,alias_key,remote_prefix) VALUES(?,?,?,?,?)",
                (root_path, root_path, alias, _remote_key(alias), remote_prefix),
            )
            return Root(cursor.lastrowid, root_path, alias, remote_prefix)

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
                            remote = root.remote_prefix + "/" + relative
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
                                "skip_reason=CASE WHEN status='uploaded' THEN skip_reason ELSE 'missing' END, "
                                "last_error=CASE WHEN status='uploaded' THEN last_error "
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

    def _ingest(self, batch: list[tuple], result: ScanResult) -> None:
        now = time.time()
        with self._transaction():
            for values in batch:
                result.seen += 1
                try:
                    row = self._db.execute("SELECT * FROM jobs WHERE local_key=?", (values[3],)).fetchone()
                    if row is None:
                        self._db.execute(
                            "INSERT INTO jobs(root_id,relative_path,local_path,local_key,remote_path,remote_key,"
                            "size,mtime_ns,device,inode,created_at,updated_at,seen_scan,next_attempt) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                            values + (now, now, result.scan_id),
                        )
                        result.added += 1
                        continue
                    if row["root_id"] != values[0] or row["remote_path"] != values[4]:
                        raise ScanCollisionError(f"Source mapping changed for {values[2]}")
                    old_fingerprint = (row["size"], row["mtime_ns"], row["device"], str(row["inode"]))
                    if old_fingerprint != values[6:10]:
                        self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (row["id"],))
                        self._db.execute(
                            "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status='pending',attempts=0,"
                            "next_attempt=0,claim_token=NULL,last_error='Source changed since previous scan',"
                            "skip_reason=NULL,remote_hash=NULL,remote_id=NULL,verified_metadata=NULL,uploaded_at=NULL,storage_mode=NULL,"
                            "seen_scan=?,present=1,updated_at=? WHERE id=?",
                            values[6:10] + (result.scan_id, now, row["id"]),
                        )
                        result.changed += 1
                    else:
                        self._db.execute(
                            "UPDATE jobs SET seen_scan=?,present=1,updated_at=?,"
                            "status=CASE WHEN status='skipped' AND skip_reason='missing' THEN 'pending' ELSE status END,"
                            "next_attempt=CASE WHEN status='skipped' AND skip_reason='missing' THEN 0 ELSE next_attempt END,"
                            "last_error=CASE WHEN status='skipped' AND skip_reason='missing' THEN NULL ELSE last_error END,"
                            "skip_reason=CASE WHEN skip_reason='missing' THEN NULL ELSE skip_reason END WHERE id=?",
                            (result.scan_id, now, row["id"]),
                        )
                        result.unchanged += 1
                except (sqlite3.IntegrityError, ScanCollisionError, UnicodeError) as exc:
                    result.error(f"Cannot map {values[2]} to {values[4]} (name collision or invalid name): {exc}")

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
        retry_index = "" if storage_mode is None else f" INDEXED BY jobs_retry_{storage_mode}"
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
                    "AND next_attempt IS NOT NULL AND next_attempt<=? "
                    f"AND {partition} AND (? IS NULL OR attempts<?) ORDER BY next_attempt,id LIMIT 1",
                    (current, max_attempts, max_attempts),
                ).fetchone()
                if row is None:
                    row = self._db.execute(
                        f"SELECT * FROM jobs INDEXED BY {pending_index} WHERE status='pending' AND present=1 "
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
                self._invalidate_claim(job, snapshot)
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

    def _invalidate_claim(self, job: Job, snapshot: tuple[int, int, int, int] | None) -> None:
        with self._transaction():
            self._require_claim(job)
            self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (job.id,))
            if snapshot is None:
                self._db.execute(
                    "UPDATE jobs SET status='skipped',skip_reason='missing',present=0,claim_token=NULL,"
                    "storage_mode=NULL,last_error='Source is missing, inaccessible, or no longer a regular file',updated_at=? WHERE id=?",
                    (time.time(), job.id),
                )
            else:
                self._db.execute(
                    "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status='pending',attempts=0,next_attempt=0,"
                    "claim_token=NULL,last_error='Source changed; any remote partial result must be reconciled',"
                    "remote_hash=NULL,remote_id=NULL,verified_metadata=NULL,uploaded_at=NULL,storage_mode=NULL,skip_reason=NULL,updated_at=? "
                    "WHERE id=?",
                    snapshot[:3] + (str(snapshot[3]), time.time(), job.id),
                )

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
            intent = {
                "cloud_hash": cloud_hash, "remote_path": job.remote_path,
                "source_size": job.size, "source_mtime_ns": job.mtime_ns,
                "device": job.device, "inode": job.inode,
                "account_id": account, "destination": self.destination, "created_at": time.time(),
                "storage_mode": storage_mode,
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
                self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (job.id,))
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
        try:
            snapshot = _snapshot(job.local_path)
        except OSError:
            snapshot = None
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed before saving {storage_mode} confirmation: {job.local_path}")
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

    def mark_uploaded(
        self,
        job: Job,
        *,
        remote_hash: str | None = None,
        remote_id: str | None = None,
        verified_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Record a transport-confirmed commit, retaining its evidence separately.

        Never call merely because bytes reached an upload endpoint.  The
        transport must verify the committed remote object and its storage mode.
        """
        try:
            snapshot = _snapshot(job.local_path)
        except OSError:
            snapshot = None
        if snapshot != _job_snapshot(job):
            self._invalidate_claim(job, snapshot)
            raise FileChangedError(f"Source changed during upload: {job.local_path}")
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
                "INSERT INTO upload_results(job_id,source_size,source_mtime_ns,remote_hash,remote_id,verified_metadata,uploaded_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (job.id, job.size, job.mtime_ns, remote_hash, remote_id, evidence, now),
            )
            self._db.execute(
                "UPDATE jobs SET status='uploaded',claim_token=NULL,last_error=NULL,skip_reason=NULL,"
                "remote_hash=?,remote_id=?,verified_metadata=?,storage_mode=?,uploaded_at=?,updated_at=?,next_attempt=NULL WHERE id=?",
                (remote_hash, remote_id, evidence, storage_mode, now, now, job.id),
            )
            self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (job.id,))

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

    def mark_skipped(self, job: Job, reason: str, *, code: str = "manual") -> None:
        if not isinstance(code, str) or not code or "\x00" in code:
            raise ValueError("A nonempty skip code is required")
        with self._transaction():
            self._require_claim(job)
            if code != "large_files_disabled":
                self._db.execute("DELETE FROM commit_intents WHERE job_id=?", (job.id,))
            self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason=?,claim_token=NULL,last_error=?,"
                "next_attempt=NULL,updated_at=? WHERE id=?", (code, str(reason)[:16384], time.time(), job.id),
            )

    def skip_large_disabled(self) -> int:
        """Park oversized ready/retryable files without losing commit evidence.

        Recover interrupted claims first under exclusive_run(). Active claims,
        finished files, missing sources, and permanent failures are untouched.
        """
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='skipped',skip_reason='large_files_disabled',claim_token=NULL,"
                "last_error=?,next_attempt=NULL,updated_at=? "
                f"WHERE present=1 AND {_SIZE_PARTITIONS['regular']} AND "
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
                f"WHERE present=1 AND {_SIZE_PARTITIONS['regular']} AND status='skipped' AND "
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

    def recover_running(self) -> int:
        """Recover interrupted claims without spending unfinished attempts.

        A retried claim may already exist remotely if the process died between
        remote commit and local acknowledgement; the transport must reconcile.
        """
        owns_lock = self._runner_lock_depth and self._runner_lock_owner == threading.get_ident()
        lock = contextlib.nullcontext() if owns_lock else self.exclusive_run()
        with lock, _file_lock(self.path + ".scan.lock"), self._transaction():
            self._db.execute("DELETE FROM metadata WHERE key='_scan_token'")
            cursor = self._db.execute(
                "UPDATE jobs SET status='pending',attempts=MAX(0,attempts-1),claim_token=NULL,next_attempt=0,updated_at=?,"
                "last_error='Interrupted upload; reconcile remote state before retry' WHERE status='running'",
                (time.time(),),
            )
            return cursor.rowcount

    def retry_failed(self, *, include_permanent: bool = False) -> int:
        with self._transaction():
            return self._db.execute(
                "UPDATE jobs SET status='pending',attempts=0,next_attempt=0,last_error=NULL,updated_at=? "
                "WHERE status='failed' AND present=1 AND (? OR next_attempt IS NOT NULL)",
                (time.time(), int(include_permanent)),
            ).rowcount

    def stats(self, *, now: float | None = None, storage_mode: str | None = None) -> dict[str, Any]:
        current = time.time() if now is None else now
        partition = _size_partition(storage_mode)
        with self._mutex:
            counts = {state: 0 for state in STATES}
            sizes = {state: 0 for state in STATES}
            for row in self._db.execute(
                f"SELECT status,COUNT(*) AS n,COALESCE(SUM(size),0) AS bytes FROM jobs WHERE {partition} GROUP BY status"
            ):
                counts[row["status"]] = row["n"]
                sizes[row["status"]] = row["bytes"]
            ready = self._db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {partition} AND present=1 AND (status='pending' OR "
                "(status='failed' AND next_attempt IS NOT NULL AND next_attempt<=?))", (current,),
            ).fetchone()[0]
            next_retry = self._db.execute(
                f"SELECT MIN(next_attempt) FROM jobs WHERE {partition} AND status='failed' AND present=1 AND next_attempt>?",
                (current,),
            ).fetchone()[0]
            permanent = self._db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {partition} AND status='failed' AND next_attempt IS NULL"
            ).fetchone()[0]
            return {
                **counts, "total": sum(counts.values()), "total_bytes": sum(sizes.values()),
                "uploaded_bytes": sizes["uploaded"], "bytes_by_status": sizes,
                "ready": ready, "permanent_failures": permanent, "next_retry_at": next_retry,
                "scan_active": bool(self._get_meta("_scan_token")),
                "roots": self._db.execute("SELECT COUNT(*) FROM roots").fetchone()[0],
            }

    def list_jobs(self, status: str | None = None, *, limit: int = 100, offset: int = 0,
                  summary: bool = False) -> list[dict[str, Any]]:
        if status is not None and status not in STATES:
            raise ValueError(f"Unknown status: {status}")
        if not 1 <= limit <= 10000 or offset < 0:
            raise ValueError("limit must be in 1..10000 and offset nonnegative")
        # A fixed projection keeps persisted verification evidence out of UI
        # polling. The default retains the complete records used by the CLI.
        columns = _JOB_SUMMARY_COLUMNS if summary else "*"
        where = " WHERE status=?" if status is not None else ""
        parameters = (status, limit, offset) if status is not None else (limit, offset)
        with self._mutex:
            return [dict(row) for row in self._db.execute(
                f"SELECT {columns} FROM jobs{where} ORDER BY id LIMIT ? OFFSET ?",
                parameters,
            )]

    def upload_history(self, job_id: int) -> list[dict[str, Any]]:
        with self._mutex:
            result = []
            for row in self._db.execute("SELECT * FROM upload_results WHERE job_id=? ORDER BY id", (job_id,)):
                item = dict(row)
                item["verified_metadata"] = json.loads(item["verified_metadata"] or "{}")
                result.append(item)
            return result

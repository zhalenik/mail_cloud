"""A directory-only, stat-only planning inventory with bounded traversal memory.

The database stores directories and their aggregates, never a row per file.
Cloud queries and content hashing belong to the later upload scan. Exclusions
change selected aggregates while retaining the complete observed tree.
"""
from __future__ import annotations

import contextlib
import copy
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
import uuid

from .media import MEDIA_EXTENSIONS
from .transport import Cancelled


_MAX_INTEGER = 2**63 - 1
_MAX_EXCLUSIONS = 50000


def _identifier(value):
    if not isinstance(value, str) or len(value) != 32:
        raise ValueError("Обзор каталога не найден.")
    try:
        int(value, 16)
    except ValueError:
        raise ValueError("Некорректный идентификатор обзора каталога.") from None
    return value


def _page(limit, offset, maximum=200):
    if type(limit) is not int or not 1 <= limit <= maximum or type(offset) is not int or offset < 0:
        raise ValueError(f"Размер страницы: 1–{maximum}; смещение не может быть отрицательным.")


def _private_file(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("База плана должна быть обычным файлом.")
        if os.name != "nt":
            os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return path


class PlanInventory:
    """Thread-safe facade; long scans use their own connection, not the UI lock."""

    def __init__(self, path, *, clock=time.time):
        self.path = _private_file(path)
        self._clock = clock
        self._lock = threading.RLock()
        self._active = set()
        self._summaries = {}
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS inventories(
                    id TEXT PRIMARY KEY,root TEXT NOT NULL,status TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0,extensions TEXT NOT NULL,
                    created_at REAL NOT NULL,updated_at REAL NOT NULL,root_id INTEGER,
                    seen_count INTEGER NOT NULL DEFAULT 0,seen_bytes INTEGER NOT NULL DEFAULT 0,
                    directory_count INTEGER NOT NULL DEFAULT 0,ignored_files INTEGER NOT NULL DEFAULT 0,
                    ignored_symlinks INTEGER NOT NULL DEFAULT 0,message TEXT NOT NULL DEFAULT '');
                CREATE INDEX IF NOT EXISTS inventories_recent ON inventories(created_at DESC,id);
                CREATE TABLE IF NOT EXISTS directories(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,inventory_id TEXT NOT NULL,parent_id INTEGER,
                    name TEXT NOT NULL,path TEXT NOT NULL,relative_path TEXT NOT NULL,depth INTEGER NOT NULL,
                    device INTEGER NOT NULL,inode TEXT NOT NULL,scan_done INTEGER NOT NULL DEFAULT 0,
                    child_count INTEGER NOT NULL DEFAULT 0,direct_count INTEGER NOT NULL DEFAULT 0,
                    direct_bytes INTEGER NOT NULL DEFAULT 0,direct_errors INTEGER NOT NULL DEFAULT 0,
                    total_count INTEGER NOT NULL DEFAULT 0,total_bytes INTEGER NOT NULL DEFAULT 0,
                    total_errors INTEGER NOT NULL DEFAULT 0,included_count INTEGER NOT NULL DEFAULT 0,
                    included_bytes INTEGER NOT NULL DEFAULT 0,included_errors INTEGER NOT NULL DEFAULT 0,
                    excluded INTEGER NOT NULL DEFAULT 0 CHECK(excluded IN (0,1)),errors TEXT NOT NULL DEFAULT '[]',
                    UNIQUE(inventory_id,relative_path));
                CREATE INDEX IF NOT EXISTS directories_frontier ON directories(inventory_id,scan_done,id);
                CREATE INDEX IF NOT EXISTS directories_children ON directories(inventory_id,parent_id,name,id);
                CREATE INDEX IF NOT EXISTS directories_bottom_up ON directories(inventory_id,depth DESC,id DESC);
                CREATE INDEX IF NOT EXISTS directories_excluded ON directories(inventory_id,excluded,relative_path);
            """)
            # Directory descriptors cannot survive a process restart. A later
            # scan starts from scratch instead of declaring the partial tree valid.
            db.execute("UPDATE inventories SET status='incomplete',message=? WHERE status='scanning'",
                       ("Предварительный обзор прерван остановкой приложения. Выполните его заново.",))
            for row in db.execute("SELECT id FROM inventories"):
                self._summaries[row["id"]] = self._summary(db, row["id"])

    @contextlib.contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA cache_size=-4096")
        db.execute("PRAGMA temp_store=FILE")
        try:
            with db:
                yield db
        finally:
            db.close()

    def _remember(self, db, identifier):
        value = self._summary(db, identifier)
        with self._lock:
            self._summaries[identifier] = value
        return copy.deepcopy(value)

    @staticmethod
    def _summary(db, identifier):
        row = db.execute("SELECT * FROM inventories WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise ValueError("Обзор каталога не найден.")
        value = dict(row)
        value["extensions"] = json.loads(value["extensions"])
        root = db.execute("SELECT * FROM directories WHERE id=?", (row["root_id"],)).fetchone()
        selected = root is not None and not root["excluded"]
        complete = value["status"] == "ready"
        value.update(total_count=root["total_count"] if root else 0,
                     total_bytes=root["total_bytes"] if root else 0,
                     selected_count=root["included_count"] if selected else 0,
                     selected_bytes=root["included_bytes"] if selected else 0,
                     error_count=root["total_errors"] if root else 0,
                     selected_errors=root["included_errors"] if selected else 0,
                     complete=complete)
        return value

    def summary(self, identifier):
        identifier = _identifier(identifier)
        with self._lock:
            value = self._summaries.get(identifier)
            if value is None:
                raise ValueError("Обзор каталога не найден.")
            return copy.deepcopy(value)

    def list(self, *, limit=20, offset=0):
        _page(limit, offset, 100)
        with self._lock:
            rows = sorted(self._summaries.values(), key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return {"inventories": copy.deepcopy(rows[offset:offset + limit]), "total": len(rows),
                    "offset": offset, "limit": limit}

    def create(self, root, extensions=None):
        if not isinstance(root, (str, os.PathLike)) or not os.fspath(root) or "\x00" in os.fspath(root):
            raise ValueError("Укажите корневую папку на сервере.")
        original = Path(root).expanduser()
        if original.is_symlink():
            raise ValueError("Корневая папка не должна быть символической ссылкой.")
        directory = original.resolve(strict=True)
        value = directory.stat(follow_symlinks=False)
        if not stat.S_ISDIR(value.st_mode):
            raise ValueError("Для плана нужен каталог, а не отдельный файл.")
        selected = MEDIA_EXTENSIONS if extensions is None else extensions
        if isinstance(selected, (str, bytes)):
            raise ValueError("Расширения должны быть списком.")
        selected = list(selected)
        if not selected or any(not isinstance(suffix, str) or not suffix.startswith('.')
                               or '/' in suffix or '\\' in suffix or '\x00' in suffix for suffix in selected):
            raise ValueError("Некорректный список расширений.")
        selected = sorted(set(suffix.casefold() for suffix in selected))
        identifier, now = uuid.uuid4().hex, self._clock()
        with self._connect() as db:
            db.execute("INSERT INTO inventories(id,root,status,extensions,created_at,updated_at) VALUES(?,?,'new',?,?,?)",
                       (identifier, str(directory), json.dumps([suffix.casefold() for suffix in selected]), now, now))
            self._insert_directory(db, identifier, None, directory.name or str(directory), str(directory), '', 0, value)
            root_id = db.execute("SELECT id FROM directories WHERE inventory_id=?", (identifier,)).fetchone()[0]
            db.execute("UPDATE inventories SET root_id=? WHERE id=?", (root_id, identifier))
            db.commit()
            return self._remember(db, identifier)

    @staticmethod
    def _insert_directory(db, inventory_id, parent, name, path, relative, depth, identity):
        name.encode('utf-8')
        return db.execute("INSERT INTO directories(inventory_id,parent_id,name,path,relative_path,depth,device,inode) "
                          "VALUES(?,?,?,?,?,?,?,?)", (inventory_id, parent, name, path, relative, depth,
                                                     identity.st_dev, str(identity.st_ino))).lastrowid

    @staticmethod
    def _check(stop):
        if stop.is_set():
            raise Cancelled("Предварительный обзор остановлен.")

    @contextlib.contextmanager
    def _entries(self, root_fd, directory):
        if root_fd is not None:
            fd = os.dup(root_fd)
            try:
                for component in directory["relative_path"].split('/') if directory["relative_path"] else ():
                    next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | getattr(os, 'O_NOFOLLOW', 0), dir_fd=fd)
                    os.close(fd)
                    fd = next_fd
                identity = os.fstat(fd)
                if (identity.st_dev, str(identity.st_ino)) != (directory["device"], directory["inode"]):
                    raise OSError("Каталог изменился во время обзора.")
                with os.scandir(fd) as entries:
                    yield entries
            finally:
                os.close(fd)
        else:
            identity = os.stat(directory['path'], follow_symlinks=False)
            if not stat.S_ISDIR(identity.st_mode) or (identity.st_dev, str(identity.st_ino)) != (directory["device"], directory["inode"]):
                raise OSError("Каталог изменился во время обзора.")
            with os.scandir(directory['path']) as entries:
                yield entries

    @staticmethod
    def _error(db, folder_id, message):
        row = db.execute("SELECT errors FROM directories WHERE id=?", (folder_id,)).fetchone()
        values = json.loads(row[0])
        if len(values) < 5:
            values.append(str(message)[:300])
        db.execute("UPDATE directories SET direct_errors=direct_errors+1,errors=? WHERE id=?",
                   (json.dumps(values, ensure_ascii=False), folder_id))

    @staticmethod
    def _aggregate(db, inventory_id, stop=None):
        db.execute("UPDATE directories SET total_count=direct_count,total_bytes=direct_bytes,total_errors=direct_errors "
                   "WHERE inventory_id=?", (inventory_id,))
        last_depth, last_id = _MAX_INTEGER, _MAX_INTEGER
        while True:
            if stop is not None and stop.is_set():
                raise Cancelled("Предварительный обзор остановлен.")
            rows = db.execute("SELECT id,parent_id,depth,total_count,total_bytes,total_errors FROM directories "
                              "WHERE inventory_id=? AND (depth<? OR (depth=? AND id<?)) "
                              "ORDER BY depth DESC,id DESC LIMIT 500", (inventory_id, last_depth, last_depth, last_id)).fetchall()
            if not rows:
                break
            for row in rows:
                if row['parent_id'] is not None:
                    # A fetched page can contain both a child and its parent.
                    # Read the child's *current* totals after deeper rows in the
                    # same page have contributed, rather than the fetch snapshot.
                    totals = db.execute("SELECT total_count,total_bytes,total_errors FROM directories WHERE id=?", (row['id'],)).fetchone()
                    db.execute("UPDATE directories SET total_count=total_count+?,total_bytes=total_bytes+?,"
                               "total_errors=total_errors+? WHERE id=?",
                               (*tuple(totals), row['parent_id']))
            last_depth, last_id = rows[-1]['depth'], rows[-1]['id']
            db.commit()
        db.execute("UPDATE directories SET included_count=total_count,included_bytes=total_bytes,"
                   "included_errors=total_errors WHERE inventory_id=?", (inventory_id,))

    def scan(self, identifier, *, stop=None, progress=None):
        identifier = _identifier(identifier)
        stop = stop or threading.Event()
        with self._lock:
            if identifier in self._active:
                raise ValueError("Этот каталог уже просматривается.")
            self.summary(identifier)
            self._active.add(identifier)
        root_fd = None
        last_report = 0.0
        try:
            with self._connect() as db:
                saved = db.execute("SELECT * FROM inventories WHERE id=?", (identifier,)).fetchone()
                root = Path(saved['root'])
                identity = root.stat(follow_symlinks=False)
                if not stat.S_ISDIR(identity.st_mode):
                    raise ValueError("Корневая папка больше недоступна как обычный каталог.")
                db.execute("DELETE FROM directories WHERE inventory_id=?", (identifier,))
                root_id = self._insert_directory(db, identifier, None, root.name or str(root), str(root), '', 0, identity)
                db.execute("UPDATE inventories SET status='scanning',revision=revision+1,root_id=?,seen_count=0,seen_bytes=0,"
                           "directory_count=1,ignored_files=0,ignored_symlinks=0,message='',updated_at=? WHERE id=?",
                           (root_id, self._clock(), identifier))
                db.commit()
                self._remember(db, identifier)
                if os.name == 'posix' and os.scandir in os.supports_fd:
                    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, 'O_NOFOLLOW', 0))
                    opened = os.fstat(root_fd)
                    if (opened.st_dev, opened.st_ino) != (identity.st_dev, identity.st_ino):
                        raise OSError("Корневая папка изменилась во время обзора.")
                extensions = set(json.loads(saved['extensions']))
                seen_count = seen_bytes = ignored_files = ignored_symlinks = 0
                directory_count = 1
                mutations = 0
                last_commit = time.monotonic()

                def checkpoint(force=False):
                    nonlocal last_report, mutations, last_commit
                    if not force and mutations < 500 and time.monotonic() - last_commit < 0.25:
                        return
                    db.execute("UPDATE inventories SET seen_count=?,seen_bytes=?,directory_count=?,ignored_files=?,"
                               "ignored_symlinks=?,updated_at=? WHERE id=?",
                               (seen_count, seen_bytes, directory_count, ignored_files, ignored_symlinks, self._clock(), identifier))
                    db.commit()
                    mutations = 0
                    last_commit = time.monotonic()
                    value = self._remember(db, identifier)
                    if progress is not None and (force or time.monotonic() - last_report >= 0.25):
                        last_report = time.monotonic()
                        progress(value)

                try:
                    while directory := db.execute("SELECT * FROM directories WHERE inventory_id=? AND scan_done=0 ORDER BY id LIMIT 1", (identifier,)).fetchone():
                        self._check(stop)
                        direct_count = direct_bytes = entries_count = 0
                        try:
                            with self._entries(root_fd, directory) as entries:
                                for entry in entries:
                                    self._check(stop)
                                    entries_count += 1
                                    mutations += 1
                                    try:
                                        entry.name.encode('utf-8')
                                        if entry.is_symlink():
                                            ignored_symlinks += 1
                                        elif entry.is_dir(follow_symlinks=False):
                                            child_stat = entry.stat(follow_symlinks=False)
                                            relative = (directory['relative_path'] + '/' if directory['relative_path'] else '') + entry.name
                                            self._insert_directory(db, identifier, directory['id'], entry.name,
                                                str(root.joinpath(*relative.split('/'))), relative, directory['depth'] + 1, child_stat)
                                            db.execute("UPDATE directories SET child_count=child_count+1 WHERE id=?", (directory['id'],))
                                            directory_count += 1
                                        elif entry.is_file(follow_symlinks=False):
                                            if Path(entry.name).suffix.casefold() not in extensions:
                                                ignored_files += 1
                                            else:
                                                size = entry.stat(follow_symlinks=False).st_size
                                                if type(size) is not int or size < 0 or size > _MAX_INTEGER - seen_bytes:
                                                    raise ValueError("Суммарный размер файлов превышает поддерживаемый диапазон.")
                                                direct_count += 1
                                                direct_bytes += size
                                                seen_count += 1
                                                seen_bytes += size
                                    except (OSError, UnicodeError) as error:
                                        self._error(db, directory['id'], f"Не удалось проверить элемент {entry.name!r}: {type(error).__name__}.")
                                    if mutations >= 500:
                                        db.execute("UPDATE directories SET direct_count=?,direct_bytes=? WHERE id=?", (direct_count, direct_bytes, directory['id']))
                                        checkpoint()
                        except OSError as error:
                            self._error(db, directory['id'], "Не удалось прочитать каталог: " + (getattr(error, 'strerror', None) or str(error)))
                        finally:
                            mutations += 1
                            db.execute("UPDATE directories SET direct_count=?,direct_bytes=?,scan_done=1 WHERE id=?", (direct_count, direct_bytes, directory['id']))
                            checkpoint()
                    self._check(stop)
                    checkpoint(force=True)
                    self._aggregate(db, identifier, stop)
                    self._check(stop)
                    db.execute("UPDATE inventories SET status='ready',message='',updated_at=? WHERE id=?", (self._clock(), identifier))
                except Cancelled:
                    db.execute("UPDATE inventories SET status='cancelled',message=?,updated_at=? WHERE id=?",
                               ("Предварительный обзор остановлен. Перед созданием плана выполните его заново.", self._clock(), identifier))
                except BaseException:
                    db.execute("UPDATE inventories SET status='incomplete',message=?,updated_at=? WHERE id=?",
                               ("Обзор не завершён. Проверьте доступ к каталогу и повторите его.", self._clock(), identifier))
                    db.commit()
                    self._remember(db, identifier)
                    raise
                checkpoint(force=True)
                return self.summary(identifier)
        except BaseException:
            # Failures opening the root happen before the normal scan loop too.
            with self._connect() as db:
                db.execute("UPDATE inventories SET status='incomplete',message=?,updated_at=? WHERE id=? AND status!='cancelled'",
                           ("Обзор не завершён. Корневая папка недоступна или изменилась.", self._clock(), identifier))
                db.commit()
                self._remember(db, identifier)
            raise
        finally:
            if root_fd is not None:
                os.close(root_fd)
            with self._lock:
                self._active.discard(identifier)

    @staticmethod
    def _inherited_exclusion(db, row):
        parent_id = row['parent_id']
        while parent_id is not None:
            parent = db.execute("SELECT parent_id,excluded FROM directories WHERE id=?", (parent_id,)).fetchone()
            if parent is None:
                raise ValueError("Структура каталога изменилась. Повторите обзор.")
            if parent['excluded']:
                return True
            parent_id = parent['parent_id']
        return False

    @staticmethod
    def _folder(row, inherited=False):
        value = {name: row[name] for name in ('id', 'parent_id', 'name', 'path', 'relative_path', 'child_count', 'total_count', 'total_bytes', 'direct_count', 'direct_bytes')}
        value.update(excluded=bool(row['excluded']), inherited_excluded=bool(inherited),
                     selected_count=0 if inherited or row['excluded'] else row['included_count'],
                     selected_bytes=0 if inherited or row['excluded'] else row['included_bytes'],
                     error_count=row['total_errors'], selected_errors=0 if inherited or row['excluded'] else row['included_errors'],
                     errors=json.loads(row['errors']))
        return value

    def tree(self, identifier, parent_id=None, *, limit=200, offset=0):
        identifier = _identifier(identifier)
        _page(limit, offset)
        if parent_id is not None and (type(parent_id) is not int or parent_id <= 0):
            raise ValueError("Некорректная папка дерева.")
        inventory = self.summary(identifier)
        with self._connect() as db:
            if parent_id is None:
                row = db.execute("SELECT * FROM directories WHERE inventory_id=? AND parent_id IS NULL", (identifier,)).fetchone()
                return {"inventory": inventory, "parent": None,
                        "folders": [self._folder(row)] if row is not None and offset == 0 else [],
                        "total": int(row is not None), "offset": offset, "limit": limit}
            parent = db.execute("SELECT * FROM directories WHERE inventory_id=? AND id=?", (identifier, parent_id)).fetchone()
            if parent is None:
                raise ValueError("Папка обзора не найдена.")
            inherited = self._inherited_exclusion(db, parent)
            rows = db.execute("SELECT * FROM directories WHERE inventory_id=? AND parent_id=? ORDER BY name,id LIMIT ? OFFSET ?",
                              (identifier, parent_id, limit, offset))
            return {"inventory": inventory, "parent": self._folder(parent, inherited),
                    "folders": [self._folder(row, inherited or bool(parent['excluded'])) for row in rows],
                    "total": parent['child_count'], "offset": offset, "limit": limit}

    def exclude(self, identifier, folder_id, excluded):
        identifier = _identifier(identifier)
        if type(folder_id) is not int or folder_id <= 0 or type(excluded) is not bool:
            raise ValueError("Укажите папку и состояние её включения.")
        with self._lock:
            if identifier in self._active or self.summary(identifier)['status'] != 'ready':
                raise ValueError("Сначала завершите предварительный обзор каталога.")
            with self._connect() as db:
                row = db.execute("SELECT * FROM directories WHERE inventory_id=? AND id=?", (identifier, folder_id)).fetchone()
                if row is None:
                    raise ValueError("Папка обзора не найдена.")
                if not excluded and self._inherited_exclusion(db, row):
                    raise ValueError("Сначала включите родительскую папку.")
                if bool(row['excluded']) == excluded:
                    return self.summary(identifier)
                sign = -1 if excluded else 1
                delta = tuple(sign * row[name] for name in ('included_count', 'included_bytes', 'included_errors'))
                db.execute("UPDATE directories SET excluded=? WHERE id=?", (int(excluded), folder_id))
                parent_id = row['parent_id']
                while parent_id is not None:
                    parent = db.execute("SELECT parent_id,excluded FROM directories WHERE id=?", (parent_id,)).fetchone()
                    db.execute("UPDATE directories SET included_count=included_count+?,included_bytes=included_bytes+?,"
                               "included_errors=included_errors+? WHERE id=?", (*delta, parent_id))
                    if parent['excluded']:
                        break
                    parent_id = parent['parent_id']
                db.execute("UPDATE inventories SET revision=revision+1,updated_at=? WHERE id=?", (self._clock(), identifier))
                db.commit()
                return self._remember(db, identifier)

    def iter_units(self, identifier, *, batch_size=500):
        identifier = _identifier(identifier)
        _page(batch_size, 0, 1000)
        with self._lock:
            summary = self.summary(identifier)
            if identifier in self._active or summary['status'] != 'ready':
                raise ValueError("Для плана нужен завершённый предварительный обзор.")
            if summary['selected_errors']:
                raise ValueError("В выбранных папках есть ошибки чтения. Исключите эти папки или повторите обзор.")
        # Hold a coherent read snapshot; an exclusion edit in another connection
        # cannot mix two revisions in the same generated plan.
        with self._connect() as db:
            db.execute('BEGIN')
            state = db.execute("SELECT * FROM inventories WHERE id=?", (identifier,)).fetchone()
            if state['revision'] != summary['revision'] or state['status'] != 'ready':
                raise ValueError("Выбор папок изменился. Повторите построение плана.")
            root = db.execute("SELECT * FROM directories WHERE id=?", (state['root_id'],)).fetchone()
            if root['excluded']:
                return
            if root['direct_count']:
                yield {'path': root['path'], 'remote_subdir': '', 'relative_path': '', 'recursive': False,
                       'excluded_paths': [], 'file_count': root['direct_count'], 'total_bytes': root['direct_bytes'],
                       'folder_id': root['id'], 'name': 'Файлы в корне'}
            after = 0
            while True:
                rows = db.execute("SELECT * FROM directories WHERE inventory_id=? AND parent_id=? AND excluded=0 "
                                  "AND included_count>0 AND id>? ORDER BY id LIMIT ?",
                                  (identifier, root['id'], after, batch_size)).fetchall()
                if not rows:
                    break
                for row in rows:
                    after = row['id']
                    prefix = row['relative_path'] + '/'
                    excluded_paths = []
                    covered = None
                    excluded_rows = db.execute("SELECT relative_path FROM directories WHERE inventory_id=? AND excluded=1 "
                                               "AND relative_path>=? AND relative_path<? ORDER BY relative_path",
                                               (identifier, prefix, row['relative_path'] + '0'))
                    for child in excluded_rows:
                        relative = child['relative_path'][len(prefix):]
                        if covered is not None and relative.startswith(covered + '/'):
                            continue
                        if len(excluded_paths) >= _MAX_EXCLUSIONS:
                            raise ValueError('В одной исходной папке слишком много отдельных исключений (более 50000). Исключите их общую родительскую папку или выберите более узкий корневой каталог.')
                        excluded_paths.append(relative)
                        covered = relative
                    yield {'path': row['path'], 'remote_subdir': row['relative_path'], 'relative_path': row['relative_path'],
                           'recursive': True, 'excluded_paths': excluded_paths, 'file_count': row['included_count'],
                           'total_bytes': row['included_bytes'], 'folder_id': row['id'], 'name': row['name']}

    def remove(self, identifier):
        identifier = _identifier(identifier)
        with self._lock:
            if identifier in self._active:
                raise ValueError("Сначала остановите предварительный обзор.")
            self.summary(identifier)
            with self._connect() as db:
                db.execute("DELETE FROM directories WHERE inventory_id=?", (identifier,))
                db.execute("DELETE FROM inventories WHERE id=?", (identifier,))
            self._summaries.pop(identifier, None)

    def close(self):
        # There is no idle connection or timer. The last SQLite connection
        # closes after each operation, checkpointing its WAL as appropriate.
        pass

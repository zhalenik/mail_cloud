"""A fresh, bounded-memory cloud inventory and an independent local hash cache.

Only the current successful listing is eligible for matching. Cached hashes
describe local bytes, never the continued existence or billing of a remote file.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time
import unicodedata

from .api import FileIdentity, ProtocolError, SourceChanged, cloud_hash
from .transport import Cancelled


def _path(value):
    if (not isinstance(value, str) or not value.startswith("/") or "\x00" in value
            or "\\" in value or any(p in {".", ".."} for p in value.split("/"))):
        raise ProtocolError("Некорректный путь в списке Облака.")
    return unicodedata.normalize("NFC", "/" + "/".join(p for p in value.split("/") if p))


def _snapshot(path):
    value = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(value.st_mode):
        raise SourceChanged("Исходник больше не является обычным файлом.")
    return (value.st_size, value.st_mtime_ns, value.st_dev, str(value.st_ino), value.st_ctime_ns)


class CloudInventory:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("Индекс Облака должен быть обычным файлом.")
            if os.name != "nt":
                os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA cache_size=-8192")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS files(
                path TEXT PRIMARY KEY, size INTEGER NOT NULL, hash TEXT NOT NULL,
                storage_mode TEXT NOT NULL, nodeid TEXT, evidence TEXT NOT NULL,
                path_key TEXT NOT NULL UNIQUE);
            CREATE INDEX IF NOT EXISTS files_content ON files(size,hash,path);
            CREATE TABLE IF NOT EXISTS folders(
                path TEXT PRIMARY KEY, offset INTEGER NOT NULL DEFAULT 0,
                total INTEGER, revision TEXT, done INTEGER NOT NULL DEFAULT 0,
                path_key TEXT NOT NULL UNIQUE);
            CREATE INDEX IF NOT EXISTS folders_pending ON folders(done,path);
            CREATE TABLE IF NOT EXISTS seen_paths(path_key TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS local_hashes(
                local_key TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
                size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, device INTEGER NOT NULL,
                inode TEXT NOT NULL, ctime_ns INTEGER NOT NULL, hash TEXT NOT NULL);
        """)
        self.complete = False
        self._pending_hashes = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        try:
            # Bound the optional cache when libraries are repeatedly reorganized.
            # Eviction only costs a future hash calculation, never correctness.
            self.db.execute("DELETE FROM local_hashes WHERE rowid IN (SELECT rowid FROM local_hashes "
                            "ORDER BY rowid LIMIT MAX(0,(SELECT COUNT(*) FROM local_hashes)-1000000))")
            self.db.commit()
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            self.db.close()

    @staticmethod
    def _check(stop):
        if stop.is_set():
            raise Cancelled("Сверка с Облаком приостановлена.")

    def refresh(self, api, *, stop, progress=None):
        """Walk every personal folder once; never use a previous pass as proof."""
        self.complete = False
        with self.db:
            self.db.execute("DELETE FROM files")
            self.db.execute("DELETE FROM folders")
            self.db.execute("DELETE FROM seen_paths")
            self.db.execute("INSERT INTO folders(path,path_key) VALUES('/','/')")
            self.db.execute("INSERT INTO seen_paths VALUES('/')")
        summary = {"cloud_files": 0, "cloud_folders": 0, "cloud_pages": 0, "excluded": 0}
        while folder := self.db.execute("SELECT * FROM folders WHERE done=0 ORDER BY path LIMIT 1").fetchone():
            self._check(stop)
            page = api.list_folder(folder["path"], offset=folder["offset"], limit=500)
            self._check(stop)
            if not isinstance(page, dict) or not isinstance(page.get("items"), list):
                raise ProtocolError("Не получена полная страница списка Облака.")
            total, following = page.get("total"), page.get("next_offset")
            listed, excluded = page.get("listed", len(page["items"])), page.get("excluded", 0)
            revision = json.dumps(page.get("revision"), sort_keys=True, ensure_ascii=False)
            if (type(total) is not int or total < 0 or type(listed) is not int or not 0 <= listed <= 500
                    or type(excluded) is not int or excluded < 0 or listed != len(page["items"]) + excluded
                    or (following is None and folder["offset"] + listed != total)
                    or (following is not None and following != folder["offset"] + listed)
                    or (following is not None and (type(following) is not int or not folder["offset"] < following < total))
                    or (folder["total"] is not None and (folder["total"] != total or folder["revision"] != revision))):
                raise ProtocolError("Каталог Облака изменился во время чтения или получена неполная страница. Повторите сканирование.")
            raw_paths = page.get("listed_paths")
            if raw_paths is None and excluded == 0:
                raw_paths = [item.get("path") if isinstance(item, dict) else None for item in page["items"]]
            if not isinstance(raw_paths, list) or len(raw_paths) != listed:
                raise ProtocolError("Не подтверждена полнота путей страницы Облака.")
            keys = [_path(value).casefold() for value in raw_paths]
            page_keys = set(keys)
            if any((key.rsplit("/", 1)[0] or "/") != folder["path"].casefold() or key == "/" for key in keys):
                raise ProtocolError("Страница Облака содержит путь другого каталога.")
            seen = set()
            try:
                with self.db:
                    self.db.executemany("INSERT INTO seen_paths VALUES(?)", ((key,) for key in keys))
                    for item in page["items"]:
                        if not isinstance(item, dict):
                            raise ProtocolError("Некорректный объект в списке Облака.")
                        path = _path(item.get("path"))
                        if path.casefold() in seen or path.casefold() not in page_keys or (path.rsplit("/", 1)[0] or "/").casefold() != folder["path"].casefold() or path == "/":
                            raise ProtocolError("Повторный или посторонний путь в странице Облака.")
                        seen.add(path.casefold())
                        if item.get("kind") == "folder":
                            self.db.execute("INSERT INTO folders(path,path_key) VALUES(?,?)", (path, path.casefold()))
                        elif item.get("kind") == "file":
                            size, digest = item.get("size"), item.get("hash")
                            if (type(size) is not int or size < 0 or not isinstance(digest, str)
                                    or re.fullmatch(r"[a-fA-F0-9]{40}", digest) is None):
                                raise ProtocolError("Не подтверждены размер и хеш объекта в Облаке.")
                            flag = item.get("is_unlim_upload")
                            mode = "unlimited" if flag is True else "regular" if flag is False else "unknown"
                            self.db.execute("INSERT INTO files VALUES(?,?,?,?,?,?,?)", (
                                path, size, digest.upper(), mode, item.get("nodeid"),
                                json.dumps(item, ensure_ascii=False, allow_nan=False), path.casefold()))
                            summary["cloud_files"] += 1
                        else:
                            raise ProtocolError("Неизвестный тип объекта в списке Облака.")
                    self.db.execute("UPDATE folders SET offset=?,total=?,revision=?,done=? WHERE path=?",
                                    (following if following is not None else total, total, revision,
                                     int(following is None), folder["path"]))
            except sqlite3.IntegrityError:
                raise ProtocolError("Список Облака содержит повторяющиеся пути. Повторите сканирование.") from None
            summary["cloud_pages"] += 1
            summary["cloud_folders"] += int(following is None)
            summary["excluded"] += excluded
            if progress is not None:
                progress(dict(summary))
        self._check(stop)
        self.complete = True
        return summary

    def has_size(self, size):
        if not self.complete:
            raise ProtocolError("Неполный индекс не подтверждает наличие или отсутствие файлов.")
        return self.db.execute("SELECT 1 FROM files WHERE size=? LIMIT 1", (size,)).fetchone() is not None

    def find(self, digest, size, preferred_path):
        if not self.complete:
            raise ProtocolError("Облачная сверка ещё не завершена.")
        row = self.db.execute(
            "SELECT * FROM files WHERE size=? AND hash=? ORDER BY CASE WHEN path=? THEN 0 ELSE 1 END,"
            "CASE storage_mode WHEN 'unlimited' THEN 0 WHEN 'regular' THEN 1 ELSE 2 END,path LIMIT 1",
            (size, digest.upper(), _path(preferred_path)),
        ).fetchone()
        return dict(row) if row else None

    def local_hash(self, candidate, *, stop):
        """Reuse local hashes only on Linux with an unchanged ctime fingerprint."""
        self._check(stop)
        path = candidate["local_path"]
        before = _snapshot(path)
        expected = (candidate["size"], candidate["mtime_ns"], candidate["device"], str(candidate["inode"]))
        if before[:4] != expected:
            raise SourceChanged("Исходный файл изменился после сканирования.")
        key = os.path.normcase(os.path.abspath(path))
        cacheable = sys.platform.startswith("linux") and before[4] > 0
        cached = self.db.execute("SELECT * FROM local_hashes WHERE local_key=?", (key,)).fetchone() if cacheable else None
        cache_hit = (cached is not None and cached["algorithm"] == "mailru-sha1-v1"
                     and tuple(cached[k] for k in ("size", "mtime_ns", "device", "inode", "ctime_ns")) == before)
        if cache_hit:
            digest = cached["hash"]
        else:
            digest, identity = cloud_hash(path, expected=FileIdentity(*expected[:3], int(expected[3])), stop=stop)
            if (identity.size, identity.mtime_ns, identity.device, str(identity.inode)) != expected:
                raise SourceChanged("Исходный файл изменился во время чтения.")
        self._check(stop)
        if _snapshot(path) != before:
            raise SourceChanged("Исходный файл изменился во время сверки.")
        if cacheable and not cache_hit:
            self.db.execute("INSERT INTO local_hashes VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(local_key) DO UPDATE SET "
                            "algorithm=excluded.algorithm,size=excluded.size,mtime_ns=excluded.mtime_ns,device=excluded.device,"
                            "inode=excluded.inode,ctime_ns=excluded.ctime_ns,hash=excluded.hash",
                            (key, "mailru-sha1-v1", *before, digest.upper()))
            self._pending_hashes += 1
            if self._pending_hashes >= 250:
                self.db.commit()
                self._pending_hashes = 0
        return digest.upper(), before[4]

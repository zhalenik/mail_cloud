"""Independent cloud-inventory contracts on real temporary SQLite databases.

The remote endpoint is a small in-memory fixture; no account or network is used.
The 300,000-object workload lives in scripts/benchmark_inventory.py so ordinary
unit-test runs stay short. Local hash-cache tests read real temporary files.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest import mock

from mailru_photos import inventory
from mailru_photos.api import ProtocolError, SourceChanged, cloud_hash
from mailru_photos.inventory import CloudInventory
from mailru_photos.transport import Cancelled


def file_item(path, number=1, **extra):
    return {"kind": "file", "path": path, "size": 1000 + number,
            "hash": f"{number:040x}", **extra}


def page(items, *, total=None, following=None, path="/", revision=None, excluded=0):
    listed = len(items) + excluded
    return {"path": path, "items": items, "total": listed if total is None else total,
            "next_offset": following, "listed": listed, "excluded": excluded,
            "revision": revision}


class TreeCloud:
    """Already decoded own-folder pages, as produced by CloudAPI.list_folder."""

    def __init__(self, folders):
        self.folders = folders
        self.calls = []
        self.revision = 1

    def list_folder(self, path, *, offset=0, limit=500):
        self.calls.append((path, offset, limit))
        entries = self.folders[path]
        items = copy.deepcopy(entries[offset:offset + limit])
        following = offset + len(items)
        return page(items, total=len(entries),
                    following=following if following < len(entries) else None,
                    path=path, revision={"tree": "own-tree", "rev": self.revision})


class ScriptedCloud:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []

    def list_folder(self, path, *, offset=0, limit=500):
        self.calls.append((path, offset, limit))
        response = next(self.pages)
        if isinstance(response, BaseException):
            raise response
        return copy.deepcopy(response)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "inventory.sqlite3"
        self.stop = threading.Event()

    def assert_incomplete(self, index):
        self.assertFalse(index.complete)
        with self.assertRaises(ProtocolError):
            index.find("1" * 40, 1, "/candidate.jpg")
        with self.assertRaises(ProtocolError):
            index.has_size(1)

    def test_fresh_index_and_reopened_index_cannot_prove_presence_or_absence(self):
        remote = file_item("/present.jpg")
        with CloudInventory(self.path) as index:
            self.assert_incomplete(index)
            index.refresh(TreeCloud({"/": [remote]}), stop=self.stop)
            self.assertIsNotNone(index.find(remote["hash"], remote["size"], remote["path"]))
        with CloudInventory(self.path) as index:
            self.assert_incomplete(index)
            index.refresh(TreeCloud({"/": []}), stop=self.stop)
            self.assertIsNone(index.find(remote["hash"], remote["size"], remote["path"]))

    def test_walks_nested_folders_in_bounded_pages_and_matches_another_path(self):
        root_files = [file_item(f"/root_{number}.jpg", number) for number in range(1, 1102)]
        remote = file_item("/Семья/Отпуск/ёж.jpg", 1200, is_unlim_upload=True)
        cloud = TreeCloud({"/": root_files + [{"kind": "folder", "path": "/Семья"}],
                           "/Семья": [{"kind": "folder", "path": "/Семья/Отпуск"}],
                           "/Семья/Отпуск": [remote]})
        snapshots = []
        with CloudInventory(self.path) as index:
            summary = index.refresh(cloud, stop=self.stop, progress=snapshots.append)
            self.assertEqual(summary, {"cloud_files": 1102, "cloud_folders": 3,
                                       "cloud_pages": 5, "excluded": 0})
            found = index.find(remote["hash"], remote["size"], "/NewDestination/ёж.jpg")
            self.assertEqual(found["path"], remote["path"])
            self.assertEqual(found["storage_mode"], "unlimited")
            self.assertIsNone(index.find(remote["hash"], remote["size"] + 1, remote["path"]))
            self.assertIsNone(index.find("F" * 40, remote["size"], remote["path"]))
            self.assertTrue(index.has_size(remote["size"]))
            self.assertFalse(index.has_size(10000000))
        self.assertEqual(cloud.calls[:3], [("/", 0, 500), ("/", 500, 500), ("/", 1000, 500)])
        self.assertEqual(len(snapshots), 5)
        self.assertEqual(snapshots[0]["cloud_pages"], 1)
        self.assertEqual(snapshots[-1], summary)

    def test_refresh_drops_deleted_paths_and_observes_new_location(self):
        removed = file_item("/removed.jpg", 1, is_unlim_upload=True)
        moved = file_item("/old.jpg", 2, is_unlim_upload=False)
        cloud = TreeCloud({"/": [removed, moved]})
        with CloudInventory(self.path) as index:
            first = index.refresh(cloud, stop=self.stop)
            self.assertEqual(first["cloud_files"], 2)
            cloud.folders = {"/": [{"kind": "folder", "path": "/Elsewhere"}],
                             "/Elsewhere": [{**moved, "path": "/Elsewhere/moved.jpg"}]}
            cloud.revision += 1
            second = index.refresh(cloud, stop=self.stop)
            self.assertEqual(second["cloud_files"], 1)
            self.assertIsNone(index.find(removed["hash"], removed["size"], removed["path"]))
            self.assertEqual(index.find(moved["hash"], moved["size"], moved["path"])["path"],
                             "/Elsewhere/moved.jpg")
            self.assertEqual(index.db.execute("SELECT COUNT(*) FROM files").fetchone()[0], 1)
            cloud.folders = {"/": []}
            self.assertEqual(index.refresh(cloud, stop=self.stop)["cloud_files"], 0)
            self.assertFalse(index.has_size(moved["size"]))

    def test_matching_prefers_requested_path_then_confirmed_unlimited(self):
        entries = [file_item("/unknown.jpg", is_unlim_upload="true"),
                   file_item("/regular.jpg", is_unlim_upload=False),
                   file_item("/unlimited.jpg", is_unlim_upload=True)]
        with CloudInventory(self.path) as index:
            index.refresh(TreeCloud({"/": entries}), stop=self.stop)
            digest, size = entries[0]["hash"], entries[0]["size"]
            self.assertEqual(index.find(digest, size, "/unknown.jpg")["storage_mode"], "unknown")
            self.assertEqual(index.find(digest, size, "/regular.jpg")["storage_mode"], "regular")
            self.assertEqual(index.find(digest, size, "/elsewhere.jpg")["path"], "/unlimited.jpg")

    def test_storage_classification_requires_an_actual_boolean(self):
        flags = [True, False, None, 0, 1, "true", "false"]
        entries = [file_item(f"/{number}.jpg", number + 1, is_unlim_upload=flag)
                   for number, flag in enumerate(flags)]
        entries.append(file_item("/missing.jpg", 99))
        with CloudInventory(self.path) as index:
            index.refresh(TreeCloud({"/": entries}), stop=self.stop)
            modes = [index.find(item["hash"], item["size"], item["path"])["storage_mode"] for item in entries]
            self.assertEqual(modes, ["unlimited", "regular"] + ["unknown"] * 6)

    def test_excluded_entries_advance_raw_offset_and_do_not_enter_index(self):
        cloud = ScriptedCloud([{**page([], total=3, following=2, excluded=2),
                                "listed_paths": ["/Foreign", "/Collection"]},
                               {**page([file_item("/own.jpg")], total=3), "listed_paths": ["/own.jpg"]}])
        with CloudInventory(self.path) as index:
            summary = index.refresh(cloud, stop=self.stop)
            self.assertEqual(summary["cloud_files"], 1)
            self.assertEqual(summary["excluded"], 2)
            self.assertEqual(cloud.calls, [("/", 0, 500), ("/", 2, 500)])

    def test_excluded_entries_without_raw_paths_cannot_prove_complete_listing(self):
        with CloudInventory(self.path) as index:
            with self.assertRaises(ProtocolError):
                index.refresh(ScriptedCloud([page([], excluded=1)]), stop=self.stop)
            self.assert_incomplete(index)

    def test_repeated_excluded_only_page_is_detected_between_pages(self):
        first = {**page([], total=2, following=1, excluded=1), "listed_paths": ["/Foreign"]}
        second = {**page([], total=2, excluded=1), "listed_paths": ["/FOREIGN"]}
        with CloudInventory(self.path) as index:
            with self.assertRaises(ProtocolError):
                index.refresh(ScriptedCloud([first, second]), stop=self.stop)
            self.assert_incomplete(index)

    def test_short_nonfinal_page_can_continue_without_assuming_500_entries(self):
        cloud = ScriptedCloud([page([file_item("/a.jpg", 1)], total=2, following=1),
                               page([file_item("/b.jpg", 2)], total=2)])
        with CloudInventory(self.path) as index:
            self.assertEqual(index.refresh(cloud, stop=self.stop)["cloud_files"], 2)
        self.assertEqual(cloud.calls, [("/", 0, 500), ("/", 1, 500)])

    def test_incomplete_or_nonprogressing_pages_never_become_complete(self):
        good = page([file_item("/first.jpg")], total=2, following=1)
        faults = [None, {}, {"items": None},
                  page([], total=2),
                  page([], total=2, following=1),
                  page([file_item("/second.jpg", 2)], total=3),
                  page([file_item("/second.jpg", 2)], total=2, following=2),
                  page([file_item("/second.jpg", 2)], total=2, following=True),
                  {**page([file_item("/second.jpg", 2)], total=2), "listed": 3},
                  {**page([file_item("/second.jpg", 2)], total=2), "excluded": -1},
                  {**page([file_item("/second.jpg", 2)], total=2), "total": True}]
        with CloudInventory(self.path) as index:
            for response in faults:
                with self.subTest(response=response), self.assertRaises(ProtocolError):
                    index.refresh(ScriptedCloud([good, response]), stop=self.stop)
                self.assert_incomplete(index)

    def test_revision_or_total_changes_between_pages_abort_refresh(self):
        first = page([file_item("/first.jpg")], total=2, following=1,
                     revision={"tree": "personal", "rev": 4, "grev": 10})
        later = page([file_item("/second.jpg", 2)], total=2,
                     revision={"tree": "personal", "rev": 4, "grev": 10})
        cases = [{**later, "total": 3, "next_offset": 2},
                 {**later, "revision": {"tree": "personal", "rev": 5, "grev": 10}},
                 {**later, "revision": {"tree": "personal", "rev": 4, "grev": 11}},
                 {**later, "revision": {"tree": "different", "rev": 4, "grev": 10}},
                 {**later, "revision": None}]
        with CloudInventory(self.path) as index:
            for response in cases:
                with self.subTest(response=response), self.assertRaises(ProtocolError):
                    index.refresh(ScriptedCloud([first, response]), stop=self.stop)
                self.assert_incomplete(index)

    def test_repeated_page_and_case_or_unicode_aliases_are_rejected(self):
        repeated = page([file_item("/same.jpg")], total=2, following=1)
        scenarios = [[repeated, page([file_item("/same.jpg")], total=2)],
                     [repeated, page([file_item("/SAME.JPG", 2)], total=2)],
                     [page([file_item("/е\u0308ж.jpg"), file_item("/ёж.jpg", 2)])],
                     [page([{"kind": "folder", "path": "/folder"}], total=2, following=1),
                      page([{"kind": "folder", "path": "/FOLDER"}], total=2)]]
        with CloudInventory(self.path) as index:
            for responses in scenarios:
                with self.subTest(pages=responses), self.assertRaises(ProtocolError):
                    index.refresh(ScriptedCloud(responses), stop=self.stop)
                self.assert_incomplete(index)

    def test_same_path_cannot_change_between_file_and_folder_on_later_page(self):
        for first, second in [(file_item("/same"), {"kind": "folder", "path": "/same"}),
                              ({"kind": "folder", "path": "/same"}, file_item("/same"))]:
            with self.subTest(first=first["kind"]), CloudInventory(self.path) as index:
                cloud = ScriptedCloud([page([first], total=2, following=1),
                                       page([second], total=2), page([], path="/same")])
                with self.assertRaises(ProtocolError):
                    index.refresh(cloud, stop=self.stop)
                self.assert_incomplete(index)

    def test_unknown_or_outside_items_abort_instead_of_claiming_absence(self):
        bad_items = [None, {}, {"kind": "album", "path": "/album"},
                     file_item("/outside/child.jpg"), file_item("relative.jpg"), file_item("/../bad"),
                     file_item("/"), file_item("/bad\\name"), file_item("/bad\0name"),
                     file_item("/bad.jpg", size=-1), file_item("/bad.jpg", size=True),
                     file_item("/bad.jpg", hash="0" * 39), file_item("/bad.jpg", hash="z" * 40)]
        with CloudInventory(self.path) as index:
            for item in bad_items:
                with self.subTest(item=item), self.assertRaises(ProtocolError):
                    index.refresh(ScriptedCloud([page([item])]), stop=self.stop)
                self.assert_incomplete(index)

    def test_transport_failure_invalidates_previous_success_and_next_refresh_recovers(self):
        old = file_item("/old.jpg")
        new = file_item("/new.jpg", 2)
        with CloudInventory(self.path) as index:
            index.refresh(TreeCloud({"/": [old]}), stop=self.stop)
            broken = ScriptedCloud([page([new], total=2, following=1), ProtocolError("fixture failure")])
            with self.assertRaises(ProtocolError):
                index.refresh(broken, stop=self.stop)
            self.assert_incomplete(index)
            index.refresh(TreeCloud({"/": [new]}), stop=self.stop)
            self.assertIsNone(index.find(old["hash"], old["size"], old["path"]))
            self.assertEqual(index.find(new["hash"], new["size"], new["path"])["path"], new["path"])

    def test_cancel_mid_refresh_does_not_reuse_old_or_partial_rows(self):
        old = file_item("/old.jpg")
        fresh = [file_item(f"/new-{number}.jpg", number) for number in range(1, 1002)]
        with CloudInventory(self.path) as index:
            index.refresh(TreeCloud({"/": [old]}), stop=self.stop)
            cloud = TreeCloud({"/": fresh})
            with self.assertRaises(Cancelled):
                index.refresh(cloud, stop=self.stop, progress=lambda _: self.stop.set())
            self.assertEqual(len(cloud.calls), 1)
            self.assert_incomplete(index)
            self.stop.clear()
            summary = index.refresh(TreeCloud({"/": fresh[500:]}), stop=self.stop)
            self.assertEqual(summary["cloud_files"], 501)
            self.assertIsNone(index.find(old["hash"], old["size"], old["path"]))
            self.assertIsNone(index.find(fresh[0]["hash"], fresh[0]["size"], fresh[0]["path"]))

    def test_pre_cancelled_refresh_makes_no_network_request(self):
        self.stop.set()
        cloud = TreeCloud({"/": []})
        with CloudInventory(self.path) as index:
            with self.assertRaises(Cancelled):
                index.refresh(cloud, stop=self.stop)
            self.assertEqual(cloud.calls, [])
            self.assert_incomplete(index)

    def test_close_checkpoints_real_wal_and_keeps_database_private(self):
        with CloudInventory(self.path) as index:
            index.refresh(TreeCloud({"/": [file_item("/photo.jpg")]}), stop=self.stop)
            self.assertEqual(index.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(index.db.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertGreater(Path(str(self.path) + "-wal").stat().st_size, 0)
            if os.name != "nt":
                self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        wal = Path(str(self.path) + "-wal")
        self.assertTrue(not wal.exists() or wal.stat().st_size == 0)
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM files").fetchone()[0], 1)


class LocalHashCacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.database = self.directory / "inventory.sqlite3"
        self.path = self.directory / "ёж.jpg"
        self.path.write_bytes(b"original local media bytes\n" * 30)
        self.stop = threading.Event()

    @staticmethod
    def candidate(path):
        details = path.stat(follow_symlinks=False)
        return {"local_path": str(path), "size": details.st_size, "mtime_ns": details.st_mtime_ns,
                "device": details.st_dev, "inode": str(details.st_ino)}

    @unittest.skipUnless(sys.platform.startswith("linux"), "persistent cache trusts Linux ctime only")
    def test_unchanged_real_file_reuses_hash_across_refresh_and_restart(self):
        candidate = self.candidate(self.path)
        with mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            with CloudInventory(self.database) as index:
                first = index.local_hash(candidate, stop=self.stop)
                self.assertEqual(index.local_hash(candidate, stop=self.stop), first)
                index.refresh(TreeCloud({"/": []}), stop=self.stop)
                self.assertEqual(index.local_hash(candidate, stop=self.stop), first)
                self.assertEqual(hasher.call_count, 1)
            with CloudInventory(self.database) as index:
                self.assertEqual(index.local_hash(candidate, stop=self.stop), first)
                self.assertEqual(hasher.call_count, 1)
                self.assert_incomplete_cache_only(index)

    def assert_incomplete_cache_only(self, index):
        self.assertFalse(index.complete)
        with self.assertRaises(ProtocolError):
            index.find("0" * 40, 0, "/photo.jpg")

    @unittest.skipUnless(sys.platform.startswith("linux"), "persistent cache trusts Linux ctime only")
    def test_rewrite_same_size_and_restored_mtime_invalidates_cache_via_ctime(self):
        candidate = self.candidate(self.path)
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            before_hash, before_ctime = index.local_hash(candidate, stop=self.stop)
            old_stat = self.path.stat()
            self.path.write_bytes(b"X" * old_stat.st_size)
            os.utime(self.path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
            changed = self.candidate(self.path)
            self.assertEqual(changed, candidate)
            after_hash, after_ctime = index.local_hash(changed, stop=self.stop)
            self.assertNotEqual(before_ctime, after_ctime)
            self.assertNotEqual(before_hash, after_hash)
            self.assertEqual(after_hash, cloud_hash(self.path)[0])
            self.assertEqual(hasher.call_count, 2)

    @unittest.skipUnless(sys.platform.startswith("linux"), "persistent cache trusts Linux ctime only")
    def test_rename_is_checked_then_cached_at_its_new_path(self):
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            digest, _ = index.local_hash(self.candidate(self.path), stop=self.stop)
            renamed = self.path.with_name("перенесённый.jpg")
            self.path.rename(renamed)
            new = self.candidate(renamed)
            self.assertEqual(index.local_hash(new, stop=self.stop)[0], digest)
            self.assertEqual(index.local_hash(new, stop=self.stop)[0], digest)
            self.assertEqual(hasher.call_count, 2)

    def test_changed_identity_and_symlink_are_rejected_before_hashing(self):
        candidate = self.candidate(self.path)
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            self.path.write_bytes(b"changed-size")
            with self.assertRaises(SourceChanged):
                index.local_hash(candidate, stop=self.stop)
            if hasattr(os, "symlink"):
                link = self.path.with_name("link.jpg")
                try:
                    link.symlink_to(self.path)
                except OSError:
                    pass
                else:
                    with self.assertRaises(SourceChanged):
                        index.local_hash(self.candidate(link), stop=self.stop)
            hasher.assert_not_called()

    def test_change_during_hashing_never_enters_the_cache(self):
        candidate = self.candidate(self.path)
        def changing_hash(path, **kwargs):
            result = cloud_hash(path, **kwargs)
            details = Path(path).stat()
            Path(path).write_bytes(b"Q" * details.st_size)
            os.utime(path, ns=(details.st_atime_ns, details.st_mtime_ns))
            return result
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash", side_effect=changing_hash):
            with self.assertRaises(SourceChanged):
                index.local_hash(candidate, stop=self.stop)
            self.assertEqual(index.db.execute("SELECT COUNT(*) FROM local_hashes").fetchone()[0], 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "persistent cache trusts Linux ctime only")
    def test_algorithm_version_change_invalidates_old_hash(self):
        candidate = self.candidate(self.path)
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            expected, _ = index.local_hash(candidate, stop=self.stop)
            index.db.execute("UPDATE local_hashes SET algorithm='unknown-v0', hash=?", ("F" * 40,))
            self.assertEqual(index.local_hash(candidate, stop=self.stop)[0], expected)
            self.assertEqual(hasher.call_count, 2)

    def test_non_linux_platform_does_not_trust_persistent_hash_cache(self):
        candidate = self.candidate(self.path)
        with CloudInventory(self.database) as index, mock.patch.object(inventory.sys, "platform", "darwin"), \
                mock.patch.object(inventory, "cloud_hash", wraps=cloud_hash) as hasher:
            self.assertEqual(index.local_hash(candidate, stop=self.stop), index.local_hash(candidate, stop=self.stop))
            self.assertEqual(hasher.call_count, 2)
            self.assertEqual(index.db.execute("SELECT COUNT(*) FROM local_hashes").fetchone()[0], 0)

    def test_cancelled_hash_does_not_read_source(self):
        self.stop.set()
        with CloudInventory(self.database) as index, mock.patch.object(inventory, "cloud_hash") as hasher:
            with self.assertRaises(Cancelled):
                index.local_hash(self.candidate(self.path), stop=self.stop)
            hasher.assert_not_called()


if __name__ == "__main__":
    unittest.main()

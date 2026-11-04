"""Fresh cloud-tree comparison using real queues, SQLite and temporary files.

The fixture exposes only paged reads. Per-file stat or remote mutations fail.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos import api as api_module
from mailru_photos import inventory as inventory_module
from mailru_photos.api import ProtocolError, RemoteError, cloud_hash
from mailru_photos.auth import AuthError
from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.queue import Queue, QueueBusyError, QueueConfigurationError
from mailru_photos.reconcile import reconcile
from mailru_photos.transport import Cancelled, SourceReadError, TransportError


class ReadOnlyAPI:
    def __init__(self, objects=None, *, failure=None, page_hook=None):
        self.objects = {} if objects is None else objects
        self.calls, self.revision = [], 1
        self.failure, self.page_hook = failure, page_hook

    def list_folder(self, path, *, offset=0, limit=500):
        self.calls.append((path, offset, limit))
        if self.failure is not None:
            raise self.failure
        assert 0 <= offset and 1 <= limit <= 500
        entries = {}
        prefix = path.rstrip("/") + "/"
        for home, metadata in self.objects.items():
            if not home.startswith(prefix):
                continue
            relative = home[len(prefix):]
            if "/" in relative:
                child = prefix + relative.split("/", 1)[0]
                entries[child] = {"kind": "folder", "path": child}
            else:
                entries[home] = {"kind": "file", "path": home, **metadata}
        ordered = sorted(entries.values(), key=lambda item: item["path"])
        items = copy.deepcopy(ordered[offset:offset + limit])
        following = offset + len(items)
        result = {"path": path, "items": items, "total": len(ordered), "listed": len(items), "excluded": 0,
                  "listed_paths": [item["path"] for item in items],
                  "next_offset": following if following < len(ordered) else None,
                  "revision": {"tree": "offline-own-tree", "rev": self.revision}}
        return self.page_hook(path, offset, result) if self.page_hook else result

    def stat(self, *args, **kwargs):
        raise AssertionError("No per-file network stat is allowed during inventory reconciliation")

    def upload_photo(self, *args, **kwargs):
        raise AssertionError("Reconciliation must never write to the cloud")

    upload_regular = ensure_parent = delete = trash_file = upload_photo


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / "photos"
        self.source.mkdir()
        self.queue = Queue(self.base / "queue.sqlite3", destination="/Archive", account_id="fixture-account")
        self.root = self.queue.add_root(self.source, "camera")
        self.stop = threading.Event()

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def photo(self, name="photo.jpg", contents=b"\xff\xd8" + b"offline image bytes" * 10):
        path = self.source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def objects(self):
        result = {}
        for row in self.queue.list_jobs(limit=10000):
            digest, identity = cloud_hash(row["local_path"])
            result[row["remote_path"]] = {"hash": digest, "size": identity.size}
        return result

    def fresh_scan(self, root_ids=None):
        self.queue.reset_for_scan(root_ids=root_ids)
        return self.queue.scan(root_ids=root_ids)

    def test_exact_current_match_is_observation_not_an_upload_receipt(self):
        original = self.photo("nested/photo.jpg")
        self.queue.scan()
        job = self.queue.list_jobs()[0]
        objects = self.objects()
        objects[job["remote_path"]].update(is_unlim_upload=True, nodeid="fresh-node")
        api = ReadOnlyAPI(objects)
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1, 1, 0))
        self.assertTrue(report["complete"] and report["inventory_complete"])
        self.assertEqual(report["matched_elsewhere"], 0)
        self.assertEqual(api.calls, [("/", 0, 500), ("/Archive", 0, 500),
                                     ("/Archive/camera", 0, 500), ("/Archive/camera/nested", 0, 500)])
        self.assertTrue((self.base / "cloud-index.sqlite3").exists(), "Queue.path is a string")
        row = self.queue.get_job(job["id"])
        self.assertEqual((row["status"], row["skip_reason"]), ("skipped", "remote_exists"))
        self.assertEqual(row["observed_mode"], "unlimited")
        self.assertTrue(row["cloud_present"])
        self.assertEqual(row["cloud_path"], job["remote_path"])
        self.assertIsNone(row["storage_mode"])
        self.assertIsNone(row["uploaded_at"])
        self.assertIsNotNone(row["completed_at"])
        self.assertEqual(row["progress_bytes"], row["size"])
        observed = json.loads(row["verified_metadata"])
        self.assertEqual(observed["quota_status"], "unlimited")
        self.assertIs(observed["is_unlim_upload"], True)
        self.assertNotIn("unlimited_confirmed", observed)
        self.assertEqual(self.queue.upload_history(job["id"]), [])
        self.assertEqual(self.queue.commit_history(job["id"]), [])
        stats = self.queue.stats()
        self.assertEqual((stats["uploaded"], stats["existing"], stats["processed"], stats["remaining"]), (0, 1, 1, 0))
        self.assertEqual(stats["existing_unlimited_count"], 1)
        self.assertEqual(stats["transferred_total_count"], 0)
        self.assertEqual(stats["uploaded_bytes"], 0)
        self.assertTrue(original.exists())

    def test_same_content_elsewhere_records_actual_path_and_current_storage_mode(self):
        self.photo("отпуск/ёж.jpg")
        self.queue.scan()
        row = self.queue.list_jobs()[0]
        actual = "/Другая папка/Телефон/копия.jpg"
        data = self.objects()[row["remote_path"]]
        api = ReadOnlyAPI({actual: {**data, "is_unlim_upload": False}})
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["matched"], report["matched_elsewhere"], report["errors"]), (1, 1, 0))
        result = self.queue.get_job(row["id"])
        self.assertEqual(result["remote_path"], row["remote_path"])
        self.assertEqual(result["cloud_path"], actual)
        self.assertEqual(result["observed_mode"], "regular")
        self.assertFalse(result["cloud_path_matches"])
        self.assertEqual(self.queue.stats()["existing_regular_count"], 1)

    def test_unknown_storage_is_not_guessed_from_extension_or_old_observation(self):
        self.photo()
        self.queue.scan()
        objects = self.objects()
        old_id = self.queue.list_jobs()[0]["id"]
        for data in objects.values():
            data["is_unlim_upload"] = True
        self.assertEqual(reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)["matched"], 1)
        self.fresh_scan()
        new = self.queue.list_jobs()[0]
        self.assertNotEqual(new["id"], old_id)
        for data in objects.values():
            data.pop("is_unlim_upload")
        report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertEqual(report["matched"], 1)
        row = self.queue.get_job(new["id"])
        self.assertEqual(row["observed_mode"], "unknown")
        self.assertEqual(json.loads(row["verified_metadata"])["quota_status"], "unknown")
        self.assertEqual(self.queue.stats()["existing_unknown_count"], 1)
        self.assertEqual(self.queue.stats()["existing_unlimited_count"], 0)

    def test_every_fresh_scan_rechecks_deleted_and_moved_remote_content(self):
        self.photo("deleted.jpg", b"deleted content" * 30)
        self.photo("moved.jpg", b"moved content" * 30)
        self.queue.scan()
        rows = {row["relative_path"]: row for row in self.queue.list_jobs()}
        api = ReadOnlyAPI(self.objects())
        self.assertEqual(reconcile(self.queue, api, stop=self.stop)["matched"], 2)
        old_calls = len(api.calls)
        moved_data = api.objects.pop(rows["moved.jpg"]["remote_path"])
        api.objects.pop(rows["deleted.jpg"]["remote_path"])
        api.objects["/Moved/current.jpg"] = {**moved_data, "is_unlim_upload": True}
        api.revision += 1
        self.fresh_scan()
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["matched_elsewhere"]), (2, 1, 1))
        self.assertEqual(api.calls[old_calls], ("/", 0, 500))
        updated = {row["relative_path"]: row for row in self.queue.list_jobs()}
        self.assertEqual(updated["deleted.jpg"]["status"], "pending")
        self.assertFalse(updated["deleted.jpg"]["cloud_present"])
        self.assertEqual(updated["moved.jpg"]["cloud_path"], "/Moved/current.jpg")
        api.objects.clear()
        self.fresh_scan()
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["cloud_files"]), (2, 0, 0))
        self.assertEqual(self.queue.stats()["existing"], 0)
        self.assertEqual(self.queue.stats()["pending"], 2)

    def test_prior_upload_receipt_is_not_remote_presence_evidence_after_fresh_scan(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        digest, _ = cloud_hash(job.local_path)
        self.queue.mark_uploaded(job, remote_hash=digest, verified_metadata={"is_unlim_upload": True})
        self.fresh_scan()
        report = reconcile(self.queue, ReadOnlyAPI(), stop=self.stop)
        self.assertTrue(report["complete"])
        self.assertEqual(report["matched"], 0)
        row = self.queue.list_jobs()[0]
        self.assertEqual(row["status"], "pending")
        self.assertFalse(row["cloud_present"])
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 1)

    def test_same_index_is_reread_even_if_there_are_no_candidates(self):
        self.photo()
        self.queue.scan()
        api = ReadOnlyAPI(self.objects())
        reconcile(self.queue, api, stop=self.stop)
        first_calls = list(api.calls)
        second = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual(second["checked"], 0)
        self.assertTrue(second["inventory_complete"])
        self.assertEqual(api.calls[len(first_calls):], first_calls)

    def test_absent_size_and_empty_cloud_avoid_local_contents_reads(self):
        self.photo("missing.jpg")
        self.photo("different-size.jpg")
        self.queue.scan()
        rows = self.queue.list_jobs()
        objects = {"/Elsewhere/too-big.jpg": {"size": rows[0]["size"] + 1, "hash": "A" * 40}}
        before = self.queue.list_jobs()
        for remote in (objects, {}):
            with self.subTest(remote=remote), patch.object(inventory_module, "cloud_hash",
                    side_effect=AssertionError("unnecessary file read")):
                report = reconcile(self.queue, ReadOnlyAPI(remote), stop=self.stop)
            self.assertEqual((report["checked"], report["matched"], report["errors"]), (2, 0, 0))
            self.assertEqual(self.queue.list_jobs(), before)

    def test_hash_mismatch_never_overwrites_or_completes_the_task(self):
        self.photo()
        self.queue.scan()
        row = self.queue.list_jobs()[0]
        api = ReadOnlyAPI({"/unrelated.jpg": {"size": row["size"], "hash": "A" * 40}})
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1, 0, 0))
        self.assertEqual(api.calls, [("/", 0, 500)])
        self.assertEqual(self.queue.get_job(row["id"]), row)

    def test_incomplete_listing_never_matches_first_page_or_trusts_previous_index(self):
        self.photo()
        self.queue.scan()
        valid = next(iter(self.objects().values()))
        self.assertEqual(reconcile(self.queue, ReadOnlyAPI({"/old.jpg": valid}), stop=self.stop)["matched"], 1)
        self.fresh_scan()
        before = self.queue.list_jobs()
        first = {"path": "/", "items": [{"kind": "file", "path": "/first.jpg", **valid}],
                 "listed": 1, "listed_paths": ["/first.jpg"], "excluded": 0,
                 "total": 2, "next_offset": 1, "revision": {"rev": 1}}
        failures = [TransportError("connection dropped"),
                    {**first, "items": [], "listed": 0, "listed_paths": [], "next_offset": None},
                    {**first, "next_offset": None},
                    {**first, "items": [{"kind": "file", "path": "/second.jpg", **valid}],
                     "listed_paths": ["/second.jpg"], "next_offset": None, "revision": {"rev": 2}}]
        for failure in failures:
            calls = []
            class Pages:
                def list_folder(self, path, *, offset=0, limit=500):
                    calls.append((path, offset, limit))
                    value = first if offset == 0 else failure
                    if isinstance(value, BaseException):
                        raise value
                    return copy.deepcopy(value)
            with self.subTest(failure=failure), patch.object(inventory_module, "cloud_hash",
                    side_effect=AssertionError("No matching before complete inventory")):
                report = reconcile(self.queue, Pages(), stop=self.stop)
            self.assertFalse(report["inventory_complete"] or report["complete"])
            self.assertEqual((report["checked"], report["matched"], report["errors"]), (0, 0, 1))
            self.assertEqual(len(calls), 2)
            self.assertEqual(self.queue.list_jobs(), before)
        recovered = reconcile(self.queue, ReadOnlyAPI(), stop=self.stop)
        self.assertTrue(recovered["complete"])
        self.assertEqual(recovered["matched"], 0)

    def test_invalid_remote_identity_aborts_listing_before_local_matching(self):
        self.photo()
        self.queue.scan()
        valid = next(iter(self.objects().values()))
        for bad in ({**valid, "path": "/Another/file.jpg"}, {**valid, "hash": "short"},
                    {**valid, "size": True}, {**valid, "size": -1}, {**valid, "kind": "mounted"}):
            with self.subTest(metadata=bad):
                report = reconcile(self.queue, ReadOnlyAPI({"/bad.jpg": bad}), stop=self.stop)
                self.assertEqual((report["checked"], report["matched"], report["errors"]), (0, 0, 1))
                self.assertEqual(report["aborted"], "protocol")
                self.assertEqual(self.queue.stats()["pending"], 1)

    def test_paused_removed_intent_uploaded_and_empty_are_excluded_but_large_can_be_compared(self):
        for name in ("eligible.jpg", "paused.jpg", "intent.jpg", "uploaded.jpg", "removed.jpg"):
            self.photo(name)
        self.photo("empty.jpg", b"")
        large = self.photo("large.mp4")
        with large.open("r+b") as stream:
            stream.truncate(UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        jobs = []
        while job := self.queue.claim():
            jobs.append(job)
        for job in jobs:
            if job.relative_path == "intent.jpg":
                self.queue.save_commit_intent(job, "uncertain")
            if job.relative_path == "uploaded.jpg":
                self.queue.mark_uploaded(job, remote_hash="earlier-proof")
            else:
                if job.relative_path == "paused.jpg":
                    self.queue.request_control(job.id, "pause")
                if job.relative_path == "removed.jpg":
                    self.queue.request_control(job.id, "remove")
                self.queue.release(job)
        candidates = self.queue.reconcile_candidates()
        self.assertEqual({row["relative_path"] for row in candidates}, {"eligible.jpg", "large.mp4"})
        eligible = next(row for row in candidates if row["relative_path"] == "eligible.jpg")
        digest, _ = cloud_hash(eligible["local_path"])
        report = reconcile(self.queue, ReadOnlyAPI({eligible["remote_path"]: {"size": eligible["size"], "hash": digest}}), stop=self.stop)
        self.assertEqual((report["checked"], report["matched"]), (2, 1))
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 1)
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 1)

    def test_permanent_file_error_can_be_resolved_by_existing_content(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_failed(job, "old file failure", retry_delay=None)
        self.assertEqual(reconcile(self.queue, ReadOnlyAPI(self.objects()), stop=self.stop)["matched"], 1)
        self.assertEqual(self.queue.get_job(job.id)["skip_reason"], "remote_exists")
        self.assertEqual(self.queue.stats()["failed"], 0)

    def test_changed_or_missing_source_fails_only_its_task_and_others_continue(self):
        for name in ("changed.jpg", "gone.jpg", "good.jpg"):
            self.photo(name)
        self.queue.scan()
        objects = self.objects()
        (self.source / "changed.jpg").write_bytes(b"different bytes after scan")
        (self.source / "gone.jpg").unlink()
        report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (3, 1, 2))
        self.assertIsNone(report["aborted"])
        self.assertTrue(report["complete"])
        self.assertEqual(self.queue.stats()["failed"], 2)
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 0)

    def test_final_adoption_rechecks_source_pause_mapping_and_commit_intent(self):
        for change in ("source", "same_size_mtime", "pause", "mapping", "intent", "remove"):
            with self.subTest(change=change):
                source = self.base / change
                source.mkdir()
                local = source / "image.jpg"
                local.write_bytes(b"matching source" * 20)
                with Queue(self.base / (change + ".sqlite3"), destination="/Archive", account_id="fixture-account") as queue:
                    queue.add_root(source, "camera")
                    queue.scan()
                    row = queue.list_jobs()[0]
                    digest, identity = cloud_hash(local)
                    adopt = queue.mark_remote_existing
                    def mutate(candidate, **kwargs):
                        if change == "source":
                            local.write_bytes(b"replaced after hashing")
                        elif change == "same_size_mtime":
                            previous = local.stat()
                            local.write_bytes(b"X" * previous.st_size)
                            os.utime(local, ns=(previous.st_atime_ns, previous.st_mtime_ns))
                        elif change == "pause":
                            queue.request_control(row["id"], "pause")
                        elif change == "mapping":
                            queue.update_root(row["root_id"], "/Changed")
                        elif change == "intent":
                            claimed = queue.claim()
                            queue.save_commit_intent(claimed, digest)
                            queue.release(claimed)
                        else:
                            queue.request_control(row["id"], "remove")
                        return adopt(candidate, **kwargs)
                    api = ReadOnlyAPI({"/elsewhere.jpg": {"size": identity.size, "hash": digest}})
                    with patch.object(queue, "mark_remote_existing", side_effect=mutate):
                        report = reconcile(queue, api, stop=self.stop, inventory_path=self.base / (change + "-index.sqlite3"))
                    self.assertEqual(report["matched"], 0)
                    self.assertEqual(queue.stats()["existing"], 0)
                    self.assertEqual(queue.upload_history(row["id"]), [])

    def test_global_network_protocol_and_auth_errors_end_before_matching(self):
        for number in range(8):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        for error, reason, auth in ((TransportError("offline"), "network", False),
                                    (ProtocolError("unknown protocol"), "protocol", False),
                                    (RemoteError(503), "remote", False),
                                    (RemoteError(403, authenticated=False), "remote", False),
                                    (RemoteError(401), "auth", True), (RemoteError(403), "auth", True),
                                    (AuthError("expired"), "auth", True)):
            with self.subTest(error=type(error).__name__, reason=reason):
                api = ReadOnlyAPI(failure=error)
                report = reconcile(self.queue, api, stop=self.stop)
                self.assertEqual((report["checked"], report["errors"], len(api.calls)), (0, 1, 1))
                self.assertEqual(report["aborted"], reason)
                self.assertIs(report["auth_required"], auth)
                self.assertFalse(report["inventory_complete"])
                self.assertEqual(self.queue.stats()["pending"], 8)

    def test_local_error_samples_are_capped_and_progress_is_an_independent_snapshot(self):
        for number in range(130):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        objects, updates = self.objects(), []
        def progress(value):
            updates.append(value)
            value["error_samples"].clear()
        with patch.object(inventory_module, "cloud_hash", side_effect=SourceReadError("unreadable")):
            report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop, progress=progress)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (130, 0, 130))
        self.assertEqual(len(report["error_samples"]), 100)
        self.assertEqual(updates[-1]["errors"], 130)
        self.assertEqual(self.queue.stats()["failed"], 130)
        self.assertTrue(report["complete"])

    def test_cancellation_before_or_during_listing_does_not_change_queue(self):
        self.photo()
        self.queue.scan()
        before = self.queue.list_jobs()
        api = ReadOnlyAPI(self.objects())
        self.stop.set()
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(api.calls, [])
        self.stop.clear()
        def stopped(path, offset, result):
            self.stop.set()
            return result
        api = ReadOnlyAPI(self.objects(), page_hook=stopped)
        with patch.object(inventory_module, "cloud_hash", side_effect=AssertionError("hash after cancel")):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["errors"], 0)
        self.assertFalse(report["inventory_complete"])
        self.assertEqual(self.queue.list_jobs(), before)
        self.stop.clear()
        report = reconcile(self.queue, ReadOnlyAPI(failure=Cancelled("pause")), stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["errors"], 0)

    def test_actual_streaming_hash_observes_cancellation_between_blocks(self):
        self.photo(contents=b"source block" * 200000)
        self.queue.scan()
        api = ReadOnlyAPI(self.objects())
        original_check, checks = api_module.check_cancelled, 0
        def stop_during_hash(event):
            nonlocal checks
            checks += 1
            if checks == 2:
                self.stop.set()
            original_check(event)
        with patch.object(api_module, "check_cancelled", side_effect=stop_during_hash):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"] and report["inventory_complete"])
        self.assertFalse(report["complete"])
        self.assertEqual(report["matched"], 0)
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_cancellation_after_one_adoption_preserves_finished_observation(self):
        self.photo("first.jpg", b"first source bytes" * 20)
        self.photo("second.jpg", b"second source bytes" * 20)
        self.queue.scan()
        objects, original = self.objects(), self.queue.mark_remote_existing
        def adopt_then_stop(*args, **kwargs):
            result = original(*args, **kwargs)
            self.stop.set()
            return result
        with patch.object(self.queue, "mark_remote_existing", side_effect=adopt_then_stop):
            report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["matched"], 1)
        self.assertEqual(self.queue.stats()["existing"], 1)
        self.assertEqual(self.queue.stats()["pending"], 1)
        self.stop.clear()
        recovered = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertTrue(recovered["complete"])
        self.assertEqual((recovered["checked"], recovered["matched"]), (1, 1))
        self.assertEqual(self.queue.stats()["existing"], 2)

    def test_database_failure_propagates_instead_of_becoming_a_skipped_file(self):
        self.photo()
        self.queue.scan()
        with patch.object(self.queue, "mark_remote_existing", side_effect=sqlite3.OperationalError("database unavailable")):
            with self.assertRaises(sqlite3.OperationalError):
                reconcile(self.queue, ReadOnlyAPI(self.objects()), stop=self.stop)
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_unbound_account_and_live_upload_are_rejected_before_cloud_listing(self):
        self.photo()
        self.queue.scan()
        api = ReadOnlyAPI()
        self.queue._db.execute("DELETE FROM metadata WHERE key='_account_id'")
        with self.assertRaises(QueueConfigurationError):
            reconcile(self.queue, api, stop=self.stop)
        self.queue.bind_account("fixture-account")
        self.queue.claim()
        with self.assertRaises(QueueBusyError):
            reconcile(self.queue, api, stop=self.stop)
        self.assertEqual(api.calls, [])

    def test_selected_sources_use_global_inventory_without_mutating_other_source_tasks(self):
        self.photo("one.jpg")
        another = self.base / "another-source"
        another.mkdir()
        (another / "two.jpg").write_bytes(b"another source media" * 20)
        second_root = self.queue.add_root(another, "second")
        self.queue.scan()
        rows = self.queue.list_jobs()
        first = next(row for row in rows if row["root_id"] == self.root.id)
        second = next(row for row in rows if row["root_id"] == second_root.id)
        objects = self.objects()
        metadata = objects.pop(first["remote_path"])
        objects["/Unrelated/already.jpg"] = metadata
        report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop, root_ids=[self.root.id])
        self.assertEqual((report["checked"], report["matched"], report["matched_elsewhere"]), (1, 1, 1))
        self.assertEqual(self.queue.get_job(second["id"]), second)
        self.assertEqual(self.queue.get_job(first["id"])["cloud_path"], "/Unrelated/already.jpg")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Persistent reuse requires Linux ctime")
    def test_hash_cache_survives_fresh_task_ids_but_not_same_size_rewrites(self):
        local = self.photo()
        self.queue.scan()
        api = ReadOnlyAPI(self.objects())
        with patch.object(inventory_module, "cloud_hash", wraps=cloud_hash) as hasher:
            reconcile(self.queue, api, stop=self.stop)
            self.assertEqual(hasher.call_count, 1)
            self.fresh_scan()
            self.assertEqual(reconcile(self.queue, api, stop=self.stop)["matched"], 1)
            self.assertEqual(hasher.call_count, 1)
            before = local.stat()
            local.write_bytes(b"X" * before.st_size)
            os.utime(local, ns=(before.st_atime_ns, before.st_mtime_ns))
            self.fresh_scan()
            self.assertEqual(reconcile(self.queue, api, stop=self.stop)["matched"], 0)
            self.assertEqual(hasher.call_count, 2)
        self.assertEqual(sum(path == "/" for path, _, _ in api.calls), 3)
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_keyset_pages_stay_bounded_as_matches_leave_candidate_index(self):
        for number in range(1201):
            self.photo(f"{number}.jpg", f"unique content number {number}".encode() * 10)
        self.queue.scan()
        api, fetch, pages = ReadOnlyAPI(self.objects()), self.queue.reconcile_candidates, []
        def bounded_page(*, after_id=0, limit=500, root_ids=None):
            self.assertLessEqual(limit, 500)
            rows = fetch(after_id=after_id, limit=limit, root_ids=root_ids)
            self.assertTrue(all(row["id"] > after_id for row in rows))
            if limit == 500:
                pages.append((after_id, len(rows)))
            return rows
        with patch.object(self.queue, "reconcile_candidates", side_effect=bounded_page):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1201, 1201, 0))
        self.assertEqual([size for _, size in pages], [500, 500, 201, 0])
        self.assertEqual(api.calls, [("/", 0, 500), ("/Archive", 0, 500),
                                    ("/Archive/camera", 0, 500), ("/Archive/camera", 500, 500),
                                    ("/Archive/camera", 1000, 500)])
        self.assertEqual(self.queue.stats()["existing"], 1201)
        self.assertEqual(self.queue.stats()["uploaded"], 0)


if __name__ == "__main__":
    unittest.main()

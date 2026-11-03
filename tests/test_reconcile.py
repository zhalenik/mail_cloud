"""Offline optional reconciliation: observations never become upload proof."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos import api as api_module
from mailru_photos.api import ProtocolError, RemoteError, cloud_hash
from mailru_photos.auth import AuthError
from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.queue import Queue, QueueBusyError, QueueConfigurationError
from mailru_photos.reconcile import reconcile
from mailru_photos.transport import Cancelled, SourceReadError, TransportError


class ReadOnlyAPI:
    def __init__(self, objects=None):
        self.objects = objects or {}
        self.calls = []

    def stat(self, path):
        self.calls.append(path)
        result = self.objects.get(path)
        if isinstance(result, list):
            if not result:
                raise AssertionError("Unexpected extra stat request")
            result = result.pop(0)
        if callable(result):
            result = result(path)
        if isinstance(result, Exception):
            raise result
        return result

    def upload_photo(self, *args, **kwargs):
        raise AssertionError("Reconciliation must not upload bytes")

    def ensure_parent(self, *args, **kwargs):
        raise AssertionError("Reconciliation must not create cloud folders")

    def delete(self, *args, **kwargs):
        raise AssertionError("Reconciliation must not delete cloud objects")


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / "photos"
        self.source.mkdir()
        self.queue = Queue(self.base / "queue.sqlite3", destination="/Archive", account_id="fixture-account")
        self.queue.add_root(self.source, "camera")
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

    def test_exact_match_is_skipped_observation_without_upload_or_billing_proof(self):
        original = self.photo("nested/photo.jpg")
        self.queue.scan()
        job = self.queue.list_jobs()[0]
        objects = self.objects()
        objects[job["remote_path"]]["is_unlim_upload"] = True  # Never adopted as evidence.
        api = ReadOnlyAPI(objects)
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1, 1, 0))
        self.assertEqual(api.calls, [job["remote_path"], job["remote_path"]])
        row = self.queue.get_job(job["id"])
        self.assertEqual((row["status"], row["skip_reason"]), ("skipped", "remote_exists"))
        self.assertIsNone(row["storage_mode"])
        self.assertIsNone(row["uploaded_at"])
        observed = json.loads(row["verified_metadata"])
        self.assertEqual(observed["path"], job["remote_path"])
        self.assertEqual(observed["quota_status"], "unknown")
        self.assertNotIn("is_unlim_upload", observed)
        self.assertNotIn("unlimited_confirmed", observed)
        self.assertEqual(self.queue.upload_history(job["id"]), [])
        self.assertEqual(self.queue.commit_history(job["id"]), [])
        stats = self.queue.stats()
        self.assertEqual((stats["uploaded"], stats["skipped"], stats["existing"], stats["processed"], stats["remaining"]),
                         (0, 1, 1, 1, 0))
        self.assertEqual(stats["uploaded_bytes"], 0)
        self.assertTrue(original.exists())

    def test_missing_target_and_different_size_do_not_read_local_contents(self):
        self.photo("missing.jpg")
        self.photo("different-size.jpg")
        self.queue.scan()
        rows = self.queue.list_jobs()
        objects = {row["remote_path"]: {"size": row["size"] + 1, "hash": "A" * 40}
                   for row in rows if row["relative_path"] == "different-size.jpg"}
        before = self.queue.list_jobs()
        with patch("mailru_photos.reconcile.cloud_hash", side_effect=AssertionError("unnecessary file read")):
            report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (2, 0, 0))
        self.assertEqual(self.queue.list_jobs(), before)

    def test_hash_mismatch_never_overwrites_or_marks_the_task_complete(self):
        self.photo()
        self.queue.scan()
        row = self.queue.list_jobs()[0]
        api = ReadOnlyAPI({row["remote_path"]: {"size": row["size"], "hash": "A" * 40}})
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1, 0, 0))
        self.assertEqual(len(api.calls), 1)
        self.assertEqual(self.queue.get_job(row["id"]), row)

    def test_wrong_path_invalid_hash_and_zero_remote_size_never_match(self):
        self.photo()
        self.queue.scan()
        row = self.queue.list_jobs()[0]
        valid = self.objects()[row["remote_path"]]
        for bad in ({**valid, "path": "/Another/file.jpg"}, {**valid, "hash": "short"},
                    {**valid, "size": 0}, {**valid, "size": True}, None):
            with self.subTest(metadata=bad):
                report = reconcile(self.queue, ReadOnlyAPI({row["remote_path"]: bad}), stop=self.stop)
                self.assertEqual(report["matched"], 0)
                self.assertEqual(report["errors"], int(bad is not None))
                self.assertEqual(self.queue.get_job(row["id"])["status"], "pending")

    def test_remote_change_while_hashing_does_not_complete_a_stale_observation(self):
        self.photo()
        self.queue.scan()
        row = self.queue.list_jobs()[0]
        valid = self.objects()[row["remote_path"]]
        for later in (None, {**valid, "hash": "B" * 40}, {**valid, "size": valid["size"] + 1}):
            with self.subTest(later=later):
                api = ReadOnlyAPI({row["remote_path"]: [valid, later]})
                report = reconcile(self.queue, api, stop=self.stop)
                self.assertEqual((report["matched"], report["errors"]), (0, 0))
                self.assertEqual(self.queue.get_job(row["id"]), row)

    def test_paused_removed_intent_uploaded_empty_and_oversized_tasks_are_excluded(self):
        names = ("eligible.jpg", "paused.jpg", "intent.jpg", "uploaded.jpg", "removed.jpg")
        for name in names:
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
        self.assertEqual([row["relative_path"] for row in candidates], ["eligible.jpg"])
        eligible = candidates[0]
        digest, _ = cloud_hash(eligible["local_path"])
        api = ReadOnlyAPI({eligible["remote_path"]: {"size": eligible["size"], "hash": digest}})
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"]), (1, 1))
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 1)
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 1)

    def test_permanent_file_error_can_be_resolved_by_existing_content_without_upload(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_failed(job, "old individual file failure", retry_delay=None)
        report = reconcile(self.queue, ReadOnlyAPI(self.objects()), stop=self.stop)
        self.assertEqual(report["matched"], 1)
        self.assertEqual(self.queue.get_job(job.id)["skip_reason"], "remote_exists")
        self.assertEqual(self.queue.stats()["failed"], 0)

    def test_source_changed_after_scan_is_reported_and_other_tasks_continue(self):
        self.photo("changed.jpg")
        self.photo("good.jpg")
        self.queue.scan()
        objects = self.objects()
        changed = next(row for row in self.queue.list_jobs() if row["relative_path"] == "changed.jpg")
        Path(changed["local_path"]).write_bytes(b"new source bytes after scan")
        report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (2, 1, 1))
        self.assertIsNone(report["aborted"])
        self.assertEqual(self.queue.get_job(changed["id"])["status"], "pending")
        self.assertEqual(self.queue.upload_history(changed["id"]), [])

    def test_final_transition_rechecks_source_pause_mapping_and_commit_intent(self):
        for change in ("source", "pause", "mapping", "intent", "remove"):
            with self.subTest(change=change):
                db = self.base / (change + ".sqlite3")
                source = self.base / change
                source.mkdir()
                local = source / "image.jpg"
                local.write_bytes(b"matching source" * 20)
                with Queue(db, destination="/Archive", account_id="fixture-account") as queue:
                    queue.add_root(source, "camera")
                    queue.scan()
                    row = queue.list_jobs()[0]
                    digest, identity = cloud_hash(local)
                    valid = {"size": identity.size, "hash": digest}
                    def mutate(path):
                        if change == "source":
                            local.write_bytes(b"replaced during second stat")
                        elif change == "pause":
                            queue.request_control(row["id"], "pause")
                        elif change == "mapping":
                            queue.update_root(row["root_id"], "/Changed")
                        elif change == "intent":
                            job = queue.claim()
                            queue.save_commit_intent(job, digest)
                            queue.release(job)
                        else:
                            queue.request_control(row["id"], "remove")
                        return valid
                    report = reconcile(queue, ReadOnlyAPI({row["remote_path"]: [valid, mutate]}), stop=self.stop)
                    self.assertEqual(report["matched"], 0)
                    self.assertEqual(queue.stats()["existing"], 0)
                    self.assertEqual(queue.upload_history(row["id"]), [])

    def test_global_network_protocol_and_auth_errors_end_the_optional_pass_early(self):
        for number in range(8):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        rows = self.queue.list_jobs()
        for error, reason, auth in ((TransportError("offline"), "network", False),
                                    (ProtocolError("unknown protocol"), "protocol", False),
                                    (RemoteError(503), "remote", False),
                                    (RemoteError(403), "auth", True),
                                    (AuthError("expired"), "auth", True)):
            with self.subTest(error=type(error).__name__, reason=reason):
                api = ReadOnlyAPI({row["remote_path"]: error for row in rows})
                report = reconcile(self.queue, api, stop=self.stop)
                self.assertEqual((report["checked"], report["errors"], len(api.calls)), (1, 1, 1))
                self.assertEqual(report["aborted"], reason)
                self.assertIs(report["auth_required"], auth)
                self.assertEqual(self.queue.stats()["pending"], len(rows))

    def test_repeated_malformed_responses_do_not_generate_hundreds_of_thousands_of_requests(self):
        for number in range(8):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        api = ReadOnlyAPI({row["remote_path"]: {"size": row["size"], "hash": "invalid"}
                           for row in self.queue.list_jobs()})
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["errors"], len(api.calls)), (3, 3, 3))
        self.assertEqual(report["aborted"], "invalid_responses")

    def test_local_error_samples_are_capped_and_progress_is_a_snapshot(self):
        for number in range(130):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        objects = self.objects()
        updates = []
        def progress(value):
            updates.append(value)
            value["error_samples"].clear()  # A callback cannot mutate internal accounting.
        with patch("mailru_photos.reconcile.cloud_hash", side_effect=SourceReadError("unreadable")):
            report = reconcile(self.queue, ReadOnlyAPI(objects), stop=self.stop, progress=progress)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (130, 0, 130))
        self.assertEqual(len(report["error_samples"]), 100)
        self.assertEqual(updates[-1]["errors"], 130)
        self.assertEqual(self.queue.stats()["pending"], 130)

    def test_cancellation_before_or_during_stat_does_not_change_queue(self):
        self.photo()
        self.queue.scan()
        before = self.queue.list_jobs()
        api = ReadOnlyAPI(self.objects())
        self.stop.set()
        report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(api.calls, [])
        self.stop.clear()
        row = before[0]
        valid = self.objects()[row["remote_path"]]
        def stopped(path):
            self.stop.set()
            return valid
        api = ReadOnlyAPI({row["remote_path"]: stopped})
        with patch("mailru_photos.reconcile.cloud_hash", side_effect=AssertionError("hash started after cancel")):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["errors"], 0)
        self.assertEqual(self.queue.list_jobs(), before)
        self.stop.clear()
        report = reconcile(self.queue, ReadOnlyAPI({row["remote_path"]: Cancelled("pause")}), stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["errors"], 0)

    def test_actual_streaming_hash_observes_cancellation_between_blocks(self):
        self.photo(contents=b"source block" * 200000)
        self.queue.scan()
        api = ReadOnlyAPI(self.objects())
        original_check = api_module.check_cancelled
        checks = 0
        def stop_during_hash(event):
            nonlocal checks
            checks += 1
            if checks == 2:
                self.stop.set()
            original_check(event)
        with patch.object(api_module, "check_cancelled", side_effect=stop_during_hash):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertTrue(report["cancelled"])
        self.assertEqual(report["matched"], 0)
        self.assertEqual(len(api.calls), 1)
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_database_failure_propagates_instead_of_becoming_a_skipped_file(self):
        self.photo()
        self.queue.scan()
        with patch.object(self.queue, "mark_remote_existing", side_effect=sqlite3.OperationalError("database unavailable")):
            with self.assertRaises(sqlite3.OperationalError):
                reconcile(self.queue, ReadOnlyAPI(self.objects()), stop=self.stop)
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_unbound_account_and_live_upload_are_rejected_before_cloud_lookups(self):
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

    def test_keyset_pages_stay_bounded_as_matches_leave_the_candidate_index(self):
        for number in range(1201):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        api = ReadOnlyAPI(self.objects())
        fetch = self.queue.reconcile_candidates
        pages = []
        def bounded_page(*, after_id=0, limit=500):
            self.assertLessEqual(limit, 500)
            rows = fetch(after_id=after_id, limit=limit)
            self.assertTrue(all(row["id"] > after_id for row in rows))
            pages.append((after_id, len(rows)))
            return rows
        with patch.object(self.queue, "reconcile_candidates", side_effect=bounded_page):
            report = reconcile(self.queue, api, stop=self.stop)
        self.assertEqual((report["checked"], report["matched"], report["errors"]), (1201, 1201, 0))
        self.assertEqual([size for _, size in pages], [500, 500, 201, 0])
        self.assertEqual(self.queue.stats()["existing"], 1201)
        self.assertEqual(self.queue.stats()["uploaded"], 0)


if __name__ == "__main__":
    unittest.main()

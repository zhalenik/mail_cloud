"""Behavior tests for interruption, rescan, identity and concurrent claims."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import unittest

from mailru_photos.queue import (
    FileChangedError, Queue, QueueBusyError, QueueConfigurationError, QueueError,
    ScanCollisionError, StaleClaimError,
)


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.db = self.base / "queue.sqlite3"
        self.queue = Queue(self.db, destination="/Archive", account_id="test-account")
        self.queue.add_root(self.source, "camera")

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def photo(self, name="photo.jpg", contents=b"small-test-photo", root=None):
        path = (root or self.source) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def test_multiple_roots_preserve_paths_and_rescan_is_idempotent(self):
        other = self.base / "other"
        other.mkdir()
        self.queue.add_root(other, "old_phone")
        self.photo("2020/photo.jpg")
        self.photo("2020/photo.jpg", root=other)
        self.photo("ignored.txt")
        first = self.queue.scan()
        second = self.queue.scan()
        self.assertTrue(first.complete)
        self.assertEqual((first.added, second.added, second.unchanged), (2, 0, 2))
        self.assertEqual({r["remote_path"] for r in self.queue.list_jobs()}, {
            "/Archive/camera/2020/photo.jpg", "/Archive/old_phone/2020/photo.jpg",
        })
        self.assertEqual(self.queue.stats()["total"], 2)

    def test_database_cannot_be_reused_for_another_destination_or_account(self):
        with self.assertRaises(QueueConfigurationError):
            Queue(self.db, destination="/Other", account_id="test-account")
        with self.assertRaises(QueueConfigurationError):
            self.queue.bind_account("another-account")
        with self.assertRaises(QueueConfigurationError):
            self.queue.set_metadata("_account_id", "another-account")
        self.assertEqual(self.queue.account_id, "test-account")

    def test_reopening_initialized_queue_commits_no_changes(self):
        self.photo()
        self.queue.scan()
        version_before = self.queue._db.execute("PRAGMA data_version").fetchone()[0]
        for account in (None, "test-account"):
            with self.subTest(account=account), Queue(self.db, destination="/Archive", account_id=account) as other:
                self.assertEqual(other.stats()["total"], 1)
                self.assertEqual(other.account_id, "test-account")
                self.assertEqual(other._db.total_changes, 0)
                self.assertEqual(other._db.execute("PRAGMA synchronous").fetchone()[0], 2)
                self.assertEqual(other._db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(self.queue._db.execute("PRAGMA data_version").fetchone()[0], version_before)

    def test_reader_can_open_and_validate_queue_while_another_connection_is_writing(self):
        self.photo()
        self.queue.scan()

        def read():
            with Queue(self.db, destination="/Archive", account_id="test-account") as other:
                return other.stats()["total"]

        def reject_destination():
            with self.assertRaises(QueueConfigurationError):
                Queue(self.db, destination="/Other", account_id="test-account")

        def reject_account():
            with self.assertRaises(QueueConfigurationError):
                Queue(self.db, destination="/Archive", account_id="other-account")

        with ThreadPoolExecutor(max_workers=1) as executor:
            # Release the transaction before waiting for the executor on a
            # failure, so a regression fails promptly rather than taking 30 s.
            with self.queue._transaction():
                self.assertEqual(executor.submit(read).result(timeout=2), 1)
                executor.submit(reject_destination).result(timeout=2)
                executor.submit(reject_account).result(timeout=2)

    def test_older_schema_one_queue_installs_missing_objects_once(self):
        self.photo()
        self.queue.scan()
        self.queue._db.execute("DROP INDEX jobs_status")
        self.queue._db.execute("DROP TABLE commit_intents")
        with Queue(self.db, destination="/Archive", account_id="test-account") as upgraded:
            self.assertEqual(upgraded.stats()["pending"], 1)
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 1)
            names = {row[0] for row in upgraded._db.execute("SELECT name FROM sqlite_master")}
            self.assertIn("jobs_status", names)
            self.assertIn("commit_intents", names)
        after_upgrade = self.queue._db.execute("PRAGMA data_version").fetchone()[0]
        with Queue(self.db, destination="/Archive"):
            pass
        self.assertEqual(self.queue._db.execute("PRAGMA data_version").fetchone()[0], after_upgrade)

    def test_account_binding_is_persisted_on_first_open_with_account(self):
        other_path = self.base / "unbound.sqlite3"
        with Queue(other_path, destination="/New") as unbound:
            self.assertIsNone(unbound.account_id)
        with Queue(other_path, destination="/New", account_id="first-account") as bound:
            self.assertEqual(bound.account_id, "first-account")
        with self.assertRaises(QueueConfigurationError):
            Queue(other_path, destination="/New", account_id="different-account")
        with Queue(other_path, destination="/New") as reopened:
            self.assertEqual(reopened.account_id, "first-account")

    def test_unsupported_schema_is_rejected_without_altering_it(self):
        self.queue._db.execute("PRAGMA user_version=999")
        with self.assertRaisesRegex(QueueConfigurationError, "Unsupported queue schema"):
            Queue(self.db, destination="/Archive")
        self.assertEqual(self.queue._db.execute("PRAGMA user_version").fetchone()[0], 999)

    def test_filtered_pages_and_summary_preserve_order_without_loading_evidence(self):
        for n in range(6):
            self.photo(f"{n}.jpg")
        self.queue.scan()
        first = self.queue.claim()
        self.queue.mark_uploaded(first, remote_hash="example", verified_metadata={"proof": "x" * 10000})
        second = self.queue.claim()
        self.queue.mark_failed(second, "test failure", retry_delay=None)
        all_rows = self.queue.list_jobs()
        uploaded = self.queue.list_jobs("uploaded", summary=True)
        self.assertEqual([row["id"] for row in uploaded], [first.id])
        self.assertNotIn("verified_metadata", uploaded[0])
        self.assertNotIn("remote_hash", uploaded[0])
        self.assertEqual(uploaded[0]["size"], first.size)
        self.assertIn("proof", all_rows[0]["verified_metadata"])
        pending = self.queue.list_jobs("pending", limit=2, offset=1, summary=True)
        self.assertEqual([row["id"] for row in pending], [row["id"] for row in all_rows[3:5]])
        self.assertEqual(self.queue.list_jobs("failed", limit=2, offset=1), [])
        self.assertEqual([row["id"] for row in self.queue.list_jobs(limit=2, offset=2)],
                         [row["id"] for row in all_rows[2:4]])

    def test_root_alias_and_overlap_cannot_overwrite_another_mapping(self):
        nested = self.source / "nested"
        nested.mkdir()
        other = self.base / "other"
        other.mkdir()
        with self.assertRaises(ScanCollisionError):
            self.queue.add_root(other, "CAMERA")
        with self.assertRaises(QueueConfigurationError):
            self.queue.add_root(nested, "nested")
        with self.assertRaises(QueueConfigurationError):
            self.queue.add_root(self.source, "new_alias")
        self.assertEqual(self.queue.add_root(self.source, "camera").id, 1)

    def test_concurrent_connections_never_claim_same_job(self):
        for n in range(80):
            self.photo(f"{n}.jpg")
        self.queue.scan()

        def claim_all(_):
            claims = []
            with Queue(self.db, destination="/Archive", account_id="test-account") as queue:
                while (job := queue.claim()) is not None:
                    claims.append(job.id)
            return claims

        with ThreadPoolExecutor(max_workers=4) as executor:
            ids = [job_id for group in executor.map(claim_all, range(4)) for job_id in group]
        self.assertEqual(len(ids), 80)
        self.assertEqual(len(set(ids)), 80)

    def test_recovery_does_not_lose_success_and_rejects_stale_completion(self):
        self.photo("a.jpg")
        self.photo("b.jpg")
        self.queue.scan()
        done = self.queue.claim()
        self.queue.mark_uploaded(done, remote_hash="verified-a", verified_metadata={"mode": "confirmed-test"})
        interrupted = self.queue.claim()
        with self.queue.exclusive_run():
            self.assertEqual(self.queue.recover_running(), 1)
            with self.assertRaises(StaleClaimError):
                self.queue.mark_uploaded(interrupted, remote_hash="too-late")
            recovered = self.queue.claim()
            self.assertEqual(recovered.id, interrupted.id)
            self.assertNotEqual(recovered.claim_token, interrupted.claim_token)
            self.queue.mark_uploaded(recovered, remote_hash="verified-b")
        self.assertEqual(self.queue.stats()["uploaded"], 2)
        self.assertEqual(len(self.queue.upload_history(done.id)), 1)

    def test_an_active_runner_blocks_recovery_from_another_connection(self):
        with self.queue.exclusive_run():
            with Queue(self.db, destination="/Archive") as other:
                with self.assertRaises(QueueBusyError):
                    other.recover_running()

    def test_source_changed_during_upload_is_not_marked_successful(self):
        path = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        path.write_bytes(b"new-and-different-file")
        with self.assertRaises(FileChangedError):
            self.queue.mark_uploaded(job, remote_hash="hash-of-old-content")
        self.assertEqual(self.queue.stats()["pending"], 1)
        self.assertEqual(self.queue.upload_history(job.id), [])
        updated = self.queue.claim()
        self.assertEqual(updated.size, path.stat().st_size)
        self.queue.mark_uploaded(updated, remote_hash="new-content-hash")

    def test_source_changed_since_scan_is_refreshed_before_claim(self):
        path = self.photo()
        self.queue.scan()
        path.write_bytes(b"fresh-version")
        job = self.queue.claim()
        self.assertEqual(job.size, len(b"fresh-version"))
        self.assertTrue(self.queue.source_matches(job))

    def test_modified_uploaded_file_is_rescheduled_with_history_retained(self):
        path = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="old-hash", verified_metadata={"unlimited": True})
        path.write_bytes(b"modified-picture-with-different-size")
        result = self.queue.scan()
        self.assertEqual(result.changed, 1)
        self.assertEqual(self.queue.stats()["pending"], 1)
        row = self.queue.list_jobs()[0]
        self.assertIsNone(row["remote_hash"])
        self.assertEqual(self.queue.upload_history(job.id)[0]["remote_hash"], "old-hash")

    def test_missing_file_is_not_uploaded_or_falsely_reported_successful(self):
        first = self.photo("first.jpg")
        second = self.photo("second.jpg")
        self.queue.scan()
        first.unlink()
        job = self.queue.claim()
        self.assertEqual(job.local_path, str(second))
        self.queue.mark_uploaded(job)
        self.assertEqual(self.queue.stats()["skipped"], 1)
        self.assertEqual(self.queue.stats()["uploaded"], 1)

    def test_complete_rescan_marks_missing_and_restored_file_returns_to_queue(self):
        path = self.photo()
        self.queue.scan()
        saved = path.read_bytes()
        path.unlink()
        result = self.queue.scan()
        self.assertEqual(result.missing, 1)
        self.assertEqual(self.queue.stats()["skipped"], 1)
        path.write_bytes(saved)
        self.queue.scan()
        self.assertEqual(self.queue.stats()["pending"], 1)

    def test_retry_time_and_permanent_failure_are_respected(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim(now=100)
        self.queue.mark_failed(job, "temporary", retry_delay=50, now=100)
        self.assertIsNone(self.queue.claim(now=149))
        retry = self.queue.claim(now=150)
        self.assertEqual(retry.attempts, 2)
        self.queue.mark_failed(retry, "permanent", retry_delay=None)
        self.assertIsNone(self.queue.claim(now=9999999999))
        self.assertEqual(self.queue.stats()["permanent_failures"], 1)
        self.assertEqual(self.queue.retry_failed(), 0)
        self.assertEqual(self.queue.retry_failed(include_permanent=True), 1)
        self.assertIsNotNone(self.queue.claim())

    def test_scan_cannot_invalidate_a_live_upload(self):
        self.photo()
        self.queue.scan()
        self.queue.claim()
        with self.assertRaises(QueueBusyError):
            self.queue.scan()

    def test_interrupted_scan_keeps_unseen_records_and_releases_scan_lock(self):
        for n in range(5):
            self.photo(f"{n}.jpg")
        self.queue.scan()
        walk = self.queue._walk_files

        def interrupted(root, extensions, result):
            for number, entry in enumerate(walk(root, extensions, result)):
                if number == 2:
                    raise KeyboardInterrupt()
                yield entry

        self.queue._walk_files = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.queue.scan(batch_size=1)
        self.assertEqual(self.queue.stats()["pending"], 5)
        self.assertFalse(self.queue.stats()["scan_active"])
        self.queue._walk_files = walk
        self.assertTrue(self.queue.scan().complete)

    def test_case_collision_is_reported_and_never_silently_overwrites(self):
        if os.name == "nt":
            self.skipTest("Windows filesystem ordinarily cannot create this fixture")
        self.photo("photo.jpg")
        self.photo("PHOTO.jpg")
        result = self.queue.scan()
        self.assertFalse(result.complete)
        self.assertEqual(result.error_count, 1)
        self.assertEqual(self.queue.stats()["total"], 1)

    def test_symlink_directory_is_not_followed(self):
        outside = self.base / "outside"
        outside.mkdir()
        self.photo(root=outside)
        try:
            (self.source / "linked").symlink_to(outside, target_is_directory=True)
        except OSError:
            self.skipTest("Symlink creation is unavailable")
        result = self.queue.scan()
        self.assertEqual(result.ignored_symlinks, 1)
        self.assertEqual(self.queue.stats()["total"], 0)

    def test_metadata_evidence_survives_reopening(self):
        self.queue.set_metadata("capability", {"unlimited": True, "checked_at": 123})
        with Queue(self.db, destination="/Archive") as other:
            self.assertEqual(other.get_metadata("capability"), {"unlimited": True, "checked_at": 123})

    def test_commit_intent_survives_crash_and_is_cleared_after_confirmed_commit(self):
        self.photo()
        self.queue.scan()
        first = self.queue.claim()
        self.assertIsNone(self.queue.get_commit_intent(first))
        self.queue.save_commit_intent(first, "abc123")
        self.queue.recover_running()
        resumed = self.queue.claim()
        intent = self.queue.get_commit_intent(resumed)
        self.assertEqual(intent["cloud_hash"], "abc123")
        self.assertEqual(intent["remote_path"], resumed.remote_path)
        self.assertEqual(intent["account_id"], "test-account")
        self.queue.mark_uploaded(resumed, remote_hash="abc123")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)
        self.assertEqual(self.queue.upload_history(first.id)[0]["remote_hash"], "abc123")

    def test_changed_source_cannot_inherit_a_previous_commit_intent(self):
        path = self.photo()
        self.queue.scan()
        first = self.queue.claim()
        self.queue.save_commit_intent(first, "old-hash")
        self.queue.mark_failed(first, "ambiguous response", retry_delay=0)
        path.write_bytes(b"changed-after-ambiguous-upload")
        resumed = self.queue.claim()
        self.assertIsNone(self.queue.get_commit_intent(resumed))
        self.queue.save_commit_intent(resumed, "new-hash")
        self.assertEqual(self.queue.get_commit_intent(resumed)["cloud_hash"], "new-hash")

    def test_unlimited_confirmation_requires_matching_intent_and_survives_recovery(self):
        self.photo()
        self.queue.scan()
        first = self.queue.claim()
        with self.assertRaises(QueueError):
            self.queue.confirm_unlimited_commit(first, "abc123")
        self.queue.save_commit_intent(first, "abc123")
        before = self.queue.get_commit_intent(first)
        self.assertNotIn("unlimited_confirmed", before)
        with self.assertRaises(QueueError):
            self.queue.confirm_unlimited_commit(first, "different-hash")
        self.assertEqual(self.queue.get_commit_intent(first), before)
        self.queue.confirm_unlimited_commit(first, "ABC123")
        with Queue(self.db, destination="/Archive", account_id="test-account") as other:
            confirmation = other.get_commit_intent(first)
        self.assertIs(confirmation["unlimited_confirmed"], True)
        self.assertGreater(confirmation["unlimited_confirmed_at"], 0)
        self.assertEqual(confirmation["cloud_hash"], "abc123")
        self.queue.recover_running()
        resumed = self.queue.claim()
        with self.assertRaises(StaleClaimError):
            self.queue.confirm_unlimited_commit(first, "abc123")
        self.assertEqual(self.queue.get_commit_intent(resumed), confirmation)
        self.queue.mark_uploaded(resumed, remote_hash="abc123",
                                 verified_metadata={"is_unlim_upload": True, "unlimited_evidence": "saved_commit_response"})
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)
        self.assertIs(self.queue.upload_history(resumed.id)[0]["verified_metadata"]["is_unlim_upload"], True)

    def test_unlimited_confirmation_rejects_mismatched_scope_and_fingerprint(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "same-hash")
        original = self.queue.get_commit_intent(job)
        changes = {"account_id": "other-account", "destination": "/Elsewhere",
                   "remote_path": "/Archive/other/photo.jpg", "source_size": job.size + 1,
                   "source_mtime_ns": job.mtime_ns + 1, "device": job.device + 1,
                   "inode": job.inode + 1, "cloud_hash": "other-hash"}
        for key, value in changes.items():
            with self.subTest(field=key):
                mutated = {**original, key: value}
                self.queue._db.execute("UPDATE commit_intents SET intent_json=? WHERE job_id=?",
                                       (json.dumps(mutated), job.id))
                with self.assertRaises(QueueError):
                    self.queue.confirm_unlimited_commit(job, "same-hash")
                raw = self.queue._db.execute("SELECT intent_json FROM commit_intents WHERE job_id=?", (job.id,)).fetchone()[0]
                self.assertNotIn("unlimited_confirmed", json.loads(raw))
        self.queue._db.execute("UPDATE commit_intents SET intent_json=?,source_size=? WHERE job_id=?",
                               (json.dumps(original), job.size + 1, job.id))
        with self.assertRaises(QueueError):
            self.queue.confirm_unlimited_commit(job, "same-hash")

    def test_new_add_intent_clears_previous_unlimited_confirmation_for_identical_file(self):
        from unittest.mock import patch

        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "same-hash")
        self.queue.confirm_unlimited_commit(job, "same-hash")
        confirmed = self.queue.get_commit_intent(job)
        self.assertIs(confirmed["unlimited_confirmed"], True)
        self.assertIn("unlimited_confirmed_at", confirmed)

        # The remote object disappeared, so the API starts a NEW add for the
        # unchanged source/path/hash. A lost or negative response to this add
        # must never inherit the earlier add's positive billing confirmation.
        new_attempt_at = confirmed["created_at"] + 5
        with patch("mailru_photos.queue.time.time", return_value=new_attempt_at):
            self.queue.save_commit_intent(job, "same-hash")
        fresh = self.queue.get_commit_intent(job)
        self.assertNotIn("unlimited_confirmed", fresh)
        self.assertNotIn("unlimited_confirmed_at", fresh)
        self.assertEqual(fresh["created_at"], new_attempt_at)
        for key in ("cloud_hash", "remote_path", "source_size", "source_mtime_ns",
                    "device", "inode", "account_id", "destination"):
            self.assertEqual(fresh[key], confirmed[key])

        # Simulate losing the new add response and restarting. The durable
        # record stays unconfirmed until this attempt gets explicit evidence.
        self.queue.release(job, "Response to new add was lost")
        resumed = self.queue.claim()
        self.assertEqual(self.queue.get_commit_intent(resumed), fresh)
        self.queue.confirm_unlimited_commit(resumed, "same-hash")
        self.assertIs(self.queue.get_commit_intent(resumed)["unlimited_confirmed"], True)

    def test_changed_source_cannot_receive_unlimited_confirmation(self):
        path = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "old-hash")
        path.write_bytes(b"Changed source after the add response")
        with self.assertRaises(FileChangedError):
            self.queue.confirm_unlimited_commit(job, "old-hash")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)
        self.assertEqual(self.queue.list_jobs()[0]["status"], "pending")
        self.assertEqual(self.queue.stats()["uploaded"], 0)

    def test_unlimited_confirmation_rejects_corrupt_intent_json(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "same-hash")
        for malformed in ("{broken", "[]", "null"):
            with self.subTest(malformed=malformed):
                self.queue._db.execute("UPDATE commit_intents SET intent_json=? WHERE job_id=?", (malformed, job.id))
                with self.assertRaises(QueueError):
                    self.queue.confirm_unlimited_commit(job, "same-hash")

    def test_windows_sized_128_bit_inode_round_trips_without_sqlite_overflow(self):
        from unittest.mock import patch

        path = self.photo()
        self.queue.scan()
        value = path.stat()
        inode = (1 << 100) + 123
        self.queue._db.execute("UPDATE jobs SET inode=?", (str(inode),))
        with patch("mailru_photos.queue._snapshot", return_value=(value.st_size, value.st_mtime_ns, value.st_dev, inode)):
            job = self.queue.claim()
            self.assertEqual(job.inode, inode)
            self.queue.save_commit_intent(job, "sample-hash")
            self.assertEqual(self.queue.get_commit_intent(job)["inode"], inode)
            self.queue.mark_uploaded(job, remote_hash="sample-hash")

    def test_release_and_recovery_preserve_intent_without_exhausting_retry_limit(self):
        self.photo()
        self.queue.scan()
        first = self.queue.claim(max_attempts=1)
        self.queue.save_commit_intent(first, "saved-hash")
        self.queue.release(first)
        second = self.queue.claim(max_attempts=1)
        self.assertIsNotNone(second)
        self.assertEqual(second.attempts, 1)
        self.assertEqual(self.queue.get_commit_intent(second)["cloud_hash"], "saved-hash")
        self.queue.recover_running()
        third = self.queue.claim(max_attempts=1)
        self.assertIsNotNone(third)
        self.assertEqual(third.attempts, 1)
        self.assertEqual(self.queue.get_commit_intent(third)["cloud_hash"], "saved-hash")


if __name__ == "__main__":
    unittest.main()

"""Behavior tests for interruption, rescan, identity and concurrent claims."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.queue import (
    FileChangedError, Queue, QueueBusyError, QueueConfigurationError, QueueError,
    MEDIA_EXTENSIONS, PHOTO_EXTENSIONS, VIDEO_EXTENSIONS, ScanCollisionError, StaleClaimError,
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

    def sparse_media(self, name, size):
        path = self.photo(name)
        with path.open("r+b") as stream:
            stream.truncate(size)
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

    def test_default_scan_discovers_nested_photos_and_videos_case_insensitively(self):
        for suffix in MEDIA_EXTENSIONS:
            self.photo('nested/clip' + suffix.upper())
        self.photo('ignored.mp3')
        self.photo('ignored.txt')
        first = self.queue.scan()
        self.assertTrue(first.complete)
        self.assertEqual(first.added, len(MEDIA_EXTENSIONS))
        self.assertEqual(self.queue.stats()['pending'], len(MEDIA_EXTENSIONS))
        self.assertEqual({Path(job['relative_path']).suffix.casefold() for job in self.queue.list_jobs()},
                         MEDIA_EXTENSIONS)
        second = self.queue.scan()
        self.assertEqual((second.added, second.unchanged), (0, len(MEDIA_EXTENSIONS)))

    def test_explicit_photo_filter_remains_photo_only_until_expanded(self):
        self.photo('original.JPG')
        self.photo('camera/clip.MOV')
        self.photo('camera/clip.MP4')
        self.assertEqual(self.queue.scan(PHOTO_EXTENSIONS).added, 1)
        original_id = self.queue.list_jobs()[0]['id']
        expanded = self.queue.scan(MEDIA_EXTENSIONS)
        self.assertEqual((expanded.added, expanded.unchanged), (2, 1))
        self.assertEqual(self.queue.list_jobs()[0]['id'], original_id)
        self.assertEqual(self.queue.stats()['total'], 3)
        self.assertTrue({'.mp4', '.mov', '.mkv', '.webm', '.avi', '.3gp'} <= VIDEO_EXTENSIONS)

    def test_media_upgrade_preserves_uploaded_jobs_and_history_and_rescans_once(self):
        self.photo('done.jpg')
        self.photo('nested/new.MOV')
        previous = self.queue.scan(PHOTO_EXTENSIONS)
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash='offline-confirmed-photo',
                                 verified_metadata={'is_unlim_upload': True})
        self.queue.set_metadata('extensions', sorted(suffix.lstrip('.').upper() for suffix in PHOTO_EXTENSIONS))
        self.queue.set_metadata('last_scan', asdict(previous))
        jobs_before = self.queue.list_jobs()
        history_before = self.queue.upload_history(job.id)
        with self.queue.exclusive_run():
            self.assertTrue(self.queue.upgrade_media_discovery())
        self.assertEqual(self.queue.discovery_extensions(), MEDIA_EXTENSIONS)
        self.assertEqual(self.queue.list_jobs(), jobs_before)
        self.assertEqual(self.queue.upload_history(job.id), history_before)
        self.assertFalse(self.queue.get_metadata('last_scan')['complete'])
        self.assertEqual(self.queue.get_metadata('last_scan')['reason'], 'media_formats_added')
        with Queue(self.db, destination='/Archive') as observer:
            version = observer._db.execute('PRAGMA data_version').fetchone()[0]
            with self.queue.exclusive_run():
                for _ in range(3):
                    self.assertFalse(self.queue.upgrade_media_discovery())
            self.assertEqual(observer._db.execute('PRAGMA data_version').fetchone()[0], version)
        current = self.queue.scan(self.queue.discovery_extensions())
        self.assertEqual((current.added, current.unchanged), (1, 1))
        self.queue.set_metadata('last_scan', asdict(current))
        self.assertEqual(self.queue.stats()['uploaded'], 1)
        self.assertEqual(self.queue.stats()['pending'], 1)
        self.assertEqual(self.queue.list_jobs('uploaded')[0]['id'], job.id)
        self.assertEqual(self.queue.upload_history(job.id), history_before)
        with self.queue.exclusive_run():
            self.assertFalse(self.queue.upgrade_media_discovery())
        self.assertTrue(self.queue.get_metadata('last_scan')['complete'])

    def test_media_upgrade_handles_legacy_complete_scan_without_saved_filter(self):
        self.photo('old.jpg')
        self.queue.set_metadata('last_scan', asdict(self.queue.scan(PHOTO_EXTENSIONS)))
        with self.queue.exclusive_run():
            self.assertTrue(self.queue.upgrade_media_discovery())
            self.assertFalse(self.queue.upgrade_media_discovery())
        self.assertEqual(set(self.queue.get_metadata('extensions')), MEDIA_EXTENSIONS)
        self.assertFalse(self.queue.get_metadata('last_scan')['complete'])

    def test_fresh_queue_needs_no_discovery_migration_write(self):
        before = self.queue._db.total_changes
        with self.queue.exclusive_run():
            self.assertFalse(self.queue.upgrade_media_discovery())
            self.assertFalse(self.queue.upgrade_media_discovery())
        self.assertEqual(self.queue._db.total_changes, before)
        self.assertEqual(self.queue.discovery_extensions(), MEDIA_EXTENSIONS)
        self.assertIsNone(self.queue.get_metadata('last_scan'))

    def test_media_upgrade_preserves_custom_filters_including_explicit_old_default(self):
        for extensions, custom in [([' JPG ', 'MP4'], None), (sorted(PHOTO_EXTENSIONS), True)]:
            with self.subTest(extensions=extensions):
                self.queue.set_metadata('extensions', extensions)
                if custom is not None:
                    self.queue.set_metadata('extensions_custom', custom)
                self.queue.set_metadata('last_scan', {'complete': True})
                before = self.queue._db.total_changes
                with self.queue.exclusive_run():
                    self.assertFalse(self.queue.upgrade_media_discovery())
                self.assertEqual(self.queue._db.total_changes, before)
                self.assertTrue(self.queue.get_metadata('last_scan')['complete'])
                expected = PHOTO_EXTENSIONS if custom else frozenset({'.jpg', '.mp4'})
                self.assertEqual(self.queue.discovery_extensions(), expected)
                self.assertEqual(self.queue.get_metadata('extensions'), extensions)

    def test_media_upgrade_filter_and_scan_invalidation_are_atomic(self):
        self.queue.set_metadata('extensions', sorted(PHOTO_EXTENSIONS))
        self.queue.set_metadata('last_scan', {'complete': True})
        original = self.queue._put_meta
        def fail_invalidation(key, value):
            if key == 'last_scan':
                raise OSError('offline interrupted migration')
            return original(key, value)
        with self.queue.exclusive_run(), patch.object(self.queue, '_put_meta', side_effect=fail_invalidation):
            with self.assertRaises(OSError):
                self.queue.upgrade_media_discovery()
        self.assertEqual(self.queue.get_metadata('extensions'), sorted(PHOTO_EXTENSIONS))
        self.assertTrue(self.queue.get_metadata('last_scan')['complete'])

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
        self.queue.mark_uploaded(done, remote_hash="verified-a",
                                 verified_metadata={"storage_mode": "unlimited", "proof": "confirmed-test"})
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

    def test_storage_mode_migration_preserves_legacy_intents_and_read_only_reopens(self):
        self.photo()
        self.queue.scan()
        original = self.queue.claim()
        self.queue.save_commit_intent(original, "old-hash")
        self.queue.confirm_unlimited_commit(original, "old-hash")
        legacy = self.queue.get_commit_intent(original)
        legacy.pop("storage_mode")
        self.queue._db.execute("UPDATE commit_intents SET intent_json=?", (json.dumps(legacy),))
        self.queue.release(original)
        for name in ("jobs_pending_unlimited", "jobs_pending_regular", "jobs_retry_unlimited", "jobs_retry_regular"):
            self.queue._db.execute(f"DROP INDEX {name}")
        self.queue._db.execute("ALTER TABLE jobs DROP COLUMN storage_mode")
        with Queue(self.db, destination="/Archive", account_id="test-account") as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertIsNone(upgraded.list_jobs(summary=True)[0]["storage_mode"])
            resumed = upgraded.claim(storage_mode="unlimited")
            self.assertIsNone(resumed.storage_mode)
            restored = upgraded.get_commit_intent(resumed)
            self.assertEqual(restored, {"storage_mode": "unlimited", **legacy})
            with self.assertRaises(QueueError):
                upgraded.confirm_regular_commit(resumed, "old-hash")
            upgraded.confirm_unlimited_commit(resumed, "old-hash")
            upgraded.mark_uploaded(resumed, remote_hash="old-hash")
            self.assertEqual(upgraded.list_jobs(summary=True)[0]["storage_mode"], "unlimited")
            before = upgraded._db.execute("PRAGMA data_version").fetchone()[0]
            with Queue(self.db, destination="/Archive", account_id="test-account") as reopened:
                self.assertEqual(reopened._db.total_changes, 0)
                self.assertEqual(reopened.stats()["uploaded"], 1)
            self.assertEqual(upgraded._db.execute("PRAGMA data_version").fetchone()[0], before)

    def test_claim_size_partitions_use_exact_binary_boundary(self):
        for name, size in (("below.mp4", UNLIMITED_MAX_BYTES - 1),
                           ("at.mp4", UNLIMITED_MAX_BYTES),
                           ("above.mp4", UNLIMITED_MAX_BYTES + 1)):
            self.sparse_media(name, size)
        self.queue.scan()
        regular = self.queue.claim(storage_mode="regular")
        self.assertEqual(regular.relative_path, "above.mp4")
        self.assertIsNone(self.queue.claim(storage_mode="regular"))
        unlimited = [self.queue.claim(storage_mode="unlimited") for _ in range(2)]
        self.assertEqual({job.size for job in unlimited}, {UNLIMITED_MAX_BYTES - 1, UNLIMITED_MAX_BYTES})
        self.assertIsNone(self.queue.claim(storage_mode="unlimited"))
        self.assertIsNone(self.queue.claim())

    def test_partitioned_retry_claims_respect_due_time_and_attempt_limit(self):
        self.photo("small.jpg")
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        small = self.queue.claim(storage_mode="unlimited")
        large = self.queue.claim(storage_mode="regular")
        self.queue.mark_failed(small, "later", now=100, retry_delay=20)
        self.queue.mark_failed(large, "earlier", now=100, retry_delay=10)
        self.assertIsNone(self.queue.claim(storage_mode="regular", now=109))
        self.assertIsNone(self.queue.claim(storage_mode="unlimited", now=119))
        retried = self.queue.claim(storage_mode="regular", now=110, max_attempts=2)
        self.assertEqual((retried.id, retried.attempts), (large.id, 2))
        self.queue.mark_failed(retried, "again", now=110, retry_delay=0)
        self.assertIsNone(self.queue.claim(storage_mode="regular", now=120, max_attempts=2))
        self.assertEqual(self.queue.claim(storage_mode="unlimited", now=120).id, small.id)

    def test_partitioned_stats_include_all_statuses_and_retry_timing(self):
        fixtures = [
            ("small-pending.jpg", 10, "pending", 0),
            ("small-later.jpg", 11, "failed", 200),
            ("small-permanent.jpg", 12, "failed", None),
            ("small-done.jpg", 13, "uploaded", None),
            ("large-pending.mp4", UNLIMITED_MAX_BYTES + 1, "pending", 0),
            ("large-due.mp4", UNLIMITED_MAX_BYTES + 2, "failed", 90),
            ("large-later.mp4", UNLIMITED_MAX_BYTES + 3, "failed", 300),
            ("large-permanent.mp4", UNLIMITED_MAX_BYTES + 4, "failed", None),
            ("large-done.mp4", UNLIMITED_MAX_BYTES + 5, "uploaded", None),
        ]
        for name, size, _, _ in fixtures:
            self.sparse_media(name, size)
        self.queue.scan()
        for name, _, status, next_attempt in fixtures:
            self.queue._db.execute("UPDATE jobs SET status=?,next_attempt=? WHERE relative_path=?",
                                   (status, next_attempt, name))
        small = self.queue.stats(now=100, storage_mode="unlimited")
        large = self.queue.stats(now=100, storage_mode="regular")
        all_jobs = self.queue.stats(now=100)
        self.assertEqual((small["total"], large["total"], all_jobs["total"]), (4, 5, 9))
        self.assertEqual((small["ready"], large["ready"], all_jobs["ready"]), (1, 2, 3))
        self.assertEqual((small["next_retry_at"], large["next_retry_at"]), (200, 300))
        self.assertEqual((small["permanent_failures"], large["permanent_failures"]), (1, 1))
        for key in ("uploaded", "total_bytes", "uploaded_bytes", "permanent_failures"):
            self.assertEqual(all_jobs[key], small[key] + large[key])
        for status in all_jobs["bytes_by_status"]:
            self.assertEqual(all_jobs["bytes_by_status"][status],
                             small["bytes_by_status"][status] + large["bytes_by_status"][status])

    def test_source_change_moves_size_partition_and_clears_saved_mode_and_proof(self):
        path = self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        original = self.queue.claim(storage_mode="regular")
        self.queue.save_commit_intent(original, "old-hash", storage_mode="regular")
        self.queue.confirm_regular_commit(original, "old-hash")
        self.queue.release(original)
        path.write_bytes(b"now-small")
        self.assertIsNone(self.queue.claim(storage_mode="regular"))
        updated = self.queue.claim(storage_mode="unlimited")
        self.assertEqual(updated.size, len(b"now-small"))
        self.assertIsNone(updated.storage_mode)
        self.assertIsNone(self.queue.get_commit_intent(updated))

    def test_regular_confirmation_is_durable_separate_and_never_claims_unlimited(self):
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        first = self.queue.claim(storage_mode="regular")
        with self.assertRaises(QueueError):
            self.queue.confirm_regular_commit(first, "abc123")
        self.queue.save_commit_intent(first, "abc123", storage_mode="regular")
        with self.assertRaises(QueueError):
            self.queue.confirm_unlimited_commit(first, "abc123")
        with self.assertRaises(QueueError):
            self.queue.confirm_regular_commit(first, "wrong-hash")
        self.queue.confirm_regular_commit(first, "ABC123")
        confirmed = self.queue.get_commit_intent(first)
        self.assertEqual(confirmed["storage_mode"], "regular")
        self.assertIs(confirmed["regular_confirmed"], True)
        self.assertGreater(confirmed["regular_confirmed_at"], 0)
        self.assertNotIn("unlimited_confirmed", confirmed)
        self.queue.recover_running()
        resumed = self.queue.claim(storage_mode="regular")
        self.assertEqual(resumed.storage_mode, "regular")
        self.assertEqual(self.queue.get_commit_intent(resumed), confirmed)
        with self.assertRaises(StaleClaimError):
            self.queue.confirm_regular_commit(first, "abc123")
        self.queue.mark_uploaded(resumed, remote_hash="abc123", verified_metadata={
            "storage_mode": "regular", "regular_evidence": "saved_commit_response",
        })
        self.assertEqual(self.queue.list_jobs(summary=True)[0]["storage_mode"], "regular")
        result = self.queue.upload_history(first.id)[0]["verified_metadata"]
        self.assertEqual(result["storage_mode"], "regular")
        self.assertNotIn("is_unlim_upload", result)
        self.assertNotIn("unlimited_evidence", result)
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)

    def test_regular_confirmation_rejects_changed_scope_and_cross_mode_proof(self):
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        job = self.queue.claim(storage_mode="regular")
        self.queue.save_commit_intent(job, "same-hash", storage_mode="regular")
        original = self.queue.get_commit_intent(job)
        changes = {"account_id": "other-account", "destination": "/Other", "storage_mode": "unlimited",
                   "remote_path": "/Other/file.mp4", "source_size": job.size + 1,
                   "source_mtime_ns": job.mtime_ns + 1, "device": job.device + 1,
                   "inode": job.inode + 1, "cloud_hash": "other-hash"}
        for key, value in changes.items():
            with self.subTest(field=key):
                self.queue._db.execute("UPDATE commit_intents SET intent_json=? WHERE job_id=?",
                                       (json.dumps({**original, key: value}), job.id))
                with self.assertRaises(QueueError):
                    self.queue.confirm_regular_commit(job, "same-hash")
                saved = json.loads(self.queue._db.execute("SELECT intent_json FROM commit_intents").fetchone()[0])
                self.assertNotIn("regular_confirmed", saved)
        self.queue._db.execute("UPDATE commit_intents SET intent_json=?,source_size=? WHERE job_id=?",
                               (json.dumps(original), job.size + 1, job.id))
        with self.assertRaises(QueueError):
            self.queue.confirm_regular_commit(job, "same-hash")

    def test_new_intent_clears_confirmation_of_both_storage_modes(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        for previous_mode, new_mode in (("unlimited", "regular"), ("regular", "regular"), ("regular", "unlimited")):
            with self.subTest(previous=previous_mode, new=new_mode):
                self.queue.save_commit_intent(job, "same-hash", storage_mode=previous_mode)
                getattr(self.queue, f"confirm_{previous_mode}_commit")(job, "same-hash")
                self.queue.save_commit_intent(job, "same-hash", storage_mode=new_mode)
                fresh = self.queue.get_commit_intent(job)
                self.assertEqual(fresh["storage_mode"], new_mode)
                self.assertEqual(self.queue.list_jobs()[0]["storage_mode"], new_mode)
                for key in ("regular_confirmed", "regular_confirmed_at", "unlimited_confirmed", "unlimited_confirmed_at"):
                    self.assertNotIn(key, fresh)

    def test_regular_confirmation_source_change_and_rescan_clear_mode(self):
        path = self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        job = self.queue.claim(storage_mode="regular")
        self.queue.save_commit_intent(job, "old-hash", storage_mode="regular")
        path.write_bytes(b"different")
        with self.assertRaises(FileChangedError):
            self.queue.confirm_regular_commit(job, "old-hash")
        self.assertIsNone(self.queue.list_jobs()[0]["storage_mode"])
        retried = self.queue.claim()
        self.queue.mark_uploaded(retried, verified_metadata={"storage_mode": "unlimited"})
        path.write_bytes(b"changed again")
        self.queue.scan()
        self.assertIsNone(self.queue.list_jobs()[0]["storage_mode"])
        self.assertEqual(self.queue.upload_history(job.id)[0]["verified_metadata"]["storage_mode"], "unlimited")

    def test_regular_completion_rejects_conflicting_or_omitted_storage_mode(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "hash", storage_mode="regular")
        for evidence in ({}, {"storage_mode": "unlimited"}, {"storage_mode": "regular", "is_unlim_upload": True}):
            with self.subTest(evidence=evidence), self.assertRaises(QueueError):
                self.queue.mark_uploaded(job, remote_hash="hash", verified_metadata=evidence)
        self.assertEqual(self.queue.upload_history(job.id), [])
        self.assertEqual(self.queue.stats()["running"], 1)
        self.assertEqual(self.queue.get_commit_intent(job)["storage_mode"], "regular")

    def test_unknown_storage_modes_are_rejected_without_changing_queue(self):
        for mode in ("manual", "AUTO", "", False, [], {}):
            with self.subTest(mode=mode):
                with self.assertRaises(ValueError):
                    self.queue.claim(storage_mode=mode)
                with self.assertRaises(ValueError):
                    self.queue.stats(storage_mode=mode)
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        for mode in (None, "manual", "AUTO", [], {}):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.queue.save_commit_intent(job, "hash", storage_mode=mode)
        with self.assertRaises(ValueError):
            self.queue.mark_uploaded(job, verified_metadata={"storage_mode": "manual"})
        self.assertIsNone(self.queue.get_commit_intent(job))
        self.assertIsNone(self.queue.list_jobs()[0]["storage_mode"])

    def test_disabling_large_files_parks_only_live_retryable_work_and_preserves_evidence(self):
        names = ("pending.mp4", "retry.mp4", "permanent.mp4", "done.mp4", "active.mp4", "missing.mp4", "skip.mp4")
        for name in names:
            self.sparse_media(name, UNLIMITED_MAX_BYTES + 1)
        self.photo("small.jpg")
        self.queue.scan()
        claims = [self.queue.claim() for _ in range(len(names) + 1)]
        by_name = {job.relative_path: job for job in claims}
        for job in claims:
            if job.relative_path == "small.jpg":
                self.queue.release(job)
                continue
            self.queue.save_commit_intent(job, "hash-" + job.relative_path, storage_mode="regular")
            if job.relative_path in ("retry.mp4", "done.mp4"):
                self.queue.confirm_regular_commit(job, "hash-" + job.relative_path)
            if job.relative_path in ("pending.mp4", "missing.mp4"):
                self.queue.release(job)
            elif job.relative_path == "retry.mp4":
                self.queue.mark_failed(job, "response interrupted", retry_delay=1000)
            elif job.relative_path == "permanent.mp4":
                self.queue.mark_failed(job, "permanent account limit", retry_delay=None)
            elif job.relative_path == "done.mp4":
                self.queue.mark_uploaded(job, verified_metadata={"storage_mode": "regular"})
            elif job.relative_path == "skip.mp4":
                self.queue.mark_skipped(job, "unsupported codec")
        self.queue._db.execute("UPDATE jobs SET present=0 WHERE relative_path='missing.mp4'")
        before = {row["id"]: row for row in self.queue.list_jobs()}
        intents_before = list(self.queue._db.execute("SELECT job_id,intent_json FROM commit_intents ORDER BY job_id"))
        history_before = self.queue.upload_history(by_name["done.mp4"].id)
        self.assertEqual(self.queue.skip_large_disabled(), 2)
        self.assertEqual(self.queue.skip_large_disabled(), 0)
        parked_ids = {by_name["pending.mp4"].id, by_name["retry.mp4"].id}
        for row in self.queue.list_jobs():
            if row["id"] in parked_ids:
                self.assertEqual((row["status"], row["skip_reason"]), ("skipped", "large_files_disabled"))
                self.assertIsNone(row["next_attempt"])
            else:
                self.assertEqual(row, before[row["id"]])
        self.assertEqual(list(self.queue._db.execute("SELECT job_id,intent_json FROM commit_intents ORDER BY job_id")),
                         intents_before)
        self.assertEqual(self.queue.retry_large_skipped(), 2)
        self.assertEqual(self.queue.retry_large_skipped(), 0)
        for row in self.queue.list_jobs():
            if row["id"] in parked_ids:
                self.assertEqual((row["status"], row["attempts"], row["next_attempt"]), ("pending", 0, 0))
                self.assertIsNone(row["skip_reason"])
                self.assertIsNone(row["last_error"])
                self.assertEqual(row["storage_mode"], "regular")
            else:
                self.assertEqual(row, before[row["id"]])
        self.assertEqual(list(self.queue._db.execute("SELECT job_id,intent_json FROM commit_intents ORDER BY job_id")),
                         intents_before)
        self.assertEqual(self.queue.upload_history(by_name["done.mp4"].id), history_before)

    def test_large_file_skip_code_preserves_ambiguous_commit_through_restart(self):
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        job = self.queue.claim(storage_mode="regular")
        self.queue.save_commit_intent(job, "regular-hash", storage_mode="regular")
        self.queue.confirm_regular_commit(job, "regular-hash")
        original = self.queue.get_commit_intent(job)
        self.queue.mark_skipped(job, "regular uploads disabled", code="large_files_disabled")
        with Queue(self.db, destination="/Archive") as restarted:
            self.assertEqual(restarted.retry_large_skipped(), 1)
            resumed = restarted.claim(storage_mode="regular")
            self.assertEqual(restarted.get_commit_intent(resumed), original)
            self.assertEqual(resumed.attempts, 1)
            restarted.mark_skipped(resumed, "deliberate manual skip")
            self.assertEqual(restarted.retry_large_skipped(), 0)
            self.assertEqual(restarted._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)

    def test_retry_large_skipped_recognizes_only_exact_legacy_limit_and_explicit_code(self):
        legacy = "Файл больше 2 ГБ; он не отправлен обычной загрузкой."
        fixtures = [
            ("legacy.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "manual", legacy, 1),
            ("flag.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "large_files_disabled", "disabled", 1),
            ("other.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "manual", "Unsupported format", 1),
            ("similar.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "manual", legacy + " User skipped it.", 1),
            ("other-code.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "deliberate", legacy, 1),
            ("missing.mp4", UNLIMITED_MAX_BYTES + 1, "skipped", "large_files_disabled", "disabled", 0),
            ("done.mp4", UNLIMITED_MAX_BYTES + 1, "uploaded", "large_files_disabled", "disabled", 1),
            ("small.mp4", UNLIMITED_MAX_BYTES, "skipped", "manual", legacy, 1),
        ]
        for name, size, *_ in fixtures:
            self.sparse_media(name, size)
        self.queue.scan()
        for name, _, status, code, error, present in fixtures:
            self.queue._db.execute(
                "UPDATE jobs SET status=?,skip_reason=?,last_error=?,present=?,attempts=4 WHERE relative_path=?",
                (status, code, error, present, name),
            )
        self.assertEqual(self.queue.retry_large_skipped(), 2)
        self.assertEqual({row["relative_path"] for row in self.queue.list_jobs("pending")}, {"legacy.mp4", "flag.mp4"})
        self.assertEqual(self.queue.retry_large_skipped(), 0)
        for row in self.queue.list_jobs():
            self.assertEqual(row["attempts"], 0 if row["status"] == "pending" else 4)

    def test_partitioned_claim_cost_does_not_scan_an_unrelated_30000_job_prefix(self):
        target = self.photo("target.mp4")
        self.queue.scan()
        target_id = 30001
        self.queue._db.execute("UPDATE jobs SET id=?", (target_id,))
        with self.queue._transaction():
            self.queue._db.execute("""
                WITH RECURSIVE prefix(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM prefix WHERE n<30000
                )
                INSERT INTO jobs(id,root_id,relative_path,local_path,local_key,remote_path,remote_key,
                                 size,mtime_ns,device,inode,status,next_attempt,created_at,updated_at)
                SELECT n,1,'prefix-'||n||'.mp4','/fixture/prefix-'||n,'prefix-'||n,
                       '/Archive/camera/prefix-'||n,'/archive/camera/prefix-'||n,
                       0,0,0,'0','pending',0,0,0 FROM prefix
            """)
        for mode in ("unlimited", "regular"):
            target_size = 32 if mode == "unlimited" else UNLIMITED_MAX_BYTES + 1
            other_size = UNLIMITED_MAX_BYTES + 1 if mode == "unlimited" else 32
            with target.open("r+b") as stream:
                stream.truncate(target_size)
            snapshot = target.stat()
            for state in ("pending", "failed"):
                with self.subTest(mode=mode, state=state):
                    with self.queue._transaction():
                        self.queue._db.execute(
                            "UPDATE jobs SET size=?,status=?,claim_token=NULL,attempts=0,next_attempt=0 WHERE id<?",
                            (other_size, state, target_id),
                        )
                        self.queue._db.execute(
                            "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status=?,claim_token=NULL,"
                            "attempts=0,next_attempt=0 WHERE id=?",
                            (snapshot.st_size, snapshot.st_mtime_ns, snapshot.st_dev, str(snapshot.st_ino), state, target_id),
                        )
                    steps = 0
                    def count_steps():
                        nonlocal steps
                        steps += 1
                        return 0
                    self.queue._db.set_progress_handler(count_steps, 1000)
                    try:
                        claimed = self.queue.claim(storage_mode=mode, now=100)
                    finally:
                        self.queue._db.set_progress_handler(None, 0)
                    self.assertEqual(claimed.id, target_id)
                    self.assertLess(steps, 50, "Claim scanned jobs belonging to the other storage mode")


if __name__ == "__main__":
    unittest.main()

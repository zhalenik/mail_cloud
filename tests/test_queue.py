"""Behavior tests for interruption, rescan, identity and concurrent claims."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import errno
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.queue import (
    FileChangedError, Queue, QueueBusyError, QueueConfigurationError, QueueError,
    MEDIA_EXTENSIONS, PHOTO_EXTENSIONS, VIDEO_EXTENSIONS, SCHEMA_VERSION, ScanCollisionError, SourceUnavailableError, StaleClaimError,
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
        self.assertEqual(self.queue.get_metadata('last_scan'), {'complete': False, 'reason': 'source_added'})

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
        self.queue._db.execute("PRAGMA user_version=1")
        with Queue(self.db, destination="/Archive", account_id="test-account") as upgraded:
            self.assertEqual(upgraded.stats()["pending"], 1)
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
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

    def legacy_ledger(self, version=1):
        """Restore the pre-source-removal table shape without changing proof."""
        self.queue._db.execute("""CREATE TABLE legacy_upload_results (
            id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
            source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL,
            remote_hash TEXT, remote_id TEXT, verified_metadata TEXT, uploaded_at REAL NOT NULL
        )""")
        self.queue._db.execute("""INSERT INTO legacy_upload_results
            SELECT id,job_id,source_size,source_mtime_ns,remote_hash,remote_id,verified_metadata,uploaded_at
            FROM upload_results""")
        self.queue._db.execute("DROP TABLE upload_results")
        self.queue._db.execute("ALTER TABLE legacy_upload_results RENAME TO upload_results")
        self.queue._db.execute(f"PRAGMA user_version={version}")

    def test_legacy_ledger_migration_retains_actual_path_and_survives_source_removal(self):
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="verified-hash", remote_id="remote-object",
                                 verified_metadata={"path": "/ActualCloud/photo.jpg", "is_unlim_upload": True})
        self.legacy_ledger()
        before = dict(self.queue._db.execute("SELECT * FROM upload_results").fetchone())
        with Queue(self.db, destination="/Archive", account_id="test-account") as upgraded:
            history = upgraded.upload_history(job.id)[0]
            self.assertEqual(history["remote_path"], "/ActualCloud/photo.jpg")
            self.assertEqual(history["account_id"], "test-account")
            self.assertEqual(history["local_path"], str(source))
            self.assertIsNone(history["source_device"])
            self.assertIsNone(history["source_inode"])
            row = dict(upgraded._db.execute("SELECT * FROM upload_results").fetchone())
            self.assertEqual({key: row[key] for key in before}, before)
            self.assertEqual(upgraded.remove_root(job.root_id), 1)
            detached = dict(upgraded._db.execute("SELECT * FROM upload_results").fetchone())
            self.assertIsNone(detached.pop("job_id"))
            row.pop("job_id")
            self.assertEqual(detached, row)
            self.assertEqual(upgraded._db.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(upgraded.stats()["total"], 0)
            self.assertTrue(source.exists())
            upgraded.add_root(self.source, "camera")
            upgraded.scan()
            replacement = upgraded.claim()
            self.assertGreater(replacement.id, job.id)  # Stale task actions cannot address a new task.
            self.assertEqual(upgraded.upload_history(replacement.id), [])

    def test_schema_migration_rolls_back_tables_indexes_metadata_and_version_together(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="preserve-this-proof")
        self.legacy_ledger()
        before = [dict(row) for row in self.queue._db.execute("SELECT * FROM upload_results")]
        version = self.queue._db.execute("PRAGMA data_version").fetchone()[0]
        with patch.object(Queue, "_create_queue_indexes", side_effect=OSError("migration interrupted")):
            with self.assertRaisesRegex(OSError, "migration interrupted"):
                Queue(self.db, destination="/Archive", account_id="test-account")
        self.assertEqual(self.queue._db.execute("PRAGMA user_version").fetchone()[0], 1)
        self.assertEqual(self.queue._db.execute("PRAGMA data_version").fetchone()[0], version)
        columns = {row["name"]: row for row in self.queue._db.execute("PRAGMA table_info(upload_results)")}
        self.assertEqual(columns["job_id"]["notnull"], 1)
        self.assertNotIn("remote_path", columns)
        self.assertIsNone(self.queue._db.execute("SELECT 1 FROM sqlite_master WHERE name='upload_results_migration'").fetchone())
        self.assertEqual([dict(row) for row in self.queue._db.execute("SELECT * FROM upload_results")], before)
        with Queue(self.db, destination="/Archive") as recovered:
            self.assertEqual(recovered._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(recovered.upload_history(job.id)[0]["remote_hash"], "preserve-this-proof")

    def test_schema_zero_can_migrate_legacy_ledger_with_malformed_optional_evidence(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="still-preserved")
        self.legacy_ledger(version=0)
        self.queue._db.execute("UPDATE upload_results SET verified_metadata='invalid legacy JSON'")
        with Queue(self.db, destination="/Archive") as upgraded:
            row = upgraded._db.execute("SELECT * FROM upload_results").fetchone()
            self.assertEqual(row["verified_metadata"], "invalid legacy JSON")
            self.assertEqual(row["remote_path"], job.remote_path)
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)

    def test_explicit_source_destination_is_exact_and_idempotent(self):
        other = self.base / "other"
        other.mkdir()
        self.photo("nested/photo.jpg", root=other)
        root = self.queue.add_root(other, "label", remote_prefix="//CloudRoot//")
        self.assertEqual(root.remote_prefix, "/CloudRoot")
        self.assertTrue(self.queue.scan().complete)
        self.assertEqual(self.queue.list_jobs()[0]["remote_path"], "/CloudRoot/nested/photo.jpg")
        self.queue.set_metadata("last_scan", {"complete": True})
        before = self.queue._db.total_changes
        self.assertEqual(self.queue.add_root(other, "label"), root)
        self.assertEqual(self.queue.add_root(other, "label", "/CloudRoot"), root)
        self.assertEqual(self.queue.update_root(root.id, "/CloudRoot"), root)
        self.assertEqual(self.queue._db.total_changes, before)
        self.assertTrue(self.queue.get_metadata("last_scan")["complete"])
        with self.assertRaises(QueueConfigurationError):
            self.queue.add_root(other, "label", "/OtherDestination")
        updated = self.queue.update_root(root.id, "/", alias="new label")
        self.assertEqual(updated.alias, "new label")
        self.assertEqual(self.queue.list_jobs()[0]["remote_path"], "/nested/photo.jpg")
        self.assertTrue(self.queue.scan().complete)

    def test_source_destination_changes_only_unsent_tasks_and_preserves_commit_evidence(self):
        names = ("uploaded.jpg", "intent.jpg", "pending.jpg", "failed.jpg", "skipped.jpg")
        for name in names:
            self.photo(name)
        self.queue.scan()
        claimed = [self.queue.claim() for _ in names]
        by_name = {job.relative_path: job for job in claimed}
        for job in claimed:
            if job.relative_path == "uploaded.jpg":
                self.queue.mark_uploaded(job, remote_hash="verified", verified_metadata={"path": job.remote_path})
            elif job.relative_path == "intent.jpg":
                self.queue.save_commit_intent(job, "accepted")
                self.queue.confirm_unlimited_commit(job, "accepted")
                self.queue.mark_failed(job, "interrupted after add", retry_delay=None)
            elif job.relative_path == "failed.jpg":
                self.queue.mark_failed(job, "file error", retry_delay=None)
            elif job.relative_path == "skipped.jpg":
                self.queue.mark_skipped(job, "manual fixture")
            else:
                self.queue.release(job)
        before_history = self.queue.upload_history(by_name["uploaded.jpg"].id)
        before_intent = dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone())
        updated = self.queue.update_root(1, "/SelectedCloudFolder", alias="renamed label")
        self.assertEqual(updated.remote_prefix, "/SelectedCloudFolder")
        self.assertFalse(self.queue.get_metadata("last_scan")["complete"])
        for name, job in by_name.items():
            prefix = "/Archive/camera" if name in ("uploaded.jpg", "intent.jpg") else "/SelectedCloudFolder"
            self.assertEqual(self.queue.get_job(job.id)["remote_path"], prefix + "/" + name)
        self.assertEqual(self.queue.upload_history(by_name["uploaded.jpg"].id), before_history)
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), before_intent)
        self.assertTrue(self.queue.scan().complete)
        self.photo("new.jpg")
        self.assertTrue(self.queue.scan().complete)
        self.assertEqual(next(row for row in self.queue.list_jobs() if row["relative_path"] == "new.jpg")["remote_path"],
                         "/SelectedCloudFolder/new.jpg")
        self.assertEqual(self.queue.upload_history(by_name["uploaded.jpg"].id), before_history)

    def test_destination_rewrite_handles_intermediate_self_collision_but_rolls_back_real_collision(self):
        self.photo("x.jpg")
        self.photo("nested/x.jpg")
        self.queue.scan()
        self.queue.update_root(1, "/Archive/camera/nested")
        self.assertEqual({row["remote_path"] for row in self.queue.list_jobs()}, {
            "/Archive/camera/nested/x.jpg", "/Archive/camera/nested/nested/x.jpg",
        })
        claims = [self.queue.claim(), self.queue.claim()]
        for job in claims:
            if job.relative_path == "nested/x.jpg":
                self.queue.mark_uploaded(job, remote_hash="frozen")
            else:
                self.queue.release(job)
        before_rows = self.queue.list_jobs()
        before_root = self.queue.roots()
        before_scan = self.queue.get_metadata("last_scan")
        with self.assertRaises(ScanCollisionError):
            self.queue.update_root(1, "/Archive/camera/nested/nested")
        self.assertEqual(self.queue.list_jobs(), before_rows)
        self.assertEqual(self.queue.roots(), before_root)
        self.assertEqual(self.queue.get_metadata("last_scan"), before_scan)

    def test_source_removal_preserves_detached_evidence_originals_and_other_sources(self):
        original = self.photo("uploaded.jpg")
        pending = self.photo("pending.jpg")
        other = self.base / "other"
        other.mkdir()
        other_photo = self.photo(root=other)
        keep = self.queue.add_root(other, "keep", remote_prefix="/Keep")
        self.queue.scan()
        claimed = [self.queue.claim() for _ in range(3)]
        for job in claimed:
            if job.relative_path == "uploaded.jpg":
                self.queue.mark_uploaded(job, remote_hash="original-proof", verified_metadata={"path": job.remote_path})
                completed = job
            else:
                self.queue.release(job)
        snapshot = dict(self.queue._db.execute("SELECT * FROM upload_results").fetchone())
        self.assertEqual(snapshot["source_inode"], str(completed.inode))
        self.assertEqual(snapshot["account_id"], "test-account")
        self.assertEqual(self.queue.remove_root(1), 2)
        detached = dict(self.queue._db.execute("SELECT * FROM upload_results").fetchone())
        self.assertIsNone(detached.pop("job_id"))
        snapshot.pop("job_id")
        self.assertEqual(detached, snapshot)
        self.assertEqual(self.queue.roots(), [keep])
        self.assertEqual(self.queue.stats()["total"], 1)
        self.assertTrue(all(path.exists() for path in (original, pending, other_photo)))
        self.assertEqual(self.queue.get_metadata("last_scan")["reason"], "source_removed")
        with self.assertRaises(QueueConfigurationError):
            self.queue.remove_root(1)

    def test_all_source_mutations_refuse_active_runner_claim_and_scan(self):
        extra = self.base / "extra"
        extra.mkdir()
        self.photo()
        self.queue.scan()
        def mutations(queue):
            return (lambda: queue.add_root(extra, "extra"),
                    lambda: queue.update_root(1, "/New"),
                    lambda: queue.remove_root(1))
        with self.queue.exclusive_run(), Queue(self.db, destination="/Archive") as other:
            for change in mutations(other):
                with self.assertRaises(QueueBusyError):
                    change()
        job = self.queue.claim()
        for change in mutations(self.queue):
            with self.assertRaises(QueueBusyError):
                change()
        self.queue.release(job)
        self.queue._db.execute("INSERT INTO metadata(key,value) VALUES('_scan_token','\"fixture\"')")
        for change in mutations(self.queue):
            with self.assertRaises(QueueBusyError):
                change()
        self.queue._db.execute("DELETE FROM metadata WHERE key='_scan_token'")
        self.assertEqual(len(self.queue.roots()), 1)
        self.assertEqual(self.queue.stats()["total"], 1)

    def test_deleted_source_id_is_never_reused_after_restart_or_in_legacy_queue(self):
        self.photo()
        self.queue.scan()
        old = self.queue.roots()[0]
        # Model an existing database created before the source sequence was
        # introduced. Removal must preserve the high-water value lazily.
        self.queue._db.execute("DELETE FROM metadata WHERE key='_root_id_high_water'")
        self.queue.remove_root(old.id)
        with Queue(self.db, destination="/Archive") as reopened:
            current = reopened.add_root(self.source, "camera", remote_prefix="/New")
            self.assertGreater(current.id, old.id)
            reopened.scan()
            before_tasks = reopened.list_jobs()
            before_root = reopened.roots()
            for action in (lambda: reopened.update_root(old.id, "/StaleBrowser"),
                           lambda: reopened.remove_root(old.id)):
                with self.assertRaises(QueueConfigurationError):
                    action()
            self.assertEqual(reopened.list_jobs(), before_tasks)
            self.assertEqual(reopened.roots(), before_root)
            reopened.remove_root(current.id)
            newest = reopened.add_root(self.source, "camera")
            self.assertGreater(newest.id, current.id)

    def test_moved_pending_tasks_keep_ids_and_progress_reaches_one_hundred_percent(self):
        paths = [self.photo(f"{number}.jpg") for number in range(5)]
        self.queue.scan()
        before = {row["local_path"]: row["id"] for row in self.queue.list_jobs()}
        subfolder = self.source / "moved"
        subfolder.mkdir()
        for path in paths[:2]:
            path.rename(subfolder / path.name)
        result = self.queue.scan(batch_size=1)
        self.assertTrue(result.complete)
        self.assertEqual((result.added, result.moved, result.missing, result.unchanged), (0, 2, 0, 3))
        self.assertEqual(self.queue.stats()["total"], 5)
        for row in self.queue.list_jobs():
            self.assertEqual(row["id"], before[str(self.source / Path(row["local_path"]).name)])
            self.assertEqual(row["remote_path"], "/Archive/camera/" + row["relative_path"])
        while job := self.queue.claim():
            self.queue.mark_uploaded(job, remote_hash="offline-confirmed")
        current = self.queue.stats()
        self.assertEqual((current["uploaded"], current["total"], current["pending"]), (5, 5, 0))
        self.assertEqual(len(self.queue.list_jobs(include_missing=True)), 5)

    def test_moved_uploaded_task_keeps_actual_cloud_path_and_proof_until_source_changes(self):
        original = self.photo("before.jpg")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="original-hash", verified_metadata={"path": job.remote_path})
        history = self.queue.upload_history(job.id)
        relocated = self.source / "new.jpg"
        original.rename(relocated)
        result = self.queue.scan()
        self.assertEqual((result.moved, result.added, result.missing), (1, 0, 0))
        row = self.queue.get_job(job.id)
        self.assertEqual(row["relative_path"], "new.jpg")
        self.assertEqual(row["remote_path"], job.remote_path)
        self.assertEqual(row["status"], "uploaded")
        self.assertEqual(self.queue.upload_history(job.id), history)
        self.assertTrue(self.queue.scan().complete)
        self.assertIsNone(self.queue.claim())
        self.queue.update_root(1, "/AfterDestinationEdit")
        relocated.write_bytes(b"different file bytes")
        self.assertEqual(self.queue.scan().changed, 1)
        changed = self.queue.get_job(job.id)
        self.assertEqual(changed["status"], "pending")
        self.assertEqual(changed["remote_path"], "/AfterDestinationEdit/new.jpg")
        self.assertEqual(self.queue.upload_history(job.id), history)

    def test_move_never_transfers_a_commit_intent_to_a_new_remote_path(self):
        original = self.photo("before.jpg")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "uncertain-commit")
        self.queue.confirm_unlimited_commit(job, "uncertain-commit")
        self.queue.release(job)
        intent = dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone())
        original.rename(self.source / "after.jpg")
        result = self.queue.scan()
        self.assertEqual((result.added, result.moved, result.missing), (1, 0, 1))
        self.assertEqual(self.queue.stats()["total"], 1)
        self.assertEqual(self.queue.stats(include_missing=True)["total"], 2)
        self.assertIsNone(self.queue.get_job(job.id))
        self.assertEqual(self.queue.get_job(job.id, include_missing=True)["present"], 0)
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), intent)
        replacement = self.queue.claim()
        self.assertNotEqual(replacement.id, job.id)
        self.assertIsNone(self.queue.get_commit_intent(replacement))
        self.assertEqual(self.queue.upload_history(replacement.id), [])

    def test_source_change_after_destination_edit_cannot_erase_intent_on_collision(self):
        original = self.photo("photo.jpg")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "accepted-original")
        self.queue.confirm_unlimited_commit(job, "accepted-original")
        self.queue.release(job)
        self.queue.update_root(1, "/Conflict")
        other = self.base / "other"
        other.mkdir()
        self.photo("photo.jpg", root=other)
        self.queue.add_root(other, "other", remote_prefix="/Conflict")
        self.assertTrue(self.queue.scan().complete)
        intent = dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone())
        before = self.queue.get_job(job.id)
        original.write_bytes(b"source changed after its old upload was accepted")
        result = self.queue.scan()
        self.assertFalse(result.complete)
        self.assertEqual(result.error_count, 1)
        self.assertEqual(self.queue.get_job(job.id), before)
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), intent)
        # Claim-time detection records a local failure and moves on. It does
        # not delete the intent or strand a running claim on a UNIQUE error.
        remaining = self.queue.claim()
        self.assertEqual(remaining.local_path, str(other / "photo.jpg"))
        failed = self.queue.get_job(job.id)
        self.assertEqual(failed["status"], "failed")
        self.assertIsNone(failed["next_attempt"])
        self.assertEqual(failed["size"], before["size"])
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), intent)

    def test_source_change_during_claim_uses_current_destination_after_frozen_intent(self):
        source = self.photo()
        self.queue.scan()
        old = self.queue.claim()
        self.queue.save_commit_intent(old, "old-source-intent")
        self.queue.release(old)
        self.queue.update_root(1, "/Retargeted")
        source.write_bytes(b"this is a newer local source")
        current = self.queue.claim()
        self.assertEqual(current.id, old.id)
        self.assertEqual(current.remote_path, "/Retargeted/photo.jpg")
        self.assertEqual(current.size, source.stat().st_size)
        self.assertIsNone(self.queue.get_commit_intent(current))

    def test_hardlinks_are_independent_tasks_even_if_original_name_disappears(self):
        original = self.photo("original.jpg")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="original-hash")
        elsewhere = self.base / "hardlink.jpg"
        try:
            os.link(original, elsewhere)
        except OSError:
            self.skipTest("Hard links are unavailable")
        original.rename(self.source / "moved.jpg")
        result = self.queue.scan()
        self.assertEqual((result.added, result.moved, result.missing), (1, 0, 1))
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        self.assertEqual(self.queue.stats()["pending"], 1)
        self.assertEqual(self.queue.stats(include_missing=True)["uploaded"], 1)
        self.assertTrue(elsewhere.exists())

    def test_ambiguous_prior_inodes_do_not_adopt_an_uploaded_tasks_proof(self):
        first = self.photo("first.jpg")
        second = self.source / "second.jpg"
        try:
            os.link(first, second)
        except OSError:
            self.skipTest("Hard links are unavailable")
        self.queue.scan()
        done = self.queue.claim()
        self.queue.mark_uploaded(done, remote_hash="keep-with-old-task")
        first.rename(self.source / "third.jpg")
        second.unlink()
        result = self.queue.scan()
        self.assertEqual((result.added, result.moved, result.missing), (1, 0, 2))
        self.assertEqual(self.queue.stats()["total"], 1)
        self.assertEqual(self.queue.stats(include_missing=True)["total"], 3)
        self.assertEqual(self.queue.list_jobs()[0]["status"], "pending")
        self.assertEqual(self.queue.upload_history(done.id)[0]["remote_hash"], "keep-with-old-task")

    def test_current_views_exclude_missing_uploads_and_failed_history(self):
        keep = self.photo("keep.jpg")
        missing = self.photo("remove.jpg")
        self.queue.scan()
        claims = [self.queue.claim(), self.queue.claim()]
        for job in claims:
            self.queue.mark_uploaded(job, remote_hash="confirmed")
        removed_id = next(job.id for job in claims if job.local_path == str(missing))
        missing.unlink()
        self.queue.scan()
        self.assertEqual(self.queue.stats()["total"], 1)
        self.assertEqual(self.queue.stats()["uploaded_bytes"], keep.stat().st_size)
        self.assertEqual(self.queue.stats(include_missing=True)["total"], 2)
        self.assertIsNone(self.queue.get_job(removed_id))
        self.assertEqual(self.queue.get_job(removed_id, include_missing=True)["status"], "uploaded")
        self.assertEqual(len(self.queue.list_jobs("uploaded", summary=True)), 1)
        self.assertEqual(len(self.queue.list_jobs("uploaded", summary=True, include_missing=True)), 2)
        self.assertIsNone(self.queue.get_job(999999))

    def test_pause_survives_restart_and_resume_retries_only_that_task(self):
        self.photo("paused.jpg")
        self.photo("other.jpg")
        self.queue.scan()
        paused_id = next(row["id"] for row in self.queue.list_jobs() if row["relative_path"] == "paused.jpg")
        changed = self.queue.request_control(paused_id, "pause")
        self.assertIs(changed["paused"], True)
        self.assertEqual(changed["status"], "pending")
        other = self.queue.claim()
        self.assertEqual(other.relative_path, "other.jpg")
        self.queue.mark_uploaded(other, remote_hash="done")
        self.assertIsNone(self.queue.claim())
        stats = self.queue.stats()
        self.assertEqual((stats["pending"], stats["paused"], stats["remaining"], stats["ready"]), (1, 1, 0, 0))
        self.assertEqual((stats["processed"], stats["total"]), (1, 2))
        self.assertEqual(len(self.queue.list_jobs("pending")), stats["pending"])
        self.assertEqual([row["id"] for row in self.queue.list_jobs("paused", summary=True)], [paused_id])
        with Queue(self.db, destination="/Archive") as reopened:
            self.assertIsNone(reopened.claim())
            self.assertIs(reopened.get_job(paused_id)["paused"], True)
            reopened.request_control(paused_id, "resume")
            resumed = reopened.claim()
            self.assertEqual(resumed.id, paused_id)
            reopened.mark_failed(resumed, "file failure", retry_delay=None)
            reopened.request_control(paused_id, "resume")
            retried = reopened.claim()
            self.assertEqual((retried.id, retried.attempts), (paused_id, 1))

    def test_live_control_does_not_revoke_claim_and_is_read_only_to_worker(self):
        self.photo()
        self.queue.scan()
        with self.queue.exclusive_run():
            job = self.queue.claim()
            with Queue(self.db, destination="/Archive") as controller:
                requested = controller.request_control(job.id, "pause")
                self.assertEqual(requested["status"], "running")
                self.assertEqual(requested["claim_token"], job.claim_token)
                before = self.queue._db.total_changes
                self.assertEqual(self.queue.task_control(job), "pause")
                self.assertEqual(self.queue._db.total_changes, before)
                controller.request_control(job.id, "resume")
                self.assertIsNone(self.queue.task_control(job))
                controller.request_control(job.id, "remove")
                self.assertEqual(self.queue.task_control(job), "remove")
                self.assertIsNotNone(controller.get_job(job.id))
                with self.assertRaises(QueueBusyError):
                    controller.request_control(job.id, "resume")
            self.queue.release(job)
        self.assertIsNone(self.queue.get_job(job.id))
        with self.assertRaises(StaleClaimError):
            self.queue.task_control(job)

    def test_every_worker_finalizer_honors_requested_removal(self):
        names = ("release.jpg", "failed.jpg", "skipped.jpg", "uploaded.jpg")
        originals = [self.photo(name) for name in names]
        self.queue.scan()
        jobs = [self.queue.claim() for _ in names]
        for job in jobs:
            self.queue.save_commit_intent(job, "hash-" + job.relative_path)
            self.queue.request_control(job.id, "remove")
            if job.relative_path == "release.jpg":
                self.queue.release(job)
            elif job.relative_path == "failed.jpg":
                self.queue.mark_failed(job, "failure", retry_delay=30)
            elif job.relative_path == "skipped.jpg":
                self.queue.mark_skipped(job, "unsupported fixture")
            else:
                self.queue.confirm_unlimited_commit(job, "hash-" + job.relative_path)
                self.queue.mark_uploaded(job, remote_hash="hash-" + job.relative_path,
                                         verified_metadata={"path": job.remote_path, "is_unlim_upload": True})
            self.assertIsNone(self.queue.get_job(job.id, include_missing=True))
        self.assertEqual(self.queue.stats()["total"], 0)
        self.assertEqual(len(self.queue.commit_history(None)), 4)
        ledger = self.queue._db.execute("SELECT * FROM upload_results").fetchall()
        self.assertEqual(len(ledger), 1)
        self.assertIsNone(ledger[0]["job_id"])
        self.assertTrue(all(path.exists() for path in originals))
        self.assertEqual(self.queue._db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_pause_finalization_preserves_errors_and_late_success_is_complete(self):
        names = ("release.jpg", "failed.jpg", "skipped.jpg", "uploaded.jpg")
        for name in names:
            self.photo(name)
        self.queue.scan()
        jobs = [self.queue.claim() for _ in names]
        for job in jobs:
            self.queue.request_control(job.id, "pause")
            if job.relative_path == "release.jpg":
                self.queue.release(job)
            elif job.relative_path == "failed.jpg":
                self.queue.mark_failed(job, "permanent error", retry_delay=None)
            elif job.relative_path == "skipped.jpg":
                self.queue.mark_skipped(job, "unsupported fixture")
            else:
                self.queue.mark_uploaded(job, remote_hash="committed")
            row = self.queue.get_job(job.id)
            if job.relative_path == "uploaded.jpg":
                self.assertEqual(row["status"], "uploaded")
                self.assertIs(row["paused"], False)
                self.queue.request_control(job.id, "resume")
                self.assertEqual(self.queue.get_job(job.id)["status"], "uploaded")
            else:
                self.assertIs(row["paused"], True)
        self.assertIsNone(self.queue.claim())
        stats = self.queue.stats()
        self.assertEqual((stats["paused"], stats["remaining"], stats["ready"]), (3, 0, 0))
        self.assertEqual(len(self.queue.list_jobs("failed")), stats["failed"])
        self.assertIsNone(stats["next_retry_at"])

    def test_clear_finished_keeps_retryable_and_paused_work_without_rescanning(self):
        names = ("uploaded.jpg", "skipped.jpg", "permanent.jpg", "retry.jpg", "pending.jpg", "paused.jpg")
        originals = [self.photo(name) for name in names]
        self.queue.scan()
        self.queue.set_metadata("last_scan", {"complete": True, "fixture": "no automatic rescan"})
        previous_scan = self.queue.get_metadata("last_scan")
        jobs = [self.queue.claim() for _ in names]
        for job in jobs:
            if job.relative_path == "uploaded.jpg":
                self.queue.mark_uploaded(job, remote_hash="proof")
            elif job.relative_path == "skipped.jpg":
                self.queue.mark_skipped(job, "unsupported")
            elif job.relative_path == "permanent.jpg":
                self.queue.save_commit_intent(job, "uncertain")
                self.queue.mark_failed(job, "not retryable", retry_delay=None)
            elif job.relative_path == "retry.jpg":
                self.queue.mark_failed(job, "deferred", retry_delay=600, now=1000)
            else:
                self.queue.release(job)
                if job.relative_path == "paused.jpg":
                    self.queue.request_control(job.id, "pause")
        self.assertEqual(self.queue.clear_tasks("finished"), 3)
        self.assertEqual({row["relative_path"] for row in self.queue.list_jobs()}, {"pending.jpg", "retry.jpg", "paused.jpg"})
        stats = self.queue.stats(now=1000)
        self.assertEqual((stats["remaining"], stats["ready"], stats["paused"], stats["next_retry_at"]), (2, 1, 1, 1600))
        self.assertEqual(self.queue.get_metadata("last_scan"), previous_scan)
        self.assertEqual(self.queue.clear_tasks("all"), 3)
        self.assertEqual(self.queue.get_metadata("last_scan"), previous_scan)
        self.assertEqual(len(self.queue.roots()), 1)
        self.assertTrue(all(path.exists() for path in originals))
        self.assertIsNone(self.queue._db.execute("SELECT job_id FROM upload_results").fetchone()[0])
        self.assertEqual(self.queue.commit_history(None)[0]["stage"], "attempted")
        self.assertTrue(self.queue.scan().complete)
        self.assertGreater(min(row["id"] for row in self.queue.list_jobs()), max(job.id for job in jobs))
        with self.assertRaises(QueueConfigurationError):
            self.queue.request_control(jobs[0].id, "remove")
        self.assertEqual(self.queue.stats()["total"], len(names))

    def test_clear_and_task_changes_refuse_scan_or_external_run(self):
        self.photo()
        self.queue.scan()
        job_id = self.queue.list_jobs()[0]["id"]
        with self.queue.exclusive_run(), Queue(self.db, destination="/Archive") as other:
            with self.assertRaises(QueueBusyError):
                other.clear_tasks()
        job = self.queue.claim()
        with self.assertRaises(QueueBusyError):
            self.queue.clear_tasks()
        self.queue.release(job)
        self.queue._db.execute("INSERT INTO metadata(key,value) VALUES('_scan_token','\"fixture\"')")
        with self.assertRaises(QueueBusyError):
            self.queue.request_control(job_id, "remove")
        with self.assertRaises(QueueBusyError):
            self.queue.clear_tasks()
        self.queue._db.execute("DELETE FROM metadata WHERE key='_scan_token'")
        self.assertEqual(self.queue.stats()["total"], 1)

    def test_restart_recovery_honors_pause_and_remove_without_losing_receipts(self):
        names = ("pause.jpg", "remove.jpg", "resume.jpg")
        for name in names:
            self.photo(name)
        self.queue.scan()
        jobs = [self.queue.claim() for _ in names]
        by_name = {job.relative_path: job for job in jobs}
        for job in jobs:
            self.queue.save_commit_intent(job, "hash-" + job.relative_path)
        self.queue.confirm_unlimited_commit(by_name["remove.jpg"], "hash-remove.jpg")
        self.queue.request_control(by_name["pause.jpg"].id, "pause")
        self.queue.request_control(by_name["remove.jpg"].id, "remove")
        with Queue(self.db, destination="/Archive") as restarted:
            self.assertEqual(restarted.recover_running(), 3)
            self.assertEqual(restarted.get_job(by_name["pause.jpg"].id)["status"], "pending")
            self.assertIs(restarted.get_job(by_name["pause.jpg"].id)["paused"], True)
            self.assertIsNone(restarted.get_job(by_name["remove.jpg"].id))
            self.assertEqual(restarted.commit_history(None)[0]["stage"], "accepted")
            resumed = restarted.claim()
            self.assertEqual(resumed.id, by_name["resume.jpg"].id)
            self.assertEqual(resumed.attempts, 1)
            self.assertIsNone(restarted.claim())
            with self.assertRaises(StaleClaimError):
                self.queue.mark_failed(by_name["resume.jpg"], "stale worker")

    def test_unconfirmed_removed_attempt_is_not_a_success_or_new_task_evidence(self):
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "ambiguous-response")
        self.queue.request_control(job.id, "remove")
        self.queue.release(job)
        history = self.queue.commit_history(None)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["stage"], "attempted")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 0)
        self.assertTrue(source.exists())
        self.queue.scan()
        new = self.queue.claim()
        self.assertGreater(new.id, job.id)
        self.assertIsNone(self.queue.get_commit_intent(new))
        self.assertEqual(self.queue.commit_history(new.id), [])
        self.assertEqual(self.queue.upload_history(new.id), [])

    def test_accepted_receipt_is_committed_before_source_revalidation(self):
        from mailru_photos.queue import _snapshot
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "accepted-old-source")
        source.write_bytes(b"replaced after remote acceptance")
        observations = []
        def inspect(path):
            with Queue(self.db, destination="/Archive") as observer:
                evidence = observer.commit_history(job.id)
                self.assertEqual(len(evidence), 1)
                self.assertEqual(evidence[0]["stage"], "accepted")
                self.assertEqual(evidence[0]["source_size"], job.size)
                self.assertEqual(observer.upload_history(job.id), [])
                observations.append(evidence[0]["intent_json"])
            return _snapshot(path)
        with patch("mailru_photos.queue._snapshot", side_effect=inspect):
            with self.assertRaises(FileChangedError):
                self.queue.confirm_unlimited_commit(job, "accepted-old-source")
        self.assertEqual(len(observations), 1)
        self.assertEqual(self.queue.stats()["failed"], 1)
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.commit_history(job.id)[0]["intent_json"], observations[0])
        self.assertTrue(self.queue.scan().complete)
        self.assertIsNotNone(self.queue.claim())

    def test_verified_receipt_survives_remove_racing_before_local_finalization(self):
        from mailru_photos.queue import _snapshot
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "verified")
        self.queue.confirm_unlimited_commit(job, "verified")
        def inspect(path):
            with Queue(self.db, destination="/Archive") as controller:
                history = controller.upload_history(job.id)
                self.assertEqual(len(history), 1)
                self.assertEqual(history[0]["source_size"], job.size)
                self.assertEqual(history[0]["remote_path"], job.remote_path)
                controller.request_control(job.id, "remove")
                self.assertEqual(controller.get_job(job.id)["status"], "running")
            return _snapshot(path)
        with patch("mailru_photos.queue._snapshot", side_effect=inspect):
            self.queue.mark_uploaded(job, remote_hash="verified", verified_metadata={"path": job.remote_path})
        self.assertIsNone(self.queue.get_job(job.id))
        ledger = self.queue._db.execute("SELECT * FROM upload_results").fetchone()
        self.assertIsNone(ledger["job_id"])
        self.assertEqual(ledger["remote_hash"], "verified")
        self.assertEqual(self.queue.commit_history(None)[0]["stage"], "accepted")
        self.assertTrue(source.exists())

    def test_old_accepted_receipt_cannot_confirm_a_new_attempt_with_same_hash(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        with patch("mailru_photos.queue.time.time", return_value=1234):
            self.queue.save_commit_intent(job, "same-hash")
            self.queue.confirm_unlimited_commit(job, "same-hash")
            old_id = self.queue.get_commit_intent(job)["intent_id"]
            self.queue.save_commit_intent(job, "same-hash")
        fresh = self.queue.get_commit_intent(job)
        self.assertNotEqual(fresh["intent_id"], old_id)
        self.assertNotIn("unlimited_confirmed", fresh)
        self.queue.request_control(job.id, "remove")
        self.queue.release(job)
        history = self.queue.commit_history(None)
        self.assertEqual({row["stage"] for row in history}, {"attempted", "accepted"})
        self.assertEqual(len({row["intent_id"] for row in history}), 2)
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM upload_results").fetchone()[0], 0)

    def test_pause_is_preserved_through_source_change_rescan_and_destination_edit(self):
        source = self.photo()
        self.queue.scan()
        job_id = self.queue.list_jobs()[0]["id"]
        self.queue.request_control(job_id, "pause")
        source.write_bytes(b"new local bytes")
        self.queue.scan()
        self.queue.update_root(1, "/ChangedDestination")
        self.assertIs(self.queue.get_job(job_id)["paused"], True)
        self.assertEqual(self.queue.get_job(job_id)["remote_path"], "/ChangedDestination/photo.jpg")
        self.assertIsNone(self.queue.claim())

    def schema_two_controls_fixture(self):
        self.queue._db.execute("DROP TABLE commit_evidence")
        for name in ("jobs_pending", "jobs_pending_unlimited", "jobs_pending_regular", "jobs_retry",
                     "jobs_retry_unlimited", "jobs_retry_regular", "jobs_paused", "jobs_paused_created", "jobs_reconcile"):
            self.queue._db.execute(f"DROP INDEX {name}")
        self.queue._db.execute("ALTER TABLE jobs DROP COLUMN paused")
        self.queue._db.execute("ALTER TABLE jobs DROP COLUMN remove_requested")
        self.queue._db.execute("DELETE FROM metadata WHERE key='_job_id_high_water'")
        self.queue._db.execute("PRAGMA user_version=2")

    def test_schema_two_control_migration_is_atomic_and_keeps_existing_intents(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "legacy-confirmed")
        self.queue.confirm_unlimited_commit(job, "legacy-confirmed")
        self.queue.release(job)
        before = dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone())
        self.schema_two_controls_fixture()
        with patch.object(Queue, "_create_queue_indexes", side_effect=OSError("schema three interrupted")):
            with self.assertRaises(OSError):
                Queue(self.db, destination="/Archive")
        self.assertEqual(self.queue._db.execute("PRAGMA user_version").fetchone()[0], 2)
        self.assertNotIn("paused", {row["name"] for row in self.queue._db.execute("PRAGMA table_info(jobs)")})
        self.assertIsNone(self.queue.get_metadata("_job_id_high_water"))
        self.assertIsNone(self.queue._db.execute("SELECT 1 FROM sqlite_master WHERE name='commit_evidence'").fetchone())
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), before)
        with Queue(self.db, destination="/Archive") as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertIs(upgraded.get_job(job.id)["paused"], False)
            self.assertEqual(dict(upgraded._db.execute("SELECT * FROM commit_intents").fetchone()), before)
            self.assertEqual(upgraded.request_control(job.id, "pause")["paused"], True)
            with Queue(self.db, destination="/Archive") as reopened:
                self.assertEqual(reopened._db.total_changes, 0)
                self.assertEqual(reopened._db.execute("PRAGMA synchronous").fetchone()[0], 2)
                self.assertIsNone(reopened.claim())

    def test_detached_history_keeps_origin_ids_for_probe_resume(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "verified-sample")
        self.queue.confirm_unlimited_commit(job, "verified-sample")
        self.queue.mark_uploaded(job, remote_hash="verified-sample",
                                 verified_metadata={"path": job.remote_path, "is_unlim_upload": True})
        self.queue.set_metadata("probe", {"result": "in_progress", "active_job_id": job.id})
        self.queue.request_control(job.id, "remove")
        with Queue(self.db, destination="/Archive") as restarted:
            saved_id = restarted.get_metadata("probe")["active_job_id"]
            proof = restarted.upload_history(saved_id)
            accepted = restarted.commit_history(saved_id)
            self.assertEqual(len(proof), 1)
            self.assertEqual(proof[0]["origin_job_id"], job.id)
            self.assertIsNone(proof[0]["job_id"])
            self.assertEqual(proof[0]["source_size"], job.size)
            self.assertEqual(accepted[0]["stage"], "accepted")
            self.assertIsNone(accepted[0]["job_id"])
            self.assertEqual(accepted[0]["origin_job_id"], job.id)

    def test_schema_two_detached_records_never_receive_an_invented_origin(self):
        self.photo("first.jpg")
        self.photo("second.jpg")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="legacy-detached")
        self.queue.request_control(job.id, "remove")
        self.schema_two_controls_fixture()
        self.queue._db.execute("DROP INDEX upload_results_origin")
        self.queue._db.execute("ALTER TABLE upload_results DROP COLUMN origin_job_id")
        with Queue(self.db, destination="/Archive") as upgraded:
            self.assertEqual(upgraded.upload_history(job.id), [])
            detached = upgraded.upload_history(None)
            self.assertEqual(detached[0]["remote_hash"], "legacy-detached")
            self.assertIsNone(detached[0]["origin_job_id"])
            upgraded.scan()
            self.assertGreater(max(row["id"] for row in upgraded.list_jobs()), job.id)

    def test_source_removal_archives_unverified_intents_without_claiming_upload_success(self):
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "request-with-lost-response")
        self.queue.release(job)
        self.queue.remove_root(job.root_id)
        history = self.queue.commit_history(job.id)
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0]["job_id"])
        self.assertEqual(history[0]["stage"], "attempted")
        self.assertEqual(self.queue.upload_history(job.id), [])
        self.assertTrue(source.exists())

    def test_repeated_source_change_during_claim_cannot_starve_other_work(self):
        from mailru_photos.queue import _snapshot
        self.photo("one.jpg")
        self.photo("two.jpg")
        self.queue.scan()
        rows = self.queue.list_jobs()
        changing, stable = rows[0], rows[1]
        count = 0
        def continually_changed(path):
            nonlocal count
            original = _snapshot(path)
            if path == changing["local_path"]:
                count += 1
                return (original[0] + count, *original[1:])
            return original
        with patch("mailru_photos.queue._snapshot", side_effect=continually_changed):
            next_job = self.queue.claim()
        self.assertEqual(next_job.id, stable["id"])
        self.assertEqual(count, 2)
        blocked = self.queue.get_job(changing["id"])
        self.assertEqual(blocked["status"], "failed")
        self.assertEqual(blocked["skip_reason"], "source_changed")
        self.assertIsNone(blocked["next_attempt"])

    def test_claim_cost_skips_thirty_thousand_paused_or_removing_tasks(self):
        target = self.photo("target.jpg")
        self.queue.scan()
        target_id = 30001
        self.queue._db.execute("UPDATE jobs SET id=?", (target_id,))
        with self.queue._transaction():
            self.queue._db.execute("""
                WITH RECURSIVE prefix(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM prefix WHERE n<30000
                )
                INSERT INTO jobs(id,root_id,relative_path,local_path,local_key,remote_path,remote_key,
                                 size,mtime_ns,device,inode,status,next_attempt,created_at,updated_at,paused)
                SELECT n,1,'prefix-'||n||'.jpg','/fixture/prefix-'||n,'prefix-'||n,
                       '/Archive/camera/prefix-'||n,'/archive/camera/prefix-'||n,
                       0,0,0,'0','pending',0,0,0,1 FROM prefix
            """)
        for flag in ("paused", "remove_requested"):
            for state in ("pending", "failed"):
                for mode in (None, "unlimited"):
                    with self.subTest(flag=flag, state=state, mode=mode):
                        self.queue._db.execute(
                            "UPDATE jobs SET paused=?,remove_requested=?,status=?,next_attempt=0 WHERE id<?",
                            (int(flag == "paused"), int(flag == "remove_requested"), state, target_id),
                        )
                        self.queue._db.execute(
                            "UPDATE jobs SET status=?,attempts=0,next_attempt=0,claim_token=NULL WHERE id=?", (state, target_id),
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
                        self.assertLess(steps, 50, "Claim scanned the entire controlled task prefix")

    def test_existing_remote_observation_keeps_actual_path_through_move_and_mapping_changes(self):
        from mailru_photos.api import cloud_hash
        original = self.photo("original.jpg")
        self.queue.scan()
        candidate = self.queue.reconcile_candidates()[0]
        digest, _ = cloud_hash(original)
        self.assertTrue(self.queue.mark_remote_existing(candidate, remote_hash=digest))
        before = self.queue.get_job(candidate["id"])
        self.queue.update_root(1, "/NewDestination")
        self.assertEqual(self.queue.get_job(candidate["id"])["remote_path"], candidate["remote_path"])
        self.assertTrue(self.queue.scan().complete)
        moved = self.source / "moved.jpg"
        original.rename(moved)
        result = self.queue.scan()
        self.assertEqual((result.moved, result.added, result.missing), (1, 0, 0))
        current = self.queue.get_job(candidate["id"])
        self.assertEqual(current["remote_path"], candidate["remote_path"])
        self.assertEqual(current["local_path"], str(moved))
        self.assertEqual(current["verified_metadata"], before["verified_metadata"])
        self.assertEqual(self.queue.stats()["existing"], 1)
        self.assertIsNone(self.queue.claim())
        moved.write_bytes(b"changed after remote observation")
        self.assertEqual(self.queue.scan().changed, 1)
        updated = self.queue.get_job(candidate["id"])
        self.assertEqual(updated["remote_path"], "/NewDestination/moved.jpg")
        self.assertEqual(updated["status"], "pending")
        self.assertIsNone(updated["verified_metadata"])
        self.assertIsNone(updated["remote_hash"])
        self.assertEqual(self.queue.stats()["existing"], 0)
        self.assertEqual(self.queue.upload_history(candidate["id"]), [])

    def test_existing_observation_stays_complete_when_unchanged_original_returns(self):
        from mailru_photos.api import cloud_hash
        original = self.photo()
        self.queue.scan()
        candidate = self.queue.reconcile_candidates()[0]
        digest, _ = cloud_hash(original)
        self.queue.mark_remote_existing(candidate, remote_hash=digest)
        elsewhere = self.base / "temporarily-away.jpg"
        original.rename(elsewhere)
        self.queue.scan()
        self.assertEqual(self.queue.stats()["total"], 0)
        history = self.queue.get_job(candidate["id"], include_missing=True)
        self.assertEqual(history["skip_reason"], "remote_exists")
        elsewhere.rename(original)
        self.queue.scan()
        self.assertEqual(self.queue.stats()["existing"], 1)
        self.assertIsNone(self.queue.claim())
        before = self.queue.get_job(candidate["id"])
        self.queue.request_control(candidate["id"], "pause")
        self.queue.request_control(candidate["id"], "resume")
        self.assertEqual(self.queue.get_job(candidate["id"]), before)

    def test_optional_reconcile_index_is_installed_once_without_schema_version_change(self):
        self.photo()
        self.queue.scan()
        self.queue._db.execute("DROP INDEX jobs_reconcile")
        with Queue(self.db, destination="/Archive") as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(len(upgraded.reconcile_candidates()), 1)
            version = upgraded._db.execute("PRAGMA data_version").fetchone()[0]
            with Queue(self.db, destination="/Archive") as reopened:
                self.assertEqual(reopened._db.total_changes, 0)
                self.assertEqual(len(reopened.reconcile_candidates()), 1)
            self.assertEqual(upgraded._db.execute("PRAGMA data_version").fetchone()[0], version)

    def test_permission_and_io_failures_stay_visible_without_starving_other_tasks(self):
        from mailru_photos.queue import _snapshot
        names = ("permission.jpg", "io.jpg", "healthy.jpg")
        for name in names:
            self.photo(name)
        self.queue.scan()
        claimed = [self.queue.claim() for _ in names]
        by_name = {job.relative_path: job for job in claimed}
        for job in claimed:
            if job.relative_path != "healthy.jpg":
                self.queue.save_commit_intent(job, "hash-" + job.relative_path)
                if job.relative_path == "permission.jpg":
                    self.queue.confirm_unlimited_commit(job, "hash-" + job.relative_path)
            self.queue.release(job)
        intents = [dict(row) for row in self.queue._db.execute("SELECT * FROM commit_intents ORDER BY job_id")]
        sentinel = "private-error-message-sentinel"
        def inspect(path):
            if path == by_name["permission.jpg"].local_path:
                raise PermissionError(errno.EACCES, sentinel)
            if path == by_name["io.jpg"].local_path:
                raise OSError(errno.EIO, sentinel)
            return _snapshot(path)
        with patch("mailru_photos.queue._snapshot", side_effect=inspect):
            healthy = self.queue.claim()
            self.assertEqual(healthy.id, by_name["healthy.jpg"].id)
            self.queue.mark_uploaded(healthy, remote_hash="healthy-proof")
            self.assertIsNone(self.queue.claim())
        failures = self.queue.list_jobs("failed")
        self.assertEqual(len(failures), 2)
        self.assertEqual(self.queue.stats()["total"], 3)
        self.assertEqual(self.queue.stats()["permanent_failures"], 2)
        self.assertEqual(self.queue.stats()["uploaded"], 1)
        self.assertEqual([dict(row) for row in self.queue._db.execute("SELECT * FROM commit_intents ORDER BY job_id")], intents)
        for row in failures:
            self.assertEqual(row["present"], 1)
            self.assertIsNone(row["next_attempt"])
            self.assertNotEqual(row["skip_reason"], "missing")
            self.assertNotIn(sentinel, row["last_error"])
            self.assertIn("EACCES" if row["relative_path"] == "permission.jpg" else "EIO", row["last_error"])
            self.assertTrue(Path(row["local_path"]).exists())
        with Queue(self.db, destination="/Archive") as reopened:
            self.assertEqual(len(reopened.list_jobs("failed")), 2)
            self.assertEqual(reopened._db.total_changes, 0)
        failed_id = by_name["permission.jpg"].id
        self.queue.request_control(failed_id, "resume")
        retried = self.queue.claim()
        self.assertEqual(retried.id, failed_id)
        self.assertIs(self.queue.get_commit_intent(retried)["unlimited_confirmed"], True)

    def test_claim_honors_global_stop_between_inaccessible_or_missing_candidates(self):
        for number in range(5):
            self.photo(f"{number}.jpg")
        self.queue.scan()
        original_rows = self.queue.list_jobs()
        first_id = original_rows[0]["id"]
        for error in (PermissionError(errno.EACCES, "denied"), FileNotFoundError(errno.ENOENT, "missing")):
            with self.subTest(error=type(error).__name__):
                # Restore only the previous candidate's fixture state; other
                # tasks must remain completely untouched during both calls.
                self.queue._db.execute("UPDATE jobs SET status='pending',present=1,next_attempt=0,"
                                       "attempts=0,skip_reason=NULL,last_error=NULL WHERE id=?", (first_id,))
                stop = threading.Event()
                def inspect(path):
                    self.assertEqual(path, original_rows[0]["local_path"])
                    stop.set()
                    raise error
                with patch("mailru_photos.queue._snapshot", side_effect=inspect) as snapshot:
                    self.assertIsNone(self.queue.claim(stop=stop))
                self.assertEqual(snapshot.call_count, 1)
                for untouched in original_rows[1:]:
                    self.assertEqual(self.queue.get_job(untouched["id"]), untouched)
                before = self.queue._db.total_changes
                with patch("mailru_photos.queue._snapshot", side_effect=AssertionError("already stopped")):
                    self.assertIsNone(self.queue.claim(stop=stop))
                self.assertEqual(self.queue._db.total_changes, before)

    def test_inspection_failure_before_new_commit_preserves_previous_intent(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "same-hash")
        old = dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone())
        with patch("mailru_photos.queue._snapshot", side_effect=PermissionError(errno.EACCES, "no access")):
            with self.assertRaises(SourceUnavailableError):
                self.queue.save_commit_intent(job, "same-hash")
        self.assertEqual(dict(self.queue._db.execute("SELECT * FROM commit_intents").fetchone()), old)
        row = self.queue.get_job(job.id)
        self.assertEqual((row["status"], row["present"]), ("failed", 1))
        self.assertEqual(self.queue.upload_history(job.id), [])

    def test_accepted_commit_survives_inspection_permission_error_and_can_resume(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "server-accepted")
        with patch("mailru_photos.queue._snapshot", side_effect=PermissionError(errno.EACCES, "denied after reply")):
            with self.assertRaises(SourceUnavailableError):
                self.queue.confirm_unlimited_commit(job, "server-accepted")
        row = self.queue.get_job(job.id)
        self.assertEqual((row["status"], row["present"]), ("failed", 1))
        self.assertEqual(row["size"], job.size)
        receipt = self.queue.commit_history(job.id)[0]
        self.assertEqual(receipt["stage"], "accepted")
        self.assertEqual(receipt["source_size"], job.size)
        self.assertEqual(self.queue.upload_history(job.id), [])
        self.queue.request_control(job.id, "resume")
        resumed = self.queue.claim()
        self.assertIs(self.queue.get_commit_intent(resumed)["unlimited_confirmed"], True)
        self.queue.mark_uploaded(resumed, remote_hash="server-accepted", verified_metadata={"path": job.remote_path})
        self.assertEqual(self.queue.get_job(job.id)["status"], "uploaded")

    def test_verified_remote_proof_is_kept_when_final_source_inspection_fails(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "verified-before-io-error")
        self.queue.confirm_unlimited_commit(job, "verified-before-io-error")
        with patch("mailru_photos.queue._snapshot", side_effect=OSError(errno.EIO, "filesystem unavailable")):
            with self.assertRaises(SourceUnavailableError):
                self.queue.mark_uploaded(job, remote_hash="verified-before-io-error",
                                         verified_metadata={"path": job.remote_path, "stat_matched": True})
        row = self.queue.get_job(job.id)
        self.assertEqual((row["status"], row["present"]), ("failed", 1))
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        proof = self.queue.upload_history(job.id)[0]
        self.assertEqual(proof["remote_hash"], "verified-before-io-error")
        self.assertEqual(proof["source_size"], job.size)
        self.assertEqual(self.queue.commit_history(job.id)[0]["stage"], "accepted")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 1)

    def test_inspection_failure_honors_active_pause_or_remove_request(self):
        for action in ("pause", "remove"):
            with self.subTest(action=action):
                source = self.base / action
                source.mkdir()
                (source / "image.jpg").write_bytes(b"offline image bytes" * 10)
                with Queue(self.base / (action + ".sqlite3"), destination="/Archive", account_id="fixture") as queue:
                    queue.add_root(source, "camera")
                    queue.scan()
                    job = queue.claim()
                    queue.save_commit_intent(job, "accepted")
                    queue.confirm_unlimited_commit(job, "accepted")
                    queue.request_control(job.id, action)
                    with patch("mailru_photos.queue._snapshot", side_effect=OSError(errno.EIO, "offline")):
                        with self.assertRaises(SourceUnavailableError):
                            queue.mark_uploaded(job, remote_hash="accepted")
                    if action == "pause":
                        row = queue.get_job(job.id)
                        self.assertEqual((row["status"], row["present"]), ("failed", 1))
                        self.assertIs(row["paused"], True)
                    else:
                        self.assertIsNone(queue.get_job(job.id, include_missing=True))
                    self.assertEqual(queue.upload_history(job.id)[0]["remote_hash"], "accepted")
                    self.assertEqual(queue.commit_history(job.id)[0]["stage"], "accepted")

    def test_replacing_source_with_symlink_never_yields_a_claim_for_its_target(self):
        unsafe = self.photo("changed.jpg")
        healthy = self.photo("healthy.jpg")
        self.queue.scan()
        unsafe_id = next(row["id"] for row in self.queue.list_jobs() if row["local_path"] == str(unsafe))
        outside = self.base / "outside.jpg"
        outside.write_bytes(b"do not follow or upload" * 10)
        unsafe.unlink()
        try:
            unsafe.symlink_to(outside)
        except OSError:
            self.skipTest("Symlink creation is unavailable")
        job = self.queue.claim()
        self.assertEqual(job.local_path, str(healthy))
        self.queue.release(job)
        # Enumeration order is arbitrary; mark healthy done so every unsafe
        # pending candidate is inspected before asserting the final result.
        self.queue.mark_uploaded(self.queue.claim(), remote_hash="healthy")
        self.assertIsNone(self.queue.claim())
        row = self.queue.get_job(unsafe_id)
        self.assertEqual((row["status"], row["present"]), ("failed", 1))
        self.assertTrue(unsafe.is_symlink())
        self.assertEqual(outside.read_bytes(), b"do not follow or upload" * 10)
        self.assertEqual(self.queue.upload_history(unsafe_id), [])

    def test_enoent_and_enotdir_still_remove_only_truly_absent_sources_from_current_totals(self):
        absent = self.photo("absent.jpg")
        nested = self.photo("folder/nested.jpg")
        self.queue.scan()
        absent.unlink()
        nested.unlink()
        nested.parent.rmdir()
        nested.parent.write_bytes(b"parent is now a file")
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.stats()["total"], 0)
        self.assertEqual(self.queue.stats(include_missing=True)["total"], 2)
        for row in self.queue.list_jobs(include_missing=True):
            self.assertEqual((row["present"], row["skip_reason"]), (0, "missing"))

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
        self.assertEqual(self.queue.stats()["failed"], 1)
        self.assertEqual(self.queue.upload_history(job.id)[0]["source_size"], job.size)
        self.assertEqual(self.queue.upload_history(job.id)[0]["remote_hash"], "hash-of-old-content")
        self.assertIsNone(self.queue.claim())
        self.queue.request_control(job.id, "resume")
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
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.stats()["skipped"], 0)
        self.assertEqual(self.queue.stats(include_missing=True)["skipped"], 1)
        self.assertEqual(self.queue.stats()["uploaded"], 1)

    def test_complete_rescan_marks_missing_and_restored_file_returns_to_queue(self):
        path = self.photo()
        self.queue.scan()
        saved = path.read_bytes()
        path.unlink()
        result = self.queue.scan()
        self.assertEqual(result.missing, 1)
        self.assertEqual(self.queue.stats()["skipped"], 0)
        self.assertEqual(self.queue.stats(include_missing=True)["skipped"], 1)
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
        self.assertEqual(self.queue.list_jobs()[0]["status"], "failed")
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        receipt = self.queue.commit_history(job.id)[0]
        self.assertEqual(receipt["stage"], "accepted")
        self.assertEqual(receipt["source_size"], job.size)
        self.assertEqual(self.queue.upload_history(job.id), [])

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
        self.queue._db.execute("PRAGMA user_version=1")
        with Queue(self.db, destination="/Archive", account_id="test-account") as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
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
        self.assertTrue(self.queue.scan().complete)
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

    def test_scan_classifies_small_formats_and_oversized_files_before_upload(self):
        self.photo("Фото/снимок.JPG", b"photo")
        self.photo("Видео/ролик.MOV", b"video")
        self.photo("Видео/старый.M4V", b"unsupported")
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.assertTrue(self.queue.scan().complete)
        rows = {row["relative_path"]: row for row in self.queue.list_jobs(summary=True)}
        self.assertEqual(rows["Фото/снимок.JPG"]["planned_mode"], "unlimited")
        self.assertEqual(rows["Видео/ролик.MOV"]["planned_reason"], "account_confirmed_mov")
        self.assertEqual(rows["Видео/старый.M4V"]["planned_mode"], "regular")
        self.assertEqual(rows["large.mp4"]["planned_reason"], "size_limit")
        stats = self.queue.stats()
        self.assertEqual((stats["planned_unlimited_count"], stats["planned_unlimited_bytes"]), (2, 10))
        self.assertEqual((stats["planned_regular_count"], stats["planned_regular_bytes"]),
                         (2, UNLIMITED_MAX_BYTES + 1 + len(b"unsupported")))
        self.assertEqual(stats["uploaded_total_count"], 0)
        ordinary = self.queue.claim(storage_mode="regular")
        self.assertEqual(ordinary.planned_mode, "regular")
        self.queue.release(ordinary)
        self.assertEqual(self.queue.stats(storage_mode="regular")["total"], 2)

    def test_regular_policy_is_reversible_and_preserves_pause_errors_and_receipts(self):
        for name in ("ready.m4v", "paused.m4v", "retry.m4v", "permanent.m4v", "observed.m4v", "running.m4v"):
            self.photo(name)
        self.sparse_media("large.mp4", UNLIMITED_MAX_BYTES + 1)
        self.queue.scan()
        ids = {row["relative_path"]: row["id"] for row in self.queue.list_jobs()}
        self.queue.request_control(ids["paused.m4v"], "pause")
        retry = self.queue.claim(job_id=ids["retry.m4v"])
        self.queue.save_commit_intent(retry, "retry-hash", storage_mode="regular")
        self.queue.mark_failed(retry, "temporary", retry_delay=60)
        permanent = self.queue.claim(job_id=ids["permanent.m4v"])
        self.queue.mark_failed(permanent, "permanent", retry_delay=None)
        observed = self.queue.claim(job_id=ids["observed.m4v"])
        self.queue.record_cloud_observation(observed, mode="regular", remote_hash="remote-hash",
                                            metadata={"is_unlim_upload": False})
        self.queue.mark_failed(observed, "verification failure", retry_delay=60)
        running = self.queue.claim(job_id=ids["running.m4v"])
        self.assertEqual(self.queue.apply_regular_policy(False), {"skipped": 4, "resumed": 0})
        self.assertEqual(self.queue.get_job(ids["ready.m4v"])["skip_reason"], "regular_format_disabled")
        self.assertEqual(self.queue.get_job(ids["large.mp4"])["skip_reason"], "regular_size_disabled")
        self.assertTrue(self.queue.get_job(ids["paused.m4v"])["paused"])
        self.assertEqual(self.queue.get_job(permanent.id)["last_error"], "permanent")
        self.assertEqual(self.queue.get_job(observed.id)["status"], "failed")
        self.assertEqual(self.queue.get_job(running.id)["status"], "running")
        self.assertEqual(self.queue.stats()["planned_regular_count"], 6)
        self.assertEqual(self.queue.apply_regular_policy(False), {"skipped": 0, "resumed": 0})
        self.assertEqual(self.queue.apply_regular_policy(True), {"skipped": 0, "resumed": 4})
        self.assertTrue(self.queue.get_job(ids["paused.m4v"])["paused"])
        resumed = self.queue.claim(job_id=retry.id, storage_mode="regular")
        self.assertEqual(self.queue.get_commit_intent(resumed)["cloud_hash"], "retry-hash")
        self.queue.release(resumed)
        self.queue.release(running)

    def test_false_server_flag_is_a_quota_observation_and_an_error_not_a_success(self):
        self.photo(contents=b"unexpected quota")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(
            job, mode="regular", remote_hash="A" * 40,
            metadata={"is_unlim_upload": False, "verified": False, "size": job.size},
        )
        self.assertEqual(self.queue.get_job(job.id)["status"], "running")
        self.assertEqual(self.queue.upload_history(job.id), [])
        self.queue.mark_failed(job, "server returned false", retry_delay=None)
        self.assertEqual([row["id"] for row in self.queue.list_jobs("uploaded_regular")], [job.id])
        self.assertEqual([row["id"] for row in self.queue.list_jobs("failed")], [job.id])
        self.assertEqual(self.queue.list_jobs("uploaded"), [])
        self.assertEqual(self.queue.jobs_summary("uploaded_regular"), {"total": 1, "total_bytes": job.size})
        stats = self.queue.stats()
        self.assertEqual(stats["planned_unlimited_count"], 0)
        self.assertEqual(stats["uploaded_regular_bytes"], job.size)
        self.assertEqual(stats["uploaded_total_bytes"], job.size)
        self.assertEqual(stats["uploaded_bytes"], 0)
        with Queue(self.db, destination="/Archive") as reopened:
            reopened.request_control(job.id, "start")
            resumed = reopened.claim(job_id=job.id)
            receipt = reopened.get_commit_intent(resumed)["storage_observation"]
            self.assertEqual(receipt["mode"], "regular")
            self.assertIs(receipt["is_unlim_upload"], False)
            self.assertFalse(receipt["verified"])
            self.assertEqual(receipt["path"], job.remote_path)
            reopened.release(resumed)

    def test_absent_server_flag_stays_unknown_through_retry_and_filtering(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(job, mode="unknown", remote_hash="A" * 40)
        self.queue.mark_failed(job, "missing flag", retry_delay=None)
        stats = self.queue.stats()
        self.assertEqual((stats["uploaded_unknown_count"], stats["uploaded_regular_count"]), (1, 0))
        self.assertEqual(self.queue.jobs_summary("uploaded_regular")["total"], 0)
        self.assertEqual(self.queue.jobs_summary("uploaded_unknown")["total"], 1)
        self.queue.request_control(job.id, "start")
        resumed = self.queue.claim(job_id=job.id)
        self.assertEqual(self.queue.get_commit_intent(resumed)["storage_observation"]["mode"], "unknown")
        self.assertNotIn("is_unlim_upload", self.queue.get_commit_intent(resumed)["storage_observation"])
        with self.assertRaises(QueueError):
            self.queue.record_cloud_observation(resumed, mode="regular", remote_hash="A" * 40,
                                                metadata={"is_unlim_upload": True})
        self.queue.release(resumed)

    def test_cloud_delete_checks_object_identity_and_preserves_false_flag_history(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(job, mode="regular", remote_hash="A" * 40,
                                            metadata={"is_unlim_upload": False, "verified": True})
        self.queue.mark_failed(job, "unexpected quota", retry_delay=None)
        self.assertFalse(self.queue.mark_cloud_deleted(job.id, expected_hash="B" * 40))
        self.assertFalse(self.queue.mark_cloud_deleted(job.id, expected_hash="A" * 40, expected_path="/different"))
        self.assertEqual(self.queue.stats()["uploaded_regular_count"], 1)
        self.assertTrue(self.queue.mark_cloud_deleted(job.id, expected_hash="a" * 40, expected_path=job.remote_path))
        self.assertTrue(self.queue.mark_cloud_deleted(job.id, expected_hash="a" * 40, expected_path=job.remote_path))
        self.assertEqual([row["event"] for row in self.queue.cloud_history(job.id)], ["deleted", "stored"])
        self.assertEqual(self.queue.get_job(job.id)["status"], "failed")
        self.assertEqual(self.queue.list_jobs("uploaded_regular"), [])
        self.assertEqual(self.queue.stats()["planned_unlimited_count"], 1)
        self.queue.request_control(job.id, "start")
        resumed = self.queue.claim(job_id=job.id)
        self.assertNotIn("storage_observation", self.queue.get_commit_intent(resumed))
        self.queue.release(resumed)
        self.assertTrue(self.queue.delete_task_final(job.id))
        self.assertEqual(len(self.queue.cloud_history(None)), 2)
        self.assertEqual(len(self.queue.cloud_history(job.id)), 2)
        self.assertTrue(all(row["job_id"] is None for row in self.queue.cloud_history(job.id)))
        self.queue.scan()
        self.assertGreater(self.queue.list_jobs()[0]["id"], job.id)

    def test_completed_uploads_have_verified_observations_without_changing_legacy_history(self):
        self.photo("photo.jpg", b"photo")
        self.photo("video.m4v", b"video")
        self.queue.scan()
        unlimited = self.queue.claim(storage_mode="unlimited")
        self.queue.mark_uploaded(unlimited, remote_hash="A" * 40,
                                 verified_metadata={"storage_mode": "unlimited", "is_unlim_upload": True})
        regular = self.queue.claim(storage_mode="regular")
        self.queue.mark_uploaded(regular, remote_hash="B" * 40,
                                 verified_metadata={"storage_mode": "regular", "is_unlim_upload": False})
        stats = self.queue.stats()
        self.assertEqual((stats["uploaded_unlimited_count"], stats["uploaded_regular_count"]), (1, 1))
        self.assertEqual((stats["uploaded_total_bytes"], stats["uploaded_bytes"]), (10, 10))
        self.assertEqual((stats["planned_unlimited_bytes"], stats["planned_regular_bytes"]), (0, 0))
        self.assertTrue(self.queue.get_job(unlimited.id)["cloud_verified"])
        self.assertTrue(self.queue.get_job(regular.id)["cloud_verified"])
        self.assertEqual(len(self.queue.upload_history(unlimited.id)), 1)
        self.assertEqual(len(self.queue.upload_history(regular.id)), 1)

    def test_observed_failed_cloud_path_survives_source_move_and_retarget_without_intent(self):
        source = self.photo("before.jpg")
        self.photo("pending.jpg")
        self.queue.scan()
        rows = {row["relative_path"]: row for row in self.queue.list_jobs()}
        job = self.queue.claim(job_id=rows["before.jpg"]["id"])
        self.queue.record_cloud_observation(job, mode="regular", remote_hash="A" * 40)
        self.queue.mark_failed(job, "unexpected quota", retry_delay=None)
        self.queue.update_root(1, "/Новая папка")
        self.assertEqual(self.queue.get_job(job.id)["remote_path"], job.remote_path)
        moved = source.with_name("после.jpg")
        source.rename(moved)
        self.assertEqual(self.queue.scan().moved, 1)
        row = self.queue.get_job(job.id)
        self.assertEqual(row["relative_path"], "после.jpg")
        self.assertEqual(row["cloud_path"], job.remote_path)
        self.assertEqual(row["remote_path"], job.remote_path)
        self.assertEqual(self.queue.get_job(rows["pending.jpg"]["id"])["remote_path"], "/Новая папка/pending.jpg")

    def test_changed_source_resets_current_observation_but_keeps_original_receipt(self):
        source = self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.record_cloud_observation(job, mode="regular", remote_hash="A" * 40)
        self.queue.mark_failed(job, "unexpected quota", retry_delay=None)
        source.write_bytes(b"new source contents, with another size")
        self.assertEqual(self.queue.scan().changed, 1)
        row = self.queue.get_job(job.id)
        self.assertFalse(row["cloud_present"])
        self.assertIsNone(row["observed_mode"])
        self.assertEqual(self.queue.cloud_history(job.id)[0]["source_size"], job.size)
        self.assertEqual(self.queue.stats()["uploaded_regular_count"], 0)
        self.assertEqual(self.queue.stats()["planned_unlimited_bytes"], source.stat().st_size)

    def test_single_claim_never_falls_through_to_another_task(self):
        missing = self.photo("missing.jpg")
        changed = self.photo("changed.jpg")
        self.photo("untouched.jpg")
        self.queue.scan()
        ids = {row["relative_path"]: row["id"] for row in self.queue.list_jobs()}
        missing.unlink()
        self.assertIsNone(self.queue.claim(job_id=ids["missing.jpg"]))
        changed.write_bytes(b"new contents with different size")
        self.assertIsNone(self.queue.claim(job_id=ids["changed.jpg"]))
        self.assertEqual(self.queue.get_job(ids["changed.jpg"])["status"], "failed")
        untouched = self.queue.get_job(ids["untouched.jpg"])
        self.assertEqual((untouched["status"], untouched["attempts"]), ("pending", 0))
        self.assertIsNone(self.queue.claim(job_id=999999))
        self.queue.request_control(ids["changed.jpg"], "start")
        resumed = self.queue.claim(job_id=ids["changed.jpg"])
        self.assertEqual(resumed.id, ids["changed.jpg"])
        self.assertEqual(resumed.size, changed.stat().st_size)
        self.queue.release(resumed)

    def test_filtered_summary_and_created_sort_are_stable_across_pages(self):
        for index in range(5):
            self.photo(f"{index}.jpg", bytes(index + 1))
        self.queue.scan()
        rows = self.queue.list_jobs()
        first_id = rows[0]["id"]
        self.queue._db.execute("UPDATE jobs SET created_at=100")
        self.queue._db.execute("UPDATE jobs SET created_at=200 WHERE id=?", (first_id,))
        expected = [row["id"] for row in rows[1:]] + [first_id]
        self.assertEqual([row["id"] for row in self.queue.list_jobs(sort="asc")], expected)
        self.assertEqual([row["id"] for row in self.queue.list_jobs(sort="desc")], expected[::-1])
        self.assertEqual([row["id"] for row in self.queue.list_jobs(sort="desc", limit=2, offset=1)], expected[::-1][1:3])
        for row in rows[:2]:
            self.queue.request_control(row["id"], "pause")
        paused = rows[:2]
        self.assertEqual(self.queue.jobs_summary("paused"),
                         {"total": 2, "total_bytes": sum(row["size"] for row in paused)})
        self.assertEqual(self.queue.jobs_summary(), {"total": 5, "total_bytes": 15})
        self.assertIn("created_at", self.queue.list_jobs(summary=True)[0])
        for method in (self.queue.list_jobs, self.queue.jobs_summary):
            with self.assertRaises(ValueError):
                method("unrecognized-filter")

    def test_keyset_task_pages_include_missing_for_deletion_and_refuse_live_claim(self):
        missing = self.photo("missing.jpg")
        self.photo("next.jpg")
        self.photo("last.jpg")
        self.queue.scan()
        missing_id = next(row["id"] for row in self.queue.list_jobs() if row["relative_path"] == "missing.jpg")
        missing.unlink()
        self.queue.scan()
        all_rows = self.queue.iter_tasks()
        self.assertEqual(len(all_rows), 3)
        self.assertIn(missing_id, [row["id"] for row in all_rows])
        self.assertEqual(len(self.queue.iter_tasks(include_missing=False)), 2)
        first = self.queue.iter_tasks(limit=1)[0]
        self.assertTrue(self.queue.delete_task_final(first["id"]))
        second_page = self.queue.iter_tasks(after_id=first["id"], limit=1)
        self.assertEqual(second_page[0]["id"], all_rows[1]["id"])
        running = self.queue.claim()
        with self.assertRaises(QueueBusyError):
            self.queue.delete_task_final(running.id)
        self.queue.release(running)
        self.assertTrue(self.queue.delete_task_final(running.id))
        self.assertFalse(self.queue.delete_task_final(running.id))

    def test_partial_deletion_error_keeps_idle_task_and_all_cloud_evidence(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(job, mode="regular", remote_hash="A" * 40,
                                            metadata={"is_unlim_upload": False, "verified": True})
        with self.assertRaises(QueueBusyError):
            self.queue.record_task_error(job.id, "cannot delete while running")
        self.queue.release(job)
        self.queue.request_control(job.id, "pause")
        self.queue._db.execute("UPDATE jobs SET remove_requested=1 WHERE id=?", (job.id,))
        receipt = self.queue.cloud_history(job.id)
        result = self.queue.record_task_error(job.id, "Не удалось переместить файл в корзину")
        self.assertEqual(result["status"], "failed")
        self.assertIsNone(result["next_attempt"])
        self.assertTrue(result["cloud_present"])
        self.assertTrue(result["paused"])
        self.assertFalse(result["remove_requested"])
        self.assertEqual(self.queue.cloud_history(job.id), receipt)
        self.assertEqual(self.queue.jobs_summary("failed")["total"], 1)
        self.assertEqual(self.queue.jobs_summary("uploaded_regular")["total"], 1)
        self.queue.request_control(job.id, "start")
        resumed = self.queue.claim(job_id=job.id)
        self.assertEqual(self.queue.get_commit_intent(resumed)["storage_observation"]["mode"], "regular")
        self.queue.release(resumed)

    def schema_three_policy_fixture(self):
        self.queue._db.execute("DROP TABLE cloud_observations")
        for name in ("jobs_pending_unlimited", "jobs_pending_regular", "jobs_retry_unlimited", "jobs_retry_regular",
                     "jobs_reconcile", "jobs_current_created", "jobs_current_status_created", "jobs_paused_created",
                     "jobs_observed_created"):
            self.queue._db.execute(f"DROP INDEX {name}")
        for column in ("planned_mode", "planned_reason", "observed_mode", "cloud_present", "cloud_path", "cloud_hash",
                       "cloud_size", "cloud_verified", "cloud_observed_at", "cloud_deleted_at"):
            self.queue._db.execute(f"ALTER TABLE jobs DROP COLUMN {column}")
        self.queue._db.execute("DELETE FROM metadata WHERE key='_media_policy_version'")
        self.queue._db.execute("PRAGMA user_version=3")

    def test_schema_three_migration_classifies_without_fabricating_acceptance_from_intent(self):
        for name in ("unlimited.jpg", "ordinary.m4v", "unknown.jpg", "pending.m4v", "failed.jpg", "existing.jpg"):
            self.photo(name)
        self.queue.scan()
        ids = {row["relative_path"]: row["id"] for row in self.queue.list_jobs()}
        for name, mode, flag in (("unlimited.jpg", "unlimited", True), ("ordinary.m4v", "regular", False)):
            job = self.queue.claim(job_id=ids[name])
            self.queue.mark_uploaded(job, remote_hash="A" * 40,
                                     verified_metadata={"storage_mode": mode, "is_unlim_upload": flag})
        self.queue._db.execute("UPDATE jobs SET status='uploaded',storage_mode=NULL,verified_metadata='invalid json' WHERE id=?",
                               (ids["unknown.jpg"],))
        self.queue._db.execute("UPDATE jobs SET storage_mode='regular' WHERE id=?", (ids["pending.m4v"],))
        self.queue._db.execute("UPDATE jobs SET status='failed',next_attempt=NULL,storage_mode='unlimited' WHERE id=?",
                               (ids["failed.jpg"],))
        self.queue._db.execute("UPDATE jobs SET status='skipped',skip_reason='remote_exists',remote_hash=? WHERE id=?",
                               ("B" * 40, ids["existing.jpg"]))
        self.schema_three_policy_fixture()
        with Queue(self.db, destination="/Archive") as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 4)
            self.assertEqual(upgraded.get_job(ids["ordinary.m4v"])["planned_mode"], "regular")
            self.assertFalse(upgraded.get_job(ids["pending.m4v"])["cloud_present"])
            self.assertFalse(upgraded.get_job(ids["failed.jpg"])["cloud_present"])
            self.assertEqual(upgraded.get_job(ids["unlimited.jpg"])["observed_mode"], "unlimited")
            self.assertEqual(upgraded.get_job(ids["ordinary.m4v"])["observed_mode"], "regular")
            self.assertEqual(upgraded.get_job(ids["unknown.jpg"])["observed_mode"], "unknown")
            self.assertEqual(upgraded.get_job(ids["existing.jpg"])["observed_mode"], "unknown")
            count = upgraded._db.execute("SELECT COUNT(*) FROM cloud_observations").fetchone()[0]
            self.assertEqual(count, 4)
            version = upgraded._db.execute("PRAGMA data_version").fetchone()[0]
            with Queue(self.db, destination="/Archive") as reopened:
                self.assertEqual(reopened._db.total_changes, 0)
                self.assertEqual(reopened._db.execute("SELECT COUNT(*) FROM cloud_observations").fetchone()[0], count)
            self.assertEqual(upgraded._db.execute("PRAGMA data_version").fetchone()[0], version)

    def test_schema_three_policy_migration_rolls_back_all_new_fields_and_receipts(self):
        self.photo()
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="A" * 40)
        self.schema_three_policy_fixture()
        original = Queue._record_cloud_observation_row
        def interrupted(queue, *args, **kwargs):
            original(queue, *args, **kwargs)
            raise OSError("migration interrupted")
        with patch.object(Queue, "_record_cloud_observation_row", interrupted), self.assertRaises(OSError):
            Queue(self.db, destination="/Archive")
        self.assertEqual(self.queue._db.execute("PRAGMA user_version").fetchone()[0], 3)
        self.assertNotIn("cloud_present", {row["name"] for row in self.queue._db.execute("PRAGMA table_info(jobs)")})
        self.assertIsNone(self.queue._db.execute("SELECT 1 FROM sqlite_master WHERE name='cloud_observations'").fetchone())
        with Queue(self.db, destination="/Archive") as upgraded:
            self.assertEqual(upgraded.stats()["uploaded_unlimited_count"], 1)
            self.assertEqual(len(upgraded.cloud_history(job.id)), 1)

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
            other_mode = "regular" if mode == "unlimited" else "unlimited"
            with target.open("r+b") as stream:
                stream.truncate(target_size)
            snapshot = target.stat()
            for state in ("pending", "failed"):
                with self.subTest(mode=mode, state=state):
                    with self.queue._transaction():
                        self.queue._db.execute(
                            "UPDATE jobs SET size=?,planned_mode=?,status=?,claim_token=NULL,attempts=0,next_attempt=0 WHERE id<?",
                            (other_size, other_mode, state, target_id),
                        )
                        self.queue._db.execute(
                            "UPDATE jobs SET size=?,mtime_ns=?,device=?,inode=?,status=?,claim_token=NULL,"
                            "attempts=0,next_attempt=0,planned_mode=? WHERE id=?",
                            (snapshot.st_size, snapshot.st_mtime_ns, snapshot.st_dev, str(snapshot.st_ino), state, mode, target_id),
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

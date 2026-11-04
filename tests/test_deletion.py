"""Deletion guards exercised against real temporary files, never user data."""
from __future__ import annotations

import errno
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from mailru_photos.api import FileIdentity
from mailru_photos import deletion
from mailru_photos.deletion import (
    DeletionJournal, LocalDeletionRecoveryRequired, delete_local_file,
    deletion_preview, selected_tasks,
)
from mailru_photos.queue import Queue, QueueError


class SimulatedCrash(BaseException):
    """A process interruption must not be handled as an ordinary file error."""


@unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
class GuardedLocalDeletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "Фотографии"
        self.root.mkdir()
        self.path = self.root / "ёлка.jpg"
        self.path.write_bytes(b"original photo")
        self.expected = FileIdentity.from_stat(self.path.stat())
        self.stage_name = ".mail-cloud-delete-test-1"
        self.stage = self.root / self.stage_name

    def tearDown(self):
        self.tmp.cleanup()

    def delete(self):
        delete_local_file(self.root, self.path, self.expected, self.stage_name)

    def test_exact_file_is_removed_and_other_files_are_untouched(self):
        other = self.root / "other.jpg"
        other.write_bytes(b"keep")
        self.delete()
        self.assertFalse(self.path.exists())
        self.assertFalse(self.stage.exists())
        self.assertEqual(other.read_bytes(), b"keep")
        self.delete()  # A confirmed interrupted operation can observe absence.
        self.assertEqual(other.read_bytes(), b"keep")

    def test_changed_file_and_directory_are_not_removed(self):
        self.path.write_bytes(b"replacement with different size")
        with self.assertRaises(QueueError):
            self.delete()
        self.assertEqual(self.path.read_bytes(), b"replacement with different size")
        self.assertFalse(self.stage.exists())
        self.path.unlink()
        self.path.mkdir()
        with self.assertRaises(QueueError):
            self.delete()
        self.assertTrue(self.path.is_dir())

    def test_source_symlink_does_not_delete_external_target(self):
        external = Path(self.tmp.name) / "outside.jpg"
        external.write_bytes(b"outside")
        self.path.unlink()
        self.path.symlink_to(external)
        with self.assertRaises(QueueError):
            self.delete()
        self.assertTrue(self.path.is_symlink())
        self.assertEqual(external.read_bytes(), b"outside")

    def test_symlink_parent_and_root_are_not_followed(self):
        linked = self.root / "linked"
        linked.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(QueueError):
            delete_local_file(self.root, linked / self.path.name, self.expected, self.stage_name)
        alias = Path(self.tmp.name) / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(QueueError):
            delete_local_file(alias, alias / self.path.name, self.expected, self.stage_name)
        self.assertEqual(self.path.read_bytes(), b"original photo")

    def test_paths_outside_source_and_directory_deletion_are_rejected(self):
        cases = [self.root, self.root / ".." / "outside.jpg", Path(self.tmp.name) / "outside.jpg"]
        for path in cases:
            with self.subTest(path=path), self.assertRaises(QueueError):
                delete_local_file(self.root, path, self.expected, self.stage_name)
        self.assertTrue(self.path.exists())

    def test_untrusted_staging_directory_is_neither_used_nor_removed(self):
        self.stage.mkdir(mode=0o755)
        with self.assertRaises(QueueError):
            self.delete()
        self.assertTrue(self.stage.is_dir())
        self.assertTrue(self.path.exists())
        self.stage.rmdir()
        other = self.root / "other"
        other.mkdir()
        self.stage.symlink_to(other, target_is_directory=True)
        with self.assertRaises(QueueError):
            self.delete()
        self.assertTrue(self.stage.is_symlink())
        self.assertTrue(other.is_dir())

    def test_unexpected_existing_staged_file_requires_recovery(self):
        self.stage.mkdir(mode=0o700)
        staged = self.stage / "source"
        staged.write_bytes(b"unexpected")
        with self.assertRaises(LocalDeletionRecoveryRequired):
            self.delete()
        self.assertEqual(staged.read_bytes(), b"unexpected")
        self.assertEqual(self.path.read_bytes(), b"original photo")

    def test_renamed_replacement_is_restored_without_being_deleted(self):
        original_rename = deletion._rename_no_replace
        saved = self.root / "saved-original.jpg"
        calls = 0

        def race(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                self.path.rename(saved)
                self.path.write_bytes(b"concurrent replacement")
            original_rename(*args)

        with patch.object(deletion, "_rename_no_replace", side_effect=race), self.assertRaises(QueueError):
            self.delete()
        self.assertEqual(saved.read_bytes(), b"original photo")
        self.assertEqual(self.path.read_bytes(), b"concurrent replacement")
        self.assertFalse(self.stage.exists())

    def test_restore_race_never_overwrites_new_source_and_retains_stage(self):
        original_rename = deletion._rename_no_replace
        saved = self.root / "saved-original.jpg"
        calls = 0

        def race(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                self.path.rename(saved)
                self.path.write_bytes(b"first replacement")
                original_rename(*args)
                self.path.write_bytes(b"second replacement")
            else:
                original_rename(*args)

        with patch.object(deletion, "_rename_no_replace", side_effect=race), self.assertRaises(LocalDeletionRecoveryRequired):
            self.delete()
        self.assertEqual(saved.read_bytes(), b"original photo")
        self.assertEqual(self.path.read_bytes(), b"second replacement")
        self.assertEqual((self.stage / "source").read_bytes(), b"first replacement")

    def test_unlink_permission_failure_restores_original(self):
        with patch.object(deletion.os, "unlink", side_effect=PermissionError(errno.EACCES, "denied")), self.assertRaises(QueueError):
            self.delete()
        self.assertEqual(self.path.read_bytes(), b"original photo")
        self.assertFalse(self.stage.exists())

    def test_error_after_move_restores_original(self):
        original_fsync = os.fsync
        calls = 0

        def fail_first(fd):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError(errno.EIO, "offline disk failure")
            return original_fsync(fd)

        with patch.object(deletion.os, "fsync", side_effect=fail_first), self.assertRaises(QueueError):
            self.delete()
        self.assertEqual(self.path.read_bytes(), b"original photo")
        self.assertFalse(self.stage.exists())

    def test_failed_restore_retains_same_stage_for_resuming(self):
        original_rename = deletion._rename_no_replace
        calls = 0

        def cannot_restore(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise PermissionError(errno.EACCES, "restore denied")
            original_rename(*args)

        with patch.object(deletion, "_rename_no_replace", side_effect=cannot_restore):
            with patch.object(deletion.os, "unlink", side_effect=PermissionError(errno.EACCES, "unlink denied")):
                with self.assertRaises(LocalDeletionRecoveryRequired) as caught:
                    self.delete()
        self.assertIn(self.stage_name, str(caught.exception))
        self.assertFalse(self.path.exists())
        self.assertEqual((self.stage / "source").read_bytes(), b"original photo")
        self.path.write_bytes(b"new source created after interruption")
        self.delete()  # Removes the frozen old inode, not the newly named file.
        self.assertEqual(self.path.read_bytes(), b"new source created after interruption")
        self.assertFalse(self.stage.exists())

    def test_crash_after_move_is_resumed_from_the_frozen_stage(self):
        with patch.object(deletion.os, "fsync", side_effect=SimulatedCrash), self.assertRaises(SimulatedCrash):
            self.delete()
        self.assertFalse(self.path.exists())
        self.assertEqual((self.stage / "source").read_bytes(), b"original photo")
        self.delete()
        self.assertFalse(self.stage.exists())
        self.assertFalse(self.path.exists())

    def test_failed_sync_after_unlink_requires_same_operation_to_resume(self):
        original_fsync = os.fsync
        calls = 0

        def fail_third(fd):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise OSError(errno.EIO, "sync after unlink failed")
            return original_fsync(fd)

        with patch.object(deletion.os, "fsync", side_effect=fail_third), self.assertRaises(LocalDeletionRecoveryRequired):
            self.delete()
        self.assertFalse(self.path.exists())
        self.delete()
        self.assertFalse(self.path.exists())

    def test_readonly_source_fails_without_moving_it(self):
        with patch.object(deletion.os, "mkdir", side_effect=OSError(errno.EROFS, "read-only")):
            with self.assertRaisesRegex(QueueError, "только для чтения"):
                self.delete()
        self.assertEqual(self.path.read_bytes(), b"original photo")

    def test_invalid_stage_and_unsupported_platform_do_not_touch_source(self):
        for stage in ["x", ".mail-cloud-delete-../x", ".mail-cloud-delete-\\x", ".mail-cloud-delete-\x00", None]:
            with self.subTest(stage=stage), self.assertRaises(QueueError):
                delete_local_file(self.root, self.path, self.expected, stage)
        with patch.object(deletion, "_RENAME", None), self.assertRaises(QueueError):
            self.delete()
        self.assertEqual(self.path.read_bytes(), b"original photo")


class DeletionJournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.photos = self.root / "photos"
        self.photos.mkdir()
        self.queue = Queue(self.root / "queue.db", destination="/Archive")
        self.queue.add_root(self.photos, "photos")
        self.path = self.root / "deletions.sqlite3"

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def scan(self, count):
        for index in range(count):
            (self.photos / f"{index:03}.jpg").write_bytes(f"photo-{index}".encode())
        self.assertTrue(self.queue.scan().complete)

    def preview(self, selection=None, **flags):
        selection = selection or {"scope": "all"}
        options = {"delete_local": True, "delete_cloud": False, **flags}
        return {**deletion_preview(self.queue, selection, **options), "selection": selection, **options}

    def test_confirmed_plan_and_checkpoints_survive_reopening(self):
        self.scan(3)
        with DeletionJournal(self.path) as journal:
            operation = journal.prepare(self.queue, self.preview())
            entries = journal.pending()
            self.assertEqual(operation["state"], "confirmed")
            self.assertEqual(len(entries), 3)
            self.assertEqual(len({entry["stage"] for entry in entries}), 3)
            journal.done(entries[0]["id"], "cloud_done")
            journal.done(entries[0]["id"], "local_done")
            journal.done(entries[0]["id"], "task_done")
            journal.error(entries[1]["id"], "retained failure")
        with DeletionJournal(self.path) as journal:
            self.assertEqual(journal.operation(), operation)
            self.assertEqual([entry["id"] for entry in journal.pending()], [entries[2]["id"]])
            self.assertEqual(journal.summary(), {"total": 3, "done": 1, "errors": 1, "processed": 2})
        self.assertEqual(len(list(self.photos.iterdir())), 3)
        if os.name == "posix":
            self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_pending_pages_are_bounded_and_previous_confirmation_cannot_be_replaced(self):
        self.scan(203)
        preview = self.preview()
        with DeletionJournal(self.path) as journal:
            journal.prepare(self.queue, preview)
            first = journal.pending()
            second = journal.pending(after_id=first[-1]["id"])
            third = journal.pending(after_id=second[-1]["id"])
            self.assertEqual([len(first), len(second), len(third)], [100, 100, 3])
            with self.assertRaises(QueueError):
                journal.prepare(self.queue, preview)
            self.assertEqual(journal.summary()["total"], 203)

    def test_changed_queue_aborts_before_any_file_is_deleted(self):
        self.scan(1)
        preview = self.preview()
        (self.photos / "new.jpg").write_bytes(b"new task")
        self.queue.scan()
        with DeletionJournal(self.path) as journal:
            with self.assertRaisesRegex(QueueError, "Список задач изменился"):
                journal.prepare(self.queue, preview)
            self.assertEqual(journal.operation()["state"], "aborted")
        self.assertEqual(len(list(self.photos.iterdir())), 2)
        self.assertEqual(self.queue.stats()["total"], 2)

    def test_finished_preview_matches_terminal_errors_skips_and_clear(self):
        self.scan(4)
        failed = self.queue.claim()
        self.queue.mark_failed(failed, "terminal", retry_delay=None)
        skipped = self.queue.claim()
        self.queue.mark_skipped(skipped, "skipped")
        retrying = self.queue.claim()
        self.queue.mark_failed(retrying, "will retry", retry_delay=600)
        selection = {"scope": "finished"}
        preview = self.preview(selection, delete_local=False)
        self.assertEqual(preview["task_count"], 2)
        self.assertEqual({row["id"] for row in selected_tasks(self.queue, selection)}, {failed.id, skipped.id})
        self.assertEqual(self.queue.clear_tasks("finished"), preview["task_count"])
        self.assertEqual(len(list(self.photos.iterdir())), 4)
        self.assertEqual(self.queue.stats()["total"], 2)

    def test_readonly_mount_preview_blocks_physical_local_delete(self):
        self.scan(1)
        with patch.object(deletion.os, "statvfs", return_value=SimpleNamespace(f_flag=os.ST_RDONLY)):
            preview = self.preview()
        self.assertFalse(preview["local_delete_allowed"])
        self.assertTrue(any("только для чтения" in item for item in preview["warnings"]))
        self.assertTrue(any("навсегда" in item for item in preview["warnings"]))


if __name__ == "__main__":
    unittest.main()

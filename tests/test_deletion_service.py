"""Confirmed deletion through the controller with offline cloud and real files."""
from __future__ import annotations

from dataclasses import asdict
import errno
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mailru_photos import deletion
from mailru_photos.api import ProtocolError, RemoteError
from mailru_photos.auth import TokenStore, Tokens
from mailru_photos.deletion import DeletionJournal, LOCAL_CONFIRMATION
from mailru_photos.queue import QueueBusyError, QueueError
from mailru_photos.runner import run
from mailru_photos.transport import Cancelled, TransportError
from mailru_photos.web_service import WebService

try:
    from test_runner import EMAIL, FakeCloud
except ModuleNotFoundError:
    from tests.test_runner import EMAIL, FakeCloud


class DeletionServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.photos = self.root / "Фотографии"
        self.photos.mkdir()
        self.state = self.root / "state"
        self.state.mkdir()
        self.cloud = FakeCloud()
        self.services = []
        TokenStore(self.state / "auth.dat").save(Tokens("offline-access", "offline-refresh", EMAIL))
        self.service = self.make_service()
        self.service.setup("/Archive")
        self.service.add_folder(str(self.photos), "Photos")

    def tearDown(self):
        for service in self.services:
            service.close(timeout=2)
        self.tmp.cleanup()

    def make_service(self):
        def factory(tokens, *, stop):
            self.cloud.stop = stop
            return self.cloud
        service = WebService(self.state, [self.photos], api_factory=factory)
        self.services.append(service)
        return service

    def done(self, service=None):
        service = service or self.service
        deadline = time.monotonic() + 5
        while service._operation is not None and time.monotonic() < deadline:
            service._thread.join(timeout=0.02)
        self.assertIsNone(service._operation, "Deletion did not finish")
        return service.snapshot()

    def scan(self, count=1, *, upload=False):
        for index in range(count):
            (self.photos / f"ёлка-{index}.jpg").write_bytes(f"offline-photo-{index}".encode() + b"x" * 64)
        with self.service._queue() as queue:
            queue.bind_account(EMAIL)
            queue.set_metadata("last_scan", asdict(queue.scan()))
            if upload:
                result = run(queue, self.cloud, workers=1)
                self.assertEqual(result["outcome"], "complete")
            rows = queue.list_jobs()
        self.service._invalidate_stats()
        return rows

    def confirm(self, preview, *, service=None):
        service = service or self.service
        return service.confirm_deletion(preview["token"], owner="session-A", local_confirmation=LOCAL_CONFIRMATION)

    def journal(self):
        with DeletionJournal(self.state / "deletions.sqlite3") as journal:
            return journal.operation(), journal.summary(), [dict(row) for row in journal.db.execute("SELECT * FROM entries ORDER BY id")]

    def test_task_only_token_is_bound_to_session_and_cannot_be_replayed(self):
        row = self.scan()[0]
        preview = self.service.preview_deletion(task_id=row["id"], owner="session-A")
        self.assertNotIn("fingerprint", preview)
        with self.assertRaises(ValueError):
            self.service.confirm_deletion(preview["token"], owner="session-B")
        self.confirm(preview)
        self.assertTrue(Path(row["local_path"]).exists())
        self.assertEqual(self.service.jobs()["total"], 0)
        self.assertEqual(self.cloud.trashed, [])
        with self.assertRaises(ValueError):
            self.confirm(preview)

    def test_expired_token_does_not_remove_task_or_file(self):
        row = self.scan()[0]
        preview = self.service.preview_deletion(task_id=row["id"], owner="session-A")
        with patch("mailru_photos.web_service.time.time", return_value=preview["expires_at"] + 1):
            with self.assertRaises(ValueError):
                self.confirm(preview)
        self.assertEqual(self.service.jobs()["total"], 1)
        self.assertTrue(Path(row["local_path"]).exists())

    def test_finished_bulk_preview_matches_deleted_terminal_errors(self):
        self.scan(3)
        with self.service._queue() as queue:
            job = queue.claim()
            queue.mark_failed(job, "terminal fixture", retry_delay=None)
            job = queue.claim()
            queue.mark_skipped(job, "fixture skip")
        self.service._invalidate_stats()
        preview = self.service.preview_deletion(scope="finished", owner="session-A")
        self.assertEqual(preview["task_count"], 2)
        result = self.confirm(preview)
        self.assertEqual(result["removed_tasks"], 2)
        self.assertEqual(self.service.jobs()["total"], 1)
        self.assertEqual(len(list(self.photos.iterdir())), 3)

    def test_changed_bulk_selection_requires_new_confirmation(self):
        self.scan()
        preview = self.service.preview_deletion(owner="session-A")
        self.scan(2)
        with self.assertRaisesRegex(ValueError, "Список задач изменился"):
            self.confirm(preview)
        self.assertEqual(self.service.jobs()["total"], 2)
        self.assertEqual(len(list(self.photos.iterdir())), 2)

    def test_only_explicit_boolean_options_are_accepted(self):
        self.scan()
        for options in [{"delete_local": "false"}, {"delete_cloud": 1}, {"delete_local": None}]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.service.preview_deletion(**options)

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_local_delete_requires_phrase_and_success_does_not_touch_other_files(self):
        row = self.scan()[0]
        untouched = self.photos / "notes.txt"
        untouched.write_bytes(b"not selected")
        preview = self.service.preview_deletion(task_id=row["id"], delete_local=True, owner="session-A")
        with self.assertRaises(ValueError):
            self.service.confirm_deletion(preview["token"], owner="session-A")
        self.assertTrue(Path(row["local_path"]).exists())
        self.confirm(preview)
        state = self.done()
        self.assertFalse(Path(row["local_path"]).exists())
        self.assertEqual(untouched.read_bytes(), b"not selected")
        self.assertEqual(state["stats"]["total"], 0)
        self.assertEqual(state["deletion"]["done"], 1)
        self.assertEqual(self.journal()[0]["state"], "complete")

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_readonly_mount_rejects_even_correct_local_confirmation(self):
        row = self.scan()[0]
        with patch.object(deletion.os, "statvfs", return_value=SimpleNamespace(f_flag=os.ST_RDONLY)):
            preview = self.service.preview_deletion(task_id=row["id"], delete_local=True, owner="session-A")
        self.assertFalse(preview["local_delete_allowed"])
        with self.assertRaisesRegex(ValueError, "только для чтения"):
            self.confirm(preview)
        self.assertTrue(Path(row["local_path"]).exists())
        self.assertEqual(self.service.jobs()["total"], 1)

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_cloud_and_local_deletion_complete_and_cloud_folder_is_refused(self):
        rows = self.scan(2, upload=True)
        first, second = rows
        self.cloud.remote[second["remote_path"]]["kind"] = "folder"
        preview = self.service.preview_deletion(delete_cloud=True, delete_local=True, owner="session-A")
        self.confirm(preview)
        state = self.done()
        self.assertEqual(state["deletion"]["done"], 1)
        self.assertEqual(state["deletion"]["errors"], 1)
        self.assertEqual(self.cloud.trashed, [first["remote_path"]])
        self.assertFalse(Path(first["local_path"]).exists())
        self.assertTrue(Path(second["local_path"]).exists())
        self.assertIn(second["remote_path"], self.cloud.remote)
        kept = self.service.jobs("failed")["jobs"]
        self.assertEqual([row["id"] for row in kept], [second["id"]])
        self.assertTrue(kept[0]["cloud_present"])

    def test_absent_cloud_object_is_idempotent_and_local_copy_is_preserved(self):
        row = self.scan(upload=True)[0]
        del self.cloud.remote[row["remote_path"]]
        preview = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, owner="session-A")
        self.confirm(preview)
        state = self.done()
        self.assertEqual(state["deletion"]["done"], 1)
        self.assertEqual(self.cloud.trashed, [])
        self.assertTrue(Path(row["local_path"]).exists())

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_failed_cloud_step_keeps_local_file_and_cloud_receipt_then_continues(self):
        first, second = self.scan(2, upload=True)
        self.cloud.trash_failures[first["remote_path"]] = TransportError("offline failure")
        preview = self.service.preview_deletion(delete_cloud=True, delete_local=True, owner="session-A")
        self.confirm(preview)
        state = self.done()
        self.assertEqual(state["deletion"]["errors"], 1)
        self.assertEqual(state["deletion"]["done"], 1)
        self.assertFalse(state["deletion"]["resume_available"])
        with self.service._queue() as queue:
            retained = queue.get_job(first["id"])
            self.assertEqual(retained["status"], "failed")
            self.assertTrue(retained["cloud_present"])
            self.assertEqual(retained["cloud_hash"], first["cloud_hash"])
            self.assertIsNone(queue.get_job(second["id"]))
        self.assertTrue(Path(first["local_path"]).exists())
        self.assertFalse(Path(second["local_path"]).exists())
        self.assertEqual(self.cloud.trashed, [second["remote_path"]])

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_local_failure_after_cloud_step_keeps_task_and_restores_source(self):
        row = self.scan(upload=True)[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, delete_local=True, owner="session-A")
        original_unlink = os.unlink

        def refuse_staged(name, *args, **kwargs):
            if name == "source" and "dir_fd" in kwargs:
                raise PermissionError(errno.EACCES, "offline deny")
            return original_unlink(name, *args, **kwargs)

        with patch.object(deletion.os, "unlink", side_effect=refuse_staged):
            self.confirm(preview)
            state = self.done()
        self.assertEqual(state["deletion"]["errors"], 1)
        self.assertTrue(Path(row["local_path"]).exists())
        self.assertEqual(self.cloud.trashed, [row["remote_path"]])
        with self.service._queue() as queue:
            retained = queue.get_job(row["id"])
            self.assertEqual(retained["status"], "failed")
            self.assertFalse(retained["cloud_present"])
        operation, summary, entries = self.journal()
        self.assertEqual((entries[0]["cloud_done"], entries[0]["local_done"], entries[0]["task_done"]), (1, 0, 0))
        self.assertFalse(list(self.photos.glob(".mail-cloud-delete-*")))
        retry = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, delete_local=True, owner="session-A")
        self.confirm(retry)
        self.assertEqual(self.done()["deletion"]["done"], 1)
        self.assertFalse(Path(row["local_path"]).exists())
        self.assertEqual(self.cloud.trashed, [row["remote_path"]])

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_changed_local_source_after_confirmation_is_not_deleted(self):
        row = self.scan()[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_local=True, owner="session-A")
        Path(row["local_path"]).write_bytes(b"changed after the preview")
        self.confirm(preview)
        state = self.done()
        self.assertEqual(state["deletion"]["errors"], 1)
        self.assertEqual(Path(row["local_path"]).read_bytes(), b"changed after the preview")
        self.assertEqual(self.service.jobs("failed")["total"], 1)

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_pause_after_cloud_delete_resumes_without_second_cloud_mutation(self):
        row = self.scan(upload=True)[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, delete_local=True, owner="session-A")
        with patch("mailru_photos.web_service.delete_local_file", side_effect=Cancelled("pause between steps")):
            self.confirm(preview)
            state = self.done()
        self.assertTrue(state["deletion"]["resume_available"])
        self.assertTrue(Path(row["local_path"]).exists())
        self.assertEqual(self.cloud.trashed, [row["remote_path"]])
        self.assertEqual(self.journal()[0]["state"], "paused")
        self.service.start()
        resumed = self.done()
        self.assertEqual(resumed["deletion"]["done"], 1)
        self.assertFalse(Path(row["local_path"]).exists())
        self.assertEqual(self.cloud.trashed, [row["remote_path"]])

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_restart_after_unlink_before_checkpoint_finishes_same_plan(self):
        row = self.scan(upload=True)[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, delete_local=True, owner="session-A")
        original_done = DeletionJournal.done

        def lose_checkpoint(journal, identifier, field):
            if field == "local_done":
                raise sqlite3.OperationalError("offline checkpoint interruption")
            return original_done(journal, identifier, field)

        with patch.object(DeletionJournal, "done", lose_checkpoint):
            self.confirm(preview)
            state = self.done()
        self.assertTrue(state["deletion"]["resume_available"])
        self.assertFalse(Path(row["local_path"]).exists())
        self.assertEqual(self.journal()[2][0]["local_done"], 0)
        self.service.close()
        replacement = self.make_service()
        self.assertTrue(replacement.snapshot()["deletion"]["resume_available"])
        self.assertFalse(replacement.resume_if_requested())  # Paused plans need an explicit Continue.
        self.assertIsNone(replacement._operation)
        unrelated = self.photos / "created-after-restart.txt"
        unrelated.write_bytes(b"keep")
        replacement.start()
        resumed = self.done(replacement)
        self.assertEqual(resumed["deletion"]["done"], 1)
        self.assertEqual(self.cloud.trashed, [row["remote_path"]])
        self.assertEqual(unrelated.read_bytes(), b"keep")
        self.assertEqual(replacement.jobs()["total"], 0)

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_unrestored_stage_blocks_new_plan_and_recovers_after_restart(self):
        row = self.scan()[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_local=True, owner="session-A")
        original_rename = deletion._rename_no_replace
        original_unlink = os.unlink
        moves = 0

        def deny_restore(*args):
            nonlocal moves
            moves += 1
            if moves == 2:
                raise PermissionError(errno.EACCES, "restore denied")
            return original_rename(*args)

        def deny_unlink(name, *args, **kwargs):
            if name == "source" and "dir_fd" in kwargs:
                raise PermissionError(errno.EACCES, "unlink denied")
            return original_unlink(name, *args, **kwargs)

        with patch.object(deletion, "_rename_no_replace", side_effect=deny_restore):
            with patch.object(deletion.os, "unlink", side_effect=deny_unlink):
                self.confirm(preview)
                state = self.done()
        self.assertTrue(state["deletion"]["resume_available"])
        self.assertEqual(state["deletion"]["errors"], 0)
        self.assertIn("служебной папке", state["activity"]["last_error"])
        operation, summary, entries = self.journal()
        staged = self.photos / entries[0]["stage"] / "source"
        self.assertTrue(staged.exists())
        self.assertFalse(Path(row["local_path"]).exists())
        with self.assertRaises(QueueBusyError):
            self.service.preview_deletion(owner="session-A")
        with self.assertRaises(QueueBusyError):
            self.service.scan()
        self.service.close()
        replacement = self.make_service()
        self.assertTrue(replacement.snapshot()["deletion"]["resume_available"])
        replacement.start()
        resumed = self.done(replacement)
        self.assertEqual(resumed["deletion"]["done"], 1)
        self.assertFalse(staged.exists())
        self.assertEqual(replacement.jobs()["total"], 0)

    @unittest.skipUnless(deletion.local_delete_supported(), "Linux renameat2 is required")
    def test_confirmed_but_not_started_plan_auto_resumes_after_restart(self):
        row = self.scan()[0]
        preview = self.service.preview_deletion(task_id=row["id"], delete_local=True, owner="session-A")
        with patch.object(self.service, "_launch", return_value={"ok": True}):
            self.confirm(preview)
        self.assertEqual(self.journal()[0]["state"], "confirmed")
        self.assertTrue(Path(row["local_path"]).exists())
        self.service.close()
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        self.assertEqual(self.done(replacement)["deletion"]["done"], 1)
        self.assertFalse(Path(row["local_path"]).exists())

    def test_authentication_failure_pauses_confirmed_cloud_plan(self):
        row = self.scan(upload=True)[0]
        self.cloud.trash_failures[row["remote_path"]] = RemoteError(401)
        preview = self.service.preview_deletion(task_id=row["id"], delete_cloud=True, owner="session-A")
        self.confirm(preview)
        state = self.done()
        self.assertTrue(state["auth_required"])
        self.assertTrue(state["deletion"]["resume_available"])
        self.assertEqual(self.journal()[0]["state"], "paused")
        self.assertTrue(Path(row["local_path"]).exists())
        self.assertIn(row["remote_path"], self.cloud.remote)
        self.assertEqual(self.cloud.trashed, [])


if __name__ == "__main__":
    unittest.main()

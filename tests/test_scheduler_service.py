"""Scheduled scans and uploads use the production service and queue offline."""
from pathlib import Path
import errno
import json
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mailru_photos.auth import TokenStore, Tokens
from mailru_photos.cli import write_json
from mailru_photos.web_service import WebService
try:
    from test_runner import EMAIL, FakeCloud
except ModuleNotFoundError:
    from tests.test_runner import EMAIL, FakeCloud


class ScheduledCloud(FakeCloud):
    def __init__(self):
        super().__init__()
        self.block = False
        self.started = threading.Event()

    def account_info(self):
        return self.require_account()

    def _upload_blob(self, path, digest, identity, *, progress=None, is_retried=False, upload_type="AUTO"):
        self.started.set()
        while self.block:
            self._check_stopped()
            time.sleep(0.002)
        return super()._upload_blob(path, digest, identity, progress=progress,
                                    is_retried=is_retried, upload_type=upload_type)


class ScheduledServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.library = self.root / "Фотографии"
        self.library.mkdir()
        self.first = self.library / "Семья"
        self.second = self.library / "Поездки"
        self.first.mkdir(); self.second.mkdir()
        self.state = self.root / "state"
        self.state.mkdir()
        self.cloud = ScheduledCloud()
        self.services = []
        TokenStore(self.state / "auth.dat").save(Tokens("offline-access", "offline-refresh", EMAIL))
        self.service = self.make_service()
        self.service.add_folder(str(self.first), "Семья", "/Семья")
        self.service.add_folder(str(self.second), "Поездки", "/Поездки")
        self.now = time.time()
        self.service.scheduler._clock = lambda: self.now

    def make_service(self):
        def factory(tokens, *, stop):
            self.cloud.stop = stop
            return self.cloud
        service = WebService(self.state, [self.library], api_factory=factory)
        self.services.append(service)
        return service

    def tearDown(self):
        for service in self.services:
            service.close(timeout=3)
        self.temp.cleanup()

    def done(self, service=None):
        service = service or self.service
        deadline = time.monotonic() + 6
        while service._operation is not None and time.monotonic() < deadline:
            service._thread.join(0.02)
        self.assertIsNone(service._operation, service.snapshot()["activity"])
        return service.snapshot()

    def put(self, parent, name):
        path = parent / name
        path.write_bytes((name.encode("utf-8") + b"-payload") * 64)
        return path

    def alarm(self, sources):
        roots = self.service.snapshot()["sources"]
        ids = [root["id"] for root in roots if root["alias"] in sources]
        result = self.service.save_schedule({"name": "Ночной архив", "source_ids": ids,
            "time": "23:10", "mode": "weekly", "weekdays": [1, 2, 3, 4, 5, 6, 7],
            "month_days": [], "enabled": True})
        return result["schedules"][-1]

    def test_scans_selected_roots_then_uploads_only_their_tasks(self):
        self.put(self.first, "до.jpg")
        other = self.put(self.second, "другой.jpg")
        self.service.scan(); self.done()
        other.unlink()
        self.put(self.first, "после.mov")
        selected = self.alarm(["Семья"])
        self.now = selected["next_run"]
        self.assertTrue(self.service.scheduler.tick())
        state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 2)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertEqual(state["last_scan"]["root_ids"], selected["source_ids"])
        self.assertTrue(all(item["path"].startswith("/Семья/") for item in self.cloud.commits))
        self.assertEqual(self.service.schedules()["schedules"][0]["last_run"]["status"], "complete")

    def test_multiple_selected_sources_and_full_quota_continue_unlimited(self):
        self.put(self.first, "обычный.m4v")
        self.put(self.second, "автозагрузка.jpg")
        self.service.settings(regular_policy="upload", unexpected_unlimited_action="keep")
        self.cloud.quota_values = [self.cloud.total_bytes]
        selected = self.alarm(["Семья", "Поездки"])
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(state["stats"]["failed"], 1)
        self.assertEqual(self.cloud.regular_commits, [])
        self.assertEqual([Path(p).suffix for p in self.cloud.upload_calls], [".jpg"])
        self.assertEqual(state["activity"]["phase"], "completed")

    def test_shutdown_and_restart_restore_only_scheduled_sources(self):
        self.put(self.first, "первый.jpg")
        self.put(self.second, "невыбранный.jpg")
        self.service.scan(); self.done()
        selected = self.alarm(["Семья"])
        self.cloud.block = True
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        self.assertTrue(self.cloud.started.wait(2))
        self.service.close(timeout=3)
        self.assertTrue(self.service._resume_upload)
        self.cloud.block = False
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        state = self.done(replacement)
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertEqual(len(self.cloud.commits), 1)
        self.assertTrue(self.cloud.commits[0]["path"].startswith("/Семья/"))

    def test_busy_alarm_waits_and_disabled_queued_alarm_does_not_start(self):
        self.put(self.first, "файл.jpg")
        selected = self.alarm(["Семья"])
        self.service._operation = "quota"
        self.now = selected["next_run"]
        try:
            self.assertFalse(self.service.scheduler.tick())
            self.assertTrue(self.service.schedules()["schedules"][0]["queued"])
            self.service.toggle_schedule(selected["id"], False)
        finally:
            self.service._operation = None
        self.assertFalse(self.service.scheduler.tick())
        self.assertEqual(self.cloud.upload_calls, [])

    def test_removing_last_source_disables_its_alarm(self):
        selected = self.alarm(["Семья"])
        self.service.remove_source(selected["source_ids"][0])
        schedule = self.service.schedules()["schedules"][0]
        self.assertFalse(schedule["enabled"])
        self.assertEqual(schedule["source_ids"], [])
        self.assertIsNone(schedule["next_run"])

    def test_failed_scan_never_starts_scheduled_upload(self):
        selected = self.alarm(["Семья"])
        self.first.rmdir()
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        state = self.done()
        self.assertEqual(state["activity"]["phase"], "error")
        self.assertEqual(self.cloud.upload_calls, [])
        self.assertEqual(self.service.schedules()["schedules"][0]["last_run"]["status"], "error")

    def test_overlapping_alarms_serialize_and_ui_reads_during_cloud_listing(self):
        self.put(self.first, "первое-расписание.jpg")
        self.put(self.second, "второе-расписание.jpg")
        first = self.alarm(["Семья"])
        second = self.alarm(["Поездки"])
        self.assertEqual(first["next_run"], second["next_run"])
        listing_started = threading.Event()
        release_listing = threading.Event()
        original_list = self.cloud.list_folder
        activity = {"active": 0, "maximum": 0}

        def blocked_listing(path="/", **kwargs):
            activity["active"] += 1
            activity["maximum"] = max(activity["maximum"], activity["active"])
            listing_started.set()
            try:
                while not release_listing.wait(0.01):
                    self.cloud._check_stopped()
                return original_list(path, **kwargs)
            finally:
                activity["active"] -= 1

        with patch.object(self.cloud, "list_folder", side_effect=blocked_listing):
            self.now = first["next_run"]
            self.assertTrue(self.service.scheduler.tick())
            try:
                self.assertTrue(listing_started.wait(2), "The scheduled cloud listing did not start")
                reads, errors = [], []
                read_finished = threading.Event()

                def read_panel():
                    try:
                        reads.append((self.service.snapshot(), self.service.jobs(sort_by="name", limit=25)))
                    except BaseException as error:
                        errors.append(error)
                    finally:
                        read_finished.set()

                reader = threading.Thread(target=read_panel, daemon=True)
                reader.start()
                self.assertTrue(read_finished.wait(2), "An in-flight cloud page blocked the dashboard or queue")
                reader.join(1)
                self.assertEqual(errors, [])
                self.assertEqual(reads[0][0]["activity"]["phase"], "reconciling")
                self.assertEqual(reads[0][1]["total"], 1)
                self.assertEqual(self.cloud.upload_calls, [])
                for _ in range(3):
                    self.assertFalse(self.service.scheduler.tick())
                self.assertEqual(self.service.scheduler.summary()["queued"], 1)
                self.assertEqual(activity["maximum"], 1)
            finally:
                release_listing.set()
            first_done = self.done()
            self.assertEqual(first_done["stats"]["uploaded"], 1)
            self.assertFalse(first_done["last_scan"]["warning_allowed"])
            self.assertTrue(self.service.scheduler.tick())
            second_done = self.done()
        self.assertEqual(activity["maximum"], 1)
        self.assertEqual(second_done["stats"]["uploaded"], 2)
        self.assertEqual(len(self.cloud.commits), 2)
        self.assertEqual(len({item["path"] for item in self.cloud.commits}), 2)
        self.assertTrue(all(item["last_run"]["status"] == "complete" for item in self.service.schedules()["schedules"]))

    def test_next_schedule_rechecks_deleted_cloud_file_and_suppresses_quota_warning(self):
        self.put(self.first, "вернуть.jpg")
        self.put(self.first, "уже-в-квоте.m4v")
        self.service.settings(regular_policy="upload")
        selected = self.alarm(["Семья"])
        self.now = selected["next_run"]
        self.assertTrue(self.service.scheduler.tick())
        initial = self.done()
        self.assertEqual(initial["stats"]["uploaded"], 2)
        old_ids = {row["id"] for row in self.service.jobs()["jobs"]}
        remote_photo = self.cloud.commits[0]["path"]
        remote_regular = self.cloud.regular_commits[0]["path"]
        with self.cloud._fake_lock:
            del self.cloud.remote[remote_photo]
            self.cloud.remote[remote_regular]["is_unlim_upload"] = False
        self.put(self.first, "новый-в-квоте.m4v")
        self.cloud.started.clear()
        self.cloud.block = True
        self.now = self.service.schedules()["schedules"][0]["next_run"]
        self.assertTrue(self.service.scheduler.tick())
        try:
            self.assertTrue(self.cloud.started.wait(2), self.service.snapshot()["activity"])
            current = self.service.snapshot()
            self.assertEqual(current["last_scan"]["trigger"], "scheduled")
            self.assertTrue(current["last_scan"]["complete"])
            self.assertFalse(current["last_scan"]["warning_allowed"])
            self.assertEqual(current["storage_plan"]["regular"]["count"], 1)
            self.assertEqual(current["stats"]["existing"], 1)
            self.assertNotIn("existing_regular_count", current["stats"])
            rows = self.service.jobs()["jobs"]
            self.assertFalse(old_ids.intersection(row["id"] for row in rows))
            photo = next(row for row in rows if row["relative_path"] == "вернуть.jpg")
            self.assertFalse(photo["cloud_present"], "Old upload history cannot prove the deleted object still exists")
            self.assertIn(photo["status"], {"pending", "running"})
        finally:
            self.cloud.block = False
        completed = self.done()
        self.assertEqual(completed["stats"]["uploaded"], 2)
        self.assertEqual(completed["stats"]["existing"], 1)
        self.assertEqual([item["path"] for item in self.cloud.commits], [remote_photo, remote_photo])
        self.assertEqual(len(self.cloud.regular_commits), 2, "An existing quota file must not be uploaded again")
        self.assertFalse(completed["last_scan"]["warning_allowed"])

    def test_manual_scan_after_paused_schedule_keeps_manual_quota_warning(self):
        self.put(self.first, "пауза.jpg")
        self.put(self.first, "обычная-квота.m4v")
        selected = self.alarm(["Семья"])
        self.cloud.block = True
        self.now = selected["next_run"]
        self.assertTrue(self.service.scheduler.tick())
        self.assertTrue(self.cloud.started.wait(2))
        self.service.pause()
        self.done()
        self.cloud.block = False
        self.service.scan()
        manual = self.done()
        self.assertTrue(manual["last_scan"]["complete"])
        self.assertEqual(manual["last_scan"]["trigger"], "manual")
        self.assertTrue(manual["last_scan"]["warning_allowed"])
        self.assertEqual(manual["storage_plan"]["regular"]["count"], 1)
        self.assertEqual(self.cloud.commits, [])

    def test_close_at_completed_schedule_boundary_never_resumes_unselected_sources(self):
        self.put(self.first, "выбранный.jpg")
        self.put(self.second, "чужой.jpg")
        self.service.scan(); self.done()
        selected = self.alarm(["Семья"])
        original_finish = self.service.scheduler.finish

        def shutdown_at_checkpoint(identifier, status, message):
            original_finish(identifier, status, message)
            if status == "complete":
                # Model Docker stop after the durable schedule completion,
                # before the controller's final upload-intent checkpoint.
                self.service.close(timeout=0)

        with patch.object(self.service.scheduler, "finish", side_effect=shutdown_at_checkpoint):
            self.now = selected["next_run"]
            self.assertTrue(self.service.scheduler.tick())
            state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertFalse(self.service._resume_upload)
        replacement = self.make_service()
        self.assertFalse(replacement.resume_if_requested())
        resumed = self.done(replacement)
        self.assertEqual(resumed["stats"]["pending"], 1)
        self.assertEqual(len(self.cloud.commits), 1)
        self.assertTrue(self.cloud.commits[0]["path"].startswith("/Семья/"))

    def test_auto_resume_disabled_does_not_restart_saved_schedule_from_timer(self):
        self.put(self.first, "файл.jpg")
        self.service.settings(auto_resume=False)
        selected = self.alarm(["Семья"])
        self.cloud.block = True
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        self.assertTrue(self.cloud.started.wait(2))
        self.service.close(timeout=3)
        self.cloud.block = False
        replacement = self.make_service()
        # Exercise the two startup stages separately, without a timing-based
        # assertion against a live timer thread.
        self.assertFalse(replacement._resume_if_requested())
        self.assertFalse(replacement.scheduler.tick())
        self.assertEqual(replacement.schedules()["schedules"][0]["last_run"]["status"], "paused")
        self.assertEqual(self.cloud.commits, [])
        self.assertIsNone(replacement._operation)
        with patch.object(replacement, "_scan_queue", side_effect=AssertionError("Already completed this occurrence's scan")):
            replacement.start()
            self.assertEqual(self.done(replacement)["stats"]["uploaded"], 1)

    def test_removing_one_source_of_paused_schedule_resumes_remaining_scope(self):
        self.put(self.first, "удалённый-источник.jpg")
        self.put(self.second, "нужный.jpg")
        selected = self.alarm(["Семья", "Поездки"])
        self.cloud.block = True
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        self.assertTrue(self.cloud.started.wait(2))
        self.service.pause()
        self.done()
        removed_id = next(root["id"] for root in self.service.snapshot()["sources"] if root["alias"] == "Семья")
        self.service.remove_source(removed_id)
        self.cloud.block = False
        self.service.start()
        state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(len(self.cloud.commits), 1)
        self.assertTrue(self.cloud.commits[0]["path"].startswith("/Поездки/"))
        self.assertEqual(self.service.schedules()["schedules"][0]["last_run"]["status"], "complete")

    def test_explicit_selected_task_start_is_not_replaced_by_paused_schedule(self):
        self.put(self.first, "расписание.jpg")
        self.put(self.second, "вручную.jpg")
        self.service.scan(); self.done()
        selected = self.alarm(["Семья"])
        self.cloud.block = True
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        self.assertTrue(self.cloud.started.wait(2))
        self.service.pause()
        self.done()
        self.cloud.block = False
        with self.service._queue() as queue:
            task = next(row for row in queue.list_jobs() if row["remote_path"].startswith("/Поездки/"))
        self.service.start(job_id=task["id"])
        state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertEqual([item["path"] for item in self.cloud.commits], [task["remote_path"]])

    def test_invalid_schedule_edit_leaves_saved_sources_time_and_events_unchanged(self):
        selected = self.alarm(["Семья"])
        before = self.service.schedules()
        events = self.service.snapshot()["events"]
        for changes in ({"source_ids": [True]}, {"source_ids": [999999]}, {"time": "25:30"},
                        {"month_days": [False]}, {"mode": {}}, {"enabled": []}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.service.save_schedule({**selected, **changes})
        self.assertEqual(self.service.schedules(), before)
        self.assertEqual(self.service.snapshot()["events"], events)

    def test_terminal_scheduler_checkpoint_overrides_stale_upload_resume_intent(self):
        self.put(self.first, "завершённый.jpg")
        self.put(self.second, "невыбранный.jpg")
        self.service.scan(); self.done()
        selected = self.alarm(["Семья"])
        self.now = selected["next_run"]
        self.service.scheduler.tick()
        self.done()
        finished = self.service.schedules()["schedules"][0]["last_run"]
        self.assertEqual(finished["status"], "complete")
        self.service.close()
        activity_path = self.state / "web_activity.json"
        activity = json.loads(activity_path.read_text(encoding="utf-8"))
        activity.update(resume_upload=True, occurrence_id=finished["id"], root_ids=selected["source_ids"],
                        phase="uploading", job_id=None)
        write_json(activity_path, activity)  # Last activity checkpoint before a simulated crash.
        replacement = self.make_service()
        self.assertFalse(replacement.resume_if_requested())
        state = self.done(replacement)
        self.assertFalse(replacement._resume_upload)
        self.assertIsNone(state["activity"]["last_error"])
        self.assertNotEqual(state["activity"]["phase"], "error")
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertEqual(len(self.cloud.commits), 1)

    def test_manual_pause_checkpoint_wins_if_worker_did_not_checkpoint_occurrence(self):
        self.put(self.first, "пауза.jpg")
        self.put(self.second, "другая-папка.jpg")
        self.service.scan(); self.done()
        selected = self.alarm(["Семья"])
        self.service._operation = "quota"
        self.now = selected["next_run"]
        try:
            self.assertFalse(self.service.scheduler.tick())
        finally:
            self.service._operation = None
        identifier = next(iter(self.service.scheduler._occurrences))
        self.service.scheduler.begin(identifier)  # Durable pre-worker state.
        self.service.close()
        activity_path = self.state / "web_activity.json"
        activity = json.loads(activity_path.read_text(encoding="utf-8"))
        activity.update(resume_upload=False, occurrence_id=identifier, root_ids=selected["source_ids"],
                        phase="pausing", job_id=None)
        write_json(activity_path, activity)  # pause() was saved before an interrupted worker drained.
        replacement = self.make_service()
        self.assertFalse(replacement._resume_if_requested())
        self.assertFalse(replacement.scheduler.tick())
        self.assertEqual(replacement.scheduler.occurrence(identifier)["status"], "paused")
        self.assertEqual(self.cloud.commits, [])
        replacement.start()
        state = self.done(replacement)
        self.assertEqual(state["stats"]["uploaded"], 1)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertTrue(self.cloud.commits[0]["path"].startswith("/Семья/"))

    def test_event_write_failure_before_worker_launch_does_not_strand_running_occurrence(self):
        self.put(self.first, "план.jpg")
        self.put(self.second, "невыбранный.jpg")
        self.service.scan(); self.done()
        self.put(self.first, "добавлен-после-прошлого-сканирования.jpg")
        selected = self.alarm(["Семья"])
        original_event = self.service._event

        def fail_start_event(message, level="info"):
            if message.startswith("Запущен планировщик:"):
                raise OSError(errno.ENOSPC, "offline full disk")
            return original_event(message, level)

        self.now = selected["next_run"]
        with patch.object(self.service, "_event", side_effect=fail_start_event), self.assertRaises(OSError):
            self.service.scheduler.tick()
        self.assertIsNone(self.service._operation)
        last = self.service.schedules()["schedules"][0]["last_run"]
        self.assertEqual(last["status"], "paused")
        self.assertEqual(self.cloud.commits, [])
        self.service.start()
        state = self.done()
        self.assertEqual(state["stats"]["uploaded"], 2)
        self.assertEqual(state["stats"]["pending"], 1)
        self.assertEqual(len(self.cloud.commits), 2)
        self.assertTrue(all(item["path"].startswith("/Семья/") for item in self.cloud.commits))


if __name__ == "__main__":
    unittest.main()

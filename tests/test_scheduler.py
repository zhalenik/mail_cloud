"""Real persistent alarms with an injected clock; no waiting or cloud calls."""
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from mailru_photos.scheduler import Scheduler, next_occurrence, validate_schedule


def stamp(value):
    return datetime.fromisoformat(value).timestamp()


def alarm(**changes):
    return {"name": "Архив", "source_ids": [1, 2], "time": "12:30", "mode": "weekly",
            "weekdays": list(range(1, 8)), "month_days": [], "enabled": True, **changes}


class SchedulerCalendarTests(unittest.TestCase):
    def test_weekly_days_and_timezone(self):
        value = alarm(weekdays=[1, 3, 5])
        result = next_occurrence(value, "Europe/Moscow", stamp("2026-11-04T09:00:00+00:00"))
        self.assertEqual(result, stamp("2026-11-04T09:30:00+00:00"))
        self.assertEqual(next_occurrence(value, "Europe/Moscow", result), stamp("2026-11-06T09:30:00+00:00"))

    def test_monthly_skips_absent_day_and_deduplicates(self):
        value = validate_schedule(alarm(mode="monthly", month_days="30, 1, 30"), [1, 2])
        self.assertEqual(value["month_days"], [1, 30])
        value["month_days"] = [30]
        self.assertEqual(next_occurrence(value, "UTC", stamp("2027-02-01T00:00:00+00:00")),
                         stamp("2027-03-30T12:30:00+00:00"))

    def test_dst_missing_hour_skips_date_repeated_hour_fires_once(self):
        value = alarm(time="02:30")
        self.assertEqual(next_occurrence(value, "Europe/Berlin", stamp("2026-03-29T00:00:00+00:00")),
                         stamp("2026-03-30T00:30:00+00:00"))
        first = next_occurrence(value, "Europe/Berlin", stamp("2026-10-25T00:00:00+00:00"))
        self.assertEqual(first, stamp("2026-10-25T00:30:00+00:00"))
        self.assertEqual(next_occurrence(value, "Europe/Berlin", first), stamp("2026-10-26T01:30:00+00:00"))

    def test_validation_rejects_invalid_ids_dates_and_times(self):
        for changes in ({"source_ids": []}, {"source_ids": [999]}, {"source_ids": [True]},
                        {"time": "24:00"}, {"mode": "monthly", "month_days": "31"},
                        {"mode": "monthly", "month_days": "1,,2"}, {"weekdays": []},
                        {"enabled": "true"}, {"weekdays": [0]}, {"name": ""}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_schedule(alarm(**changes), [1, 2])


class SchedulerPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "schedules.sqlite3"
        self.now = stamp("2026-11-04T09:00:00+00:00")
        self.launched = []
        self.busy = False
        def launch(value):
            if self.busy:
                return False
            self.launched.append(self.scheduler.begin(value["id"]))
            self.busy = True
            return True
        self.launch = launch
        self.scheduler = Scheduler(self.path, launch=launch, clock=lambda: self.now)

    def tearDown(self):
        self.scheduler.close()
        self.tmp.cleanup()

    def test_due_alarm_persists_before_launch_and_runs_once(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.now = value["next_run"]
        self.assertTrue(self.scheduler.tick())
        self.assertEqual(len(self.launched), 1)
        self.assertEqual(self.launched[0]["source_ids"], [1, 2])
        self.assertEqual(self.scheduler.list()["schedules"][0]["last_run"]["status"], "running")
        self.scheduler.tick()
        self.assertEqual(len(self.launched), 1)
        self.scheduler.finish(self.launched[0]["id"], "complete", "Готово")
        self.busy = False
        self.scheduler.tick()
        self.assertEqual(len(self.launched), 1)

    def test_busy_queue_survives_restart_and_is_not_lost(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.busy = True
        self.now = value["next_run"]
        self.assertFalse(self.scheduler.tick())
        self.assertTrue(self.scheduler.list()["schedules"][0]["queued"])
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 60)
        self.busy = False
        self.assertTrue(self.scheduler.tick())
        self.assertEqual(self.launched[0]["scheduled_for"], self.now)

    def test_missed_time_while_off_is_not_caught_up(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.scheduler.close()
        self.now = value["next_run"] + 3600
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now)
        self.assertFalse(self.scheduler.tick())
        self.assertEqual(self.launched, [])
        self.assertGreater(self.scheduler.list()["schedules"][0]["next_run"], self.now)

    def test_restarted_in_flight_occurrence_can_be_resumed(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.now = value["next_run"]
        self.scheduler.tick()
        identifier = self.launched[0]["id"]
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 10)
        self.assertEqual(self.scheduler.occurrence(identifier)["status"], "queued")
        resumed = self.scheduler.begin(identifier)
        self.assertEqual(resumed["source_ids"], [1, 2])

    def test_manual_pause_stays_paused_after_restart(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.now = value["next_run"]
        self.scheduler.tick()
        identifier = self.launched[0]["id"]
        self.scheduler.finish(identifier, "paused", "Пауза")
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 10)
        self.assertEqual(self.scheduler.occurrence(identifier)["status"], "paused")
        self.assertFalse(self.scheduler.tick())
        self.assertEqual(self.scheduler.begin(identifier)["status"], "running")

    def test_disabled_auto_resume_durably_pauses_running_and_queued_occurrences(self):
        first = self.scheduler.save(alarm(), [1, 2])
        self.scheduler.save(alarm(name="Следующий"), [1, 2])
        self.now = first["next_run"]
        self.scheduler.tick()  # One running occurrence, one waiting behind it.
        identifiers = list(self.scheduler._occurrences)
        self.assertEqual({self.scheduler.occurrence(key)["status"] for key in identifiers}, {"running", "queued"})
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 10,
                                   resume_interrupted=False)
        self.busy = False
        self.assertFalse(self.scheduler.tick())
        self.assertEqual({self.scheduler.occurrence(key)["status"] for key in identifiers}, {"paused"})
        self.assertTrue(all(row["last_run"]["status"] == "paused" for row in self.scheduler.list()["schedules"]))
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 20,
                                   resume_interrupted=True)
        self.assertFalse(self.scheduler.tick())
        self.assertEqual({self.scheduler.occurrence(key)["status"] for key in identifiers}, {"paused"})
        self.now = first["next_run"] + 86400
        self.assertTrue(self.scheduler.tick())  # Future alarms remain enabled.
        self.assertEqual(len(self.launched), 2)

    def test_removing_source_changes_paused_scope_and_reopens_safely(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.now = value["next_run"]
        self.scheduler.tick()
        identifier = self.launched[0]["id"]
        self.scheduler.finish(identifier, "paused", "Пауза")
        self.scheduler.remove_source(1)
        self.assertEqual(self.scheduler.occurrence(identifier)["source_ids"], [2])
        self.assertEqual(self.scheduler.list()["schedules"][0]["last_run"]["source_ids"], [2])
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 10)
        self.assertEqual(self.scheduler.begin(identifier)["source_ids"], [2])
        self.scheduler.finish(identifier, "paused", "Пауза")
        self.scheduler.remove_source(2)
        self.assertEqual(self.scheduler.occurrence(identifier)["status"], "cancelled")
        with self.assertRaises(ValueError):
            self.scheduler.begin(identifier)

    def test_disabled_and_removed_alarms_cancel_queued_not_running(self):
        first = self.scheduler.save(alarm(), [1, 2])
        second = self.scheduler.save(alarm(name="Второй"), [1, 2])
        self.busy = True
        self.now = first["next_run"]
        self.scheduler.tick()
        self.assertEqual(self.scheduler.summary()["queued"], 2)
        self.scheduler.toggle(first["id"], False)
        self.assertEqual(self.scheduler.summary()["queued"], 1)
        self.scheduler.remove(second["id"])
        self.assertEqual(self.scheduler.summary()["queued"], 0)
        self.assertEqual(self.scheduler.summary()["enabled"], 0)

    def test_removed_source_updates_alarm_and_empty_scope_disables(self):
        self.scheduler.save(alarm(), [1, 2])
        self.scheduler.remove_source(1)
        self.assertEqual(self.scheduler.list()["schedules"][0]["source_ids"], [2])
        self.scheduler.remove_source(2)
        self.assertFalse(self.scheduler.list()["schedules"][0]["enabled"])
        self.assertIsNone(self.scheduler.list()["schedules"][0]["next_run"])

    def test_idle_list_summary_and_timer_ticks_never_open_database(self):
        self.scheduler.save(alarm(), [1, 2])
        with patch.object(self.scheduler, "_connect", side_effect=AssertionError("Idle disk access")):
            for _ in range(100):
                self.scheduler.list()
                self.scheduler.summary()
                self.scheduler.tick()

    def test_failed_write_does_not_create_a_phantom_alarm(self):
        with patch.object(self.scheduler, "_persist_schedule", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                self.scheduler.save(alarm(), [1, 2])
        self.assertEqual(self.scheduler.summary()["count"], 0)
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, clock=lambda: self.now)
        self.assertEqual(self.scheduler.summary()["count"], 0)

    def test_failed_due_record_rolls_back_memory_and_disk_before_launch(self):
        saved = self.scheduler.save(alarm(), [1, 2])
        self.now = saved["next_run"]
        with patch.object(self.scheduler, "_persist_occurrence", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                self.scheduler.tick()
        self.assertEqual(self.launched, [])
        self.assertEqual(self.scheduler.summary()["queued"], 0)
        self.assertEqual(self.scheduler.list()["schedules"][0]["next_run"], saved["next_run"])
        self.assertEqual(self.scheduler.list()["schedules"][0]["last_scheduled_for"], 0)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0], 0)
        self.assertTrue(self.scheduler.tick())
        self.assertEqual(len(self.launched), 1)

    def test_failed_edit_rolls_back_queued_scope_and_schedule_atomically(self):
        saved = self.scheduler.save(alarm(), [1, 2])
        self.now = saved["next_run"]
        self.busy = True
        self.scheduler.tick()
        previous = self.scheduler.list()
        identifier = next(iter(self.scheduler._occurrences))
        queued = self.scheduler.occurrence(identifier)
        with patch.object(self.scheduler, "_persist_occurrence", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                self.scheduler.save(alarm(id=saved["id"], source_ids=[2], time="15:10"), [1, 2])
        self.assertEqual(self.scheduler.list(), previous)
        self.assertEqual(self.scheduler.occurrence(identifier), queued)
        self.scheduler.close()
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now + 10)
        self.assertEqual(self.scheduler.occurrence(identifier)["source_ids"], [1, 2])
        self.assertEqual(self.scheduler.occurrence(identifier)["status"], "queued")
        self.assertEqual(self.scheduler.list()["schedules"][0]["time"], "12:30")

    def test_disk_failure_on_due_alarm_does_not_retry_at_fifty_milliseconds(self):
        saved = self.scheduler.save(alarm(), [1, 2])
        self.now = saved["next_run"]
        waits = []

        def stop_after_wait(timeout=None):
            waits.append(timeout)
            self.scheduler._stopping = True

        with patch.object(self.scheduler, "_connect", side_effect=sqlite3.OperationalError("disk full")) as connect:
            with patch.object(self.scheduler._condition, "wait", side_effect=stop_after_wait):
                self.scheduler._loop()
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(waits, [60])
        self.assertEqual(self.launched, [])
        self.assertIn("OperationalError", self.scheduler.summary()["error"])

    def test_controller_wake_between_tick_and_wait_is_not_lost(self):
        calls, waits = [], []

        def tick():
            calls.append(1)
            if len(calls) == 1:
                self.scheduler.wake()
            else:
                self.scheduler._stopping = True

        with patch.object(self.scheduler, "tick", side_effect=tick):
            with patch.object(self.scheduler._condition, "wait", side_effect=lambda timeout=None: waits.append(timeout)):
                self.scheduler._loop()
        self.assertEqual(len(calls), 2)
        self.assertEqual(waits, [])

    def test_cancel_waiting_occurrences_keeps_future_alarm_enabled(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.busy = True
        self.now = value["next_run"]
        self.scheduler.tick()
        self.assertEqual(self.scheduler.cancel_queued("Пауза"), 1)
        self.assertEqual(self.scheduler.summary()["queued"], 0)
        self.assertEqual(self.scheduler.summary()["enabled"], 1)
        self.assertGreater(self.scheduler.list()["schedules"][0]["next_run"], self.now)

    def test_clock_rollback_and_restart_do_not_replay_completed_occurrence(self):
        value = self.scheduler.save(alarm(), [1, 2])
        self.now = value["next_run"]
        self.scheduler.tick()
        self.scheduler.finish(self.launched[0]["id"], "complete", "Готово")
        self.busy = False
        self.scheduler.close()
        self.now -= 600
        self.scheduler = Scheduler(self.path, launch=self.launch, clock=lambda: self.now)
        self.assertGreater(self.scheduler.list()["schedules"][0]["next_run"], value["next_run"])
        self.now += 600
        self.assertFalse(self.scheduler.tick())
        self.assertEqual(len(self.launched), 1)

    def test_many_alarms_have_no_product_count_limit(self):
        for i in range(110):
            self.scheduler.save(alarm(name=str(i)), [1, 2])
        self.assertEqual(self.scheduler.summary()["count"], 110)


if __name__ == "__main__":
    unittest.main()

"""Persistent scan/upload alarms; idle scheduling uses memory and a condition.

The controller owns execution and serializes it with manual work. Occurrences
are recorded before they start, so a restart cannot invent a second daily run.
Missed wall-clock alarms while the process was off are not caught up. Already
queued or running occurrences survive, along with their frozen source scope.
"""
from __future__ import annotations

import copy
import contextlib
from datetime import date, datetime, time as daytime, timedelta, timezone
import json
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc


def checked_timezone(value):
    if not isinstance(value, str) or not value or len(value) > 100:
        raise ValueError("Укажите часовой пояс IANA, например Europe/Moscow.")
    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError):
        raise ValueError("Неизвестный часовой пояс. Пример: Europe/Moscow.") from None
    return value


def validate_schedule(data, available_sources):
    if not isinstance(data, dict):
        raise ValueError("Некорректное расписание.")
    name = data.get("name", "Планировщик")
    if not isinstance(name, str) or not name.strip() or len(name) > 200 or "\x00" in name:
        raise ValueError("Укажите название планировщика, до 200 символов.")
    ids = data.get("source_ids")
    if not isinstance(ids, list) or not ids or any(type(value) is not int or value <= 0 for value in ids):
        raise ValueError("Выберите хотя бы одну исходную папку.")
    ids = sorted(set(ids))
    if set(ids) - set(available_sources):
        raise ValueError("Некоторые выбранные папки больше не подключены. Обновите список.")
    clock = data.get("time")
    if not isinstance(clock, str) or re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", clock) is None:
        raise ValueError("Укажите время в формате ЧЧ:ММ.")
    mode = data.get("mode", "weekly")
    if not isinstance(mode, str) or mode not in {"weekly", "monthly"}:
        raise ValueError("Выберите дни недели или числа месяца.")
    weekdays = data.get("weekdays", list(range(1, 8)))
    month_days = data.get("month_days", [])
    if isinstance(month_days, str):
        if not month_days.strip():
            month_days = []
        else:
            pieces = month_days.split(",")
            if any(re.fullmatch(r"\s*[0-9]{1,2}\s*", part) is None for part in pieces):
                raise ValueError("Числа месяца задаются через запятую, от 1 до 30.")
            month_days = [int(part) for part in pieces]
    for field, values, maximum in (("weekdays", weekdays, 7), ("month_days", month_days, 30)):
        if not isinstance(values, list) or any(type(value) is not int or not 1 <= value <= maximum for value in values):
            raise ValueError("Дни недели: 1–7; числа месяца: 1–30.")
    if not (weekdays if mode == "weekly" else month_days):
        raise ValueError("Выберите хотя бы один день запуска.")
    enabled = data.get("enabled", True)
    if type(enabled) is not bool:
        raise ValueError("Состояние планировщика должно быть включено или выключено.")
    return {"name": name.strip(), "source_ids": ids, "time": clock, "mode": mode,
            "weekdays": sorted(set(weekdays)), "month_days": sorted(set(month_days)), "enabled": enabled}


def next_occurrence(schedule, timezone_name, after):
    if not schedule.get("enabled") or not schedule.get("source_ids"):
        return None
    zone = ZoneInfo(timezone_name)
    first = datetime.fromtimestamp(after, UTC).astimezone(zone).date()
    hour, minute = map(int, schedule["time"].split(":"))
    for offset in range(370):
        day = first + timedelta(days=offset)
        matches = (day.isoweekday() in schedule["weekdays"] if schedule["mode"] == "weekly"
                   else day.day in schedule["month_days"])
        if not matches:
            continue
        wall = datetime.combine(day, daytime(hour, minute))
        candidates = set()
        for fold in (0, 1):
            aware = wall.replace(tzinfo=zone, fold=fold)
            timestamp = aware.timestamp()
            if datetime.fromtimestamp(timestamp, zone).replace(tzinfo=None) == wall:
                candidates.add(timestamp)
        # A repeated autumn hour fires once, at its first occurrence. A missing
        # spring hour is skipped instead of inventing a different local time.
        if candidates:
            candidate = min(candidates)
            if candidate > after:
                return candidate
    return None


class Scheduler:
    def __init__(self, path: Path, *, timezone_name="Europe/Moscow", launch=None, clock=time.time,
                 resume_interrupted=True):
        if type(resume_interrupted) is not bool:
            raise ValueError("Автопродолжение планировщика должно быть включено или выключено.")
        self.path = Path(path)
        if self.path.is_symlink():
            raise ValueError("Файл расписаний не должен быть символической ссылкой.")
        self.timezone = checked_timezone(timezone_name)
        self._clock = clock
        self._launch = launch or (lambda occurrence: False)
        self._condition = threading.Condition(threading.RLock())
        self._stopping = False
        self._wake_generation = 0
        self._error = None
        self._thread = None
        self._schedules = {}
        self._occurrences = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS occurrences(id TEXT PRIMARY KEY,schedule_id INTEGER NOT NULL,payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS occurrence_schedule ON occurrences(schedule_id);
            """)
            for row in db.execute("SELECT id,payload FROM schedules"):
                value = json.loads(row["payload"])
                value.update(id=row["id"], next_run=next_occurrence(value, self.timezone, max(clock(), value.get("last_scheduled_for", 0))))
                self._schedules[row["id"]] = value
            for row in db.execute("SELECT id,payload FROM occurrences").fetchall():
                value = json.loads(row["payload"])
                # The service restores an in-flight upload before starting this
                # timer. Otherwise a crash before _launch is retried as queued.
                previous_status = value.get("status")
                if previous_status in {"running", "queued"} and not resume_interrupted:
                    value.update(status="paused", message="Автопродолжение после перезапуска отключено. Запуск сохранён на паузе.")
                elif previous_status == "running":
                    value["status"] = "queued"
                self._occurrences[row["id"]] = value
                if value.get("status") != previous_status:
                    # A restart with auto-resume disabled is a durable pause:
                    # enabling the setting later must not revive this old run.
                    self._persist_occurrence(value, db)
                    schedule = self._schedules.get(value["schedule_id"])
                    last = (schedule or {}).get("last_run") or {}
                    if schedule is not None and (last.get("id") == value["id"]
                                                 or value["scheduled_for"] >= last.get("scheduled_for", 0)):
                        schedule["last_run"] = copy.deepcopy(value)
                        self._persist_schedule(schedule, db)
        self.path.chmod(0o600)

    @contextlib.contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextlib.contextmanager
    def _mutation(self):
        with self._condition:
            previous = (copy.deepcopy(self._schedules), copy.deepcopy(self._occurrences))
            try:
                with self._connect() as db:
                    yield db
            except BaseException:
                self._schedules, self._occurrences = previous
                raise

    def _persist_schedule(self, schedule, db):
        stored = {key: value for key, value in schedule.items() if key not in {"id", "next_run"}}
        db.execute("UPDATE schedules SET payload=? WHERE id=?", (json.dumps(stored, ensure_ascii=False), schedule["id"]))

    def _persist_occurrence(self, occurrence, db):
        db.execute("INSERT OR REPLACE INTO occurrences(id,schedule_id,payload) VALUES(?,?,?)", (
            occurrence["id"], occurrence["schedule_id"], json.dumps(occurrence, ensure_ascii=False)))

    def list(self):
        with self._condition:
            values = []
            for value in self._schedules.values():
                values.append({**copy.deepcopy(value), "queued": any(
                    item["schedule_id"] == value["id"] and item["status"] == "queued"
                    for item in self._occurrences.values())})
            return {"schedules": values, "timezone": self.timezone, "server_time": self._clock()}

    def summary(self):
        with self._condition:
            pending = [row for row in self._occurrences.values() if row["status"] == "queued"]
            future = [row["next_run"] for row in self._schedules.values() if row.get("next_run") is not None]
            return {"count": len(self._schedules), "enabled": sum(bool(row["enabled"]) for row in self._schedules.values()),
                    "queued": len(pending), "next_run": min(future) if future else None, "timezone": self.timezone, "error": self._error}

    def save(self, data, available_sources):
        value = validate_schedule(data, available_sources)
        identifier = data.get("id")
        if identifier is not None and (type(identifier) is not int or identifier <= 0):
            raise ValueError("Некорректный номер планировщика.")
        with self._mutation() as db:
            if identifier is None:
                identifier = db.execute("INSERT INTO schedules(payload) VALUES(?)", ("{}",)).lastrowid
            elif identifier not in self._schedules:
                raise ValueError("Планировщик не найден.")
            previous = self._schedules.get(identifier, {})
            value.update(id=identifier, last_run=previous.get("last_run"),
                         last_scheduled_for=previous.get("last_scheduled_for", 0))
            value["next_run"] = next_occurrence(value, self.timezone, max(self._clock(), value.get("last_scheduled_for", 0)))
            self._schedules[identifier] = value
            self._persist_schedule(value, db)
            # Edits replace future/queued plans, but never interrupt a running
            # upload. The next execution uses exactly the newly selected roots.
            for item in self._occurrences.values():
                if item["schedule_id"] == identifier and item["status"] == "queued":
                    item.update(status="cancelled", finished_at=self._clock(), message="Расписание изменено.")
                    self._persist_occurrence(item, db)
            self._condition.notify_all()
            return copy.deepcopy(value)

    def toggle(self, identifier, enabled):
        if type(identifier) is not int or type(enabled) is not bool:
            raise ValueError("Некорректное состояние планировщика.")
        with self._condition:
            value = self._schedules.get(identifier)
            if value is None:
                raise ValueError("Планировщик не найден.")
            return self.save({**value, "enabled": enabled}, value["source_ids"])

    def remove(self, identifier):
        if type(identifier) is not int:
            raise ValueError("Некорректный номер планировщика.")
        with self._mutation() as db:
            if identifier not in self._schedules:
                raise ValueError("Планировщик не найден.")
            db.execute("DELETE FROM schedules WHERE id=?", (identifier,))
            del self._schedules[identifier]
            for item in self._occurrences.values():
                if item["schedule_id"] == identifier and item["status"] == "queued":
                    item.update(status="cancelled", finished_at=self._clock(), message="Планировщик удалён.")
                    self._persist_occurrence(item, db)
            self._condition.notify_all()

    def remove_source(self, source_id):
        with self._mutation() as db:
            for row in self._schedules.values():
                if source_id not in row["source_ids"]:
                    continue
                row["source_ids"].remove(source_id)
                if not row["source_ids"]:
                    row["enabled"] = False
                    row["last_run"] = {"status": "cancelled", "message": "Все исходные папки удалены из приложения."}
                row["next_run"] = next_occurrence(row, self.timezone, max(self._clock(), row.get("last_scheduled_for", 0)))
                self._persist_schedule(row, db)
            for item in self._occurrences.values():
                if item["status"] in {"queued", "paused"} and source_id in item["source_ids"]:
                    item["source_ids"].remove(source_id)
                    if not item["source_ids"]:
                        item.update(status="cancelled", finished_at=self._clock(), message="Исходные папки удалены.")
                    self._persist_occurrence(item, db)
                    schedule = self._schedules.get(item["schedule_id"])
                    if schedule is not None and (schedule.get("last_run") or {}).get("id") == item["id"]:
                        schedule["last_run"] = copy.deepcopy(item)
                        self._persist_schedule(schedule, db)
            self._condition.notify_all()

    def cancel_queued(self, message="Запуск отменён пользователем."):
        with self._condition:
            if not any(item["status"] == "queued" for item in self._occurrences.values()):
                return 0
        with self._mutation() as db:
            count = 0
            for item in self._occurrences.values():
                if item["status"] != "queued":
                    continue
                item.update(status="cancelled", finished_at=self._clock(), message=message)
                self._persist_occurrence(item, db)
                schedule = self._schedules.get(item["schedule_id"])
                if schedule is not None:
                    schedule["last_run"] = copy.deepcopy(item)
                    self._persist_schedule(schedule, db)
                count += 1
            self._prune(db)
            self._condition.notify_all()
            return count

    def set_timezone(self, value):
        value = checked_timezone(value)
        with self._condition:
            self.timezone = value
            for row in self._schedules.values():
                row["next_run"] = next_occurrence(row, value, max(self._clock(), row.get("last_scheduled_for", 0)))
            self._condition.notify_all()

    def occurrence(self, identifier):
        with self._condition:
            return copy.deepcopy(self._occurrences.get(identifier))

    def begin(self, identifier):
        with self._mutation() as db:
            item = self._occurrences.get(identifier)
            if item is None or item["status"] not in {"queued", "paused"}:
                raise ValueError("Запуск планировщика больше не ожидает выполнения.")
            item.update(status="running", started_at=item.get("started_at") or self._clock())
            self._persist_occurrence(item, db)
            schedule = self._schedules.get(item["schedule_id"])
            if schedule is not None:
                schedule["last_run"] = copy.deepcopy(item)
                self._persist_schedule(schedule, db)
            return copy.deepcopy(item)

    def finish(self, identifier, status, message):
        if status not in {"complete", "error", "paused", "running"}:
            raise ValueError("Unknown occurrence result")
        with self._mutation() as db:
            item = self._occurrences.get(identifier)
            if item is None:
                return
            item.update(status=status, message=str(message)[:2000])
            if status in {"complete", "error"}:
                item["finished_at"] = self._clock()
            self._persist_occurrence(item, db)
            schedule = self._schedules.get(item["schedule_id"])
            if schedule is not None:
                schedule["last_run"] = copy.deepcopy(item)
                self._persist_schedule(schedule, db)
            self._prune(db)
            self._condition.notify_all()

    def _prune(self, db):
        # Ten finished occurrences per alarm; active and paused state survives.
        for identifier in {row["schedule_id"] for row in self._occurrences.values()}:
            history = sorted((row for row in self._occurrences.values() if row["schedule_id"] == identifier
                              and row["status"] in {"complete", "error", "cancelled"}),
                             key=lambda row: row.get("finished_at", 0), reverse=True)
            for row in history[10:]:
                db.execute("DELETE FROM occurrences WHERE id=?", (row["id"],))
                del self._occurrences[row["id"]]

    def tick(self, now=None):
        """Record alarms due now, then offer one pending run to the controller."""
        now = self._clock() if now is None else now
        with self._condition:
            due = [value for value in self._schedules.values() if value.get("next_run") is not None
                   and value["next_run"] <= now]
            if due:
                with self._mutation() as db:
                    for value in due:
                        scheduled_for = value["next_run"]
                        value["next_run"] = next_occurrence(value, self.timezone, now)
                        if scheduled_for <= value.get("last_scheduled_for", 0):
                            continue
                        value["last_scheduled_for"] = scheduled_for
                        self._persist_schedule(value, db)
                        if any(item["schedule_id"] == value["id"] and item["status"] in {"queued", "running"}
                               for item in self._occurrences.values()):
                            continue
                        item = {"id": uuid.uuid4().hex, "schedule_id": value["id"], "name": value["name"],
                                "source_ids": list(value["source_ids"]), "scheduled_for": scheduled_for,
                                "timezone": self.timezone, "status": "queued", "started_at": None,
                                "finished_at": None, "message": "Ожидает завершения текущей операции."}
                        self._occurrences[item["id"]] = item
                        self._persist_occurrence(item, db)
            pending = sorted((value for value in self._occurrences.values() if value["status"] == "queued"),
                             key=lambda value: (value["scheduled_for"], value["id"]))
            candidate = copy.deepcopy(pending[0]) if pending else None
        # Never call the controller while holding the scheduler condition.
        return bool(candidate is not None and self._launch(candidate))

    def wake(self):
        with self._condition:
            self._wake_generation += 1
            self._condition.notify_all()

    def start(self):
        with self._condition:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._loop, name="mail-cloud-scheduler", daemon=True)
            self._thread.start()

    def _loop(self):
        while True:
            with self._condition:
                if self._stopping:
                    return
                generation = self._wake_generation
            try:
                self.tick()
                self._error = None
            except Exception as error:
                # A storage error must not create an unrecorded execution. The
                # controller will surface an execution error; a later wake may
                # retry the still-pending record, without an idle disk poll.
                self._error = "Не удалось запустить расписание: " + type(error).__name__
            with self._condition:
                if self._stopping:
                    return
                if generation != self._wake_generation:
                    continue
                future = [row["next_run"] for row in self._schedules.values() if row.get("next_run") is not None]
                delay = max(0.05, min(60, min(future) - self._clock())) if future else None
                if self._error:
                    # A failed durable write leaves the alarm due. Retrying
                    # that past deadline every 50ms would hammer a full disk.
                    # An explicit wake can retry sooner; otherwise back off.
                    delay = 60
                # A minute-only in-memory clock check also handles NTP/manual
                # wall-clock changes. No file is opened by a healthy idle tick.
                self._condition.wait(timeout=delay)

    def close(self, *, wait=True):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
            worker = self._thread
        if wait and worker is not None and worker is not threading.current_thread():
            worker.join(timeout=3)

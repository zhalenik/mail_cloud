"""Persistent one-shot upload plans, independent of recurring upload alarms.

Packing is a deterministic two-dimensional best-fit-decreasing heuristic. Both
hard caps apply; oversized indivisible directories occupy a batch alone. A
second, bounded-memory pass balances the same number of regular batches when
that complete repack succeeds. It does not claim to solve optimal bin packing.

Calendar intervals are elapsed N*24 hours between *different* batches' actual
starts, combined with permitted local weekdays and a wall-clock start time.
Pausing/resuming one batch retains its original start and event identity. The
controller owns uploads; this module owns atomic state and a disk-idle timer.
A configured HH:MM denotes that local minute, allowing sub-minute timer jitter
while retaining the exact elapsed interval; larger delays move to a new slot.
"""
from __future__ import annotations

from collections import OrderedDict
import contextlib
import copy
from datetime import date, datetime, time as daytime, timedelta, timezone
import json
import math
import re
import sqlite3
import threading
import time
import uuid
from zoneinfo import ZoneInfo

from .plan_inventory import _identifier, _page, _private_file
from .scheduler import checked_timezone
from .transport import Cancelled

UTC = timezone.utc
MAX_BATCHES = 10000
MAX_BATCH_MEMBERS = 50000
_MAX_INTEGER = 2**63 - 1
_TERMINAL = {'completed', 'completed_with_errors'}
_UNFINISHED = {'queued', 'running', 'paused', 'deleting'}
_UNSET = object()


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def validate_settings(data):
    if not isinstance(data, dict):
        raise ValueError('Некорректные настройки плана.')
    result = {}
    for name, default, low, high, label in (
        ('target_count', 10000, 1, 1000000000, 'Количество файлов'),
        ('target_bytes', 20 * 1024**3, 1, _MAX_INTEGER // 2, 'Размер файлов'),
        ('tolerance_percent', 15, 0, 100, 'Допустимое превышение'),
        ('interval_days', 1, 1, 3660, 'Интервал в сутках'),
    ):
        value = data.get(name, default)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f'{label}: укажите целое число от {low} до {high}.')
        result[name] = value
    weekdays = data.get('weekdays', list(range(1, 8)))
    if not isinstance(weekdays, list) or not weekdays or any(type(v) is not int or not 1 <= v <= 7 for v in weekdays):
        raise ValueError('Выберите хотя бы один день недели: 1–7.')
    result['weekdays'] = sorted(set(weekdays))
    wall_time = data.get('time', '02:00')
    if not isinstance(wall_time, str) or re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]', wall_time) is None:
        raise ValueError('Укажите время в формате ЧЧ:ММ.')
    result['time'] = wall_time
    result['timezone'] = checked_timezone(data.get('timezone', 'Europe/Moscow'))
    start = data.get('start_date')
    if not isinstance(start, str) or re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', start) is None:
        raise ValueError('Укажите первую дату плана в формате ГГГГ-ММ-ДД.')
    try:
        date.fromisoformat(start)
    except ValueError:
        raise ValueError('Первая дата плана не существует.') from None
    result['start_date'] = start
    return result


def _wall_candidates(wall, zone):
    result = set()
    for fold in (0, 1):
        timestamp = wall.replace(tzinfo=zone, fold=fold).timestamp()
        if datetime.fromtimestamp(timestamp, zone).replace(tzinfo=None) == wall:
            result.add(timestamp)
    return sorted(result)


def parse_local_datetime(value, timezone_name):
    """Parse an explicit wall time; missing DST times reject, repeated use first."""
    checked_timezone(timezone_name)
    if not isinstance(value, str) or re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]', value) is None:
        raise ValueError('Укажите местную дату и время: ГГГГ-ММ-ДДТЧЧ:ММ.')
    try:
        wall = datetime.fromisoformat(value)
        candidates = _wall_candidates(wall, ZoneInfo(timezone_name))
    except (ValueError, OverflowError, OSError):
        raise ValueError('Некорректная местная дата и время.') from None
    if not candidates:
        raise ValueError('Это местное время отсутствует из-за перевода часов. Выберите другое время.')
    return candidates[0]


def next_allowed(settings, earliest):
    """First eligible local minute, carrying seconds inside that minute.

    Choosing max(slot start, earliest) within a 60-second window avoids losing a
    whole day to timer jitter while still obeying N*24 elapsed hours exactly.
    """
    zone = ZoneInfo(settings['timezone'])
    try:
        first = max(datetime.fromtimestamp(earliest, zone).date(), date.fromisoformat(settings['start_date']))
        hour, minute = map(int, settings['time'].split(':'))
        for offset in range(15):
            day = first + timedelta(days=offset)
            if day.isoweekday() not in settings['weekdays']:
                continue
            candidates = _wall_candidates(datetime.combine(day, daytime(hour, minute)), zone)
            # Do not fire twice in the repeated autumn hour.
            if candidates and earliest < candidates[0] + 60:
                return max(candidates[0], earliest)
    except (ValueError, OverflowError, OSError):
        raise ValueError('Расписание выходит за поддерживаемый диапазон дат. Уменьшите интервал или число партий.') from None
    raise ValueError('Не удалось подобрать дату запуска. Проверьте дни недели и часовой пояс.')


def _minute_end(timestamp, timezone_name):
    local = datetime.fromtimestamp(timestamp, ZoneInfo(timezone_name))
    return local.replace(second=0, microsecond=0).timestamp() + 60


def _epoch(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('Некорректное время запуска.')
    try:
        datetime.fromtimestamp(value, UTC)
    except (ValueError, OverflowError, OSError):
        raise ValueError('Некорректное время запуска.') from None
    return float(value)


class PlanDeferred(ValueError):
    """A missed clock slot was durably moved; normal timer deferral, not failure."""


class PlanStore:
    """Plan summaries and only the first unfinished batch are cached in memory.

    All SQLite connections belong to the calling thread. Large build work uses
    a private connection and staged generations; publication is atomic, and UI
    summary reads do not wait for packing. No disk reads occur in an idle timer.
    """

    def __init__(self, path, *, clock=time.time, launch=None, resume_interrupted=True):
        if type(resume_interrupted) is not bool:
            raise ValueError('Автопродолжение должно быть включено или выключено.')
        self.path = _private_file(path)
        self._clock = clock
        self._launch = launch or (lambda batch: False)
        self._condition = threading.Condition(threading.RLock())
        self._summaries = {}
        self._heads = {}
        self._pages = OrderedDict()
        self._building = set()
        self._thread = None
        self._stopping = False
        self._launching = False
        self._wake_generation = 0
        self._error = None
        with self._connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS plans(
                    id TEXT PRIMARY KEY,inventory_id TEXT NOT NULL,root TEXT NOT NULL,name TEXT NOT NULL,
                    remote_prefix TEXT NOT NULL,settings TEXT NOT NULL,status TEXT NOT NULL,
                    inventory_revision INTEGER,revision INTEGER NOT NULL DEFAULT 0,completion_cycle INTEGER NOT NULL DEFAULT 0,
                    generation TEXT,building_generation TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL,
                    message TEXT NOT NULL DEFAULT '');
                CREATE INDEX IF NOT EXISTS plans_recent ON plans(created_at DESC,id);
                CREATE TABLE IF NOT EXISTS batches(
                    id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,generation TEXT NOT NULL,
                    position INTEGER NOT NULL,name TEXT NOT NULL,status TEXT NOT NULL,
                    scheduled_for REAL,requested_for REAL,started_at REAL,finished_at REAL,
                    run_key TEXT NOT NULL,attempt INTEGER NOT NULL DEFAULT 0,source_id INTEGER,
                    file_count INTEGER NOT NULL DEFAULT 0,total_bytes INTEGER NOT NULL DEFAULT 0,
                    oversized INTEGER NOT NULL DEFAULT 0,member_count INTEGER NOT NULL DEFAULT 0,
                    members_preview TEXT NOT NULL DEFAULT '[]',summary TEXT NOT NULL DEFAULT '{}',
                    message TEXT NOT NULL DEFAULT '',interrupted INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS batches_order ON batches(plan_id,generation,position,id);
                CREATE INDEX IF NOT EXISTS batches_state ON batches(plan_id,generation,status,position,id);
                CREATE INDEX IF NOT EXISTS batches_pack_count ON batches(plan_id,generation,oversized,file_count DESC,total_bytes);
                CREATE INDEX IF NOT EXISTS batches_pack_bytes ON batches(plan_id,generation,oversized,total_bytes DESC,file_count);
                CREATE UNIQUE INDEX IF NOT EXISTS batches_source ON batches(source_id) WHERE source_id IS NOT NULL;
                CREATE TABLE IF NOT EXISTS units(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,plan_id TEXT NOT NULL,generation TEXT NOT NULL,batch_id TEXT,
                    member TEXT NOT NULL,file_count INTEGER NOT NULL,total_bytes INTEGER NOT NULL,load REAL NOT NULL,
                    sort_path TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS units_pack ON units(plan_id,generation,load DESC,total_bytes DESC,file_count DESC,sort_path,id);
                CREATE INDEX IF NOT EXISTS units_batch ON units(batch_id,id);
                CREATE INDEX IF NOT EXISTS units_generation ON units(plan_id,generation,id);
                CREATE TABLE IF NOT EXISTS plan_events(
                    key TEXT PRIMARY KEY,batch_id TEXT NOT NULL,plan_id TEXT NOT NULL,run_key TEXT NOT NULL,
                    payload TEXT NOT NULL,created_at REAL NOT NULL,acknowledged INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS plan_events_pending ON plan_events(acknowledged,created_at,key);
            ''')
            if 'completion_cycle' not in {row[1] for row in db.execute('PRAGMA table_info(plans)')}:
                db.execute('ALTER TABLE plans ADD COLUMN completion_cycle INTEGER NOT NULL DEFAULT 0')
            # A running upload is restored by the controller, never by a timer
            # racing a manual-pause intent persisted in its own activity journal.
            db.execute("UPDATE batches SET status='paused',interrupted=1,message=? WHERE status='running'",
                       ('Загрузка прервана остановкой приложения; состояние партии сохранено.',))
            db.execute("UPDATE batches SET status='paused',interrupted=0,message=? WHERE status='deleting'",
                       ('Очистка прервана. Проверьте журнал удаления перед продолжением партии.',))
            for row in db.execute("SELECT id,building_generation FROM plans WHERE status='building'").fetchall():
                if row['building_generation']:
                    self._drop_generation(db, row['id'], row['building_generation'])
                db.execute("UPDATE plans SET status='draft',building_generation=NULL,inventory_revision=NULL,message=? WHERE id=?",
                           ('Построение прервано. Сформируйте план заново.', row['id']))
            # A one-shot batch is not lost while the application is off, but
            # a missed slot must not run immediately on a forbidden weekday.
            for row in db.execute("SELECT * FROM plans WHERE status='active'").fetchall():
                head = db.execute("SELECT status,scheduled_for FROM batches WHERE plan_id=? AND generation=? "
                                  "AND status IN ('queued','running','paused','deleting') ORDER BY position,id LIMIT 1",
                                  (row['id'], row['generation'])).fetchone()
                settings = json.loads(row['settings'])
                if head is not None and head['status'] == 'queued' and (head['scheduled_for'] is None or self._clock() >= _minute_end(head['scheduled_for'], settings['timezone'])):
                    self._calendar(db, row['id'], row['generation'], settings, now=self._clock())
                    self._touch(db, row['id'], self._clock())
            db.commit()
            for row in db.execute("SELECT id FROM plans WHERE status!='deleted'").fetchall():
                self._refresh(db, row['id'])

    @contextlib.contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA cache_size=-4096')
        db.execute('PRAGMA temp_store=FILE')
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextlib.contextmanager
    def _mutation(self, plan_id=None):
        with self._condition:
            with self._connect() as db:
                db.execute('BEGIN IMMEDIATE')
                yield db
                db.commit()
                if plan_id is not None:
                    self._refresh(db, plan_id)
            self._wake_generation += 1
            self._condition.notify_all()

    @staticmethod
    def _row_plan(db, identifier):
        row = db.execute("SELECT * FROM plans WHERE id=? AND status!='deleted'", (_identifier(identifier),)).fetchone()
        if row is None:
            raise ValueError('План загрузки не найден.')
        return row

    @staticmethod
    def _row_batch(db, identifier):
        row = db.execute("SELECT b.* FROM batches b JOIN plans p ON p.id=b.plan_id AND p.generation=b.generation "
                         "WHERE b.id=? AND b.status!='deleted' AND p.status!='deleted'", (_identifier(identifier),)).fetchone()
        if row is None:
            raise ValueError('Партия загрузки не найдена.')
        return row

    @staticmethod
    def _batch(row):
        value = dict(row)
        value.pop('generation', None)
        for name in ('members_preview', 'summary'):
            value[name] = json.loads(value[name])
        for name in ('oversized', 'interrupted'):
            value[name] = bool(value[name])
        value['owner_key'] = 'plan-batch:' + value['id']
        return value

    def _refresh(self, db, identifier):
        row = db.execute('SELECT * FROM plans WHERE id=?', (identifier,)).fetchone()
        if row is None or row['status'] == 'deleted':
            self._summaries.pop(identifier, None)
            self._heads.pop(identifier, None)
            self._pages.clear()
            return
        value = dict(row)
        value['settings'] = json.loads(value['settings'])
        value.pop('building_generation', None)
        value.pop('generation', None)
        aggregate = db.execute("SELECT COUNT(*) AS batch_count,COALESCE(SUM(file_count),0) AS file_count,"
                               "COALESCE(SUM(total_bytes),0) AS total_bytes,"
                               "COALESCE(SUM(status IN ('completed','completed_with_errors')),0) AS completed_count,"
                               "COALESCE(SUM(status='completed_with_errors'),0) AS errors_count,"
                               "COALESCE(SUM(status='queued'),0) AS queued_count,"
                               "COALESCE(SUM(status='running'),0) AS running_count,"
                               "COALESCE(SUM(status IN ('paused','deleting')),0) AS paused_count,"
                               "COALESCE(SUM(oversized),0) AS oversized_count FROM batches "
                               "WHERE plan_id=? AND generation=? AND status!='deleted'", (identifier, row['generation'])).fetchone()
        value.update(dict(aggregate))
        head = db.execute("SELECT * FROM batches WHERE plan_id=? AND generation=? "
                          "AND status IN ('queued','running','paused','deleting') ORDER BY position,id LIMIT 1",
                          (identifier, row['generation'])).fetchone()
        value['next_run'] = head['scheduled_for'] if head is not None and head['status'] == 'queued' and row['status'] == 'active' else None
        value['progress_percent'] = round(100 * value['completed_count'] / value['batch_count'], 1) if value['batch_count'] else 0
        value['warnings'] = []
        if value['oversized_count']:
            value['warnings'].append(f"Папок сверх ограничения: {value['oversized_count']}. Каждая выделена в отдельную партию.")
        if row['status'] == 'active':
            if head is None:
                value['status'] = 'completed_with_errors' if value['errors_count'] else 'completed'
            elif head['status'] in {'paused', 'deleting'}:
                value['status'] = 'paused'
        self._summaries[identifier] = value
        if head is not None and row['status'] == 'active':
            self._heads[identifier] = self._batch(head)
        else:
            self._heads.pop(identifier, None)
        self._pages.clear()

    @staticmethod
    def _touch(db, identifier, now, message=None):
        if message is None:
            db.execute('UPDATE plans SET revision=revision+1,updated_at=? WHERE id=?', (now, identifier))
        else:
            db.execute('UPDATE plans SET revision=revision+1,updated_at=?,message=? WHERE id=?', (now, message, identifier))

    def plan(self, identifier):
        identifier = _identifier(identifier)
        with self._condition:
            if identifier not in self._summaries:
                raise ValueError('План загрузки не найден.')
            return copy.deepcopy(self._summaries[identifier])

    def list(self, *, limit=50, offset=0):
        _page(limit, offset)
        with self._condition:
            rows = sorted(self._summaries.values(), key=lambda value: (value['created_at'], value['id']), reverse=True)
            return {'plans': copy.deepcopy(rows[offset:offset + limit]), 'total': len(rows), 'offset': offset, 'limit': limit}

    def summary(self):
        with self._condition:
            heads = list(self._heads.values())
            future = [row['scheduled_for'] for row in heads if row['status'] == 'queued' and row['scheduled_for'] is not None]
            return {'count': len(self._summaries), 'active': len(heads),
                    'running': sum(row['status'] == 'running' for row in heads),
                    'paused': sum(row['status'] in {'paused', 'deleting'} for row in heads),
                    'next_run': min(future) if future else None, 'error': self._error}

    def batches(self, plan_id, *, limit=50, offset=0):
        _page(limit, offset)
        with self._condition:
            plan = self.plan(plan_id)
            key = (plan_id, plan['revision'], limit, offset)
            if key in self._pages:
                self._pages.move_to_end(key)
                return copy.deepcopy(self._pages[key])
            # Reads are deliberately short; build never holds this condition.
            with self._connect() as db:
                raw = self._row_plan(db, plan_id)
                rows = db.execute("SELECT * FROM batches WHERE plan_id=? AND generation=? AND status!='deleted' "
                                  'ORDER BY position,id LIMIT ? OFFSET ?', (plan_id, raw['generation'], limit, offset))
                value = {'batches': [self._batch(row) for row in rows], 'total': plan['batch_count'], 'limit': limit, 'offset': offset}
            self._pages[key] = value
            while len(self._pages) > 12:
                self._pages.popitem(last=False)
            return copy.deepcopy(value)

    def batch(self, identifier):
        with self._connect() as db:
            return self._batch(self._row_batch(db, identifier))

    def iter_members(self, batch_id, *, batch_size=200):
        _page(batch_size, 0, 1000)
        with self._connect() as db:
            self._row_batch(db, batch_id)
            after = 0
            while True:
                rows = db.execute('SELECT id,member FROM units WHERE batch_id=? AND id>? ORDER BY id LIMIT ?',
                                  (batch_id, after, batch_size)).fetchall()
                if not rows:
                    break
                for row in rows:
                    after = row['id']
                    yield json.loads(row['member'])

    @staticmethod
    def _name(value):
        if not isinstance(value, str) or not value.strip() or len(value) > 200 or '\x00' in value:
            raise ValueError('Укажите название плана, до 200 символов.')
        return value.strip()

    @staticmethod
    def _remote(value):
        if not isinstance(value, str) or not value.startswith('/') or '\x00' in value or '\\' in value or len(value) > 4096:
            raise ValueError('Укажите абсолютный путь папки в Облаке, например /Фото.')
        if any(part in {'.', '..'} for part in value.split('/')):
            raise ValueError('Путь в Облаке не должен содержать . или ...')
        return '/' + '/'.join(part for part in value.split('/') if part)

    def create(self, inventory_id, root, name, settings, remote_prefix='/PhotoArchivePC'):
        _identifier(inventory_id)
        if not isinstance(root, str) or not root or '\x00' in root:
            raise ValueError('Укажите корневой каталог.')
        name, settings, remote_prefix = self._name(name), validate_settings(settings), self._remote(remote_prefix)
        identifier, now = uuid.uuid4().hex, self._clock()
        with self._mutation(identifier) as db:
            db.execute('INSERT INTO plans(id,inventory_id,root,name,remote_prefix,settings,status,created_at,updated_at) '
                       "VALUES(?,?,?,?,?,?,'draft',?,?)", (identifier, inventory_id, root, name, remote_prefix, _json(settings), now, now))
        return self.plan(identifier)

    def update_draft(self, identifier, *, inventory_id=None, root=None, name=None, settings=None, remote_prefix=None):
        with self._mutation(identifier) as db:
            row = self._row_plan(db, identifier)
            if row['status'] != 'draft' or (inventory_id is not None and inventory_id != row['inventory_id']):
                raise ValueError('Изменять предварительный план можно только для того же обзора до активации.')
            if root is not None and root != row['root']:
                raise ValueError('Корневая папка предварительного плана не может измениться.')
            db.execute('UPDATE plans SET name=?,settings=?,remote_prefix=?,inventory_revision=NULL WHERE id=?',
                       (self._name(name) if name is not None else row['name'],
                        _json(validate_settings(settings)) if settings is not None else row['settings'],
                        self._remote(remote_prefix) if remote_prefix is not None else row['remote_prefix'], identifier))
            self._touch(db, identifier, self._clock())
        return self.plan(identifier)

    @staticmethod
    def _drop_generation(db, plan_id, generation):
        db.execute('DELETE FROM units WHERE plan_id=? AND generation=?', (plan_id, generation))
        db.execute('DELETE FROM batches WHERE plan_id=? AND generation=?', (plan_id, generation))

    @staticmethod
    def _check(stop):
        if stop is not None and stop.is_set():
            raise Cancelled('Построение плана остановлено.')

    def _new_bin(self, db, plan_id, generation, position, *, oversized=False):
        if position >= MAX_BATCHES:
            raise ValueError(f'Получается более {MAX_BATCHES} партий. Увеличьте желаемый объём партии или разделите корневой каталог на несколько планов.')
        identifier = uuid.uuid4().hex
        db.execute('INSERT INTO batches(id,plan_id,generation,position,name,status,run_key,oversized) '
                   "VALUES(?,?,?,?,?,'queued',?,?)", (identifier, plan_id, generation, position, f'Партия {position + 1}', uuid.uuid4().hex, int(oversized)))
        return identifier

    @staticmethod
    def _pack_candidates(db, plan_id, generation, count, size, cap_count, cap_bytes):
        # Two indexed extremes give bounded candidate memory. Both queries
        # filter both caps before LIMIT, so an existing feasible bin is found.
        rows = {}
        for column in ('file_count', 'total_bytes'):
            query = ('SELECT id,position,file_count,total_bytes FROM batches WHERE plan_id=? AND generation=? AND oversized=0 '
                     f'AND member_count<? AND file_count<=? AND total_bytes<=? ORDER BY {column} DESC LIMIT 32')
            for row in db.execute(query, (plan_id, generation, MAX_BATCH_MEMBERS, cap_count - count, cap_bytes - size)):
                rows[row['id']] = row
        return rows.values()

    def _pack(self, db, plan_id, generation, settings, stop, progress):
        cap_count = settings['target_count'] * (100 + settings['tolerance_percent']) // 100
        cap_bytes = settings['target_bytes'] * (100 + settings['tolerance_percent']) // 100
        bins = seen = 0
        # A separate cursor reads at most 256 units at a time; there is no list
        # of source files or directories in Python memory.
        cursor = db.execute('SELECT id,file_count,total_bytes FROM units WHERE plan_id=? AND generation=? '
                            'ORDER BY load DESC,total_bytes DESC,file_count DESC,sort_path,id', (plan_id, generation))
        while rows := cursor.fetchmany(256):
            self._check(stop)
            for row in rows:
                oversized = row['file_count'] > cap_count or row['total_bytes'] > cap_bytes
                candidates = () if oversized else self._pack_candidates(db, plan_id, generation, row['file_count'], row['total_bytes'], cap_count, cap_bytes)
                chosen = max(candidates, key=lambda b: (
                    max((b['file_count'] + row['file_count']) / cap_count, (b['total_bytes'] + row['total_bytes']) / cap_bytes),
                    b['file_count'] / cap_count + b['total_bytes'] / cap_bytes, -b['position']), default=None)
                if chosen is None:
                    batch_id = self._new_bin(db, plan_id, generation, bins, oversized=oversized)
                    bins += 1
                else:
                    batch_id = chosen['id']
                db.execute('UPDATE units SET batch_id=? WHERE id=?', (batch_id, row['id']))
                db.execute('UPDATE batches SET file_count=file_count+?,total_bytes=total_bytes+?,member_count=member_count+1 WHERE id=?',
                           (row['file_count'], row['total_bytes'], batch_id))
                seen += 1
            db.commit()
            if progress:
                progress({'phase': 'grouping', 'units': seen, 'batch_count': bins})
        self._balance(db, plan_id, generation, settings, cap_count, cap_bytes, stop)
        for row in db.execute('SELECT id FROM batches WHERE plan_id=? AND generation=?', (plan_id, generation)).fetchall():
            preview = [{key: value for key, value in json.loads(unit['member']).items() if key != 'excluded_paths'} for unit in db.execute('SELECT member FROM units WHERE batch_id=? ORDER BY id LIMIT 3', (row['id'],))]
            db.execute('UPDATE batches SET members_preview=? WHERE id=?', (_json(preview), row['id']))
        return bins

    def _balance(self, db, plan_id, generation, settings, cap_count, cap_bytes, stop):
        """Try a least-loaded repack with identical bin count; keep only success.

        Temporary assignments live in SQLite. A failed attempt leaves the first
        pass intact, including the dedicated oversized bins.
        """
        regular = db.execute('SELECT COUNT(*) FROM batches WHERE plan_id=? AND generation=? AND oversized=0', (plan_id, generation)).fetchone()[0]
        if regular <= 1:
            return
        db.executescript('CREATE TEMP TABLE balance_bins(id TEXT PRIMARY KEY,position INTEGER NOT NULL,member_count INTEGER NOT NULL DEFAULT 0,file_count INTEGER NOT NULL DEFAULT 0,total_bytes INTEGER NOT NULL DEFAULT 0,load REAL NOT NULL DEFAULT 0);'
                         'CREATE INDEX balance_load ON balance_bins(load,position);'
                         'CREATE TEMP TABLE balance_assign(unit_id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL);')
        db.execute('INSERT INTO balance_bins(id,position) SELECT id,position FROM batches WHERE plan_id=? AND generation=? AND oversized=0', (plan_id, generation))
        cursor = db.execute('SELECT u.id,u.file_count,u.total_bytes FROM units u JOIN batches b ON b.id=u.batch_id '
                            'WHERE u.plan_id=? AND u.generation=? AND b.oversized=0 '
                            'ORDER BY u.load DESC,u.total_bytes DESC,u.file_count DESC,u.sort_path,u.id', (plan_id, generation))
        success = True
        while rows := cursor.fetchmany(256):
            self._check(stop)
            for row in rows:
                chosen = db.execute('SELECT * FROM balance_bins WHERE member_count<? AND file_count<=? AND total_bytes<=? ORDER BY load,position LIMIT 1',
                                    (MAX_BATCH_MEMBERS, cap_count - row['file_count'], cap_bytes - row['total_bytes'])).fetchone()
                if chosen is None:
                    success = False
                    break
                count, size = chosen['file_count'] + row['file_count'], chosen['total_bytes'] + row['total_bytes']
                db.execute('UPDATE balance_bins SET file_count=?,total_bytes=?,load=?,member_count=member_count+1 WHERE id=?',
                           (count, size, max(count / settings['target_count'], size / settings['target_bytes']), chosen['id']))
                db.execute('INSERT INTO balance_assign(unit_id,batch_id) VALUES(?,?)', (row['id'], chosen['id']))
            if not success:
                break
        if success:
            before = db.execute('SELECT MIN(MAX(file_count*1.0/?,total_bytes*1.0/?)),MAX(MAX(file_count*1.0/?,total_bytes*1.0/?)) '
                                'FROM batches WHERE plan_id=? AND generation=? AND oversized=0',
                                (settings['target_count'], settings['target_bytes'], settings['target_count'], settings['target_bytes'], plan_id, generation)).fetchone()
            after = db.execute('SELECT MIN(load),MAX(load) FROM balance_bins').fetchone()
            if after[1] - after[0] < before[1] - before[0] - 1e-12:
                db.execute('UPDATE units SET batch_id=(SELECT batch_id FROM balance_assign WHERE unit_id=units.id) '
                           'WHERE id IN (SELECT unit_id FROM balance_assign)')
                db.execute('UPDATE batches SET file_count=(SELECT file_count FROM balance_bins WHERE id=batches.id),'
                           'total_bytes=(SELECT total_bytes FROM balance_bins WHERE id=batches.id),'
                           'member_count=(SELECT COUNT(*) FROM units WHERE batch_id=batches.id) WHERE id IN (SELECT id FROM balance_bins)')
        db.execute('DROP TABLE balance_assign')
        db.execute('DROP TABLE balance_bins')
        db.commit()

    def build(self, plan_id, inventory, *, stop=None, progress=None):
        with self._mutation(plan_id) as db:
            plan = self._row_plan(db, plan_id)
            if plan['status'] != 'draft' or plan_id in self._building:
                raise ValueError('Формировать партии можно только у неактивированного плана.')
            state = inventory.summary(plan['inventory_id'])
            if state['status'] != 'ready' or state['selected_errors'] or state['root'] != plan['root']:
                raise ValueError('Завершите обзор без ошибок в выбранных папках перед построением плана.')
            if not state['selected_count']:
                raise ValueError('В выбранных папках нет подходящих файлов.')
            generation = uuid.uuid4().hex
            settings = json.loads(plan['settings'])
            previous_generation = plan['generation']
            self._building.add(plan_id)
            db.execute("UPDATE plans SET status='building',building_generation=?,message='' WHERE id=?", (generation, plan_id))
            self._touch(db, plan_id, self._clock())
        try:
            with self._connect() as db:
                seen = 0
                for unit in inventory.iter_units(plan['inventory_id']):
                    self._check(stop)
                    member = {key: unit[key] for key in ('path', 'remote_subdir', 'recursive', 'excluded_paths')}
                    db.execute('INSERT INTO units(plan_id,generation,member,file_count,total_bytes,load,sort_path) VALUES(?,?,?,?,?,?,?)',
                               (plan_id, generation, _json(member), unit['file_count'], unit['total_bytes'],
                                max(unit['file_count'] / settings['target_count'], unit['total_bytes'] / settings['target_bytes']), unit['relative_path']))
                    seen += 1
                    if seen % 500 == 0:
                        db.commit()
                        if progress:
                            progress({'phase': 'preparing', 'units': seen})
                db.commit()
                self._check(stop)
                count = self._pack(db, plan_id, generation, settings, stop, progress)
                if not count:
                    raise ValueError('В выбранных папках нет подходящих файлов.')
                if inventory.summary(plan['inventory_id'])['revision'] != state['revision']:
                    raise ValueError('Выбор папок изменился во время построения. Сформируйте план заново.')
                # Stage dates too, before making this generation visible.
                self._calendar(db, plan_id, generation, settings, now=self._clock(), clear_pins=True)
                db.commit()
            with self._mutation(plan_id) as db:
                current = self._row_plan(db, plan_id)
                if current['status'] != 'building' or current['building_generation'] != generation:
                    raise ValueError('Построение плана было отменено.')
                db.execute("UPDATE plans SET status='draft',generation=?,building_generation=NULL,inventory_revision=?,message='' WHERE id=?",
                           (generation, state['revision'], plan_id))
                self._touch(db, plan_id, self._clock())
                if previous_generation:
                    self._drop_generation(db, plan_id, previous_generation)
            return self.plan(plan_id)
        except BaseException as error:
            with self._mutation(plan_id) as db:
                self._drop_generation(db, plan_id, generation)
                db.execute("UPDATE plans SET status='draft',building_generation=NULL,inventory_revision=NULL,message=? WHERE id=?",
                           ('Построение остановлено.' if isinstance(error, Cancelled) else str(error)[:500], plan_id))
                self._touch(db, plan_id, self._clock())
            raise
        finally:
            with self._condition:
                self._building.discard(plan_id)

    @staticmethod
    def _calendar(db, plan_id, generation, settings, *, now, clear_pins=False):
        previous_start = None
        for row in db.execute("SELECT id,status,started_at,scheduled_for,requested_for FROM batches WHERE plan_id=? AND generation=? AND status!='deleted' ORDER BY position,id", (plan_id, generation)).fetchall():
            if row['status'] != 'queued':
                anchor = row['started_at'] if row['started_at'] is not None else row['scheduled_for']
                if anchor is not None:
                    previous_start = max(previous_start, anchor) if previous_start is not None else anchor
                continue
            earliest = max(now, previous_start + settings['interval_days'] * 86400 if previous_start is not None else now)
            requested = None if clear_pins else row['requested_for']
            if requested is not None and earliest < _minute_end(requested, settings['timezone']):
                scheduled = max(requested, earliest)
                message = ''
            else:
                scheduled = next_allowed(settings, earliest)
                message = 'Дата сдвинута: предыдущая партия или интервал не позволяют начать раньше.' if requested is not None else ''
            db.execute('UPDATE batches SET scheduled_for=?,requested_for=?,message=? WHERE id=?', (scheduled, requested, message, row['id']))
            previous_start = scheduled

    def activate(self, plan_id, inventory_revision=None):
        with self._mutation(plan_id) as db:
            plan = self._row_plan(db, plan_id)
            if plan['status'] == 'activating':
                return self.plan(plan_id)
            if plan['status'] != 'draft' or plan['generation'] is None or plan['inventory_revision'] is None:
                raise ValueError('Сначала сформируйте предварительный план.')
            if inventory_revision is not None and plan['inventory_revision'] != inventory_revision:
                raise ValueError('После построения изменился выбор папок. Сформируйте план заново.')
            db.execute("UPDATE plans SET status='activating' WHERE id=?", (plan_id,))
            self._calendar(db, plan_id, plan['generation'], json.loads(plan['settings']), now=self._clock())
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def bind_sources(self, plan_id, bindings):
        """Bind one bounded activation page with one aggregate/cache refresh.

        A page is all-or-nothing. Identical bindings can be replayed after a
        crash, including after activation; replacing an existing binding is
        never allowed. Source creation itself remains owned by the controller.
        """
        _identifier(plan_id)
        if not isinstance(bindings, list) or not 1 <= len(bindings) <= 200:
            raise ValueError('Привязывайте исходные папки страницами от 1 до 200 партий.')
        checked = []
        for value in bindings:
            if not isinstance(value, dict):
                raise ValueError('Укажите идентификаторы партии и исходной папки.')
            batch_id = _identifier(value.get('batch_id'))
            source_id = value.get('source_id')
            if type(source_id) is not int or not 1 <= source_id <= _MAX_INTEGER:
                raise ValueError('Некорректная исходная папка партии.')
            checked.append({'batch_id': batch_id, 'source_id': source_id})
        identifiers = [value['batch_id'] for value in checked]
        if len(set(identifiers)) != len(checked) or len({value['source_id'] for value in checked}) != len(checked):
            raise ValueError('Партии и исходные папки не должны повторяться в одной странице привязки.')
        try:
            with self._mutation(plan_id) as db:
                plan = self._row_plan(db, plan_id)
                marks = ','.join('?' for _ in identifiers)
                rows = {row['id']: row['source_id'] for row in db.execute(
                    "SELECT id,source_id FROM batches WHERE plan_id=? AND generation=? AND status!='deleted' "
                    f'AND id IN ({marks})', (plan_id, plan['generation'], *identifiers))}
                if len(rows) != len(checked):
                    raise ValueError('Некоторые партии не относятся к этому плану или больше не существуют.')
                changes = []
                for value in checked:
                    previous = rows[value['batch_id']]
                    if previous == value['source_id']:
                        continue
                    if plan['status'] != 'activating':
                        raise ValueError('Привязать исходную папку можно только при активации плана.')
                    if previous is not None:
                        raise ValueError('Эта партия уже привязана к другой исходной папке.')
                    changes.append((value['source_id'], value['batch_id']))
                if changes:
                    db.executemany('UPDATE batches SET source_id=? WHERE id=?', changes)
                    self._touch(db, plan_id, self._clock())
        except sqlite3.IntegrityError:
            raise ValueError('Одна из исходных папок уже привязана к другой партии.') from None
        return {'plan_id': plan_id, 'bound': len(checked), 'bindings': checked}

    def bind_source(self, batch_id, source_id):
        """Compatibility wrapper; bulk activation should use bind_sources."""
        batch = self.batch(batch_id)
        self.bind_sources(batch['plan_id'], [{'batch_id': batch_id, 'source_id': source_id}])
        return self.batch(batch_id)

    def commit_activation(self, plan_id):
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] == 'active':
                return self.plan(plan_id)
            if row['status'] != 'activating':
                raise ValueError('План не находится в процессе активации.')
            unbound = db.execute("SELECT 1 FROM batches WHERE plan_id=? AND generation=? AND status!='deleted' AND source_id IS NULL LIMIT 1", (plan_id, row['generation'])).fetchone()
            if unbound:
                raise ValueError('Не все партии привязаны к исходным папкам. Повторите активацию.')
            db.execute("UPDATE plans SET status='active' WHERE id=?", (plan_id,))
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def begin_activation_rollback(self, plan_id, message='Активация отменяется; созданные исходные папки будут отключены.'):
        """Persist cancellation intent before the first owned group is removed.

        A restart must finish this rollback instead of recreating removed
        groups against stale binding IDs or reviving a user-cancelled plan.
        """
        if not isinstance(message, str):
            raise ValueError('Сообщение активации должно быть строкой.')
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] not in {'activating', 'rolling_back'}:
                raise ValueError('Этот план не находится в процессе активации или её отмены.')
            db.execute("UPDATE plans SET status='rolling_back',message=? WHERE id=?", (message[:2000], plan_id))
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def abort_activation(self, plan_id, message='Активация отменена. Исправьте причину и повторите активацию.'):
        """Root must first remove all owned groups, including unbound orphans.

        Clearing IDs and returning to draft happen together. Call
        begin_activation_rollback before deleting groups; interrupted cleanup
        remains rolling_back and cannot be mistaken for a requested activation.
        """
        if not isinstance(message, str):
            raise ValueError('Сообщение активации должно быть строкой.')
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] not in {'activating', 'rolling_back'}:
                raise ValueError('Этот план не находится в процессе активации или её отмены.')
            db.execute('UPDATE batches SET source_id=NULL WHERE plan_id=? AND generation=?', (plan_id, row['generation']))
            db.execute("UPDATE plans SET status='draft',message=? WHERE id=?", (message[:2000], plan_id))
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def invalidate_inventory(self, inventory_id, revision):
        with self._condition:
            identifiers = [row['id'] for row in self._summaries.values() if row['inventory_id'] == inventory_id and row['status'] == 'draft' and row['inventory_revision'] != revision]
        for identifier in identifiers:
            with self._mutation(identifier) as db:
                db.execute('UPDATE plans SET inventory_revision=NULL,message=? WHERE id=? AND status=\'draft\'',
                           ('Выбор папок изменился. Сформируйте план заново.', identifier))
                self._touch(db, identifier, self._clock())

    def recalculate(self, plan_id):
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] not in {'draft', 'active'} or row['generation'] is None:
                raise ValueError('Сейчас нельзя пересчитать это расписание.')
            self._calendar(db, plan_id, row['generation'], json.loads(row['settings']), now=self._clock(), clear_pins=True)
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def schedule(self, plan_id, settings):
        settings = validate_settings(settings)
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] not in {'draft', 'active'}:
                raise ValueError('Сейчас нельзя изменить расписание.')
            old = json.loads(row['settings'])
            for field in ('target_count', 'target_bytes', 'tolerance_percent'):
                if settings[field] != old[field]:
                    raise ValueError('Размер уже сформированных партий не меняется при настройке календаря. Создайте новый предварительный план.')
            db.execute('UPDATE plans SET settings=? WHERE id=?', (_json(settings), plan_id))
            self._calendar(db, plan_id, row['generation'], settings, now=self._clock(), clear_pins=True)
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def set_batch_time(self, batch_id, scheduled_for):
        scheduled_for = _epoch(scheduled_for)
        batch = self.batch(batch_id)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            plan = self._row_plan(db, row['plan_id'])
            settings = json.loads(plan['settings'])
            if row['status'] != 'queued' or plan['status'] not in {'active', 'draft'}:
                raise ValueError('Можно изменить только дату ещё не запущенной партии.')
            if scheduled_for < self._clock():
                raise ValueError('Дата запуска не может быть в прошлом.')
            local = datetime.fromtimestamp(scheduled_for, ZoneInfo(settings['timezone']))
            if local.isoweekday() not in settings['weekdays'] or local.date() < date.fromisoformat(settings['start_date']):
                raise ValueError('Дата должна соответствовать выбранным дням недели и первой дате плана.')
            previous = db.execute("SELECT * FROM batches WHERE plan_id=? AND generation=? AND position<? AND status!='deleted' ORDER BY position DESC,id DESC LIMIT 1", (row['plan_id'], row['generation'], row['position'])).fetchone()
            previous_start = (previous['started_at'] if previous['started_at'] is not None else previous['scheduled_for']) if previous else None
            if previous_start is not None and scheduled_for < previous_start + settings['interval_days'] * 86400:
                raise ValueError('Дата нарушает минимальный интервал между партиями.')
            db.execute('UPDATE batches SET requested_for=?,scheduled_for=? WHERE id=?', (scheduled_for, scheduled_for, batch_id))
            self._calendar(db, row['plan_id'], row['generation'], settings, now=self._clock())
            self._touch(db, row['plan_id'], self._clock())
        return self.batch(batch_id)

    def reorder(self, plan_id, queued_ids):
        if not isinstance(queued_ids, list) or any(not isinstance(value, str) for value in queued_ids):
            raise ValueError('Укажите порядок партий в очереди.')
        with self._mutation(plan_id) as db:
            plan = self._row_plan(db, plan_id)
            if plan['status'] not in {'active', 'draft'}:
                raise ValueError('Сейчас нельзя изменить порядок партий.')
            rows = db.execute("SELECT id,position FROM batches WHERE plan_id=? AND generation=? AND status='queued' ORDER BY position,id", (plan_id, plan['generation'])).fetchall()
            if len(queued_ids) != len(rows) or len(set(queued_ids)) != len(rows) or set(queued_ids) != {row['id'] for row in rows}:
                raise ValueError('Список партий изменился. Обновите страницу и повторите изменение порядка.')
            for identifier, row in zip(queued_ids, rows):
                db.execute('UPDATE batches SET position=? WHERE id=?', (row['position'], identifier))
            self._calendar(db, plan_id, plan['generation'], json.loads(plan['settings']), now=self._clock(), clear_pins=True)
            self._touch(db, plan_id, self._clock())
        return self.plan(plan_id)

    def move_batch(self, batch_id, direction=None, *, before_id=_UNSET):
        with self._condition:
            batch = self.batch(batch_id)
            if batch['status'] != 'queued':
                raise ValueError('Перемещать можно только партии в очереди.')
            with self._connect() as db:
                plan = self._row_plan(db, batch['plan_id'])
                ids = [row[0] for row in db.execute("SELECT id FROM batches WHERE plan_id=? AND generation=? AND status='queued' ORDER BY position,id", (batch['plan_id'], plan['generation']))]
            index = ids.index(batch_id)
            if before_id is not _UNSET:
                if direction is not None or (before_id is not None and before_id not in ids):
                    raise ValueError('Укажите партию, перед которой нужно поставить выбранную.')
                if before_id == batch_id:
                    return self.plan(batch['plan_id'])
                ids.remove(batch_id)
                ids.insert(ids.index(before_id) if before_id is not None else len(ids), batch_id)
            elif direction in {'up', 'down'}:
                target = index + (-1 if direction == 'up' else 1)
                if 0 <= target < len(ids):
                    ids[index], ids[target] = ids[target], ids[index]
            else:
                raise ValueError('Направление перемещения: up или down.')
            return self.reorder(batch['plan_id'], ids)

    def _align_overdue(self, batch_id):
        """Move a missed head slot on a real wake, never by idle DB polling."""
        with self._condition:
            head = next((value for value in self._heads.values() if value['id'] == batch_id), None)
            if head is None or head['status'] != 'queued':
                return False
            plan = self._summaries[head['plan_id']]
            now = self._clock()
            if head['scheduled_for'] is not None and now < _minute_end(head['scheduled_for'], plan['settings']['timezone']):
                return False
            with self._mutation(head['plan_id']) as db:
                raw = self._row_plan(db, head['plan_id'])
                self._calendar(db, head['plan_id'], raw['generation'], plan['settings'], now=now)
                self._touch(db, head['plan_id'], now)
            return True

    def begin(self, batch_id, manual=False, retry=False):
        if type(manual) is not bool or type(retry) is not bool or (retry and not manual):
            raise ValueError('Повтор завершённой партии возможен только явным ручным запуском.')
        batch = self.batch(batch_id)
        if not manual and self._align_overdue(batch_id):
            raise PlanDeferred('Пропущенное время запуска перенесено на следующий разрешённый слот.')
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            plan = self._row_plan(db, row['plan_id'])
            if plan['status'] != 'active' or row['source_id'] is None or row['status'] not in ({'completed', 'completed_with_errors'} if retry else {'queued', 'paused'}):
                raise ValueError('Эту партию сейчас нельзя запустить.')
            if db.execute("SELECT 1 FROM batches b JOIN plans p ON p.id=b.plan_id AND p.generation=b.generation WHERE b.status IN ('running','deleting') AND p.status='active' LIMIT 1").fetchone():
                raise ValueError('Уже выполняется другая партия или её очистка.')
            if retry:
                # A user may retry a failed task after this party completed.
                # Start a fresh completion cycle without rewriting old receipts
                # or event identities, and retain all other completed parties.
                db.execute("UPDATE batches SET status='queued',started_at=NULL,finished_at=NULL,run_key=?,summary='{}',requested_for=NULL,interrupted=0 WHERE id=?",
                           (uuid.uuid4().hex, batch_id))
                db.execute('UPDATE plans SET completion_cycle=completion_cycle+1 WHERE id=?', (row['plan_id'],))
            head = db.execute("SELECT * FROM batches WHERE plan_id=? AND generation=? AND status IN ('queued','paused') ORDER BY position,id LIMIT 1", (row['plan_id'], row['generation'])).fetchone()
            now = self._clock()
            if not manual:
                if row['status'] != 'queued' or head['id'] != batch_id or row['scheduled_for'] is None or row['scheduled_for'] > now:
                    raise ValueError('Время запуска партии ещё не наступило или предыдущая партия не завершена.')
            elif head['id'] != batch_id:
                # Explicit manual launch may promote a future batch ahead of a
                # paused predecessor. Finished rows keep their positions.
                remaining = db.execute("SELECT id,position FROM batches WHERE plan_id=? AND generation=? AND status IN ('queued','paused') ORDER BY position,id", (row['plan_id'], row['generation'])).fetchall()
                ids = [batch_id] + [item['id'] for item in remaining if item['id'] != batch_id]
                for identifier, slot in zip(ids, remaining):
                    db.execute('UPDATE batches SET position=? WHERE id=?', (slot['position'], identifier))
            db.execute("UPDATE batches SET status='running',started_at=COALESCE(started_at,?),finished_at=NULL,attempt=attempt+1,interrupted=0,message='' WHERE id=?", (now, batch_id))
            self._calendar(db, row['plan_id'], row['generation'], json.loads(plan['settings']), now=now)
            self._touch(db, row['plan_id'], now)
        return self.batch(batch_id)

    def finish(self, batch_id, status, summary=None, message=''):
        if status not in _TERMINAL | {'paused'}:
            raise ValueError('Некорректный результат партии.')
        if summary is not None and not isinstance(summary, dict):
            raise ValueError('Итоги партии должны быть объектом.')
        if not isinstance(message, str):
            raise ValueError('Сообщение партии должно быть строкой.')
        batch = self.batch(batch_id)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            if row['status'] in _TERMINAL and row['status'] == status:
                return self._batch(row)
            if row['status'] != 'running':
                raise ValueError('Завершить можно только выполняющуюся партию.')
            now = self._clock()
            db.execute('UPDATE batches SET status=?,finished_at=?,summary=?,message=?,interrupted=0 WHERE id=?',
                       (status, now if status in _TERMINAL else None, _json(summary if summary is not None else json.loads(row['summary'])), message[:2000], batch_id))
            plan = self._row_plan(db, row['plan_id'])
            self._calendar(db, row['plan_id'], row['generation'], json.loads(plan['settings']), now=now)
            self._touch(db, row['plan_id'], now)
            if status in _TERMINAL:
                finished = self._batch(self._row_batch(db, batch_id))
                payload = {'batch': finished, 'plan': {'id': plan['id'], 'name': plan['name'], 'root': plan['root'], 'settings': json.loads(plan['settings']), 'completion_cycle': plan['completion_cycle']}}
                db.execute('INSERT OR IGNORE INTO plan_events(key,batch_id,plan_id,run_key,payload,created_at) VALUES(?,?,?,?,?,?)',
                           (f"batch:{batch_id}:complete:{row['run_key']}", batch_id, row['plan_id'], row['run_key'], _json(payload), now))
        return self.batch(batch_id)

    def pause(self, batch_id, summary=None, message='Партия приостановлена.'):
        batch = self.batch(batch_id)
        if batch['status'] == 'running':
            return self.finish(batch_id, 'paused', summary, message)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            plan = self._row_plan(db, row['plan_id'])
            if plan['status'] != 'active' or row['status'] not in {'queued', 'paused'}:
                raise ValueError('Эту партию нельзя приостановить.')
            db.execute("UPDATE batches SET status='paused',message=?,interrupted=0 WHERE id=?", (message, batch_id))
            self._touch(db, row['plan_id'], self._clock())
        return self.batch(batch_id)

    def begin_cleanup(self, batch_id):
        batch = self.batch(batch_id)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            if row['status'] != 'paused':
                raise ValueError('Перед очисткой остановите партию и дождитесь завершения текущих задач.')
            db.execute("UPDATE batches SET status='deleting',interrupted=0,message='Перемещение файлов партии в корзину Облака.' WHERE id=?", (batch_id,))
            self._touch(db, row['plan_id'], self._clock())
        return self.batch(batch_id)

    def finish_cleanup(self, batch_id, message='Файлы партии перемещены в корзину. Можно возобновить загрузку.'):
        batch = self.batch(batch_id)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            if row['status'] != 'deleting':
                raise ValueError('Эта партия не находится в процессе очистки.')
            db.execute("UPDATE batches SET status='paused',message=?,interrupted=0 WHERE id=?", (message[:2000], batch_id))
            self._touch(db, row['plan_id'], self._clock())
        return self.batch(batch_id)

    def recoverable(self, *, limit=200, offset=0):
        _page(limit, offset)
        with self._connect() as db:
            clause = "FROM batches b JOIN plans p ON p.id=b.plan_id AND p.generation=b.generation WHERE b.interrupted=1 AND b.status='paused' AND p.status='active'"
            total = db.execute('SELECT COUNT(*) ' + clause).fetchone()[0]
            rows = db.execute('SELECT b.* ' + clause + ' ORDER BY b.started_at,b.id LIMIT ? OFFSET ?', (limit, offset))
            return {'batches': [self._batch(row) for row in rows], 'total': total, 'offset': offset, 'limit': limit}

    def remove_batch(self, batch_id):
        batch = self.batch(batch_id)
        with self._mutation(batch['plan_id']) as db:
            row = self._row_batch(db, batch_id)
            plan = self._row_plan(db, row['plan_id'])
            if row['status'] in {'running', 'deleting'} or plan['status'] not in {'draft', 'active'}:
                raise ValueError('Сначала остановите выполняющуюся партию или завершите её очистку.')
            db.execute("UPDATE batches SET status='deleted' WHERE id=?", (batch_id,))
            # Compact display positions but preserve completed timestamps.
            rows = db.execute("SELECT id FROM batches WHERE plan_id=? AND generation=? AND status!='deleted' ORDER BY position,id", (row['plan_id'], row['generation'])).fetchall()
            for position, current in enumerate(rows):
                db.execute('UPDATE batches SET position=? WHERE id=?', (position, current['id']))
            self._calendar(db, row['plan_id'], row['generation'], json.loads(plan['settings']), now=self._clock(), clear_pins=True)
            self._touch(db, row['plan_id'], self._clock())
        return batch

    def remove_plan(self, plan_id):
        with self._mutation(plan_id) as db:
            row = self._row_plan(db, plan_id)
            if row['status'] == 'building' or db.execute("SELECT 1 FROM batches WHERE plan_id=? AND generation=? AND status IN ('running','deleting') LIMIT 1", (plan_id, row['generation'])).fetchone():
                raise ValueError('Сначала остановите выполняющуюся партию или завершите создание/очистку плана.')
            # Return at most MAX_BATCHES tiny cleanup bindings. File/member lists
            # are never part of deletion responses.
            bindings = [{'batch_id': item['id'], 'source_id': item['source_id'], 'owner_key': 'plan-batch:' + item['id']}
                        for item in db.execute('SELECT id,source_id FROM batches WHERE plan_id=? AND generation=? AND source_id IS NOT NULL', (plan_id, row['generation']))]
            db.execute("UPDATE plans SET status='deleted' WHERE id=?", (plan_id,))
            db.execute("UPDATE batches SET status='deleted' WHERE plan_id=?", (plan_id,))
        return {'id': plan_id, 'bindings': bindings}

    def deleted_bindings(self, *, limit=200, offset=0):
        _page(limit, offset)
        with self._connect() as db:
            total = db.execute("SELECT COUNT(*) FROM batches WHERE status='deleted' AND source_id IS NOT NULL").fetchone()[0]
            rows = db.execute("SELECT id,plan_id,source_id FROM batches WHERE status='deleted' AND source_id IS NOT NULL ORDER BY id LIMIT ? OFFSET ?", (limit, offset))
            return {'bindings': [{'batch_id': row['id'], 'plan_id': row['plan_id'], 'source_id': row['source_id'], 'owner_key': 'plan-batch:' + row['id']} for row in rows], 'total': total}

    def ack_deleted_binding(self, batch_id):
        with self._mutation() as db:
            db.execute("UPDATE batches SET source_id=NULL WHERE id=? AND status='deleted'", (_identifier(batch_id),))

    def completed_events(self, *, limit=100):
        _page(limit, 0)
        with self._connect() as db:
            result = []
            for row in db.execute('SELECT * FROM plan_events WHERE acknowledged=0 ORDER BY created_at,key LIMIT ?', (limit,)):
                value = {key: row[key] for key in ('key', 'batch_id', 'plan_id', 'run_key', 'created_at')}
                value.update(json.loads(row['payload']))
                result.append(value)
            return result

    def ack_event(self, key):
        if not isinstance(key, str) or len(key) > 200:
            raise ValueError('Некорректный ключ уведомления.')
        with self._mutation() as db:
            db.execute('UPDATE plan_events SET acknowledged=1 WHERE key=?', (key,))

    def wake(self):
        with self._condition:
            self._wake_generation += 1
            self._condition.notify_all()

    def tick(self):
        with self._condition:
            if self._stopping or self._launching or any(row['status'] in {'running', 'deleting'} for row in self._heads.values()):
                return False
            now = self._clock()
            eligible = [row for row in self._heads.values() if row['status'] == 'queued' and row['scheduled_for'] is not None and row['scheduled_for'] <= now]
            if not eligible:
                return False
            batch = copy.deepcopy(min(eligible, key=lambda row: (row['scheduled_for'], row['plan_id'], row['position'])))
            self._launching = True
        try:
            if self._align_overdue(batch['id']):
                return False
            accepted = bool(self._launch(batch))
            with self._condition:
                self._error = None
            return accepted
        except PlanDeferred:
            with self._condition:
                self._error = None
            return False
        except Exception as error:
            with self._condition:
                self._error = str(error)[:500]
            return False
        finally:
            with self._condition:
                self._launching = False

    def start(self):
        with self._condition:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(target=self._run, name='mail-cloud-plans', daemon=True)
            self._thread.start()

    def _run(self):
        while True:
            with self._condition:
                if self._stopping:
                    return
                generation = self._wake_generation
            launched = self.tick()
            with self._condition:
                if self._stopping:
                    return
                if generation != self._wake_generation:
                    continue
                now = self._clock()
                due = [row['scheduled_for'] for row in self._heads.values() if row['status'] == 'queued' and row['scheduled_for'] is not None]
                # If another operation holds the controller, it wakes us when
                # done. A past-due date never becomes a zero-time busy loop.
                timeout = max(0, min(due) - now) if due and min(due) > now else None
                if any(row['status'] in {'running', 'deleting'} for row in self._heads.values()):
                    timeout = None
                self._condition.wait(timeout=timeout)

    def close(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)

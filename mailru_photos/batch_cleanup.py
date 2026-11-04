"""Durable, bounded recycle-bin operations over batch creation receipts.

This journal deliberately does not use the task-deletion journal: a batch can
be rescanned or its tasks removed, but its confirmed upload receipts must still
identify the objects it created. Original files are never part of this operation.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time

from .plan_inventory import _private_file


def records(queue, batch_id):
    after = 0
    while True:
        page = queue.batch_cloud_objects(batch_id, after_id=after, limit=500)
        if not page:
            return
        yield from page
        after = page[-1]['id']


def _encoded(row):
    fields = ('id', 'batch_id', 'account_id', 'remote_path', 'cloud_hash', 'cloud_size', 'remote_id')
    return json.dumps({key: row.get(key) for key in fields}, ensure_ascii=False,
                      sort_keys=True, separators=(',', ':'), allow_nan=False)


def preview(queue, batch_id):
    digest = hashlib.sha256()
    count = total = 0
    for row in records(queue, batch_id):
        digest.update(_encoded(row).encode('utf-8') + b'\n')
        count += 1
        total += row['cloud_size']
    return {'batch_id': batch_id, 'count': count, 'total_bytes': total, 'fingerprint': digest.hexdigest()}


class BatchCleanupJournal:
    def __init__(self, path):
        self.path = _private_file(path)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA cache_size=-2048;
            CREATE TABLE IF NOT EXISTS operation(
                id INTEGER PRIMARY KEY CHECK(id=1),batch_id TEXT NOT NULL,state TEXT NOT NULL,
                count INTEGER NOT NULL,total_bytes INTEGER NOT NULL,created_at REAL NOT NULL,
                updated_at REAL NOT NULL,message TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS objects(
                receipt_id INTEGER PRIMARY KEY,record TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
                message TEXT NOT NULL DEFAULT '');
            CREATE INDEX IF NOT EXISTS objects_state ON objects(state,receipt_id);
        ''')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    def operation(self):
        row = self.db.execute('SELECT * FROM operation WHERE id=1').fetchone()
        return dict(row) if row else None

    def prepare(self, queue, expected):
        old = self.operation()
        if old and old['state'] != 'complete':
            raise ValueError('Сначала завершите предыдущую очистку партии.')
        with self.db:
            self.db.execute('DELETE FROM objects')
            self.db.execute('DELETE FROM operation')
            digest = hashlib.sha256()
            count = total = 0
            for row in records(queue, expected['batch_id']):
                encoded = _encoded(row)
                digest.update(encoded.encode('utf-8') + b'\n')
                self.db.execute('INSERT INTO objects(receipt_id,record) VALUES(?,?)', (row['id'], encoded))
                count += 1
                total += row['cloud_size']
            if digest.hexdigest() != expected['fingerprint'] or count != expected['count']:
                raise ValueError('Список файлов партии изменился. Подтвердите очистку ещё раз.')
            now = time.time()
            self.db.execute('INSERT INTO operation(id,batch_id,state,count,total_bytes,created_at,updated_at) VALUES(1,?,\'confirmed\',?,?,?,?)',
                            (expected['batch_id'], count, total, now, now))
        return self.summary()

    def set_state(self, state, message=''):
        if state not in {'confirmed', 'running', 'paused', 'complete'}:
            raise ValueError('Некорректное состояние очистки.')
        with self.db:
            self.db.execute('UPDATE operation SET state=?,message=?,updated_at=? WHERE id=1',
                            (state, message[:2000], time.time()))

    def pending(self, limit=100):
        return [json.loads(row['record']) for row in self.db.execute(
            "SELECT record FROM objects WHERE state='pending' ORDER BY receipt_id LIMIT ?", (limit,))]

    def finish_item(self, receipt_id, *, error=None):
        with self.db:
            self.db.execute('UPDATE objects SET state=?,message=? WHERE receipt_id=?',
                            ('error' if error else 'done', (error or '')[:2000], receipt_id))

    def summary(self):
        result = self.operation()
        if result is None:
            return None
        result.update(done=0, failed=0, pending=0)
        for row in self.db.execute('SELECT state,COUNT(*) AS n FROM objects GROUP BY state'):
            result[{'error': 'failed', 'done': 'done', 'pending': 'pending'}[row['state']]] = row['n']
        return result

#!/usr/bin/env python3
"""Reproducible SQLite queue benchmark using virtual filesystem metadata.

No network calls or media uploads occur. Production Queue.scan/claim/stats/
list/reconcile_candidates methods operate on a real FULL/WAL SQLite database.
Only enumeration, os.stat for virtual source paths, and _snapshot are replaced.
Bulk paused/terminal prefixes are explicit SQL fixtures, not measured user APIs.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import sqlite3
import stat
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch


_OBSERVED_PEAK_RSS_MB = 0.0


def rss_mb():
    global _OBSERVED_PEAK_RSS_MB
    try:
        for line in Path('/proc/self/status').read_text().splitlines():
            if line.startswith('VmRSS:'):
                value = int(line.split()[1]) / 1024
                _OBSERVED_PEAK_RSS_MB = max(_OBSERVED_PEAK_RSS_MB, value)
                return value
    except OSError:
        pass
    return None


def peak_mb():
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return max(_OBSERVED_PEAK_RSS_MB, value / (1024 * 1024 if sys.platform == 'darwin' else 1024))
    except ImportError:
        return None


class VirtualSources:
    def __init__(self, root: Path, count: int, original_stat, original_snapshot):
        self.root, self.count = root, count
        self.moved = 0
        self.dropped = 0
        self.original_stat, self.original_snapshot = original_stat, original_snapshot

    def relative(self, number):
        parent = 'moved' if number <= self.moved else 'original'
        return f'{parent}/{number % 1000:04d}/media_{number:06d}.jpg'

    def identity(self, number):
        return 1024 * (1 + number % 4096), 1700000000000000000 + number, 123, 1000000 + number

    def local_path(self, number):
        return str(self.root / self.relative(number))

    def virtual_number(self, path):
        if isinstance(path, int):
            return None
        value = os.path.abspath(os.fspath(path))
        if not value.startswith(str(self.root) + os.sep):
            return None
        match = re.search(r'media_(\d+)\.jpg$', value)
        if not match:
            return None
        number = int(match.group(1))
        if (not 1 <= number <= self.count - self.dropped
                or os.path.normcase(value) != os.path.normcase(self.local_path(number))):
            raise FileNotFoundError(value)
        return number

    def stat(self, path, *args, **kwargs):
        number = self.virtual_number(path)
        if number is None:
            return self.original_stat(path, *args, **kwargs)
        size, mtime, device, inode = self.identity(number)
        return SimpleNamespace(st_size=size, st_mtime_ns=mtime, st_dev=device, st_ino=inode,
                               st_mode=stat.S_IFREG | 0o600, st_nlink=1)

    def snapshot(self, path):
        number = self.virtual_number(path)
        return self.original_snapshot(path) if number is None else self.identity(number)

    def walk(self, root, extensions, result, stop=None):
        for number in range(1, self.count - self.dropped + 1):
            if stop is not None and stop.is_set():
                return
            relative = self.relative(number)
            local = self.local_path(number)
            remote = root.remote_prefix.rstrip('/') + '/' + relative
            size, mtime, device, inode = self.identity(number)
            yield (root.id, relative, local, os.path.normcase(local), remote, remote.casefold(),
                   size, mtime, device, str(inode))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--count', type=int, default=300000)
    parser.add_argument('--workdir', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.count < 1000:
        parser.error('--count must be at least 1000')
    sys.path.insert(0, str(args.repo.resolve()))
    from mailru_photos import queue as module
    from mailru_photos.queue import Queue, UNLIMITED_MAX_BYTES

    temporary = tempfile.TemporaryDirectory(prefix='mail-cloud-benchmark-') if args.workdir is None else None
    folder = Path(temporary.name) if temporary else args.workdir.resolve()
    if not temporary:
        folder.mkdir(parents=True, exist_ok=True)
        if any(folder.iterdir()):
            parser.error('--workdir must be new or empty; no existing data is overwritten')
    source = folder / 'virtual_source'
    source.mkdir()
    database = folder / 'queue.sqlite3'
    world = VirtualSources(source, args.count, os.stat, module._snapshot)
    report = {
        'count': args.count,
        'environment': {'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
                        'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                        'queue_source_sha256': hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()},
        'scope': {'real_sqlite': True, 'journal_mode': 'WAL', 'synchronous': 'FULL',
                  'virtual_source_enumeration': True, 'virtual_stat': True, 'patched_snapshot': True,
                  'network_uploads': False, 'bulk_prefixes_are_sql_fixtures': True},
        'baseline_rss_mb': rss_mb(), 'measurements': {}, 'plans': {}, 'assertions': [],
    }
    started = time.perf_counter()

    def emit(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)

    def size():
        return {suffix or 'db': round((folder / ('queue.sqlite3' + suffix)).stat().st_size / 1048576, 3)
                for suffix in ('', '-wal', '-shm') if (folder / ('queue.sqlite3' + suffix)).exists()}

    def measured(name, callback, *, summarize=None):
        before = time.perf_counter()
        value = callback()
        result = {'seconds': time.perf_counter() - before, 'rss_mb': rss_mb(), 'peak_rss_mb': peak_mb(),
                  'disk_mb': size()}
        if summarize:
            result['result'] = summarize(value)
        report['measurements'][name] = result
        emit({'completed': name, **result})
        return value

    def latency(name, callback, samples=7):
        timings = []
        last = None
        for _ in range(samples):
            begin = time.perf_counter()
            last = callback()
            timings.append(1000 * (time.perf_counter() - begin))
        timings.sort()
        result = {'median_ms': statistics.median(timings), 'p95_ms': timings[math.ceil(.95 * samples) - 1],
                  'min_ms': timings[0], 'max_ms': timings[-1], 'samples': samples, 'rss_mb': rss_mb()}
        report['measurements'][name] = result
        emit({'completed': name, **result})
        return last

    queue = Queue(database, destination='/Benchmark', account_id='offline-benchmark-fixture')
    queue.add_root(source, 'media')
    queue._walk_files = world.walk
    assert queue._db.execute('PRAGMA synchronous').fetchone()[0] == 2
    assert queue._db.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
    last_progress = [0.0]
    phase = ['initial_scan']

    def progress(value):
        now = time.perf_counter()
        if now - last_progress[0] >= 5:
            last_progress[0] = now
            emit({'progress': phase[0], 'seen': value.seen, 'seconds': value.seconds, 'rss_mb': rss_mb()})

    def scan(name):
        phase[0] = name
        value = measured(name, lambda: queue.scan(progress=progress), summarize=asdict)
        queue.set_metadata('last_scan', asdict(value))
        assert value.complete and value.error_count == 0
        return value

    def claims(name, expected, mode=None, samples=11):
        times, instructions = [], []
        for _ in range(samples):
            ticks = [0]
            def count_steps():
                ticks[0] += 1
                return 0
            queue._db.set_progress_handler(count_steps, 100)
            begin = time.perf_counter()
            job = queue.claim(storage_mode=mode, now=time.time())
            elapsed = 1000 * (time.perf_counter() - begin)
            queue._db.set_progress_handler(None, 0)
            assert job and job.id == expected, (name, job, expected)
            assert ticks[0] < 100, (name, 'claim scanned a large prefix', ticks[0])
            queue.release(job)
            times.append(elapsed)
            instructions.append((ticks[0] + 1) * 100)
        times.sort()
        result = {'median_ms': statistics.median(times), 'p95_ms': times[math.ceil(.95 * samples) - 1],
                  'max_ms': times[-1], 'samples': samples, 'claimed_id': expected,
                  'sqlite_vm_operations_upper_bound': max(instructions)}
        report['measurements'][name] = result
        emit({'completed': name, **result})

    with patch.object(module.os, 'stat', new=world.stat), patch.object(module, '_snapshot', new=world.snapshot):
        initial = scan('initial_scan')
        assert initial.added == args.count and queue.stats()['total'] == args.count
        initial_rss = rss_mb()
        latency('stats_initial', queue.stats, samples=3)
        for offset in (0, args.count // 2, args.count - 100):
            rows = latency(f'list_offset_{offset}', lambda offset=offset: queue.list_jobs(limit=100, offset=offset, summary=True))
            assert len(rows) == 100
        for after in (0, args.count // 2, args.count - 500):
            rows = latency(f'candidate_after_{after}', lambda after=after: queue.reconcile_candidates(after_id=after, limit=500))
            assert len(rows) == 500
        claims('claim_first', 1)
        uploaded = 100
        for _ in range(uploaded):
            job = queue.claim()
            queue.mark_uploaded(job, remote_hash=f'{job.id:040X}',
                                verified_metadata={'benchmark_fixture': True, 'path': job.remote_path})
        for row in queue.reconcile_candidates(limit=25):
            assert queue.mark_remote_existing(row, remote_hash=f'{row["id"]:040X}')
        report['scope']['fixture_verified_completions'] = uploaded
        report['scope']['fixture_existing_observations'] = 25
        with queue._transaction():
            queue._db.execute("UPDATE jobs SET status='skipped',skip_reason='benchmark_fixture',next_attempt=NULL "
                              'WHERE id>125 AND id<?', (args.count,))
        claims('claim_last_after_terminal_prefix', args.count)
        with queue._transaction():
            queue._db.execute("UPDATE jobs SET status='pending',paused=1,skip_reason=NULL,next_attempt=0 WHERE id>125 AND id<?",
                              (args.count,))
        claims('claim_last_after_paused_prefix', args.count)
        with queue._transaction():
            queue._db.execute("UPDATE jobs SET status='failed',next_attempt=0 WHERE id>125")
        claims('claim_retry_after_paused_prefix', args.count)
        queue.request_control(args.count, 'pause')
        assert queue.claim() is None
        paused = latency('stats_paused_prefix', queue.stats, samples=3)
        assert paused['remaining'] == 0 and paused['paused'] == args.count - 125
        with queue._transaction():
            queue._db.execute("UPDATE jobs SET status='pending',paused=0,remove_requested=0,attempts=0,"
                              "skip_reason=NULL,last_error=NULL,next_attempt=0 WHERE id>125")
        same = scan('unchanged_rescan')
        assert same.unchanged == args.count and same.added == same.moved == same.missing == 0
        world.moved = min(3000, args.count // 10)
        moved = scan('moved_rescan')
        assert moved.moved == world.moved and moved.added == moved.missing == 0
        assert queue.stats()['total'] == args.count
        for number in (1, 100, 101, 125, 126, world.moved):
            row = queue.get_job(number)
            assert row['local_path'] == world.local_path(number)
            if number <= 125:
                assert row['remote_path'].startswith('/Benchmark/media/original/')
            else:
                assert row['remote_path'] == '/Benchmark/media/' + world.relative(number)
        assert queue.upload_history(1)[0]['local_path'].find('/original/') >= 0
        world.dropped = min(10000, args.count // 10)
        dropped = scan('removed_rescan')
        assert dropped.missing == world.dropped and dropped.added == 0
        current = queue.stats()
        assert current['total'] == args.count - world.dropped
        assert queue.stats(include_missing=True)['total'] == args.count
        assert current['uploaded'] == 100 and current['existing'] == 25
        report['assertions'].extend([
            'initial count exactly matches virtual source count',
            'unchanged rescan creates no tasks',
            f'{world.moved} moves preserve task IDs and do not grow the queue',
            'uploaded and existing observations retain their actual cloud paths after moves',
            f'{world.dropped} absent sources excluded from current totals and retained in history',
            'paused prefix has remaining=0 and no claimable task',
            'first/last/paused-prefix claims stay below 10000 SQLite VM operations',
        ])
        sql = {
            'pending_claim': ("SELECT * FROM jobs INDEXED BY jobs_pending WHERE status='pending' AND present=1 "
                              'AND paused=0 AND remove_requested=0 AND (? IS NULL OR attempts<?) ORDER BY id LIMIT 1', (None, None)),
            'retry_claim': ("SELECT * FROM jobs INDEXED BY jobs_retry WHERE status='failed' AND present=1 "
                            'AND paused=0 AND remove_requested=0 AND next_attempt IS NOT NULL AND next_attempt<=? '
                            'AND (? IS NULL OR attempts<?) ORDER BY next_attempt,id LIMIT 1', (time.time(), None, None)),
            'task_list': ('SELECT id,local_path,remote_path,status FROM jobs WHERE present=1 ORDER BY id LIMIT ? OFFSET ?', (100, args.count // 2)),
            'reconcile_candidates': ("SELECT id,local_path FROM jobs INDEXED BY jobs_reconcile WHERE id>? AND present=1 "
                                     'AND paused=0 AND remove_requested=0 '
                                     f"AND status IN ('pending','failed') AND size>0 AND size<={UNLIMITED_MAX_BYTES} "
                                     'AND NOT EXISTS(SELECT 1 FROM commit_intents WHERE job_id=jobs.id) ORDER BY id LIMIT ?', (args.count // 2, 500)),
        }
        for name, (statement, parameters) in sql.items():
            plan = [tuple(row) for row in queue._db.execute('EXPLAIN QUERY PLAN ' + statement, parameters)]
            assert not any('TEMP B-TREE' in row[3] for row in plan), (name, plan)
            report['plans'][name] = plan
        report['assertions'].append('ordered claim/list/keyset queries require no temporary sort')
        before_history = queue.upload_history(1)
        queue.close()
        begin = time.perf_counter()
        queue = Queue(database, destination='/Benchmark', account_id='offline-benchmark-fixture')
        reopen_ms = 1000 * (time.perf_counter() - begin)
        assert queue._db.total_changes == 0
        assert queue.stats() == current
        assert queue.upload_history(1) == before_history
        assert queue._db.execute('PRAGMA foreign_key_check').fetchall() == []
        report['measurements']['reopen'] = {'milliseconds': reopen_ms, 'writes': queue._db.total_changes, 'rss_mb': rss_mb()}
        report['assertions'].append('reopen performs zero writes and preserves current counts, actual paths and proof history')
        latency('stats_final', queue.stats, samples=3)
        queue.close()
        gc.collect()
        report['final_disk_mb'] = size()
        report['final_rss_mb'] = rss_mb()
        report['peak_rss_mb'] = peak_mb()
        report['observed_peak_rss_mb'] = _OBSERVED_PEAK_RSS_MB
        report['rss_after_initial_scan_mb'] = initial_rss
    report['total_seconds'] = time.perf_counter() - started
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    emit({'finished': True, 'output': str(args.output), 'seconds': report['total_seconds'],
          'peak_rss_mb': report['peak_rss_mb'], 'final_disk_mb': report['final_disk_mb']})
    if temporary:
        temporary.cleanup()


if __name__ == '__main__':
    main()

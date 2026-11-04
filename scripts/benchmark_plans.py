#!/usr/bin/env python3
"""Offline planner benchmark using real sparse files and unmodified production code.

The files contain no media data and are never uploaded. Their apparent sizes
exercise stat aggregates without allocating a terabyte. Real directory entries,
stat calls, SQLite, grouping, exclusions, pages, restart and calendar are used.
This measures the local machine, not HDD/TrueNAS or Mail throughput.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import sqlite3
import sys
import tempfile
import threading
import time
from unittest.mock import patch


def rss_mb():
    try:
        for line in Path('/proc/self/status').read_text().splitlines():
            if line.startswith('VmRSS:'):
                return int(line.split()[1]) / 1024
    except OSError:
        return None


def peak_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024**2 if sys.platform == 'darwin' else 1024)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--count', type=int, default=300000)
    parser.add_argument('--folders', type=int, default=3000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.count < args.folders or args.folders < 10:
        parser.error('Require count >= folders >= 10.')
    repo = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo))
    from mailru_photos.plan_inventory import PlanInventory
    from mailru_photos.plans import PlanStore
    from mailru_photos import plans as plan_module
    from mailru_photos import plan_inventory as inventory_module
    from mailru_photos.transport import Cancelled

    started = time.perf_counter()
    report = {
        'benchmark': 'plans-v0.9.0', 'count': args.count, 'top_level_folders': args.folders,
        'scope': {'real_local_directory_entries': True, 'real_stat_calls': True, 'real_sqlite': True,
                  'journal_mode': 'WAL', 'synchronous': 'FULL', 'media_contents_read': False,
                  'network_requests': False, 'account_access': False, 'sparse_file_sizes_synthetic': True,
                  'limitations': ['Local filesystem timings do not predict HDD, NAS, cloud scans or upload speed.',
                                  'Sparse files are metadata fixtures, not valid photographs or videos.',
                                  'RSS covers this benchmark process; it is not total production application memory.']},
        'environment': {'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
                        'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                        'inventory_sha256': hashlib.sha256(Path(inventory_module.__file__).read_bytes()).hexdigest(),
                        'plans_sha256': hashlib.sha256(Path(plan_module.__file__).read_bytes()).hexdigest()},
        'baseline_rss_mb': rss_mb(), 'measurements': {}, 'assertions': [],
    }
    def emit(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)
    def measured(name, fn):
        tick = time.perf_counter()
        result = fn()
        value = {'seconds': time.perf_counter() - tick, 'result': result,
                 'rss_mb': rss_mb(), 'peak_rss_mb': peak_mb()}
        report['measurements'][name] = value
        emit({'completed': name, **value})
        return result
    progress_at = [0.0]
    def progress(value):
        if time.perf_counter() - progress_at[0] >= 5:
            progress_at[0] = time.perf_counter()
            emit({'progress': value, 'rss_mb': rss_mb()})

    with tempfile.TemporaryDirectory(prefix='mail-cloud-planner-') as temporary:
        work = Path(temporary)
        root = work / 'Архив'
        root.mkdir()
        def create_files():
            total_size = 0
            for folder_index in range(args.folders):
                folder = root / f'Альбом_{folder_index:06d}'
                folder.mkdir()
                for number in range(folder_index, args.count, args.folders):
                    size = 3 * 1024**2 + (number % 1024) * 1024
                    with (folder / f'фото_{number:06d}.jpg').open('wb') as stream:
                        stream.truncate(size)
                    total_size += size
                if folder_index % 250 == 0:
                    progress({'phase': 'creating_sparse_fixture', 'folders': folder_index})
            return {'files': args.count, 'total_bytes': total_size}
        fixture = measured('create_sparse_fixture', create_files)
        inventory = PlanInventory(work / 'inventory.sqlite3')
        state = inventory.create(root)
        def scan():
            result = inventory.scan(state['id'], progress=progress)
            assert result['status'] == 'ready' and result['total_count'] == args.count
            assert result['total_bytes'] == fixture['total_bytes'] and result['selected_errors'] == 0
            return {key: result[key] for key in ('status', 'total_count', 'total_bytes', 'directory_count', 'selected_errors')}
        measured('initial_stat_inventory', scan)
        measured('repeat_stat_inventory', scan)
        state = inventory.summary(state['id'])
        def tree_pages():
            seen = 0
            while seen < args.folders:
                page = inventory.tree(state['id'], state['root_id'], offset=seen, limit=200)
                assert len(page['folders']) <= 200
                seen += len(page['folders'])
            assert seen == args.folders
            return {'folders_read': seen, 'max_page': 200}
        measured('paged_tree', tree_pages)
        first = inventory.tree(state['id'], state['root_id'], limit=1)['folders'][0]
        inventory.exclude(state['id'], first['id'], True)
        assert inventory.summary(state['id'])['selected_count'] == args.count - first['total_count']
        inventory.exclude(state['id'], first['id'], False)
        assert inventory.summary(state['id'])['selected_count'] == args.count
        # Empty content files are never read: no cloud hash implementation is
        # involved in inventory or grouping. This also validates source scope.
        now = 1793782800.0
        store = PlanStore(work / 'plans.sqlite3', clock=lambda: now)
        settings = {'target_count': 10000, 'target_bytes': 20 * 1024**3, 'tolerance_percent': 15,
                    'interval_days': 1, 'weekdays': [1, 2, 3, 4, 5, 6, 7],
                    'start_date': '2026-11-04', 'time': '10:00', 'timezone': 'UTC'}
        plan = store.create(state['id'], str(root), 'Benchmark', settings, '/Benchmark')
        def build():
            result = store.build(plan['id'], inventory, progress=progress)
            assert result['file_count'] == args.count and result['total_bytes'] == fixture['total_bytes']
            return {key: result[key] for key in ('batch_count', 'file_count', 'total_bytes', 'oversized_count')}
        measured('grouping_and_calendar', build)
        def validate_batches():
            offset = count = size = members = 0
            paths = set()  # Only directory identities for assertion, after measured packing.
            previous = None
            while True:
                page = store.batches(plan['id'], offset=offset, limit=50)
                for batch in page['batches']:
                    assert batch['file_count'] <= settings['target_count'] * 115 // 100
                    assert batch['total_bytes'] <= settings['target_bytes'] * 115 // 100
                    if previous is not None:
                        assert batch['scheduled_for'] - previous >= 86400
                    previous = batch['scheduled_for']
                    count += batch['file_count']
                    size += batch['total_bytes']
                    members += batch['member_count']
                    for member in store.iter_members(batch['id']):
                        assert member['path'] not in paths
                        paths.add(member['path'])
                offset += len(page['batches'])
                if offset >= page['total']:
                    break
            assert count == args.count and size == fixture['total_bytes'] and members == args.folders
            return {'batches': offset, 'files': count, 'bytes': size, 'unique_directory_members': members}
        measured('verify_every_batch_member', validate_batches)
        def cancel_rebuild():
            before = store.batches(plan['id'])['batches'][0]['id']
            stop = threading.Event()
            stop.set()
            try:
                store.build(plan['id'], inventory, stop=stop)
            except Cancelled:
                pass
            else:
                raise AssertionError('Cancelled build was published')
            assert store.batches(plan['id'])['batches'][0]['id'] == before
            assert store.plan(plan['id'])['inventory_revision'] is None
            return {'previous_preview_preserved': True, 'stale_preview_activation_blocked': True}
        measured('cancelled_rebuild_atomicity', cancel_rebuild)
        measured('regroup_after_cancellation', build)
        cached_page = store.batches(plan['id'])
        def idle_queries():
            with patch.object(store, '_connect', side_effect=AssertionError('idle DB access')), patch.object(inventory, '_connect', side_effect=AssertionError('idle inventory DB access')):
                for _ in range(100):
                    store.summary()
                    store.plan(plan['id'])
                    store.list()
                    store.batches(plan['id'])
                    assert not store.tick()
                    inventory.summary(state['id'])
                    inventory.list()
            return {'iterations': 100, 'sqlite_connections': 0}
        measured('idle_cached_reads', idle_queries)
        store.close()
        inventory.close()
        reopened = PlanStore(work / 'plans.sqlite3', clock=lambda: now)
        assert reopened.batches(plan['id'])['batches'][0]['id'] == cached_page['batches'][0]['id']
        reopened.close()
        for database in (work / 'inventory.sqlite3', work / 'plans.sqlite3'):
            with sqlite3.connect(database) as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        report['database_bytes'] = {path.name: path.stat().st_size for path in work.glob('*.sqlite3')}
        report['assertions'] = [
            '300k-scale initial and repeated inventories use real stat calls and store directories only.',
            'Nested tree pages are capped at 200; selected counts subtract and restore an excluded directory.',
            'All directory members appear exactly once; both hard caps and serial dates are respected.',
            'Canceling a rebuild preserves the old preview atomically and invalidates activation until rebuild.',
            'One hundred idle summary/list/cached-page/tick cycles open no SQLite connections.',
            'SQLite integrity checks pass and the last preview survives process-object restart.',
        ]
    gc.collect()
    report.update(total_seconds=time.perf_counter() - started, final_rss_mb=rss_mb(), peak_rss_mb=peak_mb())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    emit({'finished': True, 'output': str(args.output), 'seconds': report['total_seconds'], 'peak_rss_mb': report['peak_rss_mb']})


if __name__ == '__main__':
    main()

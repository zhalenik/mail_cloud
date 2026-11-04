"""Offline integration of planning, queue ownership, recovery and cloud cleanup.

Real WebService, PlanStore, inventory, SQLite queue and CloudAPI state machines
are exercised. Only remote I/O is replaced by the existing FakeCloud boundary;
SMTP delivery is disabled so these tests never contact an account or recipient.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs

from mailru_photos.api import ProtocolError, cloud_hash
from mailru_photos.auth import TokenStore, Tokens
from mailru_photos.batch_cleanup import BatchCleanupJournal
from mailru_photos.cli import write_json
from mailru_photos.export import options as export_options
from mailru_photos.notifications import NotificationManager
from mailru_photos.queue import Queue, QueueBusyError
from mailru_photos.web_service import WebService
from mailru_photos.transport import Cancelled
try:
    from test_runner import EMAIL, FakeCloud, InstantEvent
except ModuleNotFoundError:
    from tests.test_runner import EMAIL, FakeCloud, InstantEvent


class PlannedCloud(FakeCloud):
    def __init__(self):
        super().__init__()
        self.block = False
        self.block_names = set()
        self.block_after = None
        self.started = threading.Event()
        self.blocked = threading.Event()

    def _upload_blob(self, path, digest, identity, *, progress=None, is_retried=False, upload_type='AUTO'):
        self.started.set()
        if progress:
            progress(identity.size // 2)
        while self.block or Path(path).name in self.block_names or (self.block_after is not None and len(self.commits) >= self.block_after):
            self.blocked.set()
            self._check_stopped()
            time.sleep(0.002)
        return super()._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried, upload_type=upload_type)

    def _request(self, method, url, *, body=b'', **kwargs):
        result = super()._request(method, url, body=body, **kwargs)
        if method == 'POST' and url.endswith('/api/v1/private/add'):
            payload = json.loads(body)
            with self._fake_lock:
                self.remote[payload['path']]['nodeid'] = 'offline-node-' + payload['hash'][:8]
                self.remote[payload['path']]['is_unlim_upload'] = self.unlimited_flags.get(payload['path'], True)
        return result


class PlanningServiceTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict('os.environ', {'MAILRU_TIMEZONE': 'UTC'}))
        self.enterContext(patch.object(NotificationManager, 'start', return_value=None))
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.library = self.base / 'Медиатека'
        self.archive = self.library / 'Архив'
        self.manual = self.library / 'Ручные'
        self.archive.mkdir(parents=True)
        self.manual.mkdir()
        self.state = self.base / 'state'
        self.state.mkdir()
        TokenStore(self.state / 'auth.dat').save(Tokens('offline-access', 'offline-refresh', EMAIL))
        self.cloud = PlannedCloud()
        self.services = []
        self.now = time.time()
        self.service = self.make_service()
        self.service.setup('/Archive')
        self.service.settings(workers=1)

    def make_service(self):
        def factory(tokens, *, stop):
            self.cloud.stop = stop
            return self.cloud
        service = WebService(self.state, [self.library], api_factory=factory, event_factory=threading.Event)
        service.plans._clock = lambda: self.now
        service.scheduler._clock = lambda: self.now
        self.services.append(service)
        return service

    def tearDown(self):
        self.cloud.block = False
        self.cloud.block_names.clear()
        self.cloud.block_after = None
        for service in self.services:
            service.close(timeout=3)
        self.tmp.cleanup()

    def done(self, service=None):
        service = service or self.service
        deadline = time.monotonic() + 8
        while service._operation is not None and time.monotonic() < deadline:
            thread = service._thread
            if thread is not None:
                thread.join(0.02)
        self.assertIsNone(service._operation, service.snapshot()['activity'])
        return service.snapshot()

    def put(self, relative, *, parent=None):
        path = (parent or self.archive) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(relative.encode('utf-8') * 4 + b'-offline-content')
        return path

    def build(self, *, target_count=10000, activate=True, service=None):
        service = service or self.service
        inventory = service.plan_inspect(str(self.archive))['inventory']
        self.done(service)
        self.assertEqual(service.inventory.summary(inventory['id'])['status'], 'ready')
        settings = {'target_count': target_count, 'target_bytes': 20 * 1024**3, 'tolerance_percent': 0,
                    'interval_days': 1, 'weekdays': list(range(1, 8)), 'start_date': '2090-01-01',
                    'time': '10:00', 'timezone': 'UTC'}
        plan = service.plan_build({'inventory_id': inventory['id'], 'name': 'Семейный архив',
                                  'settings': settings, 'remote_prefix': '/Фото'})['plan']
        self.done(service)
        plan = service.plans.plan(plan['id'])
        self.assertEqual(plan['status'], 'draft')
        self.assertGreater(plan['batch_count'], 0, service.snapshot()['activity'])
        if activate:
            service.plan_activate(plan['id'])
            state = self.done(service)
            self.assertEqual(service.plans.plan(plan['id'])['status'], 'active', state['activity'])
        return service.plans.plan(plan['id']), service.plans.batches(plan['id'])['batches']

    def enable_outbox(self):
        self.service.notifications.save({'enabled': True, 'host': 'smtp.example.test', 'port': 465,
            'security': 'ssl', 'username': '', 'from_email': EMAIL, 'recipients': [EMAIL]})

    def outbox(self, service=None):
        manager = (service or self.service).notifications
        with manager._connect() as db:
            return [dict(row) for row in db.execute('SELECT * FROM mail_events ORDER BY created_at,event_key')]

    def add_manual(self):
        return self.service.add_folder(str(self.manual), 'Ручные', '/Manual')['source']

    def test_inventory_build_activation_run_preserves_structure_scope_and_notification_outbox(self):
        self.enable_outbox()
        self.put('корень.jpg')
        self.put('Поездки/кадр.jpg')
        other = self.put('невыбранный.jpg', parent=self.manual)
        manual = self.add_manual()
        self.service.scan(source_id=manual['id'])
        self.done()
        plan, batches = self.build()
        self.assertEqual(len(batches), 1)
        batch = batches[0]
        self.assertEqual(batch['member_count'], 2)
        self.assertEqual(self.service.schedules()['schedules'], [])
        self.service.plan_start_batch(batch['id'])
        state = self.done()
        self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'completed', state['activity'])
        self.assertEqual({row['path'] for row in self.cloud.commits}, {'/Фото/корень.jpg', '/Фото/Поездки/кадр.jpg'})
        self.assertEqual(self.service.jobs(source_id=manual['id'])['jobs'][0]['status'], 'pending')
        self.assertTrue(other.exists())
        with self.service._queue() as queue:
            receipts = queue.batch_cloud_objects(batch['id'])
            self.assertEqual(len(receipts), 2)
            self.assertEqual({row['batch_id'] for row in receipts}, {batch['id']})
            self.assertIsNone(queue.get_metadata('active_batch'))
        self.assertEqual(self.service.notifications.status()['pending'], 2)
        self.assertEqual(self.service.plans.completed_events(), [])
        self.service._flush_plan_notifications()
        self.assertEqual(self.service.notifications.status()['pending'], 2)
        self.assertEqual(state['last_scan']['trigger'], 'planned')
        self.assertFalse(state['last_scan']['warning_allowed'])

    def test_manual_later_batch_starts_only_its_group_and_plan_sources_cannot_be_bypassed(self):
        self.put('a/первый.jpg')
        self.put('b/второй.jpg')
        plan, batches = self.build(target_count=1)
        self.assertEqual(len(batches), 2)
        with self.assertRaises(ValueError):
            self.service.start()
        with self.assertRaises(ValueError):
            self.service.remove_source(batches[0]['source_id'])
        with self.assertRaises(ValueError):
            self.service.save_schedule({'name': 'Нельзя', 'source_ids': [batches[0]['source_id']],
                'time': '23:00', 'mode': 'weekly', 'weekdays': list(range(1, 8)), 'enabled': True})
        self.service.plan_start_batch(batches[1]['id'])
        self.done()
        self.assertEqual(self.service.plans.batch(batches[1]['id'])['status'], 'completed')
        self.assertEqual(self.service.plans.batch(batches[0]['id'])['status'], 'queued')
        self.assertEqual(self.cloud.commits[0]['path'], '/Фото/b/второй.jpg')
        self.assertEqual(len(self.cloud.commits), 1)

    def test_pause_restart_manual_resume_reuses_run_key_and_completed_scan(self):
        self.put('a/пауза.jpg')
        plan, batches = self.build()
        batch = batches[0]
        self.cloud.block = True
        self.service.plan_start_batch(batch['id'])
        self.assertTrue(self.cloud.blocked.wait(8), self.service.snapshot()['activity'])
        self.service.plan_pause_batch(batch['id'])
        paused_state = self.done()
        before = self.service.plans.batch(batch['id'])
        self.assertEqual(before['status'], 'paused')
        job_ids = [row['id'] for row in self.service.jobs(source_id=batch['source_id'])['jobs']]
        self.assertFalse(self.service._resume_upload)
        self.service.close()
        self.cloud.block = False
        replacement = self.make_service()
        self.assertFalse(replacement._resume_if_requested())
        with patch.object(replacement, '_scan_queue', side_effect=AssertionError('A paused batch must continue its saved scan')):
            replacement.plan_start_batch(batch['id'])
            state = self.done(replacement)
        finished = replacement.plans.batch(batch['id'])
        self.assertEqual(finished['status'], 'completed', state['activity'])
        self.assertEqual((finished['run_key'], finished['started_at']), (before['run_key'], before['started_at']))
        self.assertEqual([row['id'] for row in replacement.jobs(source_id=batch['source_id'])['jobs']], job_ids)
        self.assertEqual(len(self.cloud.commits), 1)

    def test_shutdown_auto_resume_does_not_upload_manual_source(self):
        self.put('a/план.jpg')
        self.put('другая.jpg', parent=self.manual)
        source = self.add_manual()
        self.service.scan(source_id=source['id'])
        self.done()
        plan, batches = self.build()
        batch = batches[0]
        self.cloud.block = True
        self.service.plan_start_batch(batch['id'])
        self.assertTrue(self.cloud.blocked.wait(8), self.service.snapshot()['activity'])
        self.service.close(timeout=3)
        self.assertTrue(self.service._resume_upload)
        self.cloud.block = False
        replacement = self.make_service()
        self.assertTrue(replacement._resume_if_requested())
        state = self.done(replacement)
        self.assertEqual(replacement.plans.batch(batch['id'])['status'], 'completed', state['activity'])
        self.assertEqual([row['path'] for row in self.cloud.commits], ['/Фото/a/план.jpg'])
        self.assertEqual(replacement.jobs(source_id=source['id'])['jobs'][0]['status'], 'pending')

    def test_activation_conflict_rolls_back_all_owned_groups_and_keeps_existing_source(self):
        self.put('a/первый.jpg')
        self.put('b/второй.jpg')
        existing = self.service.add_folder(str(self.archive / 'b'), 'Уже подключена', '/Existing')['source']
        plan, batches = self.build(target_count=1, activate=False)
        self.service.plan_activate(plan['id'])
        state = self.done()
        self.assertEqual(self.service.plans.plan(plan['id'])['status'], 'draft', state['activity'])
        with self.service._queue() as queue:
            groups = queue.source_groups()
            self.assertEqual([row['id'] for row in groups], [existing['id']])
        self.assertTrue(all(row['source_id'] is None for row in self.service.plans.batches(plan['id'])['batches']))
        self.service.remove_source(existing['id'])
        self.service.plan_activate(plan['id'])
        state = self.done()
        self.assertEqual(self.service.plans.plan(plan['id'])['status'], 'active', state['activity'])

    def test_exclusion_invalidates_draft_then_replace_draft_build_publishes_new_scope(self):
        self.put('a/one.jpg')
        self.put('b/two.jpg')
        plan, _ = self.build(activate=False)
        inventory = self.service.inventory.summary(plan['inventory_id'])
        tree = self.service.inventory.tree(inventory['id'], inventory['root_id'])
        excluded = next(row for row in tree['folders'] if row['name'] == 'b')
        self.service.planning_request('POST', '/api/plans/exclude', {},
            {'inventory_id': inventory['id'], 'folder_id': excluded['id'], 'excluded': True})
        with self.assertRaises(ValueError):
            self.service.plan_activate(plan['id'])
        self.service.plan_build({'replace_draft_id': plan['id'], 'inventory_id': inventory['id'], 'name': plan['name'],
            'settings': plan['settings'], 'remote_prefix': plan['remote_prefix']})
        self.done()
        result = self.service.plans.plan(plan['id'])
        self.assertEqual(result['file_count'], 1)
        self.assertEqual(self.service.plans.list()['total'], 1)
        self.service.plan_activate(plan['id'])
        self.done()
        batch = self.service.plans.batches(plan['id'])['batches'][0]
        self.service.plan_start_batch(batch['id'])
        self.done()
        self.assertEqual([row['path'] for row in self.cloud.commits], ['/Фото/a/one.jpg'])

    def test_scoped_multifolder_scan_export_and_task_deletion_keep_other_source(self):
        a = self.library / 'a'
        b = self.library / 'b'
        a.mkdir(); b.mkdir()
        self.put('a.jpg', parent=a)
        self.put('b.jpg', parent=b)
        self.put('manual.jpg', parent=self.manual)
        combined = self.service.add_folder(None, 'Несколько', '/Combined', paths=[str(a), str(b)])['source']
        manual = self.add_manual()
        self.service.scan()
        self.done()
        before_manual = self.service.jobs(source_id=manual['id'])['jobs'][0]['id']
        self.put('second.jpg', parent=a)
        self.service.scan(source_id=combined['id'])
        self.done()
        self.assertEqual(self.service.jobs(source_id=combined['id'])['total'], 3)
        self.assertEqual(self.service.jobs(source_id=manual['id'])['jobs'][0]['id'], before_manual)
        exported = b''.join(self.service.export_jobs(export_options(parse_qs(f'source_id={combined["id"]}&fields=name&format=txt&header=0')))).decode()
        self.assertEqual(set(exported.splitlines()), {'a.jpg', 'b.jpg', 'second.jpg'})
        preview = self.service.preview_deletion(source_id=combined['id'], owner='offline-session')
        self.assertEqual(preview['task_count'], 3)
        self.service.confirm_deletion(preview['token'], owner='offline-session')
        self.assertEqual(self.service.jobs(source_id=combined['id'])['total'], 0)
        self.assertEqual(self.service.jobs(source_id=manual['id'])['total'], 1)
        self.assertTrue((a / 'a.jpg').exists())
        self.assertEqual(self.cloud.trashed, [])

    def test_timer_waits_for_shared_operation_and_reschedules_missed_slot(self):
        self.put('a/one.jpg')
        plan, batches = self.build()
        batch = batches[0]
        self.now = batch['scheduled_for']
        self.service._operation = 'quota'
        try:
            self.assertFalse(self.service.plans.tick())
            self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'queued')
            self.assertEqual(self.cloud.commits, [])
        finally:
            self.service._operation = None
        self.now += 120
        self.service.plans.wake()
        self.assertFalse(self.service.plans.tick())
        moved = self.service.plans.batch(batch['id'])
        self.assertGreater(moved['scheduled_for'], self.now)
        self.now = moved['scheduled_for']
        self.assertTrue(self.service.plans.tick())
        state = self.done()
        self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'completed', state['activity'])

    def pause_with_one_owned_and_one_existing(self, *, with_other=False):
        first = self.put('a/00-created.jpg')
        waiting = self.put('a/99-wait.jpg')
        existing = self.put('a/existing.jpg')
        digest, identity = cloud_hash(existing)
        self.cloud.remote['/Old/preexisting.jpg'] = {'hash': digest, 'size': identity.size, 'nodeid': 'old-preexisting'}
        if with_other:
            self.put('b/other-batch.jpg')
        plan, batches = self.build(target_count=3 if with_other else 10000)
        batch = next(row for row in batches if row['file_count'] == 3)
        if with_other:
            other = next(row for row in batches if row['id'] != batch['id'])
            self.service.plan_start_batch(other['id'])
            self.done()
            self.other_batch_id = other['id']
            self.other_remote_path = self.cloud.commits[0]['path']
        before_count = len(self.cloud.commits)
        self.cloud.block_after = before_count + 1
        self.service.plan_start_batch(batch['id'])
        self.assertTrue(self.cloud.blocked.wait(8), self.service.snapshot()['activity'])
        self.service.plan_pause_batch(batch['id'])
        state = self.done()
        self.cloud.block_names.clear()
        self.cloud.block_after = None
        self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'paused', state['activity'])
        self.assertEqual(len(self.cloud.commits), before_count + 1)
        return plan, batch, [first, waiting, existing]

    def test_batch_cleanup_only_owned_objects_then_resume_rechecks_cloud(self):
        plan, batch, files = self.pause_with_one_owned_and_one_existing()
        preview = self.service.plan_cleanup_preview(batch['id'], owner='session-A')
        self.assertEqual(preview['count'], 1)
        with self.assertRaises(ValueError):
            self.service.plan_cleanup(batch['id'], preview['token'], owner='session-B')
        self.service.plan_cleanup(batch['id'], preview['token'], owner='session-A')
        state = self.done()
        self.assertEqual(self.cloud.trashed, [self.cloud.commits[0]['path']], state['activity'])
        self.assertIn('/Old/preexisting.jpg', self.cloud.remote)
        self.assertTrue(all(path.exists() for path in files))
        self.assertIsNone(self.service._batch_cleanup_pending)
        with self.service._queue() as queue:
            self.assertEqual(queue.batch_cloud_objects(batch['id']), [])
        self.service.plan_start_batch(batch['id'])
        state = self.done()
        self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'completed', state['activity'])
        self.assertEqual(len(self.cloud.commits), 3, 'One owned file is uploaded again; pre-existing object is never owned or uploaded')
        with self.service._queue() as queue:
            self.assertEqual(len(queue.batch_cloud_objects(batch['id'])), 2)

    def test_changed_remote_identity_is_not_trashed_by_batch_cleanup(self):
        plan, batch, _ = self.pause_with_one_owned_and_one_existing()
        preview = self.service.plan_cleanup_preview(batch['id'], owner='A')
        owned_path = self.cloud.commits[0]['path']
        self.cloud.remote[owned_path]['hash'] = 'F' * 40
        self.service.plan_cleanup(batch['id'], preview['token'], owner='A')
        state = self.done()
        self.assertEqual(self.cloud.trashed, [])
        self.assertIn(owned_path, self.cloud.remote)
        with BatchCleanupJournal(self.state / 'batch-cleanup.sqlite3') as journal:
            self.assertEqual(journal.summary()['failed'], 1)
        self.assertEqual(self.service.plans.batch(batch['id'])['status'], 'paused', state['activity'])

    def test_cleanup_receipts_survive_removed_tasks_and_never_include_other_batch(self):
        plan, batch, files = self.pause_with_one_owned_and_one_existing(with_other=True)
        owned_path = self.cloud.commits[-1]['path']
        self.service.clear_tasks(source_id=batch['source_id'])
        self.assertEqual(self.service.jobs(source_id=batch['source_id'])['total'], 0)
        preview = self.service.plan_cleanup_preview(batch['id'], owner='A')
        self.assertEqual(preview['count'], 1)
        self.service.plan_cleanup(batch['id'], preview['token'], owner='A')
        state = self.done()
        self.assertEqual(self.cloud.trashed, [owned_path], state['activity'])
        self.assertIn(self.other_remote_path, self.cloud.remote)
        self.assertIn('/Old/preexisting.jpg', self.cloud.remote)
        with self.service._queue() as queue:
            self.assertEqual(len(queue.batch_cloud_objects(self.other_batch_id)), 1)
            self.assertEqual(queue.batch_cloud_objects(batch['id']), [])
        self.assertTrue(all(path.exists() for path in files))

    def test_cleanup_restart_after_cloud_accept_before_receipt_checkpoint_is_idempotent(self):
        plan, batch, _ = self.pause_with_one_owned_and_one_existing()
        owned_path = self.cloud.commits[0]['path']
        preview = self.service.plan_cleanup_preview(batch['id'], owner='A')
        with patch.object(Queue, 'mark_batch_cloud_deleted', side_effect=Cancelled('offline interrupted checkpoint')):
            self.service.plan_cleanup(batch['id'], preview['token'], owner='A')
            self.done()
        self.assertEqual(self.cloud.trashed, [owned_path])
        self.assertIsNotNone(self.service._batch_cleanup_pending)
        self.service.close()
        replacement = self.make_service()
        self.assertFalse(replacement._resume_if_requested())
        replacement.start()
        state = self.done(replacement)
        self.assertEqual(self.cloud.trashed, [owned_path], 'No second blind cloud delete after lost local checkpoint')
        self.assertIsNone(replacement._batch_cleanup_pending, state['activity'])
        self.assertEqual(replacement.plans.batch(batch['id'])['status'], 'paused')
        with replacement._queue() as queue:
            self.assertEqual(queue.batch_cloud_objects(batch['id']), [])

    def test_terminal_task_retry_opens_new_completion_cycle_without_rescanning_successes(self):
        self.enable_outbox()
        self.put('a/good.jpg')
        self.put('a/bad.jpg')
        self.cloud.failures['bad.jpg'] = [ProtocolError('offline permanent file error')]
        plan, batches = self.build()
        batch = batches[0]
        self.service.plan_start_batch(batch['id'])
        state = self.done()
        before = self.service.plans.batch(batch['id'])
        self.assertEqual(before['status'], 'completed_with_errors', state['activity'])
        failed = self.service.jobs('failed', source_id=batch['source_id'])['jobs'][0]
        with patch.object(self.service, '_scan_queue', side_effect=AssertionError('Task retry must preserve the completed scan')):
            self.service.control_task(failed['id'], 'start')
            state = self.done()
        after = self.service.plans.batch(batch['id'])
        self.assertEqual(after['status'], 'completed', state['activity'])
        self.assertNotEqual(after['run_key'], before['run_key'])
        self.assertEqual(self.service.plans.plan(plan['id'])['completion_cycle'], 1)
        self.assertEqual(sorted(row['path'] for row in self.cloud.commits), ['/Фото/a/bad.jpg', '/Фото/a/good.jpg'])
        self.assertEqual(self.service.notifications.status()['pending'], 4)
        self.assertEqual(len({row['event_key'] for row in self.outbox()}), 4)

    def test_activation_recovery_reuses_owned_group_created_before_plan_bind(self):
        self.put('a/one.jpg')
        plan, batches = self.build(activate=False)
        batch = batches[0]
        self.service.plans.activate(plan['id'], plan['inventory_revision'])
        members = list(self.service.plans.iter_members(batch['id']))
        alias = plan['name'][:150] + ' · ' + batch['name'][:60] + ' · ' + batch['id'][:8]
        with self.service._queue() as queue, queue.exclusive_run():
            created = queue.add_source_group(members, alias, plan['remote_prefix'], owner_key='plan-batch:' + batch['id'])
        self.service.close()
        replacement = self.make_service()
        replacement._start_planning()
        state = self.done(replacement)
        self.assertEqual(replacement.plans.plan(plan['id'])['status'], 'active', state['activity'])
        self.assertEqual(replacement.plans.batch(batch['id'])['source_id'], created['id'])
        with replacement._queue() as queue:
            self.assertEqual(len(queue.source_groups()), 1)
        self.assertEqual(self.cloud.commits, [])

    def test_delayed_event_from_deleted_plan_does_not_block_other_notifications(self):
        self.enable_outbox()
        self.put('a/one.jpg')
        plan, batches = self.build()
        batch = batches[0]
        with patch.object(self.service, '_flush_plan_notifications', return_value=None):
            self.service.plan_start_batch(batch['id'])
            self.done()
        self.assertEqual(len(self.service.plans.completed_events()), 1)
        self.service.planning_request('POST', '/api/plans/delete', {}, {'id': plan['id']})
        self.service._flush_plan_notifications()
        self.assertEqual(self.service.plans.completed_events(), [])
        self.assertEqual(self.service.notifications.status()['pending'], 1)

    def test_completion_shutdown_boundary_keeps_email_event_for_reopen(self):
        self.enable_outbox()
        self.put('a/one.jpg')
        plan, batches = self.build()
        batch = batches[0]
        original_finish = self.service.plans.finish
        def shut_down_after_checkpoint(*args, **kwargs):
            result = original_finish(*args, **kwargs)
            if args[1] == 'completed':
                self.service.close(timeout=0)
            return result
        with patch.object(self.service.plans, 'finish', side_effect=shut_down_after_checkpoint):
            self.service.plan_start_batch(batch['id'])
            self.done()
        replacement = self.make_service()
        replacement._flush_plan_notifications()
        self.assertFalse(replacement._resume_if_requested())
        self.assertEqual(replacement.notifications.status()['pending'], 2)
        self.assertEqual(replacement.plans.completed_events(), [])


if __name__ == '__main__':
    unittest.main()

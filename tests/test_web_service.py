"""Offline tests for the UI controller using the real upload/queue state machine."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import sqlite3
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mailru_photos.api import PROTOCOL_VERSION, ProtocolError, RemoteError, cloud_hash
from mailru_photos.auth import AuthError, TokenStore, Tokens
from mailru_photos.cli import write_json
from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.queue import Queue, QueueBusyError, QueueError
from mailru_photos.runner import process_job
from mailru_photos.transport import Cancelled, TransportError
from mailru_photos.web_service import WebService
from test_runner import EMAIL, FakeCloud, InstantEvent


class EventFactory:
    cancel_next = None
    def __call__(self):
        value = InstantEvent(cancel_on_wait=self.cancel_next)
        self.cancel_next = None
        return value


class UICloud(FakeCloud):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.block_upload = False
        self.stop_error = None
        self.upload_started = threading.Event()
    def account_info(self):
        return self.require_account()
    def _upload_blob(self, path, digest, identity, *, progress=None, is_retried=False):
        self.upload_started.set()
        while self.block_upload:
            if self.stop.is_set() and self.stop_error is not None:
                raise self.stop_error
            self._check_stopped()
            time.sleep(0.002)
        if progress:
            progress(identity.size)
        return super()._upload_blob(path, digest, identity, progress=progress, is_retried=is_retried)


class WebServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.photos = self.root / 'photos'
        self.photos.mkdir()
        self.state = self.root / 'state'
        self.state.mkdir()
        self.cloud = UICloud()
        self.events = EventFactory()
        self.services = []
        TokenStore(self.state / 'auth.dat').save(Tokens('offline-access', 'offline-refresh', EMAIL))
        self.service = self.make_service()
        self.service.setup('/Archive')
        self.service.add_folder(str(self.photos), 'Photos')

    def tearDown(self):
        for service in self.services:
            service.close(timeout=2)
        self.tmp.cleanup()

    def make_service(self, *, state_dir=None, **kwargs):
        def factory(tokens, *, stop):
            self.cloud.stop = stop
            return self.cloud
        service = WebService(state_dir or self.state, [self.photos], api_factory=factory,
                             event_factory=self.events, **kwargs)
        self.services.append(service)
        return service

    def photos_count(self, count):
        for i in range(count):
            (self.photos / f'{i:03}.jpg').write_bytes(b'offline-image-' + str(i).encode() + b'x' * 64)

    def large_video(self, name='large.mp4'):
        # Sparse, synthetic queue fixture. Neither its body nor a real account
        # is read: the hash and all CloudAPI network boundaries are replaced.
        path = self.photos / name
        with path.open('wb') as stream:
            stream.truncate(UNLIMITED_MAX_BYTES + 1024)
        return path



    def done(self, service=None):
        service = service or self.service
        deadline = time.monotonic() + 5
        while service._operation is not None and time.monotonic() < deadline:
            service._thread.join(timeout=0.05)
        self.assertIsNone(service._operation, 'Background operation did not finish')
        return service.snapshot()

    def test_task_pause_resume_survives_restart_and_keeps_global_pause(self):
        self.photos_count(2)
        self.service.scan(); self.done()
        task = self.service.jobs()['jobs'][0]
        self.service.pause()
        self.service.control_task(task['id'], 'pause')
        self.assertTrue(self.service.jobs('paused')['jobs'][0]['paused'])
        self.assertEqual(self.service.jobs('paused')['total'], 1)
        self.service.close()
        replacement = self.make_service()
        self.assertEqual(replacement.snapshot()['stats']['paused'], 1)
        replacement.control_task(task['id'], 'resume')
        self.assertEqual(replacement.snapshot()['stats']['paused'], 0)
        self.assertIsNone(replacement._operation)
        self.assertFalse(replacement._resume_upload)
        replacement.settings(preflight_check=False)
        replacement.start()
        self.assertEqual(self.done(replacement)['stats']['uploaded'], 2)

    def test_task_signal_belongs_to_claim_and_does_not_set_global_stop(self):
        self.photos_count(1)
        self.service.scan(); self.done()
        with self.service._queue() as queue:
            job = queue.claim()
            self.service._operation = 'upload'
            self.service._activity['phase'] = 'uploading'
            try:
                with self.service._task_scope(job) as event:
                    self.service._emit('file_started', {'job_id':job.id, 'path':job.remote_path, 'size':job.size})
                    self.service._emit('upload_progress', {'job_id':job.id, 'bytes':job.size//2})
                    row = self.service.jobs()['jobs'][0]
                    self.assertGreater(row['progress']['percent'], 0)
                    self.assertLess(row['progress']['percent'], 100)
                    self.service.control_task(job.id, 'pause')
                    self.assertTrue(event.is_set())
                    self.assertFalse(self.service._stop.is_set())
                    self.service.control_task(job.id, 'resume')
                    self.assertTrue(event.is_set(), 'Never revive the old attempt signal')
                    queue.release(job)
                self.assertEqual(self.service._task_signals, {})
                self.assertEqual(queue.stats()['remaining'], 1)
            finally:
                self.service._operation = None
                self.service._files.clear()

    def test_remove_and_clear_tasks_preserve_sources_files_and_completed_scan(self):
        self.photos_count(3)
        self.service.scan(); self.done()
        previous = self.service.snapshot()
        first = self.service.jobs()['jobs'][0]
        self.service.control_task(first['id'], 'remove')
        self.assertEqual(self.service.snapshot()['stats']['total'], 2)
        self.assertTrue(Path(first['local_path']).exists())
        cleared = self.service.clear_tasks('all')
        self.assertEqual(cleared['removed_tasks'], 2)
        state = self.service.snapshot()
        self.assertEqual(state['sources'], previous['sources'])
        self.assertEqual(state['last_scan'], previous['last_scan'])
        self.service.start()
        self.assertEqual(self.done()['stats']['total'], 0, 'Start must not undo a deliberate clear')
        self.assertEqual(self.cloud.commits, [])
        self.service.scan(); self.done()
        self.assertEqual(self.service.snapshot()['stats']['total'], 3)

    def test_task_controls_respect_external_run_lock_and_scan_exclusion(self):
        self.photos_count(1)
        self.service.scan(); self.done()
        task_id = self.service.jobs()['jobs'][0]['id']
        with self.service._queue() as external, external.exclusive_run():
            with self.assertRaises(QueueBusyError):
                self.service.control_task(task_id, 'remove')
            with self.assertRaises(QueueBusyError):
                self.service.clear_tasks('all')
        for operation in ('scanning', 'login', 'logout'):
            self.service._operation = operation
            self.service._activity['phase'] = operation
            try:
                self.assertFalse(self.service.snapshot()['task_controls_available'])
                with self.assertRaises(QueueBusyError):
                    self.service.control_task(task_id, 'pause')
                with self.assertRaises(QueueBusyError):
                    self.service.clear_tasks('all')
            finally:
                self.service._operation = None
        self.assertEqual(self.service.snapshot()['stats']['total'], 1)

    def test_only_individually_paused_tasks_finish_without_failed_probe(self):
        self.photos_count(1)
        self.service.scan(); self.done()
        task_id = self.service.jobs()['jobs'][0]['id']
        self.service.control_task(task_id, 'pause')
        with patch('mailru_photos.web_service.probe', side_effect=AssertionError('No runnable tasks')):
            self.service.start()
            state = self.done()
        self.assertEqual(state['activity']['phase'], 'paused')
        self.assertIsNone(state['activity']['last_error'])
        self.assertEqual(state['stats']['paused'], 1)
        self.assertFalse(state['stats']['failed'])

    def test_pausing_one_active_task_does_not_stop_another_worker(self):
        self.photos_count(2)
        self.service.scan(); self.done()
        first = self.service.jobs()['jobs'][0]
        entered, release = threading.Event(), threading.Event()
        original = self.cloud._upload_blob
        def upload(path, digest, identity, **kwargs):
            if str(path) == first['local_path']:
                entered.set()
                while not release.is_set():
                    self.cloud._check_stopped()
                    time.sleep(0.002)
            return original(path, digest, identity, **kwargs)
        self.cloud._upload_blob = upload
        self.service.settings(preflight_check=False, workers=2)
        self.service.start()
        self.assertTrue(entered.wait(2))
        self.assertTrue(self.service.snapshot()['task_controls_available'])
        self.assertFalse(self.service.snapshot()['queue_clear_available'])
        self.service.control_task(first['id'], 'pause')
        state = self.done()
        self.assertEqual(state['stats']['uploaded'], 1, state)
        self.assertEqual(state['stats']['paused'], 1)
        self.assertEqual(state['stats']['failed'], 0)
        self.assertFalse(self.service._stop.is_set())
        release.set()
        self.service.control_task(first['id'], 'resume')
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)

    def test_remove_at_positive_commit_preserves_receipt_without_false_success(self):
        self.photos_count(1)
        self.service.settings(preflight_check=False)
        original = Queue.confirm_unlimited_commit
        def confirmed_then_remove(queue, job, digest):
            original(queue, job, digest)
            self.service.control_task(job.id, 'remove')
        with patch.object(Queue, 'confirm_unlimited_commit', confirmed_then_remove):
            self.service.start()
            state = self.done()
        self.assertEqual(state['stats']['total'], 0, state)
        self.assertEqual(len(self.cloud.commits), 1)
        self.assertEqual(len(list(self.photos.iterdir())), 1)
        with self.service._queue() as queue:
            self.assertEqual(queue._db.execute("SELECT COUNT(*) FROM commit_evidence WHERE stage='accepted'").fetchone()[0], 1)
            self.assertEqual(queue._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0], 0)
        self.assertFalse(self.service._stop.is_set())

    def test_probe_can_pause_its_current_task_and_continue_with_another(self):
        self.photos_count(2)
        self.service.scan(); self.done()
        first = self.service.jobs()['jobs'][0]
        entered = threading.Event()
        original = self.cloud._upload_blob
        def upload(path, digest, identity, **kwargs):
            if str(path) == first['local_path']:
                entered.set()
                while True:
                    self.cloud._check_stopped()
                    time.sleep(0.002)
            return original(path, digest, identity, **kwargs)
        self.cloud._upload_blob = upload
        self.service.start()
        self.assertTrue(entered.wait(2))
        self.assertEqual(self.service.snapshot()['activity']['phase'], 'probing')
        self.service.control_task(first['id'], 'pause')
        state = self.done()
        self.assertEqual(state['probe']['result'], 'provisional_pass', state)
        self.assertEqual(state['stats']['uploaded'], 1)
        self.assertEqual(state['stats']['paused'], 1)
        self.assertFalse(self.service._stop.is_set())

    def test_quota_cache_combines_cloud_and_mail_without_network_on_poll(self):
        self.assertIsNone(self.service.snapshot()['quota'])
        self.assertEqual(self.cloud.quota_calls, 0)
        self.cloud.quota = lambda: {'used_bytes':10, 'mail_bytes':20, 'combined_used_bytes':30, 'total_bytes':100}
        self.service.refresh_quota()
        state = self.done()
        quota = state['quota']
        self.assertEqual((quota['used_bytes'], quota['cloud_bytes'], quota['mail_bytes'], quota['free_bytes']), (30,10,20,70))
        self.assertFalse(quota['stale'])
        measured = quota['updated_at']
        with patch.object(self.cloud, 'quota', side_effect=AssertionError('GET state must use cache')):
            self.assertEqual(self.service.snapshot()['quota']['updated_at'], measured)
        self.assertTrue(self.service.snapshot()['quota_refresh_available'])

    def test_quota_failure_retains_previous_values_and_marks_them_stale(self):
        self.service.refresh_quota(); state = self.done()
        previous = state['quota']
        self.cloud.quota_values = [TransportError('Offline quota unavailable')]
        self.service.refresh_quota(); state = self.done()
        self.assertEqual(state['quota']['used_bytes'], previous['used_bytes'])
        self.assertEqual(state['quota']['updated_at'], previous['updated_at'])
        self.assertTrue(state['quota']['stale'])
        self.assertIn('Offline quota', state['quota']['error'])
        self.assertIsNotNone(state['quota_error'])
        self.assertTrue(state['authenticated'])

    def test_quota_rejected_credentials_disconnects_account_and_retains_stale_values(self):
        for number, error in enumerate((AuthError('Expired quota fixture'), RemoteError(401), RemoteError(403))):
            with self.subTest(error=type(error).__name__):
                state_dir = self.root / f'quota-auth-{number}'
                state_dir.mkdir()
                TokenStore(state_dir / 'auth.dat').save(Tokens('saved-access', 'saved-refresh', EMAIL))
                service = self.make_service(state_dir=state_dir)
                self.cloud.quota_values = [100]
                service.refresh_quota(); before = self.done(service)['quota']
                self.cloud.quota_values = [error]
                service.refresh_quota(); state = self.done(service)
                self.assertFalse(state['authenticated'])
                self.assertFalse(state['quota_refresh_available'])
                self.assertTrue(state['quota']['stale'])
                self.assertEqual(state['quota']['updated_at'], before['updated_at'])
                self.assertEqual(state['quota']['used_bytes'], before['used_bytes'])
                self.assertTrue(state['quota']['error'])
                self.assertTrue((state_dir / 'auth.dat').is_file(), 'Never discard tokens as a side effect')

    def test_failed_run_rejection_updates_auth_indicator_and_stales_quota(self):
        self.photos_count(2)
        self.service.scan(); self.done()
        self.service.settings(preflight_check=False)
        first = self.service.jobs()['jobs'][0]
        self.cloud.failures[Path(first['local_path']).name] = [RemoteError(403)]
        self.service.start(); state = self.done()
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertFalse(state['authenticated'])
        self.assertTrue(state['quota']['stale'])
        self.assertTrue(state['quota']['error'])
        self.assertFalse(state['quota_refresh_available'])
        self.assertEqual(state['stats']['pending'], 1)
        self.assertEqual(state['stats']['failed'], 1)
        self.assertEqual(self.cloud.commits, [])
        with self.service._queue() as queue:
            self.assertTrue(queue.get_metadata('last_run')['auth_required'])
        with self.assertRaises(ValueError):
            self.service.start()

    def test_auth_rejection_survives_restart_until_successful_login(self):
        self.service.refresh_quota(); self.done()
        self.cloud.quota_values = [RemoteError(403)]
        self.service.refresh_quota(); state = self.done()
        self.assertTrue(state['auth_required'])
        self.service.close()
        activity = json.loads((self.state / 'web_activity.json').read_text())
        self.assertTrue(activity['auth_required'], 'Graceful shutdown must preserve the rejection')
        # Even an older pending upload intention must not override an explicit
        # rejected-authentication marker when the controller restarts.
        activity['resume_upload'] = True
        write_json(self.state / 'web_activity.json', activity)
        replacement = self.make_service()
        self.assertFalse(replacement.snapshot()['authenticated'])
        self.assertTrue(replacement.snapshot()['auth_required'])
        account_calls = self.cloud.account_calls
        self.assertFalse(replacement.resume_if_requested())
        self.assertEqual(self.cloud.account_calls, account_calls)
        self.assertTrue((self.state / 'auth.dat').is_file())
        class Auth:
            def login(self, email, password):
                return Tokens('accepted-relogin-access', 'accepted-relogin-refresh', email)
        replacement._auth_client_factory = Auth
        self.cloud.quota_values = [100]
        replacement.login(EMAIL, 'disposable-fixture-password'); state = self.done(replacement)
        self.assertTrue(state['authenticated'])
        self.assertFalse(state['auth_required'])
        self.assertFalse(state['quota']['stale'])
        replacement.close()
        restored = self.make_service()
        self.assertTrue(restored.snapshot()['authenticated'])
        self.assertFalse(restored.snapshot()['auth_required'])

    def test_manual_pause_racing_auth_rejection_does_not_lose_the_rejection(self):
        self.photos_count(1)
        self.service.settings(preflight_check=False)
        def reject_after_pause(*args, **kwargs):
            self.service.pause()
            raise RemoteError(403)
        with patch.object(self.cloud, '_upload_blob', side_effect=reject_after_pause):
            self.service.start(); state = self.done()
        self.assertEqual(state['activity']['phase'], 'paused')
        self.assertFalse(state['authenticated'])
        self.assertTrue(state['auth_required'])
        self.assertTrue(state['quota']['stale'])
        self.assertTrue(json.loads((self.state / 'web_activity.json').read_text())['auth_required'])
        self.assertEqual(self.cloud.commits, [])

    def test_old_activity_without_auth_required_keeps_saved_login(self):
        state_dir = self.root / 'old-activity'
        state_dir.mkdir()
        TokenStore(state_dir / 'auth.dat').save(Tokens('old-access', 'old-refresh', EMAIL))
        write_json(state_dir / 'web_activity.json', {'version': 1, 'phase': 'idle', 'resume_upload': False})
        restored = self.make_service(state_dir=state_dir)
        state = restored.snapshot()
        self.assertTrue(state['authenticated'])
        self.assertFalse(state['auth_required'])

    def test_explicit_logout_clears_auth_rejection_without_deleting_queue(self):
        self.photos_count(1)
        self.service.scan(); self.done()
        self.cloud.quota_values = [RemoteError(401)]
        self.service.refresh_quota(); self.done()
        self.service.logout(); state = self.done()
        self.assertFalse(state['authenticated'])
        self.assertFalse(state['auth_required'])
        self.assertIsNone(state['quota'])
        self.assertIsNone(state['quota_error'])
        self.assertEqual(state['stats']['pending'], 1)
        self.assertFalse((self.state / 'auth.dat').exists())

    def test_upload_measurements_update_quota_without_extra_cloud_calls(self):
        self.photos_count(1)
        self.service.settings(preflight_check=False)
        self.service.start(); state = self.done()
        self.assertEqual(state['quota']['cloud_bytes'], 100)
        self.assertEqual(state['quota']['used_bytes'], 100)
        self.assertEqual(self.cloud.quota_calls, 2, 'Use the existing baseline/final guard reads')
        self.service.logout(); state = self.done()
        self.assertIsNone(state['quota'])
        self.assertFalse(state['quota_refresh_available'])

    def test_quota_refresh_uses_controller_and_external_cli_locks(self):
        self.service._operation = 'upload'
        try:
            self.assertFalse(self.service.snapshot()['quota_refresh_available'])
            with self.assertRaises(QueueBusyError):
                self.service.refresh_quota()
        finally:
            self.service._operation = None
        with self.service._queue() as external, external.exclusive_run():
            self.service.refresh_quota()
            state = self.done()
        self.assertIsNone(state['quota'])
        self.assertEqual(self.cloud.quota_calls, 0)
        self.assertEqual(state['activity']['phase'], 'error')

    def test_disabled_reconciliation_does_not_call_optional_module(self):
        self.photos_count(1)
        with patch('mailru_photos.reconcile.reconcile', side_effect=AssertionError('Disabled optional pass')) as reconcile:
            self.service.scan(); state = self.done()
            self.assertFalse(reconcile.called)
        self.assertTrue(state['last_scan']['complete'])
        self.assertEqual(state['stats']['pending'], 1)
        self.assertEqual(self.cloud.account_calls, 0)

    def test_optional_reconciliation_marks_existing_without_new_upload_or_proof(self):
        self.photos_count(1)
        path = self.photos / '000.jpg'
        digest, identity = cloud_hash(path)
        self.cloud.remote['/Archive/Photos/000.jpg'] = {'hash':digest, 'size':identity.size}
        self.service.settings(remote_reconcile=True)
        self.service.scan(); state = self.done()
        self.assertTrue(state['last_scan']['complete'])
        self.assertEqual(state['stats']['existing'], 1, state)
        self.assertEqual(state['stats']['uploaded'], 0)
        job = self.service.jobs()['jobs'][0]
        self.assertEqual(job['skip_reason'], 'remote_exists')
        self.assertIsNone(job['storage_mode'])
        self.assertTrue(job['cloud_url'].startswith('https://cloud.mail.ru/home/'))
        with self.service._queue() as queue:
            self.assertEqual(queue.upload_history(job['id']), [])
        self.service.start(); state = self.done()
        self.assertEqual(self.cloud.commits, [])
        self.service.close()
        replacement = self.make_service()
        self.assertTrue(replacement.snapshot()['settings']['remote_reconcile'])

    def test_optional_module_failure_leaves_scan_and_normal_uploader_usable(self):
        self.photos_count(2)
        self.service.settings(remote_reconcile=True)
        with patch('mailru_photos.reconcile.reconcile', side_effect=RuntimeError('Optional module fixture failure')):
            self.service.scan(); state = self.done()
        self.assertTrue(state['last_scan']['complete'])
        self.assertEqual(state['stats']['pending'], 2)
        self.assertEqual(state['activity']['phase'], 'idle')
        self.service.settings(remote_reconcile=False, preflight_check=False)
        self.service.start(); state = self.done()
        self.assertEqual(state['stats']['uploaded'], 2)

    def test_early_reconcile_account_rejection_updates_auth_and_keeps_local_scan(self):
        self.photos_count(1)
        self.service.settings(remote_reconcile=True)
        self.service.refresh_quota(); self.done()
        with patch.object(self.cloud, 'require_account', side_effect=RemoteError(401)):
            self.service.scan(); state = self.done()
        self.assertFalse(state['authenticated'])
        self.assertTrue(state['quota']['stale'])
        self.assertTrue(state['last_scan']['complete'])
        self.assertEqual(state['stats']['pending'], 1)
        self.assertEqual(state['activity']['phase'], 'idle')
        self.assertEqual(self.cloud.commits, [])

    def test_pipeline_does_not_upload_after_optional_comparison_loses_auth(self):
        self.photos_count(2)
        self.service.settings(remote_reconcile=True, preflight_check=False)
        with patch.object(self.cloud, 'stat', side_effect=RemoteError(403)):
            self.service.start(); state = self.done()
        self.assertFalse(state['authenticated'])
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertTrue(state['last_scan']['complete'])
        self.assertEqual(state['stats']['pending'], 2)
        self.assertEqual(self.cloud.account_calls, 1, 'Only the comparison may validate this rejected account')
        self.assertEqual(self.cloud.quota_calls, 0)
        self.assertEqual(self.cloud.upload_calls, [])
        self.assertEqual(self.cloud.commits, [])

    def test_task_source_and_links_use_id_and_encoded_private_cloud_path(self):
        path = self.photos / 'Фото #?%.JPG'
        path.write_bytes(b'original source fixture')
        self.service.scan(); self.done()
        task = self.service.jobs()['jobs'][0]
        source, local, identity = self.service.task_source(task['id'])
        self.assertEqual(source, self.photos)
        self.assertEqual(local, path)
        self.assertEqual(identity.size, path.stat().st_size)
        self.assertEqual(task['preview_url'], f"/api/tasks/{task['id']}/content?download=0")
        self.assertEqual(task['download_url'], f"/api/tasks/{task['id']}/content?download=1")
        self.assertIsNone(task['cloud_url'])
        self.service.settings(preflight_check=False)
        self.service.start(); self.done()
        uploaded = self.service.jobs()['jobs'][0]
        self.assertIn('%23%3F%25.JPG', uploaded['cloud_url'])
        self.assertNotIn('#', uploaded['cloud_url'])
        self.service.control_task(task['id'], 'remove')
        with self.assertRaises(FileNotFoundError):
            self.service.task_source(task['id'])

    def test_start_scans_automatically_probes_and_uploads_all_files(self):
        self.photos_count(3)
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['uploaded'], 3)
        self.assertEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(len(self.cloud.commits), 3)
        self.assertTrue(all(p['unlimited_upload'] is True for p in self.cloud.commits))
        activity = json.loads((self.state / 'web_activity.json').read_text())
        self.assertFalse(activity['resume_upload'])
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 3)
        self.assertEqual(len(self.cloud.commits), 3, 'Repeated start must not upload completed work again')

    def test_disabled_preflight_is_persisted_and_skips_control_upload_wait(self):
        self.photos_count(3)
        self.service.settings(preflight_check=False)
        self.service.close()
        replacement = self.make_service()
        self.assertFalse(replacement.snapshot()['settings']['preflight_check'])
        with patch('mailru_photos.web_service.probe', side_effect=AssertionError('Probe must be skipped')):
            replacement.start()
            state = self.done(replacement)
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['uploaded'], 3)
        self.assertIsNone(state['probe'])
        self.assertEqual(self.cloud.stop.waits, [])
        self.assertTrue(all(item['unlimited_upload'] is True for item in self.cloud.commits))

    def test_mixed_photo_video_queue_uploads_and_preserves_both_after_restart(self):
        self.photos_count(1)
        (self.photos / 'clip.MOV').write_bytes(b'\x00\x00\x00\x18ftypqt  ' + b'offline-video' * 20)
        self.service.settings(preflight_check=False)
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['uploaded'], 2)
        self.assertTrue(any(item['path'].endswith('clip.MOV') for item in self.cloud.commits))
        self.service.close()
        replacement = self.make_service()
        replacement.start()
        self.assertEqual(self.done(replacement)['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)

    def test_large_media_default_is_visible_as_skipped_without_hashing_or_probe(self):
        path = self.large_video()
        self.assertNotIn('upload_large_files', self.service.snapshot()['settings'])
        with patch('mailru_photos.api.cloud_hash', side_effect=AssertionError('Large source must not be read')), \
                patch('mailru_photos.web_service.probe', side_effect=AssertionError('No eligible probe sample')):
            self.service.start()
            state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['total'], 1)
        self.assertEqual(state['stats']['skipped'], 1)
        self.assertEqual(state['stats']['uploaded'], 0)
        self.assertEqual(state['stats']['pending'], 0)
        job = self.service.jobs(status='skipped')['jobs'][0]
        self.assertTrue(job['remote_path'].endswith(path.name))
        self.assertEqual(job['skip_reason'], 'large_files_disabled')
        self.assertIsNone(job['storage_mode'])
        self.assertIsNone(state['probe'])
        self.assertEqual(self.cloud.upload_calls, [])
        self.assertEqual(self.cloud.commits, [])
        self.service.close()
        replacement = self.make_service()
        self.assertNotIn('upload_large_files', replacement.snapshot()['settings'])
        self.assertEqual(replacement.jobs(status='skipped')['jobs'][0]['id'], job['id'])
        self.assertFalse(replacement.resume_if_requested())






    def test_legacy_large_upload_setting_cannot_enable_ordinary_storage(self):
        self.large_video()
        self.photos_count(1)
        self.service.close()
        write_json(self.state / 'web_settings.json', {
            'workers': 2, 'auto_resume': True, 'preflight_check': False,
            'upload_large_files': True})
        replacement = self.make_service()
        self.assertNotIn('upload_large_files', replacement.snapshot()['settings'])
        replacement.start()
        state = self.done(replacement)
        self.assertEqual(state['stats']['uploaded'], 1, state)
        self.assertEqual(state['stats']['skipped'], 1, state)
        self.assertTrue(all(item['unlimited_upload'] is True for item in self.cloud.commits))
        self.assertEqual(len(self.cloud.commits), 1)

    def test_upgrade_rescans_old_photo_filter_once_and_retains_completed_photos(self):
        from mailru_photos.queue import PHOTO_EXTENSIONS
        self.photos_count(1)
        self.service.settings(preflight_check=False)
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 1)
        self.service.close()
        (self.photos / 'new-video.mp4').write_bytes(b'\x00\x00\x00\x18ftypisom' + b'offline-video' * 20)
        with Queue(self.state / 'queue.sqlite3', destination='/Archive') as queue:
            queue.set_metadata('extensions', sorted(PHOTO_EXTENSIONS))
            photo_id = queue.list_jobs()[0]['id']
        replacement = self.make_service()
        self.assertFalse(replacement.snapshot()['last_scan']['complete'])
        replacement.start()
        state = self.done(replacement)
        self.assertEqual(state['stats']['uploaded'], 2, state)
        self.assertEqual(len(self.cloud.commits), 2)
        self.assertEqual(replacement.jobs()['jobs'][0]['id'], photo_id)
        replacement.close()
        self.assertTrue(self.make_service().snapshot()['last_scan']['complete'])

    def test_mail_logout_forgets_tokens_but_keeps_queue_and_account_binding(self):
        self.photos_count(2)
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 2)
        self.service.logout()
        state = self.done()
        self.assertFalse(state['authenticated'])
        self.assertEqual(state['account'], EMAIL)
        self.assertEqual(state['stats']['uploaded'], 2)
        self.assertFalse((self.state / 'auth.dat').exists())
        self.assertEqual(state['probe']['result'], 'invalidated')
        with self.assertRaises(ValueError):
            self.service.start()
        class Auth:
            def login(self, email, password):
                return Tokens('relogin-offline-access', 'relogin-offline-refresh', email)
        self.service._auth_client_factory = Auth
        self.service.login(EMAIL, 'offline-test-only-password')
        self.assertTrue(self.done()['authenticated'])
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)

    def test_logout_drains_workers_before_removing_refreshed_tokens(self):
        self.photos_count(2)
        self.service.settings(workers=1, preflight_check=False)
        entered, release = threading.Event(), threading.Event()
        def pending_refresh(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError('Test did not release the worker')
            TokenStore(self.state / 'auth.dat').save(Tokens('late-offline-access', 'late-offline-refresh', EMAIL))
            self.cloud._check_stopped()
        self.cloud._upload_blob = pending_refresh
        self.service.start()
        self.assertTrue(entered.wait(2))
        try:
            self.service.logout()
            self.service.logout()  # Repeated click does not race a second task.
            self.service.pause()   # A stale pause cannot hide a pending logout.
            state = self.service.snapshot()
            self.assertEqual(state['activity']['phase'], 'logout')
            self.assertTrue((self.state / 'auth.dat').exists())
            intent = json.loads((self.state / 'web_activity.json').read_text())
            self.assertTrue(intent['logout_requested'])
            self.assertFalse(intent['resume_upload'])
            with self.assertRaises(QueueBusyError):
                self.service.start()
        finally:
            release.set()
        state = self.done()
        self.assertFalse(state['authenticated'])
        self.assertFalse((self.state / 'auth.dat').exists())
        self.assertEqual(state['stats']['pending'], 2)
        self.assertEqual(state['stats']['running'], 0)
        self.service.close()
        replacement = self.make_service()
        self.assertFalse(replacement.resume_if_requested())
        self.assertFalse(replacement.snapshot()['authenticated'])

    def test_interrupted_logout_completes_before_auto_resume_after_restart(self):
        self.photos_count(1)
        self.service.scan()
        self.done()
        self.service.close()
        write_json(self.state / 'web_activity.json', {
            'version': 1, 'phase': 'logout', 'resume_upload': True, 'logout_requested': True})
        replacement = self.make_service()
        self.assertFalse(replacement.snapshot()['authenticated'])
        self.assertFalse(replacement.resume_if_requested())
        self.assertFalse((self.state / 'auth.dat').exists())
        self.assertEqual(replacement.snapshot()['stats']['pending'], 1)
        self.assertFalse(json.loads((self.state / 'web_activity.json').read_text())['logout_requested'])

    def test_failed_logout_is_visible_and_can_be_retried(self):
        with patch.object(TokenStore, 'clear', side_effect=AuthError('Offline clear failure')):
            self.service.logout()
            state = self.done()
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertTrue(state['authenticated'])
        self.assertTrue((self.state / 'auth.dat').exists())
        self.service.logout()
        self.assertFalse(self.done()['authenticated'])

    def test_disabling_preflight_can_continue_after_interrupted_settling(self):
        self.photos_count(3)
        self.events.cancel_next = 1
        self.service.start()
        self.assertEqual(self.done()['probe']['result'], 'interrupted')
        self.service.settings(preflight_check=False)
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['uploaded'], 3)
        self.assertEqual(len(self.cloud.commits), 3)
        self.assertEqual(self.cloud.stop.waits, [])

    def test_reenabled_preflight_runs_for_new_files_after_skipping_it(self):
        self.photos_count(1)
        self.service.settings(preflight_check=False)
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 1)
        (self.photos / '001.jpg').write_bytes(b'new-offline-photo-' + b'x' * 64)
        self.service.scan()
        self.done()
        self.service.settings(preflight_check=True)
        self.service.start()
        state = self.done()
        self.assertEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(state['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)
        self.assertEqual(self.cloud.stop.waits, [30, 30])

    def test_old_settings_keep_preflight_enabled_and_invalid_update_is_atomic(self):
        before = self.service.snapshot()['settings']
        self.assertTrue(before['preflight_check'])
        with self.assertRaises(ValueError):
            self.service.settings(workers=4, preflight_check='false')
        self.assertEqual(self.service.snapshot()['settings'], before)
        self.service.close()
        write_json(self.state / 'web_settings.json', {'workers': 3, 'auto_resume': False})
        replacement = self.make_service()
        self.assertEqual(replacement.snapshot()['settings'],
                         {'workers': 3, 'auto_resume': False, 'preflight_check': True, 'remote_reconcile': False})

    def test_container_lifecycle_logs_do_not_echo_private_messages(self):
        private_message = "test-only-sensitive-value"
        with self.assertLogs("mailru_photos.service", level="INFO") as captured:
            self.service._phase("error", private_message, error=private_message)
        logs = "\n".join(captured.output)
        self.assertIn("phase=error", logs)
        self.assertNotIn(private_message, logs)

    def test_quota_change_blocks_mass_upload_and_keeps_confirmed_sample(self):
        self.photos_count(3)
        self.cloud.quota_values = [100, 101, 101]
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertEqual(state['probe']['result'], 'quota_changed')
        self.assertEqual(state['stats']['uploaded'], 1)
        self.assertEqual(state['stats']['pending'], 2)
        self.assertEqual(len(self.cloud.commits), 1)

    def test_pause_during_probe_settling_reuses_same_sample_even_if_queue_empty(self):
        self.photos_count(1)
        self.events.cancel_next = 1
        self.service.start()
        interrupted = self.done()
        self.assertEqual(interrupted['probe']['result'], 'interrupted')
        self.assertEqual(interrupted['stats']['uploaded'], 1)
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(len(self.cloud.commits), 1)
        self.assertEqual(len(self.cloud.upload_calls), 1)

    def test_removed_probe_sample_retains_evidence_for_settling_after_restart(self):
        self.photos_count(1)
        self.events.cancel_next = 1
        self.service.start()
        state = self.done()
        self.assertEqual(state['probe']['result'], 'interrupted')
        task_id = self.service.jobs()['jobs'][0]['id']
        self.service.control_task(task_id, 'remove')
        self.service.close()
        replacement = self.make_service()
        replacement.start()
        state = self.done(replacement)
        self.assertEqual(state['probe']['result'], 'provisional_pass', state)
        self.assertEqual(state['stats']['total'], 0)
        self.assertEqual(len(self.cloud.commits), 1)

    def test_pause_after_positive_add_before_stat_recovers_same_probe(self):
        self.photos_count(2)
        self.service.scan()
        self.done()
        first_job = self.service.jobs(limit=1)['jobs'][0]
        path = first_job['remote_path']
        self.cloud.after_confirmation_stat_failures[path] = Cancelled('Pause after positive add')
        self.service.start()
        paused = self.done()
        self.assertEqual(paused['activity']['phase'], 'paused')
        self.assertEqual(paused['stats']['pending'], 2)
        self.assertEqual(len(self.cloud.commits), 1)
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(state['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)
        self.assertEqual(self.cloud.upload_calls.count(first_job['local_path']), 1)

    def test_crash_between_mark_uploaded_and_probe_report_uses_durable_history(self):
        self.photos_count(1)
        with Queue(self.state / 'queue.sqlite3', destination='/Archive', account_id=EMAIL) as queue:
            queue.set_metadata('last_scan', asdict(queue.scan()))
            started = time.time()
            job = queue.claim()
            process_job(queue, self.cloud, job)
            queue.set_metadata('probe', {
                'result': 'incomplete', 'protocol': PROTOCOL_VERSION, 'account': EMAIL,
                'destination': '/Archive', 'started_at': started, 'before': {'used_bytes': 100},
                'samples': [], 'active_job_id': job.id, 'measurements': [],
            })
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(len(self.cloud.commits), 1)

    def test_resumed_probe_claim_cancellation_remains_an_interruption(self):
        self.photos_count(2)
        self.service.scan(); self.done()
        with self.service._queue() as queue:
            queue.bind_account(EMAIL)
            first = queue.list_jobs()[0]
            queue.set_metadata('probe', {
                'result': 'interrupted', 'protocol': PROTOCOL_VERSION, 'account': EMAIL,
                'destination': '/Archive', 'started_at': time.time(), 'before': {'used_bytes': 100},
                'samples': [], 'active_job_id': first['id'], 'measurements': [],
            })
        original = Queue.claim
        def cancel_claim(queue, *, stop=None, **kwargs):
            self.assertIs(stop, self.service._stop)
            self.service.pause()
            return original(queue, stop=stop, **kwargs)
        with patch.object(Queue, 'claim', new=cancel_claim):
            self.service.start(); state = self.done()
        self.assertEqual(state['activity']['phase'], 'paused')
        self.assertEqual(state['probe']['result'], 'interrupted')
        self.assertEqual(state['stats']['pending'], 2)
        self.assertTrue(state['authenticated'])
        self.assertFalse(state['auth_required'])
        self.assertEqual(self.cloud.commits, [])

    def test_old_confirmation_cannot_be_used_as_new_probe_sample(self):
        self.photos_count(1)
        with Queue(self.state / 'queue.sqlite3', destination='/Archive', account_id=EMAIL) as queue:
            queue.set_metadata('last_scan', asdict(queue.scan()))
            job = queue.claim()
            digest, identity = cloud_hash(job.local_path)
            queue.save_commit_intent(job, digest)
            queue.confirm_unlimited_commit(job, digest)
            self.cloud.remote[job.remote_path] = {'hash': digest, 'size': identity.size}
            started = time.time()
            process_job(queue, self.cloud, job)
            queue.set_metadata('probe', {
                'result': 'incomplete', 'protocol': PROTOCOL_VERSION, 'account': EMAIL,
                'destination': '/Archive', 'started_at': started, 'before': {'used_bytes': 100},
                'samples': [], 'active_job_id': job.id, 'measurements': [],
            })
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'completed')
        self.assertEqual(state['probe']['result'], 'insufficient_new_samples')
        self.assertNotEqual(state['probe']['result'], 'provisional_pass')
        self.assertEqual(len(self.cloud.commits), 0)

    def test_manual_pause_preserves_queue_but_does_not_auto_resume_on_restart(self):
        self.photos_count(1)
        self.cloud.block_upload = True
        self.service.start()
        self.assertTrue(self.cloud.upload_started.wait(2))
        with self.assertRaises(QueueBusyError):
            self.service.scan()
        self.service.pause()
        self.assertEqual(self.done()['activity']['phase'], 'paused')
        self.service.close()
        replacement = self.make_service()
        self.assertFalse(replacement.resume_if_requested())
        self.assertEqual(replacement.snapshot()['stats']['pending'], 1)
        self.assertEqual(len(self.cloud.commits), 0)
        self.cloud.block_upload = False
        replacement.start()
        self.assertEqual(self.done(replacement)['stats']['uploaded'], 1)

    def test_shutdown_retains_active_intention_and_restart_resumes_automatically(self):
        self.photos_count(2)
        self.cloud.block_upload = True
        self.service.start()
        self.assertTrue(self.cloud.upload_started.wait(2))
        self.service.close(timeout=2)
        state = json.loads((self.state / 'web_activity.json').read_text())
        self.assertTrue(state['resume_upload'])
        self.cloud.block_upload = False
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        final = self.done(replacement)
        self.assertEqual(final['activity']['phase'], 'completed', final)
        self.assertEqual(final['stats']['uploaded'], 2)
        self.assertEqual(len(self.cloud.commits), 2)

    def test_shutdown_at_success_boundary_does_not_forget_remaining_work(self):
        self.photos_count(1)
        self.service.scan()
        self.done()
        def shutdown_before_result_is_committed():
            self.service.close(timeout=0)
            return "completed", "Result raced with shutdown"
        self.service._pipeline = shutdown_before_result_is_committed
        self.service.start()
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'paused')
        saved = json.loads((self.state / 'web_activity.json').read_text())
        self.assertTrue(saved['resume_upload'])
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        self.assertEqual(self.done(replacement)['stats']['uploaded'], 1)

    def test_pipeline_holds_token_lock_between_preflight_probe_and_run(self):
        self.photos_count(2)
        phases = []
        original_phase = self.service._phase
        def phase(name, message, **kwargs):
            if name in {'probing', 'uploading'}:
                with Queue(self.state / 'queue.sqlite3', destination='/Archive') as competing_cli:
                    with self.assertRaises(QueueBusyError):
                        with competing_cli.exclusive_run():
                            self.fail('CLI could rotate a cached token between web phases')
                phases.append(name)
            original_phase(name, message, **kwargs)
        self.service._phase = phase
        self.service.start()
        self.assertEqual(self.done()['activity']['phase'], 'completed')
        self.assertEqual(phases, ['probing', 'uploading'])

    def test_shutdown_timeout_in_probe_is_interruption_not_permanent_failure(self):
        self.photos_count(1)
        self.cloud.block_upload = True
        self.cloud.stop_error = TransportError('Offline timeout after stop', category='timeout')
        self.service.start()
        self.assertTrue(self.cloud.upload_started.wait(2))
        self.service.close(timeout=2)
        state = self.service.snapshot()
        self.assertEqual(state['activity']['phase'], 'paused')
        self.assertEqual(state['probe']['result'], 'interrupted')
        self.assertEqual(state['stats']['pending'], 1)
        self.assertEqual(state['stats']['permanent_failures'], 0)
        self.assertTrue(json.loads((self.state / 'web_activity.json').read_text())['resume_upload'])
        self.cloud.block_upload = False
        self.cloud.stop_error = None
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        self.assertEqual(self.done(replacement)['stats']['uploaded'], 1)
        self.assertEqual(len(self.cloud.commits), 1)

    def test_shutdown_timeout_during_probe_quota_keeps_original_sample(self):
        self.photos_count(1)
        reached_quota = threading.Event()
        original_quota = self.cloud.quota
        calls = 0
        def quota():
            nonlocal calls
            calls += 1
            if calls == 2:
                reached_quota.set()
                while not self.cloud.stop.is_set():
                    time.sleep(0.002)
                raise TransportError('Offline quota timeout after shutdown', category='timeout')
            return original_quota()
        self.cloud.quota = quota
        self.service.start()
        self.assertTrue(reached_quota.wait(2))
        self.service.close(timeout=2)
        state = self.service.snapshot()
        self.assertEqual(state['activity']['phase'], 'paused')
        self.assertEqual(state['probe']['result'], 'interrupted')
        self.assertEqual(state['stats']['uploaded'], 1)
        self.assertTrue(json.loads((self.state / 'web_activity.json').read_text())['resume_upload'])
        self.cloud.quota = original_quota
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        final = self.done(replacement)
        self.assertEqual(final['probe']['result'], 'provisional_pass')
        self.assertEqual(len(self.cloud.commits), 1)

    def test_shutdown_does_not_hide_protocol_failure_as_user_pause(self):
        self.photos_count(1)
        self.cloud.block_upload = True
        self.cloud.stop_error = ProtocolError('Offline server confirmation failure')
        self.service.start()
        self.assertTrue(self.cloud.upload_started.wait(2))
        self.service.close(timeout=2)
        state = self.service.snapshot()
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertEqual(state['stats']['permanent_failures'], 1)
        self.assertFalse(json.loads((self.state / 'web_activity.json').read_text())['resume_upload'])

    def test_disabled_auto_resume_keeps_queue_available_for_manual_continue(self):
        self.service.settings(workers=3, auto_resume=False)
        self.service.close()
        write_json(self.state / 'web_activity.json', {'version': 1, 'resume_upload': True, 'phase': 'uploading'})
        replacement = self.make_service()
        self.assertFalse(replacement.resume_if_requested())
        state = replacement.snapshot()
        self.assertEqual(state['settings'], {'workers': 3, 'auto_resume': False, 'preflight_check': True, 'remote_reconcile': False})
        self.assertEqual(state['activity']['phase'], 'paused')

    def test_dashboard_and_jobs_share_stats_until_expiry_without_sharing_mutable_data(self):
        self.photos_count(4)
        self.service.scan()
        self.done()
        self.service._invalidate_stats()
        original = Queue.stats
        with patch.object(Queue, 'stats', autospec=True, side_effect=original) as stats:
            with patch('mailru_photos.web_service.time.monotonic', return_value=100):
                state = self.service.snapshot()
                state['stats']['total'] = -1
                state['stats']['bytes_by_status']['pending'] = -1
                self.assertEqual(self.service.jobs(limit=2)['total'], 4)
                fresh = self.service.snapshot()['stats']
                self.assertEqual(fresh['total'], 4)
                self.assertGreater(fresh['bytes_by_status']['pending'], 0)
                self.assertEqual(stats.call_count, 1)
            with patch('mailru_photos.web_service.time.monotonic', return_value=106):
                self.service.snapshot()
                self.assertEqual(stats.call_count, 2)

    def test_stats_refresh_after_scan_and_retry_controls(self):
        self.assertEqual(self.service.snapshot()['stats']['total'], 0)
        self.photos_count(2)
        self.service.scan()
        self.assertEqual(self.done()['stats']['total'], 2)
        with self.service._queue() as queue:
            job = queue.claim()
            queue.mark_failed(job, 'offline fixture', retry_delay=None)
        self.service._invalidate_stats()
        self.assertEqual(self.service.snapshot()['stats']['failed'], 1)
        self.service.retry_failed(include_permanent=True)
        self.assertEqual(self.service.snapshot()['stats']['pending'], 2)
        self.assertEqual(self.service.snapshot()['stats']['failed'], 0)

    def test_paginated_tasks_never_expose_claims_or_confirmation_blob(self):
        self.photos_count(4)
        self.service.scan()
        self.done()
        first = self.service.jobs(limit=2)
        second = self.service.jobs(limit=2, offset=2)
        self.assertEqual(first['total'], 4)
        self.assertEqual(len(first['jobs']), 2)
        self.assertNotEqual(first['jobs'][0]['id'], second['jobs'][0]['id'])
        self.assertNotIn('claim_token', first['jobs'][0])
        self.assertNotIn('verified_metadata', first['jobs'][0])
        with self.assertRaises(ValueError):
            self.service.jobs(status="uploaded' OR 1=1")
        with self.assertRaises(ValueError):
            self.service.jobs(limit=10000)

    def test_folders_are_confined_to_allowed_roots_including_symlink_escape(self):
        child = self.photos / 'family'
        child.mkdir()
        outside = self.root / 'private'
        outside.mkdir()
        (self.photos / 'escape').symlink_to(outside, target_is_directory=True)
        names = [d['name'] for d in self.service.folders(str(self.photos))['directories']]
        self.assertEqual(names, ['family'])
        self.assertIsNone(self.service.folders(str(self.photos))['parent'])
        for path in [str(outside), str(self.photos / 'escape'), str(self.photos / '..')]:
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.service.add_folder(path, 'Private')
        self.assertEqual(self.service.snapshot()['stats']['roots'], 1)

    def test_new_panel_can_log_in_without_global_destination_setup(self):
        class Auth:
            def login(self, email, password):
                return Tokens('fresh-offline-access', 'fresh-offline-refresh', email)
        fresh = self.make_service(state_dir=self.root / 'fresh-login', auth_client_factory=Auth)
        state = fresh.snapshot()
        self.assertFalse(state['configured'])
        self.assertFalse(state['authenticated'])
        self.assertEqual(state['sources'], [])
        fresh.login(EMAIL, 'offline-application-password')
        state = self.done(fresh)
        self.assertTrue(state['configured'])
        self.assertTrue(state['authenticated'])
        self.assertEqual(state['destination'], '/')
        self.assertEqual(state['account'], EMAIL)
        self.assertEqual(state['sources'], [])
        self.assertEqual(self.cloud.commits, [])
        self.assertEqual(json.loads((fresh.state_dir / 'project.json').read_text()),
                         {'version': 1, 'destination': '/'})
        # New onboarding must not change a separately configured older queue.
        self.assertEqual(self.service.snapshot()['destination'], '/Archive')

    def test_source_add_initializes_panel_and_uses_exact_destination(self):
        fresh = self.make_service(state_dir=self.root / 'fresh-source')
        result = fresh.add_folder(str(self.photos), 'Local label', remote_prefix='/Camera/Family')
        state = fresh.snapshot()
        self.assertTrue(state['configured'])
        self.assertFalse(state['authenticated'])
        self.assertEqual(state['destination'], '/')
        self.assertEqual(state['sources'], [result['source']])
        self.assertEqual(state['sources'][0]['remote_prefix'], '/Camera/Family')
        self.photos_count(1)
        fresh.scan()
        self.assertEqual(self.done(fresh)['stats']['total'], 1)
        self.assertEqual(fresh.jobs()['jobs'][0]['remote_path'], '/Camera/Family/000.jpg')

    def test_source_destination_update_is_independent_and_invalidates_probe(self):
        first_dir, second_dir = self.photos / 'one', self.photos / 'two'
        first_dir.mkdir()
        second_dir.mkdir()
        (first_dir / 'one.jpg').write_bytes(b'offline-photo-one')
        (second_dir / 'two.jpg').write_bytes(b'offline-photo-two')
        fresh = self.make_service(state_dir=self.root / 'separate-sources')
        first = fresh.add_folder(str(first_dir), 'One', remote_prefix='/First')['source']
        second = fresh.add_folder(str(second_dir), 'Two', remote_prefix='/Second')['source']
        fresh.scan()
        self.assertEqual(self.done(fresh)['stats']['pending'], 2)
        with fresh._queue() as queue:
            queue.set_metadata('probe', {'result': 'provisional_pass', 'finished_at': time.time()})
        updated = fresh.update_source(first['id'], remote_prefix='/Changed/OnlyOne', alias='Renamed')['source']
        self.assertEqual(updated['remote_prefix'], '/Changed/OnlyOne')
        self.assertEqual(updated['alias'], 'Renamed')
        self.assertEqual(updated['path'], first['path'])
        state = fresh.snapshot()
        self.assertIn(second, state['sources'])
        self.assertFalse(state['last_scan']['complete'])
        self.assertEqual(state['probe']['result'], 'invalidated')
        self.assertEqual({row['remote_path'] for row in fresh.jobs()['jobs']},
                         {'/Changed/OnlyOne/one.jpg', '/Second/two.jpg'})
        self.assertEqual(state['stats']['total'], 2)
        fresh.close()
        restored = self.make_service(state_dir=fresh.state_dir)
        self.assertEqual(restored.snapshot()['sources'], state['sources'])

    def test_source_removal_clears_tasks_but_preserves_files_account_and_upload_evidence(self):
        self.photos_count(2)
        self.service.start()
        before = self.done()
        self.assertEqual(before['stats']['uploaded'], 2)
        source = before['sources'][0]
        result = self.service.remove_source(source['id'])
        self.assertEqual(result['removed_tasks'], 2)
        state = self.service.snapshot()
        self.assertEqual(state['sources'], [])
        self.assertEqual(state['stats']['total'], 0)
        self.assertEqual(self.service.jobs()['jobs'], [])
        self.assertTrue(state['authenticated'])
        self.assertEqual(state['account'], EMAIL)
        self.assertEqual(state['probe']['result'], 'invalidated')
        self.assertTrue((self.state / 'auth.dat').exists())
        self.assertEqual(len(list(self.photos.glob('*.jpg'))), 2)
        self.assertEqual(len(self.cloud.commits), 2)
        with sqlite3.connect(self.state / 'queue.sqlite3') as database:
            self.assertEqual(database.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0], 2)
            self.assertEqual(database.execute('SELECT COUNT(*) FROM upload_results WHERE job_id IS NULL').fetchone()[0], 2)

    def test_readding_same_source_keeps_scan_and_probe_valid(self):
        self.photos_count(1)
        self.service.start()
        before = self.done()
        self.service.add_folder(str(self.photos), 'Photos')
        after = self.service.snapshot()
        self.assertEqual(after['last_scan'], before['last_scan'])
        self.assertEqual(after['probe'], before['probe'])
        self.assertEqual(after['sources'], before['sources'])
        self.assertEqual(after['stats']['uploaded'], 1)

    def test_source_mutations_reject_active_controller_and_competing_queue_lock(self):
        source = self.service.snapshot()['sources'][0]
        mutations = [
            lambda: self.service.add_folder(str(self.photos), 'Photos'),
            lambda: self.service.update_source(source['id'], remote_prefix='/Changed'),
            lambda: self.service.remove_source(source['id']),
        ]
        self.service._operation = 'scanning'
        try:
            for mutate in mutations:
                with self.assertRaises(QueueBusyError):
                    mutate()
        finally:
            self.service._operation = None
        with self.service._queue() as queue, queue.exclusive_run():
            for mutate in mutations:
                with self.assertRaises(QueueBusyError):
                    mutate()
        self.assertEqual(self.service.snapshot()['sources'], [source])

    def test_invalid_source_edits_leave_tasks_and_configuration_unchanged(self):
        self.photos_count(1)
        self.service.scan()
        before = self.done()
        jobs = self.service.jobs()
        source_id = before['sources'][0]['id']
        for target in ('relative/path', '/Photo/../Other', '/Bad\\Path'):
            with self.subTest(target=target), self.assertRaises(QueueError):
                self.service.update_source(source_id, remote_prefix=target)
        for source_id in (0, -1, True, '1', 2**63):
            with self.subTest(source_id=source_id), self.assertRaises(ValueError):
                self.service.remove_source(source_id)
        with self.assertRaises(ValueError):
            self.service.update_source(999999, remote_prefix='/NoSource')
        after = self.service.snapshot()
        self.assertEqual(after['sources'], before['sources'])
        self.assertEqual(after['last_scan'], before['last_scan'])
        self.assertEqual(after['destination'], '/Archive')
        self.assertEqual(self.service.jobs(), jobs)

    def test_login_remembers_tokens_and_never_persists_password(self):
        secret = 'unique-offline-test-password-never-persist'
        class Auth:
            def login(self, email, password):
                if email != EMAIL or password != secret:
                    raise AssertionError('Wrong fixture credentials')
                return Tokens('new-offline-access', 'new-offline-refresh', EMAIL)
        self.service._auth_client_factory = Auth
        self.service.login(EMAIL, secret)
        state = self.done()
        self.assertTrue(state['authenticated'])
        self.assertEqual(state['account'], EMAIL)
        self.assertEqual(state['destination'], '/Archive')
        for path in self.state.iterdir():
            if path.is_file():
                self.assertNotIn(secret.encode(), path.read_bytes(), str(path))
        serialized = json.dumps(state)
        self.assertNotIn('new-offline-access', serialized)
        self.assertNotIn('new-offline-refresh', serialized)
        self.service.close()
        replacement = self.make_service()
        self.assertTrue(replacement.snapshot()['authenticated'])

    def test_login_rejected_by_quota_does_not_replace_saved_credentials_or_probe(self):
        class Auth:
            def login(self, email, password):
                return Tokens('rejected-new-access', 'rejected-new-refresh', email)
        self.service._auth_client_factory = Auth
        original_tokens = (self.state / 'auth.dat').read_bytes()
        original_probe = {'result': 'provisional_pass', 'offline_fixture': True}
        with self.service._queue() as queue:
            queue.set_metadata('probe', original_probe)
        self.service.refresh_quota(); self.done()
        for error in (AuthError('Rejected refresh fixture'), RemoteError(401), RemoteError(403)):
            with self.subTest(error=type(error).__name__):
                self.cloud.quota_values = [error]
                self.service.login(EMAIL, 'disposable-fixture-password'); state = self.done()
                self.assertFalse(state['authenticated'])
                self.assertEqual(state['activity']['phase'], 'error')
                self.assertTrue(state['quota']['stale'])
                self.assertEqual((self.state / 'auth.dat').read_bytes(), original_tokens)
                with self.service._queue() as queue:
                    self.assertEqual(queue.get_metadata('probe'), original_probe)
                    self.assertEqual(queue.get_metadata('last_auth')['result'], 'failed')
                self.assertNotIn('rejected-new-', json.dumps(state))
                self.assertNotIn('disposable-fixture-password', json.dumps(state))

    def test_login_can_survive_non_auth_quota_outage_without_fabricating_current_quota(self):
        class Auth:
            def login(self, email, password):
                return Tokens('accepted-new-access', 'accepted-new-refresh', email)
        self.service._auth_client_factory = Auth
        self.service.refresh_quota(); self.done()
        for error in (TransportError('Proxy HTTP 403 diagnostic'), RemoteError(503), ProtocolError('Missing quota field')):
            with self.subTest(error=type(error).__name__):
                self.cloud.quota_values = [error]
                self.service.login(EMAIL, 'disposable-fixture-password'); state = self.done()
                self.assertTrue(state['authenticated'])
                self.assertEqual(state['activity']['phase'], 'idle')
                self.assertTrue(state['quota']['stale'])
                self.assertEqual(TokenStore(self.state / 'auth.dat').load().access_token, 'accepted-new-access')
                with self.service._queue() as queue:
                    self.assertEqual(queue.get_metadata('last_auth')['result'], 'success')

    def test_multiworker_pause_resume_with_concurrent_dashboard_polling(self):
        self.photos_count(126)
        original_upload = self.cloud._upload_blob
        def slow_upload(path, digest, identity, *, progress=None, is_retried=False):
            for _ in range(4):
                self.cloud._check_stopped()
                time.sleep(0.003)
            return original_upload(path, digest, identity, progress=progress, is_retried=is_retried)
        self.cloud._upload_blob = slow_upload
        polling_stop = threading.Event()
        polling_errors = []
        polls = []
        def poll_dashboard():
            while not polling_stop.is_set():
                try:
                    self.service.snapshot()
                    self.service.jobs(limit=50)
                    polls.append(1)
                except Exception as error:
                    polling_errors.append(type(error).__name__ + ': ' + str(error))
                    polling_stop.set()
                time.sleep(0.001)
        threads = [threading.Thread(target=poll_dashboard) for _ in range(4)]
        for thread in threads:
            thread.start()
        try:
            self.service.start()
            deadline = time.monotonic() + 10
            # Display totals may be cached; synchronize with simulated commits
            # to pause during real work rather than waiting for a UI refresh.
            while len(self.cloud.commits) < 3:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.002)
            self.service.pause()
            self.service._thread.join(timeout=10)
            self.assertIsNone(self.service._operation)
            paused = self.service.snapshot()
            self.assertEqual(paused['stats']['running'], 0)
            self.assertGreater(paused['stats']['pending'], 0)
            self.service.settings(workers=3)
            self.service.start()
            self.service._thread.join(timeout=15)
            self.assertIsNone(self.service._operation)
        finally:
            polling_stop.set()
            for thread in threads:
                thread.join(timeout=5)
        state = self.service.snapshot()
        self.assertFalse(polling_errors, polling_errors)
        self.assertGreater(len(polls), 5)
        self.assertEqual(state['activity']['phase'], 'completed', state)
        self.assertEqual(state['stats']['uploaded'], 126)
        self.assertEqual(state['stats']['running'], 0)
        self.assertEqual(state['stats']['failed'], 0)
        self.assertEqual(len(self.cloud.commits), 126)
        self.assertEqual(len({item['path'] for item in self.cloud.commits}), 126)
        self.service.close()
        with sqlite3.connect(self.state / 'queue.sqlite3') as database:
            self.assertEqual(database.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
            self.assertEqual(database.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0], 126)
            self.assertEqual(database.execute('SELECT COUNT(*) FROM commit_intents').fetchone()[0], 0)

    def test_second_controller_cannot_open_same_state_while_first_is_running(self):
        with self.assertRaises(QueueBusyError):
            WebService(self.state, [self.photos])


class ScanCancellationTests(unittest.TestCase):
    def test_nested_run_lock_allows_only_owning_thread_and_connection(self):
        from concurrent.futures import ThreadPoolExecutor
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'queue.db'
            with Queue(path, destination='/Archive') as queue:
                with queue.exclusive_run():
                    with queue.exclusive_run():
                        queue.recover_running()
                    def another_thread():
                        with self.assertRaises(QueueBusyError):
                            with queue.exclusive_run():
                                pass
                        with self.assertRaises(QueueBusyError):
                            queue.recover_running()
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pool.submit(another_thread).result(timeout=2)
                    with Queue(path, destination='/Archive') as other:
                        with self.assertRaises(QueueBusyError):
                            with other.exclusive_run():
                                pass
                with Queue(path, destination='/Archive') as other, other.exclusive_run():
                    pass

    def test_cancelled_scan_never_marks_unseen_files_missing(self):
        class CountStop:
            calls = 0
            def is_set(self):
                self.calls += 1
                return self.calls > 8
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'photos'
            source.mkdir()
            for i in range(20):
                (source / f'{i:02}.jpg').write_bytes(b'x' * 50)
            with Queue(root / 'queue.db', destination='/Archive') as queue:
                queue.add_root(source, 'Photos')
                queue.scan()
                (source / '19.jpg').unlink()
                result = queue.scan(batch_size=2, stop=CountStop())
                self.assertTrue(result.cancelled)
                self.assertFalse(result.complete)
                self.assertEqual(result.missing, 0)
                self.assertEqual(queue.stats()['pending'], 20)
                self.assertFalse(queue.stats()['scan_active'])
                completed = queue.scan()
                self.assertTrue(completed.complete)
                self.assertEqual(completed.missing, 1)


if __name__ == '__main__':
    unittest.main()

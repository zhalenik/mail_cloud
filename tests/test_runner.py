"""Offline end-to-end queue/runner tests; every server operation is simulated.

FakeCloud uses the actual CloudAPI.upload_photo state machine, replacing its
HTTP, account, quota, byte transfer and metadata boundary.  No credentials,
sockets or external service are used.
"""
from __future__ import annotations

from dataclasses import asdict
import contextlib
import json
from pathlib import Path
import posixpath
import sqlite3
import tempfile
import threading
import time
import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from mailru_photos.api import (
    AccountInfo, CloudAPI, FileIdentity, PROTOCOL_VERSION, ProtocolError,
    FileVerificationError, RemoteConflict, RemoteError, SourceChanged, UnsupportedPhoto, cloud_hash,
)
from mailru_photos.metadata import read_varint
from mailru_photos.auth import AuthError
from mailru_photos.queue import Queue, QueueError
from mailru_photos.media import UNLIMITED_MAX_BYTES
from mailru_photos.runner import QuotaTelemetry, run
from mailru_photos.transport import Cancelled, Response, SourceReadError, TaskCancelled, TransportError, check_cancelled


EMAIL = "owner@example.test"


class InstantEvent(threading.Event):
    """Avoid real retry waits while retaining cancellation semantics."""
    def __init__(self, cancel_on_wait: int | None = None):
        super().__init__()
        self.waits = []
        self.cancel_on_wait = cancel_on_wait

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if len(self.waits) == self.cancel_on_wait:
            self.set()
        return self.is_set()


class FakeCloud(CloudAPI):
    def __init__(self, *, stop=None, quota_values=None, barrier=None):
        # Deliberately do not initialize any token manager or HTTP factory.
        self.stop = stop if stop is not None else threading.Event()
        self._fake_lock = threading.RLock()
        self.remote = {}
        self.commits = []
        self.regular_commits = []
        self.trashed = []
        self.trash_failures = {}
        self.unlimited_flags = {}
        self.upload_calls = []
        self.upload_types = []
        self.upload_limit = 2_000_000_000
        self.total_bytes = 100_000_000
        self.failures = {}
        self.after_commit_failures = {}
        self.after_confirmation_stat_failures = {}
        self.quota_values = list(quota_values or [100])
        self.quota_rule = None
        self.quota_calls = 0
        self.account_calls = 0
        self.closed_threads = 0
        self.barrier = barrier
        self._active_uploads = 0
        self.max_active_uploads = 0
        self.folder_calls = []

    def _check_stopped(self):
        check_cancelled(self.request_stop)

    def require_account(self, expected_email=None):
        self._check_stopped()
        with self._fake_lock:
            self.account_calls += 1
        if expected_email and expected_email != EMAIL:
            raise ProtocolError("Unexpected account in offline fixture")
        return AccountInfo(EMAIL, True, self.upload_limit)

    def account_info(self):
        return self.require_account()

    def _metadata_endpoint(self):
        return 'https://offline.cloud.mail.ru/meta'

    def quota(self):
        self._check_stopped()
        with self._fake_lock:
            self.quota_calls += 1
            if self.quota_rule is not None:
                value = self.quota_rule(self)
            else:
                value = self.quota_values[0]
                if len(self.quota_values) > 1:
                    self.quota_values.pop(0)
        if isinstance(value, BaseException):
            if isinstance(value, Cancelled):
                self.stop.set()
            raise value
        return {"used_bytes": value, "total_bytes": self.total_bytes,
                "mail_bytes": 0, "combined_used_bytes": value}

    def ensure_parent(self, path):
        self._check_stopped()

    def stat(self, path):
        self._check_stopped()
        with self._fake_lock:
            value = self.remote.get(path)
            if value is not None:
                failure = self.after_confirmation_stat_failures.pop(path, None)
                if failure is not None:
                    raise failure
            return dict(value) if value else None

    def _upload_blob(self, path, digest, identity, *, progress=None, is_retried=False, upload_type="AUTO"):
        if upload_type not in {"AUTO", "MANUAL"}:
            raise AssertionError("Unknown upload type")
        self._check_stopped()
        with self._fake_lock:
            self.upload_calls.append(str(path))
            self.upload_types.append(upload_type)
            failures = self.failures.get(Path(path).name, [])
            failure = failures.pop(0) if failures else None
            self._active_uploads += 1
            self.max_active_uploads = max(self.max_active_uploads, self._active_uploads)
        try:
            if self.barrier is not None:
                self.barrier.wait(timeout=5)
            if failure is not None:
                if isinstance(failure, Cancelled):
                    self.stop.set()
                raise failure
            if progress is not None:
                progress(identity.size)
        finally:
            with self._fake_lock:
                self._active_uploads -= 1

    def _request(self, method, url, *, body=b"", **kwargs):
        self._check_stopped()
        parsed = urlsplit(url)
        if method == 'POST' and parsed.path == '/api/m1/folder':
            path = parse_qs(body.decode('ascii'))['home'][0]
            query = parse_qs(parsed.query)
            offset, limit = int(query.get('offset', ['0'])[0]), int(query.get('limit', ['500'])[0])
            prefix = path.rstrip('/') + '/'
            entries = {}
            with self._fake_lock:
                self.folder_calls.append((path, offset, limit))
                for remote_path, value in self.remote.items():
                    if not remote_path.startswith(prefix):
                        continue
                    tail = remote_path[len(prefix):]
                    if '/' in tail:
                        child = prefix + tail.split('/', 1)[0]
                        entries[child] = {'kind': 'folder', 'home': child}
                    else:
                        entries[remote_path] = {'kind': 'file', 'home': remote_path, **value}
            items = [entries[key] for key in sorted(entries)]
            content = {'kind': 'folder', 'home': path,
                       'count': {'files': sum(x['kind'] == 'file' for x in items),
                                 'folders': sum(x['kind'] == 'folder' for x in items)},
                       'list': items[offset:offset + limit]}
            return Response(200, {}, json.dumps({'status': 200, 'body': content}).encode())
        if method == 'GET' and parsed.path == '/api/m1/file':
            path = parse_qs(parsed.query)['home'][0]
            with self._fake_lock:
                value = self.remote.get(path)
                if value is None:
                    return Response(404, {}, b'{}')
                item = {'kind': 'file', 'home': path, **value}
            return Response(200, {}, json.dumps({'status': 200, 'body': item}).encode())
        if method == 'POST' and parsed.path == '/api/m1/file/remove':
            path = parse_qs(body.decode('ascii'))['home'][0]
            with self._fake_lock:
                failure = self.trash_failures.pop(path, None)
                if failure is not None:
                    raise failure
                self.remote.pop(path, None)
                self.trashed.append(path)
            return Response(200, {}, b'{"status":200}')
        if method == 'POST' and parsed.path == '/meta':
            if body[:2] != b'\x67\x00':
                raise AssertionError('Only create-only metadata AddBlob is supported')
            length, offset = read_varint(body, 2)
            path = body[offset:offset + length - 1].decode('utf-8')
            if body[offset + length - 1] != 0:
                raise AssertionError('Path terminator missing')
            size, offset = read_varint(body, offset + length)
            mtime, offset = read_varint(body, offset)
            if body[offset:offset + 1] != b'\x00' or body[-1:] != b'\x03' or len(body) - offset != 22:
                raise AssertionError('Metadata must use raw hash and create-only flags3')
            digest = body[offset + 1:offset + 21].hex().upper()
            payload = {'path': path, 'size': size, 'hash': digest, 'mtime': mtime, 'upload_type': 'MANUAL'}
            with self._fake_lock:
                if path in self.remote:
                    return Response(200, {}, b'\x04')
                self.remote[path] = {'hash': digest, 'size': size}
                self.regular_commits.append(payload)
                failure = self.after_commit_failures.pop(path, None)
            if failure is not None:
                raise failure
            return Response(200, {}, b'\x00\x00')
        if method != "POST" or not url.endswith("/api/v1/private/add"):
            raise AssertionError(f"Unexpected simulated request: {method} {url}")
        payload = json.loads(body)
        if payload.get("unlimited_upload") is not True or payload.get("overwrite") is not False:
            raise AssertionError("The runner must keep unlimited and no-overwrite commit options")
        with self._fake_lock:
            self.remote[payload["path"]] = {"hash": payload["hash"], "size": payload["size"]}
            self.commits.append(payload)
            failure = self.after_commit_failures.pop(payload["path"], None)
        if failure is not None:
            raise failure
        response = {"path": payload["path"], "name": posixpath.basename(payload["path"]),
                    "nodeid": "offline-node-" + payload["hash"][:8]}
        flag = self.unlimited_flags.get(payload['path'], True)
        if flag is not None:
            response['is_unlim_upload'] = flag
        return Response(200, {}, json.dumps(response).encode())

    def close_thread(self):
        with self._fake_lock:
            self.closed_threads += 1


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.photos = self.root / "photos"
        self.photos.mkdir()
        self.queue = Queue(self.root / "queue.db", destination="/Archive", account_id=EMAIL)
        self.queue.add_root(self.photos, "camera")

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def make_photos(self, count):
        for index in range(count):
            (self.photos / f"{index:03}.jpg").write_bytes(b"offline-photo-fixture:" + str(index).encode() + b"x" * 128)
        scanned = self.queue.scan()
        self.queue.set_metadata("last_scan", asdict(scanned))
        self.assertTrue(scanned.complete)

    def make_large_video(self, name='large.mp4'):
        path = self.photos / name
        with path.open('wb') as stream:
            stream.truncate(UNLIMITED_MAX_BYTES + 1024)
        self.queue.set_metadata('last_scan', asdict(self.queue.scan()))
        return path

    def make_video(self, name='video.m4v'):
        path = self.photos / name
        path.write_bytes(b'offline-video-fixture:' + name.encode('utf-8') + b'x' * 128)
        self.queue.set_metadata('last_scan', asdict(self.queue.scan()))
        return path

    def test_completed_scan_is_required_but_no_probe_or_settling_exists(self):
        api = FakeCloud(stop=InstantEvent())
        with self.assertRaises(QueueError):
            run(self.queue, api, workers=1)
        self.assertEqual(api.account_calls, 0)
        self.make_photos(2)
        result = run(self.queue, api, workers=1)
        self.assertEqual(result['outcome'], 'complete')
        self.assertEqual(result['stats']['uploaded'], 2)
        self.assertIsNone(self.queue.get_metadata('probe'))
        self.assertEqual(api.stop.waits, [])
        self.assertEqual(len(api.commits), 2)

    def test_mixed_photos_mov_and_m4v_use_separate_explicit_routes(self):
        self.make_photos(1)
        self.make_video('movie.mov')
        self.make_video('movie.m4v')
        api = FakeCloud()
        result = run(self.queue, api, workers=2, regular_policy='upload')
        self.assertEqual(result['outcome'], 'complete')
        self.assertEqual(result['stats']['uploaded'], 3)
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 2)
        self.assertEqual(result['stats']['uploaded_regular_count'], 1)
        self.assertEqual(result['stats']['planned_regular_bytes'], 0)
        self.assertEqual(len(api.commits), 2)
        self.assertEqual(len(api.regular_commits), 1)
        self.assertTrue(api.regular_commits[0]['path'].endswith('.m4v'))
        self.assertEqual(sorted(api.upload_types), ['AUTO', 'AUTO', 'MANUAL'])
        regular = self.queue.list_jobs(status='uploaded_regular')
        self.assertEqual(len(regular), 1)
        self.assertEqual(regular[0]['status'], 'uploaded')
        self.assertEqual(self.queue.jobs_summary('uploaded_regular')['total_bytes'], regular[0]['size'])

    def test_skipped_regular_files_can_resume_when_setting_changes_without_rescan(self):
        self.make_photos(1)
        self.make_video()
        api = FakeCloud()
        first = run(self.queue, api, workers=1, regular_policy='skip')
        self.assertEqual(first['outcome'], 'finished_with_skips')
        self.assertEqual(first['stats']['uploaded'], 1)
        skipped = self.queue.list_jobs(status='skipped')[0]
        self.assertEqual(skipped['skip_reason'], 'regular_format_disabled')
        self.assertEqual(api.regular_commits, [])
        second = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertEqual(second['outcome'], 'complete')
        self.assertEqual(second['stats']['uploaded'], 2)
        self.assertEqual(len(api.regular_commits), 1)
        self.assertEqual(len(api.commits), 1)

    def test_negative_flag_keep_counts_billed_object_and_continues_queue(self):
        self.make_photos(3)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.unlimited_flags[first['remote_path']] = False
        result = run(self.queue, api, workers=1, unexpected_unlimited_action='keep')
        self.assertFalse(result['stopped'])
        self.assertEqual(result['outcome'], 'finished_with_errors')
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['uploaded'], 2)
        self.assertEqual(result['stats']['uploaded_regular_count'], 1)
        self.assertEqual(result['stats']['uploaded_regular_bytes'], first['size'])
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 2)
        self.assertEqual(api.trashed, [])
        self.assertIn(first['remote_path'], api.remote)
        billed = self.queue.list_jobs(status='uploaded_regular')
        self.assertEqual([r['id'] for r in billed], [first['id']])
        self.assertEqual(billed[0]['status'], 'failed')
        self.assertFalse(billed[0]['cloud_verified'])
        self.assertEqual(self.queue.jobs_summary('failed')['total'], 1)

    def test_negative_flag_trash_preserves_error_and_continues_other_tasks(self):
        self.make_photos(3)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.unlimited_flags[first['remote_path']] = False
        result = run(self.queue, api, workers=1, unexpected_unlimited_action='trash')
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 2)
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['uploaded_regular_count'], 0)
        self.assertEqual(api.trashed, [first['remote_path']])
        self.assertNotIn(first['remote_path'], api.remote)
        row = self.queue.get_job(first['id'])
        self.assertFalse(row['cloud_present'])
        self.assertIsNotNone(row['cloud_deleted_at'])
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(api.regular_commits, [])

    def test_failed_cleanup_keeps_negative_observation_and_does_not_stop_queue(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.unlimited_flags[first['remote_path']] = False
        api.trash_failures[first['remote_path']] = TransportError('Offline trash outage')
        result = run(self.queue, api, workers=1, unexpected_unlimited_action='trash')
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['uploaded_regular_count'], 1)
        row = self.queue.get_job(first['id'])
        self.assertTrue(row['cloud_present'])
        self.assertEqual(row['observed_mode'], 'regular')
        self.assertIn('не завершён', row['last_error'])
        self.assertIn(first['remote_path'], api.remote)

    def test_missing_flag_is_unknown_and_never_triggers_false_flag_cleanup(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.unlimited_flags[first['remote_path']] = None
        result = run(self.queue, api, workers=1, unexpected_unlimited_action='trash')
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['uploaded_unknown_count'], 1)
        self.assertEqual(result['stats']['uploaded_regular_count'], 0)
        self.assertEqual(api.trashed, [])

    def test_known_false_retry_after_restart_can_trash_without_second_upload(self):
        self.make_photos(1)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.unlimited_flags[first['remote_path']] = False
        run(self.queue, api, workers=1, unexpected_unlimited_action='keep')
        path = self.queue.path
        self.queue.close()
        self.queue = Queue(path, destination='/Archive', account_id=EMAIL)
        self.queue.request_control(first['id'], 'resume')
        result = run(self.queue, api, workers=1, unexpected_unlimited_action='trash', job_id=first['id'])
        self.assertFalse(result['stopped'])
        self.assertEqual(len(api.commits), 1)
        self.assertEqual(api.trashed, [first['remote_path']])
        self.assertFalse(self.queue.get_job(first['id'])['cloud_present'])
        self.assertEqual(self.queue.get_job(first['id'])['status'], 'failed')

    def test_start_one_pending_task_does_not_drain_other_tasks(self):
        self.make_photos(4)
        selected = self.queue.list_jobs()[2]
        api = FakeCloud()
        result = run(self.queue, api, workers=4, job_id=selected['id'])
        self.assertEqual(result['outcome'], 'single_finished')
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['pending'], 3)
        self.assertEqual([c['path'] for c in api.commits], [selected['remote_path']])
        self.assertEqual(api.closed_threads, 1)

    def test_retry_one_error_starts_it_without_starting_the_whole_queue(self):
        self.make_photos(3)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.failures[Path(first['local_path']).name] = [SourceReadError('Offline source error')]
        failed = run(self.queue, api, workers=1, job_id=first['id'])
        self.assertEqual(failed['stats']['failed'], 1)
        self.assertEqual(failed['stats']['pending'], 2)
        self.queue.request_control(first['id'], 'resume')
        retried = run(self.queue, api, workers=4, job_id=first['id'])
        self.assertEqual(retried['stats']['uploaded'], 1)
        self.assertEqual(retried['stats']['pending'], 2)
        self.assertEqual(len(api.commits), 1)

    def test_quota_change_and_unknown_file_protocol_error_do_not_pause_batch(self):
        self.make_photos(5)
        api = FakeCloud()
        errors = [RemoteError(400, 'CONDITION/OVERQUOTA'), ProtocolError('Unsupported response structure'),
                  RemoteError(403, authenticated=False), ValueError('Unrelated file detail')]
        for row, error in zip(self.queue.list_jobs(), errors):
            api.failures[Path(row['local_path']).name] = [error]
        api.quota_rule = lambda client: 1000 if client.commits else 100
        result = run(self.queue, api, workers=1)
        self.assertFalse(result['stopped'])
        self.assertFalse(result['auth_required'])
        self.assertEqual(result['stats']['failed'], 4)
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['last_quota']['used_bytes'], 1000)

    def test_actual_auth_failure_stops_before_unrelated_tasks(self):
        self.make_photos(3)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.failures[Path(first['local_path']).name] = [RemoteError(403)]
        result = run(self.queue, api, workers=1)
        self.assertTrue(result['stopped'])
        self.assertTrue(result['auth_required'])
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['pending'], 2)
        self.assertEqual(api.commits, [])

    def test_unavailable_quota_telemetry_does_not_pause_verified_uploads(self):
        self.make_photos(2)
        api = FakeCloud(quota_values=[TransportError('Offline quota'), TransportError('Offline quota')])
        events = []
        result = run(self.queue, api, workers=1, emit=lambda name, data: events.append(name))
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 2)
        self.assertEqual(events.count('quota_warning'), 2)

    def test_regular_positive_commit_recovers_after_interrupted_stat(self):
        self.make_video()
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        api.after_confirmation_stat_failures[first['remote_path']] = Cancelled('Offline pause after ordinary add')
        interrupted = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertTrue(interrupted['stopped'])
        self.assertEqual(interrupted['stats']['uploaded_regular_count'], 1)
        self.assertEqual(interrupted['stats']['uploaded'], 0)
        api.stop.clear()
        resumed = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertEqual(resumed['outcome'], 'complete')
        self.assertEqual(resumed['stats']['uploaded'], 1)
        self.assertEqual(len(api.regular_commits), 1)
        self.assertEqual(api.commits, [])
        receipt = self.queue.upload_history(first['id'])[-1]['verified_metadata']
        self.assertEqual(receipt['regular_evidence'], 'saved_commit_response')
        self.assertTrue(receipt['recovered_commit'])

    def test_exhausted_quota_never_calls_regular_api_but_photos_continue(self):
        self.make_video('first.m4v')
        self.make_video('second.m4v')
        self.make_photos(2)
        api = FakeCloud(quota_values=[100])
        api.total_bytes = 100
        with patch.object(api, 'upload_regular', wraps=api.upload_regular) as ordinary:
            result = run(self.queue, api, workers=2, regular_policy='upload')
        ordinary.assert_not_called()
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 2)
        self.assertEqual(result['stats']['failed'], 2)
        self.assertEqual(api.upload_types, ['AUTO', 'AUTO'])
        self.assertEqual(api.regular_commits, [])
        self.assertEqual(api.account_calls, 1)  # Only the initial account binding.
        self.assertEqual(api.quota_calls, 2)  # Initial and final telemetry, no per-file GETs.
        self.assertTrue(all('Передача не начата' in row['last_error'] for row in self.queue.list_jobs('failed')))

    def test_two_workers_cannot_spend_the_same_fifteen_megabytes(self):
        for name in ('first.m4v', 'second.m4v'):
            with (self.photos / name).open('wb') as stream:
                stream.truncate(10 * 1024 * 1024)
        self.queue.set_metadata('last_scan', asdict(self.queue.scan()))
        quota_barrier = threading.Barrier(2)

        class ConcurrentQuotaCloud(FakeCloud):
            def quota(self):
                value = super().quota()
                if threading.current_thread().name.startswith('media-upload'):
                    # Both workers have passed their precheck before either
                    # can acquire its first byte reservation.
                    quota_barrier.wait(timeout=5)
                return value

        api = ConcurrentQuotaCloud(quota_values=[0])
        api.total_bytes = 15 * 1024 * 1024
        result = run(self.queue, api, workers=2, regular_policy='upload')
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded_regular_count'], 1)
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(api.upload_types, ['MANUAL'])
        self.assertEqual(len(api.regular_commits), 1)
        self.assertEqual(api.regular_commits[0]['size'], 10 * 1024 * 1024)
        self.assertIn('Передача не начата', self.queue.list_jobs('failed')[0]['last_error'])

    def test_new_overquota_response_stops_further_regular_requests_not_photos(self):
        self.make_video('first.m4v')
        self.make_video('second.m4v')
        self.make_photos(1)
        api = FakeCloud(quota_values=[0, 10000, 10000])
        api.total_bytes = 10000
        with patch.object(api, 'upload_regular', wraps=api.upload_regular) as ordinary:
            result = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertEqual(ordinary.call_count, 1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['failed'], 2)
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 1)
        self.assertEqual(api.upload_types, ['AUTO'])
        self.assertEqual(api.regular_commits, [])

    def test_existing_regular_confirmation_recovers_even_when_quota_is_full(self):
        self.make_video()
        row = self.queue.list_jobs()[0]
        api = FakeCloud(quota_values=[0])
        api.after_confirmation_stat_failures[row['remote_path']] = Cancelled('pause after acknowledged commit')
        first = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertTrue(first['stopped'])
        self.assertEqual(len(api.regular_commits), 1)
        api.stop.clear()
        api.quota_values = [api.total_bytes]
        with patch.object(api, '_upload_blob', wraps=api._upload_blob) as body:
            resumed = run(self.queue, api, workers=1, regular_policy='upload')
        body.assert_not_called()
        self.assertEqual(resumed['outcome'], 'complete')
        self.assertEqual(resumed['stats']['uploaded_regular_count'], 1)
        self.assertEqual(len(api.regular_commits), 1)
        self.assertTrue(self.queue.upload_history(row['id'])[-1]['verified_metadata']['recovered_commit'])

    def _negative_commit_spends_budget(self, action):
        self.make_photos(1)
        first = self.queue.list_jobs()[0]
        self.make_video('ordinary.m4v')
        regular = next(row for row in self.queue.list_jobs() if row['planned_mode'] == 'regular')
        self.make_video('last.mov')
        api = FakeCloud(quota_values=[0])
        api.total_bytes = first['size'] + regular['size'] - 1
        api.unlimited_flags[first['remote_path']] = False
        with patch.object(api, 'upload_regular', wraps=api.upload_regular) as ordinary:
            result = run(self.queue, api, workers=1, regular_policy='upload', unexpected_unlimited_action=action)
        ordinary.assert_not_called()
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['failed'], 2)
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 1)
        self.assertEqual(api.regular_commits, [])
        self.assertEqual(api.upload_types, ['AUTO', 'AUTO'])
        return api, first

    def test_unexpected_regular_file_kept_in_cloud_spends_batch_budget(self):
        api, first = self._negative_commit_spends_budget('keep')
        self.assertIn(first['remote_path'], api.remote)
        self.assertEqual(api.trashed, [])

    def test_moving_unexpected_regular_file_to_trash_does_not_free_batch_budget(self):
        api, first = self._negative_commit_spends_budget('trash')
        self.assertEqual(api.trashed, [first['remote_path']])
        self.assertNotIn(first['remote_path'], api.remote)

    def test_failed_transfer_before_commit_releases_reservation_for_next_file(self):
        self.make_video('first.m4v')
        self.make_video('second.m4v')
        rows = self.queue.list_jobs()
        api = FakeCloud(quota_values=[0])
        api.total_bytes = max(row['size'] for row in rows) + 1
        api.failures['first.m4v'] = [SourceReadError('offline read error before commit')]
        result = run(self.queue, api, workers=1, regular_policy='upload', max_attempts=1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(result['stats']['uploaded_regular_count'], 1)
        self.assertEqual(len(api.regular_commits), 1)
        self.assertTrue(api.regular_commits[0]['path'].endswith('second.m4v'))

    def test_ambiguous_regular_commit_keeps_reserved_bytes_for_following_files(self):
        self.make_video('first.m4v')
        self.make_video('second.m4v')
        rows = self.queue.list_jobs()
        self.make_photos(1)
        api = FakeCloud(quota_values=[0])
        api.total_bytes = max(row['size'] for row in rows) + 1
        api.after_commit_failures[rows[0]['remote_path']] = TransportError('response lost after ordinary commit')
        result = run(self.queue, api, workers=1, regular_policy='upload', max_attempts=1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['failed'], 2)
        self.assertEqual(result['stats']['uploaded_unlimited_count'], 1)
        self.assertEqual(api.upload_types, ['MANUAL', 'AUTO'])
        self.assertEqual(len(api.regular_commits), 1)
        self.assertIn(rows[0]['remote_path'], api.remote)

    def test_before_and_after_stat_observations_only_charge_once(self):
        self.make_video('first.m4v')
        self.make_video('second.m4v')
        sizes = [row['size'] for row in self.queue.list_jobs()]
        api = FakeCloud(quota_values=[0])
        api.total_bytes = sum(sizes)
        result = run(self.queue, api, workers=1, regular_policy='upload')
        self.assertEqual(result['outcome'], 'complete')
        self.assertEqual(result['stats']['uploaded_regular_count'], 2)
        self.assertEqual(api.upload_types, ['MANUAL', 'MANUAL'])

    def test_incomplete_or_unpersisted_upload_result_never_marks_uploaded(self):
        self.make_photos(1)
        for flag in ({}, {'is_unlim_upload': False}, {'is_unlim_upload': 1},
                     {'is_unlim_upload': 'true'}, {'is_unlim_upload': None}):
            with self.subTest(flag=flag):
                self.queue.retry_failed(include_permanent=True)
                api = FakeCloud()
                def incomplete(path, remote_path, *, expected, save_intent, save_confirmation, **kwargs):
                    save_intent('A' * 40)
                    save_confirmation('A' * 40)
                    return {'hash': 'A' * 40, 'size': expected.size, 'path': remote_path,
                            'commit_accepted': True, 'recovered_commit': False, 'stat_matched': True,
                            'unlimited_evidence': 'commit_response', **flag}
                api.upload_photo = incomplete
                result = run(self.queue, api, workers=1)
                self.assertFalse(result['stopped'])
                self.assertEqual(result['stats']['uploaded'], 0)
                self.assertEqual(result['stats']['failed'], 1)

    def test_large_files_are_skipped_by_default_without_hashing_or_regular_commit(self):
        self.make_large_video()
        api = FakeCloud()
        with patch('mailru_photos.api.cloud_hash', side_effect=AssertionError('No large file read expected')):
            result = run(self.queue, api, workers=1)
        self.assertEqual(result['outcome'], 'finished_with_skips')
        self.assertEqual(result['stats']['skipped'], 1)
        self.assertEqual(api.commits, [])
        self.assertEqual(self.queue.list_jobs()[0]['skip_reason'], 'regular_size_disabled')

    def test_multithreaded_run_claims_every_photo_once(self):
        self.make_photos(8)
        api = FakeCloud(barrier=threading.Barrier(4))
        result = run(self.queue, api, workers=4)
        self.assertFalse(result["stopped"])
        self.assertEqual(result["outcome"], "complete")
        self.assertEqual(result["stats"]["uploaded"], 8)
        self.assertEqual(len({entry["path"] for entry in api.commits}), 8)
        self.assertEqual(len(api.commits), 8)
        self.assertEqual(api.max_active_uploads, 4)
        self.assertEqual(api.closed_threads, 4)

    def test_pausing_one_task_keeps_other_workers_running_and_survives_restart(self):
        self.make_photos(3)
        first = self.queue.list_jobs()[0]
        signals = {}
        @contextlib.contextmanager
        def scope(job):
            signal = threading.Event()
            signals[job.id] = signal
            try:
                yield signal
            finally:
                signals.pop(job.id, None)
        api = FakeCloud()
        original = api._upload_blob
        pause_once = True
        def interrupt_first(path, digest, identity, **kwargs):
            nonlocal pause_once
            original(path, digest, identity, **kwargs)
            if str(path) == first['local_path'] and pause_once:
                pause_once = False
                with Queue(self.queue.path, destination=self.queue.destination) as controls:
                    controls.request_control(first['id'], 'pause')
                signals[first['id']].set()
                api._check_stopped()
        api._upload_blob = interrupt_first
        result = run(self.queue, api, workers=2, task_scope=scope)
        self.assertFalse(api.stop.is_set())
        self.assertFalse(result['stopped'])
        self.assertEqual(result['outcome'], 'paused_tasks')
        self.assertEqual(result['stats']['uploaded'], 2)
        self.assertEqual(result['stats']['paused'], 1)
        self.assertEqual(result['stats']['running'], 0)
        self.assertEqual(signals, {})
        queue_path = self.queue.path
        self.queue.close()
        self.queue = Queue(queue_path, destination='/Archive', account_id=EMAIL)
        paused = self.queue.list_jobs(status='paused')
        self.assertEqual([row['id'] for row in paused], [first['id']])
        self.assertEqual(paused[0]['attempts'], 0)
        self.queue.request_control(first['id'], 'resume')
        resumed = run(self.queue, api, workers=2, task_scope=scope)
        self.assertEqual(resumed['outcome'], 'complete')
        self.assertEqual(resumed['stats']['uploaded'], 3)
        self.assertEqual(len(api.commits), 3)
        self.assertEqual(signals, {})

    def test_task_pause_with_positive_commit_keeps_proof_and_resumes_without_second_add(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        signals = {}
        @contextlib.contextmanager
        def scope(job):
            signal = threading.Event()
            signals[job.id] = signal
            try:
                yield signal
            finally:
                signals.pop(job.id, None)
        api = FakeCloud()
        original = api._request
        def pause_after_ack(method, url, **kwargs):
            response = original(method, url, **kwargs)
            if json.loads(kwargs['body'])['path'] == first['remote_path']:
                with Queue(self.queue.path, destination=self.queue.destination) as controls:
                    controls.request_control(first['id'], 'pause')
                signals[first['id']].set()
            return response
        api._request = pause_after_ack
        result = run(self.queue, api, workers=1, task_scope=scope)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['paused'], 1)
        saved = json.loads(self.queue._db.execute(
            'SELECT intent_json FROM commit_intents WHERE job_id=?', (first['id'],)).fetchone()[0])
        self.assertIs(saved['unlimited_confirmed'], True)
        self.assertEqual(len(api.commits), 2)
        self.queue.request_control(first['id'], 'resume')
        resumed = run(self.queue, api, workers=1, task_scope=scope)
        self.assertEqual(resumed['stats']['uploaded'], 2)
        self.assertEqual(resumed['outcome'], 'complete')
        self.assertEqual(len(api.commits), 2)
        evidence = self.queue.upload_history(first['id'])[-1]['verified_metadata']
        self.assertTrue(evidence['recovered_commit'])
        self.assertEqual(evidence['unlimited_evidence'], 'saved_commit_response')

    def test_durable_pause_without_ui_signal_is_checked_before_commit(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        original = api._upload_blob
        def durable_pause(path, digest, identity, **kwargs):
            original(path, digest, identity, **kwargs)
            if str(path) == first['local_path']:
                with Queue(self.queue.path, destination=self.queue.destination) as controls:
                    controls.request_control(first['id'], 'pause')
        api._upload_blob = durable_pause
        result = run(self.queue, api, workers=1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['paused'], 1)
        self.assertNotIn(first['remote_path'], api.remote)

    def test_remove_requested_with_positive_commit_keeps_receipt_and_continues(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        signals = {}
        @contextlib.contextmanager
        def scope(job):
            signal = threading.Event()
            signals[job.id] = signal
            try:
                yield signal
            finally:
                signals.pop(job.id, None)
        api = FakeCloud()
        original = api._request
        def remove_after_ack(method, url, **kwargs):
            reply = original(method, url, **kwargs)
            if json.loads(kwargs['body'])['path'] == first['remote_path']:
                with Queue(self.queue.path, destination=self.queue.destination) as controls:
                    controls.request_control(first['id'], 'remove')
                signals[first['id']].set()
            return reply
        api._request = remove_after_ack
        result = run(self.queue, api, workers=1, task_scope=scope)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['total'], 1)
        self.assertIsNone(self.queue.get_job(first['id']))
        self.assertTrue(Path(first['local_path']).exists())
        self.assertIn(first['remote_path'], api.remote)
        receipts = self.queue.commit_history(first['id'])
        accepted = [row for row in receipts if row['stage'] == 'accepted']
        self.assertEqual(len(accepted), 1)
        self.assertIs(json.loads(accepted[0]['intent_json'])['unlimited_confirmed'], True)
        self.assertEqual(self.queue.upload_history(first['id']), [], 'A receipt without stat is not verified upload success')
        self.assertEqual(len(api.commits), 2)

    def test_source_change_after_positive_commit_keeps_receipt_without_stopping_others(self):
        self.make_photos(2)
        first = self.queue.list_jobs()[0]
        api = FakeCloud()
        original = api._request
        def replace_source_after_ack(method, url, **kwargs):
            reply = original(method, url, **kwargs)
            if json.loads(kwargs['body'])['path'] == first['remote_path']:
                Path(first['local_path']).write_bytes(b'a newer local version' * 32)
            return reply
        api._request = replace_source_after_ack
        result = run(self.queue, api, workers=1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['failed'], 1)
        self.assertEqual(self.queue.get_job(first['id'])['skip_reason'], 'source_changed')
        accepted = [row for row in self.queue.commit_history(first['id']) if row['stage'] == 'accepted']
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0]['source_size'], first['size'])
        self.assertEqual(self.queue.upload_history(first['id']), [])
        self.assertEqual(len(api.commits), 2)

    def test_file_failures_do_not_stop_later_tasks(self):
        failures = [SourceReadError('Unreadable source'), SourceChanged('Changed source'),
                    RemoteConflict('Existing object'), FileVerificationError('Hash mismatch'),
                    RemoteError(400, 'INVALID/FILE')]
        self.make_photos(len(failures) + 1)
        api = FakeCloud()
        for row, error in zip(self.queue.list_jobs(), failures):
            api.failures[Path(row['local_path']).name] = [error]
        result = run(self.queue, api, workers=1)
        self.assertFalse(result['stopped'])
        self.assertFalse(result['auth_required'])
        self.assertEqual(result['outcome'], 'finished_with_errors')
        self.assertEqual(result['stats']['failed'], len(failures))
        self.assertEqual(result['stats']['uploaded'], 1)
        self.assertEqual(result['stats']['running'], 0)

    def test_run_claim_cancellation_is_saved_without_claiming_or_auth_error(self):
        self.make_photos(3)
        api = FakeCloud()
        original = Queue.claim
        def cancel_claim(queue, *, stop=None, **kwargs):
            self.assertIs(stop, api.stop)
            stop.set()
            return original(queue, stop=stop, **kwargs)
        with patch.object(Queue, 'claim', new=cancel_claim):
            result = run(self.queue, api, workers=1)
        self.assertTrue(result['stopped'])
        self.assertFalse(result['auth_required'])
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['stats']['pending'], 3)
        self.assertEqual(result['stats']['running'], 0)
        self.assertEqual(api.commits, [])

    def test_auth_rejection_is_not_erased_if_saving_the_failed_task_also_fails(self):
        self.make_photos(1)
        api = FakeCloud()
        first = self.queue.list_jobs()[0]
        api.failures[Path(first['local_path']).name] = [RemoteError(403)]
        with patch.object(Queue, 'mark_failed', side_effect=sqlite3.OperationalError('offline write failure')):
            result = run(self.queue, api, workers=1)
        self.assertTrue(result['stopped'])
        self.assertTrue(result['auth_required'])
        self.assertEqual(result['stats']['uploaded'], 0)
        self.assertEqual(api.commits, [])

    def test_database_confirmation_failure_stops_other_tasks(self):
        self.make_photos(3)
        api = FakeCloud()
        with patch.object(Queue, 'confirm_unlimited_commit', side_effect=sqlite3.OperationalError('disk full')):
            result = run(self.queue, api, workers=1)
        self.assertTrue(result['stopped'])
        self.assertEqual(result['stats']['uploaded'], 0)
        self.assertEqual(len(api.commits), 1)
        self.assertEqual(result['stats']['pending'], 2)
        self.assertTrue(result['errors'])

    def test_lost_add_response_cannot_confirm_unlimited_or_complete_upload(self):
        self.make_photos(1)
        api = FakeCloud()
        remote_path = self.queue.list_jobs()[0]["remote_path"]
        api.after_commit_failures[remote_path] = TransportError("Response lost after remote commit")
        started = time.monotonic()
        with patch("mailru_photos.runner.random.random", return_value=0):
            result = run(self.queue, api, workers=1, max_attempts=2)
        self.assertLess(time.monotonic() - started, 8)
        self.assertEqual(result["outcome"], "finished_with_errors")
        self.assertEqual(result["stats"]["uploaded"], 0)
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(len(api.commits), 1)
        row = self.queue.list_jobs()[0]
        self.assertEqual(row["attempts"], 2)
        self.assertEqual(self.queue.upload_history(row["id"]), [])
        intent = json.loads(self.queue._db.execute("SELECT intent_json FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone()[0])
        self.assertIsNot(intent.get("unlimited_confirmed"), True)

    def test_confirmed_unlimited_commit_survives_interrupted_stat_and_recovers(self):
        self.make_photos(1)
        api = FakeCloud()
        remote_path = self.queue.list_jobs()[0]["remote_path"]
        api.after_confirmation_stat_failures[remote_path] = Cancelled("Interrupted after unlimited confirmation")
        first = run(self.queue, api, workers=1)
        self.assertEqual(first["outcome"], "stopped")
        self.assertEqual(first["stats"]["uploaded"], 0)
        self.assertEqual(first["stats"]["pending"], 1)
        saved = json.loads(self.queue._db.execute("SELECT intent_json FROM commit_intents").fetchone()[0])
        self.assertIs(saved["unlimited_confirmed"], True)
        self.assertGreater(saved["unlimited_confirmed_at"], 0)
        api.stop.clear()
        resumed = run(self.queue, api, workers=1)
        self.assertEqual(resumed["outcome"], "complete")
        self.assertEqual(resumed["stats"]["uploaded"], 1)
        self.assertEqual(len(api.commits), 1)
        row = self.queue.list_jobs()[0]
        evidence = self.queue.upload_history(row["id"])[0]["verified_metadata"]
        self.assertIs(evidence["is_unlim_upload"], True)
        self.assertTrue(evidence["recovered_commit"])
        self.assertFalse(evidence["commit_accepted"])
        self.assertEqual(evidence["unlimited_evidence"], "saved_commit_response")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)

    def test_no_intent_means_existing_matching_file_is_a_conflict(self):
        self.make_photos(1)
        api = FakeCloud()
        row = self.queue.list_jobs()[0]
        digest, identity = cloud_hash(row["local_path"])
        api.remote[row["remote_path"]] = {"hash": digest, "size": identity.size}
        result = run(self.queue, api, workers=1)
        self.assertEqual(result["outcome"], "finished_with_errors")
        self.assertEqual(result["stats"]["uploaded"], 0)
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(api.commits, [])
        self.assertEqual(api.upload_calls, [])

    def test_retry_limit_is_terminal_and_is_not_reported_as_complete(self):
        self.make_photos(1)
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [RemoteError(503)]
        result = run(self.queue, api, workers=1, max_attempts=1)
        self.assertEqual(result["outcome"], "finished_with_errors")
        self.assertEqual(result["stats"]["permanent_failures"], 1)
        self.assertEqual(len(api.upload_calls), 1)

    def test_skipped_photos_are_explicit_in_the_final_outcome(self):
        self.make_photos(2)
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [UnsupportedPhoto("unsupported offline fixture")]
        result = run(self.queue, api, workers=1)
        self.assertEqual(result["outcome"], "finished_with_skips")
        self.assertEqual(result["stats"]["skipped"], 1)
        self.assertEqual(result["stats"]["uploaded"], 1)

    def test_cancellation_inside_final_quota_telemetry_is_saved(self):
        self.make_photos(0)
        api = FakeCloud(quota_values=[100, Cancelled("cancel final quota")])
        result = run(self.queue, api, workers=1)
        self.assertTrue(result["stopped"])
        self.assertEqual(self.queue.get_metadata("last_run")["outcome"], "stopped")

    def test_interrupt_on_last_attempt_can_resume(self):
        self.make_photos(1)
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [KeyboardInterrupt()]
        interrupted = run(self.queue, api, workers=1, max_attempts=1)
        self.assertTrue(interrupted["stopped"])
        resumed = run(self.queue, FakeCloud(), workers=1, max_attempts=1)
        self.assertEqual(resumed["outcome"], "complete")
        self.assertEqual(resumed["stats"]["uploaded"], 1)
        self.assertEqual(self.queue.list_jobs()[0]["attempts"], 1)

    def test_cancelled_last_attempt_is_released_without_spending_retry(self):
        self.make_photos(1)
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [Cancelled("pause upload")]
        first = run(self.queue, api, workers=1, max_attempts=1)
        self.assertTrue(first["stopped"])
        row = self.queue.list_jobs()[0]
        self.assertEqual((row["status"], row["attempts"]), ("pending", 0))
        second = run(self.queue, FakeCloud(), workers=1, max_attempts=1)
        self.assertEqual(second["outcome"], "complete")


if __name__ == "__main__":
    unittest.main()

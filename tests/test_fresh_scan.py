"""Fresh task snapshots, global content matching, and selected-task recovery."""
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from mailru_photos.api import cloud_hash
from mailru_photos.transport import Response, TransportError
import test_web_service as fixtures


class FreshScanTests(unittest.TestCase):
    setUp = fixtures.WebServiceTests.setUp
    tearDown = fixtures.WebServiceTests.tearDown
    make_service = fixtures.WebServiceTests.make_service
    photos_count = fixtures.WebServiceTests.photos_count
    done = fixtures.WebServiceTests.done

    def remote_copy(self, local, remote, *, unlimited=True):
        self.cloud.remote[remote] = {'hash': cloud_hash(local)[0], 'size': local.stat().st_size}
        if unlimited is not None:
            self.cloud.remote[remote]['is_unlim_upload'] = unlimited

    def test_rescan_replaces_ids_and_reuploads_only_the_file_deleted_from_cloud(self):
        self.photos_count(2)
        self.service.start()
        self.assertEqual(self.done()['stats']['uploaded'], 2)
        old = self.service.jobs()['jobs']
        old_ids = {row['id'] for row in old}
        lost, retained = old
        self.cloud.remote.pop(lost['remote_path'])
        self.cloud.remote[retained['remote_path']]['is_unlim_upload'] = True
        self.service.scan()
        scanned = self.done()
        self.assertTrue(scanned['last_scan']['cloud_checked'])
        self.assertEqual((scanned['stats']['pending'], scanned['stats']['existing']), (1, 1))
        fresh = self.service.jobs()['jobs']
        self.assertTrue(old_ids.isdisjoint(row['id'] for row in fresh))
        self.assertEqual(scanned['storage_plan']['uploaded']['count'], 0)
        self.assertEqual(scanned['storage_plan']['existing']['count'], 1)
        with self.service._queue() as queue:
            self.assertEqual(len(queue.upload_history(lost['id'])), 1)
        self.service.start()
        self.assertEqual(self.done()['stats']['pending'], 0)
        self.assertEqual(len(self.cloud.commits), 3)
        self.assertEqual(self.cloud.commits[-1]['path'], lost['remote_path'])

    def test_same_content_in_other_folder_is_found_but_same_name_is_not_enough(self):
        self.photos_count(2)
        self.remote_copy(self.photos / '000.jpg', '/Семья/Архив/другое имя.jpg', unlimited=False)
        # Same filename and size with different content does not match.
        self.cloud.remote['/Elsewhere/001.jpg'] = {'hash': 'A' * 40, 'size': (self.photos / '001.jpg').stat().st_size,
                                                  'is_unlim_upload': True}
        self.service.scan()
        state = self.done()
        rows = {row['relative_path']: row for row in self.service.jobs()['jobs']}
        found = rows['000.jpg']
        self.assertEqual(found['skip_reason'], 'remote_exists')
        self.assertFalse(found['cloud_path_matches'])
        self.assertEqual(found['cloud_path'], '/Семья/Архив/другое имя.jpg')
        self.assertIsNone(found['observed_mode'], 'Discovery does not publish an unreliable billing mode')
        self.assertEqual(rows['001.jpg']['status'], 'pending')
        self.assertEqual(state['storage_plan']['existing']['count'], 1)
        self.assertEqual(state['storage_plan']['uploaded']['count'], 0)
        self.assertEqual(self.cloud.commits, [])

    def test_existing_files_are_recognized_before_skip_policy_without_publishing_modes(self):
        path = self.photos / 'clip.m4v'
        path.write_bytes(b'ordinary format')
        self.remote_copy(path, '/Camera/clip.m4v', unlimited=False)
        photo = self.photos / 'photo.jpg'
        photo.write_bytes(b'unknown storage')
        self.remote_copy(photo, '/Camera/photo.jpg', unlimited=None)
        self.service.scan()
        state = self.done()
        self.assertEqual(state['stats']['existing'], 2)
        self.assertEqual(state['stats']['skipped_tasks_count'], 0)
        self.assertEqual(state['storage_plan']['regular']['count'], 0)
        self.assertEqual(state['storage_plan']['existing']['count'], 2)
        for mode in ('regular', 'unknown', 'unlimited'):
            self.assertNotIn('existing_' + mode, state['storage_plan'])
            self.assertNotIn('existing_' + mode + '_count', state['stats'])
        self.assertTrue(all(row['observed_mode'] is None for row in self.service.jobs('existing')['jobs']))

    def test_disabled_cloud_check_still_rebuilds_without_inheriting_old_presence(self):
        self.photos_count(1)
        self.service.start()
        self.done()
        self.cloud.remote.clear()
        self.service.settings(remote_reconcile=False)
        calls = len(self.cloud.folder_calls)
        self.service.scan()
        state = self.done()
        self.assertEqual(state['stats']['pending'], 1)
        self.assertEqual(state['stats']['uploaded'], 0)
        self.assertFalse(state['last_scan']['cloud_checked'])
        self.assertFalse(state['last_scan']['warning_allowed'])
        self.assertEqual(len(self.cloud.folder_calls), calls)

    def test_regular_positive_meta_with_optional_extensions_is_success_and_rescan_is_existing(self):
        video = self.photos / 'clip.m4v'
        video.write_bytes(b'm4v offline fixture')
        self.service.settings(regular_policy='upload')
        request = self.cloud._request
        def opaque_success(method, url, **kwargs):
            result = request(method, url, **kwargs)
            if method == 'POST' and url.endswith('/meta'):
                return Response(200, {}, b'\x00\x80\xff\xfeopaque-server-revision')
            return result
        with patch.object(self.cloud, '_request', side_effect=opaque_success):
            self.service.start()
            state = self.done()
        self.assertEqual(state['stats']['failed'], 0, state['activity'])
        self.assertEqual(state['storage_plan']['uploaded_regular']['count'], 1)
        self.assertEqual(len(self.cloud.regular_commits), 1)
        self.assertEqual(self.cloud.upload_types, ['MANUAL'])
        remote = self.cloud.regular_commits[0]['path']
        self.cloud.remote[remote]['is_unlim_upload'] = False
        self.service.scan()
        state = self.done()
        self.assertEqual(state['stats']['failed'], 0)
        self.assertEqual(state['storage_plan']['existing']['count'], 1)
        self.assertEqual(state['storage_plan']['regular']['count'], 0)

    def test_manual_warning_counts_only_new_quota_tasks(self):
        for name in ('old.m4v', 'new.m4v'):
            (self.photos / name).write_bytes(name.encode())
        self.remote_copy(self.photos / 'old.m4v', '/Existing/old.m4v', unlimited=False)
        self.service.settings(regular_policy='upload')
        self.service.scan()
        state = self.done()
        self.assertTrue(state['last_scan']['warning_allowed'])
        self.assertEqual(state['last_scan']['trigger'], 'manual')
        self.assertEqual(state['storage_plan']['regular'], {'count': 1, 'bytes': (self.photos / 'new.m4v').stat().st_size})
        self.assertEqual(state['storage_plan']['existing']['count'], 1)

    def test_selected_start_survives_new_task_ids_without_starting_other_tasks(self):
        self.photos_count(3)
        self.service.scan()
        self.done()
        chosen = self.service.jobs()['jobs'][1]
        with self.service._queue() as queue:
            queue.set_metadata('last_scan', {'complete': False})
        self.service.control_task(chosen['id'], 'start')
        state = self.done()
        self.assertEqual(state['stats']['uploaded'], 1, state['activity'])
        self.assertEqual(state['stats']['pending'], 2)
        completed = self.service.jobs('uploaded_unlimited')['jobs'][0]
        self.assertNotEqual(completed['id'], chosen['id'])
        self.assertEqual(completed['local_path'], chosen['local_path'])
        self.assertEqual(self.cloud.upload_calls, [chosen['local_path']])

    def test_selected_start_rejects_changed_source_instead_of_uploading_another_task(self):
        self.photos_count(2)
        self.service.scan()
        self.done()
        chosen = self.service.jobs()['jobs'][0]
        Path(chosen['local_path']).write_bytes(b'different content and different size')
        with self.service._queue() as queue:
            queue.set_metadata('last_scan', {'complete': False})
        self.service.control_task(chosen['id'], 'start')
        state = self.done()
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertEqual(self.cloud.upload_calls, [])
        self.assertEqual(state['stats']['pending'], 2)

    def test_interrupted_inventory_restores_single_selection_after_restart(self):
        self.photos_count(3)
        self.service.scan()
        self.done()
        chosen = self.service.jobs()['jobs'][1]
        with self.service._queue() as queue:
            queue.set_metadata('last_scan', {'complete': False})
        entered = threading.Event()
        def stopped_listing(*args, **kwargs):
            entered.set()
            self.cloud.stop.wait(3)
            self.cloud._check_stopped()
            raise AssertionError('Expected cancellation')
        with patch.object(self.cloud, 'list_folder', side_effect=stopped_listing):
            self.service.control_task(chosen['id'], 'start')
            self.assertTrue(entered.wait(2))
            self.service.close(timeout=2)
        replacement = self.make_service()
        self.assertTrue(replacement.resume_if_requested())
        state = self.done(replacement)
        self.assertEqual(state['stats']['uploaded'], 1, state['activity'])
        self.assertEqual(state['stats']['pending'], 2)
        self.assertEqual(self.cloud.upload_calls, [chosen['local_path']])
        self.assertFalse(state['last_scan']['warning_allowed'])
        self.assertEqual(state['last_scan']['trigger'], 'automatic')

    def test_incomplete_cloud_inventory_does_not_start_uploads(self):
        self.photos_count(2)
        with patch.object(self.cloud, 'list_folder', side_effect=TransportError('offline interruption')):
            self.service.start()
            state = self.done()
        self.assertFalse(state['last_scan']['complete'])
        self.assertTrue(state['last_scan']['local_complete'])
        self.assertFalse(state['last_scan']['warning_allowed'])
        self.assertEqual(state['activity']['phase'], 'error')
        self.assertEqual(self.cloud.upload_calls, [])
        self.assertEqual(state['stats']['pending'], 2)

    def test_task_progress_freezes_on_pause_and_survives_process_restart(self):
        path = self.photos / 'progress.jpg'
        path.write_bytes(b'x' * 1000)
        self.service.scan()
        self.done()
        chosen = self.service.jobs()['jobs'][0]
        entered, release = threading.Event(), threading.Event()
        def partial_upload(path, digest, identity, *, progress=None, **kwargs):
            progress(333)
            entered.set()
            release.wait(2)
            self.cloud._check_stopped()
            raise AssertionError('Expected task cancellation')
        with patch.object(self.cloud, '_upload_blob', side_effect=partial_upload):
            self.service.control_task(chosen['id'], 'start')
            self.assertTrue(entered.wait(2))
            self.service.control_task(chosen['id'], 'pause')
            release.set()
            self.done()
        paused = self.service.jobs('paused')['jobs'][0]
        self.assertEqual(paused['progress']['sent'], 333)
        self.assertAlmostEqual(paused['progress']['percent'], 33.3)
        self.service.close()
        replacement = self.make_service()
        restored = replacement.jobs('paused')['jobs'][0]
        self.assertEqual(restored['progress']['sent'], 333)
        original = self.cloud._upload_blob
        entered.clear()
        release.clear()
        def resumed_upload(path, digest, identity, *, progress=None, **kwargs):
            progress(100)
            entered.set()
            release.wait(2)
            return original(path, digest, identity, progress=progress, **kwargs)
        with patch.object(self.cloud, '_upload_blob', side_effect=resumed_upload):
            replacement.control_task(chosen['id'], 'resume')
            self.assertTrue(entered.wait(2))
            self.assertEqual(replacement.jobs()['jobs'][0]['progress']['sent'], 333)
            release.set()
            state = self.done(replacement)
        self.assertEqual(state['stats']['uploaded'], 1)


if __name__ == '__main__':
    unittest.main()

"""Frozen cleanup journals must revalidate creation ownership before I/O."""
from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from mailru_photos.batch_cleanup import BatchCleanupJournal
from mailru_photos.queue import Queue

try:
    import test_planning_service as fixtures
except ModuleNotFoundError:
    from tests import test_planning_service as fixtures


class BatchCleanupOwnershipTests(unittest.TestCase):
    def setUp(self):
        # Reuse the real offline controller fixture without inheriting and
        # re-running the entire integration test class during discovery.
        self.fixture = fixtures.PlanningServiceTests()
        self.addCleanup(self.fixture.doCleanups)
        self.addCleanup(self.fixture.tearDown)
        self.fixture.setUp()

    def freeze_journal(self, batch):
        service = self.fixture.service
        preview = service.plan_cleanup_preview(batch['id'], owner='session')
        expected = service._batch_cleanup_previews[preview['token']]
        with service._queue() as queue, BatchCleanupJournal(self.fixture.state / 'batch-cleanup.sqlite3') as journal:
            journal.prepare(queue, expected)
            return journal.pending()[0]

    def restart_and_resume(self):
        self.fixture.service.close()
        replacement = self.fixture.make_service()
        replacement.start()
        self.fixture.done(replacement)
        return replacement

    def test_superseding_commit_after_journal_checkpoint_never_trashes_same_content_object(self):
        _, batch, _ = self.fixture.pause_with_one_owned_and_one_existing()
        service, cloud = self.fixture.service, self.fixture.cloud
        with service._queue() as queue:
            # Simulate a successful create response without a server node ID;
            # this is a supported receipt shape, especially before final stat.
            queue._db.execute('UPDATE cloud_observations SET remote_id=NULL WHERE batch_id=?', (batch['id'],))
        frozen = self.freeze_journal(batch)
        self.assertIsNone(frozen['remote_id'])
        with service._queue() as queue:
            target = queue.batch_cloud_object_state(batch['id'], frozen['id'])['object']
            queue.record_task_error(target['job_id'], 'External replacement fixture')
            queue.request_control(target['job_id'], 'start')
            job = queue.claim(job_id=target['job_id'])
            queue.save_commit_intent(job, frozen['cloud_hash'])
            queue.record_cloud_observation(job, mode='unlimited', remote_hash=frozen['cloud_hash'],
                remote_id='different-creation', metadata={'commit_accepted': True, 'path': frozen['remote_path'], 'size': frozen['cloud_size']})
            queue.mark_failed(job, 'Foreign creation already exists', retry_delay=None)
            self.assertEqual(queue.batch_cloud_object_state(batch['id'], frozen['id'])['state'], 'superseded')
        cloud.remote[frozen['remote_path']]['nodeid'] = 'different-creation'
        with patch.object(cloud, 'trash_file', side_effect=AssertionError('Stale ownership must never reach cloud I/O')):
            replacement = self.restart_and_resume()
        self.assertIn(frozen['remote_path'], cloud.remote)
        self.assertEqual(cloud.trashed, [])
        self.assertIsNone(replacement._batch_cleanup_pending)
        with BatchCleanupJournal(self.fixture.state / 'batch-cleanup.sqlite3') as journal:
            self.assertEqual(journal.summary()['failed'], 1)

    def test_exact_queue_tombstone_before_journal_checkpoint_resumes_without_second_cloud_call(self):
        _, batch, _ = self.fixture.pause_with_one_owned_and_one_existing()
        frozen = self.freeze_journal(batch)
        cloud = self.fixture.cloud
        cloud.stop = threading.Event()
        cloud.trash_file(frozen['remote_path'], expected_hash=frozen['cloud_hash'],
                         expected_size=frozen['cloud_size'], expected_node_id=frozen['remote_id'])
        with self.fixture.service._queue() as queue:
            self.assertTrue(queue.mark_batch_cloud_deleted(batch['id'], frozen['id'],
                expected_hash=frozen['cloud_hash'], expected_path=frozen['remote_path']))
            self.assertEqual(queue.batch_cloud_object_state(batch['id'], frozen['id'])['state'], 'deleted')
        with patch.object(cloud, 'trash_file', side_effect=AssertionError('Exact local deletion checkpoint already exists')):
            replacement = self.restart_and_resume()
        self.assertEqual(cloud.trashed, [frozen['remote_path']])
        self.assertIsNone(replacement._batch_cleanup_pending)
        with BatchCleanupJournal(self.fixture.state / 'batch-cleanup.sqlite3') as journal:
            self.assertEqual((journal.summary()['done'], journal.summary()['failed']), (1, 0))


if __name__ == '__main__':
    unittest.main()

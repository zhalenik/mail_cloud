"""Planner invariants against real SQLite, without cloud or a waiting clock."""
from datetime import datetime
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos.plans import PlanStore, next_allowed, parse_local_datetime, validate_settings
from mailru_photos.transport import Cancelled


def stamp(value):
    return datetime.fromisoformat(value).timestamp()


def settings(**changes):
    return {'target_count': 10, 'target_bytes': 100, 'tolerance_percent': 0,
            'interval_days': 1, 'weekdays': list(range(1, 8)), 'start_date': '2026-11-04',
            'time': '10:00', 'timezone': 'UTC', **changes}


class InventoryFixture:
    def __init__(self, pairs):
        self.id = '1' * 32
        self.revision = 1
        self.pairs = pairs

    def summary(self, identifier):
        return {'id': self.id, 'status': 'ready', 'root': '/archive', 'selected_errors': 0,
                'revision': self.revision, 'selected_count': sum(v[0] for v in self.pairs)}

    def iter_units(self, identifier):
        for index, (count, size) in enumerate(self.pairs):
            name = f'folder-{index:06d}'
            yield {'path': '/archive/' + name, 'relative_path': name, 'remote_subdir': name,
                   'recursive': True, 'excluded_paths': [], 'file_count': count, 'total_bytes': size}


class CalendarTests(unittest.TestCase):
    def test_count_bytes_caps_and_validation(self):
        self.assertEqual(validate_settings(settings(weekdays=[7, 1, 1]))['weekdays'], [1, 7])
        for change in ({'target_count': 0}, {'target_bytes': True}, {'tolerance_percent': 101},
                       {'interval_days': 0}, {'weekdays': []}, {'weekdays': [True]},
                       {'time': '24:30'}, {'timezone': 'unknown/nowhere'}, {'start_date': '2026-02-30'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_settings(settings(**change))

    def test_weekdays_exact_slot_and_start_date(self):
        value = settings(weekdays=[5, 7])
        self.assertEqual(next_allowed(value, stamp('2026-11-04T09:00+00:00')), stamp('2026-11-06T10:00+00:00'))
        self.assertEqual(next_allowed(value, stamp('2026-11-06T10:00+00:00')), stamp('2026-11-06T10:00+00:00'))
        self.assertEqual(next_allowed(value, stamp('2026-11-06T10:00:01+00:00')), stamp('2026-11-06T10:00:01+00:00'))
        self.assertEqual(next_allowed(value, stamp('2026-11-06T10:01+00:00')), stamp('2026-11-08T10:00+00:00'))

    def test_dst_manual_gap_reject_and_fold_first(self):
        with self.assertRaisesRegex(ValueError, 'отсутствует'):
            parse_local_datetime('2026-03-29T02:30', 'Europe/Berlin')
        self.assertEqual(parse_local_datetime('2026-10-25T02:30', 'Europe/Berlin'), stamp('2026-10-25T00:30+00:00'))
        value = settings(start_date='2026-03-28', time='02:30', timezone='Europe/Berlin')
        self.assertEqual(next_allowed(value, stamp('2026-03-29T00:00+00:00')), stamp('2026-03-30T00:30+00:00'))
        first = next_allowed(value, stamp('2026-03-28T00:00+00:00'))
        # Twenty-four elapsed hours do not become a 23-hour spring interval.
        second = next_allowed(value, first + 86400)
        self.assertGreaterEqual(second - first, 86400)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'plans.sqlite3'
        self.now = stamp('2026-11-04T09:00+00:00')
        self.launched = []
        self.busy = False
        def launch(batch):
            if self.busy:
                return False
            self.launched.append(self.store.begin(batch['id']))
            return True
        self.launch = launch
        self.store = PlanStore(self.path, clock=lambda: self.now, launch=launch)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def build(self, pairs, **changes):
        inventory = InventoryFixture(pairs)
        plan = self.store.create(inventory.id, '/archive', 'Архив', settings(**changes), '/Фото')
        plan = self.store.build(plan['id'], inventory)
        return plan, inventory

    def activate(self, plan):
        self.store.activate(plan['id'], plan['inventory_revision'])
        batches = self.store.batches(plan['id'], limit=200)['batches']
        for index, batch in enumerate(batches):
            self.store.bind_source(batch['id'], index + 1)
        self.store.commit_activation(plan['id'])
        return self.store.batches(plan['id'], limit=200)['batches']

    def test_tolerance_uses_both_limits_and_oversize_has_own_batch(self):
        plan, _ = self.build([(10, 100), (1, 1), (2, 15)], tolerance_percent=15)
        self.assertEqual(plan['batch_count'], 2)
        for batch in self.store.batches(plan['id'])['batches']:
            self.assertLessEqual(batch['file_count'], 11)
            self.assertLessEqual(batch['total_bytes'], 115)
        plan, _ = self.build([(150000, 1000000), (5, 20), (5, 20)])
        batches = self.store.batches(plan['id'])['batches']
        oversized = next(batch for batch in batches if batch['oversized'])
        self.assertEqual(oversized['member_count'], 1)
        self.assertEqual(len(batches), 2)
        self.assertEqual(plan['file_count'], 150010)
        self.assertEqual(plan['total_bytes'], 1000040)
        self.assertTrue(plan['warnings'])

    def test_tolerance_avoids_small_tail(self):
        plan, _ = self.build([(10, 100), (1, 5)], tolerance_percent=15)
        self.assertEqual(plan['batch_count'], 1)
        plan, _ = self.build([(10, 100), (1, 5)], tolerance_percent=0)
        self.assertEqual(plan['batch_count'], 2)

    def test_membership_deterministic_and_balancing_preserves_caps(self):
        fixture = [(4, 20), (4, 20), (3, 50), (2, 20), (2, 20), (1, 10)]
        outputs = []
        for _ in range(2):
            plan, _ = self.build(fixture)
            batches = self.store.batches(plan['id'])['batches']
            outputs.append([sorted(m['path'] for m in self.store.iter_members(b['id'])) for b in batches])
            self.assertEqual(sum(b['file_count'] for b in batches), sum(p[0] for p in fixture))
            self.assertEqual(sum(b['member_count'] for b in batches), len(fixture))
            self.assertTrue(all(b['file_count'] <= 10 and b['total_bytes'] <= 100 for b in batches))
        self.assertEqual(outputs[0], outputs[1])

    def test_balances_without_increasing_count(self):
        plan, _ = self.build([(1, 10)] * 11)
        batches = self.store.batches(plan['id'])['batches']
        self.assertEqual(len(batches), 2)
        self.assertEqual(sorted(b['file_count'] for b in batches), [5, 6])

    def test_atomic_rebuild_and_cancel_leave_previous_preview_unactivatable(self):
        plan, inventory = self.build([(5, 20), (5, 20)])
        old_ids = [row['id'] for row in self.store.batches(plan['id'])['batches']]
        stop = threading.Event()
        stop.set()
        with self.assertRaises(Cancelled):
            self.store.build(plan['id'], inventory, stop=stop)
        self.assertEqual([row['id'] for row in self.store.batches(plan['id'])['batches']], old_ids)
        self.assertIsNone(self.store.plan(plan['id'])['inventory_revision'])
        with self.assertRaises(ValueError):
            self.store.activate(plan['id'], inventory.revision)
        rebuilt = self.store.build(plan['id'], inventory)
        self.assertNotEqual([row['id'] for row in self.store.batches(plan['id'])['batches']], old_ids)
        self.assertEqual(rebuilt['inventory_revision'], inventory.revision)

    def test_restart_during_rebuild_keeps_previous_preview_but_requires_rebuild(self):
        plan, _ = self.build([(10, 100)])
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE plans SET status='building',building_generation=? WHERE id=?", ('a' * 32, plan['id']))
        self.store.close()
        self.store = PlanStore(self.path, clock=lambda: self.now)
        recovered = self.store.plan(plan['id'])
        self.assertEqual(recovered['status'], 'draft')
        self.assertIsNone(recovered['inventory_revision'])
        self.assertEqual(recovered['batch_count'], 1)
        with self.assertRaises(ValueError):
            self.store.activate(plan['id'])

    def test_revision_and_max_batch_guard(self):
        plan, inventory = self.build([(10, 100)])
        with self.assertRaisesRegex(ValueError, 'выбор'):
            self.store.activate(plan['id'], inventory.revision + 1)
        self.store.invalidate_inventory(inventory.id, inventory.revision + 1)
        self.assertIsNone(self.store.plan(plan['id'])['inventory_revision'])
        with patch('mailru_photos.plans.MAX_BATCHES', 2), self.assertRaisesRegex(ValueError, 'Увеличьте'):
            self.build([(10, 100)] * 3)

    def test_activation_bindings_survive_restart_and_are_idempotent(self):
        plan, _ = self.build([(10, 100)] * 2)
        self.store.activate(plan['id'], plan['inventory_revision'])
        batches = self.store.batches(plan['id'])['batches']
        self.store.bind_source(batches[0]['id'], 45)
        self.store.close()
        self.store = PlanStore(self.path, clock=lambda: self.now, launch=self.launch)
        self.assertEqual(self.store.plan(plan['id'])['status'], 'activating')
        self.store.bind_source(batches[0]['id'], 45)
        with self.assertRaises(ValueError):
            self.store.commit_activation(plan['id'])
        self.store.bind_source(batches[1]['id'], 46)
        self.store.commit_activation(plan['id'])
        self.assertEqual(self.store.plan(plan['id'])['status'], 'active')

    def test_thousand_bindings_refresh_only_once_per_bounded_page(self):
        plan, _ = self.build([(10, 100)] * 1000)
        self.store.activate(plan['id'], plan['inventory_revision'])
        with patch.object(self.store, '_refresh', wraps=self.store._refresh) as refresh:
            for offset in range(0, 1000, 200):
                rows = self.store.batches(plan['id'], offset=offset, limit=200)['batches']
                result = self.store.bind_sources(plan['id'], [
                    {'batch_id': row['id'], 'source_id': offset + index + 1} for index, row in enumerate(rows)])
                self.assertEqual(result['bound'], 200)
            self.assertEqual(refresh.call_count, 5)
        self.store.commit_activation(plan['id'])
        self.assertEqual(self.store.plan(plan['id'])['status'], 'active')
        with self.store._connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM batches WHERE plan_id=? AND source_id IS NOT NULL',
                                        (plan['id'],)).fetchone()[0], 1000)

    def test_bulk_binding_replay_and_conflict_rollback_are_atomic(self):
        plan, _ = self.build([(10, 100)] * 4)
        self.store.activate(plan['id'], plan['inventory_revision'])
        rows = self.store.batches(plan['id'])['batches']
        first = [{'batch_id': rows[0]['id'], 'source_id': 10}, {'batch_id': rows[1]['id'], 'source_id': 11}]
        self.store.bind_sources(plan['id'], first)
        self.store.bind_sources(plan['id'], list(reversed(first)))
        # Validate every binding before writing any part of the page.
        with self.assertRaises(ValueError):
            self.store.bind_sources(plan['id'], [{'batch_id': rows[2]['id'], 'source_id': 12},
                                                 {'batch_id': rows[1]['id'], 'source_id': 90}])
        self.assertIsNone(self.store.batch(rows[2]['id'])['source_id'])
        self.assertEqual(self.store.batch(rows[1]['id'])['source_id'], 11)
        # A database uniqueness conflict on the second UPDATE also rolls back
        # the earlier successful UPDATE in the same executemany transaction.
        with self.assertRaises(ValueError):
            self.store.bind_sources(plan['id'], [{'batch_id': rows[2]['id'], 'source_id': 12},
                                                 {'batch_id': rows[3]['id'], 'source_id': 10}])
        self.assertIsNone(self.store.batch(rows[2]['id'])['source_id'])
        self.assertIsNone(self.store.batch(rows[3]['id'])['source_id'])
        self.store.bind_sources(plan['id'], [{'batch_id': rows[2]['id'], 'source_id': 12},
                                             {'batch_id': rows[3]['id'], 'source_id': 13}])
        self.store.commit_activation(plan['id'])
        self.store.bind_sources(plan['id'], first)
        self.assertEqual(self.store.plan(plan['id'])['status'], 'active')

    def test_bulk_binding_rejects_cross_plan_duplicate_and_unbounded_pages(self):
        first, _ = self.build([(10, 100)])
        second, _ = self.build([(10, 100)])
        self.store.activate(first['id'], first['inventory_revision'])
        self.store.activate(second['id'], second['inventory_revision'])
        batch = self.store.batches(first['id'])['batches'][0]
        other = self.store.batches(second['id'])['batches'][0]
        cases = ([], [{'batch_id': batch['id'], 'source_id': 1}] * 201,
                 [{'batch_id': batch['id'], 'source_id': 1}] * 2,
                 [{'batch_id': batch['id'], 'source_id': True}],
                 [{'batch_id': other['id'], 'source_id': 1}])
        for bindings in cases:
            with self.subTest(bindings_count=len(bindings)), self.assertRaises(ValueError):
                self.store.bind_sources(first['id'], bindings)
        self.assertIsNone(self.store.batch(batch['id'])['source_id'])
        self.assertIsNone(self.store.batch(other['id'])['source_id'])

    def test_one_running_globally_pause_blocks_and_error_completion_advances(self):
        plan, _ = self.build([(10, 100)] * 3)
        batches = self.activate(plan)
        self.now = batches[0]['scheduled_for']
        self.assertTrue(self.store.tick())
        first = self.store.batch(batches[0]['id'])
        with self.assertRaises(ValueError):
            self.store.begin(batches[1]['id'], manual=True)
        self.store.finish(first['id'], 'paused', {'total': 10, 'uploaded': 3})
        self.now += 10 * 86400
        self.assertFalse(self.store.tick())
        resumed = self.store.begin(first['id'], manual=True)
        self.assertEqual(resumed['run_key'], first['run_key'])
        self.assertEqual(resumed['started_at'], first['started_at'])
        self.assertEqual(resumed['attempt'], 2)
        self.store.finish(first['id'], 'completed_with_errors', {'failed': 1})
        next_batch = self.store.batch(batches[1]['id'])
        self.assertGreaterEqual(next_batch['scheduled_for'], self.now)
        self.now = next_batch['scheduled_for']
        self.assertTrue(self.store.tick())
        self.assertEqual(self.launched[-1]['id'], next_batch['id'])

    def test_overrun_reschedules_allowed_day_and_minimum_actual_start_gap(self):
        plan, _ = self.build([(10, 100)] * 3, interval_days=2, weekdays=[3, 5])
        batches = self.activate(plan)
        self.now = batches[0]['scheduled_for'] + 30 * 60
        first = self.store.begin(batches[0]['id'], manual=True)
        second = self.store.batch(batches[1]['id'])
        # Friday 10:00 is only 47.5h after actual Wednesday 10:30.
        self.assertEqual(second['scheduled_for'], stamp('2026-11-11T10:00+00:00'))
        self.now = stamp('2026-11-13T15:00+00:00')
        self.store.finish(first['id'], 'completed')
        second = self.store.batch(batches[1]['id'])
        self.assertEqual(second['scheduled_for'], stamp('2026-11-18T10:00+00:00'))

    def test_subminute_jitter_preserves_daily_slots_and_strict_elapsed_interval(self):
        plan, _ = self.build([(10, 100)] * 2)
        batches = self.activate(plan)
        self.now = batches[0]['scheduled_for'] + 0.01
        first = self.store.begin(batches[0]['id'])
        second = self.store.batch(batches[1]['id'])
        self.assertAlmostEqual(second['scheduled_for'], first['started_at'] + 86400)
        self.assertEqual(datetime.fromtimestamp(second['scheduled_for']).minute, 0)

    def test_missed_weekend_slot_restarts_next_weekend_and_busy_wakeup_replans(self):
        plan, _ = self.build([(10, 100)], weekdays=[6, 7])
        batch = self.activate(plan)[0]
        self.now = stamp('2026-11-09T09:00+00:00')
        self.store.close()
        self.store = PlanStore(self.path, clock=lambda: self.now, launch=self.launch)
        self.assertEqual(self.store.batch(batch['id'])['scheduled_for'], stamp('2026-11-14T10:00+00:00'))
        self.assertFalse(self.store.tick())
        self.now = stamp('2026-11-14T10:00+00:00')
        self.busy = True
        self.assertFalse(self.store.tick())
        self.now += 120
        self.busy = False
        self.assertFalse(self.store.tick())
        self.assertEqual(self.store.batch(batch['id'])['scheduled_for'], stamp('2026-11-15T10:00+00:00'))
        self.assertEqual(self.launched, [])

    def test_manual_pin_has_its_minute_window_and_reverts_to_calendar_after_miss(self):
        plan, _ = self.build([(10, 100)] * 2)
        batches = self.activate(plan)
        pin = stamp('2026-11-04T15:30+00:00')
        self.store.set_batch_time(batches[0]['id'], pin)
        self.now = pin + 10
        self.assertTrue(self.store.tick())
        self.assertEqual(self.launched[0]['started_at'], self.now)
        self.store.finish(batches[0]['id'], 'completed')
        pin2 = stamp('2026-11-06T14:15+00:00')
        self.store.set_batch_time(batches[1]['id'], pin2)
        self.now = pin2 + 120
        self.assertFalse(self.store.tick())
        self.assertEqual(self.store.batch(batches[1]['id'])['scheduled_for'], stamp('2026-11-07T10:00+00:00'))

    def test_manual_out_of_order_promotes_ahead_of_paused_and_preserves_dates(self):
        plan, _ = self.build([(10, 100)] * 3)
        batches = self.activate(plan)
        self.store.pause(batches[0]['id'])
        running = self.store.begin(batches[2]['id'], manual=True)
        self.assertEqual(running['position'], 0)
        self.store.finish(running['id'], 'completed')
        self.assertFalse(self.store.tick())
        self.assertEqual(self.store.plan(plan['id'])['status'], 'paused')

    def test_date_override_reorder_move_delete_recalculate(self):
        plan, _ = self.build([(10, 100)] * 3)
        batches = self.activate(plan)
        moved = self.store.set_batch_time(batches[1]['id'], stamp('2026-11-10T15:00+00:00'))
        self.assertEqual(moved['scheduled_for'], stamp('2026-11-10T15:00+00:00'))
        self.assertEqual(self.store.batch(batches[2]['id'])['scheduled_for'], stamp('2026-11-12T10:00+00:00'))
        with self.assertRaises(ValueError):
            self.store.set_batch_time(batches[1]['id'], batches[0]['scheduled_for'])
        self.store.move_batch(batches[2]['id'], direction='up')
        ids = [row['id'] for row in self.store.batches(plan['id'])['batches']]
        self.assertEqual(ids, [batches[0]['id'], batches[2]['id'], batches[1]['id']])
        self.assertIsNone(self.store.batch(batches[1]['id'])['requested_for'])
        self.store.remove_batch(batches[2]['id'])
        self.assertEqual(self.store.plan(plan['id'])['batch_count'], 2)
        self.assertEqual(self.store.deleted_bindings()['total'], 1)
        self.store.ack_deleted_binding(batches[2]['id'])
        self.assertEqual(self.store.deleted_bindings()['total'], 0)

    def test_restart_only_interrupted_running_is_recoverable_manual_pause_persists(self):
        plan, _ = self.build([(10, 100)] * 2)
        batches = self.activate(plan)
        first = self.store.begin(batches[0]['id'], manual=True)
        self.store.pause(batches[1]['id'])
        self.store.close()
        self.store = PlanStore(self.path, clock=lambda: self.now, launch=self.launch)
        recovery = self.store.recoverable()['batches']
        self.assertEqual([b['id'] for b in recovery], [first['id']])
        self.assertEqual(recovery[0]['run_key'], first['run_key'])
        self.assertEqual(recovery[0]['started_at'], first['started_at'])
        self.assertFalse(self.store.tick())
        self.assertEqual(self.store.batch(batches[1]['id'])['status'], 'paused')

    def test_completion_event_durable_and_deduplicated_after_pause_and_restart(self):
        plan, _ = self.build([(10, 100)])
        batch = self.activate(plan)[0]
        self.store.begin(batch['id'], manual=True)
        self.store.finish(batch['id'], 'completed', {'total': 10, 'uploaded': 10})
        self.store.finish(batch['id'], 'completed', {'total': 10, 'uploaded': 10})
        self.assertEqual(self.store.plan(plan['id'])['status'], 'completed')
        events = self.store.completed_events()
        self.assertEqual(len(events), 1)
        self.store.close()
        self.store = PlanStore(self.path, clock=lambda: self.now)
        self.assertEqual(self.store.completed_events()[0]['key'], events[0]['key'])
        self.store.ack_event(events[0]['key'])
        self.assertEqual(self.store.completed_events(), [])

    def test_retry_terminal_is_manual_new_cycle_and_keeps_other_finished_history(self):
        plan, _ = self.build([(10, 100)] * 3)
        batches = self.activate(plan)
        self.store.begin(batches[0]['id'], manual=True)
        first = self.store.finish(batches[0]['id'], 'completed_with_errors', {'failed': 1})
        self.now += 86400
        self.store.begin(batches[1]['id'], manual=True)
        second = self.store.finish(batches[1]['id'], 'completed')
        old_second = self.store.batch(second['id'])
        with self.assertRaises(ValueError):
            self.store.begin(first['id'])
        with self.assertRaises(ValueError):
            self.store.begin(first['id'], retry=True)
        self.now += 5 * 86400
        reopened = self.store.begin(first['id'], manual=True, retry=True)
        self.assertNotEqual(reopened['run_key'], first['run_key'])
        self.assertEqual(reopened['started_at'], self.now)
        self.assertIsNone(reopened['finished_at'])
        self.assertEqual(self.store.plan(plan['id'])['completion_cycle'], 1)
        self.assertEqual(self.store.batch(second['id']), old_second)
        pending = self.store.batch(batches[2]['id'])
        self.assertGreaterEqual(pending['scheduled_for'], self.now + 86400)
        self.store.finish(first['id'], 'completed', {'failed': 0})
        events = self.store.completed_events()
        self.assertEqual(len([event for event in events if event['batch_id'] == first['id']]), 2)
        self.assertEqual({event['plan']['completion_cycle'] for event in events}, {0, 1})

    def test_retry_promotes_terminal_batch_without_duplicate_positions(self):
        plan, _ = self.build([(10, 100)] * 3)
        batches = self.activate(plan)
        self.store.begin(batches[2]['id'], manual=True)
        self.store.finish(batches[2]['id'], 'completed_with_errors', {'failed': 1})
        self.store.begin(batches[0]['id'], manual=True)
        self.store.finish(batches[0]['id'], 'completed_with_errors', {'failed': 1})
        self.store.begin(batches[0]['id'], manual=True, retry=True)
        result = self.store.batches(plan['id'])['batches']
        self.assertEqual(sorted(row['position'] for row in result), [0, 1, 2])
        self.assertEqual(self.store.batch(batches[1]['id'])['status'], 'queued')

    def test_member_limit_survives_both_packing_passes(self):
        with patch('mailru_photos.plans.MAX_BATCH_MEMBERS', 3):
            plan, _ = self.build([(1, 1)] * 7, target_count=100, target_bytes=1000)
        batches = self.store.batches(plan['id'])['batches']
        self.assertEqual(len(batches), 3)
        self.assertEqual(sum(b['member_count'] for b in batches), 7)
        self.assertTrue(all(b['member_count'] <= 3 for b in batches))

    def test_failed_activation_can_return_to_draft_or_be_removed(self):
        plan, _ = self.build([(10, 100)] * 2)
        self.store.activate(plan['id'], plan['inventory_revision'])
        batches = self.store.batches(plan['id'])['batches']
        self.store.bind_source(batches[0]['id'], 15)
        # The service has removed its owned groups before this durable reset.
        result = self.store.abort_activation(plan['id'], 'Исходная папка уже подключена.')
        self.assertEqual(result['status'], 'draft')
        self.assertTrue(all(b['source_id'] is None for b in self.store.batches(plan['id'])['batches']))
        self.store.activate(plan['id'], plan['inventory_revision'])
        deleted = self.store.remove_plan(plan['id'])
        self.assertEqual(deleted['id'], plan['id'])
        self.assertEqual(self.store.list()['total'], 0)

    def test_activation_rollback_intent_survives_restart_without_reactivation(self):
        plan, _ = self.build([(10, 100)] * 2)
        self.store.activate(plan['id'], plan['inventory_revision'])
        batches = self.store.batches(plan['id'])['batches']
        self.store.bind_source(batches[0]['id'], 45)
        self.store.begin_activation_rollback(plan['id'], 'Пользователь остановил активацию.')
        self.assertEqual(self.store.plan(plan['id'])['status'], 'rolling_back')
        # The controller may already have removed group 45 when it crashes.
        # The old binding is intentionally retained until cleanup is complete.
        self.store.close()
        self.now += 100 * 86400
        self.store = PlanStore(self.path, clock=lambda: self.now, launch=self.launch)
        self.assertEqual(self.store.list()['plans'][0]['status'], 'rolling_back')
        self.assertEqual(self.store.batch(batches[0]['id'])['source_id'], 45)
        self.assertFalse(self.store.tick())
        self.assertEqual(self.launched, [])
        with self.assertRaises(ValueError):
            self.store.activate(plan['id'], plan['inventory_revision'])
        with self.assertRaises(ValueError):
            self.store.commit_activation(plan['id'])
        with self.assertRaises(ValueError):
            self.store.bind_source(batches[0]['id'], 46)
        self.store.begin_activation_rollback(plan['id'], 'Продолжаем отмену после перезапуска.')
        result = self.store.abort_activation(plan['id'], 'Исходные папки отключены.')
        self.assertEqual(result['status'], 'draft')
        self.assertTrue(all(row['source_id'] is None for row in self.store.batches(plan['id'])['batches']))
        # A later explicit activation may create fresh, different source IDs.
        self.store.activate(plan['id'], plan['inventory_revision'])
        self.store.bind_sources(plan['id'], [{'batch_id': row['id'], 'source_id': 46 + index}
                                             for index, row in enumerate(batches)])
        self.store.commit_activation(plan['id'])
        self.assertEqual(self.store.plan(plan['id'])['status'], 'active')

    def test_activation_rollback_cannot_remove_an_active_plan(self):
        plan, _ = self.build([(10, 100)])
        self.activate(plan)
        with self.assertRaises(ValueError):
            self.store.begin_activation_rollback(plan['id'])
        self.assertEqual(self.store.plan(plan['id'])['status'], 'active')

    def test_controller_busy_then_wakeup_does_not_duplicate_a_claim(self):
        plan, _ = self.build([(10, 100)])
        batch = self.activate(plan)[0]
        self.now = batch['scheduled_for']
        self.busy = True
        self.assertFalse(self.store.tick())
        self.assertEqual(self.store.batch(batch['id'])['status'], 'queued')
        self.busy = False
        self.store.wake()
        self.assertTrue(self.store.tick())
        self.assertFalse(self.store.tick())
        self.assertEqual(len(self.launched), 1)

    def test_idle_summary_list_cached_pages_and_timer_do_not_touch_database(self):
        plan, _ = self.build([(10, 100)])
        self.activate(plan)
        self.store.batches(plan['id'])
        with patch.object(self.store, '_connect', side_effect=AssertionError('idle database read')):
            self.assertFalse(self.store.tick())
            self.assertEqual(self.store.summary()['active'], 1)
            self.assertEqual(self.store.list()['total'], 1)
            self.assertEqual(self.store.plan(plan['id'])['name'], 'Архив')
            self.assertEqual(self.store.batches(plan['id'])['total'], 1)

    def test_cleanup_state_and_running_removal_guard(self):
        plan, _ = self.build([(10, 100)] * 2)
        batches = self.activate(plan)
        self.store.begin(batches[0]['id'], manual=True)
        with self.assertRaises(ValueError):
            self.store.remove_batch(batches[0]['id'])
        with self.assertRaises(ValueError):
            self.store.remove_plan(plan['id'])
        self.store.pause(batches[0]['id'])
        self.store.begin_cleanup(batches[0]['id'])
        with self.assertRaises(ValueError):
            self.store.begin(batches[1]['id'], manual=True)
        self.assertFalse(self.store.tick())
        self.store.finish_cleanup(batches[0]['id'])
        self.assertEqual(self.store.batch(batches[0]['id'])['status'], 'paused')
        deleted = self.store.remove_plan(plan['id'])
        self.assertEqual(len(deleted['bindings']), 2)
        self.assertEqual(self.store.list()['total'], 0)


if __name__ == '__main__':
    unittest.main()

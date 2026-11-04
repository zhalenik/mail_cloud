"""Real directory walks, aggregate exclusions and bounded, restartable previews."""
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos.plan_inventory import PlanInventory


class PlanInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'Фотографии'
        self.root.mkdir()
        self.inventory = PlanInventory(Path(self.tmp.name) / 'inventory.sqlite3')

    def tearDown(self):
        self.inventory.close()
        self.tmp.cleanup()

    def file(self, path, size):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'x' * size)
        return target

    def scan(self):
        state = self.inventory.create(self.root)
        return self.inventory.scan(state['id'])

    def find(self, identifier, relative):
        with self.inventory._connect() as db:
            return db.execute('SELECT id FROM directories WHERE inventory_id=? AND relative_path=?', (identifier, relative)).fetchone()[0]

    def test_nested_aggregates_no_hashes_and_units_preserve_root_structure(self):
        self.file('корень.JPG', 3)
        self.file('Альбом/один.jpg', 7)
        self.file('Альбом/Подпапка/два.MP4', 11)
        self.file('Другое/три.png', 13)
        self.file('ignored.txt', 123)
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('contents must not be read')):
            state = self.scan()
        self.assertEqual(state['selected_count'], 4)
        self.assertEqual(state['total_bytes'], 34)
        self.assertEqual(state['ignored_files'], 1)
        self.assertEqual(state['directory_count'], 4)
        units = list(self.inventory.iter_units(state['id']))
        self.assertEqual(len(units), 3)
        direct = next(unit for unit in units if not unit['recursive'])
        self.assertEqual((direct['file_count'], direct['total_bytes'], direct['remote_subdir']), (1, 3, ''))
        album = next(unit for unit in units if unit['relative_path'] == 'Альбом')
        self.assertEqual((album['file_count'], album['total_bytes']), (2, 18))
        self.assertEqual(album['path'], str(self.root / 'Альбом'))
        root_page = self.inventory.tree(state['id'])
        self.assertEqual(root_page['total'], 1)
        children = self.inventory.tree(state['id'], state['root_id'], limit=1)
        self.assertEqual(len(children['folders']), 1)
        self.assertEqual(children['total'], 2)

    def test_nested_exclusions_deltas_and_restore_parent_retain_child_choice(self):
        self.file('a/one.jpg', 10)
        self.file('a/b/two.jpg', 20)
        self.file('a/b/c/three.jpg', 30)
        self.file('a/z/four.jpg', 40)
        state = self.scan()
        aid, bid, cid = [self.find(state['id'], path) for path in ('a', 'a/b', 'a/b/c')]
        state = self.inventory.exclude(state['id'], cid, True)
        self.assertEqual((state['selected_count'], state['selected_bytes']), (3, 70))
        state = self.inventory.exclude(state['id'], bid, True)
        self.assertEqual((state['selected_count'], state['selected_bytes']), (2, 50))
        unit = list(self.inventory.iter_units(state['id']))[0]
        self.assertEqual(unit['excluded_paths'], ['b'])
        self.inventory.exclude(state['id'], aid, True)
        self.assertEqual(self.inventory.summary(state['id'])['selected_count'], 0)
        with self.assertRaisesRegex(ValueError, 'родительскую'):
            self.inventory.exclude(state['id'], cid, False)
        state = self.inventory.exclude(state['id'], aid, False)
        self.assertEqual(state['selected_bytes'], 50)
        state = self.inventory.exclude(state['id'], bid, False)
        self.assertEqual(state['selected_bytes'], 70)
        self.assertEqual(list(self.inventory.iter_units(state['id']))[0]['excluded_paths'], ['b/c'])
        state = self.inventory.exclude(state['id'], cid, False)
        self.assertEqual(state['selected_bytes'], 100)

    def test_symlink_ignored_and_cyrillic_walk_supported(self):
        self.file('обычные/пейзаж.jpg', 5)
        (self.root / 'псевдо').symlink_to(self.root / 'обычные', target_is_directory=True)
        (self.root / 'ссылка.jpg').symlink_to(self.root / 'обычные/пейзаж.jpg')
        state = self.scan()
        self.assertEqual(state['selected_count'], 1)
        self.assertEqual(state['ignored_symlinks'], 2)
        with self.assertRaises(ValueError):
            self.inventory.create(self.root / 'псевдо')

    def test_cancelled_inventory_cannot_build_and_rescan_is_fresh(self):
        self.file('a/one.jpg', 1)
        state = self.inventory.create(self.root)
        stop = threading.Event()
        stop.set()
        cancelled = self.inventory.scan(state['id'], stop=stop)
        self.assertEqual(cancelled['status'], 'cancelled')
        with self.assertRaises(ValueError):
            list(self.inventory.iter_units(state['id']))
        self.inventory.scan(state['id'])
        (self.root / 'a/one.jpg').unlink()
        self.file('b/two.jpg', 5)
        rescanned = self.inventory.scan(state['id'])
        self.assertEqual((rescanned['selected_count'], rescanned['selected_bytes']), (1, 5))
        self.assertEqual([unit['remote_subdir'] for unit in self.inventory.iter_units(state['id'])], ['b'])

    def test_errors_block_only_selected_subtree_and_cache_no_idle_io(self):
        self.file('good/ok.jpg', 4)
        self.file('bad/no.jpg', 7)
        original = self.inventory._entries
        def entries(fd, directory):
            if directory['relative_path'] == 'bad':
                raise PermissionError('denied')
            return original(fd, directory)
        with patch.object(self.inventory, '_entries', side_effect=entries):
            state = self.scan()
        self.assertEqual(state['status'], 'ready')
        self.assertEqual(state['selected_errors'], 1)
        with self.assertRaises(ValueError):
            list(self.inventory.iter_units(state['id']))
        bad = self.find(state['id'], 'bad')
        selected = self.inventory.exclude(state['id'], bad, True)
        self.assertEqual(selected['selected_errors'], 0)
        self.assertEqual(len(list(self.inventory.iter_units(state['id']))), 1)
        with patch.object(self.inventory, '_connect', side_effect=AssertionError('idle read')):
            self.assertEqual(self.inventory.summary(state['id'])['selected_count'], 1)
            self.assertEqual(self.inventory.list()['total'], 1)

    def test_source_exclusions_limit_rejects_unactivatable_preview(self):
        self.file('album/keep.jpg', 1)
        for name in ('a', 'b', 'c'):
            self.file(f'album/{name}/skip.jpg', 1)
        state = self.scan()
        for name in ('a', 'b', 'c'):
            self.inventory.exclude(state['id'], self.find(state['id'], 'album/' + name), True)
        with patch('mailru_photos.plan_inventory._MAX_EXCLUSIONS', 2), self.assertRaisesRegex(ValueError, 'исключений'):
            list(self.inventory.iter_units(state['id']))

    def test_crash_marks_partial_inventory_incomplete_and_bounds_page_sizes(self):
        state = self.inventory.create(self.root)
        with sqlite3.connect(self.inventory.path) as db:
            db.execute("UPDATE inventories SET status='scanning' WHERE id=?", (state['id'],))
        reopened = PlanInventory(self.inventory.path)
        self.assertEqual(reopened.summary(state['id'])['status'], 'incomplete')
        with self.assertRaises(ValueError):
            reopened.tree(state['id'], limit=201)
        with self.assertRaises(ValueError):
            reopened.list(limit=101)


if __name__ == '__main__':
    unittest.main()

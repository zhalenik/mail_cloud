"""Queue downloads use bounded reads and one coherent, read-only snapshot."""
import csv
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
from urllib.parse import parse_qs

from mailru_photos.export import QueueExportResponse, options
from mailru_photos.queue import Queue
import test_web as web_fixtures


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.source = self.base / 'Фотографии'
        self.source.mkdir()
        self.path = self.base / 'queue.sqlite3'
        self.queue = Queue(self.path, destination='/Archive', account_id='offline-test')
        self.queue.add_root(self.source, 'Camera')

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def scan(self, names):
        for name in names:
            (self.source / name).write_bytes(b'export-fixture')
        self.queue.scan()

    def response(self, query='', *, head=False):
        return QueueExportResponse(Queue(self.path, destination='/Archive'), options(parse_qs(query, keep_blank_values=True)), head=head)

    def test_unicode_quotes_newlines_and_formula_names_roundtrip_all_delimiters(self):
        names = ['Семья; "июль".jpg', 'с пробелами.jpg', 'строка\nдва.jpg', '=1+1.jpg', '+SUM(1).jpg', '@name.jpg']
        self.scan(names)
        for fmt in ('txt', 'csv'):
            for name, delimiter in (('comma', ','), ('space', ' '), ('tab', '\t'), ('semicolon', ';')):
                with self.subTest(format=fmt, delimiter=name):
                    response = self.response(f'format={fmt}&delimiter={name}&fields=name,size&sort_by=name&sort=asc')
                    data = b''.join(response)
                    self.assertTrue(response.closed)
                    self.assertEqual(data.startswith(b'\xef\xbb\xbf'), fmt == 'csv')
                    rows = list(csv.reader(io.StringIO(data.decode('utf-8-sig'), newline=''), delimiter=delimiter))
                    self.assertEqual(rows[0], ['Имя файла', 'Размер, байт'])
                    expected = {("'" + name if name.startswith(('=', '+', '@')) else name) for name in names}
                    self.assertEqual({row[0] for row in rows[1:]}, expected)
                    self.assertEqual({row[1] for row in rows[1:]}, {'14'})

    def test_invalid_parameters_are_rejected_before_opening_download(self):
        for query in ('fields=', 'fields=name,name', 'fields=token', 'status=existing', 'format=json',
                      'delimiter=pipe', 'header=yes', 'sort=ascending', 'sort_by=sql', 'unknown=1',
                      'sort=asc&sort=desc', 'fields=name&fields=path'):
            with self.subTest(query=query), self.assertRaises(ValueError):
                options(parse_qs(query, keep_blank_values=True))

    def test_header_free_txt_and_empty_csv_are_valid(self):
        self.assertEqual(b''.join(self.response('format=txt&header=0&fields=name')), b'')
        self.assertEqual(b''.join(self.response('format=csv&header=0&fields=name')), b'\xef\xbb\xbf')
        self.scan(['one.jpg'])
        self.assertEqual(b''.join(self.response('format=txt&header=0&fields=name')), b'one.jpg\r\n')

    def test_snapshot_survives_fresh_reset_while_new_jobs_remain_writable(self):
        self.scan(['old.jpg'])
        response = self.response('fields=name&header=0&format=txt')
        # A new scan can replace current tasks while the download is open.
        # The already-started export must still contain its original snapshot.
        (self.source / 'old.jpg').unlink()
        (self.source / 'new.jpg').write_bytes(b'new')
        self.queue.reset_for_scan()
        self.queue.scan()
        self.assertEqual(b''.join(response), b'old.jpg\r\n')
        self.assertEqual(b''.join(self.response('fields=name&header=0&format=txt')), b'new.jpg\r\n')

    def test_filters_include_observed_quota_errors_but_not_existing_in_skips(self):
        self.scan(['a.jpg', 'b.jpg', 'c.jpg', 'd.jpg'])
        rows = self.queue.list_jobs(sort_by='name')
        self.queue._db.execute("UPDATE jobs SET status='failed',cloud_present=1,observed_mode='regular',cloud_path=remote_path WHERE id=?", (rows[0]['id'],))
        self.queue._db.execute("UPDATE jobs SET status='skipped',skip_reason='remote_exists',cloud_present=1,observed_mode='unlimited',cloud_path='/Other/b.jpg' WHERE id=?", (rows[1]['id'],))
        self.queue.request_control(rows[2]['id'], 'skip')
        for status, expected in (('failed', 'a.jpg'), ('uploaded_regular', 'a.jpg'), ('uploaded_unlimited', ''), ('skipped', 'c.jpg')):
            with self.subTest(status=status):
                data = b''.join(self.response(f'format=txt&header=0&fields=name&status={status}')).decode()
                self.assertEqual(data, expected + '\r\n' if expected else '')
        statuses = b''.join(self.response('format=txt&header=0&fields=status')).decode()
        self.assertIn('другой путь: /Other/b.jpg', statuses)
        self.assertIn('Уже в Облаке; другой путь: /Other/b.jpg', statuses)
        self.assertNotIn('Уже в Облаке; безлимит', statuses)
        self.assertIn('Ошибка; в Облаке; квота', statuses)

    def test_head_and_disconnect_release_read_snapshot_and_connection(self):
        self.scan([f'{i:04}.jpg' for i in range(700)])
        head = self.response(head=True)
        self.assertEqual(list(head), [])
        self.assertTrue(head.closed)
        response = self.response('fields=name&header=0&format=txt')
        first = next(response)
        self.assertEqual(first.count(b'\r\n'), 250)
        self.assertFalse(response.closed)
        response.close()
        response.close()
        self.assertEqual(list(response), [])
        with self.assertRaises(sqlite3.ProgrammingError):
            response.queue._db.execute('SELECT 1')
        self.queue._db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.assertEqual(Path(str(self.path) + '-wal').stat().st_size, 0)


class ExportHTTPTests(unittest.TestCase):
    # Reuse only the request/session harness, without re-running its test class.
    setUp = web_fixtures.WebTests.setUp
    tearDown = web_fixtures.WebTests.tearDown
    request = web_fixtures.WebTests.request
    authenticate = web_fixtures.WebTests.authenticate

    def export(self, settings, *, head=False):
        queue = Queue(self.path / 'export.sqlite3', destination='/Archive')
        self.response = QueueExportResponse(queue, settings, head=head)
        return self.response

    def test_export_requires_session_and_enforces_host_but_no_write_csrf(self):
        self.service.export_jobs = self.export
        self.assertEqual(self.request('/api/jobs/export')['status'], 401)
        self.authenticate()
        denied = self.request('/api/jobs/export', host='attacker.invalid')
        self.assertEqual(denied['status'], 403)
        good = self.request('/api/jobs/export?fields=name&format=csv', csrf='')
        self.assertEqual(good['status'], 200)
        self.assertTrue(good['headers']['Content-Type'].startswith('text/csv'))
        self.assertIn('attachment;', good['headers']['Content-Disposition'])
        self.assertEqual(good['headers']['X-Content-Type-Options'], 'nosniff')
        self.assertEqual(good['headers']['Cache-Control'], 'no-store')
        self.assertNotIn('Content-Length', good['headers'])
        self.assertTrue(self.response.closed)
        self.assertEqual(good['body'].decode('utf-8-sig'), 'Имя файла\r\n')

    def test_bad_query_and_head_do_not_leave_download_connections(self):
        self.service.export_jobs = self.export
        self.authenticate()
        self.assertEqual(self.request('/api/jobs/export?fields=name,name')['status'], 400)
        head = self.request('/api/jobs/export', method='HEAD')
        self.assertEqual(head['status'], 200)
        self.assertEqual(head['body'], b'')
        self.assertTrue(self.response.closed)


if __name__ == '__main__':
    unittest.main()

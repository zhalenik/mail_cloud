"""Password persistence, session invalidation and stopped-server recovery."""
from __future__ import annotations

import builtins
import contextlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import Mock, patch

from mailru_photos.queue import QueueBusyError, _file_lock
from mailru_photos.web import PanelAccess, WebError, main, reset_panel_password


class PanelPasswordTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.state = Path(self.directory.name)
        self.path = self.state / 'web_access.json'
        self.access = PanelAccess(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def test_setup_write_failure_does_not_publish_password_or_epoch(self):
        before = self.path.read_bytes()
        memory = json.loads(json.dumps(self.access.data))
        with patch('mailru_photos.web.os.replace', side_effect=OSError('offline replace failure')):
            with self.assertRaises(OSError):
                self.access.setup('initial-offline-password')
        self.assertFalse(self.access.configured)
        self.assertEqual(self.access.data, memory)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(list(self.state.glob('.panel-*')), [])

    def test_changed_password_uses_private_file_and_rejects_old_password(self):
        self.access.setup('initial-offline-password')
        _, old_cookie = self.access.new_session(True, secure=False)
        self.access.change_password('initial-offline-password', 'replacement-offline-password', 'offline-peer')
        self.assertIsNone(self.access.read_session(old_cookie))
        reloaded = PanelAccess(self.path)
        reloaded.login('replacement-offline-password', 'offline-peer')
        with self.assertRaises(WebError) as caught:
            reloaded.login('initial-offline-password', 'offline-peer')
        self.assertEqual(caught.exception.status, 401)
        stored = self.path.read_text()
        self.assertNotIn('replacement-offline-password', stored)
        self.assertNotIn('initial-offline-password', stored)
        if os.name != 'nt':
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_offline_reset_preserves_account_and_queue_and_invalidates_sessions(self):
        self.access.setup('initial-offline-password')
        _, cookie = self.access.new_session(True, secure=False)
        preserved = {'auth.dat': b'offline-token-placeholder', 'queue.sqlite3': b'offline-queue-placeholder',
                     'project.json': b'{"version":1,"destination":"/Archive"}',
                     'web_settings.json': b'{"workers":4}'}
        for name, value in preserved.items():
            (self.state / name).write_bytes(value)
        reader = Mock(side_effect=['recovered-offline-password', 'recovered-offline-password'])
        reset_panel_password(self.state, read_password=reader)
        self.assertEqual(reader.call_count, 2)
        reloaded = PanelAccess(self.path)
        reloaded.login('recovered-offline-password', 'offline-peer')
        self.assertIsNone(reloaded.read_session(cookie))
        for name, value in preserved.items():
            self.assertEqual((self.state / name).read_bytes(), value, name)

    def test_offline_reset_refuses_running_server_before_prompting(self):
        self.access.setup('initial-offline-password')
        before = self.path.read_bytes()
        reader = Mock(return_value='replacement-offline-password')
        with _file_lock(self.state / 'web.run.lock'):
            with self.assertRaises(QueueBusyError):
                reset_panel_password(self.state, read_password=reader)
        reader.assert_not_called()
        self.assertEqual(self.path.read_bytes(), before)

    def test_mismatch_validation_and_io_failure_preserve_old_password(self):
        self.access.setup('initial-offline-password')
        before = self.path.read_bytes()
        for entries in (['first-offline-password', 'other-offline-password'], ['short', 'short']):
            with self.subTest(entries=entries), self.assertRaises(WebError):
                reset_panel_password(self.state, read_password=Mock(side_effect=entries))
            self.assertEqual(self.path.read_bytes(), before)
        with patch('mailru_photos.web.os.replace', side_effect=OSError('offline replace failure')):
            with self.assertRaises(OSError):
                reset_panel_password(self.state, read_password=Mock(return_value='replacement-offline-password'))
        self.assertEqual(self.path.read_bytes(), before)
        PanelAccess(self.path).login('initial-offline-password', 'offline-peer')

    def test_reset_cli_does_not_require_waitress_or_print_password(self):
        original_import = builtins.__import__
        def no_waitress(name, *args, **kwargs):
            if name == 'waitress':
                raise ImportError('Optional server dependency deliberately absent')
            return original_import(name, *args, **kwargs)
        output = io.StringIO()
        with patch('builtins.__import__', side_effect=no_waitress), \
             patch('mailru_photos.web.getpass.getpass', side_effect=['recovery-cli-password', 'recovery-cli-password']), \
             contextlib.redirect_stdout(output):
            code = main(['--state', str(self.state), '--reset-panel-password'])
        self.assertEqual(code, 0)
        self.assertNotIn('recovery-cli-password', output.getvalue())
        PanelAccess(self.path).login('recovery-cli-password', 'offline-peer')


if __name__ == '__main__':
    unittest.main()

"""Offline SMTP verification, outbox recovery and configuration races."""
from __future__ import annotations

import json
import os
from pathlib import Path
import smtplib
import sqlite3
import ssl
import tempfile
import threading
import unittest
from unittest.mock import patch

from mailru_photos.notifications import EVENTS, NotificationManager, validate_settings


SECRET = "unit-test-smtp-secret"


class FakeSMTP:
    def __init__(self, *, failure=None, rejected=None, login_entered=None, login_release=None):
        self.calls = []
        self.messages = []
        self.failure = failure
        self.rejected = rejected or {}
        self.login_entered, self.login_release = login_entered, login_release

    def ehlo(self):
        self.calls.append(("ehlo",))

    def starttls(self, *, context):
        self.calls.append(("tls", context.check_hostname, context.verify_mode))

    def login(self, username, password):
        self.calls.append(("login", username, password))
        if self.login_entered:
            self.login_entered.set()
        if self.login_release and not self.login_release.wait(3):
            raise AssertionError("Test did not release SMTP login")
        if isinstance(self.failure, smtplib.SMTPAuthenticationError):
            raise self.failure

    def send_message(self, message, *, from_addr, to_addrs):
        self.calls.append(("send", from_addr, tuple(to_addrs)))
        if self.failure:
            raise self.failure
        self.messages.append(message)
        return self.rejected

    def close(self):
        self.calls.append(("close",))


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name) / "private"
        self.now = 1000.0
        self.clients = []
        self.manager = NotificationManager(self.directory, clock=lambda: self.now, smtp_factory=self.factory)
        self.manager.save({"enabled": True, "username": "sender@example.com", "password": SECRET,
                           "from_email": "sender@example.com", "recipients": ["owner@example.com"]})

    def tearDown(self):
        self.manager.close(timeout=3)
        self.tmp.cleanup()

    def factory(self, config):
        client = FakeSMTP()
        self.clients.append(client)
        return client

    def rows(self):
        with sqlite3.connect(self.manager.database) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM mail_events ORDER BY id")]

    def enqueue(self, key="batch:1", event="batch_completed"):
        return self.manager.enqueue(key, event, {"plan_name": "Фотоархив", "batch_name": "Партия 1",
            "summary": {"total_count": 10, "total_bytes": 1024, "uploaded_count": 8,
                        "existing_count": 1, "failed_count": 1}, "errors": ["Один локальный файл недоступен"]})

    def restart(self, *, factory=None):
        self.manager.close(timeout=3)
        self.manager = NotificationManager(self.directory, clock=lambda: self.now,
                                           smtp_factory=factory or self.factory)

    def test_private_credentials_are_never_in_public_settings_status_or_outbox(self):
        self.enqueue()
        self.manager.dispatch_one()
        public = json.dumps([self.manager.settings(), self.manager.status(), self.rows()])
        self.assertNotIn(SECRET, public)
        self.assertTrue(self.manager.settings()["password_stored"])
        if os.name != "nt":
            self.assertEqual(self.manager.path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.manager.database.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.restart()
        self.assertTrue(self.manager.settings()["password_stored"])

    def test_message_is_deduplicated_with_a_stable_id_and_useful_summary(self):
        self.assertTrue(self.enqueue())
        self.assertFalse(self.enqueue())
        self.assertTrue(self.manager.dispatch_one())
        self.assertFalse(self.manager.dispatch_one())
        message = self.clients[0].messages[0]
        self.assertIn("Партия 1", str(message["Subject"]))
        self.assertIn("Загружено файлов: 8", message.get_content())
        self.assertIn("Ошибок: 1", message.get_content())
        self.assertEqual(str(message["To"]), "owner@example.com")
        self.assertTrue(str(message["Message-ID"]).endswith("@example.com>"))
        self.restart()
        self.assertFalse(self.enqueue())
        self.assertEqual(self.manager.status()["sent"], 1)

    def test_starttls_is_verified_before_authentication(self):
        self.manager.save({"security": "starttls", "port": 587})
        self.enqueue()
        self.manager.dispatch_one()
        calls = self.clients[0].calls
        self.assertEqual([call[0] for call in calls], ["ehlo", "tls", "ehlo", "login", "send", "close"])
        self.assertEqual(calls[1], ("tls", True, ssl.CERT_REQUIRED))

    def test_implicit_tls_uses_certificate_and_hostname_validation(self):
        self.manager._smtp_factory = None
        fake = FakeSMTP()
        self.enqueue()
        with patch("mailru_photos.notifications.smtplib.SMTP_SSL", return_value=fake) as connect:
            self.manager.dispatch_one()
        context = connect.call_args.kwargs["context"]
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(connect.call_args.kwargs["timeout"], 15)

    def test_plain_relay_cannot_be_configured_with_credentials(self):
        with self.assertRaises(ValueError):
            self.manager.save({"security": "plain"})
        self.manager.save({"enabled": False, "username": "", "clear_password": True, "security": "plain"})
        self.assertFalse(self.manager.settings()["password_stored"])
        self.assertFalse(self.manager.settings()["enabled"])
        public = self.manager.settings(default_email="account@example.com")
        self.assertEqual(public["username"], "")
        self.manager.save({}, default_email="account@example.com")
        self.assertEqual(self.manager.settings()["username"], "")
        self.restart()
        self.assertEqual(self.manager.settings(default_email="account@example.com")["username"], "")

    def test_account_defaults_only_apply_before_the_first_save(self):
        with tempfile.TemporaryDirectory() as directory:
            other = NotificationManager(directory, smtp_factory=self.factory)
            try:
                initial = other.settings(default_email="account@example.com")
                self.assertEqual(initial["username"], "account@example.com")
                self.assertEqual(initial["from_email"], "account@example.com")
                self.assertEqual(initial["recipients"], ["account@example.com"])
                other.save({"username": "", "recipients": []}, default_email="account@example.com")
                self.assertEqual(other.settings(default_email="account@example.com")["recipients"], [])
                self.assertEqual(other.settings(default_email="account@example.com")["username"], "")
            finally:
                other.close()

    def test_authentication_failure_blocks_persists_and_never_echoes_server_secrets(self):
        self.manager._smtp_factory = lambda config: FakeSMTP(failure=smtplib.SMTPAuthenticationError(535, SECRET.encode()))
        self.enqueue()
        self.manager.dispatch_one()
        self.assertTrue(self.manager.status()["blocked"])
        self.assertEqual(self.rows()[0]["state"], "pending")
        self.assertNotIn(SECRET, json.dumps(self.rows()))
        self.assertNotIn(SECRET, self.manager.status()["last_error"])
        self.restart()
        self.assertTrue(self.manager.status()["blocked"])
        self.assertFalse(self.manager.dispatch_one())
        self.manager.save({"password": "new-unit-test-password"})
        self.assertFalse(self.manager.status()["blocked"])
        self.assertEqual(self.manager.status()["next_attempt"], self.now)
        self.manager.dispatch_one()
        self.assertEqual(self.rows()[0]["state"], "sent")

    def test_tls_failure_and_unsupported_starttls_block_without_exposing_response(self):
        for error in (ssl.SSLError(SECRET), smtplib.SMTPNotSupportedError(SECRET)):
            with self.subTest(error=type(error).__name__):
                self.manager.save({"security": "starttls", "port": 587})
                client = FakeSMTP()
                client.starttls = lambda **kwargs: (_ for _ in ()).throw(error)
                self.manager._smtp_factory = lambda config: client
                self.enqueue(key=type(error).__name__)
                self.manager.dispatch_one()
                self.assertTrue(self.manager.status()["blocked"])
                self.assertFalse(any(call[0] == "login" for call in client.calls))
                self.assertNotIn(SECRET, self.manager.status()["last_error"])

    def test_transient_failures_have_bounded_backoff_and_settings_reset_budget(self):
        self.manager._smtp_factory = lambda config: FakeSMTP(failure=smtplib.SMTPDataError(451, SECRET.encode()))
        self.enqueue()
        for attempt, delay in enumerate((60, 300, 1800, 7200), start=1):
            self.manager.dispatch_one()
            row = self.rows()[0]
            self.assertEqual(row["attempts"], attempt)
            self.assertEqual(row["next_attempt"], self.now + delay)
            self.assertEqual(row["state"], "pending")
            self.assertFalse(self.manager.dispatch_one())
            self.now = row["next_attempt"]
        self.manager.save({"port": 465})
        self.assertEqual(self.rows()[0]["attempts"], 0)
        for _ in range(5):
            self.manager.dispatch_one()
            row = self.rows()[0]
            self.now = row["next_attempt"] or self.now
        self.assertEqual(self.rows()[0]["state"], "failed")
        self.assertIsNone(self.manager.status()["next_attempt"])
        self.assertNotIn(SECRET, json.dumps(self.rows()))

    def test_partial_recipient_rejection_is_not_retried_to_successful_recipients(self):
        self.manager._smtp_factory = lambda config: FakeSMTP(rejected={"second@example.com": (550, SECRET.encode())})
        self.enqueue()
        self.manager.dispatch_one()
        self.assertEqual(self.rows()[0]["state"], "failed")
        self.assertFalse(self.manager.dispatch_one())
        self.assertNotIn(SECRET, self.manager.status()["last_error"])

    def test_all_temporarily_rejected_recipients_are_retried(self):
        failure = smtplib.SMTPRecipientsRefused({"owner@example.com": (450, SECRET.encode())})
        self.manager._smtp_factory = lambda config: FakeSMTP(failure=failure)
        self.enqueue()
        self.manager.dispatch_one()
        self.assertEqual(self.rows()[0]["state"], "pending")
        self.assertEqual(self.rows()[0]["next_attempt"], self.now + 60)
        self.assertNotIn(SECRET, self.manager.status()["last_error"])

    def test_disabled_event_types_cancel_queued_messages_and_prevent_new_ones(self):
        self.enqueue("complete", "batch_completed")
        self.enqueue("errors", "batch_errors")
        self.manager.save({"events": ["batch_errors"]})
        self.assertEqual([row["state"] for row in self.rows()], ["cancelled", "pending"])
        self.assertFalse(self.enqueue("another", "batch_completed"))
        self.manager.dispatch_one()
        self.assertEqual(len(self.clients), 1)
        self.manager.save({"enabled": False})
        self.assertFalse(self.enqueue("disabled", "batch_errors"))
        self.assertTrue(self.manager.enqueue("test", "test", force=True))
        self.manager.dispatch_one()
        self.assertEqual(len(self.clients), 2)

    def test_interrupted_sending_is_recovered_on_restart_with_same_message_id(self):
        self.enqueue()
        with self.manager._connect() as db:
            db.execute("UPDATE mail_events SET state='sending',attempts=1,next_attempt=NULL")
        self.restart()
        self.assertEqual(self.manager.status()["pending"], 1)
        self.assertEqual(self.manager.status()["next_attempt"], self.now)
        self.manager.dispatch_one()
        self.assertEqual(self.rows()[0]["state"], "sent")

    def test_auth_error_from_old_settings_cannot_block_new_credentials(self):
        entered, release = threading.Event(), threading.Event()
        old_client = FakeSMTP(failure=smtplib.SMTPAuthenticationError(535, SECRET.encode()),
                              login_entered=entered, login_release=release)
        self.manager._smtp_factory = lambda config: old_client
        self.enqueue()
        worker = threading.Thread(target=self.manager.dispatch_one)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            self.manager.save({"password": "corrected-unit-test-password"})
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(self.manager.status()["blocked"])
        self.assertEqual(self.rows()[0]["attempts"], 0)
        self.assertEqual(self.manager.status()["next_attempt"], self.now)
        self.manager._smtp_factory = self.factory
        self.manager.dispatch_one()
        self.assertEqual(self.rows()[0]["state"], "sent")
        self.assertIn(("login", "sender@example.com", "corrected-unit-test-password"), self.clients[0].calls)

    def test_changed_settings_before_auth_never_use_the_old_password(self):
        entered, release = threading.Event(), threading.Event()
        client = FakeSMTP()

        def connect(config):
            entered.set()
            if not release.wait(3):
                raise AssertionError("Test did not release SMTP connection")
            return client

        self.manager._smtp_factory = connect
        self.enqueue()
        worker = threading.Thread(target=self.manager.dispatch_one)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            self.manager.save({"enabled": False, "clear_password": True})
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(any(call[0] in {"login", "send"} for call in client.calls))
        self.assertEqual(self.rows()[0]["state"], "cancelled")

    def test_idle_worker_and_public_reads_never_touch_the_disk(self):
        waiting = threading.Event()
        original_wait = self.manager._condition.wait

        def idle_wait(timeout=None):
            self.assertIsNone(timeout)
            waiting.set()
            return original_wait(timeout)

        with patch.object(self.manager, "_connect", side_effect=AssertionError("Unexpected idle disk access")):
            with patch.object(self.manager._condition, "wait", side_effect=idle_wait):
                self.manager.start()
                self.assertTrue(waiting.wait(2))
                self.manager.settings()
                self.manager.status()
                self.manager.close(timeout=2)

    def test_outbox_disk_failure_blocks_without_busy_retry_or_secret_echo(self):
        self.enqueue()
        waiting = threading.Event()
        original_wait = self.manager._condition.wait

        def idle_wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        with patch.object(self.manager, "_connect", side_effect=sqlite3.OperationalError(SECRET)) as connect:
            with patch.object(self.manager._condition, "wait", side_effect=idle_wait):
                self.manager.start()
                self.assertTrue(waiting.wait(2))
                self.assertTrue(self.manager.status()["blocked"])
                self.assertFalse(self.manager.status()["sending"])
                self.assertNotIn(SECRET, self.manager.status()["last_error"])
                self.assertEqual(connect.call_count, 1)
                self.manager.close(timeout=2)
        self.assertEqual(self.rows()[0]["state"], "pending")

    def test_settings_saved_during_outbox_failure_are_private_and_safely_blocked(self):
        self.enqueue()
        with patch.object(self.manager, "_connect", side_effect=sqlite3.OperationalError(SECRET)):
            with self.assertRaisesRegex(ValueError, "Настройки SMTP сохранены") as failure:
                self.manager.save({"password": "new-test-password"})
            self.assertNotIn(SECRET, str(failure.exception))
            self.assertTrue(self.manager.status()["blocked"])
            self.assertTrue(self.manager.settings()["password_stored"])
        self.manager.save({})
        self.assertFalse(self.manager.status()["blocked"])
        self.manager.dispatch_one()
        self.assertIn(("login", "sender@example.com", "new-test-password"), self.clients[0].calls)

    def test_validation_rejects_header_injection_bad_types_and_nonobject_payloads(self):
        for value in ({"from_email": "test@example.com\r\nBcc: other@example.com"},
                      {"sender_name": "Name\nBcc: other"}, {"security": {}},
                      {"port": True}, {"host": "smtp.-invalid.example"}, {"events": ["other"]}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_settings(value)
        with self.assertRaises(ValueError):
            self.manager.enqueue("bad", EVENTS[0], ["not an object"])
        with self.assertRaises(ValueError):
            self.manager.enqueue("huge", EVENTS[0], {"message": "x" * 40000})


if __name__ == "__main__":
    unittest.main()

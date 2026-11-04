"""Private SMTP settings and a durable, independently dispatched event outbox.

There is no per-file mail and no disk polling while idle. An SMTP connection
cannot participate in a SQLite transaction: after a crash at the final server
acknowledgement a message may be repeated, with the same Message-ID.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import datetime, timezone
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime, formataddr
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import smtplib
import socket
import sqlite3
import ssl
import stat
import tempfile
import threading
import time

from .auth import _dpapi


EVENTS = ("batch_completed", "batch_errors", "plan_completed", "plan_attention")
DEFAULTS = {"enabled": False, "host": "smtp.mail.ru", "port": 465, "security": "ssl",
            "username": "", "sender_name": "Mail Cloud", "from_email": "", "recipients": [],
            "events": list(EVENTS)}
_FIELDS = frozenset(DEFAULTS) | {"password", "clear_password"}


def _text(value, maximum, label, *, empty=True):
    if not isinstance(value, str) or len(value) > maximum or any(c in value for c in "\r\n\x00"):
        raise ValueError("Некорректное значение: " + label + ".")
    value = value.strip()
    if not empty and not value:
        raise ValueError("Заполните поле: " + label + ".")
    return value


def _email(value):
    value = _text(value, 254, "адрес почты", empty=False)
    if re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", value) is None:
        raise ValueError("Укажите обычный адрес почты без имени и угловых скобок.")
    local, domain = value.rsplit("@", 1)
    if ".." in domain or any(not part or len(part) > 63 or part.startswith('-') or part.endswith('-') for part in domain.split('.')):
        raise ValueError("Некорректный домен адреса почты.")
    return local + "@" + domain.lower()


def validate_settings(data, previous=None):
    if not isinstance(data, dict) or set(data) - _FIELDS:
        raise ValueError("Некорректные настройки SMTP.")
    values = {**DEFAULTS, **(previous or {}), **{key: value for key, value in data.items() if key in DEFAULTS}}
    if type(values["enabled"]) is not bool:
        raise ValueError("Уведомления должны быть включены или выключены.")
    host = _text(values["host"], 253, "сервер SMTP", empty=False)
    try:
        ipaddress.ip_address(host)
    except ValueError:
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError:
            raise ValueError("Некорректное имя сервера SMTP.") from None
        if (re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", host) is None
                or any(not part or len(part) > 63 or part.startswith('-') or part.endswith('-') for part in host.split('.'))):
            raise ValueError("Укажите имя сервера SMTP без протокола, пути и порта.")
    values["host"] = host
    if type(values["port"]) is not int or not 1 <= values["port"] <= 65535:
        raise ValueError("Порт SMTP должен быть от 1 до 65535.")
    if not isinstance(values["security"], str) or values["security"] not in {"ssl", "starttls", "plain"}:
        raise ValueError("Выберите SSL/TLS, STARTTLS или локальный сервер без шифрования.")
    values["username"] = _text(values["username"], 254, "логин SMTP")
    values["sender_name"] = _text(values["sender_name"], 200, "имя отправителя")
    values["from_email"] = _email(values["from_email"]) if values["from_email"] else ""
    recipients = values["recipients"]
    if not isinstance(recipients, list) or len(recipients) > 50:
        raise ValueError("Укажите до 50 получателей уведомлений.")
    values["recipients"] = list(dict.fromkeys(_email(item) for item in recipients))
    events = values["events"]
    if not isinstance(events, list) or any(not isinstance(item, str) or item not in EVENTS for item in events):
        raise ValueError("Неизвестный тип email-уведомления.")
    values["events"] = [kind for kind in EVENTS if kind in events]
    if values["security"] == "plain" and values["username"]:
        raise ValueError("Для передачи логина и пароля SMTP включите SSL/TLS или STARTTLS.")
    if values["enabled"] and (not values["from_email"] or not values["recipients"]):
        raise ValueError("Укажите адрес отправителя и получателей уведомлений.")
    return values


def _private_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise ValueError("Настройки почты не должны быть символической ссылкой.")
    fd, temporary = tempfile.mkstemp(prefix='.email-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            if os.name != 'nt':
                os.fchmod(stream.fileno(), 0o600)
            stream.write(json.dumps(value, ensure_ascii=False).encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != 'nt':
            parent = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _load_private(path):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or (os.name != 'nt' and info.st_mode & 0o077):
                raise ValueError("Настройки SMTP должны быть обычным файлом с правами 600.")
            data = stream.read(65537)
        if len(data) > 65536:
            raise ValueError("Файл настроек SMTP слишком большой.")
        value = json.loads(data)
        if not isinstance(value, dict) or value.get('version') != 1:
            raise ValueError("Некорректный файл настроек SMTP.")
        return value
    except FileNotFoundError:
        return None
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("Не удалось прочитать настройки SMTP.") from None


def _password_payload(password):
    if os.name == 'nt':
        return {'storage': 'windows-dpapi', 'data': base64.b64encode(_dpapi(password.encode(), decrypt=False)).decode('ascii')}
    return {'storage': 'posix-0600', 'data': password}


def _password_load(value):
    if not isinstance(value, dict):
        raise ValueError("Некорректное сохранение пароля SMTP.")
    if value.get('storage') == 'windows-dpapi' and os.name == 'nt':
        return _dpapi(base64.b64decode(value['data'], validate=True), decrypt=True).decode('utf-8')
    if value.get('storage') == 'posix-0600' and os.name != 'nt' and isinstance(value.get('data'), str):
        return value['data']
    raise ValueError("Пароль SMTP нужно заново сохранить на этом компьютере.")


def _message(event, payload, config, key, created_at):
    names = {'batch_completed': 'Партия загружена', 'batch_errors': 'Партия завершена с ошибками',
             'plan_completed': 'План загрузки завершён', 'plan_attention': 'Плану загрузки требуется внимание',
             'test': 'Проверка email-уведомлений'}
    title = names[event]
    plan = _text(str(payload.get('plan_name', '')), 200, 'название плана')
    batch = _text(str(payload.get('batch_name', '')), 200, 'название партии')
    message = EmailMessage(policy=SMTP)
    message['Subject'] = 'Mail Cloud: ' + title + ((' — ' + batch) if batch else (' — ' + plan) if plan else '')
    message['From'] = formataddr((config['sender_name'], config['from_email']), charset='utf-8')
    message['To'] = ', '.join(config['recipients'])
    message['Date'] = format_datetime(datetime.fromtimestamp(created_at, timezone.utc))
    domain = config['from_email'].rsplit('@', 1)[1]
    message['Message-ID'] = '<mail-cloud.' + hashlib.sha256(key.encode()).hexdigest()[:40] + '@' + domain + '>'
    lines = [title, '']
    if plan:
        lines.append('План: ' + plan)
    if batch:
        lines.append('Партия: ' + batch)
    labels = [('total_count', 'Задач'), ('total_bytes', 'Объём, байт'), ('uploaded_count', 'Загружено файлов'),
              ('uploaded_bytes', 'Загружено байт'), ('existing_count', 'Уже было в Облаке'),
              ('failed_count', 'Ошибок'), ('skipped_count', 'Пропущено')]
    summary = payload.get('summary', {})
    if isinstance(summary, dict):
        for field, label in labels:
            if type(summary.get(field)) is int and summary[field] >= 0:
                lines.append(f'{label}: {summary[field]:,}')
    detail = payload.get('message')
    if isinstance(detail, str) and detail:
        lines.extend(('', detail[:2000]))
    samples = payload.get('errors', [])
    if isinstance(samples, list) and samples:
        lines.extend(('', 'Примеры ошибок (полный список доступен в панели):'))
        lines.extend(str(item)[:500] for item in samples[:10])
    if event == 'test':
        lines.append('Настройки SMTP позволяют отправлять уведомления из Mail Cloud.')
    message.set_content('\n'.join(lines))
    return message


class NotificationManager:
    def __init__(self, state_dir, *, clock=time.time, smtp_factory=None):
        self.path = Path(state_dir) / 'email.json'
        self.database = Path(state_dir) / 'email-outbox.sqlite3'
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._clock, self._smtp_factory = clock, smtp_factory
        self._condition = threading.Condition(threading.RLock())
        self._thread = None
        self._closed = False
        self._sending = False
        self._generation = 0
        saved = _load_private(self.path)
        self._has_saved = saved is not None
        self._config = validate_settings(saved['settings']) if saved else validate_settings({})
        self._password = _password_load(saved['secret']) if saved else ''
        self._blocked = None
        self._status = {'pending': 0, 'sent': 0, 'failed': 0, 'sending': False, 'last_error': None, 'last_sent_at': None}
        # Reserve a private file before SQLite creates its WAL/SHM companions.
        # Their access mode follows the database, including the first open.
        try:
            descriptor = os.open(self.database, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if not stat.S_ISREG(self.database.lstat().st_mode):
                raise ValueError('Очередь уведомлений должна быть обычным файлом без символических ссылок.') from None
        else:
            os.close(descriptor)
        self.database.chmod(0o600)
        with self._connect() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS mail_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,event_key TEXT UNIQUE NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,
                state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL,created_at REAL NOT NULL,
                sent_at REAL,error TEXT);
                CREATE INDEX IF NOT EXISTS mail_ready ON mail_events(state,next_attempt,id);
                CREATE TABLE IF NOT EXISTS mail_state(key TEXT PRIMARY KEY,value TEXT NOT NULL);''')
            db.execute("UPDATE mail_events SET state='pending',next_attempt=? WHERE state='sending'", (clock(),))
            self._cancel_disabled(db, self._config)
            row = db.execute("SELECT value FROM mail_state WHERE key='blocked'").fetchone()
            self._blocked = row[0] if row else None
            self._refresh(db)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _cancel_disabled(db, config):
        selected = config['events'] if config['enabled'] else []
        clause = 'kind NOT IN (' + ','.join('?' for _ in selected) + ')' if selected else '1'
        db.execute(f"UPDATE mail_events SET state='cancelled',next_attempt=NULL WHERE state='pending' AND kind<>'test' AND {clause}", selected)

    def _refresh(self, db):
        counts = dict(db.execute('SELECT state,COUNT(*) FROM mail_events GROUP BY state'))
        self._next_due = db.execute("SELECT MIN(next_attempt) FROM mail_events WHERE state='pending'").fetchone()[0]
        latest = db.execute("SELECT MAX(sent_at) FROM mail_events WHERE state='sent'").fetchone()[0]
        failure = db.execute("SELECT error FROM mail_events WHERE error IS NOT NULL ORDER BY id DESC LIMIT 1").fetchone()
        self._status.update(pending=counts.get('pending', 0), sent=counts.get('sent', 0), failed=counts.get('failed', 0),
                            last_sent_at=latest, last_error=self._blocked or (failure[0] if failure else None))

    def settings(self, default_email=None):
        with self._condition:
            result = {**self._config, 'recipients': list(self._config['recipients']), 'events': list(self._config['events'])}
            if default_email and not self._has_saved:
                email = _email(default_email)
                for key in ('username', 'from_email'):
                    result[key] = result[key] or email
                result['recipients'] = result['recipients'] or [email]
            return {**result, 'password_stored': bool(self._password)}

    def status(self):
        with self._condition:
            return {**self._status, 'sending': self._sending, 'blocked': bool(self._blocked),
                    'next_attempt': self._next_due if not self._blocked else None}

    def save(self, data, *, default_email=None):
        with self._condition:
            previous = self.settings(default_email)
            previous.pop('password_stored')
            config = validate_settings(data, previous)
            clear = data.get('clear_password', False)
            if type(clear) is not bool or (clear and data.get('password')):
                raise ValueError('Выберите удаление или замену пароля SMTP.')
            value = data.get('password')
            if value is not None and (not isinstance(value, str) or len(value) > 4096 or '\x00' in value):
                raise ValueError('Некорректный пароль SMTP.')
            password = '' if clear else value if value else self._password
            if config['enabled'] and config['username'] and not password:
                raise ValueError('Сохраните пароль приложения для SMTP.')
            _private_write(self.path, {'version': 1, 'settings': config, 'secret': _password_payload(password)})
            self._has_saved = True
            self._config, self._password = config, password
            self._generation += 1
            self._blocked = None
            try:
                with self._connect() as db:
                    db.execute("DELETE FROM mail_state WHERE key='blocked'")
                    if not self._sending:
                        # A disk error after an SMTP acknowledgement can leave a
                        # sending row. A deliberate settings save permits recovery.
                        db.execute("UPDATE mail_events SET state='pending' WHERE state='sending'")
                    db.execute("UPDATE mail_events SET attempts=0,next_attempt=?,error=NULL WHERE state='pending'", (self._clock(),))
                    self._cancel_disabled(db, config)
                    self._refresh(db)
            except (OSError, sqlite3.Error):
                self._blocked = 'Настройки SMTP сохранены, но очередь email недоступна. Проверьте диск и права, затем сохраните настройки ещё раз.'
                self._status['last_error'] = self._blocked
                self._condition.notify_all()
                raise ValueError(self._blocked) from None
            self._condition.notify_all()
            return self.settings(default_email)

    def enqueue(self, key, event, payload=None, *, force=False):
        if not isinstance(key, str) or not key or len(key) > 300 or event not in (*EVENTS, 'test'):
            raise ValueError('Некорректное email-событие.')
        if payload is not None and not isinstance(payload, dict):
            raise ValueError('Сводка уведомления должна быть объектом.')
        encoded = json.dumps(payload or {}, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) > 32768:
            raise ValueError('Сводка уведомления слишком большая.')
        with self._condition:
            if self._closed or (not force and (not self._config['enabled'] or event not in self._config['events'])):
                return False
            if not self._config['from_email'] or not self._config['recipients']:
                raise ValueError('Сохраните адрес отправителя и получателей SMTP.')
            if self._config['username'] and not self._password:
                raise ValueError('Сохраните пароль приложения SMTP.')
            with self._connect() as db:
                inserted = db.execute("INSERT OR IGNORE INTO mail_events(event_key,kind,payload,state,next_attempt,created_at) VALUES(?,?,?,'pending',?,?)",
                                      (key, event, encoded, self._clock(), self._clock())).rowcount
                if force:
                    self._blocked = None
                    db.execute("DELETE FROM mail_state WHERE key='blocked'")
                self._refresh(db)
            self._condition.notify_all()
            return bool(inserted)

    def start(self):
        with self._condition:
            if self._thread is None and not self._closed:
                self._thread = threading.Thread(target=self._loop, name='mail-cloud-email', daemon=True)
                self._thread.start()

    def _loop(self):
        while True:
            with self._condition:
                if self._closed:
                    return
                delay = None if self._next_due is None or self._blocked else max(0, self._next_due - self._clock())
                if delay is None or delay > 0 or self._sending:
                    self._condition.wait(timeout=delay if delay is not None and delay > 0 else None)
                    continue
            try:
                self.dispatch_one()
            except (OSError, sqlite3.Error):
                # Do not let a full/unmounted state disk turn into a hot retry
                # loop or kill the dispatcher silently. The persisted outbox
                # remains recoverable and the UI reports a stable failure.
                with self._condition:
                    self._sending = False
                    self._blocked = 'Не удалось сохранить очередь email. Проверьте диск и права, затем сохраните настройки почты.'
                    self._status['last_error'] = self._blocked
                    self._condition.notify_all()

    def dispatch_one(self):
        with self._condition:
            if self._closed or self._sending or self._blocked:
                return False
            with self._connect() as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute("SELECT * FROM mail_events WHERE state='pending' AND next_attempt<=? ORDER BY next_attempt,id LIMIT 1", (self._clock(),)).fetchone()
                if row is None:
                    self._refresh(db)
                    return False
                item = dict(row)
                db.execute("UPDATE mail_events SET state='sending',attempts=attempts+1 WHERE id=?", (item['id'],))
                self._refresh(db)
            self._sending = True
            config = {**self._config, 'recipients': list(self._config['recipients'])}
            password, generation = self._password, self._generation
        client = None
        error, transient, blocked = None, False, False
        try:
            if item['kind'] != 'test' and (not config['enabled'] or item['kind'] not in config['events']):
                raise InterruptedError()
            if self._smtp_factory is not None:
                client = self._smtp_factory(config)
            elif config['security'] == 'ssl':
                client = smtplib.SMTP_SSL(config['host'], config['port'], timeout=15, context=ssl.create_default_context())
            else:
                client = smtplib.SMTP(config['host'], config['port'], timeout=15)
            client.ehlo()
            if config['security'] == 'starttls':
                client.starttls(context=ssl.create_default_context())
                client.ehlo()
            with self._condition:
                if self._closed or generation != self._generation:
                    raise InterruptedError()
            if config['username']:
                client.login(config['username'], password)
            with self._condition:
                if self._closed or generation != self._generation:
                    raise InterruptedError()
            message = _message(item['kind'], json.loads(item['payload']), config, item['event_key'], item['created_at'])
            rejected = client.send_message(message, from_addr=config['from_email'], to_addrs=config['recipients'])
            if rejected:
                # Some recipients may already have received the message.
                # Do not resend it to successful recipients automatically.
                error = 'SMTP принял письмо не для всех получателей. Проверьте адреса; автоматический повтор отключён.'
        except InterruptedError:
            error = 'Отправка отменена: приложение останавливается или настройки изменились.'
            transient = True
        except smtplib.SMTPAuthenticationError:
            error, blocked = 'SMTP отклонил авторизацию. Проверьте пароль приложения и разрешение на отправку почты.', True
        except ssl.SSLError:
            error, blocked = 'Не удалось подтвердить защищённое соединение SMTP. Проверьте сервер, порт и режим TLS.', True
        except smtplib.SMTPNotSupportedError:
            error, blocked = 'Сервер SMTP не поддерживает выбранный режим TLS или авторизации. Проверьте настройки.', True
        except smtplib.SMTPRecipientsRefused as exc:
            codes = [value[0] for value in exc.recipients.values() if isinstance(value, (tuple, list)) and value]
            transient = bool(codes) and all(type(code) is int and 400 <= code < 500 for code in codes)
            error = 'SMTP временно отклонил всех получателей. Письмо будет отправлено повторно.' if transient else 'SMTP отклонил получателей. Проверьте адреса в настройках.'
        except smtplib.SMTPResponseException as exc:
            code = int(exc.smtp_code)
            error, transient = f'SMTP отклонил операцию, код {code}.', 400 <= code < 500
        except (TimeoutError, OSError, socket.timeout, smtplib.SMTPServerDisconnected):
            error, transient = 'SMTP недоступен или не ответил вовремя. Уведомление сохранено для повторной отправки.', True
        except Exception:
            error = 'Уведомление не отправлено. Проверьте настройки SMTP и повторите проверку.'
        finally:
            password = ''
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
        with self._condition:
            attempts = item['attempts'] + 1
            settings_changed = generation != self._generation
            if settings_changed and error is not None:
                # A response from the old host/credentials must not block the
                # just-saved configuration or exhaust its new retry budget.
                error = 'Настройки SMTP изменились. Отправка будет повторена с новыми настройками.'
                transient, blocked, attempts = True, False, 0
            if blocked:
                self._blocked = error
            state = 'sent' if error is None else 'pending' if blocked or (transient and attempts < 5) else 'failed'
            if self._closed and transient:
                state = 'pending'
            if item['kind'] != 'test' and (not self._config['enabled'] or item['kind'] not in self._config['events']):
                state = 'sent' if error is None else 'cancelled'
            retry = (self._clock() if settings_changed else self._clock() + (60, 300, 1800, 7200, 86400)[min(attempts - 1, 4)]) if state == 'pending' else None
            with self._connect() as db:
                if blocked:
                    db.execute("INSERT OR REPLACE INTO mail_state VALUES('blocked',?)", (error,))
                db.execute('UPDATE mail_events SET state=?,attempts=?,next_attempt=?,sent_at=?,error=? WHERE id=?',
                           (state, attempts, retry, self._clock() if state == 'sent' else None, error, item['id']))
                self._refresh(db)
            self._sending = False
            self._condition.notify_all()
        return True

    def close(self, timeout=1):
        with self._condition:
            self._closed = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)

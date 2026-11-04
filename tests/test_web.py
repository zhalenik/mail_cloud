import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import quote

from mailru_photos.api import FileIdentity
from mailru_photos.content import open_task_content
from mailru_photos.web import WebApplication


class FakeService:
    def __init__(self):
        self.calls = []
        self.content_sources = {}

    def snapshot(self):
        return {"configured": False, "authenticated": False, "stats": {"total": 0}}

    def setup(self, destination):
        self.calls.append(("setup", destination))

    def login(self, email, password):
        self.calls.append(("login", email, password))

    def logout(self):
        self.calls.append(("logout",))

    def add_folder(self, path, alias, *, remote_prefix=None):
        self.calls.append(("add_folder", path, alias, remote_prefix))

    def update_source(self, source_id, *, remote_prefix, alias=None):
        self.calls.append(("update_source", source_id, remote_prefix, alias))

    def remove_source(self, source_id):
        self.calls.append(("remove_source", source_id))

    def control_task(self, task_id, action):
        self.calls.append(("control_task", task_id, action))

    def clear_tasks(self, scope):
        self.calls.append(("clear_tasks", scope))

    def preview_deletion(self, **kwargs):
        self.calls.append(("preview_deletion", kwargs))
        return {"token": "offline-preview-token", "tasks": 1, "bytes": 123}

    def confirm_deletion(self, token, **kwargs):
        self.calls.append(("confirm_deletion", token, kwargs))

    def schedules(self):
        self.calls.append(("schedules",))
        return {"schedules": [], "timezone": "UTC"}

    def save_schedule(self, value):
        self.calls.append(("save_schedule", value))
        return {"schedules": [value]}

    def toggle_schedule(self, identifier, enabled):
        self.calls.append(("toggle_schedule", identifier, enabled))
        return {"ok": True}

    def remove_schedule(self, identifier):
        self.calls.append(("remove_schedule", identifier))
        return {"ok": True}

    def jobs(self, **kwargs):
        self.calls.append(("jobs", kwargs))
        return {"jobs": [], "total": 0, **kwargs}

    def folders(self, path):
        return {"path": path, "parent": None, "roots": [], "directories": []}

    def settings(self, **kwargs):
        self.calls.append(("settings", kwargs))

    def refresh_quota(self):
        self.calls.append(('refresh_quota',))

    def task_source(self, task_id):
        self.calls.append(('task_source', task_id))
        if task_id not in self.content_sources:
            raise FileNotFoundError('Offline missing task')
        return self.content_sources[task_id]

    def start(self):
        self.calls.append(("start",))

    def pause(self):
        self.calls.append(("pause",))

    def scan(self):
        self.calls.append(("scan",))


class WebTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)
        self.service = FakeService()
        self.app = WebApplication(self.service, self.path)
        self.cookie = ""
        self.csrf = ""

    def tearDown(self):
        self.directory.cleanup()

    def request(self, path, *, method="GET", data=None, host="localhost:8787", origin="http://localhost:8787", csrf=None, body=None, content_type="application/json", headers=None):
        target, _, query = path.partition("?")
        raw = body if body is not None else json.dumps(data or {}).encode()
        environ = {"REQUEST_METHOD": method, "PATH_INFO": target, "QUERY_STRING": query,
                   "HTTP_HOST": host, "HTTP_ORIGIN": origin, "HTTP_COOKIE": self.cookie,
                   "HTTP_X_CSRF_TOKEN": self.csrf if csrf is None else csrf,
                   "wsgi.url_scheme": "http", "REMOTE_ADDR": "127.0.0.1",
                   "wsgi.input": io.BytesIO(raw), "CONTENT_LENGTH": str(len(raw)),
                   "CONTENT_TYPE": content_type}
        for name, value in (headers or {}).items():
            environ['HTTP_' + name.upper().replace('-', '_')] = value
        reply = {}
        def respond(status, headers):
            reply.update(status=int(status.split()[0]), headers=dict(headers))
        response = self.app(environ, respond)
        try:
            payload = b"".join(response)
        finally:
            close = getattr(response, 'close', None)
            if callable(close):
                close()
        if "Set-Cookie" in reply["headers"]:
            self.cookie = reply["headers"]["Set-Cookie"].split(";", 1)[0]
        if payload and reply["headers"]["Content-Type"].startswith("application/json"):
            reply["data"] = json.loads(payload)
            if "csrf" in reply["data"]:
                self.csrf = reply["data"]["csrf"]
        reply["body"] = payload
        return reply

    def authenticate(self):
        response = self.request("/api/session")
        self.assertTrue(response["data"]["setup_required"])
        response = self.request("/api/session/setup", method="POST", data={"password": "panel-test-only-password"})
        self.assertEqual(response["status"], 200)
        self.assertTrue(response["data"]["authenticated"])

    def test_task_controls_require_session_csrf_and_validate_inputs(self):
        self.request('/api/session')
        self.assertEqual(self.request('/api/tasks/pause', method='POST', data={'id': 1})['status'], 401)
        self.authenticate()
        for action in ('pause', 'resume', 'start', 'remove', 'skip', 'return'):
            endpoint = '/api/tasks/' + action
            self.assertEqual(self.request(endpoint, method='POST', data={'id': 1}, csrf='bad')['status'], 403)
            self.assertEqual(self.request(endpoint, method='POST', data={'id': 1})['status'], 200)
            self.assertEqual(self.service.calls[-1], ('control_task', 1, action))
        before = list(self.service.calls)
        for value in (None, True, 0, -1, 1.5, 'one', 2**63):
            self.assertEqual(self.request('/api/tasks/remove', method='POST', data={'id': value})['status'], 400)
        self.assertEqual(self.service.calls, before)
        self.assertEqual(self.request('/api/tasks/pause')['status'], 404)
        self.assertEqual(self.request('/api/jobs?status=paused')['status'], 200)
        self.assertEqual(self.service.calls[-1][1]['status'], 'paused')

    def test_clear_tasks_has_explicit_scope_and_same_request_protection(self):
        self.authenticate()
        for scope in ('all', 'finished'):
            self.assertEqual(self.request('/api/tasks/clear', method='POST', data={'scope': scope}, csrf='bad')['status'], 403)
            self.assertEqual(self.request('/api/tasks/clear', method='POST', data={'scope': scope})['status'], 200)
            self.assertEqual(self.service.calls[-1], ('clear_tasks', scope))
        before = list(self.service.calls)
        for scope in (None, '', 'everything', True):
            self.assertEqual(self.request('/api/tasks/clear', method='POST', data={'scope': scope})['status'], 400)
        self.assertEqual(self.service.calls, before)

    def test_private_routes_require_panel_session(self):
        for route in ("/api/state", "/api/folders", "/api/jobs", "/api/report", "/api/schedules"):
            self.assertEqual(self.request(route)["status"], 401)
        self.assertEqual(self.service.calls, [])

    def test_mail_logout_requires_csrf_and_keeps_panel_session(self):
        self.request('/api/session')
        self.assertEqual(self.request('/api/logout', method='POST')['status'], 401)
        self.authenticate()
        self.assertEqual(self.request('/api/logout', method='POST', csrf='wrong')['status'], 403)
        self.assertEqual(self.service.calls, [])
        self.assertEqual(self.request('/api/logout', method='POST')['status'], 200)
        self.assertEqual(self.service.calls, [('logout',)])
        self.assertTrue(self.request('/api/session')['data']['authenticated'])

    def test_setup_password_is_hashed_and_cookie_survives_server_restart(self):
        self.authenticate()
        text = (self.path / "web_access.json").read_text()
        self.assertNotIn("panel-test-only-password", text)
        self.assertIn('"digest"', text)
        self.app = WebApplication(self.service, self.path)
        self.assertEqual(self.request("/api/state")["status"], 200)
        self.assertTrue(self.request("/api/session")["data"]["authenticated"])

    def test_origin_and_csrf_both_required_before_mutation(self):
        self.authenticate()
        for kwargs in ({"origin": "https://attacker.invalid"}, {"origin": ""}, {"csrf": "wrong"}):
            result = self.request("/api/start", method="POST", **kwargs)
            self.assertEqual(result["status"], 403)
        self.assertEqual(self.service.calls, [])
        self.assertEqual(self.request("/api/start", method="POST")["status"], 200)
        self.assertEqual(self.service.calls, [("start",)])

    def test_removed_preflight_setting_is_rejected_without_reaching_service(self):
        self.authenticate()
        response = self.request('/api/settings', method='POST', data={'workers': 2, 'auto_resume': True})
        self.assertEqual(response['status'], 200)
        self.assertNotIn('preflight_check', self.service.calls[-1][1])
        calls = len(self.service.calls)
        for value in (True, False, None, 0, 1, 'false'):
            response = self.request('/api/settings', method='POST', data={
                'workers': 2, 'auto_resume': True, 'preflight_check': value})
            self.assertEqual(response['status'], 400)
        self.assertEqual(len(self.service.calls), calls)

    def test_storage_policy_settings_forward_explicit_choices_and_timezone(self):
        self.authenticate()
        for regular, unexpected in (("skip", "trash"), ("upload", "keep")):
            response = self.request('/api/settings', method='POST', data={
                'workers': 3, 'auto_resume': False, 'regular_policy': regular,
                'unexpected_unlimited_action': unexpected, 'timezone': 'Europe/Moscow'})
            self.assertEqual(response['status'], 200)
            options = self.service.calls[-1][1]
            self.assertEqual(options['regular_policy'], regular)
            self.assertEqual(options['unexpected_unlimited_action'], unexpected)
            self.assertEqual(options['timezone'], 'Europe/Moscow')

    def test_remote_reconcile_setting_and_quota_refresh_are_authenticated_mutations(self):
        self.request('/api/session')
        self.assertEqual(self.request('/api/quota/refresh', method='POST')['status'], 401)
        self.authenticate()
        self.assertEqual(self.request('/api/quota/refresh', method='POST', csrf='bad')['status'], 403)
        self.assertEqual(self.request('/api/quota/refresh', method='POST')['status'], 200)
        self.assertEqual(self.service.calls[-1], ('refresh_quota',))
        for choice in (False, True):
            response = self.request('/api/settings', method='POST', data={
                'workers': 2, 'auto_resume': True, 'remote_reconcile': choice})
            self.assertEqual(response['status'], 200)
            self.assertIs(self.service.calls[-1][1]['remote_reconcile'], choice)
        response = self.request('/api/settings', method='POST', data={'workers': 2, 'auto_resume': True})
        self.assertEqual(response['status'], 200)
        self.assertIsNone(self.service.calls[-1][1]['remote_reconcile'])
        before = list(self.service.calls)
        for choice in (None, 0, 1, 'true'):
            self.assertEqual(self.request('/api/settings', method='POST', data={
                'workers': 2, 'auto_resume': True, 'remote_reconcile': choice})['status'], 400)
        self.assertEqual(self.service.calls, before)

    def test_removed_large_upload_setting_is_rejected_without_partial_change(self):
        self.authenticate()
        before = list(self.service.calls)
        for value in (True, False, None, 'true'):
            result = self.request('/api/settings', method='POST', data={
                'workers': 2, 'auto_resume': True, 'upload_large_files': value})
            self.assertEqual(result['status'], 400)
            self.assertEqual(self.service.calls, before)

    def test_dns_rebinding_host_is_rejected_even_for_session(self):
        for host in ("attacker.invalid:8787", "localhost@attacker.invalid", "localhost/evil", "localhost:bad"):
            self.assertEqual(self.request("/api/session", host=host)["status"], 403)
        self.assertEqual(self.request("/healthz")["status"], 200)

    def test_lan_published_port_uses_host_origin_without_public_origin_override(self):
        self.app = WebApplication(self.service, self.path, allowed_hosts=["192.0.2.10"])
        target = {"host": "192.0.2.10:31800", "origin": "http://192.0.2.10:31800"}
        self.assertEqual(self.request("/api/session", **target)["status"], 200)
        self.assertEqual(self.request("/api/session/setup", method="POST",
                                     data={"password": "panel-test-only-password"}, **target)["status"], 200)
        self.assertEqual(self.request("/api/start", method="POST", **target)["status"], 200)
        self.assertEqual(self.service.calls, [("start",)])
        self.assertEqual(self.request("/api/start", method="POST", **{
            **target, "origin": "http://192.0.2.10:8787"})["status"], 403)
        self.assertEqual(self.request("/api/session", host="unapproved.invalid:31800")["status"], 403)

    def test_tampered_cookie_does_not_authorize(self):
        self.authenticate()
        self.cookie += "tampered"
        self.assertEqual(self.request("/api/state")["status"], 401)

    def test_panel_logout_does_not_pause_background_uploads(self):
        self.authenticate()
        result = self.request("/api/session/logout", method="POST")
        self.assertEqual(result["status"], 200)
        self.assertFalse(result["data"]["authenticated"])
        self.assertEqual(self.request("/api/state")["status"], 401)
        self.assertEqual(self.service.calls, [])

    def test_setup_cannot_replace_existing_password(self):
        self.authenticate()
        result = self.request("/api/session/setup", method="POST", data={"password": "replacement-password"})
        self.assertEqual(result["status"], 409)

    def test_login_is_rate_limited_without_exposing_passwords(self):
        self.authenticate()
        self.request("/api/session/logout", method="POST")
        for _ in range(8):
            result = self.request("/api/session/login", method="POST", data={"password": "wrong-private-password"})
            self.assertEqual(result["status"], 401)
            self.assertNotIn(b"wrong-private-password", result["body"])
        self.assertEqual(self.request("/api/session/login", method="POST", data={"password": "wrong-private-password"})["status"], 429)

    def test_mail_password_is_only_forwarded_to_login_not_response_or_panel_file(self):
        self.authenticate()
        response = self.request("/api/login", method="POST", data={"email": "test@example.invalid", "password": "mail-test-password-only"})
        self.assertEqual(response["status"], 200)
        self.assertEqual(self.service.calls[-1], ("login", "test@example.invalid", "mail-test-password-only"))
        self.assertNotIn(b"mail-test-password-only", response["body"])
        self.assertNotIn("mail-test-password-only", (self.path / "web_access.json").read_text())

    def test_panel_password_change_rotates_session_without_touching_mail_service(self):
        self.authenticate()
        old_cookie, old_csrf = self.cookie, self.csrf
        response = self.request('/api/session/password', method='POST', data={
            'current_password': 'panel-test-only-password', 'new_password': 'new-panel-only-password'})
        self.assertEqual(response['status'], 200)
        self.assertTrue(response['data']['authenticated'])
        self.assertNotEqual(self.cookie, old_cookie)
        self.assertNotEqual(self.csrf, old_csrf)
        self.assertIsNone(self.app.access.read_session(old_cookie))
        self.assertTrue(self.app.access.read_session(self.cookie)['authenticated'])
        self.assertEqual(self.service.calls, [])
        self.assertNotIn(b'new-panel-only-password', response['body'])
        self.assertNotIn('new-panel-only-password', (self.path / 'web_access.json').read_text())
        self.assertEqual(self.request('/api/start', method='POST', csrf=old_csrf)['status'], 403)
        self.app = WebApplication(self.service, self.path)
        self.assertEqual(self.request('/api/state')['status'], 200)
        self.app.access.login('new-panel-only-password', 'new-offline-peer')

    def test_panel_password_change_requires_auth_origin_csrf_and_correct_old_password(self):
        payload = {'current_password': 'panel-test-only-password', 'new_password': 'new-panel-only-password'}
        self.request('/api/session')
        self.assertEqual(self.request('/api/session/password', method='POST', data=payload)['status'], 401)
        self.authenticate()
        before = (self.path / 'web_access.json').read_bytes()
        for changes in ({'origin': 'http://wrong.invalid'}, {'csrf': 'invalid'}):
            self.assertEqual(self.request('/api/session/password', method='POST', data=payload, **changes)['status'], 403)
        for _ in range(8):
            response = self.request('/api/session/password', method='POST', data={**payload, 'current_password': 'wrong-current-password'})
            self.assertEqual(response['status'], 400)
            self.assertEqual(response['data']['error']['code'], 'invalid_current_password')
        self.assertEqual(self.request('/api/session/password', method='POST', data=payload)['status'], 429)
        self.assertEqual((self.path / 'web_access.json').read_bytes(), before)
        self.assertEqual(self.request('/api/state')['status'], 200)

    def test_failed_password_replace_keeps_old_disk_state_memory_and_session(self):
        self.authenticate()
        before = (self.path / 'web_access.json').read_bytes()
        memory = json.loads(json.dumps(self.app.access.data))
        cookie = self.cookie
        with patch('mailru_photos.web.os.replace', side_effect=OSError('private-error-do-not-echo')):
            response = self.request('/api/session/password', method='POST', data={
                'current_password': 'panel-test-only-password', 'new_password': 'new-panel-only-password'})
        self.assertEqual(response['status'], 500)
        self.assertNotIn(b'private-error', response['body'])
        self.assertEqual(self.app.access.data, memory)
        self.assertEqual((self.path / 'web_access.json').read_bytes(), before)
        self.assertEqual(self.cookie, cookie)
        self.assertEqual(self.request('/api/state')['status'], 200)
        self.assertEqual(list(self.path.glob('.panel-*')), [])

    def content_fixture(self):
        root = self.path / 'media'
        root.mkdir()
        path = root / 'семья.jpg'
        data = b'\xff\xd8\xff0123456789'
        path.write_bytes(data)
        self.service.content_sources[1] = (root, path, FileIdentity.from_stat(path.stat()))
        return data

    def test_task_content_requires_panel_auth_and_serves_original_bytes_ranges_and_head(self):
        data = self.content_fixture()
        self.assertEqual(self.request('/api/tasks/1/content')['status'], 401)
        self.assertEqual(self.request('/api/tasks/1/content', method='HEAD')['status'], 401)
        self.assertEqual(self.service.calls, [])
        self.authenticate()
        response = self.request('/api/tasks/1/content?download=0')
        self.assertEqual(response['status'], 200)
        self.assertEqual(response['body'], data)
        self.assertEqual(response['headers']['Content-Type'], 'image/jpeg')
        self.assertTrue(response['headers']['Content-Disposition'].startswith('inline;'))
        response = self.request('/api/tasks/1/content?download=1', headers={'Range': 'bytes=3-6'})
        self.assertEqual(response['status'], 206)
        self.assertEqual(response['body'], data[3:7])
        self.assertTrue(response['headers']['Content-Disposition'].startswith('attachment;'))
        self.assertEqual(response['headers']['Content-Range'], f'bytes 3-6/{len(data)}')
        response = self.request('/api/tasks/1/content', method='HEAD', headers={'Range': 'bytes=3-6'})
        self.assertEqual(response['status'], 200)
        self.assertEqual(response['body'], b'')
        self.assertEqual(response['headers']['Content-Length'], str(len(data)))
        response = self.request('/api/tasks/1/content', headers={'Range': 'bytes=999-'})
        self.assertEqual(response['status'], 416)
        self.assertEqual(response['headers']['Content-Range'], f'bytes */{len(data)}')

    def test_content_has_no_arbitrary_path_parameter_and_changed_source_is_not_served(self):
        self.content_fixture()
        self.authenticate()
        for target in ('/api/tasks/1/content?path=/etc/passwd', '/api/tasks/1/content?download=2',
                       '/api/tasks/1/content?download=', '/api/tasks/1/content?download=0&download=1'):
            with self.subTest(target=target):
                self.assertEqual(self.request(target)['status'], 400)
        self.assertEqual(self.service.calls, [])
        self.assertEqual(self.request('/api/tasks/999/content')['status'], 404)
        self.assertEqual(self.request('/api/tasks/../etc/passwd/content')['status'], 404)
        source = self.service.content_sources[1]
        source[1].write_bytes(b'changed file')
        response = self.request('/api/tasks/1/content')
        self.assertEqual(response['status'], 409)
        self.assertNotIn(b'changed file', response['body'])

    def test_failure_to_send_streaming_headers_closes_file_descriptor(self):
        self.content_fixture()
        self.authenticate()
        response = open_task_content(*self.service.content_sources[1])
        descriptor = response._fd
        environ = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/api/tasks/1/content', 'QUERY_STRING': '',
                   'HTTP_HOST': 'localhost:8787', 'HTTP_COOKIE': self.cookie, 'wsgi.url_scheme': 'http'}
        def disconnected(*args):
            raise BrokenPipeError('Offline disconnected browser')
        with patch('mailru_photos.web.open_task_content', return_value=response):
            with self.assertRaises(BrokenPipeError):
                self.app(environ, disconnected)
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    def test_job_listing_is_bounded_and_filter_validated(self):
        self.authenticate()
        self.assertEqual(self.request("/api/jobs?limit=1000000")["status"], 400)
        self.assertEqual(self.request("/api/jobs?offset=-1")["status"], 400)
        self.assertEqual(self.request("/api/jobs?status=deleted")["status"], 400)
        self.assertEqual(self.request("/api/jobs?status=failed&offset=50&limit=50")["status"], 200)
        self.assertEqual(self.service.calls[-1], ("jobs", {"status": "failed", "offset": 50, "limit": 50, "sort": "asc", "sort_by": "created_at"}))
        for status in ('uploaded_unlimited', 'uploaded_regular', 'uploaded_unknown', 'existing', 'skipped'):
            self.assertEqual(self.request('/api/jobs?status=' + status + '&sort=desc')['status'], 200)
            self.assertEqual(self.service.calls[-1], ('jobs', {'status': status, 'offset': 0, 'limit': 50, 'sort': 'desc', 'sort_by': 'created_at'}))
        for field in ('name', 'progress', 'status', 'size', 'created_at', 'completed_at'):
            self.assertEqual(self.request('/api/jobs?sort_by=' + field + '&limit=200')['status'], 200)
            self.assertEqual(self.service.calls[-1][1]['sort_by'], field)

    def test_deletion_preview_and_confirmation_require_auth_csrf_and_session_binding(self):
        self.request('/api/session')
        for route in ('/api/tasks/delete/preview', '/api/tasks/delete'):
            self.assertEqual(self.request(route, method='POST', data={'id': 1, 'token': 'offline-preview-token'})['status'], 401)
        self.authenticate()
        for route in ('/api/tasks/delete/preview', '/api/tasks/delete'):
            self.assertEqual(self.request(route, method='POST', data={'id': 1, 'token': 'offline-preview-token'}, csrf='bad')['status'], 403)
        self.assertEqual(self.service.calls, [])
        response = self.request('/api/tasks/delete/preview', method='POST', data={
            'id': 7, 'delete_cloud': True, 'delete_local': True})
        self.assertEqual(response['status'], 200)
        self.assertEqual(response['data']['token'], 'offline-preview-token')
        options = self.service.calls[-1][1]
        self.assertEqual(options['task_id'], 7)
        self.assertTrue(options['delete_cloud'])
        self.assertTrue(options['delete_local'])
        self.assertEqual(options['owner'], self.csrf)
        response = self.request('/api/tasks/delete', method='POST', data={
            'token': 'offline-preview-token', 'local_confirmation': 'УДАЛИТЬ НАВСЕГДА'})
        self.assertEqual(response['status'], 200)
        self.assertEqual(self.service.calls[-1], ('confirm_deletion', 'offline-preview-token',
                                                {'local_confirmation': 'УДАЛИТЬ НАВСЕГДА', 'owner': self.csrf}))
        self.assertEqual(self.request('/api/tasks/delete', method='POST', data={})['status'], 400)

    def test_scheduler_routes_keep_mutations_behind_authentication_and_csrf(self):
        payload = {'name': 'Ночные фото', 'source_ids': [1, 2], 'time': '23:15',
                   'mode': 'weekly', 'weekdays': [1, 3, 5], 'enabled': True}
        self.request('/api/session')
        self.assertEqual(self.request('/api/schedules/save', method='POST', data=payload)['status'], 401)
        self.authenticate()
        self.assertEqual(self.request('/api/schedules/save', method='POST', data=payload, csrf='bad')['status'], 403)
        self.assertEqual(self.request('/api/schedules')['status'], 200)
        self.assertEqual(self.request('/api/schedules/save', method='POST', data=payload)['status'], 200)
        self.assertEqual(self.service.calls[-1], ('save_schedule', payload))
        self.assertEqual(self.request('/api/schedules/toggle', method='POST', data={'id': 3, 'enabled': False})['status'], 200)
        self.assertEqual(self.service.calls[-1], ('toggle_schedule', 3, False))
        self.assertEqual(self.request('/api/schedules/remove', method='POST', data={'id': 3})['status'], 200)
        self.assertEqual(self.service.calls[-1], ('remove_schedule', 3))
        for value in (True, -1, 0, 2**63, 'bad'):
            self.assertEqual(self.request('/api/schedules/remove', method='POST', data={'id': value})['status'], 400)

    def test_unicode_folder_query_round_trips_as_utf8(self):
        self.authenticate()
        path = '/photos/Фотографии/Семья и друзья'
        response = self.request('/api/folders?path=' + quote(path, safe=''))
        self.assertEqual(response['status'], 200)
        self.assertEqual(response['data']['path'], path)
        self.assertIn('Семья'.encode('utf-8'), response['body'])

    def test_source_routes_forward_explicit_paths_and_keep_legacy_add(self):
        self.authenticate()
        response = self.request('/api/sources/add', method='POST', data={
            'path': '/photos/family', 'alias': 'Family', 'remote_prefix': '/Photos/Family'})
        self.assertEqual(response['status'], 200)
        self.assertEqual(self.service.calls[-1], ('add_folder', '/photos/family', 'Family', '/Photos/Family'))
        response = self.request('/api/sources/update', method='POST', data={
            'id': 7, 'remote_prefix': '/Trips', 'alias': 'Travel'})
        self.assertEqual(response['status'], 200)
        self.assertEqual(self.service.calls[-1], ('update_source', 7, '/Trips', 'Travel'))
        response = self.request('/api/sources/update', method='POST', data={'id': 7, 'remote_prefix': '/Updated'})
        self.assertEqual(response['status'], 200)
        self.assertEqual(self.service.calls[-1], ('update_source', 7, '/Updated', None))
        self.assertEqual(self.request('/api/sources/remove', method='POST', data={'id': 7})['status'], 200)
        self.assertEqual(self.service.calls[-1], ('remove_source', 7))
        for endpoint in ('/api/sources', '/api/add-folder'):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(self.request(endpoint, method='POST', data={
                    'path': '/photos/old', 'alias': 'Old'})['status'], 200)
                self.assertEqual(self.service.calls[-1], ('add_folder', '/photos/old', 'Old', None))

    def test_source_mutations_require_session_csrf_and_valid_fields(self):
        cases = [
            ('/api/sources/add', {'path': '/photos', 'alias': 'Photos', 'remote_prefix': '/Photos'}),
            ('/api/sources/update', {'id': 1, 'remote_prefix': '/Changed'}),
            ('/api/sources/remove', {'id': 1}),
        ]
        self.request('/api/session')
        for endpoint, data in cases:
            with self.subTest(endpoint=endpoint):
                self.assertEqual(self.request(endpoint, method='POST', data=data)['status'], 401)
        self.authenticate()
        for endpoint, data in cases:
            with self.subTest(endpoint=endpoint):
                self.assertEqual(self.request(endpoint, method='POST', data=data, csrf='invalid')['status'], 403)
        invalid = [
            ('/api/sources/add', {'path': '/photos', 'alias': 'Photos'}),
            ('/api/sources/add', {'path': '/photos', 'alias': 'Photos', 'remote_prefix': None}),
            ('/api/sources/update', {'id': 1, 'remote_prefix': '/Photos', 'alias': ''}),
            ('/api/sources/update', {'id': 1, 'remote_prefix': '/Photos\x00private'}),
            ('/api/sources/update', {'id': 1, 'remote_prefix': '/' + 'a' * 2048}),
        ]
        invalid.extend(('/api/sources/remove', {'id': value})
                       for value in (0, -1, True, 1.5, '1x', 2**63, None))
        for endpoint, data in invalid:
            with self.subTest(endpoint=endpoint, data=data):
                self.assertEqual(self.request(endpoint, method='POST', data=data)['status'], 400)
        self.assertEqual(self.service.calls, [])

    def test_invalid_json_and_oversized_requests_do_not_reach_engine(self):
        self.authenticate()
        self.assertEqual(self.request("/api/setup", method="POST", body=b'{"destination":NaN}')["status"], 400)
        self.assertEqual(self.request("/api/setup", method="POST", body=b"x" * (128 * 1024 + 1))["status"], 413)
        self.assertEqual(self.request("/api/setup", method="POST", data={"destination": "/Photos"}, content_type="text/plain")["status"], 415)
        self.assertEqual(self.service.calls, [])

    def test_no_static_path_can_retrieve_state(self):
        for route in ("/state/auth.dat", "/static/../web_access.json", "/static/../../state/auth.dat"):
            self.assertEqual(self.request(route)["status"], 404)

    def test_secure_origin_sets_secure_cookie_and_rejects_other_origins(self):
        self.app = WebApplication(self.service, self.path, public_origin="https://photos.example.invalid")
        response = self.request("/api/session", host="photos.example.invalid")
        self.assertIn("; Secure", response["headers"]["Set-Cookie"])
        response = self.request("/api/session/setup", method="POST", host="photos.example.invalid",
                                origin="https://photos.example.invalid", data={"password": "panel-test-only-password"})
        self.assertEqual(response["status"], 200)
        self.assertEqual(self.request("/api/start", method="POST", host="photos.example.invalid")["status"], 403)

    def test_security_headers_apply_to_api(self):
        response = self.request("/api/session")
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertIn("frame-ancestors 'none'", response["headers"]["Content-Security-Policy"])
        self.assertIn("HttpOnly", response["headers"]["Set-Cookie"])
        self.assertIn("SameSite=Strict", response["headers"]["Set-Cookie"])


if __name__ == "__main__":
    unittest.main()

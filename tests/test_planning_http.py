"""Authenticated HTTP contracts against the actual plan/group controllers."""
from __future__ import annotations

import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from mailru_photos.web import WebApplication
import test_planning_service as planning_fixtures
import test_web as web_fixtures


class PlanningHTTPTests(unittest.TestCase):
    setUpService = planning_fixtures.PlanningServiceTests.setUp
    tearDown = planning_fixtures.PlanningServiceTests.tearDown
    make_service = planning_fixtures.PlanningServiceTests.make_service
    done = planning_fixtures.PlanningServiceTests.done
    put = planning_fixtures.PlanningServiceTests.put
    request = web_fixtures.WebTests.request
    authenticate = web_fixtures.WebTests.authenticate

    def setUp(self):
        self.setUpService()
        self.app = WebApplication(self.service, self.state)
        self.cookie = self.csrf = ''

    def post(self, endpoint, payload):
        result = self.request(endpoint, method='POST', data=payload)
        self.assertEqual(result['status'], 200, result.get('data'))
        return result['data']

    def test_new_routes_require_session_and_csrf(self):
        for endpoint in ('/api/plans', '/api/notifications', '/api/plans/detail?id=' + 'a' * 32):
            self.assertEqual(self.request(endpoint)['status'], 401)
        self.authenticate()
        for endpoint in ('/api/plans/inspect', '/api/plans/build', '/api/plans/activate', '/api/plans/batch/cleanup',
                         '/api/notifications/settings', '/api/notifications/test'):
            self.assertEqual(self.request(endpoint, method='POST', data={}, csrf='invalid')['status'], 403)
        self.assertEqual(self.service.plans.summary()['count'], 0)
        self.assertEqual(self.service.notifications.status()['pending'], 0)

    def test_real_http_plan_preview_group_members_run_and_scoped_queue(self):
        self.authenticate()
        self.put('Лето/кадр.jpg')
        self.put('Зима/кадр.jpg')
        inv = self.post('/api/plans/inspect', {'root': str(self.archive)})['inventory']
        self.done()
        result = self.request('/api/plans/tree?' + urlencode({'inventory_id': inv['id'], 'limit': 1}))
        self.assertEqual(result['status'], 200, result.get('data'))
        self.assertLessEqual(len(result['data']['folders']), 1)
        self.assertEqual(result['data']['inventory']['total_count'], 2)
        plan = self.post('/api/plans/build', {'inventory_id': inv['id'], 'name': 'Архив / фото',
            'remote_prefix': '/Фото', 'settings': {'start_date': '2090-01-01', 'timezone': 'UTC'}})['plan']
        self.done()
        detail = self.request('/api/plans/detail?id=' + plan['id'])['data']['plan']
        self.assertEqual(detail['status'], 'draft')
        self.assertEqual(detail['batch_count'], 1)
        self.post('/api/plans/activate', {'id': plan['id']})
        self.done()
        page = self.request('/api/plans/batches?plan_id=' + plan['id'])['data']
        batch = page['batches'][0]
        source = batch['source_id']
        members = self.request('/api/sources/members?' + urlencode({'id': source, 'limit': 1}))['data']
        self.assertEqual(members['total'], 2)
        self.assertEqual(len(members['members']), 1)
        self.post('/api/plans/batch/start', {'id': batch['id']})
        self.done()
        state = self.request('/api/state?source_id=' + str(source))['data']
        self.assertEqual(state['source_id'], source)
        self.assertEqual(state['stats']['uploaded'], 2)
        self.assertEqual(state['sources'][0]['stats']['cloud_count'], 2)
        rows = self.request('/api/jobs?source_id=' + str(source))['data']
        self.assertEqual(rows['total'], 2)
        self.assertTrue(all(row['source_group_id'] == source for row in rows['jobs']))
        exported = self.request('/api/jobs/export?' + urlencode({'source_id': source, 'format': 'txt', 'fields': 'name', 'header': '0'}))
        self.assertEqual(exported['status'], 200)
        self.assertEqual(exported['body'].decode('utf-8').count('кадр.jpg'), 2)

    def test_smtp_defaults_private_password_and_no_implicit_send(self):
        self.authenticate()
        settings = self.request('/api/notifications')['data']['settings']
        self.assertEqual(settings['username'], planning_fixtures.EMAIL)
        self.assertEqual(settings['host'], 'smtp.mail.ru')
        self.assertFalse(settings['enabled'])
        password = 'offline-fixture-smtp-secret'
        saved = self.post('/api/notifications/settings', {'enabled': True, 'host': 'smtp.example.test', 'port': 465,
            'security': 'ssl', 'username': settings['username'], 'password': password,
            'from_email': settings['username'], 'recipients': [settings['username']], 'sender_name': 'Фотоархив'})
        self.assertTrue(saved['settings']['password_stored'])
        self.assertNotIn(password, str(saved))
        self.assertNotIn('password', saved['settings'])
        self.assertEqual(saved['status']['pending'], 0)
        saved = self.post('/api/notifications/settings', {'sender_name': 'Семейный архив', 'password': ''})
        self.assertTrue(saved['settings']['password_stored'])
        result = self.post('/api/notifications/test', {})
        self.assertTrue(result['queued'])
        self.assertEqual(self.service.notifications.status()['pending'], 1)
        self.assertNotIn(password, str(self.service.snapshot()))

    def test_source_pages_off_page_scope_and_idle_cache(self):
        self.authenticate()
        identifiers = []
        with self.service._queue() as queue:
            for index in range(55):
                directory = self.manual / f'Каталог {index:02d}'
                directory.mkdir()
                source = queue.add_source_group([{'path': str(directory)}],
                    f'Источник {index:02d}', remote_prefix=f'/Manual/{index:02d}')
                identifiers.append(source['id'])
            directory = self.archive / 'Автоматический'
            directory.mkdir()
            queue.add_source_group([{'path': str(directory)}], 'Партия',
                remote_prefix='/Planned', owner_key='plan-batch:' + 'b' * 32)
        self.service._invalidate_stats()
        state = self.request('/api/state')['data']
        self.assertEqual((state['sources_total'], state['manual_sources_total']), (56, 55))
        self.assertEqual(len(state['sources']), 50)
        self.assertNotIn(identifiers[-1], [source['id'] for source in state['sources']])
        selected = self.request('/api/state?source_id=' + str(identifiers[-1]))['data']
        self.assertEqual(selected['source_id'], identifiers[-1])
        self.assertEqual(selected['selected_source']['alias'], 'Источник 54')
        page_url = '/api/sources?' + urlencode({'kind': 'manual', 'limit': 50, 'offset': 50})
        page = self.request(page_url)['data']
        self.assertEqual(page['total'], 55)
        self.assertEqual([item['id'] for item in page['sources']], identifiers[50:])
        with patch.object(self.service, '_queue', side_effect=AssertionError('Idle pages must use cached state')):
            for _ in range(3):
                self.assertEqual(self.request(page_url)['data'], page)
                self.assertEqual(self.request('/api/state?source_id=' + str(identifiers[-1]))['data']['source_id'], identifiers[-1])
        saved = self.post('/api/schedules/save', {'name': 'Разные страницы',
            'source_ids': [identifiers[0], identifiers[-1]], 'time': '23:00',
            'mode': 'weekly', 'weekdays': [1, 7], 'enabled': False})
        self.assertEqual(saved['schedules'][0]['source_ids'], [identifiers[0], identifiers[-1]])
        missing = self.request('/api/state?source_id=999999999')
        self.assertEqual(missing['status'], 404)
        self.assertEqual(missing['data']['error']['code'], 'source_not_found')


if __name__ == '__main__':
    unittest.main()

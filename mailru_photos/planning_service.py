"""Controller integration for one-shot upload plans and private email notices.

PlanStore owns dates and durable batch transitions. The existing controller owns
the single operation slot, scan, Cloud verification and uploader. Neither plans
nor email introduce a second upload implementation.
"""
from __future__ import annotations

import contextlib
import copy
import hmac
from pathlib import Path
import secrets
import time

from .api import ProtocolError, is_auth_failure
from .auth import AuthError, TokenStore
from .batch_cleanup import BatchCleanupJournal, preview as cleanup_preview
from .notifications import NotificationManager
from .plan_inventory import PlanInventory, _identifier
from .plans import PlanStore, parse_local_datetime
from .queue import QueueError, QueueBusyError, QueueConfigurationError
from .transport import Cancelled, TransportError


def _safe_error(error):
    return str(error) if isinstance(error, (ValueError, QueueError, ProtocolError, AuthError, TransportError)) else type(error).__name__


def _page(query, maximum=200, default=50):
    result = {}
    for key, default_value, low, high in (('limit', default, 1, maximum), ('offset', 0, 0, 100000000)):
        value = query.get(key, [str(default_value)])
        if len(value) != 1 or not isinstance(value[0], str) or not value[0].isascii() or not value[0].isdecimal():
            raise ValueError('Некорректная страница списка.')
        number = int(value[0])
        if not low <= number <= high:
            raise ValueError('Некорректная страница списка.')
        result[key] = number
    return result


class PlanningService:
    def _init_planning(self, previous):
        self.inventory = PlanInventory(self.state_dir / 'plan-inventory.sqlite3')
        self.plans = PlanStore(self.state_dir / 'plans.sqlite3', launch=self._start_due_batch,
                               resume_interrupted=self._settings['auto_resume'])
        self.notifications = NotificationManager(self.state_dir)
        self._auto_batch_cleanup = False
        path = self.state_dir / 'batch-cleanup.sqlite3'
        if path.exists():
            with BatchCleanupJournal(path) as journal:
                operation = journal.operation()
                if operation and operation['state'] in {'confirmed', 'running', 'paused'}:
                    self._batch_cleanup_pending = journal.summary()
                    self._auto_batch_cleanup = operation['state'] in {'confirmed', 'running'} and self._settings['auto_resume']
                    self._activity.update(phase='paused', message='Очистка партии сохранена. Нажмите «Продолжить».')
        if self._resume_batch_id:
            try:
                batch = self.plans.batch(self._resume_batch_id)
            except ValueError:
                batch = None
            if batch is None or batch['status'] in {'completed', 'completed_with_errors', 'deleted'}:
                self._resume_batch_id = None
                self._resume_root_ids = None
                self._resume_upload = self._auto_pending = False
            elif previous.get('resume_upload') is False:
                # A saved manual Pause wins even if the upload thread did not
                # get as far as persisting its PlanStore pause checkpoint.
                self._resume_upload = self._auto_pending = False
        self._recover_deleted_bindings()

    def _start_planning(self):
        if self._closed or self.plans is None:
            return
        self._flush_plan_notifications()
        self.notifications.start()
        if self._auto_batch_cleanup:
            self._auto_batch_cleanup = False
            if self._operation is None:
                with contextlib.suppress(ValueError, QueueError):
                    self._resume_batch_cleanup()
        # Activation is user-authorized before it is durable. Complete the
        # source bindings after a process restart without making a new plan.
        if self._operation is None and not self._batch_cleanup_pending:
            offset = 0
            while True:
                page = self.plans.list(limit=200, offset=offset)
                waiting = next((plan for plan in page['plans'] if plan['status'] in {'activating', 'rolling_back'}), None)
                if waiting is not None:
                    self._launch('planning', lambda plan=waiting: self._recover_activation(plan), origin='automatic')
                    break
                offset += len(page['plans'])
                if offset >= page['total'] or not page['plans']:
                    break
        self.plans.start()

    def _close_planning(self):
        for value in (self.plans, self.notifications, self.inventory):
            if value is not None:
                with contextlib.suppress(Exception):
                    value.close()

    def _recover_activation(self, plan):
        if plan['status'] == 'rolling_back':
            self._rollback_activation(plan['id'], plan.get('message') or 'Незавершённая активация отменена.')
            return 'idle', 'Незавершённая активация отменена. Предварительный план сохранён.'
        return self._activate_plan(plan['id'])

    def _invalidate_plans(self):
        with self._stats_lock:
            self._plan_cache.clear()

    def _default_notification_email(self):
        try:
            return TokenStore(self.state_dir / 'auth.dat').load().username
        except AuthError:
            return None

    def notification_settings(self):
        return {'settings': self.notifications.settings(self._default_notification_email()),
                'status': self.notifications.status()}

    def _plan_batches(self, plan_id, *, limit=50, offset=0):
        plan = self.plans.plan(plan_id)
        key = (plan_id, plan['revision'], limit, offset)
        with self._stats_lock:
            cached = self._plan_cache.get(key)
            if cached is None or (self._operation is not None and time.monotonic() - cached[0] >= 5):
                value = self.plans.batches(plan_id, limit=limit, offset=offset)
                if self._configuration is not None:
                    with self._queue() as queue:
                        ids = [row['source_id'] for row in value['batches'] if row['source_id'] is not None]
                        groups = {row['id']: row for row in queue.source_group_stats(ids)} if ids else {}
                    for batch in value['batches']:
                        live = groups.get(batch['source_id'])
                        if batch['status'] in {'running', 'paused', 'deleting'} and live:
                            batch['summary'] = {**batch.get('summary', {}), **live,
                                'total': live['total_count'], 'uploaded': live['uploaded_count'],
                                'failed': live['failed_count'], 'skipped': live['skipped_count'] + live['existing_count']}
                self._plan_cache[key] = (time.monotonic(), value)
                self._plan_cache.move_to_end(key)
                while len(self._plan_cache) > 12:
                    self._plan_cache.popitem(last=False)
            return copy.deepcopy(self._plan_cache[key][1])

    def planning_request(self, method, path, query, data, *, owner=''):
        def qid(name='id'):
            values = query.get(name, [])
            return _identifier(values[0]) if len(values) == 1 else _identifier(None)
        if method == 'GET':
            if path == '/api/notifications':
                return self.notification_settings()
            if path == '/api/plans':
                return {**self.plans.list(**_page(query)), **{'inventories': self.inventory.list(limit=20)['inventories']}}
            if path == '/api/plans/detail':
                return {'plan': self.plans.plan(qid())}
            if path == '/api/plans/batches':
                return self._plan_batches(qid('plan_id'), **_page(query))
            if path == '/api/plans/inventory':
                return {'inventory': self.inventory.summary(qid())}
            if path == '/api/plans/tree':
                parent = query.get('parent_id', [None])[0]
                if parent is not None:
                    if not parent.isascii() or not parent.isdecimal() or int(parent) < 1:
                        raise ValueError('Некорректная папка дерева.')
                    parent = int(parent)
                return self.inventory.tree(qid('inventory_id'), parent, **_page(query, default=100))
        if method != 'POST' or not isinstance(data, dict):
            raise ValueError('Неизвестная команда плана загрузки.')
        if path == '/api/notifications/settings':
            with self._lock:
                if self._closed:
                    raise QueueBusyError('Приложение останавливается.')
                self.notifications.save(data, default_email=self._default_notification_email())
            return self.notification_settings()
        if path == '/api/notifications/test':
            self.notifications.enqueue('test:' + secrets.token_hex(16), 'test', force=True)
            self.notifications.start()
            return {'queued': True}
        if path == '/api/plans/inspect':
            return self.plan_inspect(data.get('root'))
        if path == '/api/plans/exclude':
            with self._lock:
                self._ensure_idle()
                inventory = self.inventory.exclude(data.get('inventory_id'), data.get('folder_id'), data.get('excluded'))
                self.plans.invalidate_inventory(inventory['id'], inventory['revision'])
                self._invalidate_plans()
                return {'inventory': inventory}
        if path == '/api/plans/build':
            return self.plan_build(data)
        if path == '/api/plans/activate':
            return self.plan_activate(data.get('id'))
        if path == '/api/plans/batch/start' or path == '/api/plans/batch/resume':
            return self.plan_start_batch(data.get('id'))
        if path == '/api/plans/batch/pause':
            return self.plan_pause_batch(data.get('id'))
        if path == '/api/plans/batch/cleanup-preview':
            return self.plan_cleanup_preview(data.get('id'), owner=owner)
        if path == '/api/plans/batch/cleanup':
            return self.plan_cleanup(data.get('id'), data.get('token'), owner=owner)
        with self._lock:
            # Dates/order of queued batches may be changed while another batch
            # runs. PlanStore prevents changing running/completed batches.
            if self._closed:
                raise QueueBusyError('Приложение останавливается.')
            identifier = _identifier(data.get('id'))
            if path == '/api/plans/reorder':
                self.plans.reorder(identifier, data.get('batch_ids'))
            elif path == '/api/plans/batch/move':
                if 'before_id' in data:
                    self.plans.move_batch(identifier, before_id=data['before_id'])
                else:
                    self.plans.move_batch(identifier, direction=data.get('direction'))
            elif path == '/api/plans/schedule':
                self.plans.schedule(identifier, data.get('settings'))
            elif path == '/api/plans/recalculate':
                self.plans.recalculate(identifier)
            elif path == '/api/plans/batch-time':
                batch = self.plans.batch(identifier)
                plan = self.plans.plan(batch['plan_id'])
                timestamp = parse_local_datetime(data.get('scheduled_local'), plan['settings']['timezone'])
                self.plans.set_batch_time(identifier, timestamp)
            elif path in {'/api/plans/batch/delete', '/api/plans/delete'}:
                self._ensure_idle()
                self._ensure_no_deletion()
                if path.endswith('/batch/delete'):
                    self.plans.remove_batch(identifier)
                else:
                    plan = self.plans.plan(identifier)
                    if plan['status'] in {'activating', 'rolling_back'}:
                        self._rollback_activation(identifier, 'План отменён пользователем.')
                    self.plans.remove_plan(identifier)
                self._recover_deleted_bindings()
                if self._resume_batch_id == identifier:
                    self._resume_batch_id = self._resume_root_ids = None
                    self._resume_upload = self._auto_pending = False
                    self._persist_activity()
                self._invalidate_stats()
            else:
                raise ValueError('Неизвестная команда плана загрузки.')
            self._invalidate_plans()
            return {'ok': True}

    def plan_inspect(self, root):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            path = self._allowed_path(root)
            extensions = None
            if self._configuration is not None:
                with self._queue() as queue:
                    extensions = queue.discovery_extensions()
            item = self.inventory.create(path, extensions)
            def inspect():
                def progress(report):
                    with self._lock:
                        self._activity['planning'] = report
                        self._activity['message'] = f"Предварительный обзор: найдено файлов {report.get('seen_count', report.get('files', 0)):,}."
                self.inventory.scan(item['id'], stop=self._stop, progress=progress)
                summary = self.inventory.summary(item['id'])
                self._check_stop()
                if summary['status'] != 'ready':
                    return 'idle', 'Обзор каталога сохранён. Проверьте ошибки доступа перед построением плана.'
                return 'idle', f"Обзор каталога завершён. Файлов: {summary['total_count']:,}. Выберите папки для плана."
            self._launch('planning', inspect)
            return {'inventory': self.inventory.summary(item['id'])}

    def plan_build(self, data):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            inventory = self.inventory.summary(data.get('inventory_id'))
            self._allowed_path(inventory['root'])
            if inventory['status'] != 'ready' or inventory.get('selected_errors'):
                raise ValueError('Завершите обзор и исключите недоступные папки перед построением плана.')
            arguments = {'inventory_id': inventory['id'], 'root': inventory['root'], 'name': data.get('name'),
                         'settings': data.get('settings'), 'remote_prefix': data.get('remote_prefix')}
            replacing = data.get('replace_draft_id')
            plan = self.plans.update_draft(replacing, **arguments) if replacing else self.plans.create(**arguments)
            def build():
                def progress(report):
                    with self._lock:
                        self._activity['planning'] = report
                        self._activity['message'] = 'Распределяем папки по партиям и рассчитываем календарь…'
                result = self.plans.build(plan['id'], self.inventory, stop=self._stop, progress=progress)
                self._invalidate_plans()
                return 'idle', f"План рассчитан. Партий: {result['batch_count']:,}. Проверьте и активируйте его."
            self._launch('planning', build)
            return {'plan': self.plans.plan(plan['id'])}

    def plan_activate(self, identifier):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            plan = self.plans.plan(identifier)
            inventory = self.inventory.summary(plan['inventory_id'])
            self._allowed_path(plan['root'])
            self._ensure_configuration()
            self.plans.activate(identifier, inventory_revision=inventory['revision'])
            self._launch('planning', lambda: self._activate_plan(identifier))
            return {'plan': self.plans.plan(identifier)}

    def _activate_plan(self, identifier):
        try:
            plan = self.plans.plan(identifier)
            with self._queue() as queue, queue.exclusive_run():
                offset = 0
                while True:
                    page = self.plans.batches(identifier, limit=200, offset=offset)
                    if not page['batches']:
                        break
                    bindings = []
                    for batch in page['batches']:
                        self._check_stop()
                        # Only directory definitions, never file rows. Queue
                        # bounds members to 50k and the packer observes it.
                        members = list(self.plans.iter_members(batch['id']))
                        for member in members:
                            self._allowed_path(member['path'])
                        label = plan['name'].replace('/', '∕').replace('\\', '∖')
                        alias = (label[:150] + ' · ' + batch['name'][:60] + ' · ' + batch['id'][:8])
                        source = queue.add_source_group(members, alias, plan['remote_prefix'],
                                                        owner_key='plan-batch:' + batch['id'])
                        bindings.append({'batch_id': batch['id'], 'source_id': source['id']})
                        with self._lock:
                            self._activity['message'] = f"Созданы исходные папки для партии {batch['position'] + 1} из {plan['batch_count']}."
                    self.plans.bind_sources(identifier, bindings)
                    offset += len(page['batches'])
                self.plans.commit_activation(identifier)
            self._invalidate_stats()
            self._invalidate_plans()
            return 'idle', 'План активирован. Партии будут запускаться по календарю.'
        except Cancelled:
            # A pause is reviewable: return to the same preview, keeping no
            # half-created source groups. Process shutdown resumes activation.
            if not self._shutting_down:
                self._rollback_activation(identifier, 'Активация остановлена. Предварительный план сохранён.')
            raise
        except Exception as error:
            self._rollback_activation(identifier, 'Не удалось активировать план: ' + _safe_error(error))
            raise

    def _rollback_activation(self, identifier, message):
        # Persist the cancellation before the first removal. A restart must
        # finish this rollback, never silently reactivate a manually stopped plan.
        self.plans.begin_activation_rollback(identifier, message)
        with self._queue() as queue, queue.exclusive_run():
            source_by_owner = {source['owner_key']: source['id'] for source in queue.source_groups() if source.get('owner_key')}
            offset = 0
            while True:
                page = self.plans.batches(identifier, limit=200, offset=offset)
                if not page['batches']:
                    break
                for batch in page['batches']:
                    source_id = source_by_owner.get('plan-batch:' + batch['id'])
                    if source_id is not None:
                        queue.remove_source_group(source_id)
                offset += len(page['batches'])
            self.plans.abort_activation(identifier, message)
        self._invalidate_stats()
        self._invalidate_plans()

    def _recover_deleted_bindings(self):
        if self._configuration is None or self.plans is None:
            return
        while True:
            values = self.plans.deleted_bindings(limit=200)['bindings']
            if not values:
                return
            with self._queue() as queue, queue.exclusive_run():
                for value in values:
                    try:
                        source = queue.source_group(value['source_id'])
                    except QueueConfigurationError:
                        source = None
                    if source:
                        if source.get('owner_key') != value['owner_key']:
                            raise QueueError('Исходная папка партии изменилась; автоматическое удаление остановлено.')
                        queue.remove_source_group(source['id'])
                    self.plans.ack_deleted_binding(value['batch_id'])

    def _start_due_batch(self, batch):
        with self._lock:
            if (self._closed or self._operation is not None or self._logout_requested or not self._authenticated
                    or self._deletion_pending or self._batch_cleanup_pending or self._auto_pending):
                return False
            self._launch_batch(batch['id'], manual=False)
            return True

    def plan_start_batch(self, identifier, *, automatic=False, job_id=None):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            if not self._authenticated:
                raise ValueError('Для загрузки партии войдите в Mail.ru.')
            return self._launch_batch(identifier, manual=True, job_id=job_id,
                                      origin='automatic' if automatic else 'manual')

    def _launch_batch(self, identifier, *, manual, job_id=None, origin='planned'):
        before = self.plans.batch(identifier)
        retry_cycle = before['status'] in {'completed', 'completed_with_errors'}
        with self._queue() as queue:
            group = queue.source_group(before['source_id'])
            if group.get('owner_key') != 'plan-batch:' + identifier:
                raise QueueError('Связь партии с исходной папкой нарушена. Загрузка не начата.')
            roots = queue.group_root_ids([group['id']])
            if job_id is not None:
                job = queue.get_job(job_id)
                if job is None or job['root_id'] not in roots:
                    raise ValueError('Выбранная задача не относится к этой партии.')
            elif retry_cycle:
                stats = queue.stats(root_ids=roots)
                if not stats.get('failed') and not stats.get('remaining') and not stats.get('paused'):
                    raise ValueError('Партия уже завершена. Для нового прохода сначала просканируйте её исходную папку.')
        batch = self.plans.begin(identifier, manual=manual, retry=retry_cycle)
        self._resume_job_id = job_id
        self._resume_selection = None
        self._resume_root_ids = roots
        self._resume_occurrence_id = None
        self._resume_batch_id = identifier
        try:
            if retry_cycle and job_id is None:
                with self._queue() as queue, queue.exclusive_run():
                    queue.retry_failed(include_permanent=True, root_ids=roots)
            result = self._launch('upload', lambda: self._batch_pipeline(batch, roots, scan_first=before['started_at'] is None,
                                                                         job_id=job_id),
                                  upload_intent=True, origin=origin)
            self._invalidate_plans()
            return result
        except Exception:
            with contextlib.suppress(Exception):
                self.plans.pause(identifier, message='Не удалось начать загрузку. Партию можно возобновить.')
            raise

    def _batch_summary(self, source_id):
        with self._queue() as queue:
            stats = queue.stats(root_ids=queue.group_root_ids([source_id]))
            samples = queue.list_jobs('failed', limit=10, root_ids=queue.group_root_ids([source_id]))
        return {**stats, 'total_count': stats['total'], 'uploaded_count': stats['uploaded'],
                'existing_count': stats.get('existing_total_count', 0), 'failed_count': stats['failed'],
                'skipped_count': max(0, stats['skipped'] - stats.get('existing_total_count', 0)),
                'errors': [row.get('relative_path', '') + ': ' + (row.get('last_error') or 'Ошибка загрузки') for row in samples]}

    def _batch_pipeline(self, batch, roots, *, scan_first, job_id=None):
        try:
            with self._queue() as queue, queue.exclusive_run():
                queue.set_active_batch(batch['id'], batch['source_id'])
            result = self._pipeline(job_id=job_id, root_ids=roots, scan_first=scan_first)
            summary = self._batch_summary(batch['source_id'])
            if summary.get('remaining') or summary.get('paused'):
                self.plans.pause(batch['id'], summary, 'Остались ожидающие или приостановленные задачи. Продолжите партию.')
                return 'paused', 'Партия приостановлена. Остальные партии ожидают её завершения.'
            status = 'completed_with_errors' if summary['failed'] else 'completed'
            message = result[1]
            if summary['total'] < batch['file_count']:
                message += f" В текущем списке {summary['total']} задач; при предварительном обзоре было {batch['file_count']} файлов. "
                message += 'Удалённые из очереди задачи не восстанавливаются при продолжении.'
            self.plans.finish(batch['id'], status, summary, message)
            with self._lock:
                self._resume_upload = self._auto_pending = False
                self._resume_batch_id = self._resume_root_ids = None
            self._flush_plan_notifications()
            return result
        except Cancelled:
            self.plans.pause(batch['id'], self._batch_summary(batch['source_id']), 'Партия приостановлена; очередь сохранена.')
            raise
        except Exception as error:
            if isinstance(error, TransportError) and self._stop.is_set() and (self._shutting_down or self._pause_requested):
                self.plans.pause(batch['id'], self._batch_summary(batch['source_id']), 'Партия приостановлена; очередь сохранена.')
                raise Cancelled('Загрузка остановлена; состояние партии сохранено.') from None
            with contextlib.suppress(Exception):
                self.plans.pause(batch['id'], self._batch_summary(batch['source_id']), _safe_error(error))
            with self._lock:
                self._resume_upload = self._auto_pending = False
            self._enqueue_attention(batch, _safe_error(error))
            raise
        finally:
            with contextlib.suppress(Exception):
                with self._queue() as queue, queue.exclusive_run():
                    queue.set_active_batch(None)
            self._invalidate_plans()

    def plan_pause_batch(self, identifier):
        with self._lock:
            batch = self.plans.batch(identifier)
            if self._operation is not None and self._resume_batch_id == identifier:
                return self.pause()
            self.plans.pause(identifier)
            self._invalidate_plans()
            return {'ok': True, 'batch': self.plans.batch(identifier)}

    def _enqueue_attention(self, batch, message):
        with contextlib.suppress(Exception):
            plan = self.plans.plan(batch['plan_id'])
            self.notifications.enqueue('attention:' + batch['id'] + ':' + str(batch['attempt']), 'plan_attention',
                {'plan_name': plan['name'], 'batch_name': batch['name'], 'message': message})

    def _flush_plan_notifications(self):
        with self._lock:
            if self._closed:
                return
            self._flush_plan_notifications_locked()

    def _flush_plan_notifications_locked(self):
        if self.plans is None or self.notifications is None:
            return
        try:
            while True:
                events = self.plans.completed_events(limit=100)
                if not events:
                    break
                for event in events:
                    batch, plan = event['batch'], event['plan']
                    summary = batch.get('summary', {})
                    self.notifications.enqueue(event['key'], 'batch_errors' if batch['status'] == 'completed_with_errors' else 'batch_completed',
                        {'plan_name': plan['name'], 'batch_name': batch['name'], 'summary': summary,
                         'message': batch.get('message', ''), 'errors': summary.get('errors', [])})
                    try:
                        current = self.plans.plan(plan['id'])
                    except ValueError:
                        current = None  # Deleted plans must not block later completion events.
                    if current and current['status'] in {'completed', 'completed_with_errors'}:
                        self.notifications.enqueue('plan:' + plan['id'] + ':complete:' + str(current.get('completion_cycle', 0)), 'plan_completed',
                            {'plan_name': plan['name'], 'message': f"Партий завершено: {current['completed_count']} из {current['batch_count']}. "
                                f"Из них партий с ошибками: {current.get('errors_count', 0)}.",
                             'summary': {'total_count': current['file_count'], 'total_bytes': current['total_bytes']}})
                    self.plans.ack_event(event['key'])
        except Exception:
            # A notification disk/SMTP problem never changes upload success.
            # Unacknowledged events remain durable for the next startup/event.
            with contextlib.suppress(Exception):
                self._event('Не удалось поставить уведомление в очередь. Результаты загрузки сохранены.', 'warning')

    def plan_cleanup_preview(self, identifier, *, owner=''):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            batch = self.plans.batch(identifier)
            if batch['status'] != 'paused':
                raise ValueError('Сначала остановите партию и дождитесь завершения текущих задач.')
            if not self._authenticated:
                raise ValueError('Для перемещения в корзину войдите в Mail.ru.')
            with self._queue() as queue, queue.exclusive_run():
                value = cleanup_preview(queue, identifier)
            token = secrets.token_urlsafe(32)
            self._batch_cleanup_previews = {key: item for key, item in self._batch_cleanup_previews.items() if item['expires_at'] > time.time()}
            if len(self._batch_cleanup_previews) >= 16:
                self._batch_cleanup_previews.pop(next(iter(self._batch_cleanup_previews)))
            self._batch_cleanup_previews[token] = {**value, 'owner': owner, 'expires_at': time.time() + 300}
            return {'token': token, 'count': value['count'], 'total_bytes': value['total_bytes']}

    def plan_cleanup(self, identifier, token, *, owner=''):
        with self._lock:
            self._ensure_idle()
            self._ensure_no_deletion()
            if not isinstance(token, str):
                raise ValueError('Подтвердите очистку партии заново.')
            value = self._batch_cleanup_previews.get(token)
            if (not value or value['expires_at'] < time.time() or value['batch_id'] != identifier
                    or not hmac.compare_digest(str(value['owner']), str(owner))):
                raise ValueError('Подтверждение истекло. Откройте очистку партии заново.')
            if self.plans.batch(identifier)['status'] != 'paused' or not self._authenticated:
                raise ValueError('Партия должна быть остановлена, а аккаунт Mail.ru подключён.')
            with self._queue() as queue, queue.exclusive_run(), BatchCleanupJournal(self.state_dir / 'batch-cleanup.sqlite3') as journal:
                self._batch_cleanup_pending = journal.prepare(queue, value)
            self._batch_cleanup_previews.pop(token, None)
            return self._resume_batch_cleanup()

    def _resume_batch_cleanup(self):
        with self._lock:
            self._ensure_idle()
            if not self._authenticated:
                raise ValueError('Для продолжения очистки войдите в Mail.ru.')
            identifier = self._batch_cleanup_pending['batch_id']
            if self.plans.batch(identifier)['status'] == 'paused':
                self.plans.begin_cleanup(identifier)
            self._resume_batch_id = identifier
            self._resume_job_id = self._resume_occurrence_id = None
            return self._launch('deleting', self._perform_batch_cleanup)

    def _perform_batch_cleanup(self):
        api = None
        identifier = self._batch_cleanup_pending['batch_id']
        with self._queue() as queue, queue.exclusive_run(), BatchCleanupJournal(self.state_dir / 'batch-cleanup.sqlite3') as journal:
            journal.set_state('running')
            try:
                api = self._new_api()
                account = api.require_account(queue.account_id)
                while True:
                    page = journal.pending()
                    if not page:
                        break
                    for row in page:
                        self._check_stop()
                        try:
                            ownership = queue.batch_cloud_object_state(identifier, row['id'])
                            if ownership['state'] == 'deleted':
                                # The server and receipt checkpoint may have
                                # completed before the journal write/restart.
                                journal.finish_item(row['id'])
                                continue
                            current = ownership.get('object') or {}
                            fields = ('id', 'batch_id', 'account_id', 'remote_path', 'cloud_hash', 'cloud_size', 'remote_id')
                            if ownership['state'] != 'current' or any(current.get(key) != row.get(key) for key in fields):
                                raise ValueError('Очистка пропущена: подтверждение создания файла этой партией изменилось.')
                            if row['account_id'] != account.email:
                                raise QueueError('Аккаунт не совпадает с подтверждением загрузки партии.')
                            result = api.trash_file(row['remote_path'], expected_hash=row['cloud_hash'],
                                                    expected_size=row['cloud_size'], expected_node_id=row['remote_id'])
                            if not result.get('absent'):
                                raise ProtocolError('Облако не подтвердило перемещение файла в корзину.')
                            if not queue.mark_batch_cloud_deleted(identifier, row['id'], expected_hash=row['cloud_hash'], expected_path=row['remote_path']):
                                raise ValueError('Объект отсутствует в Облаке, но подтверждение партии изменилось. Проверьте результат очистки.')
                            journal.finish_item(row['id'])
                        except Cancelled:
                            raise
                        except (ProtocolError, TransportError, ValueError) as error:
                            if is_auth_failure(error):
                                raise
                            journal.finish_item(row['id'], error=_safe_error(error))
                    with self._lock:
                        self._batch_cleanup_pending = journal.summary()
                        self._activity['message'] = f"Перемещение в корзину: {self._batch_cleanup_pending['done']} из {self._batch_cleanup_pending['count']}."
                summary = journal.summary()
                queue.set_metadata('last_scan', {'complete': False, 'reason': 'batch_cloud_cleanup', 'batch_id': identifier})
                journal.set_state('complete')
                message = ('Очистка завершена. Ошибок: ' + str(summary['failed']) + '. Партию можно продолжить.' if summary['failed']
                           else 'Файлы партии перемещены в корзину. При продолжении папки будут проверены заново.')
                self.plans.finish_cleanup(identifier, message)
                self._batch_cleanup_pending = None
                self._invalidate_stats()
                return 'paused', message
            except BaseException:
                journal.set_state('running' if self._shutting_down else 'paused', 'Очистка прервана; подтверждённый список сохранён.')
                with self._lock:
                    self._batch_cleanup_pending = journal.summary()
                raise
            finally:
                if api is not None:
                    api.close_thread()
                self._invalidate_plans()

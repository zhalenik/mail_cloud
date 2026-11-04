/* Offline UI regression suite. No Mail, SMTP, credentials, or external requests.
 * Run: NODE_PATH=/path/to/node_modules node tests/browser_plans_v090.cjs
 * Optional: CHROMIUM_PATH, UI_ARTIFACT_DIR. Requires Playwright + Chromium.
 */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '..');
const output = path.resolve(process.env.UI_ARTIFACT_DIR || '/tmp/mail-cloud-ui-v090-artifacts');
fs.mkdirSync(output, { recursive: true });
const report = { suite: 'v0.9.0 browser UI', mode: 'offline API fixtures', live_mail_requests: 0, live_smtp_requests: 0, checks: [], screenshots: [], status: 'running' };
const inputFiles = ['mailru_photos/web_static/app.js','mailru_photos/web_static/index.html','mailru_photos/web_static/style.css','tests/browser_plans_v090.cjs'];
report.source_files = Object.fromEntries(inputFiles.map(file => [file,crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex')]));
report.started_at = new Date().toISOString();
const clone = value => JSON.parse(JSON.stringify(value));
const now = 1893456000;
const counts = { total: 4, pending: 1, running: 0, uploaded: 1, failed: 0, skipped: 2, existing: 1, total_bytes: 4100, uploaded_bytes: 1000, bytes_by_status: { pending: 1000, uploaded: 1000, skipped: 2100 } };
const source = { id: 1, alias: 'Архив семьи', path: '/photos/Семья', remote_prefix: '/Архив/Семья', member_count: 1, members_preview: [{ id: 1, path: '/photos/Семья', remote_subdir: '', recursive: true }], owner_key: null, stats: { total_count: 4, total_bytes: 4100, cloud_count: 2, cloud_bytes: 2000, uploaded_count: 1, uploaded_bytes: 1000, pending_count: 1, failed_count: 0 } };
const inventory = { id: 'inventory-1', root: '/photos/Большой архив', status: 'ready', revision: 1, total_count: 300000, total_bytes: 1500000000000, selected_count: 300000, selected_bytes: 1500000000000, directory_count: 1234, error_count: 0, selected_errors: 0, ignored_files: 21, ignored_symlinks: 0 };
const settings = { target_count: 10000, target_bytes: 21474836480, tolerance_percent: 15, interval_days: 2, weekdays: [1, 2, 3, 4, 5, 6, 7], start_date: '2030-01-02', time: '02:00', timezone: 'Europe/Moscow' };
const model = {
  state: { version: '0.9.0-test', authenticated: true, account: 'demo@example.test', stats: clone(counts), sources: [clone(source)], settings: { workers: 2, auto_resume: true, regular_policy: 'skip', unexpected_unlimited_action: 'trash', remote_reconcile: true, timezone: 'Europe/Moscow' }, activity: { phase: 'idle', current_files: [] }, scheduler: { enabled: 0, queued: 0 }, plans: { count: 0, active: 0, paused: 0, running: 0, next_run: null, error: null }, task_controls_available: true, queue_clear_available: true, local_delete_available: false, quota_refresh_available: true, quota: { used_bytes: 1000, total_bytes: 20000000000, free_bytes: 19999999000, updated_at: now }, storage_plan: { unlimited: { count: 1, bytes: 1000 }, regular: { count: 1, bytes: 1100 }, uploaded: { count: 1, bytes: 1000 }, uploaded_unlimited: { count: 1, bytes: 1000 }, uploaded_regular: { count: 0, bytes: 0 }, uploaded_unknown: { count: 0, bytes: 0 }, existing: { count: 1, bytes: 1000 }, existing_regular: { count: 1, bytes: 1000 } } },
  plans: [], batches: [], inventories: [], schedules: [], excluded: false, excludedFolder: 2, largeTree: false,
  notifications: { enabled: false, host: 'smtp.example.test', port: 465, security: 'ssl', username: 'demo@example.test', password_stored: true, sender_name: 'Архив', from_email: 'demo@example.test', recipients: ['demo@example.test'], events: ['batch_completed', 'batch_errors', 'plan_completed', 'plan_attention'] }
};
const jobs = [
  { id: 1, root_id: 1, relative_path: 'IMG_001.jpg', local_path: '/photos/Семья/IMG_001.jpg', remote_path: '/Архив/Семья/IMG_001.jpg', cloud_path: '/Другой каталог/IMG_001.jpg', cloud_path_matches: false, cloud_present: true, cloud_url: 'https://cloud.mail.ru/home/Другой%20каталог/IMG_001.jpg', preview_url: '/api/tasks/1/preview', status: 'skipped', skip_reason: 'remote_exists', planned_mode: 'regular', observed_mode: 'regular', planned_reason: 'outside_documented_formats', size: 1000, created_at: now, completed_at: now, progress_bytes: 1000 },
  { id: 2, root_id: 1, relative_path: 'IMG_002.jpg', local_path: '/photos/Семья/IMG_002.jpg', remote_path: '/Архив/Семья/IMG_002.jpg', cloud_path: '/Архив/Семья/IMG_002.jpg', cloud_present: true, cloud_url: 'https://cloud.mail.ru/home/Архив/Семья/IMG_002.jpg', preview_url: '/api/tasks/2/preview', status: 'uploaded', planned_mode: 'regular', observed_mode: 'regular', size: 1000, created_at: now, completed_at: now, progress_bytes: 1000 }
];
const calls = [], unexpected = [], errors = [];
function batch(id, position, count = 10000) { return { id, plan_id: 'plan-1', position, name: 'Партия ' + (position + 1), status: 'queued', scheduled_for: now + 86400 * (2 * position + 1), started_at: null, finished_at: null, source_id: 100 + position, file_count: count, total_bytes: count * 2000000, oversized: count > 11500, member_count: 2, members_preview: [{ path: '/photos/Большой архив/Папка ' + (position + 1) }], summary: {}, message: '' }; }
function planReply() {
  if (!model.plans.length) return;
  const plan = model.plans[0]; plan.batch_count = model.batches.length; plan.completed_count = model.batches.filter(x => x.status === 'completed').length;
  plan.paused_count = model.batches.filter(x => x.status === 'paused').length; plan.running_count = model.batches.filter(x => x.status === 'running').length;
  plan.queued_count = model.batches.filter(x => x.status === 'queued').length;
  model.state.plans = { count: 1, active: plan.status === 'draft' ? 0 : 1, paused: plan.paused_count ? 1 : 0, running: plan.running_count ? 1 : 0, next_run: plan.next_run, error: null };
}
async function routeAPI(route) {
  const request = route.request(), u = new URL(request.url()), method = request.method(), pathname = u.pathname;
  const body = method === 'POST' ? request.postDataJSON() || {} : null;
  calls.push({ pathname, method, query: Object.fromEntries(u.searchParams), body });
  let result;
  if (pathname === '/api/session') result = { authenticated: true, setup_required: false, csrf: 'offline-fixture-csrf' };
  else if (pathname === '/api/state') {
    planReply(); const selectedId = Number(u.searchParams.get('source_id') || 0);
    const selected = model.state.sources.find(x => x.id === selectedId);
    if (selectedId && !selected) { await route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ error: { message: 'Источник не найден', code: 'source_not_found' } }) }); return; }
    result = { ...model.state, sources: model.state.sources.slice(0, 50), sources_total: model.state.sources.length,
      manual_sources_total: model.state.sources.filter(x => !x.owner_key).length, ...(selected ? { selected_source: selected } : {}) };
  }
  else if (pathname === '/api/sources') {
    const offset = Number(u.searchParams.get('offset') || 0), limit = Number(u.searchParams.get('limit') || 50), query = (u.searchParams.get('q') || '').toLocaleLowerCase('ru');
    const items = model.state.sources.filter(x => (u.searchParams.get('kind') !== 'manual' || !x.owner_key) && (!query || [x.alias, ...(x.members_preview || []).map(m => m.path)].some(v => String(v || '').toLocaleLowerCase('ru').includes(query))));
    result = { sources: items.slice(offset, offset + limit), total: items.length, offset, limit };
  }
  else if (pathname === '/api/schedules') result = { schedules: model.schedules, timezone: 'Europe/Moscow' };
  else if (pathname === '/api/schedules/save') { const entry = { ...body, id: body.id || 1, next_run: now + 86400 }; model.schedules = [entry]; result = { schedules: model.schedules, timezone: 'Europe/Moscow' }; }
  else if (pathname === '/api/jobs') result = { jobs, total: jobs.length, total_bytes: 2000 };
  else if (pathname === '/api/sources/members') { const item = model.state.sources.find(x => x.id === Number(u.searchParams.get('id'))); result = { members: item.members_preview, total: item.member_count }; }
  else if (pathname === '/api/sources/add') { const item = { ...clone(source), id: 2, alias: body.alias, remote_prefix: body.remote_prefix, path: null, member_count: body.paths.length, members_preview: body.paths.map((p, i) => ({ id: 20 + i, path: p, recursive: true, remote_subdir: p.split('/').at(-1) })) }; model.state.sources.push(item); result = { ok: true }; }
  else if (pathname === '/api/scan' || pathname === '/api/start') result = { ok: true };
  else if (pathname === '/api/folders') result = { path: u.searchParams.get('path'), parent: null, roots: [{ path: '/photos', name: 'Фото' }], directories: [{ path: '/photos/Большой архив', name: 'Большой архив' }] };
  else if (pathname === '/api/tasks/delete/preview') result = { token: 'delete-fixture-token', task_count: 2, total_bytes: 2000, cloud_count: 0, local_count: 0, local_delete_allowed: false, warnings: [] };
  else if (pathname === '/api/plans' && method === 'GET') result = { plans: model.plans, inventories: model.inventories, total: model.plans.length };
  else if (pathname === '/api/plans/detail') { planReply(); result = { plan: model.plans[0] }; }
  else if (pathname === '/api/plans/batches') result = { batches: model.batches, total: model.batches.length, offset: 0, limit: 50 };
  else if (pathname === '/api/plans/inspect') { inventory.root = body.root; model.inventories = [inventory]; result = { inventory }; }
  else if (pathname === '/api/plans/inventory') result = { inventory };
  else if (pathname === '/api/plans/tree') {
    const parent = u.searchParams.get('parent_id');
    const folders = !parent ? [{ id: 1, parent_id: null, name: 'Большой архив', path: inventory.root, relative_path: '', child_count: model.largeTree ? 2000 : 3, total_count: inventory.total_count, total_bytes: inventory.total_bytes, selected_count: inventory.selected_count, selected_bytes: inventory.selected_bytes, excluded: false, inherited_excluded: false, error_count: 0 }] : (model.largeTree ? Array.from({ length: 2000 }, (_, i) => i + 2) : [2, 3, 4]).map((id, i) => ({ id, parent_id: 1, name: model.largeTree ? 'Папка ' + id : ['Семья', 'Поездки', '<img src=x onerror=window.fixtureXss=1>'][i], path: inventory.root + '/Папка ' + id, relative_path: 'Папка ' + id, child_count: 0, total_count: 100000, total_bytes: 500000000000, selected_count: id === model.excludedFolder && model.excluded ? 0 : 100000, selected_bytes: id === model.excludedFolder && model.excluded ? 0 : 500000000000, excluded: id === model.excludedFolder && model.excluded, inherited_excluded: false, error_count: 0 }));
    const offset = Number(u.searchParams.get("offset") || 0), limit = Number(u.searchParams.get("limit") || 100);
    result = { inventory, parent, folders: folders.slice(offset, offset + limit), total: folders.length, offset, limit };
  }
  else if (pathname === '/api/plans/exclude') { model.excluded = body.excluded; model.excludedFolder = body.folder_id; inventory.revision += 1; inventory.selected_count = model.excluded ? 200000 : 300000; inventory.selected_bytes = model.excluded ? 1000000000000 : 1500000000000; result = { inventory }; }
  else if (pathname === '/api/plans/build') { model.plans = [{ id: 'plan-1', name: body.name, root: inventory.root, inventory_id: inventory.id, status: 'building', settings: body.settings, remote_prefix: body.remote_prefix, batch_count: 3, file_count: inventory.selected_count, total_bytes: inventory.selected_bytes, completed_count: 0, warnings: ['Одна папка превышает размер партии и включена целиком.'], next_run: now + 86400 }]; model.batches = [batch('batch-1', 0, 150000), batch('batch-2', 1), batch('batch-3', 2)]; model.plans[0].batch_count = 0; setTimeout(() => { model.plans[0].status = 'draft'; model.plans[0].batch_count = 3; }, 200); result = { plan: model.plans[0] }; }
  else if (pathname === '/api/plans/activate') { model.plans[0].status = 'active'; result = { ok: true }; }
  else if (pathname === '/api/plans/batch/move') {
    const index = model.batches.findIndex(x => x.id === body.id), item = model.batches.splice(index, 1)[0];
    const target = body.before_id ? model.batches.findIndex(x => x.id === body.before_id) : body.direction === 'up' ? Math.max(0, index - 1) : Math.min(model.batches.length, index + 1);
    model.batches.splice(target, 0, item); model.batches.forEach((x, i) => { x.position = i; }); result = { ok: true };
  }
  else if (pathname === '/api/plans/batch-time') result = { ok: true };
  else if (pathname === '/api/plans/schedule') { model.plans[0].settings = body.settings; result = { ok: true }; }
  else if (pathname === '/api/plans/recalculate') result = { ok: true };
  else if (pathname === '/api/plans/batch/start' || pathname === '/api/plans/batch/resume') { const item = model.batches.find(x => x.id === body.id); item.status = 'running'; item.started_at = now; item.summary = { total: item.file_count, total_bytes: item.total_bytes, uploaded: 3, skipped: 2, failed: 0, uploaded_bytes: 6000000 }; model.state.activity.phase = 'uploading'; model.state.stats.running = 1; result = { ok: true }; }
  else if (pathname === '/api/plans/batch/pause') { model.batches.find(x => x.id === body.id).status = 'paused'; model.state.activity.phase = 'idle'; model.state.stats.running = 0; result = { ok: true }; }
  else if (pathname === '/api/plans/batch/cleanup-preview') result = { token: 'owned-cleanup-fixture-token', count: 3, total_bytes: 6000000 };
  else if (pathname === '/api/plans/batch/cleanup') result = { ok: true };
  else if (pathname === '/api/plans/batch/delete') { model.batches = model.batches.filter(x => x.id !== body.id); model.batches.forEach((x, i) => { x.position = i; }); result = { ok: true }; }
  else if (pathname === '/api/notifications') result = { settings: model.notifications, status: { pending: 0, sent: 0, failed: 0, last_error: null } };
  else if (pathname === '/api/notifications/settings') { model.notifications = { ...model.notifications, ...body, password_stored: Boolean(body.password) || model.notifications.password_stored }; delete model.notifications.password; result = { ok: true }; }
  else if (pathname === '/api/notifications/test') result = { queued: true };
  else { unexpected.push({ pathname, method }); await route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ error: 'Unmocked API endpoint' }) }); return; }
  await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(clone(result)) });
}
async function waitFor(predicate, message, timeout = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeout) { if (await predicate()) return; await new Promise(resolve => setTimeout(resolve, 30)); }
  throw new Error(message || 'Timed out waiting for UI');
}
function checked(name) { report.checks.push({ name, passed: true }); }
async function screenshot(page, name) { await page.evaluate(() => window.scrollTo(0, 0)); if (!name.includes("failure")) await page.locator("#toast").waitFor({state:"hidden",timeout:8000}).catch(()=>{}); await new Promise(resolve => setTimeout(resolve, 450)); await page.screenshot({ path: path.join(output, name), fullPage: true }); report.screenshots.push(name); }
(async () => {
  const server = http.createServer((req, res) => {
    const p = new URL(req.url, 'http://localhost').pathname;
    const file = p === '/' ? 'index.html' : p === '/static/app.js' ? 'app.js' : p === '/static/style.css' ? 'style.css' : null;
    if (!file) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(root, 'mailru_photos/web_static', file)));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  let browser, page;
  try {
    browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}), args: ['--no-sandbox'] });
    const context = await browser.newContext({ viewport: { width: 1440, height: 1080 }, locale: 'ru-RU' });
    const origin = 'http://127.0.0.1:' + server.address().port;
    await context.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.origin !== origin) { errors.push('Unexpected external request to ' + url.origin); return route.abort(); }
      if (url.pathname.startsWith('/api/')) return routeAPI(route);
      return route.continue();
    });
    page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
    await page.goto(origin); await page.waitForSelector('#app:not([hidden])'); await waitFor(() => page.locator('#version-label').textContent().then(x => x.includes('0.9.0')));
    assert.equal(await page.locator('#stat-existing-regular').count(), 0); assert.equal(await page.locator('#overview-title').textContent(), 'Обзор'); checked('single Overview heading and no existing-object storage split');
    await page.locator('[data-page=queue]').click(); await page.waitForSelector('.task-row');
    const existingTitle = await page.locator('[data-task-id="1"] .task-status-cell').getAttribute('title');
    const existingNameTitle = await page.locator('[data-task-id="1"] .job-name').getAttribute('title');
    assert.ok(existingTitle.includes('Другой каталог')); assert.ok(!/режим|квот|безлимит/i.test(existingTitle + existingNameTitle));
    assert.match(await page.locator('[data-task-id="2"] .task-status-cell').getAttribute('title'), /квоте/); checked('remote_exists hides storage hints while confirmed upload retains them');
    await page.locator('[data-page=sources]').click(); await page.locator('#add-source').click();
    await page.locator('#source-alias').fill('Две папки'); await page.locator('#source-path').fill('/photos/Путешествия'); await page.locator('#source-add-path').click();
    await page.locator('.source-member-path').nth(1).fill('/photos/События'); await page.locator('#source-remote-prefix').fill('/Архив'); await page.locator('#source-submit').click();
    await waitFor(() => page.locator('#source-dialog').evaluate(x => !x.open));
    const added = calls.findLast(x => x.pathname === '/api/sources/add'); assert.deepEqual(added.body.paths, ['/photos/Путешествия', '/photos/События']); checked('source group accepts multiple Unicode server paths');
    await page.locator('[data-source-action=scan][data-source-id="1"]').click();
    await waitFor(() => calls.some(x => x.pathname === '/api/scan' && x.body.source_id === 1));
    assert.ok(await page.locator('#overview-source-scope').isVisible()); assert.equal(await page.locator('#overview-scope-name').textContent(), source.alias); checked('source scan selects scoped Overview and sends source_id');
    await page.locator('[data-page=sources]').click(); await page.locator('[data-source-action=queue][data-source-id="1"]').click();
    await waitFor(() => calls.some(x => x.pathname === '/api/jobs' && x.query.source_id === '1'));
    await page.locator('#clear-queue').click(); await waitFor(() => calls.some(x => x.pathname === '/api/tasks/delete/preview' && x.body.source_id === 1));
    assert.match(await page.locator('#queue-clear-description').textContent(), /Архив семьи/); await page.locator('#queue-clear-dialog [data-close-dialog]').last().click(); checked('scoped queue and bulk-delete preview stay in the selected group');
    await page.locator('[data-page=plans]').click(); await page.locator('#add-plan').click(); await page.locator('#plan-name').fill('Большой семейный архив'); await page.locator('#plan-root').fill(inventory.root); await page.locator('#plan-destination').fill('/Фото/Архив');
    await page.locator('#plan-wizard-next').click(); await waitFor(() => page.locator('.plan-tree-row').count().then(x => x === 4));
    assert.equal(await page.evaluate(() => window.fixtureXss), undefined); assert.ok((await page.locator('#plan-tree-summary').textContent()).replace(/\D/g, '').startsWith('300000')); checked('300000-file inventory renders directory-only lazy tree and escapes folder names');
    await page.locator('[data-folder-id="2"] > .plan-tree-row .tree-toggle').click(); await waitFor(() => calls.some(x => x.pathname === '/api/plans/exclude'));
    await waitFor(() => page.locator('[data-folder-id="2"] > .plan-tree-row .tree-toggle').getAttribute('aria-pressed').then(x => x === 'false'));
    assert.equal(calls.findLast(x => x.pathname === '/api/plans/exclude').body.excluded, true); await screenshot(page, 'plan-tree-desktop.png'); checked('folder exclusion refreshes persisted selection and inherited tree state');
    await page.locator('#plan-wizard-next').click(); await page.locator('#plan-start-date').fill(settings.start_date); await page.locator('#plan-start-time').fill(settings.time); await page.locator('#plan-interval-days').fill('2'); await page.locator('#plan-wizard-next').click();
    await waitFor(() => page.locator('#plan-step-preview').isVisible()); assert.equal(await page.locator('#plan-wizard-next').isEnabled(), false); await waitFor(() => page.locator('#plan-wizard-next').isEnabled()); checked('asynchronous plan construction cannot activate an incomplete draft'); assert.equal(calls.filter(x => x.pathname === '/api/plans/activate').length, 0);
    const build = calls.findLast(x => x.pathname === '/api/plans/build'); assert.equal(build.body.settings.target_count, 10000); assert.equal(build.body.settings.target_bytes, 21474836480); assert.equal(build.body.settings.interval_days, 2); assert.equal(build.body.remote_prefix, '/Фото/Архив');
    assert.ok(await page.locator('#plan-preview-warnings').isVisible()); await screenshot(page, 'plan-preview-desktop.png'); checked('preview passes combined targets/calendar and requires separate activation');
    await page.locator('#plan-wizard-next').click(); await waitFor(() => page.locator('#plan-wizard').evaluate(x => !x.open)); await waitFor(() => page.locator('.batch-card').count().then(x => x === 3));
    assert.equal(calls.filter(x => x.pathname === '/api/plans/activate').length, 1); checked('activation produces persistent batch list');
    await page.locator('[data-batch-id="batch-2"] [data-batch-action=up]').click(); await waitFor(() => calls.some(x => x.pathname === '/api/plans/batch/move'));
    await waitFor(() => page.locator('.batch-card').first().getAttribute('data-batch-id').then(x => x === 'batch-2')); checked('keyboard-friendly reorder updates batch order without full-id download');
    await page.locator('.batch-card[data-batch-id="batch-3"]').dragTo(page.locator('.batch-card[data-batch-id="batch-2"]'));
    await waitFor(() => calls.some(x => x.pathname === '/api/plans/batch/move' && x.body.before_id === 'batch-2')); checked('drag reorder sends stable anchor id');
    await page.locator('.batch-card[data-batch-id="batch-2"] .batch-date').click(); await page.locator('#batch-time-local').fill('2030-01-10T02:00'); await page.locator('#batch-time-save').click(); await waitFor(() => page.locator('#batch-time-dialog').evaluate(x => !x.open));
    assert.equal(calls.findLast(x => x.pathname === '/api/plans/batch-time').body.scheduled_local, '2030-01-10T02:00'); checked('date editor sends timezone-local value for server validation');
    await screenshot(page, 'plans-desktop.png');
    await page.locator('.batch-card[data-batch-id="batch-2"] [data-batch-action=start]').click(); await waitFor(() => page.locator('.batch-card[data-batch-id="batch-2"] [data-batch-action=pause]').count().then(Boolean));
    await page.locator('.batch-card[data-batch-id="batch-2"] [data-batch-action=pause]').click(); await page.locator('#batch-action-trash').check(); await page.locator('#batch-action-confirm').click();
    await waitFor(() => page.locator('#batch-action-title').textContent().then(x => x.includes('Убрать загруженные'))); await waitFor(() => page.locator('#batch-action-confirm').isEnabled());
    assert.equal(calls.filter(x => x.pathname === '/api/plans/batch/cleanup').length, 0); assert.equal(calls.findLast(x => x.pathname === '/api/plans/batch/pause').body.cloud_action, 'keep');
    await page.locator('#batch-action-confirm').click(); await waitFor(() => calls.some(x => x.pathname === '/api/plans/batch/cleanup'));
    assert.equal(calls.findLast(x => x.pathname === '/api/plans/batch/cleanup').body.token, 'owned-cleanup-fixture-token'); checked('pause keeps files until a separate owned-cloud cleanup preview is confirmed');
    await waitFor(() => page.locator('#batch-action-dialog').evaluate(x => !x.open));
    await page.locator('.batch-card[data-batch-id="batch-3"] [data-batch-action=delete]').click(); assert.equal(await page.locator('#batch-action-trash-row').isVisible(), false); assert.match(await page.locator('#batch-action-description').textContent(), /останутся/); await page.locator('#batch-action-dialog [data-close-dialog]').last().click(); checked('batch deletion is explicit and leaves physical files');
    await page.locator('[data-page=settings]').click(); await waitFor(() => page.locator('#smtp-host').inputValue().then(x => x === 'smtp.example.test'));
    assert.equal(await page.locator('#smtp-password').inputValue(), ''); assert.match(await page.locator('#smtp-password').getAttribute('placeholder'), /Сохранён/);
    await page.locator('#notifications-enabled').check(); await page.locator('#smtp-password').fill('fixture-only-smtp-pass'); await page.locator('#smtp-save').click(); await waitFor(() => calls.some(x => x.pathname === '/api/notifications/settings'));
    await waitFor(() => page.locator('#smtp-test').isEnabled()); assert.equal(await page.locator('#smtp-password').inputValue(), ''); assert.equal(calls.filter(x => x.pathname === '/api/notifications/test').length, 0);
    await page.locator('#smtp-test').click(); await waitFor(() => calls.filter(x => x.pathname === '/api/notifications/test').length === 1); checked('SMTP password stays write-only and test email is sent only on explicit click');
    await screenshot(page, 'notifications-desktop.png');
    await page.setViewportSize({ width: 390, height: 844 });
    for (const name of ['settings', 'plans', 'sources', 'queue', 'overview']) {
      await page.locator('[data-page=' + name + ']').click(); await new Promise(resolve => setTimeout(resolve, 120));
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 2); assert.equal(overflow, false, 'Horizontal page overflow on mobile ' + name);
      await screenshot(page, name + '-mobile.png');
    }
    checked('390px mobile pages fit viewport with usable navigation');
    model.largeTree = true; model.excluded = false; inventory.revision += 1;
    await page.setViewportSize({ width: 1440, height: 1080 }); await page.locator('[data-page=plans]').click(); await page.locator('#add-plan').click();
    await page.locator('#plan-name').fill('Большое дерево'); await page.locator('#plan-root').fill(inventory.root); await page.locator('#plan-destination').fill('/Большое дерево'); await page.locator('#plan-wizard-next').click();
    await waitFor(() => page.locator('.plan-tree-row').count().then(x => x === 101));
    for (let i = 0; i < 9; i++) { const before = await page.locator('.plan-tree-row').count(); await page.locator('.tree-more').last().click(); await waitFor(() => page.locator('.plan-tree-row').count().then(x => x > before)); }
    assert.equal(await page.locator('.plan-tree-row').count(), 1000);
    await page.locator('[data-folder-id="150"] > .plan-tree-row .tree-toggle').click();
    await waitFor(() => page.locator('[data-folder-id="150"] > .plan-tree-row .tree-toggle').getAttribute('aria-pressed').then(x => x === 'false'));
    await waitFor(() => page.locator('.plan-tree-row').count().then(x => x === 1000));
    assert.equal(await page.locator('[data-folder-id="999"]').count(), 1); checked('directory DOM is capped at 1000 and loaded tree pages survive exclusions');
    await page.locator('#plan-wizard [data-close-dialog]').click();
    for (let id = 1000; id < 1060; id++) model.state.sources.push({ ...clone(source), id, alias: 'Источник ' + id });
    await page.locator('#refresh-connection').evaluate(button => button.click()); await waitFor(() => page.locator('#stat-sources').textContent().then(x => x === '62'));
    await page.locator('[data-page=sources]').click(); await waitFor(() => page.locator('.source-card').count().then(x => x === 50));
    await page.locator('#sources-next').click(); await waitFor(() => page.locator('.source-card').count().then(x => x === 12));
    assert.ok(calls.some(x => x.pathname === '/api/sources' && x.query.offset === '50' && x.query.limit === '50')); checked('source cards request bounded server pages for large plans');
    await page.locator('[data-source-action=queue][data-source-id="1059"]').click();
    await waitFor(() => page.locator('#queue-scope-name').textContent().then(x => x === 'Источник 1059'));
    await page.locator('#refresh-connection').evaluate(button => button.click()); await waitFor(() => calls.some(x => x.pathname === '/api/state' && x.query.source_id === '1059'));
    assert.equal(await page.locator('#queue-scope-name').textContent(), 'Источник 1059'); assert.ok(await page.locator('#queue-source-scope').isVisible()); checked('source scope survives state refresh when selected source is outside first 50');
    await page.locator('[data-page=schedules]').click(); await page.locator('#add-schedule').click(); await page.locator('#schedule-name').fill('Выбор между страницами');
    await waitFor(() => page.locator('[name="schedule-source"]').count().then(x => x === 50)); await page.locator('[name="schedule-source"][value="1"]').check();
    await page.locator('#schedule-source-next').click(); await waitFor(() => page.locator('[name="schedule-source"][value="1059"]').count().then(Boolean)); await page.locator('[name="schedule-source"][value="1059"]').check();
    await page.locator('#schedule-source-search').fill('АРХИВ'); await waitFor(() => page.locator('[name="schedule-source"]').count().then(x => x === 1));
    assert.equal(await page.locator('[name="schedule-source"][value="1"]').isChecked(), true); assert.match(await page.locator('#schedule-source-selected').textContent(), /2$/);
    await screenshot(page, 'schedule-source-search.png'); await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(await page.locator('#schedule-dialog').evaluate(node => node.scrollWidth > node.clientWidth + 2), false); await screenshot(page, 'schedule-source-search-mobile.png'); checked('paged schedule picker remains within its mobile dialog');
    await page.setViewportSize({ width: 1440, height: 1080 }); await page.locator('#schedule-submit').click(); await waitFor(() => page.locator('#schedule-dialog').evaluate(x => !x.open));
    assert.deepEqual(calls.findLast(x => x.pathname === '/api/schedules/save').body.source_ids.sort((a,b) => a-b), [1, 1059]); checked('schedule source selection survives server paging and Unicode search');
    model.plans[0].status = 'rolling_back'; await page.locator('[data-page=plans]').click();
    await waitFor(() => page.locator('#plan-detail-status').textContent().then(x => x === 'Отмена активации'));
    assert.equal(await page.locator('#plan-configure').isEnabled(), false); assert.equal(await page.locator('#plan-recalculate').isEnabled(), false);
    assert.equal(await page.locator('[data-batch-action=up]:enabled').count(), 0); assert.equal(await page.locator('.batch-card[draggable="true"]').count(), 0); checked('interrupted activation rollback is named and protected from plan edits');
    assert.deepEqual(errors, []); assert.deepEqual(unexpected, []); checked('no browser exceptions or unexpected network/API calls');
    report.files_changed_during_run = inputFiles.filter(file => report.source_files[file] !== crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex'));
    assert.deepEqual(report.files_changed_during_run, []);
    report.status = 'passed'; report.browser = browser.version(); report.check_count = report.checks.length; report.finished_at = new Date().toISOString();
  } catch (error) { if (page) { try { await screenshot(page, 'failure.png'); } catch {} } report.status = 'failed'; report.error = error.stack || String(error); report.browser_errors = errors; report.unexpected_api = unexpected; process.exitCode = 1; }
  finally { if (browser) await browser.close(); await new Promise(resolve => server.close(resolve)); fs.writeFileSync(path.join(output, 'ui-0.9.0.json'), JSON.stringify(report, null, 2) + '\n'); console.log(JSON.stringify({ status: report.status, checks: report.checks.length, report: path.join(output, 'ui-0.9.0.json'), error: report.error }, null, 2)); }
})();

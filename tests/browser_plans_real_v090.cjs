/* Real WebApplication/WebService + real SQLite + browser.
 * Only the remote cloud client is the offline test fixture; SMTP workers never start.
 * NODE_PATH=/path/to/node_modules node tests/browser_plans_real_v090.cjs
 */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '..');
const output = path.resolve(process.env.UI_ARTIFACT_DIR || '/tmp/mail-cloud-ui-v090-real');
fs.mkdirSync(output, { recursive: true });
const report = { suite: 'v0.9.0 real HTTP/browser integration', mode: 'real WebService/SQLite; offline remote Cloud boundary; SMTP delivery disabled', live_mail_requests: 0, live_smtp_requests: 0, checks: [], screenshots: [], started_at: new Date().toISOString(), status: 'running' };
const inputs = ['mailru_photos/web_static/app.js','mailru_photos/web_static/index.html','mailru_photos/web_static/style.css','mailru_photos/web.py','mailru_photos/web_service.py','mailru_photos/planning_service.py','mailru_photos/plans.py','mailru_photos/plan_inventory.py','mailru_photos/queue.py','mailru_photos/batch_cleanup.py','mailru_photos/notifications.py','scripts/browser_fixture_server.py','tests/browser_plans_real_v090.cjs'];
const hashes = () => Object.fromEntries(inputs.map(file => [file, crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex')]));
report.source_files = hashes();
const errors = [], serverErrors = [];
function checked(name) { report.checks.push({ name, passed: true }); }
async function waitFor(predicate, message, timeout = 30000) { const started = Date.now(); while (Date.now() - started < timeout) { if (await predicate()) return; await new Promise(resolve => setTimeout(resolve, 80)); } throw new Error(message || 'Timed out waiting for real UI'); }
async function shot(page, name) { await page.evaluate(() => window.scrollTo(0,0)); if (!name.includes("failure")) await page.locator("#toast").waitFor({state:"hidden",timeout:8000}).catch(()=>{}); await new Promise(resolve => setTimeout(resolve, 450)); await page.screenshot({ path:path.join(output,name),fullPage:true }); report.screenshots.push(name); }
(async () => {
  const server = spawn(process.env.PYTHON || 'python3', ['-u','scripts/browser_fixture_server.py'], { cwd:root, stdio:['ignore','pipe','pipe'] });
  server.stderr.on('data', data => serverErrors.push(data.toString()));
  const metadata = await new Promise((resolve,reject) => {
    const lines = readline.createInterface({ input: server.stdout });
    const timer = setTimeout(() => reject(new Error('Fixture startup timed out: '+serverErrors.join(''))),30000);
    lines.once('line', line => { clearTimeout(timer); try { resolve(JSON.parse(line)); } catch(error) { reject(error); } });
    server.once('exit', code => { clearTimeout(timer); if(code) reject(new Error('Fixture exited: '+serverErrors.join(''))); });
  });
  let browser,page;
  try {
    browser = await chromium.launch({ headless:true, ...(process.env.CHROMIUM_PATH ? {executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox'] });
    const context = await browser.newContext({ viewport:{width:1440,height:1080},locale:'ru-RU' });
    await context.route('**/*', route => {
      if(new URL(route.request().url()).origin !== metadata.origin) { errors.push('Unexpected external browser request'); return route.abort(); }
      return route.continue(); // API requests go to the actual application unchanged.
    });
    page=await context.newPage(); page.on('pageerror', error=>errors.push(error.message));
    page.on('response', response=>{ if(response.url().includes('/api/')&&response.status()>=500) errors.push('HTTP '+response.status()+' '+new URL(response.url()).pathname); });
    await page.goto(metadata.origin); await page.waitForSelector('#access-screen:not([hidden])');
    await page.locator('#access-password').fill('offline-browser-panel-2026'); await page.locator('#access-confirm').fill('offline-browser-panel-2026'); await page.locator('#access-submit').click();
    await page.waitForSelector('#app:not([hidden])'); checked('real panel setup creates authenticated CSRF session');
    const get = async endpoint=>{ const response=await page.request.get(metadata.origin+endpoint); assert.equal(response.status(),200,endpoint+' '+await response.text()); return response.json(); };
    await page.locator('[data-page=sources]').click(); await page.locator('#add-source').click();
    await page.locator('#source-alias').fill('Ручная группа'); await page.locator('#source-path').fill(metadata.manual_paths[0]); await page.locator('#source-add-path').click(); await page.locator('.source-member-path').nth(1).fill(metadata.manual_paths[1]); await page.locator('#source-remote-prefix').fill('/Ручные'); await page.locator('#source-submit').click();
    await waitFor(()=>page.locator('#source-dialog').evaluate(x=>!x.open),'group creation');
    const initial=await get('/api/state'); const manual=initial.sources.find(x=>x.alias==='Ручная группа'); assert.ok(manual); assert.equal(manual.member_count,2);
    const members=await get('/api/sources/members?id='+manual.id); assert.equal(members.total,2); checked('multiple-source form reaches real group/membership controller');
    await page.locator('[data-source-action=scan][data-source-id="'+manual.id+'"]').click();
    await waitFor(async()=>{ const state=await get('/api/state?source_id='+manual.id); return state.activity.phase!=='scanning'&&state.stats.total===2; },'scoped source scan');
    assert.ok(await page.locator('#overview-source-scope').isVisible());
    await page.locator('[data-page=sources]').click(); await page.locator('[data-source-action=queue][data-source-id="'+manual.id+'"]').click(); await waitFor(()=>page.locator('.task-row').count().then(x=>x===2));
    await page.locator('#queue-source-scope [data-clear-source-scope]').click(); checked('source scan and filtered queue use real scoped task totals');
    await page.locator('[data-page=settings]').click(); await waitFor(()=>page.locator('#smtp-host').inputValue().then(Boolean));
    await page.locator('#notifications-enabled').check(); await page.locator('#smtp-host').fill('smtp.example.test'); await page.locator('#smtp-password').fill('offline-only-smtp-fixture'); await page.locator('#smtp-save').click();
    await waitFor(async()=>{const n=await get('/api/notifications');return n.settings.enabled&&n.settings.password_stored;});
    const notificationState=await get('/api/notifications'); assert.ok(!Object.hasOwn(notificationState.settings,'password')); assert.equal(notificationState.status.pending,0); assert.equal(await page.locator('#smtp-password').inputValue(),''); checked('real SMTP settings save write-only secret and do not send automatically');
    await page.locator('[data-page=plans]').click(); await page.locator('#add-plan').click(); await page.locator('#plan-name').fill('Браузерный план'); await page.locator('#plan-root').fill(metadata.archive); await page.locator('#plan-destination').fill('/Фото/Архив'); await page.locator('#plan-wizard-next').click();
    await waitFor(()=>page.locator('.plan-tree-row').count().then(x=>x===4),'real directory tree');
    await page.locator('.plan-tree-row').filter({hasText:'Исключить'}).locator('.tree-toggle').click();
    await waitFor(()=>page.locator('#plan-tree-summary').textContent().then(x=>x.includes('4 файла')),'selected real inventory totals');
    await page.locator('#plan-wizard-next').click(); await page.locator('#plan-target-count').fill('2'); await page.locator('#plan-target-gib').fill('1'); await page.locator('#plan-tolerance').fill('0'); await page.locator('#plan-start-date').fill('2090-01-01'); await page.locator('#plan-timezone').fill('UTC'); await page.locator('#plan-wizard-next').click();
    await waitFor(()=>page.locator('#plan-step-preview').isVisible()); await waitFor(()=>page.locator('#plan-wizard-next').isEnabled(),'real asynchronous build');
    assert.equal(await page.locator('.plan-preview-row').count(),2); const beforeActivate=await get('/api/plans'); const plan=beforeActivate.plans[0]; assert.equal(plan.status,'draft'); assert.equal(plan.file_count,4); checked('actual stat tree/exclusions and asynchronous two-dimensional planner agree with wizard');
    await shot(page,'real-plan-preview.png'); await page.locator('#plan-wizard-next').click(); await waitFor(()=>page.locator('#plan-wizard').evaluate(x=>!x.open));
    await waitFor(async()=>{ const data=await get('/api/plans/detail?id='+plan.id);return data.plan.status==='active';},'real activation');
    let batches=(await get('/api/plans/batches?plan_id='+plan.id)).batches; assert.equal(batches.length,2); assert.ok(batches.every(x=>x.source_id));
    const allSources=(await get('/api/state')).sources; assert.equal(allSources.filter(x=>x.owner_key).length,2); assert.equal((await get('/api/schedules')).schedules.length,0); checked('activation creates owned source groups and keeps one-shot batches out of recurring schedules');
    const batch=batches[0], card=()=>page.locator('.batch-card[data-batch-id="'+batch.id+'"]');
    await waitFor(()=>card().locator('[data-batch-action=start]').isEnabled(),'batch start button after activation'); await card().locator('[data-batch-action=start]').click();
    await waitFor(()=>card().locator('[data-batch-action=pause]').count().then(Boolean),'running batch row');
    await card().locator('[data-batch-action=pause]').click(); await page.locator('#batch-action-trash').check(); await page.locator('#batch-action-confirm').click();
    await waitFor(()=>page.locator('#batch-action-title').textContent().then(x=>x.includes('Убрать загруженные')),'cleanup preview after pause'); await waitFor(()=>page.locator('#batch-action-confirm').isEnabled());
    assert.match(await page.locator('#batch-action-preview').textContent(),/1 файл/); const beforeTrash=await get('/__fixture/status'); assert.equal(beforeTrash.remote_count,1);
    await page.locator('#batch-action-confirm').click(); await waitFor(()=>page.locator('#batch-action-dialog').evaluate(x=>!x.open));
    await waitFor(async()=>{const state=await get('/__fixture/status');return state.remote_count===0;},'real owned receipt cleanup'); checked('pause persists; real cleanup preview and token trash only one owned object');
    await page.request.post(metadata.origin+'/__fixture/control',{data:{block_after:null}});
    await waitFor(()=>card().locator('[data-batch-action=resume]').isEnabled(),'resume after cleanup'); await card().locator('[data-batch-action=resume]').click();
    await waitFor(async()=>{batches=(await get('/api/plans/batches?plan_id='+plan.id)).batches;return batches.find(x=>x.id===batch.id).status==='completed';},'resume completes real batch');
    const completed=batches.find(x=>x.id===batch.id); assert.equal(completed.summary.uploaded,2); assert.equal((await get('/api/jobs?source_id='+manual.id)).jobs.filter(x=>x.status==='pending').length,2);
    const cloud=await get('/__fixture/status'); assert.equal(cloud.remote_count,2); assert.ok(cloud.remote_paths.every(x=>x.startsWith('/Фото/Архив/'))); assert.ok(cloud.notifications.pending>=1); checked('resume requeues trashed objects; completed batch leaves manual-source tasks untouched and queues notification');
    await waitFor(()=>card().locator('[data-batch-action=queue]').isEnabled()); await card().locator('[data-batch-action=queue]').click(); await waitFor(()=>page.locator('.task-row').count().then(x=>x===2));
    assert.equal((await get('/api/state?source_id='+batch.source_id)).stats.uploaded,2); checked('batch task link filters the actual completed tasks');
    await page.locator('[data-page=settings]').click(); await waitFor(()=>page.locator('#smtp-test').isEnabled()); const queuedBefore=(await get('/api/notifications')).status.pending; await page.locator('#smtp-test').click();
    await waitFor(async()=> (await get('/api/notifications')).status.pending>queuedBefore); checked('explicit SMTP test enters real outbox while delivery remains disabled');
    await page.locator('[data-page=plans]').click(); await shot(page,'real-plans-completed.png');
    await page.setViewportSize({width:390,height:844}); await shot(page,'real-plans-mobile.png'); assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth+2),false); checked('real populated plan renders at 390px without page overflow');
    await page.setViewportSize({width:1440,height:1080});
    const access=await get('/api/session');
    for (let index=0;index<metadata.paged_source_paths.length;index++) {
      const response=await page.request.post(metadata.origin+'/api/sources/add',{headers:{'X-CSRF-Token':access.csrf,Origin:metadata.origin},data:{paths:[metadata.paged_source_paths[index]],alias:'Масштаб '+String(index+1).padStart(2,'0'),remote_prefix:'/Проверка страниц/'+String(index+1)}});
      assert.equal(response.status(),200,'Source seed '+index+' '+await response.text());
    }
    const seeded=await get('/api/state'); assert.equal(seeded.sources.length,50); assert.equal(seeded.sources_total,58); assert.equal(seeded.manual_sources_total,56);
    const last=(await get('/api/sources?q='+encodeURIComponent('МАСШТАБ 55'))).sources[0]; assert.ok(last); assert.ok(!seeded.sources.some(x=>x.id===last.id));
    await page.locator('[data-page=sources]').click(); await waitFor(()=>page.locator('.source-card').count().then(x=>x===50)); await page.locator('#sources-next').click();
    await waitFor(()=>page.locator('[data-source-action=queue][data-source-id="'+last.id+'"]').count().then(Boolean)); await page.locator('[data-source-action=queue][data-source-id="'+last.id+'"]').click();
    await waitFor(()=>page.locator('#queue-scope-name').textContent().then(x=>x===last.alias)); await page.locator('#refresh-connection').evaluate(button=>button.click());
    const scoped=await get('/api/state?source_id='+last.id); assert.equal(scoped.selected_source.id,last.id); assert.equal(scoped.stats.total,0);
    assert.ok(await page.locator('#queue-source-scope').isVisible()); assert.equal(await page.locator('#queue-scope-name').textContent(),last.alias); checked('real paged source API bounds state to 50 and retains selected off-page group');
    await page.locator('[data-page=schedules]').click(); await page.locator('#add-schedule').click(); await page.locator('#schedule-name').fill('Проверка разных страниц'); await page.locator('#schedule-enabled').uncheck();
    await waitFor(()=>page.locator('[name="schedule-source"]').count().then(x=>x===50)); await page.locator('[name="schedule-source"][value="'+manual.id+'"]').check();
    await page.locator('#schedule-source-next').click(); await waitFor(()=>page.locator('[name="schedule-source"][value="'+last.id+'"]').count().then(Boolean)); await page.locator('[name="schedule-source"][value="'+last.id+'"]').check();
    await page.locator('#schedule-source-search').fill('РУЧНАЯ'); await waitFor(()=>page.locator('[name="schedule-source"]').count().then(x=>x===1));
    assert.equal(await page.locator('[name="schedule-source"][value="'+manual.id+'"]').isChecked(),true); assert.match(await page.locator('#schedule-source-selected').textContent(),/2$/);
    await page.locator('#schedule-submit').click(); await waitFor(()=>page.locator('#schedule-dialog').evaluate(x=>!x.open));
    const saved=(await get('/api/schedules')).schedules[0]; assert.deepEqual(saved.source_ids.sort((a,b)=>a-b),[manual.id,last.id].sort((a,b)=>a-b)); assert.equal(saved.enabled,false); checked('real scheduler stores both selected groups across paging and Unicode search');
    assert.deepEqual(errors,[]); checked('real HTTP responses and browser produce no server exceptions');
    report.status='passed'; report.browser=browser.version(); report.check_count=report.checks.length;
  } catch(error) { if(page){try{await shot(page,'real-failure.png');}catch{}} report.status='failed';report.error=error.stack||String(error);report.browser_errors=errors;report.server_errors=serverErrors;process.exitCode=1; }
  finally {
    if(browser)await browser.close(); server.kill('SIGTERM'); await new Promise(resolve=>server.once('exit',resolve));
    const after=hashes(); report.files_changed_during_run=inputs.filter(file=>after[file]!==report.source_files[file]); report.finished_at=new Date().toISOString();
    fs.writeFileSync(path.join(output,'ui-real-0.9.0.json'),JSON.stringify(report,null,2)+'\n'); console.log(JSON.stringify({status:report.status,checks:report.checks.length,report:path.join(output,'ui-real-0.9.0.json'),error:report.error,files_changed:report.files_changed_during_run},null,2));
  }
})().catch(error=>{console.error(error.stack||String(error));process.exitCode=1;});

"""Check real historical queue writers against the current reader, offline."""
from pathlib import Path
import argparse
import io
import json
import subprocess
import sys
import tarfile
import tempfile

VERSIONS = {
    '0.4.3': '6fb4900a147816c7bc6632d046de7960f6cdffa7',
    '0.4.4': '07c47a639680f1a56d70ad8fe0a1e8a09d134835',
    '0.5.0': '363cfba34adf9e2ebbcaa2531b3a1e378fcee54d',
    '0.5.1': '982fa269ebe5ea1eefe33bd669459c86159eb4c9',
    '0.5.2': '1025b7813e8210b197b89aaceee5a89cecc54e85',
    '0.6.0': '87e9e48c1f4c54f9d49e0d0fa2d0944112acb052',
    '0.7.0': '0f0edc7a838a016e11814207d709e899fec303b2',
    '0.8.0': 'e83d530c3bd428481157f78acf5cff8b489b75d7',
}
CREATE = r'''
from dataclasses import asdict
from pathlib import Path
import json, sys
sys.path.insert(0, sys.argv[1])
from mailru_photos.queue import Queue
base = Path(sys.argv[2]); source = base / 'source'; source.mkdir()
for name in ('a.jpg','b.m4v','c.jpg','d.jpg','e.jpg','f.mp4','g.jpg','h.jpg'):
    (source/name).write_bytes(('offline fixture: '+name).encode())
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
root = q.add_root(source, 'Camera')
other = base / 'unselected'; other.mkdir()
(other/'other.jpg').write_bytes(b'offline second source')
q.add_root(other, 'Other')
q.set_metadata('last_scan', asdict(q.scan()))
done = q.claim(); q.mark_uploaded(done, remote_hash='offline-verified',
    verified_metadata={'is_unlim_upload': True, 'fixture': True})
failed = q.claim(); q.mark_failed(failed, 'offline fixture failure', retry_delay=None)
intent = q.claim(); q.save_commit_intent(intent, 'offline-accepted')
q.confirm_unlimited_commit(intent, 'offline-accepted'); q.release(intent)
pending = [dict(r) for r in q._db.execute("SELECT * FROM jobs WHERE root_id=? AND status='pending' AND id<>? ORDER BY id", (root.id,intent.id))]
Path(pending[-1]['local_path']).unlink()
q.set_metadata('last_scan', asdict(q.scan()))
paused_id = None
if hasattr(q, 'request_control'):
    paused_id = pending[0]['id']; q.request_control(paused_id, 'pause')
existing_id = None
if hasattr(q, 'mark_remote_existing'):
    eligible = {row['id'] for row in pending if row['id'] != paused_id and Path(row['local_path']).exists()}
    candidate = next(row for row in q.reconcile_candidates() if row['id'] in eligible)
    assert q.mark_remote_existing(candidate, remote_hash='B' * 40)
    existing_id = candidate['id']
snapshot = {'schema': q._db.execute('PRAGMA user_version').fetchone()[0],
    'rows': [dict(r) for r in q._db.execute('SELECT * FROM jobs ORDER BY id')],
    'roots': [asdict(r) for r in q.roots()], 'done': done.id, 'failed': failed.id,
    'intent': intent.id, 'paused': paused_id, 'existing': existing_id,
    'account_id': q.account_id, 'destination': q.destination,
    'upload_history_count': q._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0]}
(base/'before.json').write_text(json.dumps(snapshot))
q.close()
'''
VERIFY = r'''
from pathlib import Path
from dataclasses import asdict
import json, sqlite3, sys, unicodedata
sys.path.insert(0, sys.argv[1])
from mailru_photos import __version__
from mailru_photos.queue import Queue, QueueConfigurationError, SCHEMA_VERSION
base = Path(sys.argv[2]); before = json.loads((base/'before.json').read_text())
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
assert SCHEMA_VERSION == 6
assert q._db.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
backups = list((base/'backups').glob('queue-schema%d-before6-*.sqlite3' % before['schema']))
assert len(backups) == 1, backups
with sqlite3.connect(backups[0]) as backup:
    assert backup.execute('PRAGMA user_version').fetchone()[0] == before['schema']
    assert backup.execute('PRAGMA journal_mode').fetchone()[0] == 'delete'
    assert backup.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
    assert backup.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] == len(before['rows'])
    assert backup.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0] == before['upload_history_count']
    assert ('progress_bytes' in {row[1] for row in backup.execute('PRAGMA table_info(jobs)')}) == (before['schema'] >= 5)
assert [asdict(r) for r in q.roots()] == before['roots']
assert [(g['id'],g['alias'],g['remote_prefix'],g['member_count']) for g in q.source_groups()] == [
    (r['id'],r['alias'],r['remote_prefix'],1) for r in before['roots']]
assert all(r['source_group_id'] == r['root_id'] for r in q._db.execute('SELECT source_group_id,root_id FROM jobs'))
assert q.account_id == before['account_id'] == 'offline-fixture'
assert q.destination == before['destination'] == '/Legacy'
try:
    Queue(base/'queue.sqlite3', destination='/Legacy', account_id='wrong-offline-account')
except QueueConfigurationError:
    pass
else:
    raise AssertionError('An upgraded queue accepted a different account')
fields = ('id','root_id','local_path','remote_path','size','mtime_ns','device','inode',
    'status','present','attempts','last_error','skip_reason','remote_hash','uploaded_at','storage_mode')
rows = {r['id']: dict(r) for r in q._db.execute('SELECT * FROM jobs ORDER BY id')}
assert len(rows) == len(before['rows'])
for old in before['rows']:
    for name in fields:
        if name in old:
            assert rows[old['id']][name] == old[name], (old['id'], name, old[name], rows[old['id']][name])
    current = rows[old['id']]
    assert current['name_sort'] == unicodedata.normalize('NFC', old['relative_path'].rsplit('/',1)[-1]).casefold()
    terminal_upload = old['status'] == 'uploaded' or (old['status'] == 'skipped' and old['skip_reason'] == 'remote_exists')
    assert current['progress_bytes'] == old.get('progress_bytes', old['size'] if terminal_upload else 0)
    if 'completed_at' in old:
        assert current['completed_at'] == old['completed_at']
    elif old['status'] == 'uploaded':
        assert current['completed_at'] == old['uploaded_at']
    elif not terminal_upload:
        assert current['completed_at'] is None
assert q.stats()['total'] == sum(r['present'] for r in before['rows'])
assert q.get_job(before['done'])['status'] == 'uploaded'
assert q.get_job(before['done'])['observed_mode'] == 'unlimited'
assert q.get_job(before['done'])['cloud_present'] is True
assert q.get_job(before['failed'])['cloud_present'] is False
assert q._db.execute('SELECT COUNT(*) FROM cloud_observations').fetchone()[0] >= 1
assert q.get_job(before['failed'])['status'] == 'failed'
assert len(q.upload_history(before['done'])) == 1
assert q._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0] == before['upload_history_count']
receipt = q._db.execute('SELECT intent_json FROM commit_intents WHERE job_id=?', (before['intent'],)).fetchone()
assert json.loads(receipt[0])['unlimited_confirmed'] is True
if before['paused'] is not None:
    assert q.get_job(before['paused'])['paused'] is True
if before['existing'] is not None:
    found = q.get_job(before['existing'])
    assert found['skip_reason'] == 'remote_exists'
    assert found['cloud_present'] is True and found['observed_mode'] == 'unknown'
    assert found['progress_percent'] == 100 and found['completed_at'] is not None
    assert q.stats()['existing_unknown_count'] == 1
assert q._db.execute('SELECT COUNT(*) FROM rescan_intents').fetchone()[0] == 0
with q.exclusive_run():
    rescan = q.scan()
assert rescan.added == 0
assert q._db.execute('PRAGMA foreign_key_check').fetchall() == []
after_stats = q.stats(); q.close()
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
assert q._db.total_changes == 0
assert q.stats() == after_stats

# Fresh task rebuilding is a distinct operation from opening an old database.
# It drops stale presence/status, while an unresolved accepted commit still
# needs exact-identity recovery before the uploader decides what happened.
selected_root = before['roots'][0]['id']
unselected = [dict(row) for row in q._db.execute('SELECT * FROM jobs WHERE root_id<>? ORDER BY id', (selected_root,))]
old_rows = [dict(row) for row in q._db.execute('SELECT * FROM jobs WHERE root_id=? ORDER BY id', (selected_root,))]
old_max = max(row['id'] for row in rows.values())
assert q.reset_for_scan(root_ids=[selected_root]) == len(old_rows)
assert q._db.execute('SELECT COUNT(*) FROM commit_intents').fetchone()[0] == 0
assert q._db.execute('SELECT COUNT(*) FROM rescan_intents').fetchone()[0] == 1
saved = q._db.execute('SELECT intent_json FROM rescan_intents').fetchone()[0]
assert json.loads(saved)['unlimited_confirmed'] is True
assert q.upload_history(before['done'])[0]['job_id'] is None
assert all(row['job_id'] is None for row in q.commit_history(before['intent']))
assert not q.get_metadata('last_scan')['complete']
fresh = q.scan(root_ids=[selected_root])
assert fresh.complete and fresh.added == sum(row['present'] for row in old_rows)
rebuilt = [dict(row) for row in q._db.execute('SELECT * FROM jobs WHERE root_id=? ORDER BY id', (selected_root,))]
assert min(row['id'] for row in rebuilt) > old_max
assert all(row['status'] == 'pending' and not row['cloud_present'] and row['observed_mode'] is None
           and not row['paused'] and row['completed_at'] is None and row['progress_bytes'] == 0 for row in rebuilt)
assert [dict(row) for row in q._db.execute('SELECT * FROM jobs WHERE root_id<>? ORDER BY id', (selected_root,))] == unselected
assert q._db.execute('SELECT COUNT(*) FROM commit_intents').fetchone()[0] == 0
assert q._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0] == before['upload_history_count']
q.close()
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
assert q._db.total_changes == 0
assert q._db.execute('SELECT COUNT(*) FROM rescan_intents').fetchone()[0] == 1
done_path = rows[before['done']]['local_path']
fresh_done = q.find_current_job(selected_root, done_path)
job = q.claim(job_id=fresh_done['id'])
assert q.get_commit_intent(job) is None, 'A completed upload must never revive old presence or an intent'
q.release(job)
intent_path = rows[before['intent']]['local_path']
fresh_intent = q.find_current_job(selected_root, intent_path)
job = q.claim(job_id=fresh_intent['id'])
assert q.get_commit_intent(job)['unlimited_confirmed'] is True
assert not q.get_job(job.id)['cloud_present'], 'An uncertain old commit is not current cloud evidence'
assert q._db.execute('SELECT COUNT(*) FROM rescan_intents').fetchone()[0] == 0
q.release(job)
q.remove_root(selected_root)
assert q.stats()['total'] == len(unselected)
assert q.upload_history(before['done'])[0]['remote_path'].startswith('/Legacy/Camera/')
assert q._db.execute('SELECT COUNT(*) FROM rescan_intents').fetchone()[0] == 0
assert q.account_id == before['account_id']
assert q._db.execute('PRAGMA foreign_key_check').fetchall() == []
q.close()
print(json.dumps({'target_version':__version__, 'schema_before':before['schema'],'schema_after':SCHEMA_VERSION,
    'current_tasks':after_stats['total'], 'uploaded':after_stats['uploaded'],
    'paused':after_stats['paused'], 'existing':after_stats['existing'], 'rescan_added':rescan.added,
    'confirmed_intent_preserved':True,'detached_history_preserved':True,
    'observed_storage_migrated':True,'failed_tasks_not_assumed_uploaded':True,
    'migration_backup_preserved':True,'account_binding_preserved':True,
    'new_task_columns_valid':True,'fresh_scan_added':fresh.added,'fresh_scan_new_ids':True,
    'fresh_scan_drops_stale_presence':True,'unselected_source_preserved':True,
    'unfinished_intent_recovered_lazily':True,'completed_intent_not_revived':True,
    'reopen_writes':0}))
'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    results = []
    with tempfile.TemporaryDirectory(prefix='mail-cloud-real-upgrade-') as temp:
        temp = Path(temp)
        for version, sha in VERSIONS.items():
            legacy = temp / ('code-'+version); legacy.mkdir()
            archive = subprocess.check_output(['git','archive',sha,'mailru_photos'],cwd=repo)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                tar.extractall(legacy, filter='data')
            state = temp / ('state-'+version); state.mkdir()
            subprocess.run([sys.executable,'-c',CREATE,str(legacy),str(state)],check=True,cwd=temp)
            output = subprocess.check_output([sys.executable,'-c',VERIFY,str(repo),str(state)],cwd=temp,text=True)
            result = {'version':version,'source_commit':sha, **json.loads(output)}
            results.append(result); print(json.dumps(result),flush=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()

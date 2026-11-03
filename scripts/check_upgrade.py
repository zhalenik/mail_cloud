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
}
CREATE = r'''
from dataclasses import asdict
from pathlib import Path
import json, sys
sys.path.insert(0, sys.argv[1])
from mailru_photos.queue import Queue
base = Path(sys.argv[2]); source = base / 'source'; source.mkdir()
for name in ('a.jpg','b.m4v','c.jpg','d.jpg','e.jpg','f.mp4'):
    (source/name).write_bytes(('offline fixture: '+name).encode())
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
root = q.add_root(source, 'Camera')
q.set_metadata('last_scan', asdict(q.scan()))
done = q.claim(); q.mark_uploaded(done, remote_hash='offline-verified',
    verified_metadata={'is_unlim_upload': True, 'fixture': True})
failed = q.claim(); q.mark_failed(failed, 'offline fixture failure', retry_delay=None)
intent = q.claim(); q.save_commit_intent(intent, 'offline-accepted')
q.confirm_unlimited_commit(intent, 'offline-accepted'); q.release(intent)
pending = [dict(r) for r in q._db.execute("SELECT * FROM jobs WHERE status='pending' AND id<>?", (intent.id,))]
Path(pending[-1]['local_path']).unlink()
q.set_metadata('last_scan', asdict(q.scan()))
paused_id = None
if hasattr(q, 'request_control'):
    paused_id = pending[0]['id']; q.request_control(paused_id, 'pause')
snapshot = {'schema': q._db.execute('PRAGMA user_version').fetchone()[0],
    'rows': [dict(r) for r in q._db.execute('SELECT * FROM jobs ORDER BY id')],
    'roots': [asdict(r) for r in q.roots()], 'done': done.id, 'failed': failed.id,
    'intent': intent.id, 'paused': paused_id,
    'upload_history_count': q._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0]}
(base/'before.json').write_text(json.dumps(snapshot))
q.close()
'''
VERIFY = r'''
from pathlib import Path
from dataclasses import asdict
import json, sys
sys.path.insert(0, sys.argv[1])
from mailru_photos import __version__
from mailru_photos.queue import Queue
base = Path(sys.argv[2]); before = json.loads((base/'before.json').read_text())
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
assert q._db.execute('PRAGMA user_version').fetchone()[0] == 3
assert [asdict(r) for r in q.roots()] == before['roots']
fields = ('id','root_id','local_path','remote_path','size','mtime_ns','device','inode',
    'status','present','attempts','last_error','skip_reason','remote_hash','uploaded_at','storage_mode')
rows = {r['id']: dict(r) for r in q._db.execute('SELECT * FROM jobs ORDER BY id')}
assert len(rows) == len(before['rows'])
for old in before['rows']:
    for name in fields:
        if name in old:
            assert rows[old['id']][name] == old[name], (old['id'], name, old[name], rows[old['id']][name])
assert q.stats()['total'] == sum(r['present'] for r in before['rows'])
assert q.get_job(before['done'])['status'] == 'uploaded'
assert q.get_job(before['failed'])['status'] == 'failed'
assert len(q.upload_history(before['done'])) == 1
assert q._db.execute('SELECT COUNT(*) FROM upload_results').fetchone()[0] == before['upload_history_count']
receipt = q._db.execute('SELECT intent_json FROM commit_intents WHERE job_id=?', (before['intent'],)).fetchone()
assert json.loads(receipt[0])['unlimited_confirmed'] is True
if before['paused'] is not None:
    assert q.get_job(before['paused'])['paused'] is True
with q.exclusive_run():
    rescan = q.scan()
assert rescan.added == 0
assert q._db.execute('PRAGMA foreign_key_check').fetchall() == []
after_stats = q.stats(); q.close()
q = Queue(base/'queue.sqlite3', destination='/Legacy', account_id='offline-fixture')
assert q._db.total_changes == 0
assert q.stats() == after_stats
q.remove_root(before['roots'][0]['id'])
assert q.stats()['total'] == 0
assert q.upload_history(before['done'])[0]['remote_path'].startswith('/Legacy/Camera/')
assert q._db.execute('PRAGMA foreign_key_check').fetchall() == []
q.close()
print(json.dumps({'target_version':__version__, 'schema_before':before['schema'],'schema_after':3,
    'current_tasks':after_stats['total'], 'uploaded':after_stats['uploaded'],
    'paused':after_stats['paused'], 'rescan_added':rescan.added,
    'confirmed_intent_preserved':True,'detached_history_preserved':True,
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

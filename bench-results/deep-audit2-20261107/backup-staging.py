#!/usr/bin/env python3
"""Reproduce double-counted staging while two legitimate tenant backups overlap."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab
from recovery_drill import digest, save
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
args = p.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True), replicated=False)
result = {'result':'running', 'binary_sha256':digest(args.binary)}
def wait_job(tenant, job, terminal=True):
    deadline = time.monotonic()+30
    while True:
        current = lab.call(0,f'/v1/tenants/{tenant}/backup-jobs/'+job['id'])
        if (current['state'] in ('completed','failed','interrupted')) == terminal:
            return current
        assert time.monotonic()<deadline,current
        time.sleep(.01)
try:
    config=json.loads((lab.root/'backup.json').read_text())
    config.update(concurrency=2,max_backup_bytes=16<<20,max_staging_bytes=32<<20,bytes_per_second=1<<20)
    save(lab.root/'backup.json',config)
    lab.start(0)
    a,b=[lab.seed('staging-'+name,500,4096) for name in ['first','second']]
    first=lab.call(0,f'/v1/tenants/{a}/remote-backups',{})
    deadline=time.monotonic()+20
    while True:
        current=lab.call(0,f'/v1/tenants/{a}/backup-jobs/'+first['id'])
        assert current['state'] not in ('completed','failed','interrupted'),current
        if current['state']=='uploading':break
        assert time.monotonic()<deadline,current
        time.sleep(.01)
    work=lab.node_roots[0]/'backup-work'
    # Use the service's actual workspace name, without changing any files.
    candidates=list(lab.node_roots[0].rglob(first['id']))
    assert len(candidates)==1,candidates
    actual=sum(p.stat().st_size for p in candidates[0].rglob('*') if p.is_file())
    second=lab.call(0,f'/v1/tenants/{b}/remote-backups',{})
    failed=wait_job(b,second)
    assert failed['state']=='failed' and 'backup staging budget' in failed['error'],failed
    result.update(config={k:config[k] for k in ['concurrency','max_backup_bytes','max_staging_bytes']},first_staged_bytes=actual,second_while_first_active=failed)
    completed=wait_job(a,first)
    assert completed['state']=='completed',completed
    result['first_completed']=completed
    result['second_after_first_completed']=lab.job(b,'/backup-jobs/'+second['id']+'/retry')
    result['validation']=[lab.verify(0,t) for t in [a,b]]
    result['result']='reproduced'
except BaseException as error:
    result.update(result='failed',error=repr(error))
    raise
finally:
    lab.close()
    save(lab.root/'result.json',result)
print(json.dumps(result),flush=True)

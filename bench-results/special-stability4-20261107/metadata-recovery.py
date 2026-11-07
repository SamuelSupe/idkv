#!/usr/bin/env python3
"""Fail the directory sync after queued job rename, then resubmit and restart."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/"scripts"))
from qualification import Lab,RequestError
from recovery_drill import save

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary",type=Path,required=True);p.add_argument("--root",type=Path,required=True)
    p.add_argument("--expect-broken",action="store_true");p.add_argument("--terminal",action="store_true");a=p.parse_args()
    lab=Lab(a.root.resolve(),a.binary.resolve(),replicated=False)
    control,marker,shim=[lab.root/n for n in ['control-mode','fault-marker','fault.so']]
    result={'result':'running','binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest()}
    try:
        subprocess.run(['gcc','-Wall','-Wextra','-Werror','-shared','-fPIC','-O2','-o',str(shim),str(Path(__file__).with_name('backup-metadata-io.c'))],check=True)
        config=json.loads((lab.root/'backup.json').read_text());config['bytes_per_second']=1<<20;save(lab.root/'backup.json',config)
        lab.start(0,{'LD_PRELOAD':str(shim),'IDKV_META_ROOT':str(lab.node_roots[0]/'control/backup-jobs'),'IDKV_META_CONTROL':str(control),'IDKV_META_MARKER':str(marker)})
        tenant=lab.seed('metadata-fault',1500,4096);healthy=lab.seed('healthy',20,128)
        result['before']=lab.verify(0,tenant)
        control.write_text('terminal-eio' if a.terminal else 'directory-eio')
        if a.terminal:
            first=lab.call(0,f'/v1/tenants/{tenant}/remote-backups',{})
            deadline=time.monotonic()+60
            while True:
                failed=lab.call(0,f"/v1/tenants/{tenant}/backup-jobs/{first['id']}")
                if failed['state']=='interrupted':break
                assert time.monotonic()<deadline;time.sleep(.02)
            result['first_error']=failed['error']
            assert marker.read_text().startswith(str(lab.node_roots[0]/'control/backup-jobs')+'/.tmp-')
        else:
            try:
                lab.call(0,f'/v1/tenants/{tenant}/remote-backups',{})
                raise AssertionError('job admission did not fail')
            except RequestError as e:
                result['first_error']=e.value
                assert e.code=='IO_ERROR',e
            assert marker.read_text()==str(lab.node_roots[0]/'control/backup-jobs')
        jobs=lab.call(0,f'/v1/tenants/{tenant}/backup-jobs');result['visible_jobs_after_error']=jobs
        files=list((lab.node_roots[0]/'control/backup-jobs').glob('*.json'))
        assert len(files)==1 and json.loads(files[0].read_bytes()[8:])['state']==('uploading' if a.terminal else 'queued')
        try:
            second=lab.call(0,f'/v1/tenants/{tenant}/remote-backups',{})
            assert a.expect_broken,'admitted a second job after uncertain publication'
            result['second_submission']='accepted'
        except RequestError as e:
            assert not a.expect_broken and e.code=='BUSY',e
            result['second_submission']=e.code
        if not a.expect_broken:
            assert len(jobs)==1 and jobs[0]['state']=='interrupted'
            retry=lab.call(0,f"/v1/tenants/{tenant}/backup-jobs/{jobs[0]['id']}/retry",{})
            deadline=time.monotonic()+60
            while True:
                state=lab.call(0,f"/v1/tenants/{tenant}/backup-jobs/{retry['id']}")
                if state['state'] in ['completed','failed','interrupted']:
                    assert state['state']=='completed',state;break
                assert time.monotonic()<deadline;time.sleep(.03)
            result['same_job_retry_completed']=True
        lab.stop(0,kill=True)
        try:
            lab.start(0)
        except AssertionError:
            assert a.expect_broken,'fixed server failed to start'
            error=(lab.root/f"node-0-{lab.starts[0]}.log").read_text()
            assert 'multiple active backup jobs' in error,error
            result['restart_error']=error
            lab.node_roots[2]=lab.node_roots[0];lab.start(2)
            result['data_without_backup_service']=lab.verify(2,tenant)
            result['healthy']=lab.verify(2,healthy)
        else:
            assert not a.expect_broken,'expected duplicate-job startup failure'
            result['after_restart']=lab.verify(0,tenant);result['healthy']=lab.verify(0,healthy)
        result['result']='reproduced' if a.expect_broken else 'passed'
    finally:
        control.unlink(missing_ok=True);lab.close();shim.unlink(missing_ok=True);save(lab.root/'result.json',result)
    print(json.dumps(result),flush=True)

if __name__=='__main__':main()

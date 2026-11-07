#!/usr/bin/env python3
"""Fail the job-directory fsync after forget unlinks a terminal job file."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from qualification import Lab,RequestError
from recovery_drill import save

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--expect-broken',action='store_true');a=p.parse_args()
    lab=Lab(a.root.resolve(),a.binary.resolve(),replicated=False)
    control,marker,shim=[lab.root/n for n in ['control-mode','fault-marker','fault.so']]
    result={'result':'running','binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest()}
    try:
        subprocess.run(['gcc','-Wall','-Wextra','-Werror','-shared','-fPIC','-O2','-o',str(shim),str(Path(__file__).with_name('backup-metadata-io.c'))],check=True)
        lab.start(0,{'LD_PRELOAD':str(shim),'IDKV_META_ROOT':str(lab.node_roots[0]/'control/backup-jobs'),'IDKV_META_CONTROL':str(control),'IDKV_META_MARKER':str(marker)})
        tenant=lab.seed('forget-fault',100,1024)
        result['before']=lab.verify(0,tenant)
        job=lab.call(0,f'/v1/tenants/{tenant}/remote-backups',{})
        deadline=time.monotonic()+30
        while True:
            current=lab.call(0,f"/v1/tenants/{tenant}/backup-jobs/{job['id']}")
            if current['state'] in ['completed','failed','interrupted']:
                assert current['state']=='completed',current;break
            assert time.monotonic()<deadline;time.sleep(.02)
        path=f"/v1/tenants/{tenant}/backup-jobs/{job['id']}"
        control.write_text('directory-eio')
        try:
            lab.call(0,path,method='DELETE');raise AssertionError('forget did not fail')
        except RequestError as e:
            assert e.code=='IO_ERROR',e;result['first_error']=e.value
        assert not (lab.node_roots[0]/'control/backup-jobs'/f"{job['id']}.json").exists()
        try:
            lab.call(0,path,method='DELETE')
            assert not a.expect_broken,'expected retry failure';result['retry']='ok'
        except RequestError as e:
            assert a.expect_broken and e.code=='IO_ERROR' and 'os error 2' in str(e),e
            result['retry']=e.value
        result['visible_jobs_after_retry']=len(lab.call(0,f'/v1/tenants/{tenant}/backup-jobs'))
        assert result['visible_jobs_after_retry']==(1 if a.expect_broken else 0)
        assert len(lab.call(0,f'/v1/tenants/{tenant}/remote-backups'))==1
        result['after']=lab.verify(0,tenant)
        lab.stop(0,kill=True);lab.start(0);result['after_restart']=lab.verify(0,tenant)
        result['result']='reproduced' if a.expect_broken else 'passed'
    finally:
        control.unlink(missing_ok=True);lab.close();shim.unlink(missing_ok=True);save(lab.root/'result.json',result)
    print(json.dumps(result),flush=True)

if __name__=='__main__':main()

#!/usr/bin/env python3
"""Pause one backup phase fsync and poll job status from two tenants."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from qualification import Lab,RequestError
from recovery_drill import save

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--expect-blocked',action='store_true');a=p.parse_args()
    lab=Lab(a.root.resolve(),a.binary.resolve(),replicated=False)
    control,marker,shim=[lab.root/n for n in ['control-mode','fault-marker','fault.so']]
    pool=ThreadPoolExecutor(max_workers=8)
    result={'result':'running','binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest()}
    try:
        lab.options.update(max_http_requests=8,max_tenant_requests=4);save(lab.root/'options.json',lab.options)
        subprocess.run(['gcc','-Wall','-Wextra','-Werror','-shared','-fPIC','-O2','-o',str(shim),str(Path(__file__).with_name('backup-metadata-io.c'))],check=True)
        lab.start(0,{'LD_PRELOAD':str(shim),'IDKV_META_ROOT':str(lab.node_roots[0]/'control/backup-jobs'),'IDKV_META_CONTROL':str(control),'IDKV_META_MARKER':str(marker)})
        tenants=[lab.seed(name,20,128) for name in ['slow-job','status-poller','healthy']]
        result['before']=[lab.verify(0,t) for t in tenants]
        control.write_text('checkpointing-delay')
        job=lab.call(0,f'/v1/tenants/{tenants[0]}/remote-backups',{})
        deadline=time.monotonic()+10
        while not marker.exists():
            assert time.monotonic()<deadline,'phase fsync was not delayed';time.sleep(.005)
        polls=[pool.submit(lab.call,0,f'/v1/tenants/{t}/backup-jobs') for t in tenants[:2] for _ in range(4)]
        deadline=time.monotonic()+3
        while True:
            request=urllib.request.Request(lab.urls[0]+'/metrics',headers={'Authorization':'Bearer '+lab.token})
            with urllib.request.urlopen(request,timeout=3) as response:metrics=response.read().decode()
            active=int(re.search(r'^tkv_http_in_flight (\d+)$',metrics,re.MULTILINE)[1])
            if (active>=9 if a.expect_blocked else all(f.done() for f in polls)):break
            assert time.monotonic()<deadline,('polls did not reach the expected state',active)
            time.sleep(.01)
        result['pending_status_polls']=sum(not f.done() for f in polls)
        result['health']=lab.call(0,'/health')
        started=time.perf_counter()
        try:
            doc=lab.call(0,f'/v1/tenants/{tenants[2]}/collections/assets/documents/00000000')
            assert doc==lab.model(tenants[2])['00000000']
            outcome='ok'
        except RequestError as e:
            assert e.code=='RESOURCE_EXHAUSTED' and 'HTTP worker concurrency' in str(e),e
            outcome=e.code
        result['healthy_request']={'result':outcome,'elapsed_ms':(time.perf_counter()-started)*1000}
        assert (outcome=='RESOURCE_EXHAUSTED')==a.expect_blocked
        control.unlink()
        for f in polls:f.result(timeout=10)
        deadline=time.monotonic()+30
        while True:
            current=lab.call(0,f"/v1/tenants/{tenants[0]}/backup-jobs/{job['id']}")
            if current['state'] in ['completed','failed','interrupted']:
                assert current['state']=='completed',current;break
            assert time.monotonic()<deadline;time.sleep(.02)
        result['after']=[lab.verify(0,t) for t in tenants]
        lab.stop(0,kill=True);lab.start(0)
        result['after_restart']=[lab.verify(0,t) for t in tenants]
        result['result']='reproduced' if a.expect_blocked else 'passed'
    finally:
        control.unlink(missing_ok=True);pool.shutdown(wait=True);lab.close();shim.unlink(missing_ok=True);save(lab.root/'result.json',result)
    print(json.dumps(result),flush=True)

if __name__=='__main__':main()

#!/usr/bin/env python3
"""Check same-budget HTTP acknowledgements through automatic replica service loops."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from qualification import Lab
from recovery_drill import digest,save
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary',type=Path,required=True)
p.add_argument('--root',type=Path,required=True)
args=p.parse_args();args.root.mkdir(parents=True,exist_ok=False)
results=[]
for quota,payload,expected_init in [(40,512,False),(60,768,True)]:
    lab=Lab(args.root/str(payload),args.binary.resolve(strict=True))
    result={'result':'running','global_mib':64,'tenant_mib':quota,'payload_kib':payload,'binary_sha256':digest(args.binary)}
    try:
        lab.options.update(global_memory_bytes=64<<20,tenant_memory_bytes=quota<<20,memtable_bytes=1<<20,cache_bytes=64<<10)
        save(lab.root/'options.json',lab.options)
        lab.start(0)
        tenant=lab.call(0,'/v1/tenants',{'name':'global-budget'})['id']
        prefix=f'/v1/tenants/{tenant}'
        lab.call(0,prefix+'/collections',{'name':'docs'})
        for n in range(16):
            lab.retry(0,prefix+'/collections/docs/indexes',{'name':f'i{n}','fields':[{'field':'/n'}],'include':['/payload']})
        lab.start(1);lab.catch_up()
        before=lab.call(1,'/v1/replication/status')['reinitializations']
        doc={'_key':'large','n':1,'payload':'x'*(payload<<10)}
        ack=lab.call(0,prefix+'/collections/docs/documents',{'document':doc},method='PUT')
        lab.retry(0,prefix+'/compact',{})
        lab.catch_up()
        values=[lab.call(node,prefix+'/collections/docs/documents/large') for node in [0,1]]
        assert values[0]==values[1]==dict(doc,_rev=ack['rev'])
        state=lab.call(1,'/v1/replication/status')
        count=state['reinitializations']-before
        assert (count>0)==expected_init,(count,expected_init,state)
        result.update(result='passed',acknowledged_seq=ack['rev'],reinitializations=count,acknowledged_mismatches=0,
                      content_sha256=hashlib.sha256(json.dumps(values[0],sort_keys=True).encode()).hexdigest())
    except BaseException as error:
        result.update(result='failed',error=repr(error));raise
    finally:
        lab.close();save(lab.root/'result.json',result)
    results.append(result)
save(args.root/'result.json',{'result':'passed','cases':results})
print(json.dumps(results),flush=True)

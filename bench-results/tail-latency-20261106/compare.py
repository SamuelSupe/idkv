"""Compare unchanged qualification workloads; preserve each run and its baseline."""
import gzip,json
from pathlib import Path
import sys
root=Path(sys.argv[1])
rows=[]
for mode in ['before','after','before-repeat']:
 p=root/mode
 r=json.loads((p/'result.json').read_text())
 s=json.loads((p/'summary.json').read_text())
 phases=[]
 for phase in r['phases']:
  tails={}
  for kind,metrics in phase['operations'].items():
   with gzip.open(p/f"{phase['phase']}-{kind}-latencies.json.gz",'rt') as stream:v=sorted(json.load(stream))
   assert len(v)==metrics['calls']
   for fraction,key in [(.5,'p50_ms'),(.95,'p95_ms'),(.99,'p99_ms')]:
    assert round(v[int(len(v)*fraction)],3)==metrics[key]
   tails[kind]={'p99_ms':metrics['p99_ms'],'p999_ms':round(v[int(len(v)*.999)],3),'max_ms':round(v[-1],3)}
  phases.append({'phase':phase['phase'],'operations':phase['operations'],'tails':tails,
                 'verified_backup_cycles':phase['completed_verified_backups'],
                 'overlap_intervals':phase['overlap_sample_intervals']})
 for tenant in r['tenants']:
  a=[x['sha256'] for x in r['before_restart'] if x['tenant']==tenant]
  b=[x['sha256'] for x in r['after_restart'] if x['tenant']==tenant]
  assert len(set(a+b))==1
  assert len({x['sha256'] for x in r['final_validation'] if x['tenant']==tenant})==1
 assert r['gc_drained']['next_expiration_ms'] is None
 rows.append({'mode':mode,'result':r['result'],'phases':phases,
               'acknowledged_document_operations':s['acknowledged_document_operations'],
               'acknowledged_batch_requests':s['acknowledged_batch_requests'],
               'validated_documents':s['validated_current_documents'],'restored_documents':s['restored_documents'],
               'restart_ms':r['restart_to_write_and_replica_caught_up_ms'],
               'admission_retries':r['admission_retries_total']})
a=rows[1]['phases'][1]['operations']
changes={}
for old in [rows[0],rows[2]]:
 b=old['phases'][1]['operations']
 changes[old['mode']]={k:{'p99_percent':round((a[k]['p99_ms']/v['p99_ms']-1)*100,2),
                         'throughput_percent':round((a[k]['calls_per_second']/v['calls_per_second']-1)*100,2)} for k,v in b.items()}
result={'runs':rows,'after_changes_vs_old':changes,'confirmed_document_operations_total':sum(r['acknowledged_document_operations'] for r in rows),
        'note':'One before/after/before sequence on a shared VM; ranges are descriptive, not confidence intervals.'}
(root/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
for r in rows:
 print(r['mode'],'acknowledged',r['acknowledged_document_operations'],'restart_ms',round(r['restart_ms'],3))
 for phase in r['phases']:
  print(phase['phase'],'light',phase['tails']['light_writer'],'heavy',phase['tails']['heavy_writer'],'docs_per_s',round(phase['operations']['heavy_writer']['calls_per_second']*20,2),'backups',phase['verified_backup_cycles'])
print(json.dumps(changes,indent=2))

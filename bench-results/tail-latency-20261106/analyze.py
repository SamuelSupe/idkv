"""Derive performance and resource summaries from the qualification's raw samples."""
import json
import gzip
from pathlib import Path
import sys
root=Path(sys.argv[1])
r=json.loads((root/'result.json').read_text())
heavy=r['tenants'][0]
summary={'result':r['result'],'phases':[], 'restart_ms':r.get('restart_to_write_and_replica_caught_up_ms')}
for phase in r['phases']:
 name=phase['phase']
 samples=[json.loads(line) for line in (root/f'{name}-samples.jsonl').read_text().splitlines()]
 def tenant(sample):return next(t['stats'] for t in sample['metrics']['tenants'] if t['tenant']['id']==heavy)
 first,last=tenant(samples[0]),tenant(samples[-1])
 delta={k:last['io'][k]-first['io'][k] for k in ['document_bytes','wal_bytes','flush_bytes','compaction_write_bytes','compaction_read_bytes','write_lock_wait_ns','fsyncs']}
 peaks={
  'primary_engine_memory_bytes':max(s['metrics']['memory_bytes'] for s in samples),
  'heavy_engine_memory_bytes':max(tenant(s)['memory_bytes'] for s in samples),
  'heavy_disk_bytes':max(tenant(s)['disk_bytes'] for s in samples),
  'l0_files':max(tenant(s)['levels'].get('0',0) for s in samples),
  'expiration_lag_ms':max((s['retention'] or {}).get('expiration_lag_ms',0) for s in samples),
 }
 for node in ['0','1']:
  peaks[f'node_{node}_rss_bytes']=max(int(s['process'][node]['VmRSS'].split()[0])*1024 for s in samples)
  peaks[f'node_{node}_file_descriptors']=max(s['process'][node]['file_descriptors'] for s in samples)
 gaps=[]
 for s in samples:
  seq=lambda state:next(t['seq'] for t in state['tenants'] if t['info']['id']==heavy)
  gaps.append(max(0,seq(s['primary'])-seq(s['replica'])))
 peaks['observed_replication_sequence_gap']=max(gaps)
 gc_delta=((samples[-1]['retention'] or {}).get('gc',{}).get('reclaimed_records',0)
           -(samples[0]['retention'] or {}).get('gc',{}).get('reclaimed_records',0))
 summary['phases'].append({'phase':name,'operations':phase['operations'],
  'document_updates_per_second':phase['operations']['heavy_writer']['calls_per_second']*20,
  'verified_backup_cycles':phase['completed_verified_backups'],
  'overlap_intervals':phase['overlap_sample_intervals'], 'admission_retries':phase['admission_retries'],
  'sample_seconds':samples[-1]['elapsed_s']-samples[0]['elapsed_s'],'io_delta':delta,
  'write_amplification':(delta['wal_bytes']+delta['flush_bytes']+delta['compaction_write_bytes'])/delta['document_bytes'],
  'gc_reclaimed_delta':gc_delta,'peaks':peaks,
  'maintenance_error_samples':sum(any(t['stats']['maintenance_error'] for t in s['metrics']['tenants']) for s in samples)})
base=summary['phases'][0]['operations']
for phase in summary['phases'][1:]:
 phase['p99_change_vs_baseline_percent']={k:(v['p99_ms']/base[k]['p99_ms']-1)*100 for k,v in phase['operations'].items()}
summary['validated_current_documents']=sum(v['documents'] for v in r['final_validation'] if v['node']==0)
summary['acknowledged_batch_requests']=0
summary['acknowledged_document_operations']=0
ledger=root/'acknowledgements.jsonl'
stream=ledger.open() if ledger.exists() else gzip.open(str(ledger)+'.gz','rt')
for line in stream:
 row=json.loads(line)
 summary['acknowledged_batch_requests']+=1
 summary['acknowledged_document_operations']+=len(row['indices'])
stream.close()
summary['restored_documents']=r.get('restored_backup',{}).get('documents')
summary['gc_final']=r.get('gc_drained')
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))

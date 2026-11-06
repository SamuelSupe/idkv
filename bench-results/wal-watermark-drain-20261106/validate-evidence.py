#!/usr/bin/env python3
"""Recompute counts, percentiles and overlap; cross-check data hashes and source provenance."""
import gzip
import hashlib
import json
from pathlib import Path
import re

base = Path(__file__).resolve().parent
repo = base.parents[1]
read = lambda path: json.loads((base / path).read_text())
load = read('load/result.json')
faults = read('faults/result.json')
before = read('perf/before/result.json')
after = read('perf/after/result.json')
full_before = read('full-disk/before/result.json')
full_after = read('full-disk/after/result.json')
switch = read('switch/result.json')
assert all(run['result'] == 'passed' for run in [load, faults, before, after, full_before, full_after, switch])
assert read('load/environment.json')['binary_sha256'] == read('faults/environment.json')['binary_sha256'] == after['binary_sha256'] == full_after['binary_sha256'] == switch['binary_sha256']
assert full_before['full_cold_open'] == full_before['full_restart'] == 'IO_ERROR'
assert full_before['disk_full']['available_after'] == full_after['disk_full']['available_after'] == 0
assert full_after['read_only_status']['stats']['recovery_read_only']
assert full_after['write_rejected'] == full_after['fence_rejected'] == 'RESOURCE_EXHAUSTED'
assert full_after['read_only_repeated_write_bytes'] == 0
assert full_after['unclean_full_verified']['sha256'] == full_after['after_space_freed']['sha256']
assert all(full_after[key]['acknowledged_mismatches'] == 0 for key in
           ['clean_full_verified','unclean_full_verified','after_space_freed','after_retry'])
assert full_after['clean_full_verified']['documents'] == 1200
assert full_after['unclean_full_verified']['documents'] == 1208 and full_after['after_retry']['documents'] == 1216
assert switch['persistent_fault_fence'] in ('IO_ERROR', 'COMMIT_UNKNOWN') and switch['same_data'] and switch['same_reported_position']
assert all(n['acknowledged_mismatches'] == 0 and n['documents'] == 109 for n in switch['nodes'])
import statistics
for case in [before, after]:
    assert case['verified']['documents'] == 1200 and case['verified']['acknowledged_mismatches'] == 0
    assert len(case['samples']) == 7
    for kind in ['open', 'close', 'cycle']:
        assert statistics.median(sample[kind + '_ms'] for sample in case['samples']) == case[kind + '_median_ms']
assert before['options'] == after['options'] and before['wal_bytes'] == after['wal_bytes']
assert before['verified']['sha256'] == after['verified']['sha256']
assert len({tuple(sample['inodes']) for sample in after['samples']}) == 1
with gzip.open(base / 'load/acknowledgements.jsonl.gz', 'rt') as stream:
    journal = [json.loads(line) for line in stream]
checks = load['before_restart'] + load['after_restart'] + load['final_validation'] + [load['restored_backup']]
assert all(row['acknowledged_mismatches'] == 0 for row in checks)
assert {(r['node'], r['tenant']):r['sha256'] for r in load['before_restart']} == {(r['node'], r['tenant']):r['sha256'] for r in load['after_restart']}
final = {(r['node'], r['tenant']):r for r in load['final_validation']}
for tenant in load['tenants']:
    assert final[0, tenant]['sha256'] == final[1, tenant]['sha256']
assert load['restored_backup']['sha256'] == final[0, load['tenants'][0]]['sha256']
assert load['gc_drained']['next_expiration_ms'] is None
assert load['gc_drained']['expiration_lag_ms'] == 0
resumed = read('load/interrupted-job-final.json')
assert resumed['crc_validated'] and resumed['job']['id'] == load['interrupted_backup']['id'] and resumed['job']['state'] == 'completed'
phases = []
heavy = load['tenants'][0]
for phase in load['phases']:
    name = phase['phase']
    for kind, stats in phase['operations'].items():
        assert not stats['errors']
        with gzip.open(base / f'load/{name}-{kind}-latencies.json.gz', 'rt') as stream:
            values = sorted(json.load(stream))
        assert len(values) == stats['calls']
        assert round(values[min(len(values)-1, int(len(values)*.99))], 3) == stats['p99_ms']
    with gzip.open(base / f'load/{name}-samples.jsonl.gz', 'rt') as stream:
        samples = [json.loads(line) for line in stream]
    overlap = 0
    for old, new in zip(samples, samples[1:]):
        a, b = [next(t['stats'] for t in s['metrics']['tenants'] if t['tenant']['id'] == heavy) for s in [old, new]]
        if name != 'baseline' and old['backup_active'] and b['io']['flush_bytes'] > a['io']['flush_bytes'] and b['io']['compaction_write_bytes'] > a['io']['compaction_write_bytes'] and new['retention']['gc']['reclaimed_records'] > old['retention']['gc']['reclaimed_records'] and next(t['seq'] for t in new['replica']['tenants'] if t['info']['id'] == heavy) > next(t['seq'] for t in old['replica']['tenants'] if t['info']['id'] == heavy):
            overlap += 1
    assert overlap == phase['overlap_sample_intervals']
    backup_file = base / f'load/{name}-backups.json'
    if backup_file.exists():
        backups = json.loads(backup_file.read_text())
        assert len(backups) == phase['completed_verified_backups']
        assert all(v['verify']['state'] == v['create']['state'] == 'completed' for v in backups)
    phases.append({'phase':name, 'light_p99_ms':phase['operations']['light_writer']['p99_ms'], 'heavy_p99_ms':phase['operations']['heavy_writer']['p99_ms'], 'verified_backups':phase['completed_verified_backups'], 'overlap_intervals':overlap})
assert all(c['result'] == 'passed' and c['confirmed_documents_preserved'] == 2000 and c['confirmed_documents_missing'] == 0 and c['uncertain_batch_visible_before_retry'] in (0, 8) for c in faults['cases'])
provenance = read('source-provenance.json')
for name, digest in provenance['source_sha256'].items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest, name
log = (base / 'final-validation.log').read_text()
assert 'FAILED' not in log and 'error:' not in log
passed = sum(map(int, re.findall(r'test result: ok\. (\d+) passed', log)))
assert passed == 146
result = {'result':'passed', 'tests_passed':passed, 'acknowledged_batches':len(journal), 'acknowledged_document_operations':sum(len(row['indices']) for row in journal), 'acknowledged_mismatches':0, 'phases':phases, 'fault_cases':len(faults['cases']), 'fault_confirmed_documents_preserved':6000, 'source_files_verified':len(provenance['source_sha256']), 'binary_sha256':after['binary_sha256'], 'healthy_open_comparison': {'before_ms':before['open_median_ms'], 'after_ms':after['open_median_ms'], 'ratio':before['open_median_ms']/after['open_median_ms'], 'before_cycle_ms':before['cycle_median_ms'], 'after_cycle_ms':after['cycle_median_ms']}, 'full_disk_reads_verified': True}
assert after['binary_sha256'] in (base / 'readonly-poll-check.log').read_text()
result['validation_observations'] = ['One parallel recovery test observed Store lock BUSY; final serial suite and parallel recovery recheck passed; root cause unresolved.']
(base / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, ensure_ascii=False))

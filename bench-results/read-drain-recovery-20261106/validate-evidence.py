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
before = read('export/before/result.json')
after = read('export/after/result.json')
material_before = read('materialization/before/result.json')
material_after = read('materialization/after/result.json')
switch = read('switch/result.json')
assert all(run['result'] == 'passed' for run in [load, faults, before, after, material_before, material_after, switch])
assert read('load/environment.json')['binary_sha256'] == read('faults/environment.json')['binary_sha256'] == after['binary_sha256'] == material_after['binary_sha256'] == switch['binary_sha256']
assert material_before['reopen_with_wal_writes_failing'] == 'OK' and material_after['reopen_with_wal_writes_failing'] == 'IO_ERROR'
assert material_before['verified']['acknowledged_mismatches'] == material_after['verified']['acknowledged_mismatches'] == 0
assert switch['persistent_fault_fence'] in ('IO_ERROR', 'COMMIT_UNKNOWN') and switch['same_data'] and switch['same_reported_position']
assert all(n['acknowledged_mismatches'] == 0 and n['documents'] == 109 for n in switch['nodes'])
import statistics
for case in [before, after]:
    assert case['rows'] == 12032 and case['acknowledged_mismatches'] == 0
    assert all(sample['pages'] == 48 and sample['frames'] == case['head'] for sample in case['samples'])
    for phase in ['cold', 'warm']:
        assert statistics.median(s['ms'] for s in case['samples'] if s['phase'] == phase) == case[phase + '_median_ms']
assert before['options'] == after['options'] and before['wal_bytes'] == after['wal_bytes']
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
log = (base / 'validation.log').read_text()
assert 'FAILED' not in log and 'error:' not in log
passed = sum(map(int, re.findall(r'test result: ok\. (\d+) passed', log)))
assert passed == 145
result = {'result':'passed', 'tests_passed':passed, 'acknowledged_batches':len(journal), 'acknowledged_document_operations':sum(len(row['indices']) for row in journal), 'acknowledged_mismatches':0, 'phases':phases, 'fault_cases':len(faults['cases']), 'fault_confirmed_documents_preserved':6000, 'source_files_verified':len(provenance['source_sha256']), 'binary_sha256':after['binary_sha256'], 'cold_export_comparison': {'before_ms':before['cold_median_ms'], 'after_ms':after['cold_median_ms'], 'ratio':before['cold_median_ms']/after['cold_median_ms']}, 'fresh_recovery_writes_verified': True}
(base / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, ensure_ascii=False))

#!/usr/bin/env python3
"""Validate archived source fingerprints and the recorded acceptance boundaries."""
import hashlib
import gzip
import json
from pathlib import Path
import re

out = Path(__file__).resolve().parent
repo = out.parents[1]

def read(path):
    return json.loads((out / path).read_text())

def no_mismatch(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {'acknowledged_mismatches', 'confirmed_documents_missing'}:
                assert child == 0, (key, child)
            no_mismatch(child)
    elif isinstance(value, list):
        for child in value:
            no_mismatch(child)

provenance = read('source-provenance.json')
for name, expected in provenance['source_sha256'].items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == expected, name
for name in ['pending', 'faults', 'switch', 'load']:
    result = read(name + '/result.json')
    assert result['result'] == 'passed', name
    no_mismatch(result)
pending = read('pending/result.json')
assert pending['full_disk']['available_after'] == 0
assert pending['while_full']['documents'] == 2000
assert pending['before']['sha256'] == pending['while_full']['sha256'] == pending['after_space_freed']['sha256']
assert pending['full_disk_job']['state'] == 'interrupted'
assert pending['retried_job']['state'] == 'completed'
switch = read('switch/result.json')
assert switch['same_data'] and switch['same_reported_position']
comparison = read('comparison.json')
assert comparison['scheduler']['after']['served_tenants'] == 160
assert comparison['scheduler']['after']['never_served_tenants'] == 0
load = read('load/result.json')
assert read('load/interrupted-job-final.json')['job']['state'] == 'completed'
assert [phase['phase'] for phase in load['phases']] == ['baseline', 'combined', 'recovery']
combined = load['phases'][1]
assert combined['overlap_sample_intervals'] > 0 and combined['completed_verified_backups'] > 0
for phase in load['phases']:
    assert all(not operation['errors'] for operation in phase['operations'].values()), phase['phase']
    for name, operation in phase['operations'].items():
        path = out / 'load' / (phase['phase'] + '-' + name + '-latencies.json.gz')
        with gzip.open(path, 'rt') as stream:
            samples = sorted(json.load(stream))
        assert len(samples) == operation['calls'], path
        for key, fraction in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99)]:
            actual = round(samples[min(len(samples) - 1, int(len(samples) * fraction))], 3)
            assert actual == operation[key], (path, key, actual, operation[key])
assert load['restored_backup']['documents'] == 20000
assert load['publication_gc']['current_readable'] and load['publication_gc']['expired_version_rejected']
tests = (out / 'full-tests.log').read_text()
assert 'test result: FAILED' not in tests
passed = sum(map(int, re.findall(r'test result: ok\. (\d+) passed; 0 failed', tests)))
assert passed == 151, passed
result = {'result': 'passed', 'source_files_verified': len(provenance['source_sha256']),
          'full_tests_passed': passed, 'acknowledged_mismatches': 0,
          'native_fault_scenarios': ['enospc', 'wal-write-eio', 'wal-sync-eio'],
          'five_way_overlap_intervals': combined['overlap_sample_intervals']}
(out / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))

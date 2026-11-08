#!/usr/bin/env python3
"""Same HTTP cache/tiny-writer workload for the baseline and repaired binary."""
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request
from collections import Counter
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab, RequestError, summarize

binary, root = map(Path, sys.argv[1:3])
lab = Lab(root, binary, replicated=False)
lab.options.update(global_memory_bytes=64 << 20, tenant_memory_bytes=64 << 20,
                   memtable_bytes=16 << 20, cache_bytes=8 << 20)
(root / 'options.json').write_text(json.dumps(lab.options))
expected, histories, errors = {}, {}, Counter()
result = {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'options': lab.options}

def write(tenant, key, payload, operation):
    document = {'_key': key, 'payload': payload}
    body = {'document': document, 'write_options': {
        'operation_id': operation, 'expected_history': histories[tenant]}}
    started = time.monotonic()
    while True:
        try:
            receipt = lab.call(0, f'/v1/tenants/{tenant}/collections/docs/documents', body, method='PUT')
            break
        except RequestError as error:
            errors[error.code] += 1
            assert error.code in ('RESOURCE_EXHAUSTED', 'BUSY', 'RESPONSE_UNAVAILABLE'), error
            assert time.monotonic() - started < 5, ('write stalled', error.value)
            time.sleep(.02)
    document['_rev'] = receipt['rev']
    expected[tenant][key] = document
    lab.journal.write(json.dumps({'tenant': tenant, 'key': key, 'rev': receipt['rev'],
                                 'operation_id': operation}) + '\n')
    return (time.monotonic() - started) * 1000

def tenant_stats(tenant):
    return lab.retry(0, f'/v1/tenants/{tenant}')['stats']

try:
    lab.start(0)
    tenants = []
    for n in range(15):
        tenant = lab.retry(0, '/v1/tenants', {'name': f'cache-{n}'})['id']
        lab.retry(0, f'/v1/tenants/{tenant}/collections', {'name': 'docs'})
        expected[tenant] = {}
        histories[tenant] = lab.retry(0, f'/v1/tenants/{tenant}/context')['history']
        tenants.append(tenant)
        if n < 14:
            for row in range(256):
                write(tenant, str(row), 'x' * (16 << 10), f'seed-{row}')
            lab.retry(0, f'/v1/tenants/{tenant}/checkpoint', {})
    for tenant in tenants[:-1]:
        for offset in range(0, 256, 8):
            keys = [str(n) for n in range(offset, offset + 8)]
            actual = lab.retry(0, f'/v1/tenants/{tenant}/collections/docs/multi_get', {'keys': keys})
            assert actual == [expected[tenant][key] for key in keys]
    result['after_warming'] = lab.retry(0, '/v1/metrics')
    if 'memory_pressure' in result['after_warming']:
        pressure = result['after_warming']['memory_pressure']
        assert pressure['foreground_limit_bytes'] + pressure['headroom_bytes'] == 64 << 20
        assert 0 <= pressure['headroom_in_use_bytes'] <= pressure['headroom_bytes']
        request = urllib.request.Request(lab.urls[0] + '/metrics',
                    headers={'Authorization': 'Bearer ' + lab.token})
        with urllib.request.urlopen(request, timeout=30) as response:
            metrics = dict(line.split() for line in response.read().decode().splitlines()
                           if line.startswith('tkv_memory_pressure_'))
        assert int(metrics['tkv_memory_pressure_headroom_bytes']) == pressure['headroom_bytes']
        assert int(metrics['tkv_memory_pressure_foreground_limit_bytes']) == pressure['foreground_limit_bytes']
        result['pressure_metrics_verified'] = True
    light = tenants[-1]
    before = tenant_stats(light)['io']
    latencies, memory_samples = [], []
    started = time.monotonic()
    for n in range(120):
        latencies.append(write(light, 'latest', f'value-{n}', f'light-{n}'))
        time.sleep(.12)
        if n % 10 == 0:
            memory_samples.append(lab.retry(0, '/v1/metrics')['memory_bytes'])
    elapsed = time.monotonic() - started
    after = tenant_stats(light)['io']
    result.update(light_write=summarize(latencies, elapsed, {}),
                  light_flush_bytes=after['flush_bytes'] - before['flush_bytes'],
                  light_compaction_bytes=after['compaction_write_bytes'] - before['compaction_write_bytes'],
                  memory_samples=memory_samples, retry_errors=dict(errors))
    # Crash while the tiny writes are still resident; do not close tenants first.
    lab.stop(0, kill=True)
    lab.start(0)
    checked = 0
    for tenant, documents in expected.items():
        keys = list(documents)
        for offset in range(0, len(keys), 8):
            batch = keys[offset:offset + 8]
            actual = lab.retry(0, f'/v1/tenants/{tenant}/collections/docs/multi_get', {'keys': batch})
            assert actual == [documents[key] for key in batch], (tenant, offset)
            checked += len(batch)
        lab.retry(0, f'/v1/tenants/{tenant}/close', {})
    result.update(confirmed_writes=14 * 256 + 120, verified_after_crash=checked,
                  acknowledged_mismatches=0, result='passed')
finally:
    lab.close()
    (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'after_warming'}, indent=2))

#!/usr/bin/env python3
"""Compare warm/cold multi-page export with a fixed, unflushed WAL and ACK model."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab
from recovery_drill import digest, save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--rows', type=int, default=12032)
args = p.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True))
lab.options.update(global_memory_bytes=1 << 30, tenant_memory_bytes=256 << 20, memtable_bytes=64 << 20)
save(lab.root / 'options.json', lab.options)
result = {'result': 'running', 'binary_sha256': digest(args.binary), 'options': lab.options,
          'scope': 'Local HTTP, warm OS page cache, one fenced source, unflushed WAL'}
try:
    lab.start(0)
    info = lab.call(0, '/v1/tenants', {'name': 'cold-export'})
    prefix = f"/v1/tenants/{info['id']}"
    lab.call(0, prefix + '/collections', {'name': 'docs'})
    def put(i):
        doc = {'_key': f'{i:08}', 'payload': 'p' * 900}
        receipt = lab.call(0, prefix + '/collections/docs/documents', {'document': doc}, method='PUT')
        return doc['_key'], dict(doc, _rev=receipt['rev'])
    with ThreadPoolExecutor(max_workers=16) as pool:
        expected = dict(pool.map(put, range(args.rows)))
    state = lab.call(0, '/v1/replication/status')['node']
    lab.call(0, '/v1/replication/fence', {'target': str(uuid.uuid4())})
    head = lab.call(0, prefix + '/context')['applied_seq']
    result['wal_bytes'] = sum(path.stat().st_size for path in
        (lab.node_roots[0] / 'tenants' / info['id'] / 'wal').glob('*.wal'))
    result['rows'] = len(expected)
    result['head'] = head
    samples = []
    log_hash = None
    for phase in ['warm', 'cold']:
        for sample in range(3):
            if phase == 'cold':
                lab.retry(0, prefix + '/close', {})
            request = {'protocol': 3, 'cluster': state['cluster'], 'epoch': state['epoch'],
                       'tenant': info['id'], 'generation': info['generation'], 'after': 0}
            frames, pages, hashed = 0, 0, hashlib.sha256()
            started = time.perf_counter()
            while request['after'] < head:
                page = lab.call(0, '/v1/replication/log', request)
                assert page['source_position'] == head
                for encoded in page['frames']:
                    raw = base64.b64decode(encoded)
                    frames += 1
                    assert int.from_bytes(raw[8:16], 'little') == frames
                    hashed.update(raw)
                assert page['through'] > request['after']
                request.update(after=page['through'], cursor=page['cursor'])
                pages += 1
            elapsed = (time.perf_counter() - started) * 1000
            assert frames == head
            if log_hash is None: log_hash = hashed.hexdigest()
            assert hashed.hexdigest() == log_hash
            samples.append({'phase': phase, 'sample': sample, 'ms': elapsed, 'pages': pages, 'frames': frames})
    cursor, actual = None, {}
    while True:
        query = {'limit': 1000}
        if cursor: query['cursor'] = cursor
        page = lab.call(0, prefix + '/collections/docs/query', query)
        actual.update((doc['_key'], doc) for doc in page['documents'])
        cursor = page['cursor']
        if cursor is None: break
    assert actual == expected, 'acknowledged documents or revisions changed'
    result.update(result='passed', samples=samples, log_sha256=log_hash,
                  acknowledged_mismatches=0,
                  warm_median_ms=statistics.median(s['ms'] for s in samples if s['phase'] == 'warm'),
                  cold_median_ms=statistics.median(s['ms'] for s in samples if s['phase'] == 'cold'))
finally:
    lab.close()
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

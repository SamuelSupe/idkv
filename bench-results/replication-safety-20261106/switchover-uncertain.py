#!/usr/bin/env python3
"""Reproduce fsync-unknown -> cold close -> planned switchover with real processes."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab, RequestError
from recovery_drill import digest, save

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--shim', type=Path, required=True)
parser.add_argument('--expect-old', action='store_true')
args = parser.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True))
result = {'result': 'running', 'binary_sha256': digest(args.binary)}
control = lab.root / 'io-mode'
try:
    lab.start(0, {'LD_PRELOAD': str(args.shim.resolve()), 'IDKV_IO_ROOT': str(lab.node_roots[0]),
                  'IDKV_IO_CONTROL': str(control), 'IDKV_IO_LOG': str(lab.root / 'io-fault.log')})
    tenant = lab.seed('unknown-switchover', 100, 4096)
    lab.start(1)
    lab.catch_up()
    expected = lab.model(tenant)
    context = lab.context(tenant, False)
    control.write_text('sync')
    try:
        lab.write(tenant, range(100, 108), 1, 4096, context, 'unknown-before-switch', 'fault', retry=False)
        raise AssertionError('fsync fault did not fire')
    except RequestError as error:
        assert error.code == 'COMMIT_UNKNOWN', error
        result['fault'] = error.code
    assert 'sync EIO' in (lab.root / 'io-fault.log').read_text()
    if not args.expect_old:
        lab.retry(0, f'/v1/tenants/{tenant}/close', {})
        target = lab.call(1, '/v1/replication/status')['node']['node']
        try:
            lab.call(0, '/v1/replication/fence', {'target': target})
            raise AssertionError('persistent WAL fsync failure allowed a safe fence')
        except RequestError as error:
            result['persistent_fault_fence'] = error.code
            assert error.code in ('IO_ERROR', 'COMMIT_UNKNOWN'), error
    control.unlink()
    lab.retry(0, f'/v1/tenants/{tenant}/close', {})
    lab.call(0, '/v1/replication/switchover', {})
    acknowledged = {'_key': 'new-primary', 'value': 'written-after-promotion'}
    receipt = lab.call(1, f'/v1/tenants/{tenant}/collections/assets/documents',
                       {'document': acknowledged}, method='PUT')
    expected['new-primary'] = dict(acknowledged, _rev=receipt['rev'])
    result['new_primary_ack_rev'] = receipt['rev']
    # Opening A after B has written must not splice an old unknown c+1 into B's history.
    time.sleep(1)
    rows = [lab.call(node, f'/v1/tenants/{tenant}/collections/assets/query', {'limit': 500})['documents']
            for node in [0, 1]]
    result['nodes'] = []
    for node, documents in enumerate(rows):
        actual = {row['_key']: row for row in documents}
        result['nodes'].append({'node': node, 'documents': len(actual),
            'sha256': hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest(),
            'acknowledged_mismatches': sum(actual.get(key) != value for key, value in expected.items()),
            'uncertain_documents': sum(f'{i:08}' in actual for i in range(100, 108)),
            'applied_seq': lab.call(node, f'/v1/tenants/{tenant}/context')['applied_seq']})
    result['same_reported_position'] = result['nodes'][0]['applied_seq'] == result['nodes'][1]['applied_seq']
    result['same_data'] = result['nodes'][0]['sha256'] == result['nodes'][1]['sha256']
    if args.expect_old:
        assert result['same_reported_position'] and not result['same_data'], result
        assert result['nodes'][0]['uncertain_documents'] == 8 and result['nodes'][1]['uncertain_documents'] == 0
    else:
        assert result['same_reported_position'] and result['same_data'], result
        assert all(row['acknowledged_mismatches'] == 0 and row['uncertain_documents'] == 8 for row in result['nodes'])
    result['result'] = 'passed'
finally:
    control.unlink(missing_ok=True)
    lab.close()
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

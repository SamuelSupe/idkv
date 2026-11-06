#!/usr/bin/env python3
"""Reproduce forgotten export ownership across a real receiver process restart."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab, RequestError
from recovery_drill import digest, save


def run(binary, root, expect_old):
    lab = Lab(root, binary)
    result = {'binary_sha256': digest(binary), 'result': 'running'}
    for node in [0, 1]:
        path = root / f'replication-{node}.json'
        config = json.loads(path.read_text())
        config['log_retention_bytes'] = 1 << 20
        save(path, config)
    try:
        lab.start(0)
        tenant = lab.seed('restart-export', 100, 4096)
        lab.start(1)
        lab.catch_up()
        result['before_restart'] = [lab.verify(n, tenant) for n in [0, 1]]
        lab.stop(1, kill=True)
        state = lab.call(0, '/v1/replication/status')['node']
        request = {'protocol': 3, 'cluster': state['cluster'], 'epoch': state['epoch'], 'tenant': tenant}
        old = lab.call(0, '/v1/replication/snapshot', request)
        lab.call(0, '/v1/replication/snapshot/release', {'id': old['id']})
        exported = lab.call(0, '/v1/replication/snapshot', request)
        result['exported_position'] = exported['position']
        context = lab.context(tenant, False)
        lab.write(tenant, range(100), 1, 4096, context, 'tail-before-restart', 'restart')
        lab.start(1)
        lab.catch_up()
        result['after_restart'] = [lab.verify(n, tenant) for n in [0, 1]]
        other = lab.call(0, '/v1/tenants', {'name': 'other-export'})['id']
        def other_export():
            try:
                value = lab.call(0, '/v1/replication/snapshot', {**request, 'tenant': other})
                return 'OK'
            except RequestError as error:
                return error.code
        result['other_export_after_restart'] = other_export()
        for generation in range(2, 34):
            lab.write(tenant, range(100), generation, 4096, context, f'continued-{generation}', 'continued')
            lab.call(0, f'/v1/tenants/{tenant}/compact', {})
            deadline = time.monotonic() + 10
            target = lab.call(0, f'/v1/tenants/{tenant}/context')['applied_seq']
            while lab.call(1, f'/v1/tenants/{tenant}/context')['applied_seq'] < target:
                assert time.monotonic() < deadline, 'healthy tenant replication stalled'
                time.sleep(.01)
        wal = lab.node_roots[0] / 'tenants' / tenant / 'wal'
        result['wal_bytes'] = sum(p.stat().st_size for p in wal.glob('*.wal'))
        result['export_copy_exists'] = (lab.node_roots[0] / 'replication-work/export' / exported['id']).exists()
        result['other_export_after_writes'] = other_export()
        result['final_validation'] = [lab.verify(n, tenant) for n in [0, 1]]
        if expect_old:
            assert result['other_export_after_restart'] == result['other_export_after_writes'] == 'BUSY', result
            assert result['export_copy_exists'] and result['wal_bytes'] > 2 << 20, result
        else:
            assert result['other_export_after_restart'] == result['other_export_after_writes'] == 'OK', result
            assert not result['export_copy_exists'] and result['wal_bytes'] <= 2 << 20, result
        result['result'] = 'passed'
    except BaseException as error:
        result.update(result='failed', error=repr(error))
        raise
    finally:
        lab.close()
        save(root / 'result.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--expect-old', action='store_true')
    args = parser.parse_args()
    run(args.binary.resolve(strict=True), args.root.resolve(), args.expect_old)

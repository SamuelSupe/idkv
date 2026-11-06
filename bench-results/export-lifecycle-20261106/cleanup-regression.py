#!/usr/bin/env python3
"""Compare log responses and retry cleanup with an unreadable export directory."""
import argparse
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
parser.add_argument('--expect-old', action='store_true')
args = parser.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True))
result = {'result': 'running', 'binary_sha256': digest(args.binary)}
path, mode = None, None
try:
    lab.start(0)
    tenant = lab.seed('cleanup-regression', 100, 4096)
    state = lab.call(0, '/v1/replication/status')['node']
    info = lab.call(0, '/v1/replication/snapshot', {
        'protocol': 3, 'cluster': state['cluster'], 'epoch': state['epoch'], 'tenant': tenant})
    path = lab.node_roots[0] / 'replication-work/export' / info['id']
    mode = path.stat().st_mode & 0o777
    path.chmod(0o000)
    try:
        lab.call(0, '/v1/replication/log', {
            'protocol': 3, 'cluster': state['cluster'], 'epoch': state['epoch'],
            'tenant': tenant, 'generation': info['tenant']['generation'], 'after': info['position']})
        result['log_request'] = 'OK'
    except RequestError as error:
        result['log_request'] = error.code
    time.sleep(1.5)
    result['export_exists_during_fault'] = path.exists()
    result['background_error_logged'] = 'replication export cleanup:' in (lab.root / 'node-0-1.log').read_text()
    path.chmod(mode)
    deadline = time.monotonic() + 3
    while path.exists() and time.monotonic() < deadline:
        time.sleep(.02)
    result['export_exists_after_permission_repair'] = path.exists()
    result['verified'] = lab.verify(0, tenant)
    if args.expect_old:
        assert result['log_request'] == 'IO_ERROR' and result['export_exists_after_permission_repair'], result
    else:
        assert result['log_request'] == 'OK' and result['background_error_logged'], result
        assert result['export_exists_during_fault'] and not result['export_exists_after_permission_repair'], result
    result['result'] = 'passed'
finally:
    if path is not None and mode is not None and path.exists():
        path.chmod(mode)
    lab.close()
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

#!/usr/bin/env python3
"""Check that reopening a WAL requires fresh writes, not only a second fsync."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab, RequestError
from recovery_drill import digest, save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--shim', type=Path, required=True)
p.add_argument('--expect-old', action='store_true')
args = p.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True))
control = lab.root / 'io-mode'
result = {'result': 'running', 'binary_sha256': digest(args.binary),
          'scope': 'Syscall-boundary test; not a reproduction of kernel clean-page writeback failure'}
try:
    lab.start(0, {'LD_PRELOAD': str(args.shim.resolve()), 'IDKV_IO_ROOT': str(lab.node_roots[0]),
                  'IDKV_IO_CONTROL': str(control), 'IDKV_IO_LOG': str(lab.root / 'io-fault.log')})
    tenant = lab.seed('materialization', 100, 4096)
    lab.retry(0, f'/v1/tenants/{tenant}/close', {})
    control.write_text('write')
    try:
        lab.call(0, f'/v1/tenants/{tenant}/context')
        result['reopen_with_wal_writes_failing'] = 'OK'
    except RequestError as error:
        result['reopen_with_wal_writes_failing'] = error.code
    control.unlink()
    lab.retry(0, f'/v1/tenants/{tenant}/close', {})
    result['verified'] = lab.verify(0, tenant)
    if args.expect_old:
        assert result['reopen_with_wal_writes_failing'] == 'OK', result
    else:
        assert result['reopen_with_wal_writes_failing'] == 'IO_ERROR', result
        assert '.recover' in (lab.root / 'io-fault.log').read_text()
    result['result'] = 'passed'
finally:
    control.unlink(missing_ok=True)
    lab.close()
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

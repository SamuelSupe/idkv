#!/usr/bin/env python3
"""Reproduce full-disk startup failure with a real interrupted backup job."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import zlib

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab
from qualification_faults import disk_volume, fill_volume
from recovery_drill import digest, save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
args = p.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True), replicated=False)
result = {'result': 'running', 'binary_sha256': digest(args.binary)}
volume = None
try:
    volume = disk_volume(lab.root)
    lab.node_roots[0] = volume[1] / 'node-0'
    config = json.loads((lab.root / 'backup.json').read_text())
    config['bytes_per_second'] = 1 << 20
    save(lab.root / 'backup.json', config)
    lab.start(0)
    tenant = lab.seed('pending-backup-full', 2000, 4096)
    result['before'] = lab.verify(0, tenant)
    prefix = f'/v1/tenants/{tenant}'
    job = lab.call(0, prefix + '/remote-backups', {})
    deadline = time.monotonic() + 5
    while True:
        job = lab.call(0, prefix + '/backup-jobs/' + job['id'])
        assert job['state'] not in ('completed', 'failed', 'interrupted'), job
        if job['state'] != 'queued':
            break
        assert time.monotonic() < deadline
        time.sleep(.01)
    lab.stop(0, kill=True)
    raw = (lab.node_roots[0] / 'control/backup-jobs' / (job['id'] + '.json')).read_bytes()
    assert raw[:4] == b'TKJ1' and zlib.crc32(raw[8:]) == int.from_bytes(raw[4:8], 'little')
    result['persisted_job_state'] = json.loads(raw[8:])['state']
    pad, result['full_disk'] = fill_volume(volume[1])
    assert result['full_disk']['available_after'] == 0
    try:
        lab.start(0)
        raise RuntimeError('expected full-disk restart failure was not reproduced')
    except AssertionError:
        output = (lab.root / 'node-0-2.log').read_text()
        assert 'os error 28' in output, output
        result['full_disk_startup'] = 'process exited: IO_ERROR ENOSPC'
    pad.unlink()
    lab.start(0)
    result['after_space_freed'] = lab.verify(0, tenant)
    assert result['before']['sha256'] == result['after_space_freed']['sha256']
    result['result'] = 'reproduced'
finally:
    lab.close()
    if volume:
        subprocess.run(['sudo', '-n', 'umount', str(volume[1])], check=True)
        subprocess.run(['sudo', '-n', 'losetup', '-d', volume[2]], check=True)
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

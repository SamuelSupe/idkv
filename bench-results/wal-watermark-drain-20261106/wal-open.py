#!/usr/bin/env python3
"""Measure healthy reopen cycles or verify full-disk reads on an isolated ext4 volume."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab, RequestError
from qualification_faults import disk_volume, fill_volume
from recovery_drill import digest, save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--full-disk', action='store_true')
p.add_argument('--expect-old', action='store_true')
args = p.parse_args()
lab = Lab(args.root.resolve(), args.binary.resolve(strict=True))
lab.options['memtable_bytes'] = 16 << 20
save(lab.root / 'options.json', lab.options)
result = {'result': 'running', 'binary_sha256': digest(args.binary),
          'options': lab.options, 'full_disk': args.full_disk}
volume = None

try:
    if args.full_disk:
        volume = disk_volume(lab.root)
        lab.node_roots[0] = volume[1] / 'node-0'
    lab.start(0)
    clean = lab.seed('clean-open', 1200, 4096)
    prefix = f'/v1/tenants/{clean}'
    wal_dir = lab.node_roots[0] / 'tenants' / clean / 'wal'
    wals = sorted(wal_dir.glob('*.wal'))
    result['wal_bytes'] = sum(path.stat().st_size for path in wals)
    original_inodes = [path.stat().st_ino for path in wals]
    if not args.full_disk:
        samples = []
        for _ in range(7):
            started = time.perf_counter()
            lab.retry(0, prefix + '/close', {})
            closed = time.perf_counter()
            context = lab.call(0, prefix + '/context')
            opened = time.perf_counter()
            assert context['writable']
            samples.append({'close_ms': (closed-started)*1000, 'open_ms': (opened-closed)*1000,
                            'cycle_ms': (opened-started)*1000,
                            'inodes': [path.stat().st_ino for path in wals]})
        result['verified'] = lab.verify(0, clean)
        result['samples'] = samples
        for kind in ['open', 'close', 'cycle']:
            result[kind + '_median_ms'] = statistics.median(s[kind + '_ms'] for s in samples)
        if not args.expect_old:
            assert all(s['inodes'] == original_inodes for s in samples)
    else:
        dirty = lab.seed('unclean-open', 1200, 4096)
        dirty_prefix = f'/v1/tenants/{dirty}'
        lab.retry(0, prefix + '/close', {})
        lab.retry(0, dirty_prefix + '/close', {})
        context = lab.context(dirty, False)
        lab.write(dirty, range(1200, 1208), 1, 4096, context, 'ack-before-crash', 'seed')
        pad, result['disk_full'] = fill_volume(volume[1])
        assert result['disk_full']['available_after'] == 0
        if args.expect_old:
            try:
                lab.call(0, prefix + '/context')
                raise AssertionError('old cold tenant unexpectedly opened on full disk')
            except RequestError as error:
                assert error.code == 'IO_ERROR', error
                result['full_cold_open'] = error.code
            lab.stop(0, kill=True)
            try:
                lab.start(0)
                raise RuntimeError('old full-disk Store unexpectedly restarted')
            except AssertionError:
                assert 'os error 28' in (lab.root / 'node-0-2.log').read_text()
                result['full_restart'] = 'IO_ERROR'
        else:
            assert lab.call(0, prefix + '/context')['writable']
            assert [path.stat().st_ino for path in wals] == original_inodes
            result['clean_full_verified'] = lab.verify(0, clean)
            lab.stop(0, kill=True)
            lab.start(0)
            initial_status = lab.call(0, '/v1/replication/status')
            assert any(row['info']['id'] == dirty for row in initial_status['unavailable'])
            assert lab.call(0, dirty_prefix)['open'], 'status polling discarded the read-only engine'
            assert lab.call(0, prefix + '/context')['writable']
            assert not lab.call(0, dirty_prefix + '/context')['writable']
            result['unclean_full_verified'] = lab.verify(0, dirty)
            result['read_only_status'] = lab.call(0, dirty_prefix)
            assert result['read_only_status']['stats']['recovery_read_only']
            for action in ['write', 'fence']:
                try:
                    if action == 'write':
                        lab.write(dirty, range(1208, 1216), 2, 4096, context,
                                  'denied-on-full-disk', 'denied', retry=False)
                    else:
                        lab.call(0, '/v1/replication/fence', {'target': str(uuid.uuid4())})
                    raise AssertionError(action + ' unexpectedly succeeded')
                except RequestError as error:
                    assert error.code == 'RESOURCE_EXHAUSTED', error
                    result[action + '_rejected'] = error.code
            status = lab.call(0, '/v1/replication/status')
            assert any(row['info']['id'] == dirty for row in status['unavailable'])
            assert status['node']['role'] == 'primary'
            def write_bytes():
                rows = Path(f'/proc/{lab.processes[0].pid}/io').read_text().splitlines()
                return int(next(row.split(':')[1] for row in rows if row.startswith('write_bytes:')))
            before = write_bytes()
            for _ in range(4):
                time.sleep(.35)
                lab.call(0, '/v1/replication/status')
                assert lab.call(0, dirty_prefix)['open']
                assert not lab.call(0, dirty_prefix + '/context')['writable']
            result['read_only_repeated_write_bytes'] = write_bytes() - before
            assert result['read_only_repeated_write_bytes'] == 0
            pad.unlink()
            lab.retry(0, dirty_prefix + '/close', {})
            assert lab.call(0, dirty_prefix + '/context')['writable']
            result['after_space_freed'] = lab.verify(0, dirty)
            lab.write(dirty, range(1208, 1216), 2, 4096, context,
                      'denied-on-full-disk', 'repaired', retry=False)
            result['after_retry'] = lab.verify(0, dirty)
    result['result'] = 'passed'
except BaseException as error:
    result.update(result='failed', error=repr(error))
    raise
finally:
    lab.close()
    if volume:
        subprocess.run(['sudo', '-n', 'umount', str(volume[1])], check=True)
        subprocess.run(['sudo', '-n', 'losetup', '-d', volume[2]], check=True)
    save(lab.root / 'result.json', result)
print(json.dumps(result), flush=True)

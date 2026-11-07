#!/usr/bin/env python3
"""Archive the allowlisted evidence, never credentials or database files."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import zlib

p = argparse.ArgumentParser(description=__doc__)
for name in ['pending', 'faults', 'switch', 'load', 'binaries', 'global_run', 'staging']:
    p.add_argument('--' + name, type=Path, required=True)
args = p.parse_args()
out = Path(__file__).resolve().parent
repo = out.parents[1]

def copy_run(source, target):
    target.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.iterdir()):
        if not path.is_file():
            continue
        allowed = path.name in {'result.json', 'environment.json', 'restore.log', 'io-fault.log'}
        allowed |= path.name.endswith(('-result.json', '-backups.json', '-latencies.json.gz', '.jsonl'))
        allowed |= path.name.startswith('node-') and path.suffix == '.log'
        if not allowed:
            continue
        dest = target / path.name
        if path.suffix == '.jsonl':
            with path.open('rb') as src, gzip.GzipFile(str(dest) + '.gz', 'wb', mtime=0) as dst:
                shutil.copyfileobj(src, dst)
        else:
            shutil.copyfile(path, dest)

for name in ['pending', 'faults', 'switch', 'load', 'global_run', 'staging']:
    copy_run(getattr(args, name), out / name)
for name in ['enospc', 'wal-write-eio', 'wal-sync-eio']:
    copy_run(args.faults / name, out / 'faults' / name)
for name in ['512', '768']:
    copy_run(args.global_run / name, out / 'global_run' / name)
for name in ['pending', 'faults', 'switch', 'load', 'global_run', 'staging']:
    result = json.loads((out / name / 'result.json').read_text())
    assert result['result'] == 'passed', (name, result)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

load = json.loads((args.load / 'result.json').read_text())
job_path = args.load / 'node-0/control/backup-jobs' / (load['interrupted_backup']['id'] + '.json')
raw = job_path.read_bytes()
assert raw[:4] == b'TKJ1' and zlib.crc32(raw[8:]) == int.from_bytes(raw[4:8], 'little')
(out / 'load/interrupted-job-final.json').write_text(json.dumps({
    'job': json.loads(raw[8:]), 'crc_validated': True,
    'source_file_sha256': hashlib.sha256(raw).hexdigest()}, indent=2) + '\n')

files = [repo / 'Cargo.toml', repo / 'Cargo.lock']
for directory, pattern in [('src', '*.rs'), ('tests', '*.rs'), ('scripts', '*.py'), ('scripts', '*.c')]:
    files.extend((repo / directory).rglob(pattern))
files.extend(out.glob('*.py'))
files.extend(out.glob('*.rs'))
provenance = {
    'base_commit': 'd1ca7e20fc95af3e309314e7e8a5dafa28c01727',
    'platform': platform.platform(),
    'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
    'binary_sha256': {name: sha(args.binaries / name) for name in ['tenant-kv', 'tenant-kv-restore']},
    'source_sha256': {str(path.relative_to(repo)): sha(path) for path in sorted(files)},
    'scope': 'Production source uninstrumented. Unit quota tests stop maintenance to hold unflushed WAL; native process cases use normal background workers.'}
(out / 'source-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
tests = (out / 'final-tests.log').read_text()
counts = re.findall(r'test result: ok\. (\d+) passed; 0 failed', tests)
assert counts and 'test result: FAILED' not in tests
print(json.dumps({'full_tests_passed': sum(map(int, counts)), 'binaries': provenance['binary_sha256']}))

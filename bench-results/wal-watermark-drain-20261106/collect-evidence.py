#!/usr/bin/env python3
"""Archive synthetic test evidence, excluding credentials and database files."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import zlib

parser = argparse.ArgumentParser(description=__doc__)
for name in ['load', 'faults', 'before', 'after', 'full_before', 'full_after', 'switch']:
    parser.add_argument('--' + name, type=Path, required=True)
args = parser.parse_args()
out = Path(__file__).resolve().parent
repo = out.parents[1]


def archive(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix == '.jsonl':
        with source.open('rb') as src, gzip.GzipFile(str(target) + '.gz', 'wb', mtime=0) as dst:
            shutil.copyfileobj(src, dst)
    else:
        shutil.copyfile(source, target)


def copy_run(source, target):
    for path in source.iterdir():
        if path.is_file() and (path.name in {'result.json', 'environment.json', 'restore.log', 'io-fault.log'}
            or path.name.endswith(('-result.json', '-backups.json', '-latencies.json.gz', '.jsonl'))
            or path.name.startswith('node-') and path.suffix == '.log'):
            archive(path, target / path.name)


copy_run(args.load, out / 'load')
copy_run(args.faults, out / 'faults')
for name in ['enospc', 'wal-write-eio', 'wal-sync-eio']:
    copy_run(args.faults / name, out / 'faults' / name)
for name in ['before', 'after']:
    copy_run(getattr(args, name), out / 'perf' / name)
for name in ['before', 'after']:
    copy_run(getattr(args, 'full_' + name), out / 'full-disk' / name)
copy_run(args.switch, out / 'switch')
result = json.loads((args.load / 'result.json').read_text())
job = args.load / 'node-0/control/backup-jobs' / (result['interrupted_backup']['id'] + '.json')
raw = job.read_bytes()
assert raw[:4] == b'TKJ1' and zlib.crc32(raw[8:]) == int.from_bytes(raw[4:8], 'little')
(out / 'load/interrupted-job-final.json').write_text(json.dumps({
    'job': json.loads(raw[8:]), 'crc_validated': True,
    'source_file_sha256': hashlib.sha256(raw).hexdigest()}, indent=2) + '\n')
files = [repo / 'Cargo.toml', repo / 'Cargo.lock']
for directory, pattern in [('src', '*.rs'), ('tests', '*.rs'), ('scripts', '*.py'), ('scripts', '*.c')]:
    files.extend((repo / directory).rglob(pattern))
files.extend(out.glob('*.py'))
hashes = {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)}
provenance = {
    'base_commit': '163ce1bc62e923b4a20c51b6c8e2841b4ff54022',
    'source_sha256': hashes, 'platform': platform.platform(),
    'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
    'user': subprocess.check_output(['whoami'], text=True).strip(),
    'scope': 'WAL synced watermarks, full-disk read-only recovery and bounded replica admission; native Linux AArch64 OrbStack only.',
    'runs': {name: str(path) for name, path in vars(args).items()}}
(out / 'source-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(json.dumps({'archived_files': len(list(out.rglob('*'))), 'source_files': len(hashes)}))

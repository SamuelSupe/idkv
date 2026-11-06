#!/usr/bin/env python3
"""Copy only synthetic journals, measurements and logs, never databases or credentials."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import zlib

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--load', type=Path, required=True)
p.add_argument('--faults', type=Path, required=True)
p.add_argument('--http', type=Path, required=True)
args = p.parse_args()
out = Path(__file__).resolve().parent
repo = out.parents[1]

def copy(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix == '.jsonl':
        with source.open('rb') as src, gzip.GzipFile(str(target) + '.gz', 'wb', mtime=0) as dst:
            shutil.copyfileobj(src, dst)
    else:
        shutil.copyfile(source, target)

for source in args.load.iterdir():
    if source.name in {'result.json', 'environment.json', 'restore.log'} or source.name.endswith(('-result.json', '-backups.json', '-latencies.json.gz', '.jsonl', '.jsonl.gz')) or source.name.startswith('node-') and source.suffix == '.log':
        copy(source, out / 'load' / source.name)
for source in [args.faults / 'result.json', args.faults / 'environment.json']:
    copy(source, out / 'faults' / source.name)
for name in ['enospc', 'wal-write-eio', 'wal-sync-eio']:
    for source in (args.faults / name).iterdir():
        if source.name in {'result.json', 'io-fault.log'} or source.suffix == '.jsonl' or source.name.startswith('node-') and source.suffix == '.log':
            copy(source, out / 'faults' / name / source.name)
copy(args.http / 'result.json', out / 'http' / 'result.json')
for label in ['before', 'after']:
    copy(args.http / label / 'node-0-1.log', out / 'http' / label / 'node-0-1.log')
result = json.loads((args.load / 'result.json').read_text())
job = args.load / 'node-0/control/backup-jobs' / (result['interrupted_backup']['id'] + '.json')
raw = job.read_bytes()
assert raw[:4] == b'TKJ1' and zlib.crc32(raw[8:]) == int.from_bytes(raw[4:8], 'little')
(out / 'load/interrupted-job-final.json').write_text(json.dumps({'job': json.loads(raw[8:]), 'crc_validated': True, 'source_file_sha256': hashlib.sha256(raw).hexdigest()}, indent=2) + '\n')
files = [repo / 'Cargo.toml', repo / 'Cargo.lock']
for directory, pattern in [('src', '*.rs'), ('tests', '*.rs'), ('scripts', '*.py'), ('scripts', '*.c')]:
    files.extend((repo / directory).rglob(pattern))
files.extend(out.glob('*.py'))
hashes = {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)}
provenance = {'source_sha256': hashes, 'platform': platform.platform(),
              'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
              'user': subprocess.check_output(['whoami'], text=True).strip(),
              'scope': 'Uncommitted workspace, including preceding backup-progress changes. Linux AArch64 only.',
              'runs': {name: str(path) for name, path in vars(args).items()}}
(out / 'source-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(json.dumps({'copied_files': len(list(out.rglob('*'))), 'source_files': len(hashes)}))

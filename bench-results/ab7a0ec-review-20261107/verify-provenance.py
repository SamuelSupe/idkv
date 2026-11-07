#!/usr/bin/env python3
"""Verify the sources and binaries used for this review's recorded validation."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

OUT = Path(__file__).resolve().parent
REPO = OUT.parents[1]
GIT = '/Users/livesite/.cache/codex-runtimes/codex-primary-runtime/dependencies/bin/fallback/git'
BASE = 'ab7a0ecfa6077b3d3b3a030749bd2b5be9187dc8'


def run(args, cwd=REPO):
    return subprocess.check_output(args, cwd=cwd)


assert run([GIT, 'rev-parse', 'HEAD']).decode().strip() == BASE
sources = json.loads((OUT / 'shipping-source.json').read_text())
current = {str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
           for folder in ('src', 'tests') for p in (REPO / folder).rglob('*.rs')}
current.update({p: hashlib.sha256((REPO / p).read_bytes()).hexdigest() for p in ('Cargo.toml', 'Cargo.lock')})
assert current == sources, 'Source changed after validation'
scripts = {}
for path in ('scripts/qualification.py', 'scripts/qualification_faults.py',
             'scripts/recovery_drill.py', 'scripts/wal_io_fault.c'):
    data = (REPO / path).read_bytes()
    assert data == run([GIT, 'show', f'{BASE}:{path}'])
    scripts[path] = hashlib.sha256(data).hexdigest()

binary_dir = '/var/tmp/idkv-ab7a0ec-fixed-bin-20261107'
expected = json.loads((OUT / 'binaries.json').read_text())
binaries = {}
for binary in ('tenant-kv', 'tenant-kv-restore'):
    binaries[binary] = run(['orb', '-m', 'ubuntu', 'sha256sum', f'{binary_dir}/{binary}']).decode().split()[0]
assert binaries == expected
for label in ('qualification-fixed', 'faults'):
    env = json.loads((OUT / label / 'environment.json').read_text())
    assert env['binary_sha256'] == binaries['tenant-kv']
assert json.loads((OUT / 'qualification-fixed/environment.json').read_text())['restore_binary_sha256'] == binaries['tenant-kv-restore']

features = {}
for label, cwd in [('ab7a0ec', '/var/tmp/idkv-ab7a0ec-control-source-20261107'), ('fixed', str(REPO))]:
    # Fixed constant paths, no user input in shell source.
    metadata = json.loads(run(['orb', '-m', 'ubuntu', 'sh', '-lc',
        f'cd {cwd} && cargo metadata --locked --offline --format-version 1']))
    features[label] = {n['id']: n['features'] for n in metadata['resolve']['nodes']
                       if '#hyper-util@' in n['id'] or '#hyper@' in n['id'] or '#axum@' in n['id']}
    assert all('http2' not in v for v in features[label].values())
(OUT / 'hyper-transport-features.json').write_text(json.dumps(features, indent=2) + '\n')

test_counts = [int(n) for n in re.findall(r'test result: ok\. (\d+) passed;', (OUT / 'full-tests.log').read_text())]
assert sum(test_counts) == 202
control = json.loads((OUT / 'control-results.json').read_text())
result = {
    'base_commit': BASE, 'qualified_source': 'shipping-source.json',
    'source_matches_full_tests_clippy_fmt_release_and_runtime': True,
    'cargo_incremental': '0',
    'test_and_release_target': '/var/tmp/idkv-ab7a0ec-review-target-20261107',
    'control_source': '/var/tmp/idkv-ab7a0ec-control-source-20261107',
    'control_target': '/var/tmp/idkv-ab7a0ec-control-target-20261107',
    'control_proof': 'control-source-provenance.json',
    'binaries': binaries, 'fixed_binary_directory': binary_dir,
    'unchanged_runtime_scripts': scripts, 'full_test_passed': sum(test_counts),
    'validation_commands': [
        'CARGO_INCREMENTAL=0 cargo test --features failpoints -- --test-threads=1',
        'CARGO_INCREMENTAL=0 cargo clippy --all-targets --features failpoints -- -D warnings',
        'cargo fmt --all -- --check', 'CARGO_INCREMENTAL=0 cargo build --release --bins',
        'python3 -B scripts/qualification.py --root ROOT --binary BINARY --restore-binary RESTORE --rows 5000 --baseline-seconds 10 --combined-seconds 45 --recovery-seconds 60',
        'python3 -B scripts/qualification_faults.py --root ROOT --binary BINARY',
        'python3 -B bench-results/ab7a0ec-review-20261107/probe-http-protocol.py --root ROOT --predecessor-binary B206985 --before-binary AB7A0EC --binary FIXED',
    ],
    'toolchain': run(['orb', '-m', 'ubuntu', 'rustc', '--version']).decode().strip(),
    'control_results': control, 'runtime_summary': 'runtime-summary.json',
    'previous_b206985_features': '../b206985-review-20261107/hyper-transport-features.json',
    'initial_lib_test_diagnostic': 'initial-lib-tests.log records a corrected response-shape assertion in the test fixture; full-tests.log is the final run',
    'publication': 'Validation snapshot captured before commit; subsequent delivery is represented by the containing Git commit',
}
(OUT / 'source-provenance.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'source_match': True, 'binary_match': True, 'tests': sum(test_counts), 'http2_enabled': False}))

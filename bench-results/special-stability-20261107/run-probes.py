#!/usr/bin/env python3
"""Run isolated invariant probes; failing assertions are findings, not fixes."""
import json
from pathlib import Path
import subprocess

OUT = Path(__file__).resolve().parent
CASES = [
    'audit_failed_drain_must_resume_interrupted_index',
    'audit_deleted_registry_should_allow_new_tenant_after_churn',
    'audit_concurrent_quota_shrink_must_respect_reservations',
    'audit_real_background_recovery_after_failed_drain',
]
results = []
for case in CASES:
    command = ('cd /var/tmp/idkv-special-audit-source-20261107 && '
               'TMPDIR=/var/tmp/idkv-special-audit-tmp-20261107 '
               'CARGO_TARGET_DIR=/var/tmp/idkv-special-audit-target-20261107 '
               'CARGO_INCREMENTAL=0 cargo test --features failpoints --lib '
               + case + ' -- --test-threads=1 --nocapture')
    log = OUT / (case + '.log')
    with log.open('w') as stream:
        code = subprocess.run(['orb', '-m', 'ubuntu', 'sh', '-lc', command],
                              stdout=stream, stderr=subprocess.STDOUT, timeout=120).returncode
    text = log.read_text()
    assert 'could not compile' not in text and code in (0, 101), (case, code, log)
    observed = [json.loads(line.split('AUDIT_JSON ', 1)[1])
                for line in text.splitlines() if 'AUDIT_JSON ' in line]
    assert observed, (case, 'probe failed before collecting evidence', log)
    results.append({'case': case, 'exit_code': code, 'observed': observed,
                    'result': 'invariant failed' if code else 'invariant held', 'log': log.name})
    (OUT / 'probe-results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results[-1]), flush=True)

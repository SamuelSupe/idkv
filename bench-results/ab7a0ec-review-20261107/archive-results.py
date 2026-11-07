#!/usr/bin/env python3
"""Archive this review's completed Linux runs without databases or credentials."""
import hashlib
import json
from pathlib import Path
import shutil

OUT = Path(__file__).resolve().parent
ROOTS = {name: Path(f'/var/tmp/idkv-ab7a0ec-{name}-20261107') for name in
         ('qualification-before', 'qualification-fixed', 'faults', 'protocol')}


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2) + '\n')


for name, root in ROOTS.items():
    assert not list(root.rglob('auth.json')), root
    assert not list(root.rglob('peer-token')), root
    # Only top-level run evidence and one level of fault/protocol case logs.
    paths = list(root.iterdir())
    if name in ('faults', 'protocol'):
        paths += [p for d in root.iterdir() if d.is_dir() for p in d.iterdir()]
    for path in paths:
        if path.is_file() and path.suffix in ('.json', '.jsonl', '.log', '.gz'):
            target = OUT / name / path.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)

comparison = []
checked = {}
for name in ('qualification-before', 'qualification-fixed'):
    result = json.loads((OUT / name / 'result.json').read_text())
    assert result['result'] == 'passed'
    rows = [v for key in ('before_restart', 'after_restart', 'final_validation')
            for v in result[key]] + [result['restored_backup']]
    assert all(v['acknowledged_mismatches'] == 0 for v in rows)
    checked[name] = sum(v['documents'] for v in rows)
    for phase in result['phases']:
        comparison.append({'version': name, **{key: phase[key] for key in (
            'phase', 'requested_seconds', 'actual_seconds', 'operations',
            'completed_verified_backups', 'admission_retries', 'overlap_sample_intervals')}})
    assert result['gc_drained']['expiration_lag_ms'] == 0

faults = json.loads((OUT / 'faults/result.json').read_text())
assert faults['result'] == 'passed'
assert all(c['confirmed_documents_missing'] == 0 for c in faults['cases'])
protocol = json.loads((OUT / 'protocol/result.json').read_text())
assert protocol['result'] == 'passed'
assert all(not c['h2c_settings_returned'] and c['http1_health'] == 'ok' for c in protocol['cases'])
assert all(v['acknowledged_mismatches'] == 0 for c in protocol['cases'] for v in c['verification'])
save('performance-comparison.json', comparison)
save('runtime-summary.json', {
    'result': 'passed', 'roots': {k: str(v) for k, v in ROOTS.items()},
    'qualification_document_state_checks': checked,
    'fault_prior_confirmed_documents_preserved': sum(c['confirmed_documents_preserved'] for c in faults['cases']),
    'protocol_document_state_checks': sum(v['documents'] for c in protocol['cases'] for v in c['verification']),
    'fault_cases': [{k: c[k] for k in ('case', 'confirmed_documents_missing',
                    'uncertain_batch_visible_before_retry', 'restart_to_safe_retry_ms')} for c in faults['cases']],
    'protocol_versions': [c['version'] for c in protocol['cases']],
    'scope': 'OrbStack Ubuntu aarch64 native filesystem; syscall EIO injection; OpenDAL fs backup backend',
})
save('archived-files.json', {str(p.relative_to(OUT)): hashlib.sha256(p.read_bytes()).hexdigest()
     for name in ROOTS for p in sorted((OUT / name).rglob('*')) if p.is_file()})
print(json.dumps({'result': 'passed', 'qualification_checks': checked,
                  'archived_files': sum(p.is_file() for name in ROOTS for p in (OUT / name).rglob('*'))}))

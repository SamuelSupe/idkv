#!/usr/bin/env python3
"""Check the real old/new server binaries against a v5 registry and ACK model."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.request
import uuid
import zlib

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from qualification import Lab
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--old-binary', type=Path, required=True)
a = p.parse_args()
lab = Lab(a.root, a.binary, replicated=False)
try:
    control = lab.node_roots[0] / 'control'
    control.mkdir(parents=True)
    retired = str(uuid.uuid4())
    generation, history = str(uuid.uuid4()), str(uuid.uuid4())
    registry = control / 'tenants.json'
    data = json.dumps({'version': 5, 'replication_enabled': False, 'tenants': {},
        'deleted': [[retired, 'historical', generation, history, 128 << 20, 4 << 30]]},
        separators=(',', ':')).encode()
    registry.write_bytes(b'TKJ1' + zlib.crc32(data).to_bytes(4, 'little') + data)
    lab.start(0)
    status = lab.call(0, f'/v1/tenants/{retired}')
    assert status['lifecycle']['state'] == 'deleted'
    assert status['tenant']['generation'] == generation and status['tenant']['history'] == history
    tenant = lab.seed('v5-sentinel', 100, 128)
    before = lab.verify(0, tenant)
    request = urllib.request.Request(lab.urls[0] + '/metrics',
        headers={'Authorization': 'Bearer ' + lab.token})
    with urllib.request.urlopen(request, timeout=5) as response:
        metrics = response.read().decode()
    gauges = {key: int(value) for key, value in
        re.findall(r'^(tkv_registry_\w+) (\d+)$', metrics, re.MULTILINE)}
    assert gauges['tkv_registry_metadata_capacity_bytes'] == 16 << 20
    assert gauges['tkv_registry_metadata_bytes'] == registry.stat().st_size
    assert gauges['tkv_registry_deleted_tenants'] == 1
    lab.stop(0)
    digest = hashlib.sha256(registry.read_bytes()).hexdigest()
    with (lab.root / 'old-reader.log').open('w') as log:
        old = subprocess.run([str(a.old_binary), 'serve', '--root', str(lab.node_roots[0]),
            '--auth', str(lab.root / 'auth.json'), '--listen', f'127.0.0.1:{lab.ports[0]}'],
            stdout=log, stderr=subprocess.STDOUT, timeout=5)
    assert old.returncode != 0 and 'registry version' in (lab.root / 'old-reader.log').read_text()
    assert hashlib.sha256(registry.read_bytes()).hexdigest() == digest
    lab.start(0)
    after = lab.verify(0, tenant)
    assert before['sha256'] == after['sha256']
    result = {'result': 'passed', 'registry_version': 5, 'old_reader_exit_code': old.returncode,
        'old_reader_rejected_without_registry_change': True, 'retired_status_preserved': True,
        'metrics': gauges, 'verification': [before, after]}
    save(lab.root / 'result.json', result)
    print(json.dumps(result), flush=True)
finally:
    lab.close()

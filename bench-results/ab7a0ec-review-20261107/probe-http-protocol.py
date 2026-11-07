#!/usr/bin/env python3
"""Probe actual local binaries with HTTP/2 prior-knowledge preface and HTTP/1 health."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from qualification import Lab
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--predecessor-binary', type=Path, required=True)
p.add_argument('--before-binary', type=Path, required=True)
p.add_argument('--binary', type=Path, required=True)
a = p.parse_args()
a.root.mkdir(parents=True, exist_ok=False)
result = {'cases': []}
for label, binary in [('b206985', a.predecessor_binary), ('ab7a0ec', a.before_binary), ('fixed', a.binary)]:
    lab = Lab(a.root / label, binary, replicated=False)
    try:
        lab.start(0)
        tenant = lab.seed('protocol-sentinel', 100, 128)
        checks = [lab.verify(0, tenant)]
        with socket.create_connection(('127.0.0.1', lab.ports[0]), timeout=1) as client:
            client.sendall(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n' + b'\0\0\0\4\0\0\0\0\0')
            try:
                response = client.recv(4096)
            except ConnectionResetError:
                response = b''
        settings = len(response) >= 9 and response[3] == 4 and response[5:9] == b'\0\0\0\0'
        assert not settings, (label, 'unexpected HTTP/2 support; re-evaluate the compatibility claim')
        assert lab.call(0, '/health')['status'] == 'ok'
        checks.append(lab.verify(0, tenant))
        lab.stop(0, kill=True)
        lab.start(0)
        checks.append(lab.verify(0, tenant))
        assert len({s['sha256'] for s in checks}) == 1
        row = {'version': label, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
               'h2c_settings_returned': settings, 'response_prefix_hex': response[:32].hex(),
               'http1_health': 'ok', 'verification': checks}
        save(lab.root / 'verification.json', checks)
        result['cases'].append(row)
        save(a.root / 'result.json', result)
        print(json.dumps(row), flush=True)
    finally:
        lab.close()
result['result'] = 'passed'
save(a.root / 'result.json', result)

#!/usr/bin/env python3
"""Compare idle, partial-header and keep-alive exhaustion on own local servers."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from qualification import Lab
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--before-binary', type=Path, required=True)
a = p.parse_args()
a.root.mkdir(exist_ok=False, parents=True)
result = {'cases': [], 'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(),
          'before_sha256': hashlib.sha256(a.before_binary.read_bytes()).hexdigest()}


def health(client):
    client.sendall(b'GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n')
    data = b''
    while b'\r\n\r\n' not in data:
        part = client.recv(4096)
        assert part, 'health connection closed'
        data += part
    head, body = data.split(b'\r\n\r\n', 1)
    assert head.startswith(b'HTTP/1.1 200'), head
    length = next(int(line.split(b':', 1)[1]) for line in head.lower().split(b'\r\n') if line.startswith(b'content-length:'))
    while len(body) < length:
        part = client.recv(4096)
        assert part, "truncated health response"
        body += part
    return json.loads(body[:length])


for fixed in [False, True]:
    name = 'fixed' if fixed else 'before'
    lab = Lab(a.root / name, a.binary if fixed else a.before_binary, replicated=False)
    lab.options.update(max_http_connections=8, http_write_timeout_ms=200)
    if fixed:
        lab.options['http_header_timeout_ms'] = 1000
    save(lab.root / 'options.json', lab.options)
    checks = []
    try:
        lab.start(0)
        tenant = lab.seed('confirmed-connection-sentinel', 100, 128)
        checks.append(lab.verify(0, tenant))
        for kind in ['idle', 'partial-header', 'keep-alive']:
            clients = []
            try:
                for _ in range(8):
                    client = socket.create_connection(('127.0.0.1', lab.ports[0]), timeout=.5)
                    clients.append(client)
                    if kind == 'partial-header':
                        client.sendall(b'GET /health HTTP/1.1\r\nHost: localhost\r\nX-Slow: ')
                    elif kind == 'keep-alive':
                        assert health(client)['status'] == 'ok'
                time.sleep(.05)
                with socket.create_connection(('127.0.0.1', lab.ports[0]), timeout=.2) as overflow:
                    overflow.settimeout(.2)
                    start = time.perf_counter()
                    try:
                        closed = overflow.recv(1) == b''
                    except ConnectionResetError:
                        closed = True
                    except socket.timeout:
                        closed = False
                    overflow_ms = (time.perf_counter() - start) * 1000
                assert closed == fixed, (name, kind, closed)
                time.sleep(1.2)
                with socket.create_connection(('127.0.0.1', lab.ports[0]), timeout=.2) as probe:
                    probe.settimeout(.2)
                    try:
                        available = health(probe)['status'] == 'ok'
                    except socket.timeout:
                        available = False
                assert available == fixed, (name, kind, available)
                row = {'version': name, 'case': kind, 'held_connections': 8,
                       'overflow_closed_immediately': closed, 'overflow_wait_ms': overflow_ms,
                       'health_available_after_header_deadline': available}
                result['cases'].append(row)
                save(a.root / 'result.json', result)
                print(json.dumps(row), flush=True)
            finally:
                for client in clients:
                    client.close()
            lab.retry(0, '/health')
        checks.append(lab.verify(0, tenant))
        lab.stop(0, kill=True)
        lab.start(0)
        checks.append(lab.verify(0, tenant))
        assert len({c['sha256'] for c in checks}) == 1
        save(lab.root / 'verification.json', checks)
    finally:
        lab.close()
result['result'] = 'passed'
save(a.root / 'result.json', result)

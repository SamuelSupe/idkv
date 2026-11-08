#!/usr/bin/env python3
"""Real local TCP saturation, slow headers, keep-alive and stalled response readers."""
import hashlib
import http.client
import json
from pathlib import Path
import socket
import sys
import threading
import time

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab

binary, out = Path(sys.argv[1]), Path(sys.argv[2])
lab = Lab(out, binary, replicated=False)
lab.options.update(max_http_connections=16, http_header_timeout_ms=300, http_write_timeout_ms=1000,
                   tenant_memory_bytes=256 << 20)
(out / "options.json").write_text(json.dumps(lab.options))
result = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "cases": []}
clients = []
stop_sampling = threading.Event()
sampler = None

def health(client):
    client.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
    response = http.client.HTTPResponse(client)
    response.begin()
    assert response.status == 200
    value = json.loads(response.read())
    response.close()
    return value

try:
    lab.start(0)
    tenant = lab.seed("connection-sentinel", 400, 32768)
    result["before"] = lab.verify(0, tenant)
    for kind in ["idle", "partial-header", "keep-alive"]:
        try:
            for _ in range(16):
                client = socket.create_connection(("127.0.0.1", lab.ports[0]), timeout=1)
                clients.append(client)
                if kind == "partial-header":
                    client.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")
                elif kind == "keep-alive":
                    assert health(client)["status"] == "ok"
            time.sleep(.03)
            with socket.create_connection(("127.0.0.1", lab.ports[0]), timeout=.2) as overflow:
                started = time.monotonic()
                try:
                    assert overflow.recv(1) == b""
                except ConnectionResetError:
                    pass
                close_ms = (time.monotonic() - started) * 1000
            time.sleep(.5)
            assert lab.call(0, "/health")["status"] == "ok"
            result["cases"].append({"case": kind, "connections": 16, "overflow_close_ms": close_ms,
                                    "health_after_expiry": True})
        finally:
            for client in clients:
                client.close()
            clients.clear()
    before = lab.call(0, "/v1/metrics")["memory_bytes"]
    samples = []
    def sample_memory():
        while not stop_sampling.is_set():
            samples.append(lab.call(0, "/v1/metrics")["memory_bytes"])
            stop_sampling.wait(.01)
    sampler = threading.Thread(target=sample_memory)
    sampler.start()
    for _ in range(2):
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.settimeout(2)
        client.connect(("127.0.0.1", lab.ports[0]))
        clients.append(client)
        body = b'{"limit":400}'
        request = (f"POST /v1/tenants/{tenant}/collections/assets/query HTTP/1.1\r\n"
                   f"Host: localhost\r\nAuthorization: Bearer {lab.token}\r\n"
                   f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n").encode() + body
        client.sendall(request)
        time.sleep(.03)
    statuses = [client.recv(128, socket.MSG_PEEK).split(b"\r\n", 1)[0].decode() for client in clients]
    assert any(status.startswith("HTTP/1.1 200") for status in statuses), statuses
    time.sleep(2)
    stop_sampling.set()
    sampler.join()
    peak = max(samples)
    after = lab.call(0, "/v1/metrics")["memory_bytes"]
    result["response_probe"] = {"statuses": statuses, "memory_before": before,
                                "memory_peak": peak, "memory_after_timeout": after,
                                "samples": len(samples)}
    assert peak - after > 1 << 20, (before, peak, after)
    result["cases"].append({"case": "stalled-response-readers", "connections": 2,
                            "statuses": statuses, "memory_before": before,
                            "memory_peak": peak, "memory_after_timeout": after})
    for client in clients:
        client.close()
    clients.clear()
    result["after"] = lab.verify(0, tenant)
    lab.stop(0, kill=True)
    lab.start(0)
    result["after_restart"] = lab.verify(0, tenant)
    result["result"] = "passed"
finally:
    stop_sampling.set()
    if sampler is not None:
        sampler.join(timeout=5)
    for client in clients:
        client.close()
    lab.close()
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

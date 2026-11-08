#!/usr/bin/env python3
"""Run the ordinary replica bootstrap through a deliberately slow test link."""
import collections
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=False)
BINARY = Path(os.environ.get("IDKV_BINARY", "/var/tmp/idkv-stability4-bin-20261107/tenant-kv"))
shim = OUT / "wal-io-fault.so"
subprocess.run(["gcc", "-shared", "-fPIC", "-O2", str(REPO / "scripts/wal_io_fault.c"), "-o", str(shim)], check=True)
lab = Lab(OUT / "lab", BINARY)
control = OUT / "fault-control"
chunk_ready, finish_transfer = threading.Event(), threading.Event()
network_events = []
network_lock = threading.Lock()
target_tenant = None
snapshots = {}

class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *args):
        pass
    def do_GET(self):
        self.forward()
    def do_POST(self):
        self.forward()
    def forward(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        req = urllib.request.Request(lab.urls[0] + self.path, data=body if self.command == "POST" else None,
            method=self.command, headers={"Authorization": self.headers["Authorization"], "Content-Type": "application/json"})
        try:
            response = urllib.request.urlopen(req, timeout=25)
        except urllib.error.HTTPError as error:
            response = error
        except urllib.error.URLError:
            self.close_connection = True
            return
        with response:
            raw, status = response.read(), response.status
        if self.path.endswith("/snapshot") and status == 200:
            info = json.loads(raw)
            snapshots[info["id"]] = info["tenant"]["id"]
        if self.path.endswith("/snapshot/chunk") and snapshots.get(json.loads(body)["id"]) == target_tenant and not finish_transfer.is_set():
            chunk_ready.set()
            if not finish_transfer.wait(20):
                raise TimeoutError("test transfer gate")
        with network_lock:
            network_events.append({"path": self.path, "status": status, "time": time.monotonic()})
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass  # A replica can close an in-flight poll during test shutdown.

proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
proxy.daemon_threads = True
thread = threading.Thread(target=proxy.serve_forever, daemon=True)
thread.start()
config = lab.root / "replication-1.json"
cfg = json.loads(config.read_text())
cfg["peer_url"] = f"http://127.0.0.1:{proxy.server_address[1]}"
config.write_text(json.dumps(cfg))
result = {"binary_sha256": hashlib.sha256(BINARY.read_bytes()).hexdigest(), "errors": []}
try:
    lab.start(0, {"LD_PRELOAD": str(shim), "IDKV_IO_ROOT": str(lab.node_roots[0]),
        "IDKV_IO_CONTROL": str(control), "IDKV_IO_LOG": str(OUT / "faults.log")})
    tenant = lab.seed("slow-bootstrap", 100, 1024)
    other = lab.seed("healthy-control", 10, 512)
    context = lab.context(tenant, False)
    other_context = lab.context(other, False)
    result["tenant"] = tenant
    target_tenant = tenant
    lab.start(1)
    assert chunk_ready.wait(15), "replica did not start snapshot transfer"
    control.write_text("sync")
    try:
        lab.write(tenant, [0], 1, 1024, context, "unknown", "unknown", retry=False)
        raise AssertionError("WAL fault did not trigger")
    except RequestError as error:
        assert error.code == "COMMIT_UNKNOWN", error
        result["injected_write_error"] = str(error)
    finally:
        control.unlink(missing_ok=True)
    start = time.monotonic()
    while time.monotonic() - start < 6:
        try:
            lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=False)
            result["recovered_while_transfer_paused"] = True
            break
        except RequestError as error:
            result["errors"].append({"elapsed_s": round(time.monotonic() - start, 3), "code": error.code})
        lab.call(0, "/health")
        time.sleep(.05)
    result["blocked_after_device_healed_s"] = time.monotonic() - start
    result["source_status_while_blocked"] = lab.call(0, "/v1/replication/status")
    if not result.get("recovered_while_transfer_paused"):
        try:
            lab.call(0, "/v1/replication/switchover", {})
            raise AssertionError("switchover unexpectedly succeeded with a poisoned source tenant")
        except RequestError as error:
            result["switchover_while_blocked"] = str(error)
    result["source_role_after_switchover_attempt"] = lab.call(0, "/v1/replication/status")["node"]["role"]
    # The fault was tenant-specific; the other tenant must still write successfully.
    lab.write(other, [0], 3, 512, other_context, "control-while-blocked", "control", retry=False)
    result["other_tenant_write_succeeded"] = True
    resumed = time.monotonic()
    finish_transfer.set()
    deadline = resumed + 20
    while True:
        try:
            lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=False)
            break
        except RequestError as error:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.025)
    result["recovery_after_transfer_resumed_ms"] = (time.monotonic() - resumed) * 1000
    deadline = time.monotonic() + 20
    while True:
        try:
            lab.verify(1, tenant)
            lab.verify(1, other)
            break
        except (AssertionError, RequestError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(.05)
    lab.verify(0, tenant)
    lab.verify(0, other)
    result["acknowledged_data_verified_on_both_nodes"] = True
    result["replica_status_final"] = lab.call(1, "/v1/replication/status")
    lab.stop(1, kill=True)
    lab.stop(0, kill=True)
    lab.start(0)
    lab.start(1)
    lab.verify(0, tenant)
    lab.verify(0, other)
    lab.verify(1, tenant)
    lab.verify(1, other)
    result["acknowledged_data_verified_after_sigkill_restart"] = True
    switched = lab.call(0, "/v1/replication/switchover", {})
    assert switched["role"] == "replica", switched
    assert lab.call(1, "/v1/replication/status")["node"]["role"] == "primary"
    lab.verify(0, tenant)
    lab.verify(1, tenant)
    result["planned_switchover_verified"] = True
finally:
    finish_transfer.set()
    control.unlink(missing_ok=True)
    lab.stop(1)
    lab.close()
    proxy.shutdown()
    proxy.server_close()
    thread.join(timeout=5)
    shim.unlink(missing_ok=True)
    result["network_events"] = network_events
    (OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k:v for k,v in result.items() if k not in ("errors", "network_events", "replica_status_final", "source_status_while_blocked")}, indent=2))
print("error_counts", dict(collections.Counter(e["code"] for e in result["errors"])))

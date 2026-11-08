#!/usr/bin/env python3
"""Pause private checkpoint hashing, then test business recovery before resuming it."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=False)
BINARY = Path(os.environ.get("IDKV_BINARY", "/var/tmp/idkv-stability4-bin-20261107/tenant-kv"))
source, shim = OUT / "fault.c", OUT / "fault.so"
source.write_text((REPO / "scripts/wal_io_fault.c").read_text() + Path(__file__).with_name("hash-pause.c").read_text())
subprocess.run(["gcc", "-shared", "-fPIC", "-O2", "-U_FORTIFY_SOURCE", str(source), "-o", str(shim)], check=True)
lab = Lab(OUT / "lab", BINARY)
fault, pause, hash_log = OUT / "fault-control", OUT / "hash-control", OUT / "hash.log"
result = {"binary_sha256": hashlib.sha256(BINARY.read_bytes()).hexdigest(), "errors": []}
finished = threading.Event()
worker = None

def prepare(body):
    try:
        result["snapshot"] = lab.call(0, "/v1/replication/snapshot", body)
    except RequestError as error:
        result["snapshot_error"] = error.code
    except Exception as error:
        result["unexpected_snapshot_error"] = repr(error)
    finally:
        finished.set()

try:
    lab.start(0, {"LD_PRELOAD": str(shim), "IDKV_IO_ROOT": str(lab.node_roots[0]),
        "IDKV_IO_CONTROL": str(fault), "IDKV_IO_LOG": str(OUT / "faults.log"),
        "IDKV_HASH_CONTROL": str(pause), "IDKV_HASH_LOG": str(hash_log)})
    tenant = lab.seed("hash-recovery", 100, 1024)
    other = lab.seed("healthy-control", 10, 512)
    context = lab.context(tenant, False)
    state = lab.call(0, "/v1/replication/status")["node"]
    body = {"protocol": 3, "cluster": state["cluster"], "epoch": state["epoch"], "tenant": tenant}
    pause.write_text("pause")
    worker = threading.Thread(target=prepare, args=(body,))
    worker.start()
    deadline = time.monotonic() + 15
    while not hash_log.exists():
        assert time.monotonic() < deadline and not finished.is_set(), result
        time.sleep(.01)
    fault.write_text("sync")
    try:
        lab.write(tenant, [0], 1, 1024, context, "unknown", "unknown", retry=False)
        raise AssertionError("WAL fault did not trigger")
    except RequestError as error:
        assert error.code == "COMMIT_UNKNOWN", error
        result["injected_write_error"] = str(error)
    finally:
        fault.unlink(missing_ok=True)
    start = time.monotonic()
    while time.monotonic() - start < 6:
        try:
            lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=False)
            result["recovered_while_hash_paused"] = True
            break
        except RequestError as error:
            result["errors"].append(error.code)
        time.sleep(.05)
    result["recovery_observation_s"] = time.monotonic() - start
    assert not finished.is_set(), "hashing ended before the recovery observation"
    result["hash_still_paused_at_recovery_observation"] = True
    pause.unlink()
    worker.join(timeout=15)
    assert not worker.is_alive() and "unexpected_snapshot_error" not in result, result
    if "snapshot" in result:
        lab.call(0, "/v1/replication/snapshot/release", {"id": result["snapshot"]["id"]})
    lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=True)
    replacement = lab.call(0, "/v1/replication/snapshot", body)
    lab.call(0, "/v1/replication/snapshot/release", {"id": replacement["id"]})
    result["replacement_snapshot_created"] = True
    lab.verify(0, tenant)
    lab.verify(0, other)
    lab.stop(0, kill=True)
    lab.start(0)
    lab.verify(0, tenant)
    lab.verify(0, other)
    result["acknowledged_data_verified_after_sigkill_restart"] = True
finally:
    fault.unlink(missing_ok=True)
    pause.unlink(missing_ok=True)
    if worker is not None:
        worker.join(timeout=30)
    lab.close()
    shim.unlink(missing_ok=True)
    (OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k:v for k,v in result.items() if k not in ("errors", "snapshot")}, indent=2))
print("post_heal_errors", len(result["errors"]))

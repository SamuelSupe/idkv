#!/usr/bin/env python3
"""Review-only, unmodified Linux release binary; leave product sources unchanged."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--binary", type=Path, required=True)
args = parser.parse_args()
args.root.mkdir(parents=True, exist_ok=False)
result = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}


def configured(name, replicated, memtable):
    lab = Lab(args.root / name, args.binary, replicated=replicated)
    lab.options.update(global_memory_bytes=64 << 20, tenant_memory_bytes=8 << 20,
                       memtable_bytes=memtable, cache_bytes=0)
    save(lab.root / "options.json", lab.options)
    return lab


def try_call(lab, node, path, body=None, method=None):
    try:
        return {"ok": True, "value": lab.call(node, path, body, method)}
    except RequestError as error:
        return {"ok": False, "status": error.status, "code": error.code,
                "value": error.value}


def fingerprint(doc):
    return {"rev": doc["_rev"], "payload_bytes": len(doc["payload"]),
            "sha256": hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()}


lab = configured("flush", False, 4 << 20)
try:
    lab.start(0)
    tenant = lab.call(0, "/v1/tenants", {"name": "flush-working-set"})
    prefix = "/v1/tenants/" + tenant["id"]
    docs = prefix + "/collections/docs/documents"
    lab.call(0, prefix + "/collections", {"name": "docs"})
    ack = lab.call(0, docs, {"document": {"_key": "large", "payload": "x" * (700 << 10)}}, "PUT")
    before = lab.call(0, docs + "/large")
    flush = lab.call(0, prefix + "/checkpoint", {})
    after = try_call(lab, 0, docs + "/large")
    check = try_call(lab, 0, prefix + "/check", {})
    lab.stop(0, kill=True)
    lab.start(0)
    restart = try_call(lab, 0, docs + "/large")
    lab.call(0, prefix, {"memory_bytes": 16 << 20, "disk_bytes": tenant["disk_bytes"]}, "PATCH")
    restored = lab.call(0, docs + "/large")
    assert before == restored
    assert not after["ok"] and after["code"] == "RESOURCE_EXHAUSTED"
    assert not restart["ok"] and restart["code"] == "RESOURCE_EXHAUSTED"
    result["flush"] = {"tenant": tenant, "ack": ack, "before": fingerprint(before),
                       "checkpoint": flush, "after": after, "check": check,
                       "after_restart": restart, "after_quota_expansion": fingerprint(restored),
                       "acknowledged_mismatches_after_expansion": 0}
    save(args.root / "result.json", result)
    print(json.dumps({"event": "flush_probe_complete", "result": result["flush"]}), flush=True)
finally:
    lab.close()

lab = configured("replica-default", True, 1 << 20)
try:
    lab.start(0)
    tenant = lab.call(0, "/v1/tenants", {"name": "replica-working-set"})
    prefix = "/v1/tenants/" + tenant["id"]
    docs = prefix + "/collections/docs/documents"
    lab.call(0, prefix, {"memory_bytes": 16 << 20, "disk_bytes": tenant["disk_bytes"]}, "PATCH")
    lab.call(0, prefix + "/collections", {"name": "docs"})
    ack = lab.call(0, docs, {"document": {"_key": "large", "payload": "x" * (700 << 10)}}, "PUT")
    lab.call(0, prefix + "/checkpoint", {})
    before = lab.call(0, docs + "/large")
    assert lab.call(0, prefix + "/check", {})["ok"]
    lab.start(1)
    started = time.monotonic()
    while time.monotonic() - started < 20:
        status = lab.call(1, "/v1/replication/status")
        progress = status.get("progress", status)
        if progress.get("initialization_failures", 0) >= 4:
            break
        time.sleep(.05)
    failure_seconds = time.monotonic() - started
    assert status["initialization_failures"] >= 4 and not status["tenants"], status
    assert "tenant memory budget exhausted" in status["initialization_errors"][tenant["id"]], status
    save(args.root / "replica-failure-status.json", status)
    lab.stop(1, kill=True)
    lab.options["tenant_memory_bytes"] = 16 << 20
    save(lab.root / "options.json", lab.options)
    lab.start(1)
    recovered_at = time.monotonic()
    lab.catch_up()
    restored = lab.call(1, docs + "/large")
    assert restored == before
    result["replica_default"] = {"node_initial_default_bytes": 8 << 20,
        "tenant_quota_bytes": 16 << 20, "ack": ack, "source": fingerprint(before),
        "failed_status": status, "observation_seconds": failure_seconds,
        "only_follower_default_raised_to_bytes": 16 << 20,
        "catchup_seconds_after_restart": time.monotonic() - recovered_at,
        "restored_status": lab.call(1, "/v1/replication/status"),
        "replica": fingerprint(restored), "acknowledged_mismatches": 0}
    print(json.dumps({"event": "replica_probe_complete", "result": result["replica_default"]}), flush=True)
finally:
    lab.close()
    save(args.root / "result.json", result)

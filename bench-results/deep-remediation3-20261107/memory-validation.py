#!/usr/bin/env python3
"""Run original public HTTP workloads, a strict quota case, and an old-SST upgrade."""
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

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--before-binary", type=Path, required=True)
a = p.parse_args()
a.root.mkdir(parents=True, exist_ok=False)
results = {"before_binary_sha256": hashlib.sha256(a.before_binary.read_bytes()).hexdigest(),
           "fixed_binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(), "cases": []}


def configured(name, binary, replicated=False, default=8 << 20, memtable=1 << 20):
    lab = Lab(a.root / name, binary, replicated=replicated)
    lab.options.update(global_memory_bytes=64 << 20, tenant_memory_bytes=default,
                       memtable_bytes=memtable, cache_bytes=0)
    save(lab.root / "options.json", lab.options)
    if replicated:
        for node in [0, 1]:
            config = json.loads((lab.root / f"replication-{node}.json").read_text())
            config["log_retention_bytes"] = 1 << 20
            save(lab.root / f"replication-{node}.json", config)
    return lab


def seed(lab, name, quota=None, random=False):
    info = lab.call(0, "/v1/tenants", {"name": name})
    prefix = "/v1/tenants/" + info["id"]
    if quota:
        lab.call(0, prefix, {"memory_bytes": quota, "disk_bytes": info["disk_bytes"]}, "PATCH")
    lab.call(0, prefix + "/collections", {"name": "docs"})
    document = lab.document(0, 0, 700 << 10) if random else {"_key": "large", "payload": "x" * (700 << 10)}
    ack = lab.call(0, prefix + "/collections/docs/documents", {"document": document}, "PUT")
    expected = dict(document, _rev=ack["rev"])
    return info["id"], prefix, ack, expected


def verify(lab, node, prefix, expected):
    actual = lab.call(node, prefix + "/collections/docs/documents/" + expected["_key"])
    assert actual == expected
    return {"rev": actual["_rev"], "payload_bytes": len(actual["payload"]),
            "sha256": hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()}


def record(case):
    results["cases"].append(case)
    save(a.root / "result.json", results)
    print(json.dumps(case), flush=True)


def settled(lab):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = lab.call(1, "/v1/replication/status")
        assert status["initialization_failures"] == 0, status
        if not status["initializing"] and status["last_error"] is None:
            return status
        time.sleep(.02)
    raise AssertionError("initialization worker did not finish")


# Preserve the old failure on disk; change the executable only, with quota unchanged.
lab = configured("upgrade-old-sst", a.before_binary, memtable=4 << 20)
try:
    lab.start(0)
    tenant, prefix, ack, expected = seed(lab, "upgrade-old-sst")
    before = verify(lab, 0, prefix, expected)
    lab.call(0, prefix + "/checkpoint", {})
    try:
        verify(lab, 0, prefix, expected)
    except RequestError as error:
        assert error.code == "RESOURCE_EXHAUSTED"
        failure = error.value
    else:
        raise AssertionError("old binary no longer reproduces the failure")
    lab.stop(0, kill=True)
    lab.binary = a.binary
    lab.start(0)
    after = verify(lab, 0, prefix, expected)
    assert after == before
    assert lab.call(0, prefix)["tenant"]["memory_bytes"] == 8 << 20
    assert lab.call(0, prefix + "/check", {})["ok"]
    record({"case": "old_sst_upgrade_without_quota_change", "ack": ack,
            "old_failure": failure, "before": before, "after": after,
            "quota_bytes": 8 << 20, "acknowledged_mismatches": 0})
finally:
    lab.close()

for random in [False, True]:
    lab = configured("background-" + str(random).lower(), a.binary)
    try:
        lab.start(0)
        tenant, prefix, ack, expected = seed(lab, "background-flush", random=random)
        started = time.monotonic()
        reads = 0
        while time.monotonic() - started < .3 or reads < 20:
            verify(lab, 0, prefix, expected)
            reads += 1
            time.sleep(.005)
        status = lab.call(0, prefix)
        assert status["stats"]["io"]["flush_bytes"] > 0, status
        assert not status["stats"]["maintenance_error"], status
        try:
            lab.call(0, prefix, {"memory_bytes": 2 << 20, "disk_bytes": 4 << 30}, "PATCH")
        except RequestError as error:
            assert error.code == "RESOURCE_EXHAUSTED"
            shrink = error.value
        else:
            raise AssertionError("shrink accepted a quota below the document read working set")
        assert lab.call(0, prefix)["tenant"]["memory_bytes"] == 8 << 20
        verify(lab, 0, prefix, expected)
        lab.call(0, prefix + "/compact", {})
        assert lab.call(0, prefix + "/check", {})["ok"]
        lab.stop(0, kill=True)
        lab.start(0)
        recovered = verify(lab, 0, prefix, expected)
        record({"case": "automatic_flush_compact_restart", "random_payload": random,
                "ack": ack, "successful_reads": reads, "status_after_background_flush": status,
                "unsafe_shrink_rejected": shrink,
                "after_restart": recovered, "quota_bytes": 8 << 20,
                "acknowledged_mismatches": 0})
    finally:
        lab.close()

for default in [8 << 20, 1 << 20]:
    lab = configured("replica-" + str(default), a.binary, replicated=True,
                     default=default, memtable=128 << 10)
    try:
        lab.start(0)
        tenant, prefix, ack, expected = seed(lab, "larger-than-default", quota=16 << 20)
        lab.call(0, prefix + "/checkpoint", {})
        source = verify(lab, 0, prefix, expected)
        assert lab.call(0, prefix + "/check", {})["ok"]
        started = time.monotonic()
        lab.start(1)
        lab.catch_up()
        first_status = settled(lab)
        first_seconds = time.monotonic() - started
        replica = verify(lab, 1, prefix, expected)
        assert first_status["initialization_failures"] == 0
        assert first_status["tenants"][0]["info"]["memory_bytes"] == 16 << 20
        assert lab.options["tenant_memory_bytes"] == default
        reinit = None
        if default == 1 << 20:
            lab.stop(1, kill=True)
            receipts = []
            for letter in ["y", "z", "w"]:
                document = {"_key": "large", "payload": letter * (700 << 10)}
                latest = lab.call(0, prefix + "/collections/docs/documents", {"document": document}, "PUT")
                receipts.append(latest)
                expected = dict(document, _rev=latest["rev"])
                lab.call(0, prefix + "/checkpoint", {})
            lab.start(1)
            lab.catch_up()
            status = settled(lab)
            latest_source = verify(lab, 0, prefix, expected)
            latest_replica = verify(lab, 1, prefix, expected)
            assert status["reinitializations"] >= 1 and status["initialization_failures"] == 0, status
            reinit = {"status": status, "source": latest_source, "replica": latest_replica,
                      "acknowledgements": receipts}
        record({"case": "replica_actual_quota", "node_default_bytes": default,
                "tenant_quota_bytes": 16 << 20, "ack": ack, "source": source, "replica": replica,
                "startup_and_catchup_seconds": first_seconds, "first_status": first_status,
                "existing_tenant_reinitialization": reinit, "acknowledged_mismatches": 0})
    finally:
        lab.close()

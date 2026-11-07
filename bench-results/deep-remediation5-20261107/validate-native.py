#!/usr/bin/env python3
"""Repeat slow-body isolation, local-backup growth, and large-checkpoint pin scenarios."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import socket
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
result = {"fixed_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
          "before_sha256": hashlib.sha256(a.before_binary.read_bytes()).hexdigest(), "cases": []}


def record(value):
    result["cases"].append(value)
    save(a.root / "result.json", result)
    print(json.dumps(value), flush=True)


def base(tenant):
    return "/v1/tenants/" + tenant


def seed(lab, name):
    tenant = lab.call(0, "/v1/tenants", {"name": name})["id"]
    lab.call(0, base(tenant) + "/collections", {"name": "docs"})
    return tenant


def put(lab, tenant, model, i, generation, size=1024, retry=True):
    document = lab.document(i, generation, size)
    receipt = (lab.retry if retry else lab.call)(0, base(tenant) + "/collections/docs/documents", {"document": document}, method="PUT")
    actual = dict(document, _rev=receipt["rev"])
    model[document["_key"]] = actual
    lab.journal.write(json.dumps({"tenant": tenant, "key": document["_key"], "generation": generation,
        "payload_bytes": size, "rev": receipt["rev"], "sha256": hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()}) + "\n")


def verify(lab, tenant, model, stage, node=0):
    observed, cursor = {}, None
    while True:
        query = {"limit": 500}
        if cursor:
            query["cursor"] = cursor
        page = lab.retry(node, base(tenant) + "/collections/docs/query", query)
        for row in page["documents"]:
            assert row == model.get(row["_key"]), (stage, row["_key"])
            assert row["_key"] not in observed
            observed[row["_key"]] = row
        cursor = page.get("cursor")
        if not cursor:
            break
    assert observed == model, stage
    rows = [{"key": key, "rev": row["_rev"], "sha256": hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()}
            for key, row in sorted(observed.items())]
    lab.retry(node, base(tenant) + "/check", {})
    name = f"verified-{stage}-node-{node}.json"
    save(lab.root / name, rows)
    summary = {"stage": stage, "node": node, "file": name, "documents": len(rows), "mismatches": 0,
               "ack_count": len((lab.root / "acknowledgements.jsonl").read_text().splitlines()),
               "sha256": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}
    stages = getattr(lab, "stages", [])
    stages.append(summary)
    lab.stages = stages
    save(lab.root / "verification-stages.json", stages)
    return summary


def slow_bodies(fixed):
    name = "slow-bodies-" + ("fixed" if fixed else "before")
    lab = Lab(a.root / name, a.binary if fixed else a.before_binary, replicated=False)
    sockets, model, errors, health_ms = [], {}, Counter(), []
    try:
        lab.start(0)
        slow = [seed(lab, f"slow-{i}") for i in range(2)]
        tenant = seed(lab, "healthy")
        for i in range(64):
            put(lab, tenant, model, i, 0)
        verify(lab, tenant, model, "seed")
        for id in slow:
            for _ in range(32):
                connection = socket.create_connection(("127.0.0.1", lab.ports[0]), timeout=2)
                connection.sendall((f"POST {base(id)}/collections HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {lab.token}\r\nContent-Length: 100\r\n\r\n").encode())
                sockets.append(connection)
        time.sleep(.25)
        successes = 0
        for i in range(128):
            started = time.perf_counter()
            lab.call(0, "/health")
            health_ms.append((time.perf_counter() - started) * 1000)
            try:
                put(lab, tenant, model, i % 64, i // 64 + 1, retry=False)
                successes += 1
            except RequestError as error:
                errors[error.code] += 1
        if fixed:
            assert successes == 128 and not errors, (successes, errors)
        else:
            assert successes == 0 and errors == {"RESOURCE_EXHAUSTED": 128}, (successes, errors)
        for connection in sockets:
            connection.close()
        sockets.clear()
        checked = verify(lab, tenant, model, "after-slow-bodies")
        lab.stop(0, kill=True)
        lab.start(0)
        restarted = verify(lab, tenant, model, "restart")
        record({"case": name, "slow_connections": 64, "healthy_writes_succeeded": successes,
                "healthy_write_errors": dict(errors), "health_max_ms": max(health_ms), "checked": checked, "restart": restarted})
    finally:
        for connection in sockets:
            connection.close()
        lab.close()


def local_backups(fixed):
    name = "local-backups-" + ("fixed" if fixed else "before")
    lab = Lab(a.root / name, a.binary if fixed else a.before_binary, replicated=False)
    lab.options.update(tenant_disk_bytes=1 << 20, memtable_bytes=64 << 10, cache_bytes=0)
    save(lab.root / "options.json", lab.options)
    model, backups = {}, []
    try:
        lab.start(0)
        tenant = seed(lab, "local-backup-limit")
        put(lab, tenant, model, 0, 0, 64 << 10)
        failure = None
        for _ in range(32):
            try:
                backups.append(lab.call(0, base(tenant) + "/backup", {})["backup"])
            except RequestError as error:
                failure = error.value
                assert error.code == "RESOURCE_EXHAUSTED", error
                break
        directory = lab.root / "node-0/backups" / tenant
        used = sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())
        if fixed:
            assert failure and used <= 1 << 20
            lab.call(0, base(tenant) + "/backup/" + backups[0], method="DELETE")
            lab.call(0, base(tenant) + "/backup", {})
            lab.call(0, base(tenant) + "/restore", {"backup": backups[-1]})
        else:
            assert failure is None and used > 1 << 20, used
        checked = verify(lab, tenant, model, "after-backup-limit")
        lab.stop(0, kill=True)
        lab.start(0)
        restarted = verify(lab, tenant, model, "restart")
        record({"case": name, "tenant_disk_quota": 1 << 20, "successful_backups": len(backups),
                "backup_bytes_at_limit": used, "limit_error": failure, "checked": checked, "restart": restarted})
    finally:
        lab.close()


def checkpoint_window(fixed):
    name = "checkpoint-window-" + ("fixed" if fixed else "before")
    lab = Lab(a.root / name, a.binary if fixed else a.before_binary)
    lab.options.update(tenant_disk_bytes=64 << 20, memtable_bytes=64 << 10, cache_bytes=0, l0_stop=128)
    save(lab.root / "options.json", lab.options)
    for node in [0, 1]:
        config = json.loads((lab.root / f"replication-{node}.json").read_text())
        config["log_retention_bytes"] = 1 << 20
        save(lab.root / f"replication-{node}.json", config)
    model = {}
    try:
        lab.start(0)
        tenant = seed(lab, "large-transfer")
        for i in range(128):
            put(lab, tenant, model, i, 0, 64 << 10)
        lab.call(0, base(tenant) + "/checkpoint", {})
        state = lab.call(0, "/v1/replication/status")["node"]
        snapshot = lab.call(0, "/v1/replication/snapshot", {"protocol": 3, "cluster": state["cluster"], "epoch": state["epoch"], "tenant": tenant})
        for i in range(48):
            put(lab, tenant, model, i, 1, 64 << 10)
            if i % 8 == 7:
                lab.call(0, base(tenant) + "/checkpoint", {})
        expired = False
        try:
            lab.call(0, "/v1/replication/snapshot/chunk", {"id": snapshot["id"], "file": 0, "offset": 0})
        except RequestError as error:
            assert error.code in ("SNAPSHOT_EXPIRED", "NOT_FOUND"), error
            expired = True
        assert expired != fixed
        checked = verify(lab, tenant, model, "after-transfer-window")
        replicas = []
        if fixed:
            lab.start(1)
            lab.catch_up()
            replicas.append(verify(lab, tenant, model, "bootstrap", 1))
            first = lab.call(1, "/v1/replication/status")
            assert first["initialization_attempts"] == 1 and first["initialization_failures"] == 0, first
            # catch_up observes applied positions before the next empty log probe
            # necessarily releases the source lease. End it explicitly so the
            # next stage exercises a real log gap rather than incremental catch-up.
            lab.call(0, "/v1/replication/snapshot/release", {"id": snapshot["id"]})
            lab.stop(1, kill=True)
            for i in range(64):
                put(lab, tenant, model, i, 2, 64 << 10)
                if i % 8 == 7:
                    lab.call(0, base(tenant) + "/checkpoint", {})
            lab.start(1)
            lab.catch_up()
            replicas.append(verify(lab, tenant, model, "reinitialization", 1))
            second = lab.call(1, "/v1/replication/status")
            assert second["reinitializations"] == 1 and second["initialization_failures"] == 0, second
            lab.stop(1, kill=True)
        lab.stop(0, kill=True)
        lab.start(0)
        restarted = verify(lab, tenant, model, "restart")
        record({"case": name, "retention_bytes": 1 << 20, "checkpoint_bytes": sum(f["size"] for f in snapshot["files"]),
                "tail_updates_before_transfer": 48, "snapshot_expired": expired, "checked": checked, "replica_stages": replicas, "restart": restarted})
    finally:
        lab.close()


for fixed in [False, True]:
    slow_bodies(fixed)
    local_backups(fixed)
    checkpoint_window(fixed)

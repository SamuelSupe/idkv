#!/usr/bin/env python3
"""Repeat the original HTTP reproductions and verify every latest ACK."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
import threading
import time
import uuid

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
results = {"before_sha256": hashlib.sha256(a.before_binary.read_bytes()).hexdigest(),
           "fixed_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(), "cases": []}


def record(value):
    results["cases"].append(value)
    save(a.root / "result.json", results)
    print(json.dumps(value), flush=True)


def prefix(tenant):
    return "/v1/tenants/" + tenant


def put(lab, tenant, model, i, generation, retries=None, size=4096, phase="fixed"):
    document = lab.document(i, generation, size)
    deadline = time.monotonic() + 5
    while True:
        try:
            receipt = lab.call(0, prefix(tenant) + "/collections/docs/documents", {"document": document}, "PUT")
            break
        except RequestError as error:
            if retries is None or error.code not in ["BUSY", "RESOURCE_EXHAUSTED"] or time.monotonic() >= deadline:
                raise
            retries[error.code] += 1
            time.sleep(.02)
    assert receipt["key"] == document["_key"]
    expected = dict(document, _rev=receipt["rev"])
    model[document["_key"]] = expected
    lab.journal.write(json.dumps({"op": "put", "tenant": tenant, "key": document["_key"],
        "generation": generation, "payload_bytes": size, "rev": receipt["rev"], "phase": phase,
        "sha256": hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()}) + "\n")
    return receipt


def verify(lab, tenant, model, node=0, stage="final", ack_count=None):
    rows = []
    for key, expected in sorted(model.items()):
        actual = lab.call(node, prefix(tenant) + "/collections/docs/documents/" + key)
        assert actual == expected, (node, key)
        rows.append({"key": key, "rev": actual["_rev"],
                     "sha256": hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()})
    name = f"verified-{stage}-node-{node}.json"
    save(lab.root / name, rows)
    if ack_count is None:
        ack_count = len((lab.root / "acknowledgements.jsonl").read_text().splitlines())
    result = {"node": node, "stage": stage, "file": name, "ack_count": ack_count,
              "documents": len(rows), "mismatches": 0,
              "sha256": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}
    records = getattr(lab, "validation_stages", [])
    records.append(result)
    lab.validation_stages = records
    save(lab.root / "verification-stages.json", records)
    lab.event("verified-stage", **result)
    return result


def job(lab, tenant, route, body=None):
    queued = lab.retry(0, prefix(tenant) + route, body or {})
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = lab.call(0, prefix(tenant) + "/backup-jobs/" + queued["id"])
        if current["state"] in ["completed", "failed", "interrupted"]:
            return current
        time.sleep(.01)
    raise AssertionError("backup worker did not finish")


def complete(lab, tenant, route, body=None):
    result = job(lab, tenant, route, body)
    assert result["state"] == "completed", result
    return result


def import_backup(lab, tenant, backup, model, node):
    lab.stop(0, kill=True)
    command = [str(a.binary.parent / "tenant-kv-restore"), str(lab.node_roots[node]),
               str(lab.root / "backup.json"), tenant, backup]
    with (lab.root / f"offline-import-node-{node}.log").open("w") as log:
        imported = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=60)
    assert imported.returncode == 0, "offline import failed; inspect its log"
    lab.start(node)
    restored = verify(lab, tenant, model, node, stage="offline-import")
    lab.start(0)
    return restored


def compaction_case(upgrade):
    lab = Lab(a.root / ("compaction-upgrade" if upgrade else "compaction-fresh"),
              a.before_binary if upgrade else a.binary, replicated=False)
    lab.options.update(global_memory_bytes=128 << 20, tenant_memory_bytes=64 << 20,
                       memtable_bytes=256 << 10, cache_bytes=0, tenant_disk_bytes=16 << 20,
                       target_sst_bytes=4 << 20, level_base_bytes=64 << 20,
                       l0_trigger=4, l0_stop=12)
    save(lab.root / "options.json", lab.options)
    stop = threading.Event()
    samples = {"peak_directory_bytes": 0, "samples": 0}
    observer = None
    retries = Counter()
    model = {}
    try:
        lab.start(0)
        tenant = lab.call(0, "/v1/tenants", {"name": "compaction-headroom"})["id"]
        lab.call(0, prefix(tenant) + "/collections", {"name": "docs"})
        database = lab.node_roots[0] / "tenants" / tenant
        def sample():
            while not stop.is_set():
                size = 0
                for path in database.rglob("*"):
                    try:
                        if path.is_file():
                            size += path.stat().st_size
                    except FileNotFoundError:
                        pass
                samples["peak_directory_bytes"] = max(samples["peak_directory_bytes"], size)
                samples["samples"] += 1
                stop.wait(.005)
        observer = threading.Thread(target=sample)
        observer.start()
        for i in range(1000):
            put(lab, tenant, model, i, 0, retries, phase="seed")
        lab.call(0, prefix(tenant) + "/checkpoint", {})
        time.sleep(.5)
        order = list(range(1000))
        random.Random(17).shuffle(order)
        before = None
        start_index = 0
        recovery_seconds = None
        upgraded_state = None
        if upgrade:
            for offset, i in enumerate(order):
                try:
                    put(lab, tenant, model, i, 1, phase="before")
                except RequestError as error:
                    assert error.code == "RESOURCE_EXHAUSTED" and "L0 backlog" in str(error), error
                    start_index = offset
                    break
            else:
                raise AssertionError("baseline did not reproduce the L0 stall")
            failures = []
            for _ in range(30):
                time.sleep(.1)
                try:
                    put(lab, tenant, model, order[start_index], 1, phase="before-retry")
                except RequestError as error:
                    failures.append(error.value)
                else:
                    raise AssertionError("baseline unexpectedly recovered")
            before = {"confirmed_updates": start_index, "status": lab.call(0, prefix(tenant)),
                      "failed_retry_count": len(failures), "errors": failures}
            before["acknowledged_state"] = verify(lab, tenant, model, stage="baseline-stalled")
            lab.stop(0, kill=True)
            lab.start(0)
            try:
                put(lab, tenant, model, order[start_index], 1, phase="before-restart")
            except RequestError as error:
                before["after_restart_error"] = error.value
            else:
                raise AssertionError("baseline restart unexpectedly fixed the stall")
            lab.stop(0, kill=True)
            lab.binary = a.binary
            lab.start(0)
            started = time.monotonic()
            upgraded_state = verify(lab, tenant, model, stage="after-upgrade-before-writes")
            put(lab, tenant, model, order[start_index], 1, retries, phase="upgrade-recovery")
            recovery_seconds = time.monotonic() - started
            start_index += 1
        for i in order[start_index:]:
            put(lab, tenant, model, i, 1, retries)
        phase_one = verify(lab, tenant, model, stage="after-generation-1")
        for i in order:
            put(lab, tenant, model, i, 2, retries)
        phase_two = verify(lab, tenant, model, stage="after-generation-2")
        # The same tenant must also support deletes after recovering its L0.
        probe = put(lab, tenant, model, 99999999, 0, retries, size=64, phase="delete-probe")
        deleted = lab.retry(0, prefix(tenant) + "/collections/docs/documents/" + probe["key"], method="DELETE")
        assert deleted["deleted"]
        model.pop(probe["key"])
        lab.journal.write(json.dumps({"op": "delete", "key": probe["key"], "rev": deleted["rev"]}) + "\n")
        lab.call(0, prefix(tenant) + "/compact", {})
        assert lab.call(0, prefix(tenant) + "/check", {})["ok"]
        first_check = verify(lab, tenant, model, stage="after-force-compact")
        lab.stop(0, kill=True)
        lab.start(0)
        after_restart = verify(lab, tenant, model)
        status = lab.call(0, prefix(tenant))
        assert status["tenant"]["disk_bytes"] == 16 << 20
        assert status["stats"]["maintenance_error"] is None, status
        stop.set()
        observer.join()
        assert samples["peak_directory_bytes"] <= 16 << 20, samples
        record({"case": "compaction-upgrade" if upgrade else "compaction-fresh", "before": before,
                "recovery_seconds_including_ack_verification": recovery_seconds,
                "upgraded_ack_state": upgraded_state, "phase_one": phase_one, "phase_two": phase_two,
                "quota_bytes": 16 << 20,
                "admission_retries": dict(retries), "sampler": samples, "before_restart": first_check,
                "after_restart": after_restart, "final_status": status})
    finally:
        stop.set()
        if observer is not None:
            observer.join()
        lab.close()


def backup_case():
    lab = Lab(a.root / "backup-upgrade", a.before_binary, replicated=False)
    config = json.loads((lab.root / "backup.json").read_text())
    config["gc_grace_seconds"] = 0
    save(lab.root / "backup.json", config)
    model = {}
    retries = Counter()
    try:
        lab.start(0)
        tenant = lab.call(0, "/v1/tenants", {"name": "backup-history"})["id"]
        lab.call(0, prefix(tenant) + "/collections", {"name": "docs"})
        for i in range(200):
            put(lab, tenant, model, i, 0, retries, size=1024, phase="backup-seed")
        first = complete(lab, tenant, "/remote-backups")
        last = complete(lab, tenant, "/remote-backups")
        complete(lab, tenant, "/remote-backups/prune", {"keep_last": 1})
        snapshots = lab.root / "repository/tenants" / tenant / "snapshots"
        assert (snapshots / first["backup"]).is_dir()
        assert not any((snapshots / first["backup"]).iterdir())
        for _ in range(4097 - len(list(snapshots.iterdir()))):
            (snapshots / str(uuid.uuid4())).mkdir()
        try:
            lab.call(0, prefix(tenant) + "/remote-backups")
        except RequestError as error:
            assert error.code == "RESOURCE_EXHAUSTED"
            old_listing_failure = error.value
        else:
            raise AssertionError("baseline listing did not reproduce the limit")
        failed = job(lab, tenant, "/remote-backups/prune", {"keep_last": 1})
        assert failed["state"] == "failed"
        old_state = verify(lab, tenant, model, stage="before-backup-upgrade")
        lab.stop(0, kill=True)
        lab.binary = a.binary
        lab.start(0)
        count_before = len(list(snapshots.iterdir()))
        inventory = lab.call(0, prefix(tenant) + "/remote-backups")
        assert len(inventory) == 1
        assert len(list(snapshots.iterdir())) == count_before == 4097
        complete(lab, tenant, "/remote-backups/prune", {"keep_last": 1})
        assert len(list(snapshots.iterdir())) == 1
        after_prune = verify(lab, tenant, model, stage="after-legacy-prune")
        complete(lab, tenant, "/remote-backups/" + last["backup"] + "/verify")
        retained_model = dict(model)
        retained_ack_count = 200
        retained_restored = import_backup(lab, tenant, last["backup"], retained_model, 1)
        completed = []
        for generation in range(1, 6):
            put(lab, tenant, model, generation, generation, retries, size=1024, phase="backup-rotation")
            last = complete(lab, tenant, "/remote-backups")
            complete(lab, tenant, "/remote-backups/prune", {"keep_last": 1})
            assert len(list(snapshots.iterdir())) == 1
            completed.append(last["backup"])
        complete(lab, tenant, "/remote-backups/" + last["backup"] + "/verify")
        live = verify(lab, tenant, model, stage="before-latest-import")
        restored = import_backup(lab, tenant, last["backup"], model, 2)
        retained_after_gc = verify(lab, tenant, retained_model, 1, stage="retained-import-after-gc", ack_count=retained_ack_count)
        restarted = verify(lab, tenant, model)
        assert len(lab.call(0, prefix(tenant) + "/remote-backups")) == 1
        record({"case": "backup-upgrade", "old_listing_failure": old_listing_failure,
                "old_prune_failure": failed, "legacy_directories": count_before,
                "fixed_listing": inventory, "directory_count_after_prune": 1,
                "subsequent_rotations": completed, "live": live, "restored": restored,
                "after_restart": restarted, "before_upgrade": old_state, "after_legacy_prune": after_prune,
                "retained_backup_restored": retained_restored, "retained_import_after_gc": retained_after_gc})
    finally:
        lab.close()


compaction_case(True)
compaction_case(False)
backup_case()

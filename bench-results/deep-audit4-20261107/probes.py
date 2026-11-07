#!/usr/bin/env python3
"""Read-only product audit through isolated native HTTP fixtures."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import shutil
import sys
import time
import uuid
import zlib

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--case", choices=["backups", "compaction"], required=True)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--documents", type=int, default=1000)
a = p.parse_args()
lab = Lab(a.root, a.binary, replicated=False)


def job(prefix, route, body=None):
    queued = lab.retry(0, prefix + route, body or {})
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = lab.call(0, prefix + "/backup-jobs/" + queued["id"])
        if current["state"] in ["completed", "failed", "interrupted"]:
            return current
        time.sleep(.01)
    raise AssertionError("backup job did not finish")


def read_error(path):
    try:
        return {"ok": True, "value": lab.call(0, path)}
    except RequestError as error:
        return {"ok": False, "status": error.status, "code": error.code, "value": error.value}


try:
    if a.case == "backups":
        lab.start(0)
        tenant = lab.call(0, "/v1/tenants", {"name": "backup-prune-history"})["id"]
        prefix = "/v1/tenants/" + tenant
        first = job(prefix, "/remote-backups")
        second = job(prefix, "/remote-backups")
        assert first["state"] == second["state"] == "completed"
        pruned = job(prefix, "/remote-backups/prune", {"keep_last": 1})
        assert pruned["state"] == "completed", pruned
        snapshots = lab.root / "repository/tenants" / tenant / "snapshots"
        directories = [x for x in snapshots.iterdir() if x.is_dir()]
        empty_after_real_prune = [x.name for x in directories if not any(x.iterdir())]
        initial_list = lab.call(0, prefix + "/remote-backups")
        # Amplify only the empty-directory residue produced by the real prune.
        for _ in range(4097 - len(directories)):
            (snapshots / str(uuid.uuid4())).mkdir()
        listing = read_error(prefix + "/remote-backups")
        blocked_prune = job(prefix, "/remote-backups/prune", {"keep_last": 1})
        assert empty_after_real_prune and not listing["ok"], (empty_after_real_prune, listing)
        assert blocked_prune["state"] == "failed", blocked_prune
        for path in snapshots.iterdir():
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        recovered_list = lab.call(0, prefix + "/remote-backups")
        verified = job(prefix, "/remote-backups/" + second["backup"] + "/verify")
        assert verified["state"] == "completed", verified
        result = {"case": a.case, "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
                  "real_create_jobs": [first, second], "real_prune_job": pruned,
                  "empty_directories_after_real_prune": empty_after_real_prune,
                  "list_before_amplification": initial_list, "directory_count_in_fixture": 4097,
                  "listing_failure": listing, "prune_failure": blocked_prune,
                  "list_after_empty_directory_cleanup": recovered_list, "retained_backup_verification": verified}
    else:
        lab.options.update(global_memory_bytes=128 << 20, tenant_memory_bytes=64 << 20,
                           memtable_bytes=256 << 10, cache_bytes=0, tenant_disk_bytes=16 << 20,
                           target_sst_bytes=4 << 20, level_base_bytes=64 << 20,
                           l0_trigger=4, l0_stop=12)
        save(lab.root / "options.json", lab.options)
        lab.start(0)
        tenant = lab.call(0, "/v1/tenants", {"name": "compaction-headroom"})["id"]
        prefix = "/v1/tenants/" + tenant
        lab.call(0, prefix + "/collections", {"name": "docs"})
        docs = prefix + "/collections/docs/documents"
        expected = {}
        def write(i, generation):
            document = lab.document(i, generation, 4096)
            receipt = lab.call(0, docs, {"document": document}, "PUT")
            expected[document["_key"]] = dict(document, _rev=receipt["rev"])
            lab.journal.write(json.dumps({"tenant": tenant, "key": document["_key"],
                "generation": generation, "payload_bytes": 4096, "rev": receipt["rev"],
                "history": receipt["history"], "sha256": hashlib.sha256(json.dumps(
                    expected[document["_key"]], sort_keys=True).encode()).hexdigest()}) + "\n")
            return receipt
        for i in range(a.documents):
            write(i, 0)
        lab.call(0, prefix + "/checkpoint", {})
        time.sleep(.5)
        seeded = lab.call(0, prefix)
        lab.event("seed_complete", status=seeded)
        order = list(range(a.documents))
        random.Random(17).shuffle(order)
        attempts = 0
        first_failure = None
        for i in order:
            try:
                write(i, 1)
                attempts += 1
            except RequestError as error:
                first_failure = {"status": error.status, "code": error.code, "value": error.value}
                break
        assert first_failure, "update pass did not stall; investigate fixture size"
        stalled = lab.call(0, prefix)
        lab.event("first_write_failure", failure=first_failure, updates=attempts, status=stalled)
        probes = []
        for _ in range(30):
            time.sleep(.1)
            try:
                write(order[attempts], 1)
                probes.append({"ok": True})
            except RequestError as error:
                probes.append({"ok": False, "code": error.code, "value": error.value})
        after_wait = lab.call(0, prefix)
        database = lab.node_roots[0] / "tenants" / tenant
        current = (database / "CURRENT").read_text()
        manifest_bytes = (database / current).read_bytes()
        assert manifest_bytes[:4] == b"TKJ1"
        assert zlib.crc32(manifest_bytes[8:]) == int.from_bytes(manifest_bytes[4:8], "little")
        manifest = json.loads(manifest_bytes[8:])
        save(lab.root / "manifest-before-quota-increase.json", manifest)
        actual_directory_bytes = sum(x.stat().st_size for x in database.rglob("*") if x.is_file())
        filesystem_free_bytes = shutil.disk_usage(database).free
        try:
            lab.call(0, docs + "/00000000", method="DELETE")
        except RequestError as error:
            delete_attempt = {"ok": False, "code": error.code, "value": error.value}
        else:
            expected.pop("00000000", None)
            delete_attempt = {"ok": True}
        lab.stop(0, kill=True)
        lab.start(0)
        try:
            write(order[attempts], 1)
        except RequestError as error:
            after_restart_attempt = {"ok": False, "code": error.code, "value": error.value}
        else:
            after_restart_attempt = {"ok": True}
        # Raise only disk headroom, leaving workload, memory and data unchanged.
        deadline = time.monotonic() + 5
        while True:
            try:
                lab.call(0, prefix, {"memory_bytes": 64 << 20, "disk_bytes": 64 << 20}, "PATCH")
                break
            except RequestError as error:
                assert error.code == "BUSY" and time.monotonic() < deadline, error
                time.sleep(.02)
        deadline = time.monotonic() + 10
        while True:
            try:
                recovered_ack = write(order[attempts], 1)
                break
            except RequestError as error:
                assert error.code == "RESOURCE_EXHAUSTED" and time.monotonic() < deadline, error
                time.sleep(.05)
        mismatches = 0
        verified_rows = []
        for key, document in expected.items():
            actual = lab.call(0, docs + "/" + key)
            mismatches += actual != document
            verified_rows.append({"key": key, "rev": actual["_rev"], "sha256": hashlib.sha256(
                json.dumps(actual, sort_keys=True).encode()).hexdigest()})
        assert mismatches == 0
        save(lab.root / "verified-latest.json", verified_rows)
        result = {"case": a.case, "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
                  "options": dict(lab.options), "seeded": seeded, "confirmed_updates_before_stall": attempts,
                  "first_failure": first_failure, "stalled": stalled, "retry_probes": probes,
                  "after_wait": after_wait, "recovery_ack_after_disk_quota_increase": recovered_ack,
                  "actual_directory_bytes_before_increase": actual_directory_bytes,
                  "filesystem_free_bytes": filesystem_free_bytes,
                  "delete_attempt": delete_attempt, "write_attempt_after_restart": after_restart_attempt,
                  "recovered": lab.call(0, prefix), "acknowledged_documents_verified": len(expected),
                  "acknowledged_mismatches": mismatches}
    save(a.root / "result.json", result)
    print(json.dumps(result), flush=True)
finally:
    lab.close()

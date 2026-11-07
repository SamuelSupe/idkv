#!/usr/bin/env python3
"""Exercise lifecycle changes while a durable private checkpoint is hashed slowly."""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save


def wait_job(lab, tenant, job):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        state = lab.call(0, f"/v1/tenants/{tenant}/backup-jobs/{job}")
        if state["state"] in ["completed", "failed", "interrupted"]:
            assert state["state"] == "completed", state
            return state
        time.sleep(.05)
    raise TimeoutError("backup job did not finish")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expect-busy", action="store_true")
    args = parser.parse_args()
    lab = Lab(args.root.resolve(), args.binary.resolve(), replicated=False)
    result = {"result": "running", "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}
    try:
        config = json.loads((lab.root / "backup.json").read_text())
        config["bytes_per_second"] = 1 << 20
        save(lab.root / "backup.json", config)
        lab.start(0)
        tenant = lab.seed("slow-backup", 1500, 4096)
        light = lab.seed("healthy", 20, 128)
        result["before"] = lab.verify(0, tenant)
        job = lab.call(0, f"/v1/tenants/{tenant}/remote-backups", {})
        work = lab.node_roots[0] / "backup-work" / job["id"]
        deadline = time.monotonic() + 10
        while not (work / "checkpoint/WAL-SYNC").exists():
            assert time.monotonic() < deadline, "checkpoint verification did not finish"
            time.sleep(.005)
        # The private engine publishes its close watermark after verification.
        # Wait for that boundary instead of estimating the verification duration.
        assert (work / "checkpoint/CURRENT").exists()
        assert not (work / "manifest.local").exists(), "missed hash phase"
        assert lab.call(0, f"/v1/tenants/{tenant}/backup-jobs/{job['id']}")["state"] == "checkpointing"
        result["management"] = []
        for operation, body, method in [("close", {}, "POST"),
                                         ("quota", {"memory_bytes":256 << 20,"disk_bytes":4 << 30}, "PATCH")]:
            started = time.perf_counter()
            path = f"/v1/tenants/{tenant}" + ("/close" if operation == "close" else "")
            try:
                lab.call(0, path, body, method)
                outcome = "ok"
            except RequestError as error:
                assert error.code == "BUSY", error
                outcome = error.code
            result["management"].append({"operation":operation,"result":outcome,
                                           "elapsed_ms":(time.perf_counter()-started)*1000})
            assert (outcome == "BUSY") == args.expect_busy
        result["during_hash"] = lab.verify(0, tenant)
        result["healthy_during_hash"] = lab.verify(0, light)
        completed = wait_job(lab, tenant, job["id"])
        verify = lab.retry(0, f"/v1/tenants/{tenant}/remote-backups/{job['backup']}/verify", {})
        wait_job(lab, tenant, verify["id"])
        result["backup_bytes"] = completed["result"]["total_bytes"]
        result["after"] = lab.verify(0, tenant)
        restore = lab.retry(0, f"/v1/tenants/{tenant}/remote-backups/{job['backup']}/restore", {})
        wait_job(lab, tenant, restore["id"])
        result["restored"] = lab.verify(0, tenant)
        result["quota_after_restore"] = lab.call(0,f"/v1/tenants/{tenant}")["tenant"]["memory_bytes"]
        assert result["quota_after_restore"] == (128 if args.expect_busy else 256) << 20
        lab.stop(0, kill=True)
        lab.start(0)
        result["after_restart"] = lab.verify(0, tenant)
        result["healthy_after_restart"] = lab.verify(0, light)
        result["result"] = "reproduced" if args.expect_busy else "passed"
    finally:
        lab.close()
        save(lab.root / "result.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

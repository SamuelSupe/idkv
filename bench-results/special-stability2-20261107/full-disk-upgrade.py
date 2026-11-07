#!/usr/bin/env python3
"""Compare an existing v4 root on the same isolated, genuinely full ext4 disk."""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from qualification import Lab
from qualification_faults import disk_volume, fill_volume
from recovery_drill import save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expect-failure", action="store_true")
    args = parser.parse_args()
    lab = Lab(args.root.resolve(), args.legacy.resolve(), replicated=False)
    volume = None
    result = {"result": "running", "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}
    try:
        volume = disk_volume(lab.root)
        lab.node_roots[0] = volume[1] / "node-0"
        lab.start(0)
        tenant = lab.seed("full-disk-upgrade", 100, 1024)
        lab.retry(0, f"/v1/tenants/{tenant}/compact", {})
        result["before"] = lab.verify(0, tenant)
        lab.stop(0)
        path = lab.node_roots[0] / "control/tenants.json"
        before = path.read_bytes()
        assert json.loads(before[8:])["version"] == 4
        filler, result["full_disk"] = fill_volume(volume[1])
        assert result["full_disk"]["available_after"] == 0
        started = time.perf_counter()
        lab.start(0)
        result["legacy_restart_ms"] = (time.perf_counter() - started) * 1000
        result["legacy_full_disk"] = lab.verify(0, tenant)
        lab.stop(0)
        lab.binary = args.binary.resolve()
        started = time.perf_counter()
        try:
            lab.start(0)
        except AssertionError:
            assert args.expect_failure, "new binary failed to start"
            result["new_full_disk"] = "startup_failed"
            log = lab.root / f"node-0-{lab.starts[0]}.log"
            result["startup_error"] = log.read_text()
            assert "os error 28" in result["startup_error"]
        else:
            assert not args.expect_failure, "expected startup failure was not reproduced"
            result["new_restart_ms"] = (time.perf_counter() - started) * 1000
            result["new_full_disk"] = lab.verify(0, tenant)
            assert path.read_bytes() == before, "read-only startup rewrote legacy metadata"
        lab.stop(0)
        assert path.read_bytes() == before
        filler.unlink()
        lab.start(0)
        result["after_freeing_space"] = lab.verify(0, tenant)
        lab.call(0, "/v1/tenants", {"name": "publish-after-upgrade"})
        assert json.loads(path.read_bytes()[8:])["version"] == 5
        result["persisted_registry"] = 5
        result["advertised_registry"] = lab.call(0, "/v1/capabilities")["storage_registry"]
        api = lab.call(0, "/openapi.json")
        result["openapi_registry"] = api["components"]["schemas"]["Capabilities"]["properties"]["storage_registry"]["const"]
        assert (result["advertised_registry"] == result["openapi_registry"] == 5) != args.expect_failure
        lab.stop(0)
        lab.start(0)
        result["after_v5_restart"] = lab.verify(0, tenant)
        result["result"] = "reproduced" if args.expect_failure else "passed"
    finally:
        lab.close()
        if volume:
            subprocess.run(["sudo", "-n", "umount", str(volume[1])], check=True)
            subprocess.run(["sudo", "-n", "losetup", "-d", volume[2]], check=True)
        save(lab.root / "result.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

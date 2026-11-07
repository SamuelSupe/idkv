#!/usr/bin/env python3
"""Verify acknowledged, unsealed WAL data under byte and inode exhaustion."""
import argparse
import errno
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from qualification import Lab, RequestError
from qualification_faults import disk_volume, fill_volume
from recovery_drill import save


def run_case(args, mode):
    lab = Lab(args.root / mode, args.binary, replicated=False)
    volume = None
    result = {"case": mode, "result": "running"}
    try:
        volume = disk_volume(lab.root)
        lab.node_roots[0] = volume[1] / "node-0"
        lab.start(0)
        tenant = lab.seed("unsealed-" + mode, 100, 1024)
        result["before"] = lab.verify(0, tenant)
        context = lab.context(tenant, False)
        lab.stop(0, kill=True)
        wal_dir = lab.node_roots[0] / "tenants" / tenant
        (wal_dir / "WAL-SYNC").unlink(missing_ok=True)
        result["wal_bytes"] = sum(p.stat().st_size for p in (wal_dir / "wal").glob("*.wal"))
        assert result["wal_bytes"] > 64
        if mode == "bytes":
            filler, result["exhaustion"] = fill_volume(volume[1])
        else:
            filler = volume[1] / "inode-filler"
            filler.mkdir()
            count = 0
            try:
                while True:
                    (filler / str(count)).touch(exist_ok=False)
                    count += 1
            except OSError as error:
                assert error.errno == errno.ENOSPC
            os.sync()
            state = os.statvfs(volume[1])
            assert state.f_favail == 0
            result["exhaustion"] = {"files": count, "available_inodes": state.f_favail,
                                     "available_bytes": state.f_bavail * state.f_frsize}
        started = time.perf_counter()
        lab.start(0)
        result["restart_ms"] = (time.perf_counter() - started) * 1000
        result["exhausted_read"] = lab.verify(0, tenant)
        assert lab.context(tenant, False)["writable"] is False
        try:
            lab.write(tenant, range(100, 101), 1, 1024, context, "after-space", "exhausted", retry=False)
        except RequestError as error:
            assert error.code == "RESOURCE_EXHAUSTED", error
            result["write_rejected"] = error.code
        else:
            raise AssertionError("read-only recovery acknowledged a write")
        if filler.is_dir():
            shutil.rmtree(filler)
        else:
            filler.unlink()
        lab.retry(0, f"/v1/tenants/{tenant}/close", {})
        assert lab.context(tenant, False)["writable"] is True
        lab.write(tenant, range(100, 101), 1, 1024, context, "after-space", "recovered", retry=False)
        result["after_recovery"] = lab.verify(0, tenant)
        lab.stop(0, kill=True)
        lab.start(0)
        result["after_second_restart"] = lab.verify(0, tenant)
        result["result"] = "passed"
    finally:
        lab.close()
        if volume:
            subprocess.run(["sudo", "-n", "umount", str(volume[1])], check=True)
            subprocess.run(["sudo", "-n", "losetup", "-d", volume[2]], check=True)
        save(lab.root / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.root = args.root.resolve()
    args.root.mkdir(mode=0o700, parents=True, exist_ok=False)
    result = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "cases": [run_case(args, mode) for mode in ["bytes", "inodes"]]}
    save(args.root / "result.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

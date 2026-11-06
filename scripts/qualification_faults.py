#!/usr/bin/env python3
"""Real ENOSPC on an isolated loop filesystem; scoped WAL write/sync EIO injection."""
import argparse
import errno
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from qualification import Lab, RequestError
from recovery_drill import digest, save


def disk_volume(root):
    image, mount = root / "disk.img", root / "volume"
    mount.mkdir()
    with image.open("wb") as stream:
        stream.truncate(256 << 20)
    subprocess.run(["mkfs.ext4", "-q", "-F", "-m", "0", str(image)], check=True)
    loop = subprocess.check_output(["sudo", "-n", "losetup", "--find", "--show", str(image)], text=True).strip()
    try:
        subprocess.run(["sudo", "-n", "mount", loop, str(mount)], check=True)
        subprocess.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(mount)], check=True)
        return image, mount, loop
    except BaseException:
        subprocess.run(["sudo", "-n", "losetup", "-d", loop], check=True)
        raise


def fill_volume(mount):
    pad = mount / "enospc-filler"
    status = os.statvfs(mount)
    available = status.f_bavail * status.f_frsize
    with pad.open("wb", buffering=0) as stream:
        os.posix_fallocate(stream.fileno(), 0, max(0, available - (1 << 20)))
        stream.seek(0, os.SEEK_END)
        try:
            while True:
                stream.write(b"\0" * 4096)
        except OSError as error:
            assert error.errno == errno.ENOSPC
        os.fsync(stream.fileno())
    after = os.statvfs(mount)
    return pad, {"available_before": available, "available_after": after.f_bavail * after.f_frsize,
                 "filler_bytes": pad.stat().st_size,
                 "mount": subprocess.check_output(["findmnt", "-T", str(mount)], text=True)}


def run_case(args, mode):
    lab = Lab(args.root / mode, args.binary, replicated=False)
    result, volume = {"case": mode, "result": "running"}, None
    control = lab.root / "io-mode"
    try:
        env = None
        if mode == "enospc":
            volume = disk_volume(lab.root)
            lab.node_roots[0] = volume[1] / "node-0"
        else:
            env = {"LD_PRELOAD": str(args.shim), "IDKV_IO_ROOT": str(lab.node_roots[0]),
                   "IDKV_IO_CONTROL": str(control), "IDKV_IO_LOG": str(lab.root / "io-fault.log")}
        lab.start(0, env)
        tenant = lab.seed("fault-" + mode, 2000, 4096)
        result["tenant"] = tenant
        result["before"] = lab.verify(0, tenant)
        context = lab.context(tenant, False)
        if mode == "enospc":
            pad, result["disk_full"] = fill_volume(volume[1])
        else:
            control.write_text("write" if mode == "wal-write-eio" else "sync")
        failed = time.perf_counter()
        try:
            lab.write(tenant, range(2000, 2008), 1, 4096, context,
                      "uncertain-write", "fault", retry=False)
            raise AssertionError("fault did not reject WAL write")
        except RequestError as error:
            result["fault_response"] = {"status": error.status, "body": error.value}
            assert error.code == "COMMIT_UNKNOWN", error
        try:
            lab.write(tenant, range(2008, 2016), 2, 4096, context,
                      "poisoned-write", "fault", retry=False)
            raise AssertionError("engine acknowledged a write after COMMIT_UNKNOWN")
        except RequestError as error:
            result["poisoned_response"] = {"status": error.status, "body": error.value}
            assert error.code in ("COMMIT_UNKNOWN", "IO_ERROR"), error
        if mode == "enospc":
            assert "os error 28" in result["fault_response"]["body"]["error"]["message"]
            pad.unlink()
        else:
            assert "EIO" in (lab.root / "io-fault.log").read_text(), "no WAL syscall was intercepted"
            assert "os error 5" in result["fault_response"]["body"]["error"]["message"]
            control.unlink()
        lab.stop(0, kill=True)
        recovery = time.perf_counter()
        lab.start(0)
        rows = lab.call(0, f"/v1/tenants/{tenant}/collections/assets/multi_get", {
            "keys": [f"{i:08}" for i in range(2000, 2008)]})
        present = sum(row is not None for row in rows)
        assert present in (0, 8), "uncertain atomic batch recovered partially"
        result["uncertain_batch_visible_before_retry"] = present
        # Same operation/history resolves an unknown result without blind duplication.
        lab.write(tenant, range(2000, 2008), 1, 4096, context,
                  "uncertain-write", "resolved-retry", retry=False)
        result["restart_to_safe_retry_ms"] = (time.perf_counter() - recovery) * 1000
        result["after"] = lab.verify(0, tenant)
        denied = lab.call(0, f"/v1/tenants/{tenant}/collections/assets/multi_get", {
            "keys": [f"{i:08}" for i in range(2008, 2016)]})
        assert all(row is None for row in denied), "poisoned engine changed data"
        result["failure_through_validation_ms"] = (time.perf_counter() - failed) * 1000
        result["confirmed_documents_preserved"] = 2000
        result["confirmed_documents_missing"] = 0
        result["result"] = "passed"
        lab.event("fault_case_passed", case=mode, result=result)
    except BaseException as error:
        result.update(result="failed", error=repr(error))
        raise
    finally:
        lab.close()
        if volume:
            subprocess.run(["sudo", "-n", "umount", str(volume[1])], check=True)
            subprocess.run(["sudo", "-n", "losetup", "-d", volume[2]], check=True)
        save(lab.root / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    if platform.system() != "Linux" or not __debug__:
        parser.error("requires Linux without -O, gcc, ext4/loop tools and passwordless sudo")
    args.root = args.root.resolve()
    args.root.mkdir(mode=0o700, parents=True, exist_ok=False)
    args.binary = args.binary.resolve(strict=True)
    args.shim = args.root / "wal_io_fault.so"
    source = Path(__file__).with_name("wal_io_fault.c")
    subprocess.run(["gcc", "-Wall", "-Wextra", "-Werror", "-shared", "-fPIC", "-O2",
                    "-o", str(args.shim), str(source)], check=True)
    save(args.root / "environment.json", {
        "binary_sha256": digest(args.binary), "script_sha256": digest(Path(__file__)),
        "helper_sha256": digest(Path(__file__).with_name("qualification.py")),
        "shim_source_sha256": digest(source), "shim_sha256": digest(args.shim),
        "platform": platform.platform(),
        "scope": "ENOSPC on isolated 256 MiB ext4 loop volume; WAL write/sync EIO via scoped LD_PRELOAD; not physical device failure"})
    cases = [run_case(args, mode) for mode in ["enospc", "wal-write-eio", "wal-sync-eio"]]
    save(args.root / "result.json", {"result": "passed", "cases": cases})


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Inject one WAL sync failure while a private checkpoint is hashed slowly."""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save


def wait_job(lab, tenant, job):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = lab.call(0,f"/v1/tenants/{tenant}/backup-jobs/{job}")
        if current["state"] in ["completed","failed","interrupted"]:
            assert current["state"] == "completed",current
            return current
        time.sleep(.02)
    raise TimeoutError("backup job")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary",type=Path,required=True)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--expect-blocked",action="store_true")
    args = parser.parse_args()
    lab = Lab(args.root.resolve(),args.binary.resolve(),replicated=False)
    control,shim = lab.root/"io-control",lab.root/"fault.so"
    result = {"result":"running","binary_sha256":hashlib.sha256(args.binary.read_bytes()).hexdigest()}
    try:
        subprocess.run(["gcc","-Wall","-Wextra","-Werror","-shared","-fPIC","-O2","-o",str(shim),
                        str(REPO/"scripts/wal_io_fault.c")],check=True)
        config = json.loads((lab.root/"backup.json").read_text())
        config["bytes_per_second"] = 1 << 20
        save(lab.root/"backup.json",config)
        lab.start(0,{"LD_PRELOAD":str(shim),"IDKV_IO_ROOT":str(lab.node_roots[0]),
                     "IDKV_IO_CONTROL":str(control),"IDKV_IO_LOG":str(lab.root/"io-fault.log")})
        tenant = lab.seed("backup-wal-fault",1500,4096)
        light = lab.seed("healthy",20,128)
        result["before"] = lab.verify(0,tenant)
        context = lab.context(tenant,False)
        job = lab.call(0,f"/v1/tenants/{tenant}/remote-backups",{})
        work = lab.node_roots[0]/"backup-work"/job["id"]
        deadline = time.monotonic()+10
        while not (work/"checkpoint/WAL-SYNC").exists():
            assert time.monotonic()<deadline
            time.sleep(.005)
        assert not (work/"manifest.local").exists()
        control.write_text("sync")
        started = time.perf_counter()
        try:
            lab.write(tenant,range(1500,1501),1,4096,context,"unknown-during-backup","injected",retry=False)
            raise AssertionError("fault did not reject the write")
        except RequestError as error:
            assert error.code == "COMMIT_UNKNOWN",error
            result["fault"] = error.value
        finally:
            control.unlink(missing_ok=True)
        assert "sync EIO" in (lab.root/"io-fault.log").read_text()
        errors = {}
        recovered = False
        deadline = time.monotonic()+2
        while time.monotonic()<deadline:
            try:
                lab.write(tenant,range(1500,1501),1,4096,context,"unknown-during-backup","resolved",retry=False)
                recovered = True
                break
            except RequestError as error:
                assert error.code in ["COMMIT_UNKNOWN","BUSY","IO_ERROR"],error
                errors[error.code] = errors.get(error.code,0)+1
                time.sleep(.02)
        result["recovered_while_hashing"] = recovered
        result["recovery_attempt_errors"] = errors
        result["hash_still_active_at_recovery_check"] = not (work/"manifest.local").exists()
        assert result["hash_still_active_at_recovery_check"]
        assert recovered != args.expect_blocked
        if recovered:
            result["safe_retry_ms"] = (time.perf_counter()-started)*1000
        result["healthy_during_fault"] = lab.verify(0,light)
        wait_job(lab,tenant,job["id"])
        if not recovered:
            lab.write(tenant,range(1500,1501),1,4096,context,"unknown-during-backup","resolved",retry=False)
            result["safe_retry_ms"] = (time.perf_counter()-started)*1000
        result["after_recovery"] = lab.verify(0,tenant)
        verify = lab.retry(0,f"/v1/tenants/{tenant}/remote-backups/{job['backup']}/verify",{})
        wait_job(lab,tenant,verify["id"])
        lab.stop(0,kill=True)
        lab.start(0)
        result["after_restart"] = lab.verify(0,tenant)
        result["healthy_after_restart"] = lab.verify(0,light)
        result["result"] = "reproduced" if args.expect_blocked else "passed"
    finally:
        control.unlink(missing_ok=True)
        lab.close()
        shim.unlink(missing_ok=True)
        save(lab.root/"result.json",result)
    print(json.dumps(result),flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Pause private checkpoint verification, then close the serving tenant."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expect-busy", action="store_true")
    args = parser.parse_args()
    lab = Lab(args.root.resolve(), args.binary.resolve(), replicated=False)
    control, marker, shim = [lab.root / name for name in ["delay-control", "delay-marker", "delay.so"]]
    result = {"result":"running","binary_sha256":hashlib.sha256(args.binary.read_bytes()).hexdigest()}
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        subprocess.run(["gcc","-Wall","-Wextra","-Werror","-shared","-fPIC","-O2","-U_FORTIFY_SOURCE","-o",str(shim),
                        str(Path(__file__).with_name("checkpoint-read-delay.c"))], check=True)
        lab.start(0, {"LD_PRELOAD":str(shim),"IDKV_DELAY_ROOT":str(lab.node_roots[0] / "backups"),
                      "IDKV_DELAY_CONTROL":str(control),"IDKV_DELAY_MARKER":str(marker)})
        tenant = lab.seed("private-verification", 100, 1024)
        light = lab.seed("healthy", 20, 128)
        result["before"] = lab.verify(0, tenant)
        result["healthy_before"] = lab.verify(0, light)
        confirmed = {}
        for id, count in [(tenant,100),(light,20)]:
            confirmed[id] = lab.call(0,f"/v1/tenants/{id}/collections/assets/multi_get",
                                     {"keys":[f"{i:08}" for i in range(count)]})
        control.touch()
        backup = pool.submit(lab.call, 0, f"/v1/tenants/{tenant}/backup", {})
        deadline = time.monotonic() + 10
        while not marker.exists():
            assert time.monotonic() < deadline and not backup.done(), "private checkpoint read was not delayed"
            time.sleep(.005)
        result["delayed_private_file"] = marker.read_text()
        result["management"] = []
        for operation, body, method in [("close",{},"POST"),
                                         ("quota",{"memory_bytes":256 << 20,"disk_bytes":4 << 30},"PATCH")]:
            started = time.perf_counter()
            path = f"/v1/tenants/{tenant}" + ("/close" if operation == "close" else "")
            try:
                lab.call(0,path,body,method)
                outcome = "ok"
            except RequestError as error:
                assert error.code == "BUSY", error
                outcome = error.code
            result["management"].append({"operation":operation,"result":outcome,
                                           "elapsed_ms":(time.perf_counter()-started)*1000})
            assert (outcome == "BUSY") == args.expect_busy
        # A local backup owns this tenant's maintenance permit. Compare ordinary
        # reads here; the full verify helper also requests a maintenance check.
        for label,id in [("during_verification",tenant),("healthy_during_verification",light)]:
            rows = lab.call(0,f"/v1/tenants/{id}/collections/assets/multi_get",
                            {"keys":[f"{i:08}" for i in range(len(confirmed[id]))]})
            assert rows == confirmed[id], "confirmed data changed during private verification"
            result[label] = {"documents":len(rows),"acknowledged_mismatches":0}
        control.unlink()
        result["backup"] = backup.result(timeout=15)
        result["after"] = lab.verify(0,tenant)
        lab.retry(0,f"/v1/tenants/{tenant}/restore",result["backup"])
        result["restored"] = lab.verify(0,tenant)
        result["quota_after_restore"] = lab.call(0,f"/v1/tenants/{tenant}")["tenant"]["memory_bytes"]
        assert result["quota_after_restore"] == (128 if args.expect_busy else 256) << 20
        lab.stop(0,kill=True)
        lab.start(0)
        result["after_restart"] = lab.verify(0,tenant)
        result["healthy_after_restart"] = lab.verify(0,light)
        result["result"] = "reproduced" if args.expect_busy else "passed"
    finally:
        control.unlink(missing_ok=True)
        pool.shutdown(wait=True)
        lab.close()
        shim.unlink(missing_ok=True)
        save(lab.root/"result.json",result)
    print(json.dumps(result),flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Assert that a durable local backup survives a later source WAL fsync failure.

Run on Linux: python3 SCRIPT BINARY NEW_OUTPUT_DIR [--expect-before-bug].
Only isolated fixture processes and WALs under NEW_OUTPUT_DIR are affected.
"""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError

binary, out = Path(sys.argv[1]), Path(sys.argv[2])
expect_before_bug = "--expect-before-bug" in sys.argv[3:]
out.mkdir(parents=True, exist_ok=False)
shim = out / "checkpoint-pause.so"
subprocess.run(["gcc", "-shared", "-fPIC", "-O2", "-Wall", "-Werror",
                str(Path(__file__).with_name("checkpoint-completion-pause.c")),
                "-o", str(shim)], check=True)
lab = Lab(out / "lab", binary, replicated=False)
fault, pause, ready = [out / name for name in ("wal-fault", "pause", "ready")]
result = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
try:
    lab.start(0, {"LD_PRELOAD": str(shim), "IDKV_IO_ROOT": str(lab.node_roots[0]),
        "IDKV_IO_CONTROL": str(fault), "IDKV_IO_LOG": str(out / "faults.log"),
        "IDKV_CHECKPOINT_PAUSE": str(pause), "IDKV_CHECKPOINT_READY": str(ready)})
    tenant = lab.seed("checkpoint-completion", 100, 1024)
    other = lab.seed("healthy-control", 10, 512)
    context = lab.context(tenant, False)
    cutoff = max(doc["_rev"] for doc in lab.model(tenant).values())
    prefix = f"/v1/tenants/{tenant}"
    parent = lab.node_roots[0] / "backups" / tenant
    pause.write_text(str(parent))
    backup = pool.submit(lab.call, 0, prefix + "/backup", {})
    deadline = time.monotonic() + 15
    while not ready.exists():
        assert time.monotonic() < deadline, "did not reach the final checkpoint fsync"
        assert not backup.done(), "backup exited before the checkpoint fsync gate"
        time.sleep(.01)
    assert len(list(parent.glob("*/CURRENT"))) == 1
    result["paused_after_durable_publication"] = True
    fault.write_text("sync")
    try:
        lab.write(tenant, [0], 1, 1024, context, "unknown", "unknown", retry=False)
        raise AssertionError("WAL fault did not fire")
    except RequestError as error:
        assert error.code == "COMMIT_UNKNOWN", error
        result["source_write_error"] = error.code
    finally:
        fault.unlink(missing_ok=True)
        pause.unlink(missing_ok=True)
    try:
        saved = backup.result(timeout=15)["backup"]
        result["backup_result"] = "success"
        assert not expect_before_bug, "baseline bug did not reproduce"
    except RequestError as error:
        result["backup_result"] = error.code
        assert expect_before_bug and error.code == "COMMIT_UNKNOWN", error
        assert not list(parent.glob("*/CURRENT")), "failed backup was not removed"
    lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=False)
    result["live_verified"] = [lab.verify(0, tenant), lab.verify(0, other)]
    lab.stop(0, kill=True)
    lab.start(0)
    result["restart_verified"] = [lab.verify(0, tenant), lab.verify(0, other)]
    if not expect_before_bug:
        lab.retry(0, prefix + "/restore", {"backup": saved})
        result["backup_restore_verified"] = lab.verify(0, tenant, cutoff)
        lab.stop(0, kill=True)
        lab.start(0)
        result["restored_restart_verified"] = lab.verify(0, tenant, cutoff)
finally:
    fault.unlink(missing_ok=True)
    pause.unlink(missing_ok=True)
    pool.shutdown(wait=True)
    lab.close()
    shim.unlink(missing_ok=True)
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

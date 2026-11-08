#!/usr/bin/env python3
"""Independent HTTP reproduction; production source is unchanged."""
import collections
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError

OUT = Path(sys.argv[1])
BINARY = Path(os.environ.get("IDKV_BINARY", "/var/tmp/idkv-stability4-bin-20261107/tenant-kv"))
OUT.mkdir(parents=True, exist_ok=False)
shim = OUT / "wal-io-fault.so"
subprocess.run(["gcc", "-shared", "-fPIC", "-O2", str(REPO / "scripts/wal_io_fault.c"), "-o", str(shim)], check=True)
lab = Lab(OUT / "lab", BINARY)
control = OUT / "fault-control"
result = {"binary_sha256": hashlib.sha256(BINARY.read_bytes()).hexdigest(), "errors": []}
without_export = "--without-export" in sys.argv[2:]
result["export_active"] = not without_export

def peer(path, body=None):
    req = urllib.request.Request(lab.urls[0] + "/v1/replication/" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + (lab.root / "peer-token").read_text(), "Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        value = json.load(response)
        if response.status >= 400:
            raise RequestError(response.status, value)
        return value

try:
    lab.start(0, {"LD_PRELOAD": str(shim), "IDKV_IO_ROOT": str(lab.node_roots[0]),
        "IDKV_IO_CONTROL": str(control), "IDKV_IO_LOG": str(OUT / "faults.log")})
    tenant = lab.seed("export-pin", 100, 1024)
    other = lab.seed("healthy-control", 10, 512)
    context = lab.context(tenant, False)
    status = peer("status")
    info = peer("snapshot", {"protocol": 3, "cluster": status["node"]["cluster"],
        "epoch": status["node"]["epoch"], "tenant": tenant})
    result.update(tenant=tenant, snapshot=info["id"], snapshot_bytes=sum(f["size"] for f in info["files"]))
    if without_export:
        peer("snapshot/release", {"id": info["id"]})
    control.write_text("sync")
    try:
        lab.write(tenant, [0], 1, 1024, context, "unknown", "unknown", retry=False)
        raise AssertionError("WAL fault did not trigger")
    except RequestError as error:
        assert error.code == "COMMIT_UNKNOWN", error
        result["injected_write_error"] = error.value
    finally:
        control.unlink(missing_ok=True)
    start = time.monotonic()
    index = next(i for i, f in enumerate(info["files"]) if f["size"] > 0)
    while time.monotonic() - start < 6:
        if not without_export:
            try:
                peer("snapshot/chunk", {"id": info["id"], "file": index, "offset": 0})
            except RequestError as error:
                assert error.code in ("SNAPSHOT_EXPIRED", "NOT_FOUND"), error
                result.setdefault("revoked_chunk_errors", []).append(error.code)
        try:
            lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=False)
            result["first_phase_write_succeeded"] = True
            break
        except RequestError as error:
            result["errors"].append({"elapsed_s": round(time.monotonic() - start, 3), "code": error.code, "message": str(error)})
        lab.call(0, "/health")
        lab.call(0, f"/v1/tenants/{other}/collections/assets/multi_get", {"keys": ["00000000"]})
        time.sleep(.05)
    result["blocked_s"] = time.monotonic() - start
    result["status_before_release"] = peer("status")
    try:
        lab.call(0, f"/v1/tenants/{tenant}/close", {})
        result["explicit_close"] = "ok"
    except RequestError as error:
        result["explicit_close"] = str(error)
    released = time.monotonic()
    peer("snapshot/release", {"id": info["id"]})
    lab.write(tenant, [0], 2, 1024, context, "post-heal", "post-heal", retry=True)
    result["recovery_after_release_ms"] = (time.monotonic() - released) * 1000
    lab.verify(0, tenant)
    lab.verify(0, other)
    result["acknowledged_data_verified"] = True
finally:
    control.unlink(missing_ok=True)
    lab.close()
    shim.unlink(missing_ok=True)
    (OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k:v for k,v in result.items() if k not in ("errors", "status_before_release")}, indent=2))
print("error_counts", dict(collections.Counter(e["code"] for e in result["errors"])))

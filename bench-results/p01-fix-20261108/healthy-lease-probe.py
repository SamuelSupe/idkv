#!/usr/bin/env python3
"""Active healthy download and catch-up pins must survive engine idle eviction."""
import hashlib
import json
import os
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab

OUT = Path(sys.argv[1])
BINARY = Path(os.environ.get("IDKV_BINARY", "/var/tmp/idkv-stability4-bin-20261107/tenant-kv"))
lab = Lab(OUT, BINARY)
options = json.loads((OUT / "options.json").read_text())
options["idle_seconds"] = 1
(OUT / "options.json").write_text(json.dumps(options))
result = {"binary_sha256": hashlib.sha256(BINARY.read_bytes()).hexdigest(), "idle_seconds": 1}
try:
    lab.start(0)
    tenant = lab.seed("healthy-pin", 100, 1024)
    state = lab.call(0, "/v1/replication/status")["node"]
    snapshot = lab.call(0, "/v1/replication/snapshot", {
        "protocol": 3, "cluster": state["cluster"], "epoch": state["epoch"], "tenant": tenant})
    time.sleep(2.2)
    assert lab.call(0, f"/v1/tenants/{tenant}")["open"]
    chunk = lab.call(0, "/v1/replication/snapshot/chunk", {"id": snapshot["id"], "file": 0, "offset": 0})
    assert chunk["data"]
    result["healthy_download_survived_idle"] = True
    context = lab.context(tenant, False)
    lab.write(tenant, [0], 1, 1024, context, "catchup", "catchup", retry=False)
    lab.call(0, "/v1/replication/log", {"protocol": 3, "cluster": state["cluster"], "epoch": state["epoch"],
        "tenant": tenant, "generation": snapshot["tenant"]["generation"], "after": snapshot["position"]})
    time.sleep(2.2)
    assert lab.call(0, f"/v1/tenants/{tenant}")["open"]
    result["healthy_catchup_survived_idle"] = True
    lab.call(0, "/v1/replication/snapshot/release", {"id": snapshot["id"]})
    deadline = time.monotonic() + 3
    while lab.call(0, f"/v1/tenants/{tenant}")["open"]:
        assert time.monotonic() < deadline, "released pin prevented idle eviction"
        time.sleep(.025)
    result["idle_eviction_after_release"] = True
    lab.verify(0, tenant)
    result["acknowledged_data_verified"] = True
finally:
    lab.close()
    (OUT / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

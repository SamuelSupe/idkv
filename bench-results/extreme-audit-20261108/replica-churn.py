#!/usr/bin/env python3
"""Repeated replica SIGKILL while the primary advances beyond its retained WAL."""
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError

binary, out = Path(sys.argv[1]), Path(sys.argv[2])
lab = Lab(out, binary)
for node in [0, 1]:
    path = out / f"replication-{node}.json"
    config = json.loads(path.read_text())
    config["log_retention_bytes"] = 1 << 20
    path.write_text(json.dumps(config))
result = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "rounds": []}
try:
    lab.start(0)
    tenant = lab.seed("replica-churn", 500, 4096)
    sentinel = lab.seed("light-control", 20, 128)
    lab.start(1)
    lab.catch_up()
    context = lab.context(tenant, False)
    control_context = lab.context(sentinel, False)
    for generation in range(1, 4):
        old = next(row for row in lab.call(1, "/v1/replication/status")["tenants"] if row["info"]["id"] == tenant)
        lab.stop(1, kill=True)
        for offset in range(0, 500, 20):
            lab.write(tenant, range(offset, offset + 20), generation, 4096, context,
                      f"round-{generation}-{offset}", "disconnected")
        lab.write(sentinel, [0], generation, 128, control_context, f"control-{generation}", "disconnected")
        lab.retry(0, f"/v1/tenants/{tenant}/checkpoint", {})
        node = lab.call(0, "/v1/replication/status")["node"]
        try:
            lab.call(0, "/v1/replication/log", {"protocol": 3, "cluster": node["cluster"],
                     "epoch": node["epoch"], "tenant": tenant, "generation": old["info"]["generation"],
                     "after": old["seq"]})
            raise AssertionError("fixture did not exceed retained WAL")
        except RequestError as error:
            assert error.code == "LOG_GONE", error
        started = time.monotonic()
        lab.start(1)
        lab.catch_up()
        elapsed = time.monotonic() - started
        verified = [lab.verify(n, t) for n in [0, 1] for t in [tenant, sentinel]]
        status = lab.call(1, "/v1/replication/status")
        result["rounds"].append({"round": generation, "old_position": old["seq"],
            "missing_wal_confirmed": True, "restart_to_caught_up_s": elapsed,
            "reinitializations": status["reinitializations"], "verified": verified})
    switched = lab.call(0, "/v1/replication/switchover", {})
    assert switched["role"] == "replica"
    result["after_switchover"] = [lab.verify(n, t) for n in [0, 1] for t in [tenant, sentinel]]
    lab.stop(0, kill=True)
    lab.stop(1, kill=True)
    lab.start(0)
    lab.start(1)
    result["after_final_restart"] = [lab.verify(n, t) for n in [0, 1] for t in [tenant, sentinel]]
    result["result"] = "passed"
finally:
    lab.close()
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

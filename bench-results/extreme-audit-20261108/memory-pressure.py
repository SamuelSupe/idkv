#!/usr/bin/env python3
"""Shared memory exhaustion below each tenant's flush threshold; HTTP only."""
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
threshold = int(sys.argv[3]) << 20
observation = float(sys.argv[4]) if len(sys.argv) > 4 else 6
lab = Lab(out, binary, replicated=False)
options = dict(lab.options, global_memory_bytes=64 << 20, tenant_memory_bytes=64 << 20,
               memtable_bytes=threshold, cache_bytes=0)
(out / "options.json").write_text(json.dumps(options))
result = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "options": options}
expected = {}
journal = (out / "confirmed.jsonl").open("w")

def put(tenant, key, size):
    doc = {"_key": key, "payload": "x" * size}
    receipt = lab.call(0, f"/v1/tenants/{tenant}/collections/docs/documents", {"document": doc}, method="PUT")
    doc["_rev"] = receipt["rev"]
    expected.setdefault(tenant, {})[key] = doc
    journal.write(json.dumps({"tenant": tenant, "key": key, "size": size, "rev": receipt["rev"]}) + "\n")

def verify_all():
    checked = 0
    for tenant, records in expected.items():
        keys = sorted(records)
        for offset in range(0, len(keys), 8):
            batch = keys[offset:offset + 8]
            actual = lab.call(0, f"/v1/tenants/{tenant}/collections/docs/multi_get", {"keys": batch})
            assert actual == [records[key] for key in batch], (tenant, offset)
            checked += len(batch)
        lab.retry(0, f"/v1/tenants/{tenant}/close", {})
    return checked

try:
    lab.start(0)
    tenants = []
    for n in range(9):
        tenant = lab.call(0, "/v1/tenants", {"name": f"memory-{n}"})["id"]
        lab.call(0, f"/v1/tenants/{tenant}/collections", {"name": "docs"})
        tenants.append(tenant)
    sentinel = tenants.pop()
    put(sentinel, "confirmed", 128)
    errors = []
    misses = 0
    for n in range(2500):
        tenant = tenants[n % len(tenants)]
        try:
            put(tenant, f"ack-{n}", 32 << 10)
            misses = 0
        except RequestError as error:
            assert error.code == "RESOURCE_EXHAUSTED", error
            errors.append({"attempt": n, "error": error.value})
            misses += 1
            if misses >= len(tenants):
                break
    result["load_errors"] = errors
    result["confirmed_after_load"] = sum(map(len, expected.values()))
    result["pressure_metrics"] = lab.call(0, "/v1/metrics")
    if errors:
        # Small ordinary commits consume the final headroom, without taking a
        # snapshot or reserving memory externally through the embedded API.
        for n in range(1000):
            try:
                put(sentinel, f"small-{n}", 128)
            except RequestError as error:
                result["sentinel_write_error"] = error.value
                break
        result["retry_errors"] = []
        start = time.monotonic()
        while time.monotonic() - start < observation:
            for tenant in tenants + [sentinel]:
                try:
                    put(tenant, "after-pressure", 128)
                    result.setdefault("retry_successes", 0)
                    result["retry_successes"] += 1
                except RequestError as error:
                    result["retry_errors"].append(error.code)
            time.sleep(.1)
        result["retry_observation_s"] = time.monotonic() - start
        result["health"] = lab.call(0, "/health")
        result["manual_flush"] = []
        for tenant in tenants:
            try:
                lab.call(0, f"/v1/tenants/{tenant}/checkpoint", {})
                result["manual_flush"].append("ok")
            except RequestError as error:
                result["manual_flush"].append(error.value)
    # Closing releases owned memtables without requiring a successful flush.
    result["close_results"] = []
    for tenant in tenants + [sentinel]:
        try:
            lab.retry(0, f"/v1/tenants/{tenant}/close", {})
            result["close_results"].append("ok")
        except RequestError as error:
            result["close_results"].append(error.value)
    result["verified_after_close"] = verify_all()
    lab.stop(0, kill=True)
    lab.start(0)
    result["verified_after_restart"] = verify_all()
    result["acknowledged_mismatches"] = 0
finally:
    journal.close()
    lab.close()
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k:v for k,v in result.items() if k not in ("pressure_metrics", "retry_errors", "load_errors")}, indent=2))

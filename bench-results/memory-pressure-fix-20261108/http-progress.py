#!/usr/bin/env python3
"""Bounded HTTP progress under shared memory pressure, with safe receipt retries."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from qualification import Lab, RequestError, summarize

binary, root = map(Path, sys.argv[1:3])
lab = Lab(root, binary, replicated=False)
lab.options.update(global_memory_bytes=64 << 20, tenant_memory_bytes=64 << 20,
                   memtable_bytes=16 << 20, cache_bytes=0)
(root / "options.json").write_text(json.dumps(lab.options))
result = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "options": lab.options}
expected, histories = {}, {}
latencies = {"heavy": [], "light": []}
errors = Counter()
journal = (root / "confirmed.jsonl").open("w", buffering=1)

def put(tenant, key, size, operation, kind):
    document = {"_key": key, "payload": "x" * size}
    request = {"document": document, "write_options": {
        "operation_id": operation, "expected_history": histories[tenant]}}
    started = time.monotonic()
    while True:
        try:
            receipt = lab.call(0, f"/v1/tenants/{tenant}/collections/docs/documents", request, method="PUT")
            break
        except RequestError as error:
            errors[error.code] += 1
            assert error.code in ("RESOURCE_EXHAUSTED", "BUSY", "RESPONSE_UNAVAILABLE"), error
            assert time.monotonic() - started < 5, (kind, operation, "write stalled", error.value)
            # Reuse the exact operation ID/history even if the write completed
            # but its response allocation failed.
            time.sleep(.02)
    latency = (time.monotonic() - started) * 1000
    latencies[kind].append(latency)
    document["_rev"] = receipt["rev"]
    expected[tenant][key] = document
    journal.write(json.dumps({"tenant": tenant, "key": key, "size": size,
                              "rev": receipt["rev"], "operation_id": operation,
                              "latency_ms": latency}) + "\n")

def verify():
    checked = 0
    for tenant, documents in expected.items():
        keys = sorted(documents)
        for offset in range(0, len(keys), 8):
            batch = keys[offset:offset + 8]
            actual = lab.retry(0, f"/v1/tenants/{tenant}/collections/docs/multi_get", {"keys": batch})
            assert actual == [documents[key] for key in batch], (tenant, offset)
            checked += len(batch)
        lab.retry(0, f"/v1/tenants/{tenant}/close", {})
    return checked

try:
    lab.start(0)
    tenants = []
    for n in range(9):
        tenant = lab.call(0, "/v1/tenants", {"name": f"pressure-{n}"})["id"]
        lab.call(0, f"/v1/tenants/{tenant}/collections", {"name": "docs"})
        histories[tenant] = lab.call(0, f"/v1/tenants/{tenant}/context")["history"]
        expected[tenant] = {}
        tenants.append(tenant)
    start = time.monotonic()
    for n in range(2500):
        put(tenants[n % 8], str(n), 32 << 10, f"heavy-{n}", "heavy")
        put(tenants[8], "latest", 128, f"light-{n}", "light")
    elapsed = time.monotonic() - start
    result["elapsed_s"] = elapsed
    result["operations"] = {kind: summarize(values, elapsed, {}) for kind, values in latencies.items()}
    result["retry_errors"] = dict(errors)
    result["maximum_write_ms"] = {kind: max(values) for kind, values in latencies.items()}
    result["metrics_after_load"] = lab.call(0, "/v1/metrics")
    assert result["metrics_after_load"]["memory_bytes"] <= lab.options["global_memory_bytes"]
    assert sum(t["stats"]["io"]["flush_bytes"] for t in result["metrics_after_load"]["tenants"]) > 0
    result["verified_online"] = verify()
    lab.stop(0, kill=True)
    lab.start(0)
    result["verified_after_restart"] = verify()
    assert result["verified_after_restart"] == 2501
    result["acknowledged_mismatches"] = 0
    result["result"] = "passed"
finally:
    journal.close()
    lab.close()
    (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k:v for k,v in result.items() if k != "metrics_after_load"}, indent=2))

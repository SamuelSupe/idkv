#!/usr/bin/env python3
"""No checkpoint/compact request: wait for ordinary maintenance to flush an ACK."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--binary", type=Path, required=True)
a = p.parse_args()
lab = Lab(a.root, a.binary, replicated=False)
lab.options.update(global_memory_bytes=64 << 20, tenant_memory_bytes=8 << 20,
                   memtable_bytes=1 << 20, cache_bytes=0)
save(lab.root / "options.json", lab.options)
try:
    lab.start(0)
    tenant = lab.call(0, "/v1/tenants", {"name": "ordinary-background-flush"})
    prefix = "/v1/tenants/" + tenant["id"]
    docs = prefix + "/collections/docs/documents"
    lab.call(0, prefix + "/collections", {"name": "docs"})
    ack = lab.call(0, docs, {"document": {"_key": "large", "payload": "x" * (700 << 10)}}, "PUT")
    started = time.monotonic()
    successful_reads = 0
    while time.monotonic() - started < 5:
        try:
            doc = lab.call(0, docs + "/large")
            assert doc["payload"] == "x" * (700 << 10) and doc["_rev"] == ack["rev"]
            successful_reads += 1
        except RequestError as error:
            assert error.code == "RESOURCE_EXHAUSTED"
            failure = error.value
            break
        time.sleep(.005)
    else:
        raise AssertionError("background maintenance never reproduced the read failure")
    status = lab.call(0, prefix)
    result = {"ack": ack, "successful_reads_before_flush": successful_reads,
              "seconds_to_failure": time.monotonic() - started,
              "failure": failure, "status": status,
              "explicit_maintenance_requests": 0,
              "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest()}
    save(a.root / "result.json", result)
    print(json.dumps(result), flush=True)
finally:
    lab.close()

#!/usr/bin/env python3
"""Independently rebuild native ACK prefixes and verify all saved read stages."""
import argparse
import base64
import hashlib
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, default=Path(__file__).resolve().parent / "native")
a = p.parse_args()
result = json.loads((a.root / "result.json").read_text())
assert len(result["cases"]) == 6
receipts = stages = reads = 0
for case in result["cases"]:
    path = a.root / case["case"]
    checks = json.loads((path / "verification-stages.json").read_text())
    needed = {row["ack_count"] for row in checks}
    operations = [json.loads(line) for line in (path / "acknowledgements.jsonl").read_text().splitlines()]
    assert needed and min(needed) > 0 and max(needed) <= len(operations)
    model, prefixes = {}, {}
    for offset, operation in enumerate(operations, 1):
        i, size = int(operation["key"]), operation["payload_bytes"]
        generation = operation["generation"]
        payload = base64.b64encode(hashlib.shake_256(str(i).encode()).digest((size * 3 + 3) // 4)).decode()[:size]
        document = {"_key": operation["key"], "ip": f"ip-{i}", "generation": generation,
                    "owner": f"team-{generation % 17}", "payload": payload, "_rev": operation["rev"]}
        assert hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest() == operation["sha256"]
        if operation["key"] in model:
            assert operation["rev"] > model[operation["key"]]["rev"]
        model[operation["key"]] = {key: operation[key] for key in ["key", "rev", "sha256"]}
        if offset in needed:
            prefixes[offset] = dict(model)
    receipts += len(operations)
    assert len({check["file"] for check in checks}) == len(checks)
    for check in checks:
        rows = json.loads((path / check["file"]).read_text())
        expected = prefixes[check["ack_count"]]
        assert len(rows) == len(expected) == check["documents"]
        assert {row["key"]: row for row in rows} == expected
        assert hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest() == check["sha256"]
        assert check["mismatches"] == 0
        stages += 1
        reads += len(rows)
    assert case["checked"]["sha256"] == case["restart"]["sha256"] or case["case"] == "checkpoint-window-fixed"
by_name = {case["case"]: case for case in result["cases"]}
assert by_name["slow-bodies-before"]["healthy_writes_succeeded"] == 0
assert by_name["slow-bodies-fixed"]["healthy_writes_succeeded"] == 128
assert by_name["local-backups-before"]["backup_bytes_at_limit"] > 1 << 20
assert by_name["local-backups-fixed"]["backup_bytes_at_limit"] <= 1 << 20
assert by_name["checkpoint-window-before"]["snapshot_expired"]
assert not by_name["checkpoint-window-fixed"]["snapshot_expired"]
assert by_name["checkpoint-window-before"]["checked"]["sha256"] == by_name["checkpoint-window-fixed"]["checked"]["sha256"]
print(json.dumps({"native_cases_passed": 6, "acknowledged_receipts_verified": receipts,
                  "verification_stages": stages, "document_reads_verified": reads, "mismatches": 0}))

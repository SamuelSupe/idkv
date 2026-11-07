#!/usr/bin/env python3
"""Rebuild ACK journal prefixes independently and check every archived read stage."""
import base64
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
result = json.loads((root / "native-result.json").read_text())
assert len(result["cases"]) == 3
receipts = documents = stages_verified = reads_verified = restored_documents = 0
for name, count in [("compaction-upgrade", 1000), ("compaction-fresh", 1000), ("backup-upgrade", 200)]:
    path = root / "native" / name
    stages = json.loads((path / "verification-stages.json").read_text())
    operations = [json.loads(line) for line in (path / "acknowledgements.jsonl").read_text().splitlines()]
    needed = {s["ack_count"] for s in stages}
    assert needed and min(needed) > 0 and max(needed) <= len(operations)
    states = {}
    latest = {}
    for offset, row in enumerate(operations, 1):
        receipts += 1
        if row["op"] == "delete":
            assert row["key"] in latest and row["rev"] > latest[row["key"]]["rev"]
            latest.pop(row["key"])
        else:
            assert row["op"] == "put"
            i = int(row["key"])
            size = row["payload_bytes"]
            payload = base64.b64encode(hashlib.shake_256(str(i).encode()).digest((size * 3 + 3) // 4)).decode()[:size]
            document = {"_key": row["key"], "ip": f"ip-{i}", "generation": row["generation"],
                        "owner": f"team-{row['generation'] % 17}", "payload": payload, "_rev": row["rev"]}
            assert hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest() == row["sha256"]
            if row["key"] in latest:
                assert row["rev"] > latest[row["key"]]["rev"]
            latest[row["key"]] = row
        if offset in needed:
            states[offset] = dict(latest)
    assert len(latest) == count
    assert len({s["file"] for s in stages}) == len(stages)
    for stage in stages:
        expected = states[stage["ack_count"]]
        rows = json.loads((path / stage["file"]).read_text())
        assert len(rows) == len(expected) == stage["documents"]
        assert {r["key"] for r in rows} == set(expected)
        assert stage["mismatches"] == 0
        assert hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest() == stage["sha256"]
        for row in rows:
            assert row["rev"] == expected[row["key"]]["rev"]
            assert row["sha256"] == expected[row["key"]]["sha256"]
        stages_verified += 1
        reads_verified += len(rows)
        if stage["stage"] == "offline-import":
            restored_documents += len(rows)
    documents += count

for case in result["cases"]:
    if case["case"].startswith("compaction"):
        assert case["quota_bytes"] == 16 << 20
        assert case["sampler"]["peak_directory_bytes"] <= 16 << 20
        assert case["before_restart"]["sha256"] == case["after_restart"]["sha256"]
        assert case["after_restart"]["mismatches"] == 0
        assert case["final_status"]["tenant"]["disk_bytes"] == 16 << 20
        assert case["phase_one"]["documents"] == case["phase_two"]["documents"] == 1000
        if case["before"]:
            assert case["before"]["failed_retry_count"] == 30
            assert case["before"]["after_restart_error"]["error"]["code"] == "RESOURCE_EXHAUSTED"
            assert case["before"]["acknowledged_state"]["sha256"] == case["upgraded_ack_state"]["sha256"]
    else:
        assert case["legacy_directories"] == 4097
        assert case["old_prune_failure"]["state"] == "failed"
        assert case["directory_count_after_prune"] == len(case["fixed_listing"]) == 1
        assert len(case["subsequent_rotations"]) == 5
        assert case["live"]["sha256"] == case["restored"]["sha256"] == case["after_restart"]["sha256"]
        assert case["before_upgrade"]["sha256"] == case["after_legacy_prune"]["sha256"]
        assert case["retained_backup_restored"]["sha256"] == case["retained_import_after_gc"]["sha256"]

print(json.dumps({"native_cases_passed": 3, "acknowledged_receipts_verified": receipts,
                  "verification_stages": stages_verified, "document_reads_verified": reads_verified,
                  "latest_documents_verified": documents, "state_mismatches": 0,
                  "offline_restored_documents_verified": restored_documents}))

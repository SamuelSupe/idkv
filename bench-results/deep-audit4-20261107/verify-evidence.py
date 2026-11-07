#!/usr/bin/env python3
"""Recheck the archived ACK model and the two audit findings without servers."""
import base64
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
read = lambda name: json.loads((root / name).read_text())
latest = {}
acks = (root / "journal-acknowledgements.jsonl").read_text().splitlines()
for line in acks:
    row = json.loads(line)
    i = int(row["key"])
    size = row["payload_bytes"]
    payload = base64.b64encode(hashlib.shake_256(str(i).encode()).digest((size * 3 + 3) // 4)).decode()[:size]
    expected = {"_key": row["key"], "ip": f"ip-{i}", "generation": row["generation"],
                "owner": f"team-{row['generation'] % 17}", "payload": payload, "_rev": row["rev"]}
    assert hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest() == row["sha256"]
    latest[row["key"]] = row
verified = read("journal-verified-latest.json")
assert len(latest) == len(verified) == 1000
for row in verified:
    assert row["sha256"] == latest[row["key"]]["sha256"]
    assert row["rev"] == latest[row["key"]]["rev"]

for name in ["compaction-result.json", "compaction-repeat-result.json", "compaction-journal-result.json"]:
    result = read(name)
    assert result["acknowledged_mismatches"] == 0
    assert all(not p["ok"] for p in result["retry_probes"])
    assert not result["delete_attempt"]["ok"] and not result["write_attempt_after_restart"]["ok"]
    old = result["actual_directory_bytes_before_increase"]
    output = result["recovered"]["stats"]["io"]["compaction_write_bytes"]
    assert old + output + 65536 < 16 << 20
    assert old < (16 << 20) * .4

for name in ["backups-result.json", "backups-repeat-result.json"]:
    result = read(name)
    assert result["real_prune_job"]["result"]["deleted_backups"] == 1
    assert result["empty_directories_after_real_prune"]
    assert len(result["list_before_amplification"]) == 1
    assert result["directory_count_in_fixture"] == 4097
    assert result["listing_failure"]["code"] == "RESOURCE_EXHAUSTED"
    assert result["prune_failure"]["state"] == "failed"
    assert len(result["list_after_empty_directory_cleanup"]) == 1
    assert result["retained_backup_verification"]["state"] == "completed"

print(json.dumps({"latest_acknowledged_documents_verified": len(verified), "ack_receipts": len(acks),
                  "state_mismatches": 0, "compaction_reproductions": 3, "backup_reproductions": 2}))

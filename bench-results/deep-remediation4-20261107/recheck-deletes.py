#!/usr/bin/env python3
"""Reopen the native compaction databases and check acknowledged deletions."""
import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from qualification import Lab, RequestError
from recovery_drill import save

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--binary", type=Path, required=True)
a = p.parse_args()
cases = json.loads((a.root / "result.json").read_text())["cases"]
results = []
for case in cases:
    if not case["case"].startswith("compaction"):
        continue
    directory = a.root / case["case"]
    lab = Lab(a.root / (case["case"] + "-deletion-recheck-final"), a.binary, replicated=False)
    lab.node_roots[0] = directory / "node-0"
    save(lab.root / "options.json", json.loads((directory / "options.json").read_text()))
    save(lab.root / "backup.json", json.loads((directory / "backup.json").read_text()))
    tenant = case["final_status"]["tenant"]["id"]
    keys = [json.loads(line)["key"] for line in (directory / "acknowledgements.jsonl").read_text().splitlines()
            if json.loads(line)["op"] == "delete"]
    assert len(keys) == 1
    try:
        lab.start(0)
        for key in keys:
            route = f"/v1/tenants/{tenant}/collections/docs/documents/{key}"
            try:
                lab.call(0, route)
            except RequestError as error:
                assert error.code == "NOT_FOUND", error
            else:
                raise AssertionError("acknowledged delete reappeared after reopening")
        assert lab.call(0, f"/v1/tenants/{tenant}/check", {})["ok"]
        results.append({"case": case["case"], "confirmed_deletes": len(keys), "reappeared": 0})
    finally:
        lab.close()
save(a.root / "deletion-verification.json", results)
print(json.dumps(results))

#!/usr/bin/env python3
"""Exercise the public HTTP contract using only Python's standard library."""
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request
import uuid

base, token_file = sys.argv[1:3]
token = pathlib.Path(token_file).read_text().strip()


def request(method, path, body=None):
    payload = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=payload, method=method,
                                 headers={"Authorization": "Bearer " + token,
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        if method == "POST" and ("/remote-backups" in path or path.endswith("/retry")):
            assert response.status == 202
        return json.load(response)


def idle_operation(method, path, body=None):
    for attempt in range(30):
        try:
            return request(method, path, body)
        except urllib.error.HTTPError as error:
            detail = json.load(error)
            if detail.get("error", {}).get("code") != "BUSY" or attempt == 29:
                raise RuntimeError(detail) from error
            time.sleep(0.1)


def remote_job(path, body=None):
    job = idle_operation("POST", path, body)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = request("GET", prefix + "/backup-jobs/" + job["id"])
        if current["state"] == "completed":
            return current
        if current["state"] in ("failed", "interrupted"):
            raise RuntimeError(current)
        time.sleep(0.05)
    raise TimeoutError(job)


tenant = request("POST", "/v1/tenants", {"name": "smoke-" + uuid.uuid4().hex})
prefix = "/v1/tenants/" + tenant["id"]
collection = prefix + "/collections/assets"
request("POST", prefix + "/collections", {"name": "assets"})
write = request("PUT", collection + "/documents", {"document": {
    "_key": "gateway", "ip": "10.0.0.1", "owner": "platform", "env": "prod", "score": 95
}, "if_rev": 0})
request("POST", collection + "/indexes", {
    "name": "ip", "fields": [{"field": "/ip"}], "unique": True, "include": ["/owner"]
})
request("POST", collection + "/lookups", {
    "name": "owner", "index": "ip", "select": ["/_key", "/owner"]
})
lookup = request("POST", collection + "/lookups/owner/execute", {
    "inputs": [{"ip": "10.0.0.1"}, {"ip": "absent"}, {}]
})
assert lookup["matches"] == [[{"_key": "gateway", "owner": "platform"}], [], []]
assert lookup["stats"]["document_reads"] == 0
rows = request("POST", collection + "/query", {
    "filter": {"field": "/ip", "op": "eq", "value": "10.0.0.1"}, "select": ["/owner"]
})
assert rows["documents"] == [{"owner": "platform"}]
backup = request("POST", prefix + "/backup")
request("PUT", collection + "/documents", {"document": {
    "_key": "gateway", "ip": "10.0.0.1", "owner": "changed"
}, "if_rev": write["rev"], "write_options": {"expected_history": write["history"]}})
idle_operation("POST", prefix + "/restore", backup)
assert request("GET", collection + "/documents/gateway")["owner"] == "platform"
request("POST", prefix + "/check")
schema = request("GET", "/openapi.json")
assert schema["openapi"] == "3.1.0"
remote = None
if "--remote-backups" in sys.argv[3:]:
    first = remote_job(prefix + "/remote-backups")
    second = remote_job(prefix + "/remote-backups")
    assert second["reused_bytes"] > 0
    root = prefix + "/remote-backups/" + second["backup"]
    verified = remote_job(root + "/verify")
    request("PUT", collection + "/documents", {"document": {
        "_key": "gateway", "ip": "10.0.0.2", "owner": "after-remote-backup"}})
    restored = remote_job(root + "/restore")
    assert request("GET", collection + "/documents/gateway")["owner"] == "platform"
    assert request("POST", collection + "/lookups/owner/execute", {"inputs": [{"ip": "10.0.0.1"}]})["matches"] == [[{"_key": "gateway", "owner": "platform"}]]
    pruned = remote_job(prefix + "/remote-backups/prune", {"keep_last": 1})
    remaining = idle_operation("GET", prefix + "/remote-backups")
    assert len(remaining) == 1 and remaining[0]["backup"] == second["backup"]
    for job in [first, second, verified, restored, pruned]:
        request("DELETE", prefix + "/backup-jobs/" + job["id"])
    assert request("GET", prefix + "/backup-jobs") == []
    assert len(request("GET", prefix + "/remote-backups")) == 1
    remote = {"first": first, "second": second, "verified": True,
              "restored_lookup": True, "prune": pruned["result"], "jobs_forgotten": True}
idle_operation("DELETE", prefix)
print(json.dumps({"result": "passed", "tenant": tenant["id"], "backup": backup["backup"],
                  "remote_backup": remote,
                  "checks": ["CRUD/CAS", "unique covering index", "lookup", "query", "backup/restore", "integrity", "delete", "OpenAPI"]}))

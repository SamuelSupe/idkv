#!/usr/bin/env python3
"""Exercise temporal identity through the public HTTP API; stdlib only."""
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request
import uuid

base, token_file = sys.argv[1:3]
token = pathlib.Path(token_file).read_text().strip()


def request(method, path, body=None, error=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Authorization": "Bearer " + token,
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            value = json.load(response)
        assert error is None, (error, value)
        return value
    except urllib.error.HTTPError as failure:
        detail = json.load(failure)
        if error is not None:
            assert detail["error"]["code"] == error, detail
            return detail
        raise RuntimeError(detail) from failure


def remote_job(path):
    job = request("POST", path)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        job = request("GET", prefix + "/backup-jobs/" + job["id"])
        if job["state"] == "completed":
            return job
        if job["state"] in ("failed", "interrupted"):
            raise RuntimeError(job)
        time.sleep(.05)
    raise TimeoutError(job)


tenant = request("POST", "/v1/tenants", {"name": "identity-" + uuid.uuid4().hex})
prefix = "/v1/tenants/" + tenant["id"]
identity = prefix + "/identity"
request("POST", identity + "/initialize", {})
entities = {}
for name in ("Alice", "Bob"):
    entities[name] = request("POST", identity + "/write", {
        "action": "create_entity", "operation_id": "create-" + name,
        "attributes": {"name": name}})["entity"]["entity_id"]
identifier = {"kind": "account", "namespace": "crm", "value": "10086"}
ambiguous = {"action":"bindings","operation_id":"invalid-auto","changes":[{
    "identifier":identifier,"expected_revision":"absent",
    "effective":{"mode":"auto","from":"2026-11-05T09:00:00Z"},
    "change":{"action":"bind","entity_id":entities["Alice"]}}]}
request("POST",identity + "/write",ambiguous,error="INVALID_ARGUMENT")
assert request("POST",identity + "/timeline",identifier) is None
first = request("POST", identity + "/write", {
    "action": "bindings", "operation_id": "first", "changes": [{
        "identifier": identifier, "expected_revision": "absent",
        "effective": {"mode": "manual", "from": "2026-11-05T09:00:00Z", "to": "2026-11-05T12:00:00Z"},
        "change": {"action": "bind", "entity_id": entities["Alice"], "reason": "initial evidence"}}]})
remote = "--remote-backups" in sys.argv[3:]
backup = remote_job(prefix + "/remote-backups") if remote else request("POST", prefix + "/backup")
if remote:
    remote_job(prefix + "/remote-backups/" + backup["backup"] + "/verify")
correction = {"action": "bindings", "operation_id": "correction", "changes": [{
    "identifier": identifier, "expected_revision": first["timelines"][0]["revision"],
    "effective": {"mode": "manual", "from": "2026-11-05T10:00:00Z", "to": "2026-11-05T11:00:00Z"},
    "change": {"action": "rebind", "entity_id": entities["Bob"], "reason": "late handover notice"}}]}
fixed = request("POST", identity + "/write", correction)
assert request("POST", identity + "/write", correction) == fixed
query = {"identifier": identifier, "valid_at": "2026-11-05T10:30:00Z"}
latest = request("POST", identity + "/resolve", query)
old = request("POST", identity + "/resolve", dict(query, known_at=first["knowledge"]["version"]))
assert latest["binding"]["entity_id"] == entities["Bob"]
assert old["binding"]["entity_id"] == entities["Alice"]
assert latest["binding"]["assurance"] == "asserted"
assert len(request("POST", identity + "/timeline", identifier)["segments"]) == 3
request("PUT", prefix + "/collections/identity_temporal/documents",
        {"document": {"_key": "head:knowledge"}}, error="MANAGED_COLLECTION")
other = request("POST", "/v1/tenants", {"name": "identity-other-" + uuid.uuid4().hex})
other_prefix = "/v1/tenants/" + other["id"] + "/identity"
request("POST", other_prefix + "/initialize", {})
request("POST", other_prefix + "/resolve", dict(query, known_at=first["knowledge"]["version"]), error="KNOWLEDGE_UNAVAILABLE")
assert request("POST", other_prefix + "/resolve", query)["binding"] is None
if remote:
    remote_job(prefix + "/remote-backups/" + backup["backup"] + "/restore")
else:
    request("POST", prefix + "/restore", backup)
assert request("POST", identity + "/resolve", query)["binding"]["entity_id"] == entities["Alice"]
request("POST", identity + "/resolve", dict(query, known_at=fixed["knowledge"]["version"]), error="KNOWLEDGE_UNAVAILABLE")
reapplied = request("POST", identity + "/write", correction)
assert reapplied["knowledge"]["version"] != fixed["knowledge"]["version"]
request("POST", prefix + "/check")
print(json.dumps({"result": "passed", "tenant": tenant["id"], "entities": entities,
                  "latest": latest, "original_knowledge": old,
                  "restored_via": "OpenDAL" if remote else "local checkpoint",
                  "checks": ["create/update", "half-open correction", "historical resolution",
                             "idempotent retry", "managed collection protection", "tenant isolation",
                             "backup verify/restore", "discarded knowledge rejection", "integrity"]}))

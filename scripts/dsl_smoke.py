#!/usr/bin/env python3
"""Run the DSL's public HTTP workflow; creates and deletes its own test tenant."""
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
    req = urllib.request.Request(base + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)

tenant = request("POST", "/v1/tenants", {"name": "dsl-smoke-" + uuid.uuid4().hex})["id"]
root = "/v1/tenants/" + tenant
request("POST", root + "/collections", {"name": "assets"})

def dsl(statement, params=None, cursor=None):
    body = {"statement": statement, "params": params or {}}
    if cursor is not None:
        body["cursor"] = cursor
    return request("POST", root + "/dsl", body)

for i in range(4):
    dsl("put assets $doc if_rev 0", {"doc": {"_key": "k" + str(i), "ip": "ip-" + str(i), "owner": "platform", "score": i}})
assert dsl('get assets "k0"')["score"] == 0
assert dsl('mget assets ["k0","absent","k0"]')[1] is None
request("POST", root + "/collections/assets/indexes", {"name": "ip", "fields": [{"field": "/ip"}], "unique": True, "include": ["/owner"]})
request("POST", root + "/collections/assets/lookups", {"name": "owners", "index": "ip", "select": ["/owner"]})
matched = dsl("lookup assets using owners with $inputs", {"inputs": [{"ip": "ip-1"}, {}]})
assert matched["matches"] == [[{"owner": "platform"}], []]
assert matched["stats"]["document_reads"] == 0
plan = dsl('explain from assets | where ip == "ip-1" | fields owner')
assert plan["covering"] and plan["indexes"] == ["ip"]
statement = "from assets | where score >= $min | sort score desc | fields _key | limit 2"
page = dsl(statement, {"min": 0})
assert page["documents"] == [{"_key": "k3"}, {"_key": "k2"}]
dsl('delete assets "k1"')
next_page = dsl(statement, {"min": 0}, page["cursor"])
assert next_page["documents"] == [{"_key": "k1"}, {"_key": "k0"}]
try:
    dsl('put assets {"_key":"bad"}; delete assets "k0"')
    raise AssertionError("multiple statements accepted")
except urllib.error.HTTPError as error:
    assert error.code == 400
    assert "location" in json.load(error)["error"]
assert dsl('get assets "bad"') is None
assert dsl('get assets "k0"') is not None
for attempt in range(30):
    try:
        request("DELETE", root)
        break
    except urllib.error.HTTPError as error:
        if json.load(error)["error"]["code"] != "BUSY" or attempt == 29:
            raise
        time.sleep(.1)
print(json.dumps({"result": "passed", "tenant": tenant, "checks": ["put/CAS", "get/mget", "covering lookup", "explain", "sort/projection", "snapshot pagination", "single-statement rejection", "delete"]}))

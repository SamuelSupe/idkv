#!/usr/bin/env python3
"""Run and verify a private one-primary/one-replica pair; no third-party modules."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--replica-binary", type=Path, help="optional second version for rolling-upgrade validation")
    parser.add_argument("--root", required=True, type=Path, help="new exclusive directory")
    parser.add_argument("--faults", action="store_true", help="requires binary built with feature failpoints")
    parser.add_argument("--identity", action="store_true", help="also verify temporal history and receipts; requires both nodes >= v0.6")
    args = parser.parse_args()
    binary = args.binary.resolve()
    binaries = [binary, (args.replica_binary or args.binary).resolve()]
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    admin, peer = secrets.token_hex(32), secrets.token_hex(32)
    secret = root / "peer-token"
    secret.write_text(peer)
    secret.chmod(0o600)
    auth = {"tokens": [
        {"sha256": hashlib.sha256(token.encode()).hexdigest(), "subject": "demo-" + role, "tenant": None, "role": role}
        for token, role in [(admin, "admin"), (peer, "replication")]
    ]}
    (root / "auth.json").write_text(json.dumps(auth))
    # Give the tight loop of indexed batches room to run between background flushes.
    (root / "options.json").write_text(json.dumps({"memtable_bytes": 4 << 20, "cache_bytes": 8 << 20, "idle_seconds": 1}))
    urls = [f"http://127.0.0.1:{port()}" for _ in range(2)]
    assert urls[0] != urls[1]
    cluster = str(uuid.uuid4())
    for index, role in enumerate(["primary", "replica"]):
        (root / f"replication-{index}.json").write_text(json.dumps({
            "cluster": cluster, "role": role, "peer_url": urls[1-index],
            "token_file": str(secret), "poll_ms": 10, "log_retention_bytes": 8388608,
        }))
    processes = [None, None]
    logs = []
    phases = []
    latencies = []

    def call(node, path, body=None, token=admin, expected=200, timeout=40):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(urls[node]+path, data=data,
            headers={"Authorization": "Bearer "+token, "Content-Type": "application/json"})
        try:
            response = urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        status = response.status
        value = json.loads(response.read())
        if status != expected:
            raise AssertionError((node, path, status, value))
        return value, response.headers

    def state(node):
        return call(node, "/v1/replication/status")[0]

    def start(node, point=None, crash_expected=False):
        env = os.environ.copy()
        env.pop("TKV_FAILPOINT", None)
        if point:
            env["TKV_FAILPOINT"] = point
        log = (root / f"node-{node}-{len(logs)}.log").open("w")
        logs.append(log)
        processes[node] = subprocess.Popen([str(binaries[node]), "serve", "--root", str(root/f"node-{node}"),
            "--auth", str(root/"auth.json"), "--config", str(root/"options.json"),
            "--replication-config", str(root/f"replication-{node}.json"),
            "--listen", urls[node].removeprefix("http://")], stdout=log, stderr=log, env=env)
        if crash_expected:
            processes[node].wait(timeout=15)
            assert processes[node].returncode != 0
            return
        deadline = time.monotonic()+15
        while time.monotonic() < deadline:
            if processes[node].poll() is not None:
                raise AssertionError(f"node {node} exited; see {root}")
            try:
                call(node, "/health")
                return
            except (OSError, urllib.error.URLError):
                time.sleep(.03)
        raise TimeoutError("server startup")

    def stop(node, kill=False):
        process = processes[node]
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGKILL if kill else signal.SIGINT)
            process.wait(timeout=15)

    def catch_up(primary, replica, timeout=30):
        deadline = time.monotonic()+timeout
        last = None
        while time.monotonic() < deadline:
            a, b = state(primary), state(replica)
            pa = {x["info"]["id"]: x["seq"] for x in a["tenants"]}
            pb = {x["info"]["id"]: x["seq"] for x in b["tenants"]}
            if pa == pb and not a["unavailable"] and not b["unavailable"]:
                return
            last = {"primary": pa, "replica": pb, "error": b["last_error"]}
            time.sleep(.02)
        raise AssertionError(last)

    def write_round(node, tenant, round_id, count=100):
        batch = {"operations": [{"op": "put", "document": {"_key": f"key-{i:04}",
            "ip": f"10.{round_id % 200}.{i//256}.{i%256}", "owner": f"round-{round_id}",
            "payload": "x"*1024}} for i in range(count)]}
        started = time.perf_counter()
        value, headers = call(node, f"/v1/tenants/{tenant}/collections/assets/batch", batch)
        latencies.append((time.perf_counter()-started)*1000)
        return value, headers

    def verify(node, tenant, round_id, count=100):
        rows, _ = call(node, f"/v1/tenants/{tenant}/collections/assets/multi_get",
            {"keys": [f"key-{i:04}" for i in range(count)]})
        assert len(rows) == count and all(r["owner"] == f"round-{round_id}" for r in rows)
        assert len({r["_rev"] for r in rows}) == 1
        matches, _ = call(node, f"/v1/tenants/{tenant}/collections/assets/lookups/owner/execute",
            {"inputs": [{"ip": f"10.{round_id % 200}.0.0"}]})
        assert matches["matches"] == [[{"owner": f"round-{round_id}"}]]

    try:
        start(0)
        tenants = []
        for name in ["replication-alpha", "replication-beta"]:
            tenant = call(0, "/v1/tenants", {"name": name})[0]["id"]
            tenants.append(tenant)
            base = f"/v1/tenants/{tenant}/collections"
            call(0, base, {"name": "assets"})
            write_round(0, tenant, 0)
            call(0, base+"/assets/indexes", {"name":"ip","fields":[{"field":"/ip"}],"unique":True,"include":["/owner"]})
            call(0, base+"/assets/lookups", {"name":"owner","index":"ip","select":["/owner"]})
        start(1)
        catch_up(0, 1)
        versions = [call(node, "/openapi.json")[0]["info"]["version"] for node in range(2)]
        for tenant in tenants:
            verify(1, tenant, 0)
        call(1, "/v1/tenants", {"name":"denied"}, expected=409)
        call(0, "/v1/tenants", {"name":"denied"}, token=peer, expected=403)
        phases.append("checkpoint initialization, isolation, indexes and replica read-only")

        if args.identity:
            identity = f"/v1/tenants/{tenants[0]}/identity"
            call(0, identity + "/initialize", {})
            entities = [call(0, identity + "/write", {
                "action": "create_entity", "operation_id": f"entity-{i}", "attributes": {"name": str(i)}
            })[0]["entity"]["entity_id"] for i in range(2)]
            identifier = {"kind": "account", "namespace": "replication-test", "value": "one"}
            change = {"identifier": identifier, "expected_revision": "absent",
                "effective": {"mode": "manual", "from": "2026-01-01T00:00:00Z"},
                "change": {"action": "bind", "entity_id": entities[0], "reason": "initial"}}
            first = call(0, identity + "/write", {"action": "bindings", "operation_id": "bind", "changes": [change]})[0]
            change["expected_revision"] = first["timelines"][0]["revision"]
            change["change"] = {"action": "rebind", "entity_id": entities[1], "reason": "correction"}
            correction = {"action": "bindings", "operation_id": "correct", "changes": [change]}
            fixed = call(0, identity + "/write", correction)[0]
            query = {"identifier": identifier, "valid_at": "2026-01-01T00:00:00Z"}
            def verify_identity(node):
                latest = call(node, identity + "/resolve", query)[0]
                old = call(node, identity + "/resolve", dict(query, known_at=first["knowledge"]["version"]))[0]
                assert latest["binding"]["entity_id"] == entities[1]
                assert latest["knowledge_version"] == fixed["knowledge"]["version"]
                assert old["binding"]["entity_id"] == entities[0]
                assert old["knowledge_version"] == first["knowledge"]["version"]
            catch_up(0, 1)
            verify_identity(1)
            phases.append("temporal current and historical knowledge replicated unchanged")

        for round_id in range(1, 21):
            for tenant in tenants:
                write_round(0, tenant, round_id)
        catch_up(0, 1)
        for tenant in tenants:
            verify(1, tenant, 20)
        phases.append("4000 document updates streamed with atomic batch and covering lookup verification")

        stop(1, kill=True)
        for tenant in tenants:
            write_round(0, tenant, 21)
        start(1)
        catch_up(0, 1)
        for tenant in tenants:
            verify(1, tenant, 21)
        phases.append("replica SIGKILL, restart and WAL resume")

        if args.faults:
            stop(1)
            for tenant in tenants:
                for round_id in [24, 25, 21]:
                    write_round(0, tenant, round_id)
            start(1, "wal_torn_payload", crash_expected=True)
            start(1)
            catch_up(0, 1)
            for tenant in tenants:
                verify(1, tenant, 21)
            phases.append("torn multi-frame replica WAL append; complete prefix recovered and final batch fetched again")

        started = time.perf_counter()
        call(0, "/v1/replication/switchover", {})
        switch_ms = (time.perf_counter()-started)*1000
        assert state(0)["node"]["role"] == "replica" and state(1)["node"]["role"] == "primary"
        call(0, "/v1/tenants", {"name":"denied"}, expected=409)
        for tenant in tenants:
            result, headers = write_round(1, tenant, 22)
            call(0, f"/v1/tenants/{tenant}/replication/wait",
                {"epoch":result[0]["history"].rsplit(":", 1)[-1],"seq":result[0]["rev"],"timeout_ms":5000})
            verify(0, tenant, 22)
        phases.append("planned A-to-B switchover and read-your-writes token")
        if args.identity:
            verify_identity(1)
            assert call(1, identity + "/write", correction)[0] == fixed
            phases.append("temporal history and original idempotent receipt survive promotion")

        stop(0, kill=True)
        stop(1, kill=True)
        start(0)
        start(1)
        assert state(0)["node"]["role"] == "replica" and state(1)["node"]["role"] == "primary"
        catch_up(1, 0)
        call(1, "/v1/replication/switchover", {})
        catch_up(0, 1)
        phases.append("both nodes restart with stale initial configs; B-to-A switch")
        if args.identity:
            verify_identity(0)
            verify_identity(1)

        if args.faults:
            stop(0)
            start(0, "replication_fenced")
            try:
                call(0, "/v1/replication/switchover", {})
                raise AssertionError("expected fence failpoint abort")
            except (OSError, urllib.error.URLError):
                pass
            processes[0].wait(timeout=10)
            assert processes[0].returncode != 0
            start(0)
            assert state(0)["node"]["role"] == "fenced"
            call(0, "/v1/tenants", {"name":"denied"}, expected=409)
            call(0, "/v1/replication/switchover", {})
            catch_up(1,0)
            call(1, "/v1/replication/switchover", {})
            phases.append("crash immediately after durable fence; restarted old primary stays fenced; retry completes")

            stop(1)
            start(1, "replication_promoted")
            try:
                call(0, "/v1/replication/switchover", {})
            except AssertionError:
                pass
            processes[1].wait(timeout=10)
            assert processes[1].returncode != 0
            assert state(0)["node"]["role"] == "fenced"
            start(1)
            assert state(1)["node"]["role"] == "primary"
            call(0, "/v1/replication/switchover", {})
            catch_up(1, 0)
            call(1, "/v1/replication/switchover", {})
            phases.append("crash after promotion publication before response; durable new role and idempotent retry")

        catch_up(0, 1)
        stop(0, kill=True)  # This test itself fences the old process before force promotion.
        source = state(1)["node"]["source"]
        call(1, "/v1/replication/force-promote",
            {"source":source,"old_primary_fenced":False,"accept_data_loss":True}, expected=409)
        call(1, "/v1/replication/force-promote",
            {"source":source,"old_primary_fenced":True,"accept_data_loss":True})
        for tenant in tenants:
            verify(1,tenant,22)
            write_round(1,tenant,23)
        phases.append("explicit emergency promotion after stopping old primary; no automatic failback")
        sorted_ms = sorted(latencies)
        result = {"result":"passed","version":versions[1],"node_versions":versions,"phases":phases,"tenants":len(tenants),
            "documents":len(tenants)*100,"batch_requests":len(latencies),
            "batch_http_p50_ms":sorted_ms[len(sorted_ms)//2],
            "batch_http_p99_ms":sorted_ms[min(len(sorted_ms)-1,int(len(sorted_ms)*.99))],
            "planned_switchover_ms":switch_ms,"final_primary":1,"faultpoints":args.faults,
            "note":"Loopback functional validation in one OS; not independent hardware failure or a throughput benchmark."}
        (root/"result.json").write_text(json.dumps(result,indent=2)+"\n")
        print(json.dumps(result,indent=2))
    finally:
        stop(0)
        stop(1)
        for log in logs:
            log.close()
        secret.unlink(missing_ok=True)
        (root/"auth.json").unlink(missing_ok=True)


if __name__ == "__main__":
    main()

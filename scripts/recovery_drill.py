#!/usr/bin/env python3
"""Measure isolated Linux process-loss, replica rebuild and checkpoint recovery."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from replication_demo import port


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run_case(args, mode):
    root = args.root / mode
    root.mkdir(mode=0o700)
    admin, peer = secrets.token_hex(32), secrets.token_hex(32)
    auth = root / "auth.json"
    secret = root / "peer-token"
    secret.write_text(peer)
    secret.chmod(0o600)
    save(auth, {"tokens": [
        {"sha256": hashlib.sha256(token.encode()).hexdigest(),
         "subject": "recovery-drill-" + role, "tenant": None, "role": role}
        for token, role in [(admin, "admin"), (peer, "replication")]
    ]})
    auth.chmod(0o600)
    save(root / "options.json", {"memtable_bytes": 2097152, "cache_bytes": 8388608})
    save(root / "backup.json", {
        "service": "fs", "options": {"root": str(root / "repository")},
        "max_backup_bytes": 512 << 20, "max_staging_bytes": 1536 << 20,
        "reserve_free_bytes": 64 << 20, "bytes_per_second": 50 << 20,
    })
    ports = [port() for _ in range(3)]
    assert len(set(ports)) == 3, "ephemeral port collision; rerun in a new directory"
    urls = [f"http://127.0.0.1:{value}" for value in ports]
    cluster = str(uuid.uuid4())
    for node, role in enumerate(["primary", "replica"]):
        save(root / f"replication-{node}.json", {
            "cluster": cluster, "role": role, "peer_url": urls[1 - node],
            "token_file": str(secret), "poll_ms": 50, "log_retention_bytes": 8388608,
        })
    processes, logs, starts, acknowledgements, revisions = {}, [], {}, [], {}
    result = {"mode": mode, "result": "running", "timings_ms": {}, "checks": [],
              "retries": {"RESOURCE_EXHAUSTED": 0, "BUSY": 0}}
    started = time.perf_counter()

    def event(name, **fields):
        record = {"event": name, "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                  "utc": datetime.now(timezone.utc).isoformat(), **fields}
        with (root / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(json.dumps({"case": mode, **record}), flush=True)

    def elapsed(name, since):
        result["timings_ms"][name] = round((time.perf_counter() - since) * 1000, 3)

    def call(node, path, body=None, expected=200, method=None):
        request = urllib.request.Request(
            urls[node] + path, data=None if body is None else json.dumps(body).encode(),
            method=method, headers={"Authorization": "Bearer " + admin,
                                    "Content-Type": "application/json"})
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            value = json.load(response)
            allowed = (expected,) if isinstance(expected, int) else expected
            assert response.status in allowed, (node, path, response.status, value)
            return value

    def state(node):
        return call(node, "/v1/replication/status")

    def retry_call(node, path, body, expected=200):
        deadline = time.monotonic() + 30
        while True:
            value = call(node, path, body, expected=(expected, 409, 429))
            if not isinstance(value, dict) or not isinstance(value.get("error"), dict):
                return value
            code = value["error"]["code"]
            assert code in result["retries"], value
            assert time.monotonic() < deadline, (path, "admission did not recover", value)
            result["retries"][code] += 1
            time.sleep(.02)

    def start(node, replicated=True):
        starts[node] = starts.get(node, 0) + 1
        log = (root / f"node-{node}-{starts[node]}.log").open("w")
        logs.append(log)
        env = os.environ.copy()
        for key in ["TKV_FAILPOINT", "TKV_ERRORPOINT"]:
            env.pop(key, None)
        command = [str(args.binary), "serve", "--root", str(root / f"node-{node}"),
                   "--auth", str(auth), "--config", str(root / "options.json"),
                   "--listen", f"127.0.0.1:{ports[node]}"]
        if replicated:
            command += ["--replication-config", str(root / f"replication-{node}.json")]
        if node == 1:
            command += ["--backup-config", str(root / "backup.json")]
        processes[node] = subprocess.Popen(command, stdout=log, stderr=log, env=env)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            assert processes[node].poll() is None, f"node {node} exited; see logs"
            try:
                call(node, "/health")
                return
            except (OSError, urllib.error.URLError):
                time.sleep(.02)
        raise TimeoutError(f"node {node} startup")

    def stop(node, kill=False):
        process = processes.get(node)
        if process is not None and process.poll() is None:
            # A paused replica must be resumed so graceful cleanup can finish.
            process.send_signal(signal.SIGCONT)
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def isolate(node, archive):
        assert processes[node].poll() is not None, "old process still exists"
        with socket.socket() as sock:
            sock.settimeout(1)
            assert sock.connect_ex(("127.0.0.1", ports[node])) != 0, "old endpoint still accepts connections"
        (root / f"node-{node}").rename(root / archive)
        event("isolated", node=node, pid=processes[node].pid,
              returncode=processes[node].returncode, preserved_directory=archive,
              endpoint_closed=True, automatic_restart_supervisor=False)

    def catch_up():
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            a, b = state(1), state(0)
            left = {item["info"]["id"]: item["seq"] for item in a["tenants"]}
            right = {item["info"]["id"]: item["seq"] for item in b["tenants"]}
            if left == right and not a["unavailable"] and not b["unavailable"]:
                return
            time.sleep(.02)
        raise TimeoutError("rebuilt replica catch-up")

    def documents(phase, count):
        return [{"_key": f"{phase}-{i:06}", "ip": f"{phase}/{i:06}",
                 "owner": f"owner-{i % 17}", "phase": phase,
                 "payload": base64.b64encode(hashlib.shake_256(f"{phase}/{i}".encode()).digest(
                     (args.payload_bytes * 3 + 3) // 4)).decode()[:args.payload_bytes]}
                for i in range(count)]

    def write(node, phase, count):
        docs = documents(phase, count)
        history = call(node, prefix + "/context")["history"]
        for offset in range(0, len(docs), 100):
            batch = docs[offset:offset + 100]
            operation = f"{phase}-{offset}"
            before = time.perf_counter()
            body = {
                "operations": [{"op": "put", "document": doc, "if_rev": 0} for doc in batch],
                "write_options": {"operation_id": operation, "expected_history": history},
            }
            receipts = retry_call(node, collection + "/batch", body)
            assert len(receipts) == len(batch)
            for doc, receipt in zip(batch, receipts):
                revisions[doc["_key"]] = receipt["rev"]
            acknowledged = time.perf_counter()
            entry = {"phase": phase, "operation_id": operation,
                     "keys": [doc["_key"] for doc in batch], "receipts": receipts,
                     "ack_elapsed_ms": (acknowledged - started) * 1000,
                     "request_ms": (acknowledged - before) * 1000}
            acknowledgements.append(entry)
            with (root / "acknowledgements.jsonl").open("a") as stream:
                stream.write(json.dumps(entry) + "\n")
        return {doc["_key"]: doc for doc in docs}

    def verify(node, expected, absent=()):
        keys = sorted(set(expected) | set(absent))
        for offset in range(0, len(keys), 500):
            batch = keys[offset:offset + 500]
            rows = call(node, collection + "/multi_get", {"keys": batch})
            assert len(rows) == len(batch)
            for key, row in zip(batch, rows):
                if row is not None:
                    assert row.pop("_rev") == revisions[key], "acknowledged revision changed"
                assert row == expected.get(key), (node, key, "document mismatch")
            lookups = call(node, collection + "/lookups/owner/execute", {
                "inputs": [{"ip": key.replace("-", "/", 1)} for key in batch]})
            wanted = [[{field: expected[key][field] for field in ["_key", "owner", "phase"]}]
                      if key in expected else [] for key in batch]
            assert lookups["matches"] == wanted, "covering lookup mismatch"
            assert lookups["stats"]["document_reads"] == 0
        all_rows, cursor = {}, None
        while True:
            query = {"limit": 500}
            if cursor:
                query["cursor"] = cursor
            page = call(node, collection + "/query", query)
            for row in page["documents"]:
                assert row.pop("_rev") == revisions[row["_key"]], "scan revision mismatch"
                assert row["_key"] not in all_rows, "duplicate pagination row"
                all_rows[row["_key"]] = row
            cursor = page.get("cursor")
            if not cursor:
                break
        assert all_rows == expected, "full scan has missing or unexpected documents"
        call(node, prefix + "/check", {})
        return {"documents": len(expected), "absent_keys": len(absent),
                "sha256": hashlib.sha256(json.dumps(all_rows, sort_keys=True).encode()).hexdigest()}

    def probe(node, name):
        call(node, "/ready/write")
        row = call(node, collection + "/documents/base-000000")
        assert row["phase"] == "base"
        document = {"_key": name, "value": "read-write recovery probe"}
        receipt = call(node, prefix + "/collections/probes/documents",
                       {"document": document, "if_rev": 0}, method="PUT")
        read = call(node, prefix + "/collections/probes/documents/" + name)
        assert read["_rev"] == receipt["rev"] and read["value"] == document["value"]

    def backup_job(path):
        deadline = time.monotonic() + 120
        job = retry_call(1, path, {}, expected=202)
        while time.monotonic() < deadline:
            current = call(1, prefix + "/backup-jobs/" + job["id"])
            if current["state"] == "completed":
                return current
            assert current["state"] not in ["failed", "interrupted"], current
            time.sleep(.02)
        raise TimeoutError("backup job")

    try:
        event("started")
        start(0)
        tenant = call(0, "/v1/tenants", {"name": "recovery-" + mode})["id"]
        result["tenant"] = tenant
        prefix = f"/v1/tenants/{tenant}"
        collection = prefix + "/collections/assets"
        for name in ["assets", "probes"]:
            call(0, prefix + "/collections", {"name": name})
        base = write(0, "base", args.documents)
        retry_call(0, collection + "/indexes", {
            "name": "ip", "fields": [{"field": "/ip"}], "unique": True,
            "include": ["/owner", "/phase"],
        })
        retry_call(0, collection + "/lookups", {
            "name": "owner", "index": "ip", "select": ["/_key", "/owner", "/phase"],
        })
        start(1)
        # Initially node 0 is primary, so use the explicit tenant wait contract.
        initial = state(0)
        seq = next(item["seq"] for item in initial["tenants"] if item["info"]["id"] == tenant)
        deadline = time.monotonic() + 60
        while True:
            candidate = state(1)
            if any(item["info"]["id"] == tenant and item["seq"] >= seq for item in candidate["tenants"]):
                break
            assert time.monotonic() < deadline, "initial replication timed out"
            time.sleep(.02)
        verify(1, base)
        result["version"] = call(0, "/v1/capabilities")["version"]
        if mode == "lagged":
            processes[1].send_signal(signal.SIGSTOP)
            deadline = time.monotonic() + 5
            while "\nState:\tT" not in Path(f"/proc/{processes[1].pid}/status").read_text():
                assert time.monotonic() < deadline, "replica did not stop"
                time.sleep(.01)
            event("replica_paused", applied_seq=seq)
        tail = write(0, "tail", args.tail_documents)
        primary_state = state(0)
        primary_seq = next(item["seq"] for item in primary_state["tenants"] if item["info"]["id"] == tenant)
        if mode == "caught-up":
            call(1, prefix + "/replication/wait", {
                "epoch": primary_state["node"]["epoch"], "seq": primary_seq, "timeout_ms": 5000,
            })
            candidate = state(1)
        replica_seq = next(item["seq"] for item in candidate["tenants"] if item["info"]["id"] == tenant)
        save(root / "before-failure.json", {"primary": primary_state, "replica": candidate})
        failure = time.perf_counter()
        event("primary_loss_injected", last_primary_seq=primary_seq, last_replica_seq=replica_seq)
        stop(0, kill=True)
        fence_started = time.perf_counter()
        isolate(0, "old-primary-quarantined")
        elapsed("external_isolation", fence_started)
        if mode == "lagged":
            processes[1].send_signal(signal.SIGCONT)
        source = candidate["node"]["source"]
        before_promote = state(1)
        assert before_promote["node"]["role"] == "replica"
        for fenced, accepted in [(False, True), (True, False)]:
            call(1, "/v1/replication/force-promote", {
                "source": source, "old_primary_fenced": fenced, "accept_data_loss": accepted,
            }, expected=409)
            assert state(1)["node"]["role"] == "replica"
        promotion = time.perf_counter()
        call(1, "/v1/replication/force-promote", {
            "source": source, "old_primary_fenced": True, "accept_data_loss": True,
        })
        elapsed("force_promote_request", promotion)
        assert state(1)["node"]["role"] == "primary"
        probe(1, "failover")
        elapsed("failure_to_read_write_ready", failure)
        retained = base | (tail if mode == "caught-up" else {})
        missing = {} if mode == "caught-up" else tail
        result["failover_validation"] = verify(1, retained, missing)
        lost_batches = [item for item in acknowledgements if item["phase"] == "tail"] if missing else []
        result["failover_loss"] = {
            "acknowledged_documents": len(base) + len(tail), "missing_documents": len(missing),
            "missing_batch_requests": len(lost_batches), "last_primary_seq": primary_seq,
            "missing_payload_bytes": sum(len(doc["payload"].encode()) for doc in missing.values()),
            "last_replica_seq": replica_seq, "sequence_gap": primary_seq - replica_seq,
            "missing_keys": sorted(missing),
            "lost_ack_window_ms": round((failure - started) * 1000 - lost_batches[0]["ack_elapsed_ms"], 3) if missing else 0,
        }
        event("failover_validated", missing_documents=len(missing), timings_ms=result["timings_ms"])

        # Rebuild only from a new directory. The divergent original root stays offline.
        save(root / "replication-0.json", {
            "cluster": cluster, "role": "replica", "peer_url": urls[1],
            "token_file": str(secret), "poll_ms": 50, "log_retention_bytes": 8388608,
        })
        rebuild = time.perf_counter()
        start(0)
        catch_up()
        call(0, "/ready")
        elapsed("replica_rebuild_to_caught_up", rebuild)
        result["rebuild_validation"] = verify(0, retained, missing)
        call(0, prefix + "/collections/probes/documents", {
            "document": {"_key": "replica-must-reject"}}, expected=409, method="PUT")
        after_rebuild = write(1, "rebuilt", 100)
        retained |= after_rebuild
        catch_up()
        verify(0, retained, missing)
        save(root / "after-rebuild.json", {"primary": state(1), "replica": state(0)})
        event("old_node_rebuilt", documents=len(retained), rebuild_ms=result["timings_ms"]["replica_rebuild_to_caught_up"])

        backup_started = time.perf_counter()
        backup = backup_job(prefix + "/remote-backups")
        elapsed("backup_create", backup_started)
        verified = backup_job(prefix + "/remote-backups/" + backup["backup"] + "/verify")
        result["backup"] = {"create": backup, "verify": verified}
        backup_docs = retained.copy()
        later = write(1, "afterbackup", args.tail_documents)
        catch_up()
        verify(0, retained | later, missing)
        stop(1, kill=True)
        stop(0, kill=True)
        recovery = time.perf_counter()
        disaster_ms = time.time() * 1000
        event("both_online_copies_unavailable", backup=backup["backup"])
        isolate(1, "failed-new-primary")
        isolate(0, "failed-rebuilt-replica")
        imported = subprocess.run([
            str(args.restore_binary), str(root / "node-2"), str(root / "backup.json"),
            tenant, backup["backup"],
        ], capture_output=True, text=True, timeout=120)
        (root / "restore.log").write_text(imported.stdout + imported.stderr)
        assert imported.returncode == 0, "offline import failed; see restore.log"
        elapsed("backup_import", recovery)
        start(2, replicated=False)
        probe(2, "restore")
        elapsed("backup_recovery_to_read_write_ready", recovery)
        result["restore_validation"] = verify(2, backup_docs, missing | later)
        elapsed("backup_recovery_through_full_validation", recovery)
        result["backup_loss"] = {
            "acknowledged_documents_before_disaster": len(retained) + len(later),
            "checkpoint_documents": len(backup_docs), "missing_documents": len(later),
            "missing_batch_requests": sum(item["phase"] == "afterbackup" for item in acknowledgements),
            "missing_payload_bytes": sum(len(doc["payload"].encode()) for doc in later.values()),
            "missing_keys": sorted(later),
            "checkpoint_age_at_disaster_ms": round(disaster_ms - backup["result"]["checkpoint_ms"], 3),
        }
        stop(2)
        start(2, replicated=False)
        verify(2, backup_docs, missing | later)
        result["checks"] = [
            "SIGKILL primary, wait for exit, closed endpoint and quarantined original directory",
            "force promotion requires both fencing and data-loss declarations",
            "read/write recovery and full acknowledged-key comparison",
            "fresh-directory replica rebuild and read-only enforcement",
            "post-rebuild writes reach the replica",
            "completed checkpoint, full backup verify and offline import into a fresh Store",
            "Get/MultiGet, complete paginated scan, covering Lookup and integrity check",
            "restored service restart preserves checkpoint data and absent later writes",
        ]
        result["result"] = "passed"
        event("completed", failover_loss=len(missing), backup_loss=len(later))
    except BaseException as error:
        result.update(result="failed", error=str(error))
        raise
    finally:
        for node in list(processes):
            stop(node)
        for log in logs:
            log.close()
        auth.unlink(missing_ok=True)
        secret.unlink(missing_ok=True)
        save(root / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--restore-binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True, help="new exclusive Linux disk directory")
    parser.add_argument("--documents", type=int, default=20000)
    parser.add_argument("--tail-documents", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    args = parser.parse_args()
    if sys.platform != "linux" or not __debug__:
        parser.error("run on Linux without Python -O; isolation checks require /proc")
    if args.documents < 1 or args.tail_documents < 1 or not 1 <= args.payload_bytes <= 65536:
        parser.error("document counts must be positive; payload must contain 1..65536 bytes")
    args.root = args.root.resolve()
    args.binary = args.binary.resolve(strict=True)
    args.restore_binary = args.restore_binary.resolve(strict=True)
    args.root.mkdir(parents=True, mode=0o700, exist_ok=False)
    environment = {"utc": datetime.now(timezone.utc).isoformat(),
                   "platform": platform.platform(), "python": platform.python_version(),
                   "binary_sha256": digest(args.binary), "restore_binary_sha256": digest(args.restore_binary),
                   "script_sha256": digest(Path(__file__)), "root": str(args.root),
                   "documents": args.documents, "tail_documents": args.tail_documents,
                   "payload_bytes": args.payload_bytes,
                   "filesystem": subprocess.check_output(["df", "-T", str(args.root)], text=True),
                   "scope": "one Linux VM, separate loopback processes, fs backup on same virtual disk; no host/power/disk/cloud-S3 failure"}
    save(args.root / "environment.json", environment)
    results = [run_case(args, mode) for mode in ["caught-up", "lagged"]]
    save(args.root / "result.json", {"result": "passed", "environment": environment, "cases": results})
    print(json.dumps({"result": "passed", "root": str(args.root), "cases": [
        {"mode": case["mode"], "timings_ms": case["timings_ms"],
         "failover_missing_documents": case["failover_loss"]["missing_documents"],
         "backup_missing_documents": case["backup_loss"]["missing_documents"]} for case in results]}, indent=2))


if __name__ == "__main__":
    main()

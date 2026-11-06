#!/usr/bin/env python3
"""Bounded Linux qualification: acknowledged-write model, background overlap and crash recovery."""
import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

from recovery_drill import digest, port, save


class RequestError(RuntimeError):
    def __init__(self, status, value):
        self.status, self.value = status, value
        self.code = value.get("error", {}).get("code", "HTTP_ERROR")
        super().__init__(f"HTTP {status}: {value}")


class Lab:
    """Own only the processes, credentials and acknowledgement journal in a new directory."""
    def __init__(self, root, binary, replicated=True):
        self.root, self.binary, self.replicated = root, binary, replicated
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.token, peer = secrets.token_hex(32), secrets.token_hex(32)
        save(root / "auth.json", {"tokens": [
            {"sha256": hashlib.sha256(token.encode()).hexdigest(), "subject": "qualification-" + role,
             "tenant": None, "role": role}
            for token, role in [(self.token, "admin"), (peer, "replication")]
        ]})
        (root / "auth.json").chmod(0o600)
        (root / "peer-token").write_text(peer)
        (root / "peer-token").chmod(0o600)
        self.options = {"global_memory_bytes": 512 << 20, "tenant_memory_bytes": 128 << 20,
                        "memtable_bytes": 2 << 20, "cache_bytes": 8 << 20,
                        "target_sst_bytes": 4 << 20, "level_base_bytes": 16 << 20,
                        "l0_stop": 24, "tenant_disk_bytes": 4 << 30}
        save(root / "options.json", self.options)
        self.ports = [port() for _ in range(3)]
        assert len(set(self.ports)) == 3
        self.urls = [f"http://127.0.0.1:{p}" for p in self.ports]
        cluster = str(uuid.uuid4())
        for node, role in enumerate(["primary", "replica"]):
            save(root / f"replication-{node}.json", {
                "cluster": cluster, "role": role, "peer_url": self.urls[1 - node],
                "token_file": str(root / "peer-token"), "poll_ms": 50,
                "log_retention_bytes": 256 << 20,
            })
        save(root / "backup.json", {
            "service": "fs", "options": {"root": str(root / "repository")},
            "max_backup_bytes": 512 << 20, "max_staging_bytes": 2 << 30,
            "reserve_free_bytes": 64 << 20, "bytes_per_second": 16 << 20,
        })
        self.node_roots = [root / f"node-{i}" for i in range(3)]
        self.processes, self.logs, self.starts = {}, [], Counter()
        self.journal = (root / "acknowledgements.jsonl").open("a", buffering=1)
        self.journal_lock = threading.Lock()
        self.payloads, self.payload_lock = {}, threading.Lock()
        self.retry_counts = Counter()

    def call(self, node, path, body=None, method=None):
        request = urllib.request.Request(
            self.urls[node] + path, data=None if body is None else json.dumps(body).encode(),
            method=method, headers={"Authorization": "Bearer " + self.token,
                                    "Content-Type": "application/json"})
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            value = json.load(response)
            if response.status >= 400:
                raise RequestError(response.status, value)
            return value

    def retry(self, node, path, body=None, method=None):
        deadline = time.monotonic() + 30
        while True:
            try:
                return self.call(node, path, body, method)
            except RequestError as error:
                if error.code not in ("RESOURCE_EXHAUSTED", "BUSY") or time.monotonic() >= deadline:
                    raise
                self.retry_counts[error.code] += 1
                time.sleep(.02)

    def start(self, node, env_extra=None):
        self.starts[node] += 1
        log = (self.root / f"node-{node}-{self.starts[node]}.log").open("w")
        self.logs.append(log)
        env = os.environ.copy()
        for name in ["TKV_FAILPOINT", "TKV_ERRORPOINT", "LD_PRELOAD"]:
            env.pop(name, None)
        env.update(env_extra or {})
        command = [str(self.binary), "serve", "--root", str(self.node_roots[node]),
                   "--auth", str(self.root / "auth.json"), "--config", str(self.root / "options.json"),
                   "--listen", f"127.0.0.1:{self.ports[node]}"]
        if self.replicated and node < 2:
            command += ["--replication-config", str(self.root / f"replication-{node}.json")]
        if node == 0:
            command += ["--backup-config", str(self.root / "backup.json")]
        self.processes[node] = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            assert self.processes[node].poll() is None, f"node {node} exited"
            try:
                self.call(node, "/health")
                return
            except (OSError, urllib.error.URLError):
                time.sleep(.025)
        raise TimeoutError("server startup")

    def stop(self, node, kill=False):
        p = self.processes.get(node)
        if p is not None and p.poll() is None:
            p.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)

    def close(self):
        for node in list(self.processes):
            self.stop(node)
        self.journal.close()
        for log in self.logs:
            log.close()
        for name in ["auth.json", "peer-token"]:
            (self.root / name).unlink(missing_ok=True)

    def event(self, name, **fields):
        value = {"event": name, "utc": datetime.now(timezone.utc).isoformat(), **fields}
        with (self.root / "events.jsonl").open("a") as out:
            out.write(json.dumps(value) + "\n")
        print(json.dumps(value), flush=True)

    def document(self, i, generation, payload_bytes):
        key = (i, payload_bytes)
        with self.payload_lock:
            if key not in self.payloads:
                self.payloads[key] = base64.b64encode(hashlib.shake_256(str(i).encode()).digest(
                    (payload_bytes * 3 + 3) // 4)).decode()[:payload_bytes]
            payload = self.payloads[key]
        return {"_key": f"{i:08}", "ip": f"ip-{i}", "generation": generation,
                "owner": f"team-{generation % 17}", "payload": payload}

    def context(self, tenant, retention):
        prefix = f"/v1/tenants/{tenant}"
        return self.retry(0, prefix + ("/retry-context" if retention else "/context"),
                          {} if retention else None)

    def write(self, tenant, indices, generation, size, context, operation, phase, retry=True):
        docs = [self.document(i, generation, size) for i in indices]
        options = {"operation_id": operation, "expected_history": context["history"]}
        if "retry_token" in context:
            options["retry_token"] = context["retry_token"]
        body = {"operations": [{"op": "put", "document": doc} for doc in docs],
                "write_options": options}
        started = time.perf_counter()
        receipts = (self.retry if retry else self.call)(
            0, f"/v1/tenants/{tenant}/collections/assets/batch", body)
        latency = (time.perf_counter() - started) * 1000
        assert len(receipts) == len(indices)
        rev = receipts[0]["rev"]
        assert all(r["rev"] == rev and r["key"] == doc["_key"] for r, doc in zip(receipts, docs))
        entry = {"tenant": tenant, "indices": list(indices), "generation": generation,
                 "payload_bytes": size, "rev": rev, "history": receipts[0]["history"],
                 "operation_id": operation, "phase": phase, "latency_ms": latency,
                 "ack_utc": datetime.now(timezone.utc).isoformat()}
        with self.journal_lock:
            self.journal.write(json.dumps(entry) + "\n")
        return latency

    def seed(self, name, count, size):
        tenant = self.call(0, "/v1/tenants", {"name": name})["id"]
        prefix = f"/v1/tenants/{tenant}"
        for name in ["assets", "probes"]:
            self.call(0, prefix + "/collections", {"name": name})
        context = self.context(tenant, False)
        for offset in range(0, count, 100):
            self.write(tenant, range(offset, min(count, offset + 100)), 0, size, context,
                       f"seed-{offset}", "seed")
        self.retry(0, prefix + "/collections/assets/indexes", {
            "name": "ip", "fields": [{"field": "/ip"}], "unique": True,
            "include": ["/owner", "/generation"],
        })
        self.retry(0, prefix + "/collections/assets/lookups", {
            "name": "owner", "index": "ip", "select": ["/_key", "/owner", "/generation"],
        })
        return tenant

    def model(self, tenant, cutoff=None):
        self.journal.flush()
        expected = {}
        with (self.root / "acknowledgements.jsonl").open() as stream:
            for line in stream:
                row = json.loads(line)
                if row["tenant"] == tenant and (cutoff is None or row["rev"] <= cutoff):
                    for i in row["indices"]:
                        doc = self.document(i, row["generation"], row["payload_bytes"])
                        expected[doc["_key"]] = dict(doc, _rev=row["rev"])
        return expected

    def verify(self, node, tenant, cutoff=None):
        expected = self.model(tenant, cutoff)
        prefix = f"/v1/tenants/{tenant}"
        collection = prefix + "/collections/assets"
        cursor, observed = None, {}
        while True:
            query = {"limit": 500}
            if cursor:
                query["cursor"] = cursor
            page = self.retry(node, collection + "/query", query)
            for doc in page["documents"]:
                assert doc == expected.get(doc["_key"]), (node, tenant, doc["_key"], "acknowledgement mismatch")
                assert doc["_key"] not in observed
                observed[doc["_key"]] = doc
            cursor = page.get("cursor")
            if not cursor:
                break
        assert observed == expected, (node, tenant, "missing or unexpected document")
        keys = sorted(expected)
        for offset in range(0, len(keys), 500):
            batch = keys[offset:offset + 500]
            rows = self.retry(node, collection + "/multi_get", {"keys": batch})
            assert rows == [expected[key] for key in batch]
            output = self.retry(node, collection + "/lookups/owner/execute", {
                "inputs": [{"ip": expected[key]["ip"]} for key in batch]})
            assert output["matches"] == [[{field: expected[key][field] for field in
                                          ["_key", "owner", "generation"]}] for key in batch]
            assert output["stats"]["document_reads"] == 0
        self.retry(node, prefix + "/check", {})
        fingerprint = hashlib.sha256()
        for key in keys:
            fingerprint.update(json.dumps(expected[key], sort_keys=True).encode())
        return {"node": node, "tenant": tenant, "documents": len(expected),
                "sha256": fingerprint.hexdigest(), "acknowledged_mismatches": 0}

    def catch_up(self):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            a, b = [self.call(i, "/v1/replication/status") for i in [0, 1]]
            positions = lambda s: {x["info"]["id"]: x["seq"] for x in s["tenants"]}
            if positions(a) == positions(b) and not a["unavailable"] and not b["unavailable"]:
                return
            time.sleep(.025)
        raise TimeoutError("replication did not catch up")

    def job(self, tenant, path, body=None, stop=None):
        prefix = f"/v1/tenants/{tenant}"
        job = self.retry(0, prefix + path, body or {})
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            current = self.call(0, prefix + "/backup-jobs/" + job["id"])
            if current["state"] == "completed":
                return current
            assert current["state"] not in ("failed", "interrupted"), current
            if stop is not None and stop.is_set():
                return current
            time.sleep(.1)
        raise TimeoutError("backup operation timed out")


def summarize(samples, seconds, errors):
    values = sorted(samples)
    def q(fraction):
        return round(values[min(len(values) - 1, int(len(values) * fraction))], 3) if values else None
    return {"calls": len(values), "calls_per_second": round(len(values) / seconds, 3),
            "p50_ms": q(.5), "p95_ms": q(.95), "p99_ms": q(.99), "errors": dict(errors)}


def run_phase(lab, name, seconds, tenants, rows, retention, backup):
    done, backup_active = threading.Event(), threading.Event()
    failures, measurements, backups, samples = [], {}, [], []
    started = time.perf_counter()
    retry_before = lab.retry_counts.copy()
    lab.event("phase_started", phase=name, seconds=seconds, retention=retention, backups=backup)

    def worker(kind, rate, action):
        times, errors = [], Counter()
        n, next_time = 0, time.perf_counter()
        try:
            while not done.is_set():
                begin = time.perf_counter()
                try:
                    action(n)
                    times.append((time.perf_counter() - begin) * 1000)
                except RequestError as error:
                    errors[error.code] += 1
                    if error.code not in ("RESOURCE_EXHAUSTED", "REPLICA_LAG"):
                        raise
                n += 1
                next_time = max(next_time + 1 / rate, time.perf_counter())
                done.wait(max(0, next_time - time.perf_counter()))
        except BaseException as error:
            failures.append((kind, repr(error)))
            done.set()
        measurements[kind] = summarize(times, time.perf_counter() - started, errors)
        with gzip.open(lab.root / f"{name}-{kind}-latencies.json.gz", "wt") as out:
            json.dump(times, out)

    def writer(tenant, count, size, batch, phase_key):
        context, refreshed = None, 0
        def action(n):
            nonlocal context, refreshed
            if context is None or time.monotonic() - refreshed > 5:
                context = lab.context(tenant, retention)
                refreshed = time.monotonic()
            offset = (n * batch) % count
            lab.write(tenant, range(offset, min(count, offset + batch)), n + 1, size,
                      context, f"{name}-{phase_key}-{n}", name)
        return action

    heavy, light = tenants
    collection = f"/v1/tenants/{heavy}/collections/assets"
    def get(n):
        i = (n * 7919) % rows
        doc = lab.call(1, collection + f"/documents/{i:08}")
        assert doc["_key"] == f"{i:08}" and doc["ip"] == f"ip-{i}"
        assert doc["payload"] == lab.document(i, 0, 4096)["payload"]

    def lookup(n):
        start = ((n * 37) % (rows // 20)) * 20
        result = lab.call(1, collection + "/lookups/owner/execute", {
            "inputs": [{"ip": f"ip-{i}"} for i in range(start, start + 20)]})
        docs = [matches[0] for matches in result["matches"]]
        assert len(docs) == 20 and len({doc["generation"] for doc in docs}) == 1
        for i, doc in enumerate(docs, start):
            assert doc["_key"] == f"{i:08}" and doc["owner"] == f"team-{doc['generation'] % 17}"
        assert result["stats"]["document_reads"] == 0

    def query(n):
        start = ((n * 37) % (rows // 20)) * 20
        result = lab.call(1, collection + "/query", {
            "filter": {"field": "/ip", "op": "in", "value": [f"ip-{i}" for i in range(start, start + 20)]},
            "select": ["/_key", "/owner", "/generation"], "limit": 20})
        assert len(result["documents"]) == 20 and result.get("cursor") is None
        assert len({doc["generation"] for doc in result["documents"]}) == 1

    def publish():
        previous, n = None, 0
        try:
            existing = lab.call(0, f"/v1/tenants/{heavy}/resources/reference")
            previous = existing["version"]
        except RequestError as error:
            if error.status != 404:
                raise
        while not done.is_set():
            prefix = f"/v1/tenants/{heavy}"
            collection_name = f"reference_{name}_{n}"
            lab.retry(0, prefix + "/collections", {"name": collection_name})
            lab.retry(0, prefix + f"/collections/{collection_name}/batch", {
                "operations": [{"op": "put", "document": {"_key": "one", "value": collection_name}}]})
            context = lab.context(heavy, True)
            publication = lab.retry(0, prefix + "/resources/reference", {
                "version": str(uuid.uuid4()), "collection": collection_name,
                "expected_version": previous, "retry_token": context["retry_token"]}, method="PUT")
            with (lab.root / "publications.jsonl").open("a") as out:
                out.write(json.dumps(publication) + "\n")
            previous = publication["version"]
            n += 1
            done.wait(5)

    def backup_loop():
        while not done.is_set():
            backup_active.set()
            created = lab.job(heavy, "/remote-backups", stop=done)
            if created["state"] != "completed":
                break
            verified = lab.job(heavy, f"/remote-backups/{created['backup']}/verify", stop=done)
            if verified["state"] != "completed":
                break
            pruned = lab.job(heavy, "/remote-backups/prune", {"keep_last": 2}, stop=done)
            backups.append({"create": created, "verify": verified, "prune": pruned})
            save(lab.root / f"{name}-backups.json", backups)
            backup_active.clear()
            done.wait(10)

    def guarded(kind, action):
        try:
            action()
        except BaseException as error:
            failures.append((kind, repr(error)))
            done.set()

    definitions = [("heavy_writer", 40, writer(heavy, rows, 4096, 20, "heavy")),
                   ("light_writer", 20, writer(light, 200, 128, 1, "light")),
                   ("get", 200, get), ("lookup_20", 100, lookup), ("index_query_20", 50, query)]
    threads = [threading.Thread(target=worker, args=item) for item in definitions]
    if retention:
        threads.append(threading.Thread(target=guarded, args=("publisher", publish)))
    if backup:
        threads.append(threading.Thread(target=guarded, args=("backup", backup_loop)))
    for thread in threads:
        thread.start()
    try:
        while time.perf_counter() - started < seconds and not done.is_set():
            metrics = lab.call(0, "/v1/metrics")
            a, b = [lab.call(i, "/v1/replication/status") for i in [0, 1]]
            retention_status = lab.call(0, f"/v1/tenants/{heavy}/retention") if retention else None
            process = {}
            for node in [0, 1]:
                pid = lab.processes[node].pid
                text = Path(f"/proc/{pid}/status").read_text()
                process[str(node)] = {line.split(':')[0]: line.split(':', 1)[1].strip()
                                      for line in text.splitlines() if line.startswith(("VmRSS:", "VmHWM:", "Threads:"))}
                process[str(node)]["file_descriptors"] = len(list(Path(f"/proc/{pid}/fd").iterdir()))
            value = {"elapsed_s": time.perf_counter() - started, "metrics": metrics,
                     "primary": a, "replica": b, "retention": retention_status,
                     "backup_active": backup_active.is_set(), "process": process}
            samples.append(value)
            with (lab.root / f"{name}-samples.jsonl").open("a") as out:
                out.write(json.dumps(value) + "\n")
            done.wait(min(5, max(0, seconds - (time.perf_counter() - started))))
    finally:
        done.set()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads), "load thread did not finish"
    assert not failures, failures
    def heavy_stats(sample):
        return next(t["stats"] for t in sample["metrics"]["tenants"] if t["tenant"]["id"] == heavy)
    overlap = 0
    for old, new in zip(samples, samples[1:]):
        before, after = heavy_stats(old), heavy_stats(new)
        if (before and after and old["backup_active"] and retention
                and after["io"]["flush_bytes"] > before["io"]["flush_bytes"]
                and after["io"]["compaction_write_bytes"] > before["io"]["compaction_write_bytes"]
                and new["retention"]["gc"]["reclaimed_records"] > old["retention"]["gc"]["reclaimed_records"]
                and next(x["seq"] for x in new["replica"]["tenants"] if x["info"]["id"] == heavy)
                > next(x["seq"] for x in old["replica"]["tenants"] if x["info"]["id"] == heavy)):
            overlap += 1
    result = {"phase": name, "requested_seconds": seconds, "actual_seconds": time.perf_counter() - started,
              "operations": measurements, "completed_verified_backups": len(backups),
              "admission_retries": dict(lab.retry_counts - retry_before),
              "overlap_sample_intervals": overlap, "first": samples[0], "last": samples[-1]}
    save(lab.root / f"{name}-result.json", result)
    lab.event("phase_completed", phase=name, operations=measurements,
              verified_backups=len(backups), overlap_intervals=overlap)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--restore-binary", required=True, type=Path)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--baseline-seconds", type=int, default=120)
    parser.add_argument("--combined-seconds", type=int, default=600)
    parser.add_argument("--recovery-seconds", type=int, default=120)
    parser.add_argument("--rows", type=int, default=20000)
    args = parser.parse_args()
    if platform.system() != "Linux" or not __debug__ or args.rows < 100 or args.rows % 100:
        parser.error("requires Linux without -O, rows >=100 and divisible by 100")
    args.binary = args.binary.resolve(strict=True)
    args.restore_binary = args.restore_binary.resolve(strict=True)
    lab = Lab(args.root.resolve(), args.binary)
    result = {"result": "running", "phases": []}
    save(lab.root / "environment.json", {
        "utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
        "binary_sha256": digest(args.binary), "restore_binary_sha256": digest(args.restore_binary),
        "script_sha256": digest(Path(__file__)), "recovery_helper_sha256": digest(Path(__file__).with_name('recovery_drill.py')),
        "rows": args.rows, "payload_bytes": 4096, "options": lab.options,
        "filesystem": subprocess.check_output(["df", "-T", str(lab.root)], text=True)})
    try:
        lab.start(0)
        tenants = [lab.seed("qualification-heavy", args.rows, 4096), lab.seed("qualification-sentinel", 200, 128)]
        result["tenants"] = tenants
        for tenant in tenants:
            lab.retry(0, f"/v1/tenants/{tenant}/compact", {})
        lab.start(1)
        lab.catch_up()
        result["phases"].append(run_phase(lab, "baseline", args.baseline_seconds, tenants, args.rows, False, False))
        for tenant in tenants:
            lab.retry(0, f"/v1/tenants/{tenant}/retention", {"retention_seconds": 20}, method="PUT")
        result["phases"].append(run_phase(lab, "combined", args.combined_seconds, tenants, args.rows, True, True))
        assert result["phases"][-1]["overlap_sample_intervals"] > 0, "no measured five-way overlap"
        assert result["phases"][-1]["completed_verified_backups"] > 0
        for node in [0, 1]:
            result.setdefault("before_restart", []).extend(lab.verify(node, tenant) for tenant in tenants)
        active = lab.retry(0, f"/v1/tenants/{tenants[0]}/remote-backups", {})
        active = lab.call(0, f"/v1/tenants/{tenants[0]}/backup-jobs/{active['id']}")
        assert active["state"] not in ("completed", "failed", "interrupted"), active
        result["interrupted_backup"] = active
        restarted = time.perf_counter()
        lab.stop(0, kill=True)
        lab.stop(1, kill=True)
        lab.start(0)
        lab.start(1)
        lab.catch_up()
        lab.call(0, "/ready/write")
        lab.retry(0, f"/v1/tenants/{tenants[0]}/collections/probes/documents", {
            "document": {"_key": "restart", "value": "recovered"}}, method="PUT")
        result["restart_to_write_and_replica_caught_up_ms"] = (time.perf_counter() - restarted) * 1000
        for node in [0, 1]:
            result.setdefault("after_restart", []).extend(lab.verify(node, tenant) for tenant in tenants)
        lab.event("restart_verified", recovery_ms=result["restart_to_write_and_replica_caught_up_ms"])
        result["phases"].append(run_phase(lab, "recovery", args.recovery_seconds, tenants, args.rows, True, True))
        lab.catch_up()
        for node in [0, 1]:
            result.setdefault("final_validation", []).extend(lab.verify(node, tenant) for tenant in tenants)
        deadline = time.monotonic() + 90
        while True:
            status = lab.call(0, f"/v1/tenants/{tenants[0]}/retention")
            if status["gc"]["migration_complete"] and status["next_expiration_ms"] is None:
                break
            assert time.monotonic() < deadline, ("GC failed to drain", status)
            time.sleep(1)
        result["gc_drained"] = status
        result["final_metrics"] = lab.call(0, "/v1/metrics")
        assert all(t["failure"] is None and t["stats"]["maintenance_error"] is None
                   for t in result["final_metrics"]["tenants"]), "maintenance did not recover"
        lab.catch_up()
        result["final_replication"] = [lab.call(i, "/v1/replication/status") for i in [0, 1]]
        current = lab.call(0, f"/v1/tenants/{tenants[0]}/resources/reference")
        published = lab.call(0, f"/v1/tenants/{tenants[0]}/resources/reference/query", {
            "version": current["version"], "query": {"limit": 10}})
        assert published["result"]["documents"][0]["value"] == current["publication"]["collection"]
        publications = [json.loads(line) for line in (lab.root / "publications.jsonl").read_text().splitlines()]
        try:
            lab.call(0, f"/v1/tenants/{tenants[0]}/resources/reference/query", {
                "version": publications[0]["version"], "query": {"limit": 10}})
            raise AssertionError("expired publication remained readable")
        except RequestError as error:
            assert error.code == "VERSION_UNAVAILABLE"
        result["publication_gc"] = {"published_versions": len(publications), "current_readable": True,
                                     "expired_version_rejected": True,
                                     "remaining_collections": lab.call(0, f"/v1/tenants/{tenants[0]}/collections")}
        backup = lab.job(tenants[0], "/remote-backups")
        lab.job(tenants[0], f"/remote-backups/{backup['backup']}/verify")
        imported = subprocess.run([str(args.restore_binary), str(lab.node_roots[2]),
                                   str(lab.root / "backup.json"), tenants[0], backup["backup"]],
                                  capture_output=True, text=True, timeout=180)
        (lab.root / "restore.log").write_text(imported.stdout + imported.stderr)
        assert imported.returncode == 0, imported.stderr
        lab.start(2)
        result["restored_backup"] = lab.verify(2, tenants[0], backup["checkpoint_seq"])
        result["backup_checkpoint"] = backup
        result["admission_retries_total"] = dict(lab.retry_counts)
        result["result"] = "passed"
    except BaseException as error:
        result.update(result="failed", error=repr(error))
        raise
    finally:
        lab.close()
        save(lab.root / "result.json", result)
    print(json.dumps({"result": result["result"], "root": str(lab.root)}, indent=2))


if __name__ == "__main__":
    main()

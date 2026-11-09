#!/usr/bin/env python3
"""Verify production executables through public interfaces; Linux, stdlib only."""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.request
import uuid

SOURCE = Path(__file__).resolve().parents[1]


def request(base, token, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


@contextmanager
def serving(binary, root, auth, log, backup=None):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    command = [str(binary), "serve", "--root", str(root), "--auth", str(auth),
               "--listen", f"127.0.0.1:{port}"]
    if backup:
        command.extend(["--backup-config", str(backup)])
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=output)
        base = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError(log.read_text())
                try:
                    with urllib.request.urlopen(base + "/ready/write", timeout=1) as response:
                        assert response.status == 200
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(log.read_text())
                    time.sleep(0.05)
            yield base
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("server did not drain within 40 seconds")


def subject_queries(base, token):
    tenant = request(base, token, "POST", "/v1/tenants",
                     {"name": "release-subjects-" + uuid.uuid4().hex})["id"]
    prefix = "/v1/tenants/" + tenant
    identity = prefix + "/identity"
    request(base, token, "POST", identity + "/initialize", {})
    created = request(base, token, "POST", identity + "/write", {
        "action": "create_entity", "operation_id": "release-entity", "attributes": {"owner": "test"},
    })
    entity = created["entity"]["entity_id"]
    identifier = {"kind": "account", "namespace": "release", "value": "confirmed"}
    request(base, token, "POST", identity + "/write", {
        "action": "bindings", "operation_id": "release-bind",
        "changes": [{
            "identifier": identifier, "expected_revision": "absent",
            "effective": {"mode": "manual", "from": "2026-01-01T00:00:00Z"},
            "change": {"action": "bind", "entity_id": entity, "reason": "release validation"},
        }],
    })
    item = {"identifier": identifier, "valid_at": "2026-01-02T00:00:00Z"}
    many = {"items": [item, item], "known_at": "latest"}
    result = request(base, token, "POST", identity + "/resolve_many", many)
    assert len(result["results"]) == 2
    assert all(row["binding"]["entity_id"] == entity for row in result["results"])
    dsl = request(base, token, "POST", prefix + "/dsl", {
        "statement": "resolvemany with $request", "params": {"request": many},
    })
    assert dsl["results"] == result["results"]
    ids = {"entity_id": entity, "valid_at": item["valid_at"], "limit": 100}
    native = request(base, token, "POST", identity + "/ids", ids)
    dsl = request(base, token, "POST", prefix + "/dsl", {
        "statement": "ids with $request", "params": {"request": ids},
    })
    assert native["identifiers"] == dsl["identifiers"]
    assert native["identifiers"][0]["identifier"] == identifier
    events, cursor = [], None
    while True:
        body = {"statement": "history with $request",
                "params": {"request": {"entity_id": entity, "limit": 1}}}
        if cursor:
            body["cursor"] = cursor
        page = request(base, token, "POST", prefix + "/dsl", body)
        events.extend(page["events"])
        cursor = page["cursor"]
        if cursor is None:
            break
    assert {event["operation_id"] for event in events} == {"release-entity", "release-bind"}
    request(base, token, "DELETE", prefix)
    return {"resolve_many_duplicates": True, "native_dsl_ids": True, "history_paging": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True, help="new exclusive validation directory")
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    binaries = args.binary_dir.resolve()
    contract = json.loads((SOURCE / "openapi.json").read_text())
    expected_version = contract["info"]["version"]
    for name in ["tenant-kv", "tenant-kv-restore", "tenant-kv-repair"]:
        version = subprocess.check_output([str(binaries / name), "--version"], text=True).strip()
        assert version == f"{name} {expected_version}", version
        subprocess.run([str(binaries / name), "--help"], check=True, stdout=subprocess.DEVNULL)
    token = secrets.token_hex(32)
    token_file, auth, backup = root / "token.txt", root / "auth.json", root / "backup.json"
    token_file.write_text(token + "\n")
    token_file.chmod(0o600)
    auth.write_text(json.dumps({"tokens": [{
        "sha256": hashlib.sha256(token.encode()).hexdigest(),
        "subject": "release-admin", "tenant": None, "role": "admin",
    }]}))
    auth.chmod(0o600)
    backup.write_text(json.dumps({
        "service": "fs", "options": {"root": str(root / "remote")},
        "max_backup_bytes": 16 << 20, "max_staging_bytes": 64 << 20,
        "reserve_free_bytes": 1 << 20,
    }))
    hashed = subprocess.check_output(
        [str(binaries / "tenant-kv"), "token-hash"], input=token + "\n", text=True
    ).strip()
    assert hashed == hashlib.sha256(token.encode()).hexdigest()
    checks = {}
    with serving(binaries / "tenant-kv", root / "data", auth, root / "server.log", backup) as base:
        capabilities = request(base, token, "GET", "/v1/capabilities")
        assert capabilities["version"] == expected_version
        assert capabilities["storage_registry"] == 6 and capabilities["replication_protocol"] == 4
        schema = request(base, token, "GET", "/openapi.json")
        assert schema == contract
        for script in ["http_smoke.py", "dsl_smoke.py", "identity_smoke.py", "retention_smoke.py"]:
            command = ["python3", str(SOURCE / "scripts" / script), base, str(token_file)]
            if script == "http_smoke.py":
                command.append("--remote-backups")
            result = subprocess.check_output(command, text=True, timeout=180)
            checks[script] = json.loads(result.splitlines()[-1])
        checks["subjects"] = subject_queries(base, token)
    # Existing HTTP smoke keeps one remote backup after deleting its local tenant.
    smoke = checks["http_smoke.py"]
    tenant = smoke["tenant"]
    backup_id = smoke["remote_backup"]["second"]["backup"]
    imported_root = root / "imported"
    imported = subprocess.check_output([
        str(binaries / "tenant-kv-restore"), str(imported_root), str(backup),
        tenant, backup_id,
    ], text=True, timeout=180)
    assert json.loads(imported)["state"] == "completed"
    path = "/v1/tenants/" + tenant + "/collections/assets/documents/gateway"
    with serving(binaries / "tenant-kv", imported_root, auth, root / "imported.log") as base:
        assert request(base, token, "GET", path)["owner"] == "platform"
    # Repair only this disposable imported database, retaining its valid WAL prefix.
    wals = list((imported_root / "tenants" / tenant / "wal").glob("*.wal"))
    assert len(wals) == 1, wals
    wal = wals[0]
    offset = wal.stat().st_size
    damaged = b"release-validation-incomplete-tail" * 8
    with wal.open("ab") as stream:
        stream.write(damaged)
        stream.flush()
        import os
        os.fsync(stream.fileno())
    repaired = json.loads(subprocess.check_output([
        str(binaries / "tenant-kv-repair"), str(imported_root), tenant, wal.stem,
        "--at", str(offset), "--archive", str(root / "repair-archive"), "--accept-data-loss",
    ], text=True, timeout=180))
    assert repaired["phase"] == "published"
    assert repaired["discarded_bytes"] == len(damaged)
    assert repaired["old_history"] != repaired["new_history"]
    assert (Path(repaired["database_archive"]) / "sst").is_dir()
    assert Path(repaired["evidence"]).read_bytes().endswith(damaged)
    with serving(binaries / "tenant-kv", imported_root, auth, root / "repaired.log") as base:
        assert request(base, token, "GET", path)["owner"] == "platform"
    report = {
        "version": expected_version, "binary_dir": str(binaries), "passed": True,
        "checks": checks, "offline_import": True, "archived_wal_repair": True,
    }
    (root / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

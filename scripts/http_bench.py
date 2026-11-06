#!/usr/bin/env python3
"""Loopback HTTP latency probe; includes Python client, JSON and network overhead."""
import concurrent.futures
import http.client
import json
import pathlib
import sys
import threading
import time
import urllib.parse
import uuid

url = urllib.parse.urlsplit(sys.argv[1])
token = pathlib.Path(sys.argv[2]).read_text().strip()
readers, per_reader = 16, 20000
headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}


def connect():
    cls = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    return cls(url.hostname, url.port, timeout=30)


def request(connection, method, path, body=None):
    connection.request(method, url.path + path, body=None if body is None else json.dumps(body), headers=headers)
    response = connection.getresponse()
    result = json.loads(response.read())
    if response.status >= 400:
        raise RuntimeError(result)
    return result


control = connect()
tenant = request(control, "POST", "/v1/tenants", {"name": "http-bench-" + uuid.uuid4().hex})["id"]
prefix = "/v1/tenants/" + tenant
collection = prefix + "/collections/assets"
request(control, "POST", prefix + "/collections", {"name": "assets"})
request(control, "PUT", collection + "/documents", {"document": {"_key": "hot", "payload": "x" * 1024}})
barrier = threading.Barrier(readers + 1)


def reader():
    connection = connect()
    request(connection, "GET", collection + "/documents/hot")
    samples = []
    barrier.wait()
    for _ in range(per_reader):
        started = time.perf_counter_ns()
        value = request(connection, "GET", collection + "/documents/hot")
        assert len(value["payload"]) == 1024
        samples.append((time.perf_counter_ns() - started) / 1000)
    connection.close()
    return samples


with concurrent.futures.ThreadPoolExecutor(max_workers=readers) as pool:
    workers = [pool.submit(reader) for _ in range(readers)]
    barrier.wait()
    started = time.perf_counter()
    samples = sorted(sample for worker in workers for sample in worker.result())
    seconds = time.perf_counter() - started
print(json.dumps({"mode": "http_keepalive_get", "readers": readers, "operations": len(samples),
                  "seconds": seconds, "qps": len(samples) / seconds,
                  "p50_us": samples[len(samples) // 2], "p95_us": samples[len(samples) * 95 // 100],
                  "p99_us": samples[len(samples) * 99 // 100],
                  "notes": "loopback Python standard-library client; 1 hot 1KiB payload; no concurrent writes"}))
for attempt in range(50):
    try:
        request(control, "DELETE", prefix)
        break
    except RuntimeError as error:
        if error.args[0].get("error", {}).get("code") != "BUSY" or attempt == 49:
            raise
        time.sleep(0.1)
control.close()

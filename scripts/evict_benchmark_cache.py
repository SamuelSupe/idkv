#!/usr/bin/env python3
"""Best-effort eviction of this benchmark's files from the Linux page cache.

Run only while the benchmark database is closed. No global drop_caches write.
This cannot evict a device's internal cache or promise physical cold media.
"""
import os
import pathlib
import sys

root = pathlib.Path(sys.argv[1]).resolve(strict=True)
if not (root / "benchmark.json").is_file():
    raise SystemExit("expected an existing benchmark directory")
files = count = 0
for path in (root / "tenants").rglob("*"):
    if path.is_file() and path.suffix in (".sst", ".wal"):
        with path.open("rb") as file:
            os.fsync(file.fileno())
            os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        files += 1
        count += path.stat().st_size
print({"files_advised": files, "bytes_advised": count})

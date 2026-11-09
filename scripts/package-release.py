#!/usr/bin/env python3
"""Package verified static Linux tools from a clean Git checkout; stdlib only."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
if sys.version_info < (3, 11):
    raise SystemExit("Release packaging requires Python 3.11+ (stdlib tomllib/file_digest). Run it in Linux.")
import tomllib

ROOT = Path(__file__).resolve().parents[1]
TARGETS = {
    "aarch64-unknown-linux-musl": 183,
    "x86_64-unknown-linux-musl": 62,
}


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_elf(path, machine):
    if path.is_symlink():
        raise ValueError(f"symlink executable: {path}")
    with path.open("rb") as stream:
        header = stream.read(64)
        if header[:6] != b"\x7fELF\x02\x01":
            raise ValueError(f"not a little-endian ELF64 executable: {path}")
        if int.from_bytes(header[18:20], "little") != machine:
            raise ValueError(f"wrong CPU architecture: {path}")
        offset = int.from_bytes(header[32:40], "little")
        size = int.from_bytes(header[54:56], "little")
        count = int.from_bytes(header[56:58], "little")
        for index in range(count):
            stream.seek(offset + index * size)
            if int.from_bytes(stream.read(4), "little") == 3:
                raise ValueError(f"dynamic loader dependency: {path}")


def archive(directory, output, epoch):
    # Fixed metadata and gzip timestamp make packaging repeatable for the same inputs.
    with output.open("wb") as raw, gzip.GzipFile(
        fileobj=raw, filename="", mode="wb", mtime=epoch
    ) as compressed, tarfile.open(
        fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
    ) as tar:
        for path in [directory, *sorted(directory.rglob("*"))]:
            if path.is_symlink():
                raise ValueError(f"symlink in release: {path}")
            info = tar.gettarinfo(str(path), arcname=str(path.relative_to(directory.parent)))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = epoch
            info.pax_headers = {}
            if path.is_dir():
                info.mode = 0o755
                tar.addfile(info)
            else:
                info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                with path.open("rb") as stream:
                    tar.addfile(info, stream)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=all"):
        raise SystemExit("Commit the reviewed source/docs before packaging a release.")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    commit = git("rev-parse", "HEAD")
    epoch = int(git("show", "-s", "--format=%ct", "HEAD"))
    contract = json.loads((ROOT / "openapi.json").read_text())
    if contract["info"]["version"] != version:
        raise SystemExit("Regenerate openapi.json for this package version.")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    tracked = git(
        "ls-files", "-z", "README.md", "LICENSE", "openapi.json",
        "docs", "examples", "scripts", "deploy"
    ).split("\0")
    tracked = [filename for filename in tracked if filename]
    artifacts = []
    for target, machine in TARGETS.items():
        binary_dir = output / target / "bin"
        for name in ["tenant-kv", "tenant-kv-restore", "tenant-kv-repair"]:
            validate_elf(binary_dir / name, machine)
        compiler = (output / target / "RUSTC.txt").read_text()
        with tempfile.TemporaryDirectory(prefix="idkv-release-") as temp:
            package = Path(temp) / f"idkv-v{version}-{target}"
            package.mkdir()
            for filename in tracked:
                source = ROOT / filename
                if source.is_symlink():
                    raise ValueError(f"symlink in source distribution: {source}")
                destination = package / filename
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
            shutil.copytree(binary_dir, package / "bin")
            (package / "RUSTC.txt").write_text(compiler)
            (package / "BUILD-INFO.json").write_text(json.dumps({
                "version": version, "source_commit": commit,
                "source_tree": git("rev-parse", "HEAD^{tree}"),
                "source_url": f"https://github.com/SamuelSupe/idkv/tree/{commit}",
                "target": target, "static_musl": True, "features": [],
                "rustflags": "-C target-feature=+crt-static -C target-cpu=generic",
                "cargo_lock_sha256": digest(ROOT / "Cargo.lock"),
                "compiler": compiler.splitlines()[0],
            }, indent=2) + "\n")
            path = output / f"{package.name}.tar.gz"
            archive(package, path, epoch)
        artifacts.append({
            "name": path.name, "target": target, "bytes": path.stat().st_size,
            "sha256": digest(path),
        })
    shutil.copyfile(ROOT / "openapi.json", output / "openapi.json")
    artifacts.append({
        "name": "openapi.json", "bytes": (output / "openapi.json").stat().st_size,
        "sha256": digest(output / "openapi.json"),
    })
    manifest = output / "RELEASE-MANIFEST.json"
    manifest.write_text(json.dumps({
        "project": "idkv", "version": version,
        "source_commit": commit, "artifacts": artifacts,
    }, indent=2) + "\n")
    checksums = [(item["name"], item["sha256"]) for item in artifacts]
    checksums.append((manifest.name, digest(manifest)))
    (output / "SHA256SUMS").write_text(
        "".join(f"{sha}  {name}\n" for name, sha in sorted(checksums))
    )
    print(json.dumps({"version": version, "source_commit": commit, "artifacts": artifacts}))


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# Build the three production tools inside OrbStack/Docker; no failpoints.
set -euo pipefail
case "${1:-}" in
  aarch64-unknown-linux-musl) platform=linux/arm64; arch=arm64 ;;
  x86_64-unknown-linux-musl) platform=linux/amd64; arch=amd64 ;;
  *) echo "usage: $0 {aarch64-unknown-linux-musl|x86_64-unknown-linux-musl}" >&2; exit 2 ;;
esac
target=$1
cd "$(dirname "$0")/.."
mkdir -p dist
image="idkv-release-build:1.0.0-$arch"
build=(docker build --platform "$platform" -f deploy/release.Dockerfile -t "$image")
if [[ -n "${IDKV_BUILD_CA_FILE:-}" ]]; then
  build+=(--secret "id=build_ca,src=$IDKV_BUILD_CA_FILE")
fi
"${build[@]}" .
docker run --rm --platform "$platform" \
  -v "$PWD:/workspace:ro" -v "$PWD/dist:/out" \
  -v "idkv-release-$arch-target:/target" \
  -v idkv-release-registry:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/target -e CARGO_BUILD_JOBS=4 \
  -e "RELEASE_TARGET=$target" \
  -e "RUSTFLAGS=-C target-feature=+crt-static -C target-cpu=generic" \
  "$image" sh -ec '
    cargo build --locked --release --target "$RELEASE_TARGET" \
      --bin tenant-kv --bin tenant-kv-restore --bin tenant-kv-repair
    mkdir -p "/out/$RELEASE_TARGET/bin"
    for name in tenant-kv tenant-kv-restore tenant-kv-repair; do
      src="/target/$RELEASE_TARGET/release/$name"
      file "$src"
      if readelf -l "$src" | grep -q INTERP; then
        echo "release executable unexpectedly needs a dynamic loader" >&2; exit 1
      fi
      install -m 755 "$src" "/out/$RELEASE_TARGET/bin/$name"
      "/out/$RELEASE_TARGET/bin/$name" --version
    done
    rustc --version --verbose > "/out/$RELEASE_TARGET/RUSTC.txt"
  '

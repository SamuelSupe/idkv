# Release engineering

The binary distribution targets Linux aarch64 and x86_64 with static musl linking. Production archives contain tenant-kv, tenant-kv-restore and tenant-kv-repair. Source regression tests use crash-probe and the failpoints feature.

## Build

Check out the intended source/tag, including Cargo.lock and rust-toolchain.toml. Docker must be available; on macOS the release was built with OrbStack. The build image is pinned by digest in deploy/release.Dockerfile.

```sh
scripts/build-release.sh aarch64-unknown-linux-musl
scripts/build-release.sh x86_64-unknown-linux-musl
```

The script selects the matching Linux container architecture, builds the three explicit production binaries with cargo --locked --release, and writes dist/TARGET/bin plus RUSTC.txt. CARGO_BUILD_JOBS=4 bounds each build. It uses generic CPU code generation and static CRT linking; file/readelf and --version checks reject the wrong output.

The default Cargo release profile uses thin LTO and one codegen unit. The separate million-row test report describes its faster validation-build settings; do not confuse those with this distribution profile.

Container images, Cargo downloads and target caches are local build infrastructure, not release assets. Source is mounted read-only. The image download can be substantial on a fresh machine.

On a network with a trusted local TLS inspection CA, set IDKV_BUILD_CA_FILE to a PEM CA bundle already trusted by that build environment. BuildKit passes it as a build secret to establish package/Cargo download trust; it is not placed in release archives. Do not disable certificate verification.

## Validate the actual binaries

Execute each architecture in a matching Linux environment. x86_64 emulation on ARM verifies execution but is not native x86 hardware performance qualification. Check:

1. All three --version and --help commands.
2. ELF machine type and no PT_INTERP dynamic loader.
3. Server startup/readiness, capabilities version/formats and generated OpenAPI.
4. HTTP CRUD/index/lookup/check/backup/restore smoke.
5. DSL, temporal identity and retention smoke.
6. A primary/replica pair, catch-up, switchover, restart and acknowledged-data checks.

Run the source regression suite separately with failpoints enabled. The [release notes index](README.md#english-guides) lists the validation results and scope for each published version.

Regenerate the default contract from the release executable:

```sh
dist/aarch64-unknown-linux-musl/bin/tenant-kv openapi > openapi.json
```

Run that on Linux; an ARM Linux executable will not run directly on macOS. Both architectures must generate equivalent JSON.

## Package a clean source commit

Finish the release notes, commit the reviewed source, then use Python **3.11+** (standard library only) in Linux:

```sh
python3 scripts/package-release.py
cd dist
sha256sum -c SHA256SUMS
```

Packaging refuses a dirty checkout, validates ELF architecture/static loading and the OpenAPI version, and copies only tracked source documentation/examples/scripts. It adds exact source/tree/lock/compiler metadata. Archive ownership and timestamps are normalized to the source commit. Checksums cover both archives, OpenAPI and RELEASE-MANIFEST.json; SHA256SUMS does not hash itself.

Do not overwrite an already published version with different binaries. Fixes belong in a new patch release.

## Publish and verify

Create an annotated vVERSION tag at the packaged source commit and push the reviewed branch/tag. Create a GitHub draft release, attach the two archives, openapi.json, RELEASE-MANIFEST.json and SHA256SUMS, and use docs/releases/vVERSION.md as release notes.

Before publishing, download those draft assets into a fresh directory and verify SHA256SUMS against the downloaded bytes. Verify the archived BUILD-INFO source commit against the tag and the remote branch. Publish the release as the latest stable version only after these checks; ensure draft=false and prerelease=false.

The GitHub-generated source archives are available from the tag. Rust users can depend on that tag; this workflow does not publish a crates.io package or a container registry image.

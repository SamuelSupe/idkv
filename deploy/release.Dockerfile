FROM rust:1.99.1-alpine@sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383
RUN --mount=type=secret,id=build_ca \
    if [ -s /run/secrets/build_ca ]; then cat /run/secrets/build_ca >> /etc/ssl/certs/ca-certificates.crt; fi; \
    apk add --no-cache build-base binutils file python3 curl ca-certificates && \
    if [ -s /run/secrets/build_ca ]; then cat /run/secrets/build_ca >> /etc/ssl/certs/ca-certificates.crt; fi
WORKDIR /workspace

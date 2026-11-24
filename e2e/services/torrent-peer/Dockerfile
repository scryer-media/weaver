# syntax=docker/dockerfile:1.7
# A BitTorrent peer for the torrent lane: the rqbit command-line client, a
# client that is not Weaver, so the seeder and the leecher are an independent
# reading of the wire protocol. The harness passes the fork and revision it
# pins and the toolchain from rust-toolchain.toml; nothing here is a default
# to fall back on.
ARG BOOTSTRAP_IMAGE
FROM ${BOOTSTRAP_IMAGE} AS builder
ARG RQBIT_GIT
ARG RQBIT_REV
ARG RUST_TOOLCHAIN
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal && \
    rustup default "${RUST_TOOLCHAIN}"
RUN --mount=type=cache,id=weaver-e2e-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=weaver-e2e-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=weaver-e2e-torrent-peer-target,target=/build/target,sharing=locked \
    CARGO_TARGET_DIR=/build/target \
    cargo install --locked --git "${RQBIT_GIT}" --rev "${RQBIT_REV}" \
      --no-default-features --features rust-tls --root /out rqbit

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/bin/rqbit /usr/local/bin/rqbit
RUN mkdir -p /payload /leech
EXPOSE 3030 6881
ENTRYPOINT ["rqbit"]

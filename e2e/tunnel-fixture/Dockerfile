# syntax=docker/dockerfile:1.7
# SSH and WireGuard endpoints for the advanced-networking flows. The build
# context is this directory alone: the crate depends on proxy-tunnels by git
# revision and on nothing from the Weaver tree.
FROM rust:1.96-slim-bookworm@sha256:e18a79fc84dfcfc3ab5ba72290398a644c135c97eaa881447fddc354ee4701a3 AS builder
# The harness passes the channel Weaver's rust-toolchain.toml pins.
ARG RUST_TOOLCHAIN=1.97.1
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal && rustup default "${RUST_TOOLCHAIN}"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,id=weaver-e2e-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=weaver-e2e-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=weaver-e2e-tunnel-fixture-target,target=/src/target,sharing=locked \
    cargo build --locked --release && cp target/release/tunnel-fixture /tmp/tunnel-fixture

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
COPY --from=builder /tmp/tunnel-fixture /usr/local/bin/tunnel-fixture
EXPOSE 2221 2222 2223 2224 2225 51821/udp 51822/udp 51823/udp 8095
ENTRYPOINT ["/usr/local/bin/tunnel-fixture"]

FROM golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19 AS build

WORKDIR /src
COPY go.mod ./
COPY cmd/nntpshaper ./cmd/nntpshaper
COPY internal/nntpshaper ./internal/nntpshaper
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w -buildid=' -o /out/nntpshaper ./cmd/nntpshaper

# The runtime image carries iproute2 so the entrypoint can render a plan's
# fixed round trip with tc netem and the proxy can read the qdiscs back for
# every attestation snapshot. Nothing else from the base image is used.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/nntpshaper /nntpshaper
COPY docker/nntp-shaper/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh
EXPOSE 119 563 8080
ENTRYPOINT ["/entrypoint.sh"]

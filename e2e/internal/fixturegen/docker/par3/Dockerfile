# A source-locked PAR3 generator used only to create parity fixtures. It builds
# the official reference implementation, par3cmdline, from one pinned
# revision. The source tarball's SHA-256 is verified before a single line of it
# is compiled, so the image can only ever contain the revision
# test-corpus/toolchains.json pins.
FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS build

ARG PAR3_URL
ARG PAR3_SHA256

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends build-essential ca-certificates cmake curl; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/par3.tar.gz "$PAR3_URL"; \
    echo "$PAR3_SHA256  /tmp/par3.tar.gz" | sha256sum -c -; \
    mkdir -p /src; \
    tar -xzf /tmp/par3.tar.gz -C /src --strip-components=1; \
    cd /src/src; \
    cmake -D CMAKE_BUILD_TYPE=Release -B build; \
    cmake --build build --parallel "$(nproc)"; \
    install -D -m 0755 build/par3cmd/par3 /opt/par3/bin/par3

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends libstdc++6; \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/par3/bin/par3 /usr/local/bin/par3

ENTRYPOINT ["par3"]

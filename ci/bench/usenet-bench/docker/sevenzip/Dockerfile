# The official 7-Zip Linux console binary, downloaded from 7-zip.org at build
# time and verified against the SHA-256 in docker/sevenzip/toolchain.json
# before it is installed. Distribution `p7zip` forks are deliberately not
# used: they are a different codebase with a different container writer, so a
# fixture written by one is not evidence about the other.
#
# The base image is the same digest-pinned Debian the RARLAB and PAR2 writer
# images use, so every fixture in the corpus is produced on one pinned base.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

ARG SEVENZIP_URL
ARG SEVENZIP_SHA256
ARG SEVENZIP_BINARY=7zz

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends ca-certificates curl tar xz-utils; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/7z.tar.xz "$SEVENZIP_URL"; \
    echo "$SEVENZIP_SHA256  /tmp/7z.tar.xz" | sha256sum -c -; \
    case "$SEVENZIP_BINARY" in ''|.|..|*/*|*\\*) exit 64;; esac; \
    mkdir -p /opt/7zip; \
    tar -xJf /tmp/7z.tar.xz -C /opt/7zip; \
    install -m 0755 "/opt/7zip/$SEVENZIP_BINARY" /usr/local/bin/7zz; \
    rm -rf /tmp/7z.tar.xz /opt/7zip /var/lib/apt/lists/*

ENTRYPOINT ["7zz"]

# The RAR executable is downloaded from RARLAB at build time, not copied into
# this repository or a fixture. The SHA-256 argument makes a changed upstream
# download fail closed.
# RARLAB's older official Linux releases also contain a 32-bit static binary.
# A digest-pinned Debian base runs both that binary and the current x64 binary
# through Docker's supported amd64 emulation on Apple Silicon.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

ARG RAR_URL
ARG RAR_SHA256
ARG RAR_BINARY=rar

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends ca-certificates curl ffmpeg tar; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/rar.tar.gz "$RAR_URL"; \
    echo "$RAR_SHA256  /tmp/rar.tar.gz" | sha256sum -c -; \
    case "$RAR_BINARY" in ''|.|..|*/*|*\\*) exit 64;; esac; \
    mkdir -p /opt/rar; \
    tar -xzf /tmp/rar.tar.gz -C /opt/rar; \
    install -m 0755 "/opt/rar/rar/$RAR_BINARY" /usr/local/bin/rar; \
    rm -rf /tmp/rar.tar.gz /opt/rar /var/lib/apt/lists/*

ENTRYPOINT ["rar"]

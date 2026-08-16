# The official 7-Zip Linux console binary, downloaded from 7-zip.org at build
# time and verified against the SHA-256 test-corpus/toolchains.json pins before
# it is installed. Distribution `p7zip` forks are deliberately not used: they
# are a different codebase with a different container writer.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

ARG SEVENZIP_URL
ARG SEVENZIP_SHA256

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends ca-certificates curl tar xz-utils; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/7z.tar.xz "$SEVENZIP_URL"; \
    echo "$SEVENZIP_SHA256  /tmp/7z.tar.xz" | sha256sum -c -; \
    mkdir -p /opt/7zip; \
    tar -xJf /tmp/7z.tar.xz -C /opt/7zip; \
    install -m 0755 /opt/7zip/7zz /usr/local/bin/7zz; \
    rm -rf /tmp/7z.tar.xz /opt/7zip /var/lib/apt/lists/*

ENTRYPOINT ["7zz"]

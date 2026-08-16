# A source-locked PAR2 generator used only to create parity fixtures. The
# source tarball's SHA-256 is verified before a single line of it is compiled,
# so the image can only ever contain the release test-corpus/toolchains.json
# pins.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS build

ARG PAR2_URL
ARG PAR2_SHA256

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends autoconf automake build-essential ca-certificates curl; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/par2.tar.gz "$PAR2_URL"; \
    echo "$PAR2_SHA256  /tmp/par2.tar.gz" | sha256sum -c -; \
    mkdir -p /src; \
    tar -xzf /tmp/par2.tar.gz -C /src --strip-components=1; \
    cd /src; \
    ./automake.sh; \
    ./configure --prefix=/opt/par2; \
    make -j"$(nproc)"; \
    make install

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends libgomp1 libstdc++6; \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/par2/bin/par2 /usr/local/bin/par2

ENTRYPOINT ["par2"]

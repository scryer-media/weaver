# The uuencode oracle, and the decoder the generator checks itself against.
#
# uuencoding predates the harness by decades and the corpus does not hand-roll
# it: `uuenview` writes the encoding and splits it into multi-part postings at
# line boundaries, exactly as a real poster would, and `uudeview` reads the
# result back so a fixture can only ship if a real decoder recovers the source
# bytes from it. Both come out of the same UUDeview release.
#
# The source tarball's SHA-256 is verified before a single line of it is
# compiled, so the image can only ever contain the release
# test-corpus/toolchains.json pins.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS build

ARG UUDEVIEW_URL
ARG UUDEVIEW_SHA256

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends build-essential ca-certificates curl; \
    curl --fail --location --proto '=https' --tlsv1.2 --output /tmp/uudeview.tar.gz "$UUDEVIEW_URL"; \
    echo "$UUDEVIEW_SHA256  /tmp/uudeview.tar.gz" | sha256sum -c -; \
    mkdir -p /src; \
    tar -xzf /tmp/uudeview.tar.gz -C /src --strip-components=1; \
    cd /src; \
    ./configure --prefix=/opt/uu --disable-tcl --disable-tk; \
    make -j"$(nproc)"; \
    make install

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

COPY --from=build /opt/uu/bin/uuenview /usr/local/bin/uuenview
COPY --from=build /opt/uu/bin/uudeview /usr/local/bin/uudeview
COPY entrypoint.sh /usr/local/bin/uu

# This is the one image that ships two binaries, so its first argument selects
# which. Everything after it is the tool's own argument vector, passed through
# untouched.
ENTRYPOINT ["/usr/local/bin/uu"]

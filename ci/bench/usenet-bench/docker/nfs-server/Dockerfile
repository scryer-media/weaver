# A throttled NFSv4 server used only as benchmark storage. It exports one
# directory over a deterministically shaped link so a client's completion
# behaviour over slow network storage can be measured instead of guessed.
#
# The same image is the verification helper: the controller cannot read the
# export without a host-side root mount, so it runs the harness's own
# `verify-output` inside this image with the run's volume attached. Keeping one
# image means the storage lane introduces exactly one pinned base.
#
# The base is digest-pinned; the Debian packages are the distribution's own, as
# in every other image here.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends \
        nfs-kernel-server \
        iproute2 \
        kmod; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /export; \
    chmod 0777 /export

COPY docker/nfs-server/entrypoint.sh /usr/local/bin/nntpbench-nfs-entrypoint
COPY docker/nfs-server/control.sh /usr/local/bin/nntpbench-nfs-control

RUN chmod 0755 /usr/local/bin/nntpbench-nfs-entrypoint /usr/local/bin/nntpbench-nfs-control

# No port is published. Clients reach 2049 over the benchmark storage network
# only, and the shaped link is what makes the export interesting.
EXPOSE 2049

ENTRYPOINT ["/usr/local/bin/nntpbench-nfs-entrypoint"]

FROM alpine:latest@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8

ARG TARGETARCH

RUN apk add --no-cache tzdata util-linux

WORKDIR /app

COPY ${TARGETARCH}/weaver-* /opt/weaver/
COPY entrypoint.sh /entrypoint.sh
COPY runtime-select.sh /runtime-select.sh
RUN chmod +x /entrypoint.sh /runtime-select.sh /opt/weaver/weaver-*

EXPOSE 9090

RUN mkdir -p /config
VOLUME /config

ENV PUID=1000
ENV PGID=1000
ENV TZ=Etc/UTC
ENV WEAVER_DEPLOYMENT_ENV=docker
# Weaver binds loopback by default so a desktop install is never exposed by
# accident. A container's loopback is its own network namespace, where that
# default protects nothing and only makes the published port unreachable —
# what decides exposure here is whether the operator publishes a port at all.
# Overridable like any other variable, for host-network or proxy-only setups.
ENV WEAVER_HTTP_BIND_ADDRESS=0.0.0.0

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]

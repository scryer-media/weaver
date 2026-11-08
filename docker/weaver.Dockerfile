FROM alpine:latest@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

ARG TARGETARCH

RUN apk add --no-cache tzdata util-linux

WORKDIR /app

COPY ${TARGETARCH}/weaver /opt/weaver/weaver
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/weaver/weaver

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

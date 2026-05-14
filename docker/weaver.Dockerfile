FROM alpine:latest

ARG TARGETARCH
ARG WEAVER_RUNTIME_MODE=default

RUN apk add --no-cache su-exec tzdata

WORKDIR /app

COPY ${TARGETARCH}/weaver-* /opt/weaver/
COPY entrypoint.sh /entrypoint.sh
COPY runtime-select.sh /runtime-select.sh
RUN case "$WEAVER_RUNTIME_MODE:$TARGETARCH" in \
      modern:amd64 | modern:arm64) rm -f /opt/weaver/weaver-portable ;; \
    esac \
 && chmod +x /entrypoint.sh /runtime-select.sh /opt/weaver/weaver-*

EXPOSE 9090

RUN mkdir -p /config
VOLUME /config

ENV PUID=1000
ENV PGID=1000
ENV TZ=Etc/UTC
ENV WEAVER_RUNTIME_MODE=${WEAVER_RUNTIME_MODE}

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]

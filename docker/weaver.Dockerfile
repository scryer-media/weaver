FROM alpine:latest

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

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]

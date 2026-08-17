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

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]

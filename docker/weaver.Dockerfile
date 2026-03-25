FROM alpine:latest

ARG TARGETARCH

RUN apk add --no-cache su-exec tzdata

WORKDIR /app

COPY ${TARGETARCH}/weaver /usr/local/bin/weaver
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9090

RUN mkdir -p /config /data
VOLUME /config
VOLUME /data

ENV PUID=1000
ENV PGID=1000

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]

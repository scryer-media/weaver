FROM alpine:latest AS tzdata

RUN apk add --no-cache tzdata

FROM scratch

ARG TARGETARCH

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo
WORKDIR /app

COPY ${TARGETARCH}/weaver-docker-launcher /weaver-docker-launcher
COPY ${TARGETARCH}/payloads /opt/weaver/payloads

EXPOSE 9090

VOLUME /config

ENV PUID=1000
ENV PGID=1000
ENV TZ=Etc/UTC

STOPSIGNAL SIGTERM

ENTRYPOINT ["/weaver-docker-launcher"]
CMD ["--config", "/config", "serve", "--port", "9090"]

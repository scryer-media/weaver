FROM busybox:1.37 AS prep
RUN mkdir -p /data && chown 65532:65532 /data

FROM gcr.io/distroless/cc-debian13:nonroot

WORKDIR /app

COPY --chown=65532:65532 weaver /usr/local/bin/weaver

EXPOSE 9090

COPY --from=prep --chown=65532:65532 /data /data
VOLUME /data

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/weaver"]
CMD ["--config", "/data", "serve", "--port", "9090"]

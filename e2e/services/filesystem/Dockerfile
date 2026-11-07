ARG WEAVER_IMAGE=debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends gcc pkg-config libfuse3-dev && rm -rf /var/lib/apt/lists/*
COPY filesystem.c /src/filesystem.c
RUN cc -std=c11 -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Werror /src/filesystem.c $(pkg-config fuse3 --cflags --libs) -o /filesystem-fixture

# This derived image is only for tests. The product image stays unchanged.
FROM ${WEAVER_IMAGE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 python3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /filesystem-fixture /fs-fixture/filesystem
COPY run.py smoke.py regressions.py /fs-fixture/
RUN if [ -f /entrypoint.sh ]; then mv /entrypoint.sh /fs-fixture/product-entrypoint.sh; fi
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENV WEAVER_FS_PROFILE=unraid
ENTRYPOINT ["python3", "/fs-fixture/run.py"]
CMD ["--mount", "/fs", "--", "python3", "/fs-fixture/smoke.py", "/fs"]

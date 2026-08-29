FROM golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19 AS build

WORKDIR /src
COPY go.mod ./
COPY cmd/nntpshaper ./cmd/nntpshaper
COPY internal/nntpshaper ./internal/nntpshaper
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w -buildid=' -o /out/nntpshaper ./cmd/nntpshaper

FROM scratch
COPY --from=build /out/nntpshaper /nntpshaper
EXPOSE 119 563
ENTRYPOINT ["/nntpshaper"]

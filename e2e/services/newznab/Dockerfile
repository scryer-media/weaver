FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /newznab-api .

FROM alpine:3.20
COPY --from=build /newznab-api /usr/local/bin/newznab-api
EXPOSE 8088
CMD ["newznab-api"]

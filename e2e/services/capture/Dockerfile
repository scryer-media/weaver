# Packet capture and link control inside Weaver's network namespace, for the
# advanced-networking flows. Runs with network_mode service:weaver, so every
# interface it sees is one of Weaver's.
FROM alpine:latest@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
RUN apk add --no-cache tcpdump iproute2 python3
COPY capture.py /capture.py
EXPOSE 8099
ENTRYPOINT ["python3", "-u", "/capture.py"]

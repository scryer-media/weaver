# Packet capture and link control inside Weaver's network namespace, for the
# advanced-networking flows. Runs with network_mode service:weaver, so every
# interface it sees is one of Weaver's.
FROM alpine:latest@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache tcpdump iproute2 python3
COPY capture.py /capture.py
EXPOSE 8099
ENTRYPOINT ["python3", "-u", "/capture.py"]

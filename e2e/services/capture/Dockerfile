# Packet capture and link control inside Weaver's network namespace, for the
# advanced-networking flows. Runs with network_mode service:weaver, so every
# interface it sees is one of Weaver's.
FROM alpine:latest@sha256:3f4d6b32e26703c6082839234f43769d5046b3ac7c910033911d1e71d8cd630a
RUN apk add --no-cache tcpdump iproute2 python3
COPY capture.py /capture.py
EXPOSE 8099
ENTRYPOINT ["python3", "-u", "/capture.py"]

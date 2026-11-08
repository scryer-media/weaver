# Packet capture and link control inside Weaver's network namespace, for the
# advanced-networking flows. Runs with network_mode service:weaver, so every
# interface it sees is one of Weaver's.
FROM alpine:latest@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
RUN apk add --no-cache tcpdump iproute2 python3
COPY capture.py /capture.py
EXPOSE 8099
ENTRYPOINT ["python3", "-u", "/capture.py"]

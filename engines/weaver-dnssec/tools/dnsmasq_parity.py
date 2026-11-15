#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

"""Compare exported signed fixtures with an isolated, loopback-only dnsmasq.

Generate fresh fixtures with the parity_fixtures example immediately before use.
This operator-invoked diagnostic owns and tears down every process it starts.
It never connects to an existing dnsmasq instance or changes system DNS settings.
"""

import argparse
import json
import re
import socket
import socketserver
import struct
import subprocess
import threading
import time
from pathlib import Path


def question(packet):
    if len(packet) < 12 or struct.unpack_from("!H", packet, 4)[0] != 1:
        raise ValueError("expected one DNS question")
    offset, labels = 12, []
    while True:
        length = packet[offset]
        offset += 1
        if length == 0:
            break
        if length > 63 or offset + length > len(packet):
            raise ValueError("fixture server expects an uncompressed question")
        labels.append(packet[offset : offset + length].decode("ascii"))
        offset += length
    rtype, rclass = struct.unpack_from("!HH", packet, offset)
    if rclass != 1:
        raise ValueError("expected IN class")
    return ".".join(labels).lower() + ".", rtype, offset + 4


def read_exact(stream, length):
    result = bytearray()
    while len(result) < length:
        data = stream.recv(length - len(result))
        if not data:
            raise EOFError("short DNS TCP frame")
        result.extend(data)
    return bytes(result)


def name_at(packet, offset):
    labels, end, seen = [], None, set()
    while offset not in seen:
        seen.add(offset)
        length = packet[offset]
        offset += 1
        if length & 0xC0 == 0xC0:
            end = end if end is not None else offset + 1
            offset = ((length & 63) << 8) | packet[offset]
        elif length == 0:
            return ".".join(labels).lower() + ".", end if end is not None else offset
        elif length <= 63 and offset + length <= len(packet):
            labels.append(packet[offset : offset + length].decode("ascii"))
            offset += length
        else:
            raise ValueError("invalid name in parity response")
    raise ValueError("name compression loop in parity response")


def answer_records(packet):
    """Compare the A/AAAA and alias data used by these diagnostic cases."""
    _, _, offset = question(packet)
    records = []
    for _ in range(struct.unpack_from("!H", packet, 6)[0]):
        owner, offset = name_at(packet, offset)
        rtype, rclass, _, length = struct.unpack_from("!HHIH", packet, offset)
        offset += 10
        end = offset + length
        if end > len(packet):
            raise ValueError("short answer record")
        if rtype in (5, 39):
            data, consumed = name_at(packet, offset)
            if consumed != end:
                raise ValueError("invalid alias RDATA length")
        else:
            data = packet[offset:end].hex()
        if rtype != 46:
            records.append((owner, rtype, rclass, data))
        offset = end
    return sorted(records)


def encode_query(name, rtype):
    wire = b"".join(bytes([len(label)]) + label.encode("ascii") for label in name.rstrip(".").split(".") if label) + b"\0"
    # RD and AD requested, CD clear; DO in the OPT record.
    return struct.pack("!6H", 0x3141, 0x0120, 1, 0, 0, 1) + wire + struct.pack("!HH", rtype, 1) + bytes.fromhex("00002904d0000080000000")


class FixtureServer:
    def __init__(self, records):
        self.records = records
        self.requests = []
        self.missing = []
        self.lock = threading.Lock()
        owner = self

        class TCP(socketserver.ThreadingTCPServer):
            daemon_threads = True

        class UDP(socketserver.ThreadingUDPServer):
            daemon_threads = True

        class TCPHandler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(10)
                try:
                    while True:
                        length = struct.unpack("!H", read_exact(self.request, 2))[0]
                        packet = read_exact(self.request, length)
                        reply = owner.answer(packet, "tcp")
                        self.request.sendall(struct.pack("!H", len(reply)) + reply)
                except (EOFError, OSError):
                    pass

        class UDPHandler(socketserver.BaseRequestHandler):
            def handle(self):
                packet, stream = self.request
                stream.sendto(owner.answer(packet, "udp"), self.client_address)

        self.tcp = TCP(("127.0.0.1", 0), TCPHandler)
        self.port = self.tcp.server_address[1]
        self.udp = UDP(("127.0.0.1", self.port), UDPHandler)
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in (self.tcp, self.udp)]
        for thread in self.threads:
            thread.start()

    def answer(self, packet, transport):
        name, rtype, end = question(packet)
        with self.lock:
            self.requests.append({"name": name, "rtype": rtype, "transport": transport})
            response = self.records.get((name, rtype))
            if response is None:
                self.missing.append({"name": name, "rtype": rtype})
        if response is None:
            return packet[:2] + struct.pack("!5H", 0x8182, 1, 0, 0, 0) + packet[12:end]
        if transport == "udp" and len(response) > 512:
            # Force a genuine TCP retry for large signatures, without changing
            # the signed response served over TCP.
            return packet[:2] + struct.pack("!5H", 0x8380, 1, 0, 0, 0) + packet[12:end]
        reply = bytearray(response)
        reply[:2] = packet[:2]
        reply[3] &= ~0x20  # The upstream fixture never asserts authentication.
        return bytes(reply)

    def close(self):
        for server in (self.tcp, self.udp):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join()


def free_port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def run_case(binary, case, records, directory):
    directory.mkdir(parents=True, exist_ok=True)
    server = FixtureServer(records)
    process = None
    log_thread = None
    log_lines = []
    result = {}
    ready = threading.Event()
    port = free_port()
    config = [
        f"port={port}", "listen-address=127.0.0.1", "bind-interfaces",
        "no-resolv", "no-hosts", f"server=127.0.0.1#{server.port}",
        "dnssec", "dnssec-check-unsigned", "log-queries=extra", "log-facility=-",
        "cache-size=1000", "pid-file=daemon.pid",
    ]
    for value in case["anchors"]:
        raw = bytes.fromhex(value)
        tag, algorithm, digest = struct.unpack_from("!HBB", raw)
        config.append(f"trust-anchor=.,{tag},{algorithm},{digest},{raw[4:].hex()}")
    (directory / "dnsmasq.conf").write_text("\n".join(config) + "\n")
    try:
        process = subprocess.Popen(
            [str(binary), "--no-daemon", "--conf-file=dnsmasq.conf"],
            cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )

        def drain_log():
            with (directory / "dnsmasq.log").open("w") as output:
                for line in process.stderr:
                    output.write(line)
                    output.flush()
                    log_lines.append(line)
                    if "cleared cache" in line:
                        ready.set()
                ready.set()

        log_thread = threading.Thread(target=drain_log, daemon=True)
        log_thread.start()
        if not ready.wait(10) or process.poll() is not None:
            raise RuntimeError("dnsmasq failed to become ready: " + "".join(log_lines))
        request = encode_query(case["name"], case["rtype"])
        with socket.create_connection(("127.0.0.1", port), timeout=15) as client:
            client.sendall(struct.pack("!H", len(request)) + request)
            length = struct.unpack("!H", read_exact(client, 2))[0]
            reply = read_exact(client, length)
        if reply[:2] != request[:2] or question(reply)[:2] != question(request)[:2]:
            raise RuntimeError("dnsmasq response question or ID mismatch")
        if server.missing:
            raise RuntimeError(f"fixture coverage incomplete: {server.missing}")
        age = int(time.time()) - case["now"]
        if age < 0 or (case["id"] == "not-yet-60s" and age >= 60):
            raise RuntimeError("fixture clock crossed the signature boundary; regenerate fresh fixtures")
        flags = struct.unpack_from("!H", reply, 2)[0]
        rcode = flags & 15
        ad = bool(flags & 0x20)
        status = "Secure" if rcode in (0, 3) and ad else "Insecure" if rcode in (0, 3) else "ServFail" if rcode == 2 else f"Rcode{rcode}"
        expected = records[(case["name"].lower(), case["rtype"])]
        actual_records = answer_records(reply)
        local_outcome = case.get("outcome")
        outcome_equal = None
        if local_outcome is not None:
            if local_outcome["kind"] == "Records":
                selected = sorted(data for owner, rtype, rclass, data in actual_records
                                  if owner == local_outcome["name"].lower() and rtype == case["rtype"] and rclass == 1)
                outcome_equal = rcode == 0 and selected == sorted(local_outcome["rdata"])
            else:
                expected_rcode = 3 if local_outcome["kind"] == "NoSuchName" else 0
                outcome_equal = rcode == expected_rcode and not any(rtype == case["rtype"] for _, rtype, _, _ in actual_records)
        result.update({
            "case": case["id"], "intended": case.get("intended"),
            "name": case["name"], "rtype": case["rtype"],
            "fixture_age_seconds": age,
            "weaver": case["verdict"], "weaver_reason": case.get("reason"),
            "weaver_ttl": case.get("ttl"), "dnsmasq": status, "ad": ad, "rcode": rcode,
            "weaver_outcome": local_outcome, "outcome_equal": outcome_equal,
            "query_hex": request.hex(), "reply_hex": reply.hex(),
            "upstream_queries": server.requests,
            "answer_records_equal": actual_records == answer_records(expected),
            "answer_records": actual_records,
            "fixture_rcode": expected[3] & 15,
        })
        return result
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if log_thread is not None:
                log_thread.join()
            if process.stderr is not None:
                process.stderr.close()
        validation_lines = [line.strip() for line in log_lines if re.search(r"validation.*(?: is |result is )(?:SECURE|INSECURE|BOGUS|ABANDONED)", line)]
        result["validation_log"] = validation_lines
        if validation_lines:
            match = re.search(r"\b(SECURE|INSECURE|BOGUS|ABANDONED)\b", validation_lines[-1])
            result["dnsmasq_validation"] = match.group(1).title()
        server.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dnsmasq", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases, records = {}, {}
    for line in args.fixtures.read_text().splitlines():
        item = json.loads(line)
        if item["event"] == "case":
            cases[item["id"]] = item
        elif item["event"] == "reply":
            records.setdefault(item["case"], {})[(item["name"].lower(), item["rtype"])] = bytes.fromhex(item["hex"])
        else:
            raise ValueError("unknown fixture event")
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "results.jsonl").open("w") as output:
        for index, case in enumerate(cases.values()):
            result = run_case(args.dnsmasq.resolve(), case, records[case["id"]], args.output.resolve() / f"case-{index:02}")
            output.write(json.dumps(result) + "\n")
            output.flush()
            print(json.dumps({k: v for k, v in result.items() if k not in ("query_hex", "reply_hex", "upstream_queries")}), flush=True)


if __name__ == "__main__":
    main()

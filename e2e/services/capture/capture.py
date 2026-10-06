"""Capture control for the advanced-networking e2e flows.

POST /start {"name"}           tcpdump on every non-loopback interface into
                               /artifacts/captures/<name>-<iface>.pcap; answers
                               once every capture reports it is listening.
POST /stop                     stop every running capture.
GET  /count?name&iface[&filter] packets in that capture, optionally filtered.
GET  /interfaces               `ip -j addr` of the namespace.
POST /link {"iface", "up"}     `ip link set <iface> up|down`.

The control and API ports are excluded from captures so a count reflects
only the traffic under test.
"""

import json
import os
import re
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = os.environ.get("CAPTURE_DIR", "/artifacts/captures")
PORT = int(os.environ.get("CAPTURE_CONTROL_PORT", "8099"))
EXCLUDE = "not port {} and not port 9090 and not port 5432".format(PORT)
NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
IFACE = re.compile(r"^[A-Za-z0-9_.@-]{1,15}$")
FILTER = re.compile(r"^[A-Za-z0-9 .:/()-]{0,200}$")

running = []
lock = threading.Lock()


def interfaces():
    return sorted(name for name in os.listdir("/sys/class/net") if name != "lo")


def pcap_path(name, iface):
    return os.path.join(ROOT, "{}-{}.pcap".format(name, iface))


def start(name):
    os.makedirs(ROOT, exist_ok=True)
    started = []
    for iface in interfaces():
        process = subprocess.Popen(
            ["tcpdump", "-i", iface, "-nn", "-U", "-w", pcap_path(name, iface), EXCLUDE],
            stderr=subprocess.PIPE,
            text=True,
        )
        # tcpdump says "listening on <iface>" once the capture is armed.
        for line in process.stderr:
            if "listening on" in line:
                break
        else:
            raise RuntimeError("tcpdump on {} exited: {}".format(iface, process.wait()))
        threading.Thread(target=process.stderr.read, daemon=True).start()
        started.append({"iface": iface, "path": pcap_path(name, iface)})
        with lock:
            running.append(process)
    return started


def stop():
    with lock:
        processes = list(running)
        running.clear()
    for process in processes:
        process.terminate()
    for process in processes:
        process.wait()
    return len(processes)


def count(name, iface, expression):
    path = pcap_path(name, iface)
    if not os.path.exists(path):
        return None
    command = ["tcpdump", "-nn", "-r", path]
    if expression:
        command.append(expression)
    result = subprocess.run(command, capture_output=True, text=True)
    return len([line for line in result.stdout.splitlines() if line.strip()])


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_):
        pass

    def do_GET(self):
        url = urlparse(self.path)
        query = {key: values[0] for key, values in parse_qs(url.query).items()}
        if url.path == "/":
            return self.reply(200, {"interfaces": interfaces(), "running": len(running)})
        if url.path == "/interfaces":
            result = subprocess.run(["ip", "-j", "addr"], capture_output=True, text=True, check=True)
            return self.reply(200, json.loads(result.stdout))
        if url.path == "/count":
            name, iface, expression = query.get("name", ""), query.get("iface", ""), query.get("filter", "")
            if not NAME.match(name) or not IFACE.match(iface) or not FILTER.match(expression):
                return self.reply(400, {"error": "invalid name, iface or filter"})
            packets = count(name, iface, expression)
            if packets is None:
                return self.reply(404, {"error": "no capture"})
            return self.reply(200, {"name": name, "iface": iface, "packets": packets})
        return self.reply(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            return self.reply(400, {"error": "invalid json"})
        url = urlparse(self.path)
        try:
            if url.path == "/start":
                name = body.get("name", "")
                if not NAME.match(name):
                    return self.reply(400, {"error": "invalid name"})
                stop()
                return self.reply(200, {"captures": start(name)})
            if url.path == "/stop":
                return self.reply(200, {"stopped": stop()})
            if url.path == "/link":
                iface, up = body.get("iface", ""), body.get("up")
                if not IFACE.match(iface) or not isinstance(up, bool):
                    return self.reply(400, {"error": "iface and boolean up are required"})
                subprocess.run(["ip", "link", "set", iface, "up" if up else "down"], check=True)
                return self.reply(200, {"iface": iface, "up": up})
        except (RuntimeError, subprocess.CalledProcessError) as error:
            return self.reply(500, {"error": str(error)})
        return self.reply(404, {"error": "not found"})


if __name__ == "__main__":
    print("capture control ready", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

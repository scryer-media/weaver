// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Loopback BitTorrent tracker for the torrent lane. Adapted from the Go
// tracker in the scryer-media e2e harness (services/tracker/main.go): the same
// announce handling and compact peer lists, kept in memory, plus a UDP
// announce endpoint (BEP 15) and an event log the harness reads to see who
// announced what, from which address, and when.
import http from "node:http";
import dgram from "node:dgram";
import crypto from "node:crypto";
import { pathToFileURL } from "node:url";

const PEER_TTL_MS = 5 * 60 * 1000;
const EVENT_LIMIT = 50000;

function bencode(value) {
  if (Buffer.isBuffer(value)) return Buffer.concat([Buffer.from(`${value.length}:`), value]);
  if (typeof value === "string") return bencode(Buffer.from(value));
  if (typeof value === "number") return Buffer.from(`i${Math.trunc(value)}e`);
  const keys = Object.keys(value).sort();
  return Buffer.concat([Buffer.from("d"), ...keys.flatMap(key => [bencode(key), bencode(value[key])]), Buffer.from("e")]);
}

// The raw bytes of a percent-encoded query value; info_hash and peer_id are
// binary, so URLSearchParams' UTF-8 decoding would mangle them.
export function rawQueryBytes(search, name) {
  for (const part of search.replace(/^\?/, "").split("&")) {
    const [key, value = ""] = part.split("=");
    if (key !== name) continue;
    const out = [];
    for (let index = 0; index < value.length; index++) {
      const char = value[index];
      if (char === "%" && index + 2 < value.length + 1 && /^[0-9a-fA-F]{2}$/.test(value.slice(index + 1, index + 3))) {
        out.push(parseInt(value.slice(index + 1, index + 3), 16)); index += 2;
      } else if (char === "+") out.push(0x20);
      else out.push(...Buffer.from(char));
    }
    return Buffer.from(out);
  }
  return null;
}

function ipv4Of(address) {
  const text = String(address ?? "").replace(/^::ffff:/, "");
  return /^\d+\.\d+\.\d+\.\d+$/.test(text) ? text : null;
}

// A peer id is printable when it carries a client prefix such as -WV0170-.
function printablePeerId(peerId) {
  return [...peerId].map(byte => (byte >= 0x20 && byte < 0x7f ? String.fromCharCode(byte) : ".")).join("");
}

export async function startTracker(options = {}) {
  const httpPort = options.httpPort ?? Number(process.env.TRACKER_HTTP_PORT ?? 8080);
  const udpPort = options.udpPort ?? Number(process.env.TRACKER_UDP_PORT ?? 8081);
  const interval = options.interval ?? Number(process.env.TRACKER_INTERVAL_SECONDS ?? 5);
  const swarms = new Map();
  const events = [];
  let sequence = 0;

  function announce({ transport, infohash, peerId, ip, port, event, left, uploaded, downloaded }) {
    const now = Date.now();
    const infohashHex = infohash.toString("hex");
    const peerKey = peerId.toString("hex");
    events.push({
      sequence: ++sequence, at: now, transport, infohash: infohashHex, peerId: peerKey,
      client: printablePeerId(peerId).slice(0, 8), ip, port, event: event || "", left, uploaded, downloaded,
    });
    if (events.length > EVENT_LIMIT) events.splice(0, events.length - EVENT_LIMIT);
    let peers = swarms.get(infohashHex);
    if (!peers) { peers = new Map(); swarms.set(infohashHex, peers); }
    for (const [key, peer] of peers) if (now - peer.lastSeen > PEER_TTL_MS) peers.delete(key);
    if (event === "stopped") peers.delete(peerKey);
    else peers.set(peerKey, { ip, port, left, lastSeen: now });
    let complete = 0;
    let incomplete = 0;
    const list = [];
    for (const [key, peer] of peers) {
      if (peer.left === 0) complete++; else incomplete++;
      if (key === peerKey || !ipv4Of(peer.ip)) continue;
      list.push(peer);
    }
    list.sort((a, b) => (a.ip === b.ip ? a.port - b.port : a.ip < b.ip ? -1 : 1));
    const compact = Buffer.alloc(list.length * 6);
    list.forEach((peer, index) => {
      ipv4Of(peer.ip).split(".").forEach((octet, at) => compact.writeUInt8(Number(octet), index * 6 + at));
      compact.writeUInt16BE(peer.port, index * 6 + 4);
    });
    return { complete, incomplete, compact };
  }

  const server = http.createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://tracker.invalid");
    if (url.pathname === "/health") { response.setHeader("content-type", "application/json"); response.end('{"status":"ok"}'); return; }
    if (url.pathname === "/events") {
      const after = Number(url.searchParams.get("after") ?? 0);
      if (!Number.isInteger(after) || after < 0) { response.writeHead(400).end(); return; }
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ sequence, events: events.filter(item => item.sequence > after) }));
      return;
    }
    if (url.pathname !== "/announce") { response.writeHead(404).end(); return; }
    const search = (request.url ?? "").split("?")[1] ?? "";
    const infohash = rawQueryBytes(search, "info_hash");
    const peerId = rawQueryBytes(search, "peer_id");
    const port = Number(url.searchParams.get("port"));
    const fail = reason => { response.setHeader("content-type", "text/plain"); response.end(bencode({ "failure reason": reason })); };
    if (!infohash || infohash.length !== 20) { fail("invalid info_hash"); return; }
    if (!peerId || peerId.length !== 20) { fail("invalid peer_id"); return; }
    if (!Number.isInteger(port) || port < 1 || port > 65535) { fail("invalid port"); return; }
    const ip = ipv4Of(request.socket.remoteAddress) ?? String(request.socket.remoteAddress);
    const result = announce({
      transport: "http", infohash, peerId, ip, port,
      event: (url.searchParams.get("event") ?? "").toLowerCase(),
      left: Number(url.searchParams.get("left") ?? 0),
      uploaded: Number(url.searchParams.get("uploaded") ?? 0),
      downloaded: Number(url.searchParams.get("downloaded") ?? 0),
    });
    response.setHeader("content-type", "text/plain");
    response.end(bencode({ interval, "min interval": interval, complete: result.complete, incomplete: result.incomplete, peers: result.compact }));
  });

  // BEP 15: connect, then announce with the connection id handed out.
  const UDP_EVENTS = ["", "completed", "started", "stopped"];
  const connections = new Map();
  const udp = dgram.createSocket("udp4");
  udp.on("message", (message, remote) => {
    if (message.length < 16) return;
    const action = message.readUInt32BE(8);
    const transaction = message.subarray(12, 16);
    if (action === 0 && message.readBigUInt64BE(0) === 0x41727101980n) {
      const id = crypto.randomBytes(8);
      connections.set(id.toString("hex"), Date.now());
      for (const [key, at] of connections) if (Date.now() - at > 120000) connections.delete(key);
      udp.send(Buffer.concat([Buffer.from([0, 0, 0, 0]), transaction, id]), remote.port, remote.address);
      return;
    }
    if (action !== 1 || message.length < 98 || !connections.has(message.subarray(0, 8).toString("hex"))) return;
    const announced = message.readUInt16BE(96);
    const result = announce({
      transport: "udp",
      infohash: Buffer.from(message.subarray(16, 36)),
      peerId: Buffer.from(message.subarray(36, 56)),
      ip: remote.address,
      port: announced || remote.port,
      downloaded: Number(message.readBigUInt64BE(56)),
      left: Number(message.readBigUInt64BE(64)),
      uploaded: Number(message.readBigUInt64BE(72)),
      event: UDP_EVENTS[message.readUInt32BE(80)] ?? "",
    });
    const head = Buffer.alloc(20);
    head.writeUInt32BE(1, 0);
    transaction.copy(head, 4);
    head.writeUInt32BE(interval, 8);
    head.writeUInt32BE(result.incomplete, 12);
    head.writeUInt32BE(result.complete, 16);
    udp.send(Buffer.concat([head, result.compact]), remote.port, remote.address);
  });

  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(httpPort, "0.0.0.0", resolve); });
  await new Promise((resolve, reject) => { udp.once("error", reject); udp.bind(udpPort, "0.0.0.0", resolve); });
  return {
    httpPort: server.address().port,
    udpPort: udp.address().port,
    events,
    async close() { udp.close(); server.closeAllConnections?.(); await new Promise(resolve => server.close(resolve)); },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const tracker = await startTracker();
  console.log(`torrent tracker ready on ${tracker.httpPort}/tcp and ${tracker.udpPort}/udp`);
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => { await tracker.close(); process.exit(0); });
}

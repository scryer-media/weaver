import assert from "node:assert/strict";
import { test } from "node:test";
import { once } from "node:events";
import net from "node:net";
import dgram from "node:dgram";
import { startProxyFixture } from "../tests/support/proxy-fixture-server.mjs";
import { startAPIRelay, startWireGuardRelays } from "../tests/support/real-network-relay.mjs";

test("private API relay preserves concurrent streams without recording request bodies", async () => {
  const sockets = new Set();
  const origin = net.createServer(socket => {
    sockets.add(socket); socket.on("close", () => sockets.delete(socket));
    socket.on("data", data => socket.write(data));
  });
  origin.listen(0, "127.0.0.1"); await once(origin, "listening");
  const relay = await startAPIRelay({ host: "127.0.0.1", port: origin.address().port, listenPort: 0 });
  try {
    await Promise.all(Array.from({ length: 4 }, async (_, index) => {
      const client = net.connect(relay.port, "127.0.0.1");
      const marker = Buffer.alloc(4096, index + 1);
      let received = Buffer.alloc(0);
      const arrived = new Promise((resolve, reject) => {
        client.on("error", reject);
        client.on("data", chunk => {
          received = Buffer.concat([received, chunk]);
          if (received.length >= marker.length) resolve(received);
        });
      });
      client.write(marker);
      try { assert.deepEqual(await arrived, marker); } finally { client.destroy(); }
    }));
  } finally {
    await relay.close();
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => origin.close(resolve));
  }
});

test("real Usenet relays forward CONNECT and SOCKS only to the configured destination", async () => {
  const clients = new Set();
  const origin = net.createServer(socket => {
    clients.add(socket); socket.on("close", () => clients.delete(socket));
    socket.on("data", data => socket.write(data));
  });
  origin.listen(0, "127.0.0.1"); await once(origin, "listening");
  const port = origin.address().port;
  const fixture = await startProxyFixture({
    ip: "127.0.0.1", publicTargets: [{ host: "news.example.com", port, addresses: ["127.0.0.1"] }],
    ports: { primary: 0, secondary: 0, tertiary: 0, dns: 0, nntp: 0, http: 0, control: 0 },
  });
  const exchange = async (proxyPort, handshake, marker) => {
    const client = net.connect(proxyPort, "127.0.0.1");
    const chunks = [];
    const arrived = new Promise((resolve, reject) => {
      client.on("error", reject);
      client.on("data", data => {
        chunks.push(data);
        if (Buffer.concat(chunks).includes(Buffer.from(marker))) resolve(Buffer.concat(chunks));
      });
    });
    client.write(Buffer.concat([handshake, Buffer.from(marker)]));
    try { return await arrived; } finally { client.destroy(); }
  };
  try {
    const auth = Buffer.from("fixture:fixture").toString("base64");
    const connect = await exchange(fixture.ports.primary,
      Buffer.from(`CONNECT news.example.com:${port} HTTP/1.1\r\nProxy-Authorization: Basic ${auth}\r\n\r\n`), "connect-fixture-payload");
    assert.match(connect.toString(), /200 Connection Established/);
    const host = Buffer.from("news.example.com"), destinationPort = Buffer.alloc(2);
    destinationPort.writeUInt16BE(port);
    const socks = await exchange(fixture.ports.secondary, Buffer.concat([
      Buffer.from([5, 1, 2, 1, 7]), Buffer.from("fixture"), Buffer.from([7]), Buffer.from("fixture"),
      Buffer.from([5, 1, 0, 3, host.length]), host, destinationPort,
    ]), "socks-fixture-payload");
    assert.deepEqual([...socks.subarray(0, 4)], [5, 2, 1, 0]);
    for (const [host, deniedPort] of [["other.example.com", port], ["news.example.com", fixture.ports.http]]) {
      const client = net.connect(fixture.ports.primary, "127.0.0.1");
      const chunks = [];
      client.on("data", data => chunks.push(data));
      const closed = once(client, "close");
      client.write(`CONNECT ${host}:${deniedPort} HTTP/1.1\r\nProxy-Authorization: Basic ${auth}\r\n\r\n`);
      await closed;
      assert.match(Buffer.concat(chunks).toString(), /502 Bad Gateway/);
    }
    assert.equal(fixture.events.filter(event => event.kind === "connected").length, 2);
  } finally {
    await fixture.close();
    for (const client of clients) client.destroy();
    await new Promise(resolve => origin.close(resolve));
  }
});

test("WireGuard UDP relays preserve payloads and isolate concurrent clients", async () => {
  const peer = dgram.createSocket("udp4");
  const sources = new Set();
  peer.on("message", (packet, remote) => {
    sources.add(`${remote.address}:${remote.port}`);
    peer.send(packet, remote.port, remote.address);
  });
  peer.bind(0, "127.0.0.1"); await once(peer, "listening");
  const relay = await startWireGuardRelays([{ host: "127.0.0.1", port: peer.address().port, listenPort: 0 }]);
  const clients = Array.from({ length: 4 }, () => dgram.createSocket("udp4"));
  try {
    await Promise.all(clients.map(async (client, index) => {
      const payload = Buffer.alloc(1216, index+1);
      const arrived = once(client, "message");
      client.send(payload, relay.ports[0], "127.0.0.1");
      assert.deepEqual((await arrived)[0], payload);
    }));
    assert.equal(sources.size, 4);
  } finally {
    for (const client of clients) client.close();
    await relay.close(); peer.close();
  }
});

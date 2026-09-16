import assert from "node:assert/strict";
import { test } from "node:test";
import net from "node:net";
import dgram from "node:dgram";
import { once } from "node:events";
import { startProxyFixture } from "../tests/support/proxy-fixture-server.mjs";

test("canary answers destination DNS locally and records host queries", async () => {
  const fixture = await startProxyFixture({ ip: "127.0.0.1", ports: { primary: 0, secondary: 0, tertiary: 0, dns: 0, nntp: 0, http: 0, control: 0 } });
  const socket = dgram.createSocket("udp4");
  try {
    const header = Buffer.from([0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    const question = Buffer.concat([...["news", "proxy", "test"].map(label => Buffer.concat([Buffer.from([label.length]), Buffer.from(label)])), Buffer.from([0, 0, 1, 0, 1])]);
    const response = once(socket, "message");
    socket.send(Buffer.concat([header, question]), fixture.ports.dnsUdp, "127.0.0.1");
    const [body] = await response;
    assert.equal(body.readUInt16BE(6), 1);
    assert.deepEqual([...body.subarray(-4)], [127, 0, 0, 1]);
    assert.equal(fixture.events.filter(event => event.kind === "direct-dns").length, 1);
  } finally { socket.close(); await fixture.close(); }
});

test("HTTP CONNECT preserves pipelined data and refuses non-fixture targets", async () => {
  const fixture = await startProxyFixture({ ip: "127.0.0.1", ports: { primary: 0, secondary: 0, tertiary: 0, dns: 0, nntp: 0, http: 0, control: 0 } });
  try {
    const request = async (host, port, payload = "", authorization = Buffer.from("fixture:fixture").toString("base64")) => {
      const socket = net.connect(fixture.ports.primary, "127.0.0.1");
      socket.setTimeout(3000, () => socket.destroy(new Error("fixture timeout")));
      const chunks = [];
      socket.on("data", chunk => chunks.push(chunk));
      socket.write(`CONNECT ${host}:${port} HTTP/1.1\r\nProxy-Authorization: Basic ${authorization}\r\n\r\n${payload}`);
      await once(socket, "close");
      return Buffer.concat(chunks).toString();
    };
    const good = await request("news.proxy.test", fixture.ports.http, "GET /feed.xml HTTP/1.1\r\nHost: news.proxy.test\r\nConnection: close\r\n\r\n");
    assert.match(good, /200 Connection Established/);
    assert.match(good, /<rss/);
    assert.match(await request("unrelated.invalid", 443), /502 Bad Gateway/);
    assert.match(await request("news.proxy.test", fixture.ports.http, "", Buffer.from("fixture:fixture").toString("base64").toUpperCase()), /407 Proxy Authentication Required/);
    assert.equal(fixture.events.filter(event => event.kind === "connected").length, 1);
  } finally { await fixture.close(); }
});

test("SOCKS5 accepts a pipelined authenticated handshake", async () => {
  const fixture = await startProxyFixture({ ip: "127.0.0.1", ports: { primary: 0, secondary: 0, tertiary: 0, dns: 0, nntp: 0, http: 0, control: 0 } });
  try {
    const socket = net.connect(fixture.ports.secondary, "127.0.0.1");
    socket.setTimeout(3000, () => socket.destroy(new Error("fixture timeout")));
    const port = Buffer.alloc(2); port.writeUInt16BE(fixture.ports.http);
    const chunks = [];
    socket.on("data", chunk => chunks.push(chunk));
    socket.write(Buffer.concat([
      Buffer.from([5, 1, 2, 1, 7]), Buffer.from("fixture"), Buffer.from([7]), Buffer.from("fixture"),
      Buffer.from([5, 1, 0, 1, 127, 0, 0, 1]), port,
      Buffer.from("GET /feed.xml HTTP/1.1\r\nHost: news.proxy.test\r\nConnection: close\r\n\r\n"),
    ]));
    await once(socket, "close");
    const body = Buffer.concat(chunks);
    assert.deepEqual([...body.subarray(0, 4)], [5, 2, 1, 0]);
    assert.match(body.toString(), /<rss/);
    assert.equal(fixture.events.find(event => event.kind === "connected")?.route, "secondary");
  } finally { await fixture.close(); }
});

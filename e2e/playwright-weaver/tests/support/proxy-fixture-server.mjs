// Isolated e2e infrastructure, deliberately limited to the fixture destinations.
// No arbitrary forwarding, subscriptions, credentials, or internet DNS.
import net from "node:net";
import http from "node:http";
import dgram from "node:dgram";
import { pathToFileURL } from "node:url";

export async function startProxyFixture(options = {}) {
  const ip = options.ip ?? process.env.PROXY_FIXTURE_IP;
  if (!net.isIPv4(ip)) throw new Error("PROXY_FIXTURE_IP must be an IPv4 address");
  const ports = { primary: 8081, secondary: 8082, tertiary: 8083, dns: 53, nntp: 119, http: 8089, control: 8090, ...options.ports };
  const events = [];
  const sockets = new Set();
  const routes = Object.fromEntries(["primary", "secondary", "tertiary", "direct"].map(name => [name, { up: true, hold: false, sockets: new Set() }]));
  const servers = [];
  const held = new Set();
  let sequence = 0;
  let feedToken = "initial";
  let nzb = "";
  const record = (kind, fields = {}) => {
    if (events.length >= 20000) throw new Error("proxy fixture event limit exceeded");
    events.push({ sequence: ++sequence, at: Date.now(), kind, ...fields });
  };
  const own = socket => {
    if (sockets.size >= 256) { socket.destroy(); return; }
    sockets.add(socket);
    socket.setTimeout(120000, () => socket.destroy());
    socket.on("error", () => socket.destroy());
    socket.on("close", () => { sockets.delete(socket); held.delete(socket); });
    return socket;
  };
  async function listen(server, key) {
    servers.push(server);
    await new Promise((resolve, reject) => { server.once("error", reject); server.listen(ports[key], "0.0.0.0", resolve); });
    ports[key] = server.address().port;
  }
  function pipe(client, host, port, route) {
    const upstream = own(net.connect({ host, port }));
    if (!upstream) { client.destroy(); return; }
    if (route) {
      routes[route].sockets.add(client);
      routes[route].sockets.add(upstream);
      client.on("close", () => routes[route].sockets.delete(client));
      upstream.on("close", () => routes[route].sockets.delete(upstream));
    }
    client.once("close", () => upstream.destroy());
    upstream.once("close", () => client.destroy());
    client.pipe(upstream);
    upstream.on("data", chunk => {
      if (route && port === (options.nntpPort ?? 119)) record("nntp-bytes", { route, bytes: chunk.length });
      if (!client.write(chunk)) { upstream.pause(); client.once("drain", () => { if (!held.has(upstream)) upstream.resume(); }); }
      // Let the greeting/auth/capability exchange complete, then hold the body
      // after the first fragment. The test can now cut a genuinely active stream.
      if (route && routes[route].hold && chunk.includes(Buffer.from("222 "))) {
        held.add(upstream);
        upstream.pause();
        record("held-body", { route });
      }
    });
    return upstream;
  }
  function forward(client, route, host, port, reply) {
    record("attempt", { route, host, port });
    const allowedHost = host === ip || /^[a-z0-9-]+\.proxy\.test$/.test(host);
    if (!allowedHost || ![ports.dns, ports.nntp, ports.http].includes(port) || !routes[route].up) {
      reply(false); client.end(); return;
    }
    record("connected", { route, host, port });
    reply(true);
    const targetHost = port === ports.nntp ? (options.nntpHost ?? "nntp") : "127.0.0.1";
    const targetPort = port === ports.nntp ? (options.nntpPort ?? 119) : port;
    pipe(client, targetHost, targetPort, route);
    client.resume();
  }
  // Read bounded handshake bytes without losing bytes pipelined after CONNECT.
  function reader(socket) {
    let buffer = Buffer.alloc(0);
    let consume;
    let finished = false;
    const onData = chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length > 16384) { socket.destroy(); return; }
      consume?.();
    };
    socket.on("data", onData);
    return {
      stage(fn) { consume = () => { while (!finished) { const used = fn(buffer); if (!used) break; buffer = buffer.subarray(used); } }; consume(); },
      finish(used) {
        finished = true;
        socket.pause(); socket.off("data", onData);
        const rest = buffer.subarray(used);
        if (rest.length) socket.unshift(rest);
      },
    };
  }
  for (const route of ["primary", "tertiary"]) {
    await listen(net.createServer(client => {
      if (!own(client)) return;
      const input = reader(client);
      input.stage(buffer => {
        const end = buffer.indexOf("\r\n\r\n");
        if (end < 0) return;
        const header = buffer.subarray(0, end).toString();
        const match = /^CONNECT ([a-zA-Z0-9.-]+):(\d+) HTTP\/1\.[01]\r\n/.exec(header + "\r\n");
        const authorization = /^proxy-authorization:\s*Basic\s+(\S+)\s*$/im.exec(header)?.[1];
        input.finish(end + 4);
        if (!match || authorization !== Buffer.from("fixture:fixture").toString("base64")) {
          client.end("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"); return;
        }
        forward(client, route, match[1], Number(match[2]), ok => client.write(`HTTP/1.1 ${ok ? "200 Connection Established" : "502 Bad Gateway"}\r\n\r\n`));
      });
    }), route);
  }
  await listen(net.createServer(client => {
    if (!own(client)) return;
    const input = reader(client);
    let stage = 0;
    input.stage(buffer => {
      if (stage === 0) {
        if (buffer.length < 2 || buffer.length < 2 + buffer[1]) return;
        if (buffer[0] !== 5 || !buffer.subarray(2, 2 + buffer[1]).includes(2)) { client.destroy(); return; }
        stage = 1; client.write(Buffer.from([5, 2])); return 2 + buffer[1];
      }
      if (stage === 1) {
        if (buffer.length < 2 || buffer.length < 3 + buffer[1]) return;
        const end = 3 + buffer[1] + buffer[2 + buffer[1]];
        if (buffer.length < end) return;
        if (buffer[0] !== 1 || buffer.subarray(2, 2 + buffer[1]).toString() !== "fixture" || buffer.subarray(3 + buffer[1], end).toString() !== "fixture") { client.destroy(); return; }
        stage = 2; client.write(Buffer.from([1, 0])); return end;
      }
      if (buffer.length < 5) return;
      const length = buffer[3] === 1 ? 4 : buffer[3] === 3 ? buffer[4] : 0;
      const start = buffer[3] === 3 ? 5 : 4;
      if (!length || buffer[0] !== 5 || buffer[1] !== 1) { client.destroy(); return; }
      if (buffer.length < start + length + 2) return;
      const host = buffer[3] === 1 ? [...buffer.subarray(start, start + length)].join(".") : buffer.subarray(start, start + length).toString();
      const port = buffer.readUInt16BE(start + length);
      input.finish(start + length + 2);
      forward(client, "secondary", host, port, ok => client.write(Buffer.from([5, ok ? 0 : 5, 0, 1, 0, 0, 0, 0, 0, 0])));
    });
  }), "secondary");
  await listen(net.createServer(client => {
    if (!own(client)) return;
    record("direct-nntp");
    pipe(client, options.nntpHost ?? "nntp", options.nntpPort ?? 119, "direct");
  }), "nntp");
  async function dnsResponse(query, direct) {
    if (query.length < 17) return;
    let cursor = 12;
    const labels = [];
    while (cursor < query.length && query[cursor]) {
      const length = query[cursor++];
      if (length > 63 || cursor + length >= query.length) return;
      labels.push(query.subarray(cursor, cursor + length).toString()); cursor += length;
    }
    if (cursor + 5 > query.length) return;
    const name = labels.join(".").toLowerCase();
    const type = query.readUInt16BE(cursor + 1);
    const fixture = name.endsWith(".proxy.test");
    // Resolve only infrastructure names through Docker's embedded resolver.
    // Every destination name above is answered locally, even when direct.
    if (["nntp", "nntp2", "weaver-postgres"].includes(name)) {
      return await new Promise(resolve => {
        const resolver = dgram.createSocket("udp4");
        const timer = setTimeout(() => { resolver.close(); resolve(undefined); }, 2000);
        const finish = answer => { clearTimeout(timer); resolver.close(); resolve(answer); };
        resolver.once("message", finish);
        resolver.once("error", () => finish(undefined));
        resolver.send(query, 53, "127.0.0.11");
      });
    }
    if (fixture) record(direct ? "direct-dns" : "routed-dns", { name, type });
    const question = query.subarray(12, cursor + 5);
    const header = Buffer.alloc(12);
    query.copy(header, 0, 0, 2);
    header.writeUInt16BE(fixture ? 0x8180 : 0x8183, 2);
    header.writeUInt16BE(1, 4);
    if (fixture && type === 1) {
      header.writeUInt16BE(1, 6);
      const answer = Buffer.from([0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, ...ip.split(".").map(Number)]);
      return Buffer.concat([header, question, answer]);
    }
    return Buffer.concat([header, question]);
  }
  const udp = dgram.createSocket("udp4");
  udp.on("message", async (query, remote) => {
    const response = await dnsResponse(query, true);
    if (response) udp.send(response, remote.port, remote.address);
  });
  await new Promise((resolve, reject) => { udp.once("error", reject); udp.bind(ports.dns, "0.0.0.0", resolve); });
  // Port zero in unit tests allocates TCP and UDP independently: an available
  // UDP ephemeral port can already be occupied by an unrelated TCP listener.
  // The container flow binds both protocols to the configured port 53.
  ports.dnsUdp = udp.address().port;
  await listen(net.createServer(client => {
    if (!own(client)) return;
    const input = reader(client);
    input.stage(buffer => {
      if (buffer.length < 2 || buffer.length < buffer.readUInt16BE(0) + 2) return;
      const end = buffer.readUInt16BE(0) + 2;
      void dnsResponse(buffer.subarray(2, end), client.remoteAddress !== "127.0.0.1").then(response => {
        if (response && !client.destroyed) { const size = Buffer.alloc(2); size.writeUInt16BE(response.length); client.write(Buffer.concat([size, response])); }
      });
      return end;
    });
  }), "dns");
  await listen(http.createServer((request, response) => {
    record(request.socket.remoteAddress === "127.0.0.1" ? "routed-http" : "direct-http", { path: request.url, host: request.headers.host });
    const origin = `http://download.proxy.test:${ports.http}`;
    if (request.url?.startsWith("/redirect")) { response.writeHead(302, { location: `${origin}/feed.xml` }).end(); return; }
    if (request.url === "/feed.xml") {
      response.writeHead(200, { "content-type": "application/rss+xml" }).end(`<rss version="2.0"><channel><title>Proxy fixture</title><item><title>proxy-${feedToken}</title><guid>${feedToken}</guid><enclosure url="${origin}/probe.nzb" length="${Buffer.byteLength(nzb)}" type="application/x-nzb" /></item></channel></rss>`); return;
    }
    if (request.url === "/probe.nzb") { response.writeHead(200, { "content-type": "application/x-nzb" }).end(nzb); return; }
    response.writeHead(404).end();
  }), "http");
  await listen(http.createServer(async (request, response) => {
    response.setHeader("content-type", "application/json");
    if (request.method === "GET") { response.end(JSON.stringify({ ip, ports, events, active: Object.fromEntries(Object.entries(routes).map(([name, route]) => [name, route.sockets.size])) })); return; }
    try {
      let body = "";
      for await (const chunk of request) { body += chunk; if (body.length > 65536) throw new Error("control body too large"); }
      const command = JSON.parse(body);
      if (command.route) {
        const route = routes[command.route];
        if (!route) throw new Error("unknown route");
        if (typeof command.up === "boolean") route.up = command.up;
        if (typeof command.hold === "boolean") route.hold = command.hold;
        if (command.cut) for (const socket of route.sockets) socket.destroy();
      }
      if (typeof command.nzb === "string") nzb = command.nzb;
      if (typeof command.feedToken === "string" && /^[a-z0-9-]+$/.test(command.feedToken)) feedToken = command.feedToken;
      response.end("{}");
    } catch (error) { response.writeHead(400).end(JSON.stringify({ error: String(error) })); }
  }), "control");
  return { ports, events, async close() { udp.close(); for (const socket of sockets) socket.destroy(); await Promise.all(servers.map(server => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); }))); } };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const fixture = await startProxyFixture();
  console.log("proxy routing fixture ready");
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => { await fixture.close(); process.exit(0); });
}

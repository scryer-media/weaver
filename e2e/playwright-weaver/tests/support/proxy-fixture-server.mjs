// Isolated e2e infrastructure. Ordinary runs forward only fixture destinations;
// the opt-in real-provider runner supplies an exact host/port allowlist.
import net from "node:net";
import http from "node:http";
import dgram from "node:dgram";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

// The resolvers this container was given, in resolv.conf order. Docker lists
// its embedded resolver; Podman lists the network's own DNS server.
export function parseNameservers(text) {
  return text.split("\n")
    .map(line => /^\s*nameserver\s+(\S+)\s*$/.exec(line)?.[1])
    .filter(address => address && net.isIP(address))
    .map(address => ({ address, port: 53 }));
}

function containerNameservers() {
  try { return parseNameservers(readFileSync("/etc/resolv.conf", "utf8")); } catch { return []; }
}

// Pool-member routes beyond the original ladder. CONNECT members listen on
// 8101-8106 and SOCKS5 members on 8201-8204; they exist only when asked for,
// so the proxy-routing flow keeps exactly the four routes it was built on.
export const EXTRA_ROUTES = Object.freeze({
  connect1: { kind: "connect", port: 8101 }, connect2: { kind: "connect", port: 8102 },
  connect3: { kind: "connect", port: 8103 }, connect4: { kind: "connect", port: 8104 },
  connect5: { kind: "connect", port: 8105 }, connect6: { kind: "connect", port: 8106 },
  socks1: { kind: "socks", port: 8201 }, socks2: { kind: "socks", port: 8202 },
  socks3: { kind: "socks", port: 8203 }, socks4: { kind: "socks", port: 8204 },
});

// "all" or a comma list of EXTRA_ROUTES names.
export function parseRouteList(text) {
  const value = String(text ?? "").trim();
  if (!value) return [];
  if (value === "all") return Object.keys(EXTRA_ROUTES);
  const names = value.split(",").map(name => name.trim()).filter(Boolean);
  for (const name of names) if (!EXTRA_ROUTES[name]) throw new Error(`unknown proxy fixture route ${name}`);
  return [...new Set(names)];
}

// Comma list of IPv4 addresses the fixture owns, one per attached network.
export function parseAddressList(text, fallback) {
  const list = String(text ?? "").split(",").map(address => address.trim()).filter(Boolean);
  for (const address of list) if (!net.isIPv4(address)) throw new Error(`invalid fixture address ${address}`);
  return list.length ? list : [fallback];
}

// JSON object of infrastructure name -> IPv4 addresses answered locally, so a
// multi-network flow can hand each egress the destination address on its own
// network instead of whatever the container resolver would pick.
export function parseHostTable(text) {
  if (!text) return {};
  const table = JSON.parse(text);
  const out = {};
  for (const [name, addresses] of Object.entries(table)) {
    const list = Array.isArray(addresses) ? addresses : [addresses];
    for (const address of list) if (!net.isIPv4(address)) throw new Error(`invalid address ${address} for ${name}`);
    out[name.toLowerCase()] = list;
  }
  return out;
}

export function parsePublicTargets(text) {
  const targets = text ? JSON.parse(text) : [];
  if (!Array.isArray(targets)) throw new Error("public targets must be an array");
  return targets.map(target => {
    if (typeof target.host !== "string" || !/^[a-zA-Z0-9.-]+$/.test(target.host)
      || !Number.isInteger(target.port) || target.port < 1 || target.port > 65535
      || !Array.isArray(target.addresses) || !target.addresses.length
      || target.addresses.some(address => !net.isIPv4(address))) {
      throw new Error("invalid public target");
    }
    return { host: target.host.toLowerCase(), port: target.port, addresses: [...target.addresses] };
  });
}

export async function startProxyFixture(options = {}) {
  const ip = options.ip ?? process.env.PROXY_FIXTURE_IP;
  if (!net.isIPv4(ip)) throw new Error("PROXY_FIXTURE_IP must be an IPv4 address");
  const nameservers = options.nameservers ?? containerNameservers();
  const extraRoutes = options.routes ?? parseRouteList(process.env.PROXY_FIXTURE_ROUTES);
  const addresses = options.addresses ?? parseAddressList(process.env.PROXY_FIXTURE_ADDRESSES, ip);
  const hosts = options.hosts ?? parseHostTable(process.env.PROXY_FIXTURE_HOSTS);
  const publicTargets = parsePublicTargets(JSON.stringify(options.publicTargets ?? []));
  const ports = {
    primary: 8081, secondary: 8082, tertiary: 8083, dns: 53, nntp: 119, http: 8089, control: 8090,
    ...Object.fromEntries(extraRoutes.map(name => [name, EXTRA_ROUTES[name].port])),
    ...options.ports,
  };
  const events = [];
  const sockets = new Set();
  const resolvers = new Set();
  const routes = Object.fromEntries(["primary", "secondary", "tertiary", "direct", ...extraRoutes].map(name => [name, { up: true, hold: false, connectStatus: null, socksReply: null, sockets: new Set() }]));
  const servers = [];
  const held = new Set();
  let sequence = 0;
  let feedToken = "initial";
  let nzb = "";
  // Article bytes arrive in many chunks per connection; consecutive chunks on
  // one route fold into the last event so a long download cannot push the
  // events a test asserts (connections, DNS, held bodies) out of the log. The
  // log keeps the newest events; sequence numbers stay monotonic across drops.
  const EVENT_LIMIT = 20000;
  const record = (kind, fields = {}) => {
    const last = events[events.length - 1];
    if (kind === "nntp-bytes" && last?.kind === "nntp-bytes" && last.route === fields.route) {
      last.bytes += fields.bytes;
      last.chunks = (last.chunks ?? 1) + 1;
      return;
    }
    events.push({ sequence: ++sequence, at: Date.now(), kind, ...fields });
    if (events.length > EVENT_LIMIT) events.splice(0, events.length - EVENT_LIMIT);
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
    const peer = client.remoteAddress;
    record("attempt", { route, host, port, client: peer });
    const publicTarget = publicTargets.find(target => target.port === port
      && (target.host === host.toLowerCase() || target.addresses.includes(host)));
    const allowedHost = host === ip || addresses.includes(host) || /^[a-z0-9-]+\.proxy\.test$/.test(host);
    const fixtureTarget = publicTargets.length === 0 && allowedHost && [ports.dns, ports.nntp, ports.http].includes(port);
    const fixtureDns = (host === ip || addresses.includes(host)) && port === ports.dns;
    if ((!publicTarget && !fixtureTarget && !fixtureDns) || !routes[route].up) {
      reply(false); client.end(); return;
    }
    record("connected", { route, host, port, client: peer });
    reply(true);
    const targetHost = publicTarget ? publicTarget.addresses[0]
      : port === ports.nntp ? (options.nntpHost ?? "nntp") : "127.0.0.1";
    const targetPort = publicTarget ? publicTarget.port : port === ports.nntp ? (options.nntpPort ?? 119) : port;
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
  const connectRoutes = ["primary", "tertiary", ...extraRoutes.filter(name => EXTRA_ROUTES[name].kind === "connect")];
  const socksRoutes = ["secondary", ...extraRoutes.filter(name => EXTRA_ROUTES[name].kind === "socks")];
  for (const route of connectRoutes) {
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
        const status = routes[route].connectStatus;
        if (status !== null) {
          record("attempt", { route, host: match[1], port: Number(match[2]), client: client.remoteAddress, status });
          client.end(`HTTP/1.1 ${status} ${http.STATUS_CODES[status] ?? "Fixture Status"}\r\n\r\n`); return;
        }
        forward(client, route, match[1], Number(match[2]), ok => client.write(`HTTP/1.1 ${ok ? "200 Connection Established" : "502 Bad Gateway"}\r\n\r\n`));
      });
    }), route);
  }
  for (const route of socksRoutes) await listen(net.createServer(client => {
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
      const refusal = routes[route].socksReply;
      if (refusal !== null) {
        record("attempt", { route, host, port, client: client.remoteAddress, socksReply: refusal });
        client.end(Buffer.from([5, refusal, 0, 1, 0, 0, 0, 0, 0, 0])); return;
      }
      forward(client, route, host, port, ok => client.write(Buffer.from([5, ok ? 0 : 5, 0, 1, 0, 0, 0, 0, 0, 0])));
    });
  }), route);
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
    const local = hosts[name] ?? publicTargets.find(target => target.host === name)?.addresses;
    // Resolve only infrastructure names through the container's own resolvers.
    // Every destination name above is answered locally, even when direct.
    if (!local && ["nntp", "nntp2", "weaver-postgres"].includes(name)) {
      // Every resolver is asked at once and the first answer is used. One
      // that stays silent costs nothing; when none answers, neither does this
      // fixture, and the client asks again as it would of any resolver.
      return new Promise(resolve => {
        const asked = new Set();
        let open = nameservers.length;
        if (!open) resolve(undefined);
        const settle = answer => {
          for (const resolver of asked) { resolvers.delete(resolver); resolver.close(); }
          asked.clear();
          resolve(answer);
        };
        for (const server of nameservers) {
          const resolver = dgram.createSocket(net.isIPv6(server.address) ? "udp6" : "udp4");
          asked.add(resolver);
          resolvers.add(resolver);
          resolver.once("message", settle);
          resolver.once("error", () => { if (!--open) settle(undefined); });
          resolver.send(query, server.port, server.address);
        }
        // Queries nothing answered are given up oldest first.
        for (const resolver of resolvers) {
          if (resolvers.size <= 256) break;
          resolvers.delete(resolver); resolver.close();
        }
      });
    }
    if (fixture) record(direct ? "direct-dns" : "routed-dns", { name, type });
    const question = query.subarray(12, cursor + 5);
    const header = Buffer.alloc(12);
    query.copy(header, 0, 0, 2);
    const known = fixture || Boolean(local);
    header.writeUInt16BE(known ? 0x8180 : 0x8183, 2);
    header.writeUInt16BE(1, 4);
    if (known && type === 1) {
      const answers = (local ?? addresses).map(address => Buffer.from([0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, ...address.split(".").map(Number)]));
      header.writeUInt16BE(answers.length, 6);
      return Buffer.concat([header, question, ...answers]);
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
    const pathname = new URL(request.url ?? "/", "http://fixture.invalid").pathname;
    if (request.url?.startsWith("/redirect")) { response.writeHead(302, { location: `${origin}/feed.xml` }).end(); return; }
    // A body that is not an NZB, for URL submissions that must fail to scan.
    if (pathname === "/not-an-nzb.nzb") { response.writeHead(200, { "content-type": "application/x-nzb" }).end("<html><body>not an nzb</body></html>"); return; }
    if (request.url === "/feed.xml") {
      response.writeHead(200, { "content-type": "application/rss+xml" }).end(`<rss version="2.0"><channel><title>Proxy fixture</title><item><title>proxy-${feedToken}</title><guid>${feedToken}</guid><enclosure url="${origin}/probe.nzb" length="${Buffer.byteLength(nzb)}" type="application/x-nzb" /></item></channel></rss>`); return;
    }
    // The query string is ignored so URL submissions can carry a secret that
    // the script-output redaction must remove.
    if (pathname === "/probe.nzb") { response.writeHead(200, { "content-type": "application/x-nzb" }).end(nzb); return; }
    response.writeHead(404).end();
  }), "http");
  await listen(http.createServer(async (request, response) => {
    response.setHeader("content-type", "application/json");
    if (request.method === "GET") {
      const url = new URL(request.url ?? "/", "http://fixture.invalid");
      if (url.pathname === "/events") {
        // Watermark reads: only events after the sequence the caller saw.
        const after = Number(url.searchParams.get("after") ?? 0);
        if (!Number.isInteger(after) || after < 0) { response.writeHead(400).end(JSON.stringify({ error: "after must be a non-negative integer" })); return; }
        response.end(JSON.stringify({ sequence, events: events.filter(event => event.sequence > after) })); return;
      }
      response.end(JSON.stringify({ ip, addresses, ports, sequence, events, active: Object.fromEntries(Object.entries(routes).map(([name, route]) => [name, route.sockets.size])) })); return;
    }
    try {
      let body = "";
      for await (const chunk of request) { body += chunk; if (body.length > 65536) throw new Error("control body too large"); }
      const command = JSON.parse(body);
      if (command.route) {
        const route = routes[command.route];
        if (!route) throw new Error("unknown route");
        if (typeof command.up === "boolean") route.up = command.up;
        if (typeof command.hold === "boolean") route.hold = command.hold;
        if ("connectStatus" in command) {
          if (command.connectStatus !== null && !(Number.isInteger(command.connectStatus) && command.connectStatus >= 100 && command.connectStatus <= 599)) throw new Error("connectStatus must be an HTTP status or null");
          route.connectStatus = command.connectStatus;
        }
        if ("socksReply" in command) {
          if (command.socksReply !== null && !(Number.isInteger(command.socksReply) && command.socksReply >= 1 && command.socksReply <= 255)) throw new Error("socksReply must be a non-zero byte or null");
          route.socksReply = command.socksReply;
        }
        if (command.cut) for (const socket of route.sockets) socket.destroy();
      }
      if (typeof command.nzb === "string") nzb = command.nzb;
      if (typeof command.feedToken === "string" && /^[a-z0-9-]+$/.test(command.feedToken)) feedToken = command.feedToken;
      response.end("{}");
    } catch (error) { response.writeHead(400).end(JSON.stringify({ error: String(error) })); }
  }), "control");
  return { ports, events, async close() { udp.close(); for (const resolver of resolvers) resolver.close(); resolvers.clear(); for (const socket of sockets) socket.destroy(); await Promise.all(servers.map(server => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); }))); } };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const fixture = await startProxyFixture();
  console.log("proxy routing fixture ready");
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => { await fixture.close(); process.exit(0); });
}

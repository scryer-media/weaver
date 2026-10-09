import dgram from "node:dgram";
import net from "node:net";
import { lookup } from "node:dns/promises";
import { readFileSync, writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { startProxyFixture } from "./proxy-fixture-server.mjs";

// Only WireGuard datagrams leave the isolated network through these relays.
// Each client socket gets its own remote UDP socket; return packets cannot
// cross sessions, and a connected socket accepts only its configured peer.
export async function startWireGuardRelays(targets) {
  const listeners = [];
  const clients = new Set();
  try {
    for (const target of targets) {
      if (typeof target.host !== "string" || !Number.isInteger(target.port)
        || target.port < 1 || target.port > 65535 || !Number.isInteger(target.listenPort)
        || target.listenPort < 0 || target.listenPort > 65535) throw new Error("invalid WG relay target");
      const remote = net.isIP(target.host)
        ? { address: target.host, family: net.isIP(target.host) }
        : await lookup(target.host, { family: 4 });
      // Keep provider bursts from overflowing the harness's own UDP queues.
      // The operating system may clamp these requests to its socket limits.
      const buffers = { recvBufferSize: 4 * 1024 * 1024, sendBufferSize: 4 * 1024 * 1024 };
      const listener = dgram.createSocket({ type: "udp4", ...buffers });
      listeners.push(listener);
      const sessions = new Map();
      listener.on("message", (packet, peer) => {
        const key = `${peer.address}:${peer.port}`;
        let session = sessions.get(key);
        if (!session) {
          if (sessions.size >= 256) return;
          const socket = dgram.createSocket({ type: remote.family === 6 ? "udp6" : "udp4", ...buffers });
          session = { socket, pending: [], connected: false };
          sessions.set(key, session); clients.add(socket);
          socket.on("error", () => { sessions.delete(key); clients.delete(socket); socket.close(); });
          socket.on("message", response => listener.send(response, peer.port, peer.address));
          socket.connect(target.port, remote.address, () => {
            session.connected = true;
            for (const pending of session.pending) socket.send(pending);
            session.pending = [];
          });
        }
        if (session.connected) session.socket.send(packet);
        else if (session.pending.length < 32) session.pending.push(packet);
      });
      await new Promise((resolve, reject) => {
        listener.once("error", reject);
        listener.bind(target.listenPort, "0.0.0.0", resolve);
      });
    }
    return {
      ports: listeners.map(listener => listener.address().port),
      async close() {
        for (const socket of clients) socket.close();
        clients.clear();
        await Promise.all(listeners.map(listener => new Promise(resolve => listener.close(resolve))));
      },
    };
  } catch (error) {
    for (const socket of clients) socket.close();
    for (const listener of listeners) { try { listener.close(); } catch {} }
    throw error;
  }
}

export async function startAPIRelay(target) {
  if (!net.isIPv4(target.host) || !Number.isInteger(target.port) || target.port < 1 || target.port > 65535
    || !Number.isInteger(target.listenPort) || target.listenPort < 0 || target.listenPort > 65535) {
    throw new Error("invalid private API relay target");
  }
  const sockets = new Set();
  const server = net.createServer(client => {
    const upstream = net.connect(target.port, target.host);
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on("error", () => { client.destroy(); upstream.destroy(); });
      socket.on("close", () => sockets.delete(socket));
    }
    client.on("close", () => upstream.destroy());
    upstream.on("close", () => client.destroy());
    client.pipe(upstream); upstream.pipe(client);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(target.listenPort, "0.0.0.0", resolve);
  });
  return {
    port: server.address().port,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(resolve));
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  // This file contains endpoint addresses only, never provider credentials.
  const config = JSON.parse(readFileSync("/run/real-network/relay.json", "utf8"));
  const fixture = await startProxyFixture({ ip: config.ip, publicTargets: config.publicTargets });
  const relays = await startWireGuardRelays(config.wireguard);
  const api = config.api ? await startAPIRelay(config.api) : null;
  writeFileSync("/tmp/real-network-ready", "ready\n");
  console.log("real network relays ready");
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => {
    if (api) await api.close();
    await relays.close(); await fixture.close(); process.exit(0);
  });
}

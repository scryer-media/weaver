import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { APIRequestContext, TestInfo } from "@playwright/test";
import { expect, graphql, postProbeArticle, setNntpChaos, submitProbeNzb, updateConfiguredServer } from "../helpers";
import { postProbeFile, waitTerminal } from "./downloads";
import {
  type ConsumerKind, type Egress, type ProxyProfile, type ProxyProfileInput, type RouteInput,
  createPool, deleteEgress, deletePool, deleteProxyProfile, hostAddress, interfaceEgress,
  saveProxyProfile, saveRoute,
} from "./network-flow";
import { fixtureState, resetRoutes, setFixtureNzb } from "./proxy-fixture";
import { TOXIPROXY_PORTS, resetToxiproxy, type ToxiproxyName } from "./toxiproxy";
import { resetTunnels, tunnelState, TUNNEL_PORTS, type TunnelEndpoint } from "./tunnel-fixture";
import { stopCapture } from "./capture";

/**
 * Per-test world for the network-* specs. It owns everything a test creates
 * (servers, feeds, pools, profiles, egresses) and puts every fixture back
 * to normal service afterwards, so each scenario starts from the same state.
 *
 * Host names: a server reached only directly uses `nntp` (the resolver answers
 * with its egress-a and egress-b addresses). A server any leg reaches through
 * an HTTP CONNECT or SOCKS5 proxy uses `news.proxy.test`, the only kind of name
 * the proxy fixture forwards; the fixture relays it to the same NNTP server.
 * SSH forwards from the tunnel fixture's network, so SSH routes use `nntp`.
 */
export const PROXIED_HOST = "news.proxy.test";
export const DIRECT_HOST = "nntp";
const CONNECT_PORTS: Record<string, number> = { connect1: 8101, connect2: 8102, connect3: 8103, connect4: 8104, connect5: 8105, connect6: 8106 };
const SOCKS_PORTS: Record<string, number> = { socks1: 8201, socks2: 8202, socks3: 8203, socks4: 8204 };

export type Network = "a" | "b";

export class NetworkWorld {
  readonly servers: number[] = [];
  readonly feeds: number[] = [];
  readonly pools: number[] = [];
  readonly profiles: number[] = [];
  readonly egresses: number[] = [];
  private readonly egressByNetwork = new Map<Network, Egress>();
  private chaos = false;
  private held?: HeldChaosSession;

  private constructor(readonly request: APIRequestContext) {}

  /** Stand the stock servers down so only the scenario's server downloads. */
  static async create(request: APIRequestContext): Promise<NetworkWorld> {
    await updateConfiguredServer(request, "nntp", { active: false });
    await updateConfiguredServer(request, "nntp2", { active: false });
    return new NetworkWorld(request);
  }

  async egress(network: Network): Promise<Egress> {
    const cached = this.egressByNetwork.get(network);
    if (cached) return cached;
    const egress = await interfaceEgress(this.request, `egress-${network}-${Date.now()}`, network);
    this.egresses.push(egress.id);
    this.egressByNetwork.set(network, egress);
    return egress;
  }

  track(kind: "egress" | "profile" | "pool" | "server" | "feed", id: number): number {
    ({ egress: this.egresses, profile: this.profiles, pool: this.pools, server: this.servers, feed: this.feeds })[kind].push(id);
    return id;
  }

  async server(options: { host?: string; port?: number; connections?: number; route?: RouteInput; active?: boolean } = {}): Promise<number> {
    const input = {
      host: options.host ?? DIRECT_HOST, port: options.port ?? 119, tls: false,
      username: "e2e-user", password: "e2e-pass", connections: options.connections ?? 4,
      active: options.active ?? true, priority: 0, backfill: false, retentionDays: 0,
    };
    const id = (await graphql<{ addServer: { id: number } }>(this.request,
      "mutation($input: ServerInput!) { addServer(input: $input) { id } }", { input })).addServer.id;
    this.servers.push(id);
    if (options.route) await saveRoute(this.request, "SERVER", id, options.route);
    return id;
  }

  async updateServer(id: number, overrides: Record<string, unknown>): Promise<void> {
    const server = (await graphql<{ servers: Array<{ id: number; host: string; port: number; connections: number; active: boolean }> }>(this.request,
      "query { servers { id host port connections active } }")).servers.find(candidate => candidate.id === id)!;
    await graphql(this.request, "mutation($id: Int!, $input: ServerInput!) { updateServer(id: $id, input: $input) { id } }", {
      id, input: {
        host: server.host, port: server.port, tls: false, username: "e2e-user", password: "e2e-pass",
        connections: server.connections, active: server.active, priority: 0, backfill: false, retentionDays: 0, ...overrides,
      },
    });
  }

  async route(kind: ConsumerKind, id: number, route: RouteInput) {
    return saveRoute(this.request, kind, id, route);
  }

  async feed(options: { url: string; route?: RouteInput; scripts?: string[] }): Promise<number> {
    const input = { name: `network-feed-${Date.now()}`, url: options.url, enabled: false, pollIntervalSecs: 86400, ...(options.scripts ? { scripts: options.scripts } : {}) };
    const id = (await graphql<{ addRssFeed: { id: number } }>(this.request,
      "mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }", { input })).addRssFeed.id;
    this.feeds.push(id);
    await graphql(this.request, "mutation($id: Int!) { addRssRule(feedId: $id, input: { sortOrder: 0, action: ACCEPT }) { id } }", { id });
    if (options.route) await saveRoute(this.request, "RSS", id, options.route);
    return id;
  }

  async profile(input: ProxyProfileInput): Promise<ProxyProfile> {
    const saved = await saveProxyProfile(this.request, input);
    this.profiles.push(saved.id);
    return saved;
  }

  /** An HTTP CONNECT pool member, through Toxiproxy unless `direct` is set. */
  async connect(member: keyof typeof CONNECT_PORTS | string, options: { network?: Network; direct?: boolean; password?: string | null; timeoutSeconds?: number } = {}): Promise<ProxyProfile> {
    const network = options.network ?? "a";
    const viaToxiproxy = !options.direct;
    return this.profile({
      name: `${member}-${network}-${Date.now()}`, kind: "HTTP_CONNECT", enabled: true,
      host: hostAddress(viaToxiproxy ? "toxiproxy" : "proxy-fixture", network),
      port: viaToxiproxy ? TOXIPROXY_PORTS[member as ToxiproxyName] : CONNECT_PORTS[member]!,
      username: "fixture", password: options.password === undefined ? "fixture" : options.password,
      dnsServers: [hostAddress("proxy-fixture", network)], timeoutSeconds: options.timeoutSeconds ?? 5,
    });
  }

  async socks(member: string, options: { network?: Network; direct?: boolean } = {}): Promise<ProxyProfile> {
    const network = options.network ?? "a";
    const viaToxiproxy = !options.direct;
    return this.profile({
      name: `${member}-${network}-${Date.now()}`, kind: "SOCKS5", enabled: true,
      host: hostAddress(viaToxiproxy ? "toxiproxy" : "proxy-fixture", network),
      port: viaToxiproxy ? TOXIPROXY_PORTS[member as ToxiproxyName] : SOCKS_PORTS[member]!,
      username: "fixture", password: "fixture", dnsServers: [hostAddress("proxy-fixture", network)], timeoutSeconds: 5,
    });
  }

  /** An SSH profile on a tunnel-fixture endpoint (ssh1..3 through Toxiproxy when asked). */
  async ssh(endpoint: TunnelEndpoint, options: { network?: Network; auth?: "password" | "key" | "passphrase" | "wrong-passphrase"; viaToxiproxy?: boolean } = {}): Promise<ProxyProfile> {
    const network = options.network ?? "a";
    const client = (await tunnelState(this.request)).sshClient;
    const auth = options.auth ?? "password";
    const credentials =
      auth === "password" ? { password: client.password } :
      auth === "key" ? { privateKey: client.privateKey } :
      { privateKey: client.privateKeyWithPassphrase, passphrase: auth === "passphrase" ? client.passphrase : `${client.passphrase}-wrong` };
    const viaToxiproxy = options.viaToxiproxy === true;
    return this.profile({
      name: `${endpoint}-${network}-${auth}-${Date.now()}`, kind: "SSH", enabled: true,
      host: hostAddress(viaToxiproxy ? "toxiproxy" : "tunnel-fixture", network),
      port: viaToxiproxy ? TOXIPROXY_PORTS[endpoint as ToxiproxyName] : TUNNEL_PORTS[endpoint],
      username: client.username, timeoutSeconds: 5, ...credentials,
    });
  }

  /** A WireGuard profile on wg1, wg2 (preshared key) or wg-rss (preshared key). */
  async wireguard(endpoint: "wg1" | "wg2" | "wg-rss", options: { network?: Network; wrongPresharedKey?: boolean } = {}): Promise<ProxyProfile> {
    const network = options.network ?? "a";
    const peer = (await tunnelState(this.request)).wireguard[endpoint]!;
    const preshared = peer.presharedKey === null ? null
      : options.wrongPresharedKey ? Buffer.alloc(32, 7).toString("base64") : peer.presharedKey;
    return this.profile({
      name: `${endpoint}-${network}-${Date.now()}`, kind: "WIRE_GUARD", enabled: true,
      host: hostAddress("tunnel-fixture", network), port: TUNNEL_PORTS[endpoint],
      peerPublicKey: peer.peerPublicKey, privateKey: peer.clientPrivateKey, presharedKey: preshared,
      tunnelAddresses: [peer.clientAddress], dnsServers: [peer.dnsServer], timeoutSeconds: 5,
    });
  }

  async pool(kind: ProxyProfile["kind"], memberIds: number[]): Promise<number> {
    const pool = await createPool(this.request, { name: `pool-${Date.now()}`, kind, memberIds });
    this.pools.push(pool.id);
    return pool.id;
  }

  /**
   * Post a file whose parts e2e-nntp serves `slowMs` late, so a download
   * stays in flight while the test acts on it; `release` lifts the pacing and
   * waits for the job to finish. Pacing is a floor on the job's duration, not
   * a clock the test reads: every wait is still on an observable.
   */
  async pacedDownload(name: string, options: { parts?: number; partBytes?: number; slowMs?: number; chaos?: string } = {}) {
    const parts = options.parts ?? 160;
    const partBytes = options.partBytes ?? 64 * 1024;
    const articles = await postProbeFile(name, { count: parts, partBytes });
    await this.holdChaosSession();
    await this.setChaos([`slow_body=${options.slowMs ?? 1500}`, options.chaos].filter(Boolean).join(","));
    const jobId = await submit(this.request, name, articles);
    return {
      jobId, bytes: parts * partBytes,
      release: async () => { await this.setChaos("off"); return waitTerminal(this.request, jobId); },
    };
  }

  async download(name: string, options: { parts?: number; partBytes?: number } = {}) {
    const parts = options.parts ?? 32;
    const partBytes = options.partBytes ?? 64 * 1024;
    const articles = await postProbeFile(name, { count: parts, partBytes });
    const jobId = await submit(this.request, name, articles);
    return { jobId, bytes: parts * partBytes };
  }

  /**
   * Arm the proxy fixture's feed with a fresh item whose NZB names one posted
   * article, and return the feed URL (a redirect to feed.xml). `host` picks
   * the name the feed is fetched by: `feed.proxy.test` through CONNECT/SOCKS
   * routes, `rss.proxy.test` inside the wg-rss tunnel.
   */
  async armFeed(token: string, host = "feed.proxy.test"): Promise<string> {
    const messageId = `feed-${token}@e2e.invalid`;
    await postProbeArticle(messageId, 4096);
    const nzb = `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="fixture" date="1700000000" subject="feed-${token}.bin"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="4096" number="1">${messageId}</segment></segments></file></nzb>`;
    await setFixtureNzb(this.request, nzb, token);
    const ports = (await fixtureState(this.request)).ports;
    return `http://${host}:${ports.http}/redirect`;
  }

  async sync(feedId: number): Promise<{ itemsFetched: number; itemsNew: number; itemsAccepted: number; itemsSubmitted: number; errors: string[] }> {
    return (await graphql<{ runRssSync: { itemsFetched: number; itemsNew: number; itemsAccepted: number; itemsSubmitted: number; errors: string[] } }>(this.request,
      "mutation($id: Int!) { runRssSync(feedId: $id) { itemsFetched itemsNew itemsAccepted itemsSubmitted errors } }", { id: feedId })).runRssSync;
  }

  async setChaos(config: string): Promise<void> {
    if (this.held) await this.held.chaos(config);
    else await setNntpChaos(config);
    this.chaos = config !== "off";
  }

  /**
   * Open an authenticated control session to the NNTP server and keep it, so
   * chaos that refuses new sessions (`max_conns`, `greet_400=100`) can still be
   * cleared. The held session counts towards `max_conns`: a limit of N leaves
   * Weaver N - 1. Every later `setChaos` goes through it.
   */
  async holdChaosSession(): Promise<void> {
    this.held ??= await HeldChaosSession.open();
  }

  async cleanup(info: TestInfo): Promise<void> {
    const failures: string[] = [];
    const attempt = async (label: string, action: () => Promise<unknown>) => {
      try { await action(); } catch (error) { failures.push(`${label}: ${String(error)}`); }
    };
    if (this.chaos) await attempt("chaos off", () => this.held ? this.held.chaos("off") : setNntpChaos("off"));
    this.held?.close();
    this.held = undefined;
    await attempt("stop capture", () => stopCapture(this.request));
    await attempt("reset toxiproxy", () => resetToxiproxy(this.request));
    await attempt("reset proxy fixture", () => resetRoutes(this.request));
    await attempt("reset tunnels", () => resetTunnels(this.request));
    for (const id of this.feeds.splice(0)) await attempt(`feed ${id}`, () => graphql(this.request, "mutation($id: Int!) { deleteRssFeed(id: $id) }", { id }));
    for (const id of this.servers.splice(0)) await attempt(`server ${id}`, () => graphql(this.request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id }));
    for (const id of this.pools.splice(0)) await attempt(`pool ${id}`, () => deletePool(this.request, id));
    for (const id of this.profiles.splice(0)) await attempt(`profile ${id}`, () => deleteProxyProfile(this.request, id));
    for (const id of this.egresses.splice(0)) await attempt(`egress ${id}`, () => deleteEgress(this.request, id));
    this.egressByNetwork.clear();
    if (failures.length) await info.attach("network-cleanup-failures", { body: failures.join("\n"), contentType: "text/plain" });
  }
}

async function submit(request: APIRequestContext, name: string, articles: Array<{ messageId: string; bytes: number }>): Promise<number> {
  const result = await submitProbeNzb(request, name, articles, {}, "single-multipart-file");
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  return result.jobId!;
}

/** The finished file must be the posted bytes: every part decoded and placed. */
export async function verifyOutput(request: APIRequestContext, jobId: number, name: string, bytes: number): Promise<void> {
  const item = (await graphql<{ historyItem: { state: string; outputDir: string | null } | null }>(request,
    "query($id: Int!) { historyItem(id: $id) { state outputDir } }", { id: jobId })).historyItem;
  expect(item?.state, `job ${jobId}`).toBe("COMPLETED");
  expect(item?.outputDir, `job ${jobId} output directory`).toBeTruthy();
  const local = item!.outputDir!.replace(/^\/data\/complete/, "/weaver-downloads");
  const file = path.join(local, `${name}.bin`);
  const content = fs.readFileSync(file);
  expect(content.length, file).toBe(bytes);
  expect(content.every(byte => byte === 0), `${file} holds the posted zero bytes`).toBe(true);
}

export async function jobEvents(request: APIRequestContext, jobId: number): Promise<Array<{ kind: string; message: string }>> {
  return (await graphql<{ jobEvents: Array<{ kind: string; message: string }> }>(request,
    "query($id: Int!) { jobEvents(jobId: $id) { kind message } }", { id: jobId })).jobEvents;
}

export async function expectNoFailureEvents(request: APIRequestContext, jobId: number): Promise<void> {
  const failures = (await jobEvents(request, jobId)).filter(event => ["JOB_FAILED", "SEGMENT_FAILED_PERMANENT"].includes(event.kind));
  expect(failures, `job ${jobId} failure events`).toEqual([]);
}

export type ServerHealthRow = {
  host: string; port: number; state: string; activity: string; connectionsOpen: number;
  successCount: number; failureCount: number; consecutiveFailures: number; prematureDeaths: number;
};
export async function serverHealth(request: APIRequestContext, host: string): Promise<ServerHealthRow[]> {
  return (await graphql<{ serverHealth: ServerHealthRow[] }>(request,
    "query { serverHealth { host port state activity connectionsOpen successCount failureCount consecutiveFailures prematureDeaths } }"))
    .serverHealth.filter(row => row.host === host);
}

/**
 * Persist evidence for a scenario: attached to the test, and collected under
 * `<stage>/<name>` in the run's network-evidence.json beside the captures.
 */
export async function saveEvidence(info: TestInfo, name: string, evidence: unknown): Promise<void> {
  const body = JSON.stringify(evidence, null, 2);
  const root = process.env.PLAYWRIGHT_ARTIFACTS_DIR || "artifacts";
  fs.mkdirSync(root, { recursive: true });
  const file = path.join(root, "network-evidence.json");
  let all: Record<string, unknown> = {};
  try { all = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>; } catch { all = {}; }
  all[`${process.env.E2E_WEAVER_STAGE || "initial"}/${name}`] = evidence;
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(all, null, 2));
  fs.renameSync(`${file}.tmp`, file);
  await info.attach(name, { body, contentType: "application/json" });
}

/** One NNTP session held open for CHAOS commands (see `holdChaosSession`). */
class HeldChaosSession {
  private buffered = "";
  private readonly lines: string[] = [];
  private readonly waiters: Array<(line: string) => void> = [];

  private constructor(private readonly socket: net.Socket) {
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      this.buffered += chunk;
      for (let newline = this.buffered.indexOf("\n"); newline >= 0; newline = this.buffered.indexOf("\n")) {
        const line = this.buffered.slice(0, newline).replace(/\r$/, "");
        this.buffered = this.buffered.slice(newline + 1);
        const waiter = this.waiters.shift();
        if (waiter) waiter(line); else this.lines.push(line);
      }
    });
  }

  static async open(host = "nntp", port = 119): Promise<HeldChaosSession> {
    const socket = net.createConnection({ host, port });
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    const session = new HeldChaosSession(socket);
    expect(await session.line(), "control session greeting").toMatch(/^20[01] /);
    expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
    expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
    return session;
  }

  private line(): Promise<string> {
    const ready = this.lines.shift();
    if (ready !== undefined) return Promise.resolve(ready);
    return new Promise((resolve, reject) => {
      const onClose = () => reject(new Error("NNTP control session closed"));
      this.socket.once("close", onClose);
      this.waiters.push(line => { this.socket.off("close", onClose); resolve(line); });
    });
  }

  async command(line: string): Promise<string> {
    this.socket.write(`${line}\r\n`);
    return this.line();
  }

  async chaos(config: string): Promise<void> {
    expect(await this.command(`CHAOS ${config}`)).toMatch(/^290 /);
  }

  close(): void {
    this.socket.end("QUIT\r\n");
  }
}

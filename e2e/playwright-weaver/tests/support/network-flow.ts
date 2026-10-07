import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, weaverRoute } from "../helpers";

/**
 * Advanced-networking GraphQL surface for the network-* specs: egress
 * interfaces, proxy profiles and pools, routes, and the live `networkFlow`
 * sample every assertion about legs, pools and targets reads.
 *
 * A flow sample is stamped with `sampledAt` (epoch seconds). Waits take the
 * stamp of a sample read before the action and accept only a later sample,
 * so a sample taken before the change can never satisfy them.
 */

export type BindingKind = "SYSTEM" | "INTERFACE" | "SOURCE_ADDRESS";
export type ProxyKind = "HTTP_CONNECT" | "HTTP3_CONNECT" | "SOCKS5" | "SSH" | "WIRE_GUARD";
export type ConsumerKind = "SERVER" | "RSS";
export type Failover = "REDISTRIBUTE" | "HOLD";
export type LegState = "UP" | "IDLE" | "PROBING" | "DOWN" | "BLOCKED";

export type Egress = {
  id: number; name: string; bindingKind: BindingKind; interfaceName: string | null;
  sourceAddress: string | null; addresses: string[]; enabled: boolean; maxDownloadSpeed: number;
  health: string; reason: string | null;
};
export type ProxyProfile = {
  id: number; name: string; kind: ProxyKind; enabled: boolean; host: string; port: number;
  dnsServers: string[]; tunnelAddresses: string[]; peerPublicKey: string | null; tunnelPublicKey: string | null;
  mtu: number; keepaliveSeconds: number | null; timeoutSeconds: number; hostKeyFingerprint: string | null;
  hasUsername: boolean; hasPassword: boolean; hasPrivateKey: boolean; hasPassphrase: boolean; hasPresharedKey: boolean;
};
export type ProxyPool = { id: number; name: string; kind: ProxyKind; memberIds: number[]; enabled: boolean };
export type Rung = { kind: "PROXY" | "POOL" | "CHAIN"; proxyId: number | null; poolId: number | null; chainIds: number[] };
export type LegPath = { kind: "DIRECT" | "LADDER"; rungs: Rung[]; directFallback: boolean };
export type Route = { legs: Array<{ egressId: number; weight: number; path: LegPath }>; failover: Failover };
export type ConsumerFlow = { key: string; id: number; name: string; kind: ConsumerKind; cap: number; route: Route };
export type LegFlow = {
  consumer: string; position: number; egressId: number; weight: number; target: number; open: number;
  opening: number; state: LegState; reason: string | null; path: LegPath; pinnedAddress: string | null;
  sourceAddress: string | null; bytesPerSecond: number; selectedRung: number | null;
  selectedProxyId: number | null; rungStates: string[];
};
export type PoolMemberFlow = {
  id: number; state: string; open: number; opening: number; warmed: boolean; blocked: string | null;
  handshakeMs: number | null; connectMs: number | null; bytesPerSecond: number | null; samples: number; failures: number;
};
export type PoolFlow = { poolId: number; egressId: number; pinnedMember: number | null; members: PoolMemberFlow[] };
export type NetworkFlow = {
  consumers: ConsumerFlow[]; legs: LegFlow[]; pools: PoolFlow[]; egresses: Egress[];
  proxies: ProxyProfile[]; proxyPools: ProxyPool[]; sampledAt: number;
};
export type TestResult = {
  success: boolean; message: string; sourceAddress: string | null; connectMillis: number | null; proxyId: number | null;
};

const EGRESS = "id name bindingKind interfaceName sourceAddress addresses enabled maxDownloadSpeed health reason";
const PROFILE = `id name kind enabled host port dnsServers tunnelAddresses peerPublicKey tunnelPublicKey mtu
  keepaliveSeconds timeoutSeconds hostKeyFingerprint hasUsername hasPassword hasPrivateKey hasPassphrase hasPresharedKey`;
const POOL = "id name kind memberIds enabled";
const PATH = "path { kind directFallback rungs { kind proxyId poolId chainIds } }";
const ROUTE = `legs { egressId weight ${PATH} } failover`;
const TEST = "success message sourceAddress connectMillis proxyId";
export const FLOW_SELECTION = `
  consumers { key id name kind cap route { ${ROUTE} } }
  legs { consumer position egressId weight target open opening state reason ${PATH}
    pinnedAddress sourceAddress bytesPerSecond selectedRung selectedProxyId rungStates }
  pools { poolId egressId pinnedMember members { id state open opening warmed blocked handshakeMs connectMs bytesPerSecond samples failures } }
  egresses { ${EGRESS} } proxies { ${PROFILE} } proxyPools { ${POOL} } sampledAt`;

export const serverKey = (id: number) => `server:${id}`;
export const rssKey = (id: number) => `rss:${id}`;

// ---------------------------------------------------------------- environment

/** Weaver's address on egress-a or egress-b (set by the harness override). */
export function egressAddress(network: "a" | "b"): string {
  const value = process.env[network === "a" ? "E2E_WEAVER_EGRESS_A_IP" : "E2E_WEAVER_EGRESS_B_IP"];
  expect(value, `harness must set Weaver's egress-${network} address`).toBeTruthy();
  return value!;
}

/** Service → addresses, egress-a first, from the harness override. */
export function networkHosts(): Record<string, string[]> {
  const raw = process.env.E2E_NETWORK_HOSTS;
  expect(raw, "harness must set E2E_NETWORK_HOSTS").toBeTruthy();
  return JSON.parse(raw!) as Record<string, string[]>;
}

/** A service's address on egress-a (index 0) or egress-b (index 1). */
export function hostAddress(service: string, network: "a" | "b"): string {
  const addresses = networkHosts()[service] ?? [];
  const address = addresses[network === "a" ? 0 : 1];
  expect(address, `${service} must be attached to egress-${network}`).toBeTruthy();
  return address!;
}

export const netRawRetained = () => (process.env.E2E_WEAVER_NET_RAW ?? "retained") === "retained";
export const extended = () => process.env.E2E_WEAVER_EXTENDED === "1";
export const stage = () => process.env.E2E_WEAVER_STAGE ?? "initial";

// ---------------------------------------------------------------- queries

export async function networkFlow(request: APIRequestContext): Promise<NetworkFlow> {
  return (await graphql<{ networkFlow: NetworkFlow }>(request, `query WeaverE2ENetworkFlow { networkFlow { ${FLOW_SELECTION} } }`)).networkFlow;
}

/** The stamp to pass to `flowAfter` for "a sample taken after this point". */
export async function flowMark(request: APIRequestContext): Promise<number> {
  return (await networkFlow(request)).sampledAt;
}

/**
 * Poll until a sample newer than `after` satisfies `predicate`; returns it.
 * `describe` names the condition in the failure message.
 */
export async function flowAfter(
  request: APIRequestContext,
  after: number,
  predicate: (flow: NetworkFlow) => boolean,
  describe: string,
): Promise<NetworkFlow> {
  let matched: NetworkFlow | undefined;
  let last: NetworkFlow | undefined;
  await expect.poll(async () => {
    last = await networkFlow(request);
    if (last.sampledAt > after && predicate(last)) matched = last;
    return matched !== undefined;
  }, { message: describe, timeout: 0 }).toBe(true);
  return matched!;
}

export const legsOf = (flow: NetworkFlow, consumer: string) =>
  flow.legs.filter(leg => leg.consumer === consumer).sort((left, right) => left.position - right.position);
export const legOn = (flow: NetworkFlow, consumer: string, egressId: number) =>
  legsOf(flow, consumer).find(leg => leg.egressId === egressId);
export const poolOn = (flow: NetworkFlow, poolId: number, egressId: number) =>
  flow.pools.find(pool => pool.poolId === poolId && pool.egressId === egressId);
export const consumerOf = (flow: NetworkFlow, key: string) => flow.consumers.find(consumer => consumer.key === key);
export const egressOf = (flow: NetworkFlow, id: number) => flow.egresses.find(egress => egress.id === id);
export const openOn = (flow: NetworkFlow, consumer: string) => legsOf(flow, consumer).reduce((sum, leg) => sum + leg.open, 0);

export async function egressInterfaces(request: APIRequestContext): Promise<Egress[]> {
  return (await graphql<{ egressInterfaces: Egress[] }>(request, `query { egressInterfaces { ${EGRESS} } }`)).egressInterfaces;
}

export type DiscoveredInterface = { name: string; index: number | null; up: boolean; addresses: string[] };
export async function discoverInterfaces(request: APIRequestContext): Promise<DiscoveredInterface[]> {
  return (await graphql<{ discoverNetworkInterfaces: DiscoveredInterface[] }>(request,
    "query { discoverNetworkInterfaces { name index up addresses } }")).discoverNetworkInterfaces;
}

/** The interface Weaver sees carrying `address` (addresses may carry a prefix length). */
export async function interfaceForAddress(request: APIRequestContext, address: string): Promise<DiscoveredInterface> {
  const found = (await discoverInterfaces(request))
    .find(candidate => candidate.addresses.some(value => value.split("/")[0] === address));
  expect(found, `an interface in Weaver's namespace carries ${address}`).toBeTruthy();
  return found!;
}

export type PlatformNetworking = {
  platform: string; egressBindingKinds: BindingKind[]; sourceAddressHint: string; container: boolean;
  bridgeNetworkSuspected: boolean; maxWireguardInstances: number; notes: string[];
};
export async function platformNetworking(request: APIRequestContext): Promise<PlatformNetworking> {
  return (await graphql<{ platformNetworking: PlatformNetworking }>(request,
    "query { platformNetworking { platform egressBindingKinds sourceAddressHint container bridgeNetworkSuspected maxWireguardInstances notes } }")).platformNetworking;
}

// ---------------------------------------------------------------- mutations

export type EgressInput = {
  name: string; bindingKind: BindingKind; interfaceName?: string | null; sourceAddress?: string | null;
  enabled?: boolean; maxDownloadSpeed?: number;
};
export async function createEgress(request: APIRequestContext, input: EgressInput): Promise<Egress> {
  return (await graphql<{ createEgressInterface: Egress }>(request,
    `mutation($input: EgressInterfaceInput!) { createEgressInterface(input: $input) { ${EGRESS} } }`, { input })).createEgressInterface;
}
export async function updateEgress(request: APIRequestContext, id: number, input: EgressInput): Promise<Egress> {
  return (await graphql<{ updateEgressInterface: Egress }>(request,
    `mutation($id: Int!, $input: EgressInterfaceInput!) { updateEgressInterface(id: $id, input: $input) { ${EGRESS} } }`, { id, input })).updateEgressInterface;
}
export async function deleteEgress(request: APIRequestContext, id: number): Promise<boolean> {
  return (await graphql<{ deleteEgressInterface: boolean }>(request,
    "mutation($id: Int!) { deleteEgressInterface(id: $id) }", { id })).deleteEgressInterface;
}

/** Interface-bound egress on Weaver's egress-a or egress-b address. */
export async function interfaceEgress(request: APIRequestContext, name: string, network: "a" | "b", extra: Partial<EgressInput> = {}): Promise<Egress> {
  const discovered = await interfaceForAddress(request, egressAddress(network));
  return createEgress(request, { name, bindingKind: "INTERFACE", interfaceName: discovered.name, ...extra });
}
/** Source-address egress on Weaver's egress-a or egress-b address. */
export async function sourceEgress(request: APIRequestContext, name: string, network: "a" | "b", extra: Partial<EgressInput> = {}): Promise<Egress> {
  return createEgress(request, { name, bindingKind: "SOURCE_ADDRESS", sourceAddress: egressAddress(network), ...extra });
}

export type ProxyProfileInput = {
  name: string; kind: ProxyKind; enabled: boolean; host: string; port: number; dnsServers?: string[];
  tunnelAddresses?: string[]; peerPublicKey?: string | null; mtu?: number; keepaliveSeconds?: number | null;
  timeoutSeconds?: number; username?: string | null; password?: string | null; privateKey?: string | null;
  passphrase?: string | null; presharedKey?: string | null;
};
export async function saveProxyProfile(request: APIRequestContext, input: ProxyProfileInput, id?: number): Promise<ProxyProfile> {
  return (await graphql<{ saveProxyProfile: ProxyProfile }>(request,
    `mutation($id: Int, $input: ProxyProfileInput!) { saveProxyProfile(id: $id, input: $input) { ${PROFILE} } }`,
    { id: id ?? null, input })).saveProxyProfile;
}
export async function deleteProxyProfile(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { deleteProxyProfile(id: $id) }", { id });
}
export async function resetProxyHostKey(request: APIRequestContext, id: number): Promise<ProxyProfile> {
  return (await graphql<{ resetProxyHostKey: ProxyProfile }>(request,
    `mutation($id: Int!) { resetProxyHostKey(id: $id) { ${PROFILE} } }`, { id })).resetProxyHostKey;
}
export async function testProxyProfile(request: APIRequestContext, id: number): Promise<{ success: boolean; message: string }> {
  return (await graphql<{ testProxyProfile: { success: boolean; message: string } }>(request,
    "mutation($id: Int!) { testProxyProfile(id: $id) { success message } }", { id })).testProxyProfile;
}
export async function proxyProfiles(request: APIRequestContext): Promise<ProxyProfile[]> {
  return (await graphql<{ proxyProfiles: ProxyProfile[] }>(request, `query { proxyProfiles { ${PROFILE} } }`)).proxyProfiles;
}

export type ProxyPoolInput = { name: string; kind: ProxyKind; memberIds: number[]; enabled?: boolean };
export async function createPool(request: APIRequestContext, input: ProxyPoolInput): Promise<ProxyPool> {
  return (await graphql<{ createProxyPool: ProxyPool }>(request,
    `mutation($input: ProxyPoolInput!) { createProxyPool(input: $input) { ${POOL} } }`, { input })).createProxyPool;
}
export async function updatePool(request: APIRequestContext, id: number, input: ProxyPoolInput): Promise<ProxyPool> {
  return (await graphql<{ updateProxyPool: ProxyPool }>(request,
    `mutation($id: Int!, $input: ProxyPoolInput!) { updateProxyPool(id: $id, input: $input) { ${POOL} } }`, { id, input })).updateProxyPool;
}
export async function deletePool(request: APIRequestContext, id: number): Promise<boolean> {
  return (await graphql<{ deleteProxyPool: boolean }>(request, "mutation($id: Int!) { deleteProxyPool(id: $id) }", { id })).deleteProxyPool;
}

export async function testEgress(request: APIRequestContext, id: number, host: string, port: number, proxyId?: number): Promise<TestResult> {
  return (await graphql<{ testEgressInterface: TestResult }>(request,
    `mutation($id: Int!, $proxyId: Int, $host: String!, $port: Int!) { testEgressInterface(id: $id, proxyId: $proxyId, host: $host, port: $port) { ${TEST} } }`,
    { id, proxyId: proxyId ?? null, host, port })).testEgressInterface;
}
export async function testPool(request: APIRequestContext, id: number, egressId: number, host?: string, port?: number): Promise<TestResult[]> {
  return (await graphql<{ testProxyPool: TestResult[] }>(request,
    `mutation($id: Int!, $egressId: Int!, $host: String, $port: Int) { testProxyPool(id: $id, egressId: $egressId, host: $host, port: $port) { ${TEST} } }`,
    { id, egressId, host: host ?? null, port: port ?? null })).testProxyPool;
}

// ---------------------------------------------------------------- routes

export type RungInput = { proxy: number } | { pool: number } | { chain: number[] };
export type LegInput = { egressId: number; weight: number; path: { direct: true } | { ladder: { rungs: RungInput[]; directFallback: boolean } } };
export type RouteInput = { legs: LegInput[]; failover?: Failover };

export const rung = {
  proxy: (id: number): RungInput => ({ proxy: id }),
  pool: (id: number): RungInput => ({ pool: id }),
  chain: (ids: number[]): RungInput => ({ chain: ids }),
};
export const directLeg = (egressId: number, weight = 1): LegInput => ({ egressId, weight, path: { direct: true } });
export const ladderLeg = (egressId: number, rungs: RungInput[], weight = 1, directFallback = false): LegInput =>
  ({ egressId, weight, path: { ladder: { rungs, directFallback } } });

export async function saveRoute(request: APIRequestContext, kind: ConsumerKind, id: number, route: RouteInput): Promise<{ consumer: string; legs: Route["legs"]; failover: Failover }> {
  return (await graphql<{ saveNetworkRoute: { consumer: string; legs: Route["legs"]; failover: Failover } }>(request,
    `mutation($kind: NetworkConsumerKind!, $id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: $kind, id: $id, input: $input) { consumer ${ROUTE} } }`,
    { kind, id, input: route })).saveNetworkRoute;
}

/** Every saved route, keyed `server:<id>` / `rss:<id>`. */
export async function networkRoutes(request: APIRequestContext): Promise<Array<{ consumer: string; legs: Route["legs"]; failover: Failover }>> {
  return (await graphql<{ networkRoutes: Array<{ consumer: string; legs: Route["legs"]; failover: Failover }> }>(request,
    `query { networkRoutes { consumer ${ROUTE} } }`)).networkRoutes;
}

/** Send a GraphQL document expected to fail; returns the error messages. */
export async function graphqlErrors(request: APIRequestContext, query: string, variables: Record<string, unknown> = {}): Promise<string[]> {
  await networkFlow(request); // opens the API session the same way `graphql` does
  const response = await request.post(weaverRoute("/graphql"), { data: { query, variables } });
  const payload = await response.json() as { errors?: Array<{ message: string }> };
  return (payload.errors ?? []).map(error => error.message);
}

// ---------------------------------------------------------------- subscription

/**
 * Sample the `networkFlow` subscription over graphql-transport-ws. Every
 * sample is kept so a test can assert on the whole sequence (for example,
 * that a leg never went DOWN while another stayed UP).
 */
export class FlowSubscription {
  readonly samples: NetworkFlow[] = [];
  readonly errors: string[] = [];
  private socket: WebSocket;
  private acked = false;
  private closed = false;

  private constructor(url: string, cookie: string) {
    // A loginless browser session rides on the entry page's session cookie,
    // exactly as the HTTP helpers do; without it the server refuses connection_init.
    this.socket = new WebSocket(url, { protocols: ["graphql-transport-ws"], headers: { cookie } } as unknown as string[]);
    this.socket.addEventListener("open", () => this.socket.send(JSON.stringify({ type: "connection_init", payload: {} })));
    this.socket.addEventListener("message", event => {
      const message = JSON.parse(String(event.data)) as { type: string; payload?: { data?: { networkFlow: NetworkFlow }; message?: string } };
      if (message.type === "connection_ack") {
        this.acked = true;
        this.socket.send(JSON.stringify({ id: "flow", type: "subscribe", payload: { query: `subscription { networkFlow { ${FLOW_SELECTION} } }` } }));
      } else if (message.type === "next" && message.payload?.data) {
        this.samples.push(message.payload.data.networkFlow);
      } else if (message.type === "error") {
        this.errors.push(JSON.stringify(message.payload));
      } else if (message.type === "ping") {
        this.socket.send(JSON.stringify({ type: "pong" }));
      }
    });
    this.socket.addEventListener("close", () => { this.closed = true; });
  }

  static async open(): Promise<FlowSubscription> {
    const base = new URL(process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090/");
    const entry = await fetch(base);
    expect(entry.ok, `entry page ${entry.status}`).toBe(true);
    const cookie = entry.headers.getSetCookie().map(value => value.split(";")[0]).join("; ");
    expect(cookie, "entry page sets a session cookie").not.toBe("");
    base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
    base.pathname = `${base.pathname.replace(/\/+$/, "")}/graphql/ws`;
    const subscription = new FlowSubscription(base.toString(), cookie);
    await expect.poll(() => subscription.acked || subscription.closed, { timeout: 0 }).toBe(true);
    expect(subscription.closed, "flow subscription closed before connection_ack").toBe(false);
    return subscription;
  }

  /** Wait for a sample newer than `after` that satisfies `predicate`. */
  async next(after: number, predicate: (flow: NetworkFlow) => boolean, describe: string): Promise<NetworkFlow> {
    let matched: NetworkFlow | undefined;
    await expect.poll(() => {
      matched = this.samples.find(sample => sample.sampledAt > after && predicate(sample));
      if (!matched && this.closed) throw new Error(`flow subscription closed while waiting for: ${describe}`);
      return matched !== undefined;
    }, { message: describe, timeout: 0 }).toBe(true);
    return matched!;
  }

  since(after: number): NetworkFlow[] {
    return this.samples.filter(sample => sample.sampledAt > after);
  }

  close(): void {
    if (!this.closed) {
      try { this.socket.send(JSON.stringify({ id: "flow", type: "complete" })); } catch { /* already closing */ }
      this.socket.close();
    }
  }
}

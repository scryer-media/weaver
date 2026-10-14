import { closedPath, directLeg, routeInput, type NetworkRoute } from "./networking.ts";
export type ProxyKind = "HTTP_CONNECT" | "HTTP3_CONNECT" | "SOCKS5" | "SSH" | "WIRE_GUARD";
/** Who resolves a name dialled through a proxy: Weaver, or the proxy. */
export type ProxyResolver = "WEAVER" | "PROXY";
export const proxyLabels: Record<ProxyKind, string> = { HTTP_CONNECT: "HTTP CONNECT", HTTP3_CONNECT: "HTTP/3 CONNECT", SOCKS5: "SOCKS5", SSH: "SSH", WIRE_GUARD: "WireGuard" };
export type RoutingPolicy = { proxyIds: number[]; allowDirect: boolean; legs?: NetworkRoute["legs"]; failover?: NetworkRoute["failover"] };
export function policyAsRoute(policy: RoutingPolicy): NetworkRoute {
  return {failover:policy.failover??"REDISTRIBUTE",legs:policy.legs??[{...directLeg(),path:policy.proxyIds.length===0&&policy.allowDirect?directLeg().path:{kind:"LADDER",directFallback:policy.allowDirect,rungs:policy.proxyIds.map(id=>({kind:"PROXY",proxyId:id,poolId:null,chainIds:[]}))}}]};
}
/** The kill switch: no proxy to go through, and no going direct, so nothing leaves. */
export const blockedRouting: RoutingPolicy = { proxyIds: [], allowDirect: false };
/**
 * A consumer's route as its input names it. A route's ladder needs a rung, so
 * only the older `routing` field can say that nothing may leave.
 */
export function routeFields(policy: RoutingPolicy) {
  const route=policyAsRoute(policy);
  return route.legs.length===1&&closedPath(route.legs[0]!.path)?{routing:blockedRouting}:{route:routeInput(route)};
}
export type RoutingStatus = { state: string; selectedProxyId: number | null; failures: { proxyId: number; message: string }[] };
export const directRouting: RoutingPolicy = { proxyIds: [], allowDirect: true };
/** The daemon accepts at most this many proxy routes in one policy. */
export const MAX_PROXY_ROUTES = 8;
export function appendProxy(policy: RoutingPolicy, id: number): RoutingPolicy {
  if (policy.proxyIds.includes(id) || policy.proxyIds.length >= MAX_PROXY_ROUTES) return policy;
  return { proxyIds: [...policy.proxyIds, id], allowDirect: policy.proxyIds.length === 0 ? false : policy.allowDirect };
}
export function moveProxy(policy: RoutingPolicy, index: number, delta: number): RoutingPolicy {
  const target = index + delta;
  if (index < 0 || target < 0 || index >= policy.proxyIds.length || target >= policy.proxyIds.length) return policy;
  const ids = [...policy.proxyIds];
  [ids[index], ids[target]] = [ids[target]!, ids[index]!];
  return { ...policy, proxyIds: ids };
}
export type ProxyProfile = {
  id: number; name: string; kind: ProxyKind; enabled: boolean; host: string; port: number;
  resolver: ProxyResolver; dnsServers: string[]; tunnelAddresses: string[]; peerPublicKey: string | null; tunnelPublicKey: string | null;
  mtu: number; keepaliveSeconds: number | null; timeoutSeconds: number; hostKeyFingerprint: string | null;
  hasUsername: boolean; hasPassword: boolean; hasPrivateKey: boolean; hasPassphrase: boolean; hasPresharedKey: boolean;
};
type ChainProfile = Pick<ProxyProfile, "id" | "kind">;
/** What a chain's ladder rules out besides the chain's own hops. */
export type ChainLimits = {
  /** The proxies the ladder's other rungs already use, pool members included. */
  taken?: ReadonlySet<number>;
};
/**
 * Whether `profile` can sit on the hops in `beneath`, as the daemon rules it.
 * A ladder uses a proxy once. WireGuard and HTTP/3 need UDP, which only the
 * egress or a WireGuard hop beneath them carries, and only WireGuard can ride
 * a WireGuard hop.
 */
function chainHopAllows<T extends ChainProfile>(profiles: T[], beneath: number[], profile: T, limits: ChainLimits): boolean {
  if (beneath.includes(profile.id) || limits.taken?.has(profile.id)) return false;
  if (beneath.length === 0 || (profile.kind !== "WIRE_GUARD" && profile.kind !== "HTTP3_CONNECT")) return true;
  const carrier = profiles.find((candidate) => candidate.id === beneath.at(-1))?.kind;
  return profile.kind === "WIRE_GUARD" && carrier === "WIRE_GUARD";
}
/**
 * The proxies a chain's hop can use: those the hops beneath it allow and no
 * other hop holds. The hop's own choice always stays listed.
 */
export function chainHopProfiles<T extends ChainProfile>(profiles: T[], chainIds: number[], hop: number, limits: ChainLimits = {}): T[] {
  const beneath = chainIds.slice(0, hop);
  return profiles.filter((profile) =>
    profile.id === chainIds[hop]
    || (!chainIds.includes(profile.id) && chainHopAllows(profiles, beneath, profile, limits)));
}
/**
 * The chain with `id` at `hop`, or with that hop taken out when `id` is null.
 * The hops above it stay only while each can still sit on what is now beneath
 * it, so the chain is cut at the first that cannot.
 */
export function withChainHop<T extends ChainProfile>(profiles: T[], chainIds: number[], hop: number, id: number | null, limits: ChainLimits = {}): number[] {
  const ids = chainIds.slice(0, hop);
  if (id !== null) ids.push(id);
  for (const above of chainIds.slice(hop + 1)) {
    const profile = profiles.find((candidate) => candidate.id === above);
    if (!profile || !chainHopAllows(profiles, ids, profile, limits)) break;
    ids.push(above);
  }
  return ids;
}

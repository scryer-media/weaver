import { closedPath, directLeg, routeInput, type NetworkRoute } from "./networking.ts";
export type ProxyKind = "HTTP_CONNECT" | "HTTP3_CONNECT" | "SOCKS5" | "SSH" | "WIRE_GUARD";
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
  dnsServers: string[]; tunnelAddresses: string[]; peerPublicKey: string | null; tunnelPublicKey: string | null;
  mtu: number; keepaliveSeconds: number | null; timeoutSeconds: number; hostKeyFingerprint: string | null;
  hasUsername: boolean; hasPassword: boolean; hasPrivateKey: boolean; hasPassphrase: boolean; hasPresharedKey: boolean;
};

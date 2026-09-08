export type ProxyKind = "HTTP_CONNECT" | "HTTP3_CONNECT" | "SOCKS5" | "SSH" | "WIRE_GUARD";
export const proxyLabels: Record<ProxyKind, string> = { HTTP_CONNECT: "HTTP CONNECT", HTTP3_CONNECT: "HTTP/3 CONNECT", SOCKS5: "SOCKS5", SSH: "SSH", WIRE_GUARD: "WireGuard" };
export type RoutingPolicy = { proxyIds: number[]; allowDirect: boolean };
export type RoutingStatus = { state: string; selectedProxyId: number | null; failures: { proxyId: number; message: string }[] };
export const directRouting: RoutingPolicy = { proxyIds: [], allowDirect: true };
export function appendProxy(policy: RoutingPolicy, id: number): RoutingPolicy {
  if (policy.proxyIds.includes(id) || policy.proxyIds.length >= 8) return policy;
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

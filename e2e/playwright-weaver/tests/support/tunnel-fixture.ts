import type { APIRequestContext } from "@playwright/test";
import { expect } from "../helpers";

/**
 * Control of the tunnel fixture: SSH endpoints ssh1..3 (2221-2223), ssh-switch
 * (2224, host key switchable between "primary" and "other"), ssh-refuse (2225,
 * refuses forwarding), and WireGuard peers wg1, wg2 (preshared key) and
 * wg-rss on UDP 51821-51823. Inside a WireGuard tunnel `nntp.proxy.test` is
 * the NNTP server and `rss.proxy.test` / `download.proxy.test` the HTTP
 * fixture; the tunnel's DNS server answers those names.
 */
const controlUrl = () => (process.env.TUNNEL_FIXTURE_URL || "http://tunnel-fixture:8095").replace(/\/+$/, "");

export const TUNNEL_PORTS = Object.freeze({
  ssh1: 2221, ssh2: 2222, ssh3: 2223, "ssh-switch": 2224, "ssh-refuse": 2225,
  wg1: 51821, wg2: 51822, "wg-rss": 51823,
});
export type TunnelEndpoint = keyof typeof TUNNEL_PORTS;

export type TunnelEvent = {
  sequence: number; kind: "connect" | "disconnect" | "upstream-failed" | "endpoint-up" | "endpoint-down" | "cut" | "host-key";
  endpoint: string; connection?: number; client?: string; clientPort?: number; upstream?: string;
  sessions?: number; hostKey?: string; error?: string; port?: number;
};
export type WireGuardPeer = {
  peerPublicKey: string; presharedKey: string | null; clientPrivateKey: string; clientAddress: string;
  dnsServer: string; dnsQueries: string[]; requests: unknown[]; clientRxBytes: number;
};
export type TunnelState = {
  sequence: number;
  endpoints: Record<string, { transport: "tcp" | "udp"; port: number; up: boolean; active: number; upstream: string }>;
  /** Per SSH endpoint: forwarded (host, port) targets and accepted auth methods, in order. */
  ssh: Record<string, { forwarded: Array<[string, number]>; acceptedAuth: string[] }>;
  sshSwitch: "primary" | "other" | null;
  hostKeys: { primary: string; other: string };
  sshClient: { username: string; password: string; privateKey: string; privateKeyWithPassphrase: string; passphrase: string };
  wireguard: Record<string, WireGuardPeer>;
};

export async function tunnelState(request: APIRequestContext): Promise<TunnelState> {
  const response = await request.get(controlUrl());
  expect(response.ok()).toBeTruthy();
  return await response.json();
}

export async function tunnelMark(request: APIRequestContext): Promise<number> {
  return (await tunnelState(request)).sequence;
}

export async function tunnelEvents(request: APIRequestContext, after: number): Promise<TunnelEvent[]> {
  const response = await request.get(`${controlUrl()}/events?after=${after}`);
  expect(response.ok()).toBeTruthy();
  return (await response.json() as { events: TunnelEvent[] }).events;
}

/** Wait for events after `after` that satisfy `predicate`; returns them. */
export async function waitTunnelEvents(
  request: APIRequestContext, after: number, predicate: (events: TunnelEvent[]) => boolean, describe: string,
): Promise<TunnelEvent[]> {
  let events: TunnelEvent[] = [];
  await expect.poll(async () => predicate(events = await tunnelEvents(request, after)),
    { message: describe, timeout: 0 }).toBe(true);
  return events;
}

export async function controlTunnel(
  request: APIRequestContext,
  command: { endpoint: TunnelEndpoint; up?: boolean; cut?: boolean; hostKey?: "primary" | "other" },
): Promise<void> {
  const response = await request.post(controlUrl(), { data: command });
  expect(response.ok(), await response.text()).toBeTruthy();
}

/** Bring every endpoint up and the switch back to its primary key. */
export async function resetTunnels(request: APIRequestContext): Promise<void> {
  for (const endpoint of Object.keys(TUNNEL_PORTS) as TunnelEndpoint[]) await controlTunnel(request, { endpoint, up: true });
  await controlTunnel(request, { endpoint: "ssh-switch", hostKey: "primary" });
}

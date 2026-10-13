import type { APIRequestContext } from "@playwright/test";
import { expect } from "../helpers";

/**
 * Toxiproxy control for the network-* specs. Proxies are defined in
 * services/toxiproxy/networking.json: `nntp1` (:3119) and `nntp2` (:4119) in
 * front of the NNTP servers, `connect1..6` (:23101-23106) and `socks1..4`
 * (:23201-23204) in front of the proxy fixture's pool members, and
 * `ssh1..3` (:23221-23223) in front of the tunnel fixture. Toxiproxy is TCP
 * only, so WireGuard and HTTP/3 members are failed through their own fixtures.
 */
const apiUrl = () => (process.env.TOXIPROXY_URL || "http://toxiproxy:8474").replace(/\/+$/, "");

export const TOXIPROXY_PORTS = Object.freeze({
  nntp1: 3119, nntp2: 4119,
  connect1: 23101, connect2: 23102, connect3: 23103, connect4: 23104, connect5: 23105, connect6: 23106,
  socks1: 23201, socks2: 23202, socks3: 23203, socks4: 23204,
  ssh1: 23221, ssh2: 23222, ssh3: 23223,
});
export type ToxiproxyName = keyof typeof TOXIPROXY_PORTS;

export type ToxicType = "latency" | "bandwidth" | "slow_close" | "timeout" | "reset_peer" | "slicer" | "limit_data";
export type Toxic = {
  name: string; type: ToxicType; stream: "upstream" | "downstream"; toxicity: number;
  attributes: Record<string, number>;
};
export type ToxiproxyProxy = { name: string; listen: string; upstream: string; enabled: boolean; toxics: Toxic[] };

async function call(request: APIRequestContext, method: "GET" | "POST" | "DELETE", path: string, data?: unknown) {
  const response = await request.fetch(`${apiUrl()}${path}`, { method, ...(data === undefined ? {} : { data }) });
  const body = await response.text();
  expect(response.ok(), `toxiproxy ${method} ${path}: ${body}`).toBeTruthy();
  return body ? JSON.parse(body) : undefined;
}

export async function proxies(request: APIRequestContext): Promise<Record<string, ToxiproxyProxy>> {
  return await call(request, "GET", "/proxies");
}

/** Enable every proxy and remove every toxic. */
export async function resetToxiproxy(request: APIRequestContext): Promise<void> {
  await call(request, "POST", "/reset");
}

/** A disabled proxy refuses new connections and closes the open ones. */
export async function setEnabled(request: APIRequestContext, name: ToxiproxyName, enabled: boolean): Promise<void> {
  await call(request, "POST", `/proxies/${name}`, { enabled });
}

export async function addToxic(
  request: APIRequestContext,
  proxy: ToxiproxyName,
  toxic: { name: string; type: ToxicType; stream?: "upstream" | "downstream"; toxicity?: number; attributes: Record<string, number> },
): Promise<void> {
  await call(request, "POST", `/proxies/${proxy}/toxics`, { stream: "downstream", toxicity: 1, ...toxic });
}

export async function removeToxic(request: APIRequestContext, proxy: ToxiproxyName, name: string): Promise<void> {
  await call(request, "DELETE", `/proxies/${proxy}/toxics/${name}`);
}

/** Added latency in both directions, so connect and handshake times move. */
export async function addLatency(request: APIRequestContext, proxy: ToxiproxyName, latencyMs: number): Promise<void> {
  await addToxic(request, proxy, { name: `${proxy}-latency-down`, type: "latency", stream: "downstream", attributes: { latency: latencyMs, jitter: 0 } });
  await addToxic(request, proxy, { name: `${proxy}-latency-up`, type: "latency", stream: "upstream", attributes: { latency: latencyMs, jitter: 0 } });
}

/** A downstream rate cap in KB/s. */
export async function addBandwidth(request: APIRequestContext, proxy: ToxiproxyName, rateKBps: number): Promise<void> {
  await addToxic(request, proxy, { name: `${proxy}-bandwidth`, type: "bandwidth", attributes: { rate: rateKBps } });
}

/** Swallow data and never answer: a black hole, distinct from a refusal. */
export async function addBlackHole(request: APIRequestContext, proxy: ToxiproxyName): Promise<void> {
  await addToxic(request, proxy, { name: `${proxy}-blackhole`, type: "timeout", attributes: { timeout: 0 } });
}

/** Reset the connection once `bytes` have passed downstream. */
export async function addLimitData(request: APIRequestContext, proxy: ToxiproxyName, bytes: number): Promise<void> {
  await addToxic(request, proxy, { name: `${proxy}-limit`, type: "limit_data", attributes: { bytes } });
}

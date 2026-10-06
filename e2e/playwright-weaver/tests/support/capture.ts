import type { APIRequestContext } from "@playwright/test";
import { expect } from "../helpers";

/**
 * Packet capture in Weaver's own network namespace (the `capture` sidecar).
 * Captures exclude the control, API and Postgres ports, so a count reflects
 * only the traffic under test. `/link` takes an interface down or up, which
 * stands in for unplugging a network without touching the container engine.
 */
const controlUrl = () => (process.env.CAPTURE_URL || "http://weaver:8099").replace(/\/+$/, "");

async function post(request: APIRequestContext, path: string, data: unknown) {
  const response = await request.post(`${controlUrl()}${path}`, { data });
  const body = await response.text();
  expect(response.ok(), `capture POST ${path}: ${body}`).toBeTruthy();
  return JSON.parse(body);
}

/** Start one capture per interface; returns once each is listening. */
export async function startCapture(request: APIRequestContext, name: string): Promise<Array<{ iface: string; path: string }>> {
  return (await post(request, "/start", { name })).captures;
}

export async function stopCapture(request: APIRequestContext): Promise<void> {
  await post(request, "/stop", {});
}

/** Packets in capture `name` on `iface`, optionally narrowed by a tcpdump filter. */
export async function packetCount(request: APIRequestContext, name: string, iface: string, filter = ""): Promise<number> {
  const query = new URLSearchParams({ name, iface, ...(filter ? { filter } : {}) });
  const response = await request.get(`${controlUrl()}/count?${query}`);
  const body = await response.text();
  expect(response.ok(), `capture count ${query}: ${body}`).toBeTruthy();
  return (JSON.parse(body) as { packets: number }).packets;
}

export type NamespaceAddress = { ifname: string; operstate: string; addr_info: Array<{ family: string; local: string; prefixlen: number }> };
export async function namespaceInterfaces(request: APIRequestContext): Promise<NamespaceAddress[]> {
  const response = await request.get(`${controlUrl()}/interfaces`);
  expect(response.ok()).toBeTruthy();
  return await response.json();
}

/** The namespace interface carrying `address`. */
export async function ifaceFor(request: APIRequestContext, address: string): Promise<string> {
  const found = (await namespaceInterfaces(request)).find(entry => entry.addr_info.some(info => info.local === address));
  expect(found, `capture namespace has an interface with ${address}`).toBeTruthy();
  return found!.ifname;
}

export async function setLink(request: APIRequestContext, iface: string, up: boolean): Promise<void> {
  await post(request, "/link", { iface, up });
}

/** tcpdump filter for TCP traffic to `host:port`. */
export const tcpTo = (host: string, port: number) => `tcp and dst host ${host} and dst port ${port}`;
/** tcpdump filter for SYNs to `host` (a connection attempt, not its data). */
export const synTo = (host: string) => `dst host ${host} and (tcp[tcpflags] & tcp-syn) != 0 and (tcp[tcpflags] & tcp-ack) = 0`;

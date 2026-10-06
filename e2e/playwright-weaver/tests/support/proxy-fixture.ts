import type { APIRequestContext } from "@playwright/test";
import { expect } from "../helpers";

/**
 * Control of the proxy fixture (tests/support/proxy-fixture-server.mjs).
 * Routes: primary/tertiary CONNECT, secondary SOCKS5, `direct` (its NNTP
 * port), and pool members connect1..6 (CONNECT, 8101-8106) and socks1..4
 * (SOCKS5, 8201-8204). Every route answers only `*.proxy.test` names and the
 * fixture's own addresses; anything else is refused and recorded.
 */
const controlUrl = () => (process.env.PROXY_FIXTURE_URL || "http://proxy-fixture:8090").replace(/\/+$/, "");

export type FixtureEvent = {
  sequence: number; at: number; kind: string; route?: string; host?: string; port?: number;
  client?: string; status?: number; socksReply?: number; path?: string; name?: string; bytes?: number;
};
export type FixtureState = {
  ip: string; addresses: string[]; ports: Record<string, number>; sequence: number;
  events: FixtureEvent[]; active: Record<string, number>;
};

export async function fixtureState(request: APIRequestContext): Promise<FixtureState> {
  const response = await request.get(controlUrl());
  expect(response.ok()).toBeTruthy();
  return await response.json();
}

export async function fixtureMark(request: APIRequestContext): Promise<number> {
  const response = await request.get(`${controlUrl()}/events?after=${Number.MAX_SAFE_INTEGER}`);
  expect(response.ok()).toBeTruthy();
  return (await response.json() as { sequence: number }).sequence;
}

export async function fixtureEvents(request: APIRequestContext, after: number): Promise<FixtureEvent[]> {
  const response = await request.get(`${controlUrl()}/events?after=${after}`);
  expect(response.ok()).toBeTruthy();
  return (await response.json() as { events: FixtureEvent[] }).events;
}

export async function waitFixtureEvents(
  request: APIRequestContext, after: number, predicate: (events: FixtureEvent[]) => boolean, describe: string,
): Promise<FixtureEvent[]> {
  let events: FixtureEvent[] = [];
  await expect.poll(async () => predicate(events = await fixtureEvents(request, after)),
    { message: describe, timeout: 0 }).toBe(true);
  return events;
}

export type RouteCommand = {
  route: string; up?: boolean; hold?: boolean; cut?: boolean;
  /** CONNECT answers this status instead of 200; null restores normal service. */
  connectStatus?: number | null;
  /** SOCKS5 answers this reply code instead of success; null restores it. */
  socksReply?: number | null;
};
export async function controlRoute(request: APIRequestContext, command: RouteCommand): Promise<void> {
  const response = await request.post(controlUrl(), { data: command });
  expect(response.ok(), await response.text()).toBeTruthy();
}

export async function setFixtureNzb(request: APIRequestContext, nzb: string, feedToken?: string): Promise<void> {
  const response = await request.post(controlUrl(), { data: { nzb, ...(feedToken ? { feedToken } : {}) } });
  expect(response.ok(), await response.text()).toBeTruthy();
}

export const CONNECT_MEMBERS = ["connect1", "connect2", "connect3", "connect4", "connect5", "connect6"] as const;
export const SOCKS_MEMBERS = ["socks1", "socks2", "socks3", "socks4"] as const;

/** Restore every route the specs touch to normal service. */
export async function resetRoutes(request: APIRequestContext): Promise<void> {
  for (const route of ["primary", "secondary", "tertiary", "direct", ...CONNECT_MEMBERS, ...SOCKS_MEMBERS]) {
    await controlRoute(request, { route, up: true, hold: false, connectStatus: null, socksReply: null });
  }
}

/** Attempts through `route` after the watermark, in order. */
export const attemptsOn = (events: FixtureEvent[], route: string) => events.filter(event => event.kind === "attempt" && event.route === route);
/** Every connection that bypassed the proxies (host DNS or direct NNTP/HTTP). */
export const directEvents = (events: FixtureEvent[]) => events.filter(event => event.kind.startsWith("direct-"));

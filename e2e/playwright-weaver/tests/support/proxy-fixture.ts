// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
/** A route's service as the control last set it. */
export type RouteService = { up: boolean; hold: boolean; connectStatus: number | null; socksReply: number | null };
export type FixtureState = {
  ip: string; addresses: string[]; ports: Record<string, number>; sequence: number;
  events: FixtureEvent[]; active: Record<string, number>; routes: Record<string, RouteService>;
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

/** Normal service: up, not held, and answering CONNECT and SOCKS5 as it should. */
export const inService = (service: RouteService | undefined): boolean =>
  service !== undefined && service.up && !service.hold && service.connectStatus === null && service.socksReply === null;

/**
 * Wait until the fixture itself reports every named route in normal service.
 * With no names, every route it has.
 */
export async function waitRoutesInService(request: APIRequestContext, names?: string[]): Promise<void> {
  let state = await fixtureState(request);
  if (!state.routes) throw new Error("the proxy fixture does not report its routes' service");
  const wanted = names ?? Object.keys(state.routes);
  const unknown = wanted.filter(name => !(name in state.routes));
  if (unknown.length) throw new Error(`the proxy fixture has no route ${unknown.join(", ")}`);
  await expect.poll(async () => {
    state = await fixtureState(request);
    return wanted.filter(name => !inService(state.routes[name]));
  }, { message: `proxy fixture routes ${wanted.join(", ")} in service`, timeout: 0 }).toEqual([]);
}

/**
 * Restore every route the fixture has to normal service, each on its own so
 * one refused command leaves no later route cut, then wait until the fixture
 * reports every one of them back.
 */
export async function resetRoutes(request: APIRequestContext): Promise<void> {
  const state = await fixtureState(request);
  if (!state.routes) throw new Error("the proxy fixture does not report its routes' service");
  const failures: string[] = [];
  for (const route of Object.keys(state.routes)) {
    try {
      await controlRoute(request, { route, up: true, hold: false, connectStatus: null, socksReply: null });
    } catch (error) {
      failures.push(`${route}: ${String(error)}`);
    }
  }
  if (failures.length) throw new Error(`restore proxy fixture routes: ${failures.join("; ")}`);
  await waitRoutesInService(request);
}

/** Attempts through `route` after the watermark, in order. */
export const attemptsOn = (events: FixtureEvent[], route: string) => events.filter(event => event.kind === "attempt" && event.route === route);
/** Every connection that bypassed the proxies (host DNS or direct NNTP/HTTP). */
export const directEvents = (events: FixtureEvent[]) => events.filter(event => event.kind.startsWith("direct-"));

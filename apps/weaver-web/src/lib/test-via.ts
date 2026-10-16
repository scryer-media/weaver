// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import { directLeg, type Egress, type NetworkRoute, type ProxyPool } from "./networking.ts";
import { proxyTestSentence } from "./proxy-fault.ts";
import { codedSentence } from "./refusal.ts";

/**
 * What a test stacks on top of the route it is drafted through: one proxy,
 * a pool, or nothing, when the route itself is under test. A pool carries a
 * destination through the member it picks, and without one each member is
 * tried on its own. Shaped as the daemon's `TestSubjectInput` takes it.
 */
export type TestSubject = { proxy: number } | { pool: number } | null;

/** One path through a drafted route, and how the test along it went. */
export type PathTestResult = {
  /** The route leg the path came from. */
  leg: number;
  egressId: number;
  /** The proxies the path rides, nearest the egress first; none is direct. */
  carrierIds: number[];
  /** The proxy tested on top of the path, or the member the pool under test picked. */
  proxyId: number | null;
  /** The pool under test, when it carried the destination. */
  poolId: number | null;
  success: boolean;
  code: string;
  /** The daemon's English, for a code this build has no sentence for. */
  message: string;
  params: unknown;
  /** The hop that failed, a carrier or the subject; none when the egress or the target did. */
  failingProxyId: number | null;
  sourceAddress: string | null;
  connectMillis: number;
  /** How the host the path's final dial named was resolved. */
  resolution: HostResolution | null;
};

/** Who turned a test's host into addresses. */
export type ResolvedBy = "LITERAL" | "HOST" | "SERVERS" | "PROXY" | "PROXY_SERVERS";

/** How a test's host was resolved, as the daemon reports it. */
export type HostResolution = {
  host: string;
  resolvedBy: ResolvedBy;
  /** The proxy that resolved the name, or that the servers were asked through. */
  proxyId: number | null;
  dnsServers: string[];
  addresses: string[];
  /** The answer was held from an earlier lookup. */
  cached: boolean;
  connectedAddress: string | null;
  /** A failed lookup: its code, and the resolver's own words. */
  error: { code: string; detail: string } | null;
};

/** Where the words for a failed lookup's code live in the dictionary. */
export const lookupErrorKey = (code: string) => `next.lookupError.${code}`;

/** A resolution's line in parts: the addresses it found, who resolved it, and how that went. */
export type ResolutionParts = {
  host: string;
  /** Empty for an address, which is its own answer, and for a name handed to a proxy. */
  addresses: { address: string; connected: boolean }[];
  /** Who resolved it, a failed lookup's words, and whether the answer was held. */
  said: string;
  /** The resolver's own words, when the lookup failed. */
  detail: string | null;
};

/** The parts of a path's resolution line, naming its egress and proxies. */
export function resolutionParts(
  t: Translate,
  resolution: HostResolution,
  names: { egress: string; proxy: (id: number) => string },
): ResolutionParts {
  const proxy = resolution.proxyId === null ? "" : names.proxy(resolution.proxyId);
  const servers = resolution.dnsServers.join(", ");
  const by: Record<ResolvedBy, string> = {
    LITERAL: t("next.networking.testVia.resolvedLiteral"),
    HOST: t("next.networking.testVia.resolvedHost"),
    SERVERS: t("next.networking.testVia.resolvedServers", { servers, name: names.egress }),
    PROXY: t("next.networking.testVia.resolvedProxy", { name: proxy }),
    PROXY_SERVERS: t("next.networking.testVia.resolvedServers", { servers, name: proxy }),
  };
  const error = resolution.error
    ? codedSentence(t, lookupErrorKey(resolution.error.code), undefined, resolution.error.detail)
    : null;
  const marked = resolution.addresses.length > 1;
  return {
    host: resolution.host,
    addresses:
      resolution.resolvedBy === "LITERAL"
        ? []
        : resolution.addresses.map((address) => ({
            address,
            connected: marked && address === resolution.connectedAddress,
          })),
    said: [
      [by[resolution.resolvedBy], error].filter(Boolean).join(": "),
      ...(resolution.cached ? [t("next.networking.testVia.cached")] : []),
    ]
      .filter(Boolean)
      .join(" · "),
    detail: resolution.error?.detail ?? null,
  };
}

/** A resolution line as plain text. */
export function resolutionText(parts: ResolutionParts): string {
  const addresses = parts.addresses.map((part) => part.address).join(", ");
  return [addresses ? `${parts.host} → ${addresses}` : parts.host, parts.said].filter(Boolean).join(" · ");
}

/** The proxies the subject puts on top of the route: a proxy, or a pool's members. */
export function subjectProxyIds(subject: TestSubject, pools: ProxyPool[]): number[] {
  if (subject === null) return [];
  if ("proxy" in subject) return [subject.proxy];
  return pools.find((pool) => pool.id === subject.pool)?.memberIds ?? [];
}

/**
 * What the drafted route may use. A proxy cannot carry itself, so neither the
 * subject's proxies nor a pool holding one of them is offered.
 */
export function routeChoices<P extends { id: number }>(
  profiles: P[],
  pools: ProxyPool[],
  subject: TestSubject,
): { profiles: P[]; pools: ProxyPool[] } {
  const tested = new Set(subjectProxyIds(subject, pools));
  const ownPool = subject !== null && "pool" in subject ? subject.pool : null;
  return {
    profiles: profiles.filter((profile) => !tested.has(profile.id)),
    pools: pools.filter((pool) => pool.id !== ownPool && !pool.memberIds.some((id) => tested.has(id))),
  };
}

/** The route a test starts on: one direct leg, on `egressId` when given, else on the first enabled egress. */
export function draftRoute(egresses: Pick<Egress, "id" | "enabled">[], egressId?: number): NetworkRoute {
  const start = egressId ?? egresses.find((egress) => egress.enabled)?.id ?? directLeg().egressId;
  return { legs: [{ ...directLeg(), egressId: start }], failover: "REDISTRIBUTE" };
}

/** A run in one sentence: its first failing path's, else its first path's. */
export function testSummary(t: Translate, results: readonly PathTestResult[]): string | null {
  const shown = results.find((result) => !result.success) ?? results[0];
  return shown ? proxyTestSentence(t, shown) : null;
}

import type { Translate } from "@/lib/context/translate-context";
import { directLeg, type Egress, type NetworkRoute, type ProxyPool } from "./networking.ts";
import { proxyTestSentence } from "./proxy-fault.ts";

/**
 * What a test stacks on top of the route it is drafted through: one proxy,
 * each member of a pool, or nothing, when the route itself is under test.
 * Shaped as the daemon's `TestSubjectInput` takes it.
 */
export type TestSubject = { proxy: number } | { pool: number } | null;

/** One path through a drafted route, and how the test along it went. */
export type PathTestResult = {
  /** The route leg the path came from. */
  leg: number;
  egressId: number;
  /** The proxies the path rides, nearest the egress first; none is direct. */
  carrierIds: number[];
  /** The proxy tested on top of the path, when the test has a subject. */
  proxyId: number | null;
  success: boolean;
  code: string;
  /** The daemon's English, for a code this build has no sentence for. */
  message: string;
  params: unknown;
  /** The hop that failed, a carrier or the subject; none when the egress or the target did. */
  failingProxyId: number | null;
  sourceAddress: string | null;
  connectMillis: number;
};

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

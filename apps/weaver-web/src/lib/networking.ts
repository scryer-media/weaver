// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ProxyKind } from "./proxies";

export type Rung = { kind: "PROXY" | "POOL" | "CHAIN"; proxyId: number | null; poolId: number | null; chainIds: number[] };
export type Leg = { egressId: number; weight: number; path: { kind: "DIRECT" | "LADDER"; rungs: Rung[]; directFallback: boolean } };
export type NetworkRoute = { legs: Leg[]; failover: "REDISTRIBUTE" | "HOLD" };
export type Egress = { addresses?:string[]; id: number; name: string; bindingKind: "SYSTEM" | "INTERFACE" | "SOURCE_ADDRESS"; interfaceName: string | null; sourceAddress: string | null; enabled: boolean; downloadBps: number | null; uploadBps: number | null; quota?: EgressQuota; quotaUsage?: EgressQuotaUsage | null; health: string; reason: string | null };
export type QuotaPeriod = "ONE_TIME" | "DAILY" | "WEEKLY" | "MONTHLY";
export type Weekday = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";
/** A server's download allowance as it is configured. */
export type DownloadQuota = { enabled: boolean; period: QuotaPeriod; limitBytes: number; resetTimeMinutesLocal: number; weeklyResetWeekday: Weekday; monthlyResetDay: number };
/** An egress's caps per period, in bytes; null is no cap, and 0 a budget of nothing. */
export type EgressQuota = { downloadBytes: number | null; uploadSoftBytes: number | null; uploadTrickleBps: number; uploadHardBytes: number | null; period: QuotaPeriod; resetTimeMinutesLocal: number; weeklyResetWeekday: Weekday; monthlyResetDay: number };
/** Where a direction stands against its caps: open, slowed to the trickle, or stopped. */
export type QuotaState = "NORMAL" | "TRICKLE" | "STOPPED";
/** One direction's spend this period; null `remainingBytes` means it has no cap. */
export type QuotaDirectionUsage = { usedBytes: number; remainingBytes: number | null; state: QuotaState };
/** An egress's spend this period in each direction; `state` is the tighter of the two. */
export type EgressQuotaUsage = { down: QuotaDirectionUsage; up: QuotaDirectionUsage; state: QuotaState; periodStartEpochMs: number | null; periodEndEpochMs: number | null; timezoneName: string };
/** Bytes per second in each direction over every egress together; null is no limit. */
/** Usenet's percentage of the global download limit; torrents get the rest. */
export type BandwidthSplit = { usenetPct: number };
export type Bandwidth = { downloadBps: number | null; uploadBps: number | null; split: BandwidthSplit | null };
/** The split in force and the download limit each side holds to; null is no limit. */
export type BandwidthSplitState = {
  inForce: BandwidthSplit | null;
  needsDownloadLimit: boolean;
  usenetBps: number | null;
  torrentBps: number | null;
};
export type ProxyPool = { id: number; name: string; kind: ProxyKind; memberIds: number[]; enabled: boolean };
export type LegFlow = Leg & { rungStates?:string[]; consumer: string; position: number; target: number; open: number; opening: number; state: string; reason: string | null; pinnedAddress: string | null; sourceAddress?:string|null;bytesPerSecond?:number;selectedRung?:number|null;selectedProxyId?:number|null;failingHops?:FailingHop[] };
/**
 * The first proxy hop on a ladder rung known to be failing, and what its last
 * attempt came to: a code, the values its sentence names, and the daemon's English.
 */
export type FailingHop = { rung: number; proxyId: number; reason: string; code: string; params?: unknown };
export type PoolMemberFlow = { state?:string; id: number; open: number; opening: number; warmed: boolean; blocked: string | null; connectMs: number | null; bytesPerSecond: number | null; liveBytesPerSecond: number; samples: number; failures: number };
export type NetworkFlow = { consumers?:{key:string;id:number;name:string;kind:ConsumerKind;cap:number;route:NetworkRoute}[]; proxies?:import("./proxies").ProxyProfile[]; proxyPools?:ProxyPool[]; egresses?:Egress[]; sampledAt?:number;legs: LegFlow[]; pools: PoolFlow[] };
/** A pool on one egress as it runs: `liveBytesPerSecond` on a member is what it carries now, `bytesPerSecond` what the pool judges it by. */
export type PoolFlow = { poolId: number; egressId: number; pinnedMember: number | null; members: PoolMemberFlow[] };
/**
 * What owns a route: a server, a feed, Default outbound, which carries every
 * other fetch, or torrent traffic: the default route, or one category's own.
 */
export type ConsumerKind = "SERVER" | "RSS" | "OUTBOUND" | "TORRENT_DEFAULT" | "TORRENT_CATEGORY";
/** Whether a consumer carries torrent traffic, whose route has one leg and no pool. */
export const isTorrentConsumer = (kind: ConsumerKind) => kind === "TORRENT_DEFAULT" || kind === "TORRENT_CATEGORY";
/** Default outbound's key: there is one, and it is never made or removed. */
export const OUTBOUND_CONSUMER = "outbound";
/** The default torrent route's key: there is one, like Default outbound. */
export const TORRENT_DEFAULT_CONSUMER = "torrent:default";
export const directLeg = (): Leg => ({ egressId: 0, weight: 100, path: { kind: "DIRECT", rungs: [], directFallback: false } });
/** A path nothing can take: a ladder with no rung on it and no going direct. It is how a kill switch is stored. */
export const closedPath = (path: Leg["path"]) => path.kind === "LADDER" && !path.rungs.length && !path.directFallback;
export function routeInput(route: NetworkRoute) {
  return { failover: route.failover, legs: route.legs.map(leg => ({ egressId: leg.egressId, weight: leg.weight,
    path: leg.path.kind === "DIRECT" ? { direct: true } : { ladder: { directFallback: leg.path.directFallback, rungs: leg.path.rungs.map(r => r.kind === "PROXY" ? { proxy: r.proxyId } : r.kind === "POOL" ? { pool: r.poolId } : { chain: r.chainIds }) } },
  })) };
}
/** Why a route cannot be saved, as a code the interface words for itself. */
export type RouteProblem = "legCount" | "weights" | "rungCount" | "rungTarget" | "chain" | "egressDisabled" | "proxyRepeated" | "torrentLegs" | "torrentPool";
/** The proxies a rung uses, a pool's members included. */
export function rungProxyIds(rung: Rung, pools: ProxyPool[]): number[] {
  if (rung.kind === "PROXY") return rung.proxyId === null ? [] : [rung.proxyId];
  if (rung.kind === "POOL") return pools.find(pool => pool.id === rung.poolId)?.memberIds ?? [];
  return rung.chainIds;
}
export function routeProblem(route: NetworkRoute, known: { egresses?: Egress[]; pools?: ProxyPool[]; kind?: ConsumerKind } = {}): RouteProblem | null {
  if (!route.legs.length || route.legs.length > 8) return "legCount";
  // A torrent route takes exactly one leg, and no pool on it.
  const torrent = known.kind !== undefined && isTorrentConsumer(known.kind);
  if (torrent && route.legs.length !== 1) return "torrentLegs";
  if (torrent && route.legs.some(leg => leg.path.kind === "LADDER" && leg.path.rungs.some(rung => rung.kind === "POOL"))) return "torrentPool";
  if (route.legs.some(l => !Number.isInteger(l.weight) || l.weight < 1 || l.weight > 100) || route.legs.reduce((sum,l) => sum+l.weight,0) !== 100) return "weights";
  for (const leg of route.legs) {
    if (leg.path.kind === "DIRECT") continue;
    if (!leg.path.rungs.length || leg.path.rungs.length > 8) return "rungCount";
    for (const rung of leg.path.rungs) {
      if (rung.kind === "PROXY" && !rung.proxyId || rung.kind === "POOL" && !rung.poolId) return "rungTarget";
      if (rung.kind === "CHAIN" && (rung.chainIds.length < 2 || rung.chainIds.length > 3 || new Set(rung.chainIds).size !== rung.chainIds.length)) return "chain";
    }
    const used = leg.path.rungs.flatMap(rung => rungProxyIds(rung, known.pools ?? []));
    if (new Set(used).size !== used.length) return "proxyRepeated";
  }
  if (route.legs.some(leg => known.egresses?.find(egress => egress.id === leg.egressId)?.enabled === false)) return "egressDisabled";
  return null;
}
export function allocation(weights: number[], cap: number): number[] {
  const total = weights.reduce((a,b) => a+b,0);
  if (total <= 0) return weights.map(() => 0);
  const targets = weights.map(w => Math.floor(w*cap/total));
  const order = weights.map((w,i) => ({ i, remainder: w*cap%total })).sort((a,b) => b.remainder-a.remainder || a.i-b.i);
  const remaining = cap-targets.reduce((a,b) => a+b,0);
  for (let i=0;i<remaining;i++) targets[order[i]!.i]!++;
  return targets;
}

export function routeTargets(route:NetworkRoute,cap:number,down:ReadonlySet<number>):number[] {
  const weights=route.legs.map((leg,i)=>route.failover==="REDISTRIBUTE"&&down.has(i)?0:leg.weight);
  return allocation(weights,cap).map((target,i)=>down.has(i)?0:target);
}

export function adjustWeight(legs:Leg[],position:number,requested:number):Leg[] {
  if(legs.length<2||!Number.isFinite(requested))return legs;
  const neighbor=position+1<legs.length?position+1:position-1;
  const pair=legs[position]!.weight+legs[neighbor]!.weight;
  const weight=Math.max(1,Math.min(pair-1,Math.round(requested)));
  return legs.map((leg,i)=>i===position?{...leg,weight}:i===neighbor?{...leg,weight:pair-weight}:leg);
}

export function appendLeg(legs:Leg[]):Leg[] {
  if(legs.length>=8)return legs;
  const largest=legs.reduce((best,leg,i)=>leg.weight>legs[best]!.weight?i:best,0);
  const weight=Math.max(1,Math.floor(legs[largest]!.weight/2));
  return [...legs.map((leg,i)=>i===largest?{...leg,weight:leg.weight-weight}:leg),{...directLeg(),weight}];
}

export function removeLeg(legs:Leg[],position:number):Leg[] {
  if(legs.length<2)return legs;
  const neighbor=position===0?1:position-1;
  return legs.map((leg,i)=>i===neighbor?{...leg,weight:leg.weight+legs[position]!.weight}:leg).filter((_,i)=>i!==position);
}

/** What the operator took down for one speed test. Positions index the route as it is tested. */
export type PathOverride = { downLegs: number[]; downRungs: { leg: number; rung: number }[]; downProxies: number[]; downEgresses: number[] };
export const noOverrides = (): PathOverride => ({ downLegs: [], downRungs: [], downProxies: [], downEgresses: [] });
/** One box a speed test can take down: a leg, a rung inside its leg, a proxy wherever it is used, or an egress. */
export type DownTarget = { kind: "leg"; position: number } | { kind: "rung"; leg: number; rung: number } | { kind: "proxy"; id: number } | { kind: "egress"; id: number };
export function isDown(overrides: PathOverride, target: DownTarget): boolean {
  switch (target.kind) {
    case "leg": return overrides.downLegs.includes(target.position);
    case "rung": return overrides.downRungs.some(r => r.leg === target.leg && r.rung === target.rung);
    case "proxy": return overrides.downProxies.includes(target.id);
    case "egress": return overrides.downEgresses.includes(target.id);
  }
}
export function toggleDown(overrides: PathOverride, target: DownTarget): PathOverride {
  const flip = (ids: number[], id: number) => ids.includes(id) ? ids.filter(x => x !== id) : [...ids, id].sort((a,b) => a-b);
  switch (target.kind) {
    case "leg": return { ...overrides, downLegs: flip(overrides.downLegs, target.position) };
    case "proxy": return { ...overrides, downProxies: flip(overrides.downProxies, target.id) };
    case "egress": return { ...overrides, downEgresses: flip(overrides.downEgresses, target.id) };
    case "rung": return { ...overrides, downRungs: isDown(overrides, target)
      ? overrides.downRungs.filter(r => r.leg !== target.leg || r.rung !== target.rung)
      : [...overrides.downRungs, { leg: target.leg, rung: target.rung }].sort((a,b) => a.leg-b.leg || a.rung-b.rung) };
  }
}
/**
 * The route as a speed test takes it with these boxes down: which legs are down and
 * which rungs each leg loses. A pool rung is down when every member is, a chain when
 * any hop is, and a leg when it is downed, its egress is, or its ladder loses every
 * rung with no going direct.
 */
export function testedPlan(route: NetworkRoute, overrides: PathOverride, pools: ProxyPool[]): { legDown: boolean[]; rungDown: boolean[][] } {
  const proxyDown = (id: number | null) => id !== null && overrides.downProxies.includes(id);
  const rungDown = route.legs.map((leg, position) => (leg.path.kind === "DIRECT" ? [] : leg.path.rungs).map((rung, index) => {
    if (isDown(overrides, { kind: "rung", leg: position, rung: index })) return true;
    if (rung.kind === "PROXY") return proxyDown(rung.proxyId);
    if (rung.kind === "CHAIN") return rung.chainIds.some(proxyDown);
    const members = pools.find(pool => pool.id === rung.poolId)?.memberIds ?? [];
    return members.length > 0 && members.every(proxyDown);
  }));
  const legDown = route.legs.map((leg, position) =>
    overrides.downLegs.includes(position) || overrides.downEgresses.includes(leg.egressId) ||
    (leg.path.kind === "LADDER" && !leg.path.directFallback && rungDown[position]!.every(Boolean)));
  return { legDown, rungDown };
}
export type SpeedTestKind = "BANDWIDTH" | "USENET";
export type SpeedTestStatus = "RUNNING" | "COMPLETED" | "CANCELLED" | "FAILED";
export type SabTestSize = "MB_100" | "GB_1" | "GB_10";
/** `host` is where a run downloads from; null for a provider whose servers each carry their own. */
export type BandwidthProvider = { id: string; name: string; host: string | null; needsServer: boolean };
export type BandwidthProviderServer = { id: string; name: string; location: string | null; host: string };
/**
 * A bandwidth run picks its own connections and ends itself once its rate holds steady.
 * A Usenet run takes exactly one NZB source: a SAB size, a URL, an uploaded file, or a
 * held run's uploaded file again.
 */
export type SpeedTestInput = {
  kind: ConsumerKind; id: number; route?: ReturnType<typeof routeInput> | null; overrides?: PathOverride | null;
  bandwidth?: { providerId?: string | null; server?: string | null; customUrl?: string | null } | null;
  usenet?: {
    sabSize?: SabTestSize | null; nzbUrl?: string | null; nzbUpload?: File | null; nzbRunId?: number | null;
    connections?: number | null; maxDurationSeconds: number;
  } | null;
};
export type TestedLeg = { position: number; egressId: number; weight: number; down: boolean; downReason: string | null; path: Leg["path"]; target: number };
export type SpeedTestResult = {
  bytes: number; elapsedMs: number; averageBps: number; peakBps: number;
  legs: { position: number; bytes: number; averageBps: number; peakBps: number; connections: number; sourceAddresses: string[] }[];
  connections: { leg: number; connectMs: number; path: { egressId: number; rung: number | null; poolId: number | null; memberId: number | null; proxyIds: number[] }; sourceAddress: string | null; bytes: number; bytesPerSecond: number; error: string | null }[];
  usenet: { articlesRequested: number; delivered: number; missing: number; failed: number; declaredBytes: number; decodedBytes: number; pipelining: boolean; tls: boolean; tlsCipherSuite: string | null } | null;
  /** `connections` is how many the run had open when it ended. */
  bandwidth: { providerId: string | null; server: string | null; url: string; connections: number } | null;
  /** One per leg each second; `decodedBytes` is 0 for a bandwidth run. */
  samples: { atMs: number; leg: number; bytes: number; decodedBytes: number }[];
};
/**
 * What a run was asked for, as the daemon kept it: exactly one of bandwidth and usenet.
 * An uploaded NZB is named by `nzbFilename`; `nzbRunId` is set when it came from an earlier run.
 */
export type SpeedTestRequest = {
  bandwidth: { providerId: string | null; server: string | null; customUrl: string | null } | null;
  usenet: {
    sabSize: SabTestSize | null; nzbUrl: string | null; nzbFilename: string | null; nzbRunId: number | null;
    connections: number | null; maxDurationSeconds: number;
  } | null;
};
/** Why a run failed: the web words each code; OTHER, or a code it does not know, shows `message`. */
export type SpeedTestFailureCode =
  | "RATE_LIMITED" | "HTTP_STATUS" | "TLS_VERIFICATION" | "UNRESOLVED" | "ADDRESS_NOT_ALLOWED"
  | "EGRESS_QUOTA_USED" | "NO_LEG_UP" | "LEASE_REFUSED" | "SERVER_LEFT_POOL" | "STOPPED" | "STOPPED_UNEXPECTEDLY"
  | "NO_BYTES" | "OTHER";
/** `message` is the daemon's own account, naming the URL, host or status; `params` holds what the sentence names. */
export type SpeedTestFailure = { code: SpeedTestFailureCode; message: string; params: Record<string, unknown> };
export type SpeedTestRun = {
  id: number; consumer: string; consumerName: string; kind: SpeedTestKind; request: SpeedTestRequest; status: SpeedTestStatus; phase: string | null;
  /** Epoch milliseconds. */
  startedAt: number; finishedAt: number | null; failure: SpeedTestFailure | null;
  tested: { legs: TestedLeg[]; failover: NetworkRoute["failover"]; overrides: PathOverride; names: { kind: string; id: number; name: string }[] };
  result: SpeedTestResult | null;
};
/** The route a run took, as a route again: what "Run again" sends. */
export const testedRoute = (run: SpeedTestRun): NetworkRoute => ({ failover: run.tested.failover, legs: run.tested.legs.map(leg => ({ egressId: leg.egressId, weight: leg.weight, path: leg.path })) });

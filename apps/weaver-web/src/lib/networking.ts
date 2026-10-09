import type { ProxyKind } from "./proxies";

export type Rung = { kind: "PROXY" | "POOL" | "CHAIN"; proxyId: number | null; poolId: number | null; chainIds: number[] };
export type Leg = { egressId: number; weight: number; path: { kind: "DIRECT" | "LADDER"; rungs: Rung[]; directFallback: boolean } };
export type NetworkRoute = { legs: Leg[]; failover: "REDISTRIBUTE" | "HOLD" };
export type Egress = { addresses?:string[]; id: number; name: string; bindingKind: "SYSTEM" | "INTERFACE" | "SOURCE_ADDRESS"; interfaceName: string | null; sourceAddress: string | null; enabled: boolean; maxDownloadSpeed: number; downloadQuota?: DownloadQuota; downloadQuotaUsage?: DownloadQuotaUsage | null; health: string; reason: string | null };
export type QuotaPeriod = "ONE_TIME" | "DAILY" | "WEEKLY" | "MONTHLY";
export type Weekday = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";
/** A download allowance as it is configured, on a server or an egress. */
export type DownloadQuota = { enabled: boolean; period: QuotaPeriod; limitBytes: number; resetTimeMinutesLocal: number; weeklyResetWeekday: Weekday; monthlyResetDay: number };
/** How much of an egress's allowance is spent; null `remainingBytes` means it has none. */
export type DownloadQuotaUsage = { usedBytes: number; reservedBytes: number; remainingBytes: number | null; blocked: boolean; windowStartsAtEpochMs: number | null; windowEndsAtEpochMs: number | null; timezoneName: string };
export type ProxyPool = { id: number; name: string; kind: ProxyKind; memberIds: number[]; enabled: boolean };
export type LegFlow = Leg & { rungStates?:string[]; consumer: string; position: number; target: number; open: number; opening: number; state: string; reason: string | null; pinnedAddress: string | null; sourceAddress?:string|null;bytesPerSecond?:number;selectedRung?:number|null;selectedProxyId?:number|null;failingHops?:FailingHop[] };
/** The first proxy hop on a ladder rung known to be failing, and what its last attempt came to. */
export type FailingHop = { rung: number; proxyId: number; reason: string };
export type PoolMemberFlow = { state?:string; id: number; open: number; opening: number; warmed: boolean; blocked: string | null; handshakeMs: number | null; connectMs: number | null; bytesPerSecond: number | null; samples: number; failures: number };
export type NetworkFlow = { consumers?:{key:string;id:number;name:string;kind:"SERVER"|"RSS";cap:number;route:NetworkRoute}[]; proxies?:import("./proxies").ProxyProfile[]; proxyPools?:ProxyPool[]; egresses?:Egress[]; sampledAt?:number;legs: LegFlow[]; pools: { poolId: number; egressId: number; pinnedMember: number | null; members: PoolMemberFlow[] }[] };
export const directLeg = (): Leg => ({ egressId: 0, weight: 100, path: { kind: "DIRECT", rungs: [], directFallback: false } });
/** A path nothing can take: a ladder with no rung on it and no going direct. It is how a kill switch is stored. */
export const closedPath = (path: Leg["path"]) => path.kind === "LADDER" && !path.rungs.length && !path.directFallback;
export function routeInput(route: NetworkRoute) {
  return { failover: route.failover, legs: route.legs.map(leg => ({ egressId: leg.egressId, weight: leg.weight,
    path: leg.path.kind === "DIRECT" ? { direct: true } : { ladder: { directFallback: leg.path.directFallback, rungs: leg.path.rungs.map(r => r.kind === "PROXY" ? { proxy: r.proxyId } : r.kind === "POOL" ? { pool: r.poolId } : { chain: r.chainIds }) } },
  })) };
}
/** Why a route cannot be saved, as a code the interface words for itself. */
export type RouteProblem = "legCount" | "weights" | "rungCount" | "rungTarget" | "chain";
export function routeProblem(route: NetworkRoute): RouteProblem | null {
  if (!route.legs.length || route.legs.length > 8) return "legCount";
  if (route.legs.some(l => !Number.isInteger(l.weight) || l.weight < 1 || l.weight > 100) || route.legs.reduce((sum,l) => sum+l.weight,0) !== 100) return "weights";
  for (const leg of route.legs) {
    if (leg.path.kind === "DIRECT") continue;
    if (!leg.path.rungs.length || leg.path.rungs.length > 8) return "rungCount";
    for (const rung of leg.path.rungs) {
      if (rung.kind === "PROXY" && !rung.proxyId || rung.kind === "POOL" && !rung.poolId) return "rungTarget";
      if (rung.kind === "CHAIN" && (rung.chainIds.length < 2 || rung.chainIds.length > 3 || new Set(rung.chainIds).size !== rung.chainIds.length)) return "chain";
    }
  }
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

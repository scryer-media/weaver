// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import type { LegFlow, NetworkRoute, ProxyPool, Rung } from "./networking.ts";
import type { ProxyKind } from "./proxies.ts";

/** Who answers the names a route dials: the host's resolver, the leg's proxy, or Weaver asking listed servers. */
export type DnsWho = "SYSTEM" | "PROXY" | "WEAVER";
/** Where Weaver asks its listed servers from: the leg's egress, or the last hop of its proxy. */
export type DnsTransit = "EGRESS" | "PROXY";
/** What a profile serves: the names a route dials, or the address of a proxy given as a name. */
export type DnsProfileKind = "ROUTE" | "PROXY_NAME";
/** What an answer must carry to be used. */
export type DnsTrust = "AS_SENT" | "VALIDATE";
/** How a query travels to one server. */
export type DnsWire = "PLAIN" | "TLS" | "HTTPS";
/** What a lookup's answer was taken as. */
export type LookupTrust = "AS_SENT" | "VALIDATED" | "UNSIGNED" | "BOGUS";

export type DnsServer = { wire: DnsWire; address: string; tlsName: string | null; url: string | null };
export type DnsServerList = { id: number; name: string; servers: DnsServer[]; usedByProfiles: string[] };
export type DnsProfile = {
  id: number;
  name: string;
  builtIn: boolean;
  kind: DnsProfileKind;
  who: DnsWho;
  transit: DnsTransit | null;
  trust: DnsTrust;
  serverListId: number | null;
  /** PROXY only: a leg with no proxy asks the system rather than resolving nothing. */
  systemFallback: boolean;
  usedByRoutes: number;
  usedByProxies: number;
};
/** A profile as a route's DNS select reads it: System and the route profiles, read by kind. */
export type DnsProfileChoice = Pick<DnsProfile, "id" | "name" | "who" | "trust" | "systemFallback">;
/** A profile as a proxy's editor offers it: its name alone, System and the proxy name profiles, read by kind. */
export type DnsProfileName = Pick<DnsProfile, "id" | "name">;
/** What a profile's editor starts from. A proxy name profile has no transit and no system fallback. */
export type DnsProfileEdited = Pick<DnsProfile, "id" | "name" | "kind" | "who" | "trust" | "serverListId"> &
  Partial<Pick<DnsProfile, "transit" | "systemFallback">>;
/** A proxy name profile as the DNS page reads it, counted by the proxies that use it. */
export type DnsNameProfileRow = Pick<DnsProfile, "id" | "name" | "builtIn" | "who" | "trust" | "serverListId" | "usedByProxies">;
/** A route profile as the DNS page reads it, counted by the routes that use it. */
export type DnsRouteProfileRow = Pick<
  DnsProfile,
  "id" | "name" | "builtIn" | "who" | "transit" | "trust" | "serverListId" | "systemFallback" | "usedByRoutes"
>;

/** The built-in System profile: every route starts on it, and it cannot be changed. */
export const SYSTEM_DNS_PROFILE_ID = 0;
/** The daemon keeps at most this many servers on one list. */
export const MAX_DNS_SERVERS = 10;
export const DNS_WIRES: readonly DnsWire[] = ["PLAIN", "TLS", "HTTPS"];
export const DNS_WHOS: readonly DnsWho[] = ["SYSTEM", "PROXY", "WEAVER"];
export const DNS_TRANSITS: readonly DnsTransit[] = ["EGRESS", "PROXY"];
export const DNS_PROFILE_KINDS: readonly DnsProfileKind[] = ["PROXY_NAME", "ROUTE"];

/** Who may answer for each kind: a proxy's own address is never asked of a proxy. */
export function whoChoices(kind: DnsProfileKind): readonly DnsWho[] {
  return kind === "PROXY_NAME" ? ["SYSTEM", "WEAVER"] : DNS_WHOS;
}

export const dnsWireKey = (wire: DnsWire) => `next.dnsWire.${wire}`;
export const lookupTrustKey = (trust: LookupTrust) => `next.lookupTrust.${trust}`;
/** Who answers, in words. Finding a proxy, the system is the host: the one above it in a chain, else the system. */
export const dnsWhoKey = (who: DnsWho, kind: DnsProfileKind = "ROUTE") =>
  kind === "PROXY_NAME" && who === "SYSTEM" ? "next.dns.who.HOST" : `next.dns.who.${who}`;
/** A profile's name; the built-in profile is the host where a proxy is found. */
export function dnsProfileName(t: Translate, profile: Pick<DnsProfile, "id" | "name">, kind: DnsProfileKind): string {
  return kind === "PROXY_NAME" && profile.id === SYSTEM_DNS_PROFILE_ID ? t("next.dns.profiles.hostName") : profile.name;
}
export const dnsTransitKey = (transit: DnsTransit) => `next.dns.transit.${transit}`;
export const dnsTrustKey = (trust: DnsTrust) => `next.dns.trust.${trust}`;

/** The trusts each who allows: the system takes answers as sent, and the proxy's or Weaver's can also be validated here. */
export function trustChoices(who: DnsWho): readonly DnsTrust[] {
  return who === "SYSTEM" ? ["AS_SENT"] : ["AS_SENT", "VALIDATE"];
}

// Server rows.

/**
 * One server row as the form holds it. For HTTPS, `address` holds the URL,
 * and `dohAddress` the IP a URL that names a host is reached at.
 */
export type DnsServerRow = { wire: DnsWire; address: string; tlsName: string; dohAddress: string };

export const blankServer = (): DnsServerRow => ({ wire: "PLAIN", address: "", tlsName: "", dohAddress: "" });

/** Stored servers as form rows. The list always has a row to type in. */
export function serverRows(servers: readonly DnsServer[]): DnsServerRow[] {
  const rows = servers.map((server) => ({
    wire: server.wire,
    address: server.wire === "HTTPS" ? (server.url ?? "") : server.address,
    tlsName: server.wire === "TLS" ? (server.tlsName ?? "") : "",
    dohAddress: server.wire === "HTTPS" && dohProblem(server.url ?? "") === "hostname" ? server.address : "",
  }));
  return rows.length ? rows : [blankServer()];
}

/** The rows that name a server, with where each sits in the form; a row left blank is not one. */
function namedRows(rows: readonly DnsServerRow[]) {
  return rows.map((row, index) => ({ row, index })).filter(({ row }) => row.address.trim() !== "");
}

function serverInput(row: DnsServerRow): DnsServer {
  const value = row.address.trim();
  switch (row.wire) {
    case "HTTPS":
      // The IP beside the URL is sent only for a URL that names a host.
      return { wire: row.wire, address: dohProblem(value) === "hostname" ? row.dohAddress.trim() : "", tlsName: null, url: value };
    case "TLS":
      return { wire: row.wire, address: value, tlsName: row.tlsName.trim() || null, url: null };
    case "PLAIN":
      return { wire: row.wire, address: value, tlsName: null, url: null };
  }
}

/** The named rows as a save sends them, in the order they are asked. */
export function serversInput(rows: readonly DnsServerRow[]): DnsServer[] {
  return namedRows(rows).map(({ row }) => serverInput(row));
}

const IPV4 = /^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$/;
const IPV6_GROUP = /^[0-9A-Fa-f]{1,4}$/;
/** The longest path a DNS over HTTPS URL may carry, its query included. */
const MAX_DOH_PATH = 2048;

/** A port as the daemon reads one: decimal digits, leading zeros allowed, from 1 to 65535. */
function isPort(value: string): boolean {
  return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 65535;
}

export function isIpv4(value: string): boolean {
  return IPV4.test(value);
}

/** An IPv6 address as written, with `::` and an IPv4 tail allowed and no zone. */
export function isIpv6(value: string): boolean {
  const halves = value.split("::");
  if (halves.length > 2) return false;
  const groups = (part: string) => (part === "" ? [] : part.split(":"));
  const head = groups(halves[0]!);
  const tail = halves.length === 2 ? groups(halves[1]!) : [];
  const all = [...head, ...tail];
  let width = all.length;
  const last = all.at(-1);
  if (last !== undefined && last.includes(".")) {
    if (!isIpv4(last)) return false;
    all.pop();
    width += 1;
  }
  if (!all.every((group) => IPV6_GROUP.test(group))) return false;
  return halves.length === 2 ? width <= 7 : width === 8;
}

/** An IP, or an IP with a port: `192.0.2.53`, `192.0.2.53:5353`, `2001:db8::1`, `[2001:db8::1]:853`. */
export function isIpAddress(value: string): boolean {
  const bracketed = /^\[([^\]]+)\](?::(\d+))?$/.exec(value);
  if (bracketed) {
    return isIpv6(bracketed[1]!) && (bracketed[2] === undefined || isPort(bracketed[2]));
  }
  if (isIpv6(value)) return true;
  const [host, port, ...rest] = value.split(":");
  return rest.length === 0 && isIpv4(host!) && (port === undefined || isPort(port));
}

/** An IP alone, with no port: `192.0.2.53`, `2001:db8::1` or `[2001:db8::1]`. */
export function isBareIp(value: string): boolean {
  const bracketed = /^\[([^\]]+)\]$/.exec(value);
  return bracketed ? isIpv6(bracketed[1]!) : isIpv4(value) || isIpv6(value);
}

/** What a DNS over HTTPS URL lacks: a valid URL (`url`), or, for one that names a host, the IP beside it (`hostname`). */
export type DohProblem = "url" | "hostname";

/**
 * What is wrong with a DNS over HTTPS address: an https URL whose host is an
 * IP or a host name, with a path. The path, its query included, holds only
 * the characters a URI path and query may carry unescaped (letters, digits
 * and `-._~!$&'()*+,;=:@/%?`), at most 2048 of them. A URL that names a host
 * is otherwise valid, and answers `hostname`: it is reached at an IP given
 * beside it, as the host name is never looked up.
 */
export function dohProblem(value: string): DohProblem | null {
  const match = /^https:\/\/(\[[^\]\s]*\]|[^/:?#\s[\]@]+)(?::([^/]*))?(\/[^\s#]*)?$/i.exec(value);
  if (!match) return "url";
  const host = match[1]!;
  if (match[2] !== undefined && !isPort(match[2])) return "url";
  let named = false;
  if (host.startsWith("[")) {
    if (!isIpv6(host.slice(1, -1))) return "url";
  } else if (!isIpv4(host)) {
    // A name, as against something in no address form at all.
    if (!/[A-Za-z]/.test(host) || !isTlsName(host)) return "url";
    named = true;
  }
  const path = match[3];
  if (path === undefined || path.length > MAX_DOH_PATH || !/^\/[A-Za-z0-9\-._~!$&'()*+,;=:@/%?]*$/.test(path)) return "url";
  return named ? "hostname" : null;
}

/** A name a certificate could be issued for: labels of letters, digits and hyphens. */
export function isTlsName(value: string): boolean {
  if (value.length > 253) return false;
  const name = value.endsWith(".") ? value.slice(0, -1) : value;
  return name !== "" && name.split(".").every((label) => /^[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?$/.test(label));
}

/** An IPv6 address as its eight numbers, so two spellings of one address compare equal. */
function ipv6Groups(value: string): string {
  const halves = value.split("::");
  const groups = (part: string) =>
    (part === "" ? [] : part.split(":")).flatMap((group) => {
      if (!group.includes(".")) return [parseInt(group, 16)];
      const [a, b, c, d] = group.split(".").map(Number);
      return [a! * 256 + b!, c! * 256 + d!];
    });
  const head = groups(halves[0]!);
  const tail = halves.length === 2 ? groups(halves[1]!) : [];
  return [...head, ...Array<number>(8 - head.length - tail.length).fill(0), ...tail].join(":");
}

/** An IP address and port as one string, the port defaulted, for comparing two server addresses. */
function socketKey(host: string, port: string | undefined, defaultPort: number): string {
  const ip = host.startsWith("[") ? `6/${ipv6Groups(host.slice(1, -1))}` : host.includes(":") ? `6/${ipv6Groups(host)}` : `4/${host}`;
  return `${ip}/${port === undefined ? defaultPort : Number(port)}`;
}

/**
 * Which server a valid row names: its address and port, its wire, and its TLS
 * name or HTTPS path. Two rows with one key are one server, as the daemon
 * judges it; the same address over another wire is another server. A DNS
 * over HTTPS URL that names a host is the IP beside it, at the URL's port,
 * with its path and the name.
 */
function serverKey(row: DnsServerRow): string {
  const value = row.address.trim();
  if (row.wire === "HTTPS") {
    const match = /^https:\/\/(\[[^\]]*\]|[^/:]+)(?::(\d+))?(\/.*)$/i.exec(value)!;
    if (dohProblem(value) === "hostname") {
      return `HTTPS ${socketKey(row.dohAddress.trim(), match[2], 443)} ${match[3]} ${match[1]}`;
    }
    return `HTTPS ${socketKey(match[1]!, match[2], 443)} ${match[3]}`;
  }
  const bracketed = /^\[([^\]]+)\](?::(\d+))?$/.exec(value);
  const [host, port] = bracketed ? [`[${bracketed[1]}]`, bracketed[2]] : isIpv6(value) ? [value, undefined] : value.split(":");
  const address = socketKey(host!, port, row.wire === "TLS" ? 853 : 53);
  return row.wire === "TLS" ? `TLS ${address} ${row.tlsName.trim()}` : `PLAIN ${address}`;
}

export type DnsProblem = { key: string; values?: Record<string, number> };

/** A problem's code: the last part of its key, such as `PROXY_DNS_SERVER_DUPLICATE` or `tooMany`. */
export function problemCode(problem: DnsProblem): string {
  return problem.key.slice(problem.key.lastIndexOf(".") + 1);
}

/** The first thing wrong with a list's servers, as a dictionary key and its values. */
export function serversProblem(servers: readonly DnsServerRow[]): DnsProblem | null {
  const rows = namedRows(servers).map(({ row, index }) => ({ row, number: index + 1 }));
  if (rows.length === 0) return { key: "next.dns.needsServer" };
  if (rows.length > MAX_DNS_SERVERS) return { key: "next.dns.tooMany", values: { count: MAX_DNS_SERVERS } };
  // A name an encrypted server failed to answer must not then go out in the clear.
  const firstEncrypted = rows.findIndex(({ row }) => row.wire !== "PLAIN");
  const plainAfter = firstEncrypted < 0 ? undefined : rows.slice(firstEncrypted).find(({ row }) => row.wire === "PLAIN");
  // The daemon's own sentence for it, so the form and a refused save read alike.
  if (plainAfter !== undefined) return { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" };
  for (const { row, number } of rows) {
    const value = row.address.trim();
    if (row.wire === "HTTPS") {
      const problem = dohProblem(value);
      if (problem === "url") return { key: "next.dns.badUrl", values: { number } };
      if (problem === "hostname" && !isBareIp(row.dohAddress.trim())) return { key: "next.dns.dohNeedsAddress", values: { number } };
      continue;
    }
    if (!isIpAddress(value)) return { key: "next.dns.badAddress", values: { number } };
    const tlsName = row.tlsName.trim();
    if (row.wire === "TLS" && tlsName !== "" && !isTlsName(tlsName)) {
      return { key: "next.dns.badTlsName", values: { number } };
    }
  }
  // Every row is valid here, so each names one server.
  const seen = new Set<string>();
  for (const { row } of rows) {
    const key = serverKey(row);
    if (seen.has(key)) return { key: "next.refusal.PROXY_DNS_SERVER_DUPLICATE" };
    seen.add(key);
  }
  return null;
}

// Server lists.

/** A server list in its editor; a null id is one not saved yet. */
export type DnsListForm = { id: number | null; name: string; servers: DnsServerRow[] };

export function listForm(list: DnsServerList | null): DnsListForm {
  return list ? { id: list.id, name: list.name, servers: serverRows(list.servers) } : { id: null, name: "", servers: [blankServer()] };
}

export function listInput(form: DnsListForm) {
  return { ...(form.id === null ? {} : { id: form.id }), name: form.name.trim(), servers: serversInput(form.servers) };
}

/** The first thing wrong with a list: its name, then its servers. */
export function listProblem(form: DnsListForm): DnsProblem | null {
  if (form.name.trim() === "") return { key: "next.dns.lists.nameRequired" };
  return serversProblem(form.servers);
}

// Profiles.

/** A profile in its editor; a null id is one not saved yet. Transit and list mean something only for WEAVER. */
export type DnsProfileForm = {
  id: number | null;
  kind: DnsProfileKind;
  name: string;
  who: DnsWho;
  transit: DnsTransit;
  trust: DnsTrust;
  serverListId: number | null;
  systemFallback: boolean;
};

/**
 * A stored profile in the editor, or a new one of the kind: Weaver asking the first list as sent, through the proxy
 * for a route, from the egress for a proxy's name.
 */
export function profileForm(
  profile: DnsProfileEdited | null,
  lists: readonly Pick<DnsServerList, "id">[],
  kind: DnsProfileKind = profile?.kind ?? "ROUTE",
): DnsProfileForm {
  if (!profile) {
    const transit = kind === "PROXY_NAME" ? "EGRESS" : "PROXY";
    return { id: null, kind, name: "", who: "WEAVER", transit, trust: "AS_SENT", serverListId: lists[0]?.id ?? null, systemFallback: false };
  }
  return {
    id: profile.id,
    kind: profile.kind,
    name: profile.name,
    who: profile.who,
    transit: profile.transit ?? "PROXY",
    trust: profile.trust,
    serverListId: profile.serverListId,
    systemFallback: profile.systemFallback ?? false,
  };
}

/** The form with another who: a trust that who does not allow goes back to as sent. */
export function withWho(form: DnsProfileForm, who: DnsWho): DnsProfileForm {
  return { ...form, who, trust: trustChoices(who).includes(form.trust) ? form.trust : "AS_SENT" };
}

/**
 * The profile as its save sends it, with its kind, which a saved profile keeps as it was. Only WEAVER carries a
 * transit and a list, and only a route's PROXY a system fallback. A proxy's name is always asked from the egress.
 */
export function profileInput(form: DnsProfileForm) {
  const head = form.id === null ? { kind: form.kind } : { id: form.id, kind: form.kind };
  const proxyName = form.kind === "PROXY_NAME";
  const who = whoChoices(form.kind).includes(form.who) ? form.who : "SYSTEM";
  const trust = trustChoices(who).includes(form.trust) ? form.trust : "AS_SENT";
  if (who !== "WEAVER") {
    return { ...head, name: form.name.trim(), who, transit: null, trust, serverListId: null, systemFallback: who === "PROXY" && form.systemFallback };
  }
  const transit = proxyName ? "EGRESS" : form.transit;
  return { ...head, name: form.name.trim(), who, transit, trust, serverListId: form.serverListId, systemFallback: false };
}

export function profileProblem(form: DnsProfileForm): DnsProblem | null {
  if (form.name.trim() === "") return { key: "next.dns.profiles.nameRequired" };
  if (form.who === "WEAVER" && form.serverListId === null) return { key: "next.refusal.DNS_PROFILE_LIST_REQUIRED" };
  return null;
}

/** A list in one phrase: its name and the wires it uses, such as "Quad9 TLS". */
function listPhrase(t: Translate, list: Pick<DnsServerList, "name" | "servers">): string {
  const wires = DNS_WIRES.filter((wire) => list.servers.some((server) => server.wire === wire));
  return [list.name, ...wires.map((wire) => t(`next.dns.wireShort.${wire}`))].join(" ");
}

/** A profile in one line, such as "Weaver · Quad9 TLS · Via the proxy · DNSSEC on". A proxy's name has no transit. */
export function profileSummary(
  t: Translate,
  profile: Pick<DnsProfile, "kind" | "who" | "trust" | "serverListId"> & Partial<Pick<DnsProfile, "transit" | "systemFallback">>,
  lists: readonly Pick<DnsServerList, "id" | "name" | "servers">[],
): string {
  const who = t(dnsWhoKey(profile.who, profile.kind));
  if (profile.who === "SYSTEM") return who;
  if (profile.who === "PROXY") {
    return [who, t(dnsTrustKey(profile.trust)), ...(profile.systemFallback ? [t("next.dns.systemFallbackOn")] : [])].join(" · ");
  }
  const list = lists.find((candidate) => candidate.id === profile.serverListId);
  return [
    who,
    list ? listPhrase(t, list) : t("next.dns.listMissing"),
    ...(profile.kind === "PROXY_NAME" ? [] : [t(dnsTransitKey(profile.transit ?? "PROXY"))]),
    t(dnsTrustKey(profile.trust)),
  ].join(" · ");
}

// A route's DNS.

export type DnsRouteProblem = "needsProxy" | "needsWireGuard" | "needsTunnelDns";
const ROUTE_PROBLEM_KEYS: Record<DnsRouteProblem, string> = {
  needsProxy: "next.dns.routeNeedsProxy",
  needsWireGuard: "next.dns.routeNeedsWireGuard",
  needsTunnelDns: "next.dns.routeNeedsTunnelDns",
};
export const dnsRouteProblemKey = (problem: DnsRouteProblem) => ROUTE_PROBLEM_KEYS[problem];

/** The proxies and pools a route check reads; a proxy's `dnsServers` are its tunnel's, for WireGuard. */
export type DnsRouteKnown = {
  proxies: readonly { id: number; kind: ProxyKind; dnsServers?: readonly string[] }[];
  pools: readonly Pick<ProxyPool, "id" | "kind" | "memberIds">[];
};

/** The kind of proxy that answers on a rung: the proxy, the pool's kind, or a chain's last hop. */
function answeringKind(rung: Rung, known: DnsRouteKnown): ProxyKind | undefined {
  if (rung.kind === "POOL") return known.pools.find((pool) => pool.id === rung.poolId)?.kind;
  const id = rung.kind === "PROXY" ? rung.proxyId : rung.chainIds.at(-1);
  return known.proxies.find((proxy) => proxy.id === id)?.kind;
}

/**
 * The WireGuard proxies a route ends a rung at, a pool's members among them,
 * that list no DNS servers of their tunnel, in the order the route reaches them.
 */
export function wireGuardWithoutDns(route: NetworkRoute, known: DnsRouteKnown): number[] {
  const silent: number[] = [];
  for (const leg of route.legs) {
    if (leg.path.kind !== "LADDER") continue;
    for (const rung of leg.path.rungs) {
      const exits =
        rung.kind === "POOL"
          ? (known.pools.find((pool) => pool.id === rung.poolId)?.memberIds ?? [])
          : [rung.kind === "PROXY" ? rung.proxyId : rung.chainIds.at(-1)];
      for (const id of exits) {
        const proxy = known.proxies.find((candidate) => candidate.id === id);
        // Servers this page has not loaded are not taken for none.
        if (proxy?.kind === "WIRE_GUARD" && proxy.dnsServers?.length === 0 && !silent.includes(proxy.id)) {
          silent.push(proxy.id);
        }
      }
    }
  }
  return silent;
}

/**
 * Why the route cannot take the profile, in the order the daemon refuses it.
 * When the proxy answers with no system fallback, every leg and rung needs
 * one: a direct leg or a fallback to direct has none. Validating the proxy's
 * answers needs a WireGuard tunnel's own servers, so every stage must end at
 * WireGuard, a direct one never does. And a WireGuard proxy that answers needs
 * DNS servers of its tunnel.
 */
export function dnsRouteProblem(
  route: NetworkRoute,
  profile: Pick<DnsProfileChoice, "who" | "trust" | "systemFallback"> | undefined,
  known: DnsRouteKnown,
): DnsRouteProblem | null {
  if (profile?.who !== "PROXY") return null;
  const direct = route.legs.some((leg) => leg.path.kind === "DIRECT" || leg.path.directFallback);
  if (!profile.systemFallback && direct) return "needsProxy";
  if (profile.trust === "VALIDATE") {
    const kinds = route.legs.flatMap((leg) => leg.path.rungs.map((rung) => answeringKind(rung, known)));
    // A proxy or pool this page has not loaded is not known to be WireGuard, and the daemon refuses it.
    if (direct || kinds.some((kind) => kind !== "WIRE_GUARD")) return "needsWireGuard";
  }
  return wireGuardWithoutDns(route, known).length > 0 ? "needsTunnelDns" : null;
}

// Capability tests.

export type DnsProbe = { supported: boolean; millis: number; error: { code: string; detail: string } | null };
export type DnsServerCapability = {
  index: number;
  plain: DnsProbe;
  tls: DnsProbe;
  https: DnsProbe;
  dnssec: DnsProbe;
};
export type DnsProbeKind = "plain" | "tls" | "https" | "dnssec";
export const DNS_PROBE_KINDS: readonly DnsProbeKind[] = ["plain", "tls", "https", "dnssec"];
const WIRE_PROBE: Record<DnsWire, DnsProbeKind> = { PLAIN: "plain", TLS: "tls", HTTPS: "https" };

/** The probe that answers for a row's own wire. */
export const ownWireProbe = (wire: DnsWire): DnsProbeKind => WIRE_PROBE[wire];

/** What a capability test sends: the named rows, and the form row each sent server came from. */
export function probeRequest(rows: readonly DnsServerRow[]): { servers: DnsServer[]; rows: number[] } {
  const named = namedRows(rows);
  return { servers: named.map(({ row }) => serverInput(row)), rows: named.map(({ index }) => index) };
}

/** The rows a test was run on, as one string: any edit, reorder, addition or removal changes it. */
export const serverRowsKey = (rows: readonly DnsServerRow[]) => JSON.stringify(rows);

/**
 * A test, held against the rows it was run on and the route it went through.
 * `results` is null while the test is in flight.
 */
export type DnsTestRun = { key: string; rows: number[]; route: string; results: DnsServerCapability[] | null };

/**
 * Where a refused test's sentence goes: under the form row of the server the
 * refusal names by its place among those sent, or under no row. Rows edited,
 * reordered, added or removed since the test was sent are not the rows it
 * names, so a refusal of one server is dropped (null). A refusal that names
 * no server, such as one of the route, still stands under no row.
 */
export function testRefusalRow(
  sent: { key: string; rows: readonly number[] },
  index: unknown,
  rowsNow: readonly DnsServerRow[],
): { row: number | null } | null {
  if (typeof index !== "number") return { row: null };
  if (serverRowsKey(rowsNow) !== sent.key) return null;
  return { row: sent.rows[index] ?? null };
}

/** One cell of the results table: null `supported` is still pending. */
export type DnsCapabilityCell = {
  supported: boolean | null;
  millis: number | null;
  error: { code: string; detail: string } | null;
  /** The probe for the wire the row is set to. */
  own: boolean;
};
export type DnsCapabilityRow = { row: number; address: string; cells: Record<DnsProbeKind, DnsCapabilityCell> };
export type DnsCapabilityTable = { route: string; pending: boolean; rows: DnsCapabilityRow[] };

const PENDING_CELL = { supported: null, millis: null, error: null } as const;

/**
 * The results table for the rows as they stand: one line per tested row.
 * Once the rows change the results no longer describe them, and there is no
 * table.
 */
export function capabilityTable(run: DnsTestRun | null, rows: readonly DnsServerRow[]): DnsCapabilityTable | null {
  if (!run || run.key !== serverRowsKey(rows)) return null;
  const byIndex = new Map((run.results ?? []).map((result) => [result.index, result]));
  return {
    route: run.route,
    pending: run.results === null,
    rows: run.rows.flatMap((formRow, index) => {
      const row = rows[formRow];
      if (!row) return [];
      const result = byIndex.get(index);
      if (run.results !== null && !result) return [];
      const own = ownWireProbe(row.wire);
      const cell = (probe: DnsProbeKind): DnsCapabilityCell => {
        if (!result) return { ...PENDING_CELL, own: probe === own };
        return { ...result[probe], own: probe === own };
      };
      return [{
        row: formRow,
        address: row.address.trim(),
        cells: Object.fromEntries(DNS_PROBE_KINDS.map((probe) => [probe, cell(probe)])) as DnsCapabilityRow["cells"],
      }];
    }),
  };
}

/**
 * A route in one line for the table's caption: each leg's egress, then the
 * proxies on its rungs in order and a direct fallback last, such as
 * "eth0 → Mullvad → direct, wlan0 → direct".
 */
export function routeCaption(
  route: NetworkRoute,
  names: {
    egresses: readonly { id: number; name: string }[];
    proxies: readonly { id: number; name: string }[];
    pools: readonly { id: number; name: string }[];
  },
  direct: string,
): string {
  const named = (list: readonly { id: number; name: string }[], id: number | null | undefined) =>
    list.find((item) => item.id === id)?.name ?? `#${id ?? "?"}`;
  const rung = (step: Rung) =>
    step.kind === "POOL"
      ? named(names.pools, step.poolId)
      : step.kind === "PROXY"
        ? named(names.proxies, step.proxyId)
        : step.chainIds.map((id) => named(names.proxies, id)).join(" + ");
  return route.legs
    .map((leg) => {
      // A ladder's direct fallback is its last way out.
      const hops = leg.path.kind === "DIRECT" ? [direct] : [...leg.path.rungs.map(rung), ...(leg.path.directFallback ? [direct] : [])];
      return [named(names.egresses, leg.egressId), ...hops].join(" → ");
    })
    .join(", ");
}

/** A route profile as the network flow draws it: who asks, from where, how it is checked, and the list it asks. */
export type FlowDnsProfile = Pick<DnsProfile, "id" | "name" | "who" | "transit" | "trust" | "serverListId" | "systemFallback">;
/** A server list as the network flow draws it: its name and the servers on it. */
export type FlowDnsList = Pick<DnsServerList, "id" | "name" | "servers">;

/** Who answers the lookup the flow draws. */
export type LookupAsker = "HOST" | "WEAVER" | "PROXY";
/** Why a leg looks nothing up, so that its connections fail. */
export type LookupRefusal = "NO_PROXY" | "NO_TUNNEL_DNS" | "ONLY_WIREGUARD_VALIDATES";
/** Where a lookup is answered. */
export type LookupDestination =
  | { kind: "host" }
  | { kind: "list"; id: number | null }
  | { kind: "tunnel"; proxyId: number }
  | { kind: "proxy"; proxyId: number | null; poolId: number | null };

/**
 * A leg's lookups as its route's DNS profile has them, the way the tunnel
 * engine rules them for the route's own endpoint. `at` is the box the lookup
 * leaves from: the leg itself for the host's resolver (or a lookup refused
 * for want of a proxy), its egress for Weaver asking a list from there, and
 * the proxy carrying the leg now for anything asked at or through it.
 */
export type LegLookup = {
  at: "leg" | "egress" | "proxy";
  /** Null when the lookup is refused. */
  asker: LookupAsker | null;
  refused: LookupRefusal | null;
  /** The host answers only because the leg has no proxy to hand the name to. */
  fallback: boolean;
  /** Weaver checks the answer's DNSSEC. */
  validates: boolean;
  /** The proxy the leg leaves through now, when the lookup is at it. */
  exit: { proxyId: number | null; poolId: number | null } | null;
  /** Null when the lookup is refused. */
  destination: LookupDestination | null;
};

/** The proxy the leg leaves through now: the last hop of a chain, a pool's chosen member. Null when it goes direct. */
function activeExit(leg: LegFlow): { proxyId: number | null; poolId: number | null } | null {
  if (leg.path.kind === "DIRECT") return null;
  const rung = leg.path.rungs[leg.selectedRung ?? 0];
  if (!rung) return null;
  if (rung.kind === "PROXY") return { proxyId: rung.proxyId, poolId: null };
  if (rung.kind === "CHAIN") return { proxyId: rung.chainIds.at(-1) ?? null, poolId: null };
  return { proxyId: leg.selectedProxyId ?? null, poolId: rung.poolId };
}

/**
 * Where `leg`'s names are looked up under `profile`, given the proxies by id.
 * A proxy whose kind is not known (a pool that has chosen no member yet) is
 * taken as handed the name.
 */
export function legLookup(
  leg: LegFlow,
  profile: FlowDnsProfile,
  proxies: readonly { id: number; kind: ProxyKind; dnsServers: string[] }[],
): LegLookup {
  const exit = activeExit(leg);
  const none = { refused: null, fallback: false, validates: false, exit: null };
  if (profile.who === "SYSTEM") {
    return { ...none, at: "leg", asker: "HOST", destination: { kind: "host" } };
  }
  const validates = profile.trust === "VALIDATE";
  if (profile.who === "WEAVER") {
    const through = exit !== null && profile.transit === "PROXY";
    return {
      ...none,
      at: through ? "proxy" : "egress",
      asker: "WEAVER",
      validates,
      exit: through ? exit : null,
      destination: { kind: "list", id: profile.serverListId },
    };
  }
  if (!exit) {
    return profile.systemFallback
      ? { ...none, at: "leg", asker: "HOST", fallback: true, destination: { kind: "host" } }
      : { ...none, at: "leg", asker: null, refused: "NO_PROXY", destination: null };
  }
  const proxy = proxies.find((candidate) => candidate.id === exit.proxyId);
  if (proxy?.kind === "WIRE_GUARD") {
    // Weaver asks the tunnel's own servers through it, checked as the profile says.
    return proxy.dnsServers.length
      ? { ...none, at: "proxy", asker: "WEAVER", validates, exit, destination: { kind: "tunnel", proxyId: proxy.id } }
      : { ...none, at: "proxy", asker: null, refused: "NO_TUNNEL_DNS", exit, destination: null };
  }
  // A stream proxy resolves the name itself, out of Weaver's sight, so nothing can check it.
  return proxy && validates
    ? { ...none, at: "proxy", asker: null, refused: "ONLY_WIREGUARD_VALIDATES", exit, destination: null }
    : { ...none, at: "proxy", asker: "PROXY", exit, destination: { kind: "proxy", ...exit } };
}

/** A server as the flow's pill names it: its address, or a DoH server's host and path. */
export function serverPill(server: DnsServer): string {
  if (server.wire !== "HTTPS") return server.address;
  const url = server.url ?? "";
  return url.replace(/^https:\/\//i, "").replace(/\/dns-query$/i, "");
}

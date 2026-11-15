// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ProxyKind } from "./proxies.ts";

/** Who answers a name dialled through a proxy: the host, the proxy, or the servers listed. */
export type ProxyDnsWho = "HOST" | "PROXY" | "SERVERS";
/** How the listed servers are reached: straight from the egress, or through the proxy. */
export type ProxyDnsPath = "DIRECT" | "THROUGH_PROXY";
/** What the listed servers' answers must carry to be used. */
export type ProxyDnsTrust = "AS_SENT" | "REQUIRE_AD" | "VALIDATE";
/** How a query travels to one server. */
export type ProxyDnsWire = "PLAIN" | "TLS" | "HTTPS";
/** What a lookup's answer was taken as. */
export type LookupTrust = "AS_SENT" | "AUTHENTICATED" | "REFUSED" | "VALIDATED" | "UNSIGNED" | "BOGUS";

export type ProxyDnsServer = { wire: ProxyDnsWire; address: string; tlsName: string | null; url: string | null };
/** A proxy's DNS, as the daemon holds it. Path, trust and servers mean something only for SERVERS. */
export type ProxyDns = { who: ProxyDnsWho; path: ProxyDnsPath; trust: ProxyDnsTrust; servers: ProxyDnsServer[] };

/** The daemon keeps at most this many servers for one DNS setting, a proxy's or an egress's. */
export const MAX_DNS_SERVERS = 10;
export const DNS_WIRES: readonly ProxyDnsWire[] = ["PLAIN", "TLS", "HTTPS"];
export const dnsWireKey = (wire: ProxyDnsWire) => `next.dnsWire.${wire}`;
export const lookupTrustKey = (trust: LookupTrust) => `next.lookupTrust.${trust}`;

/** One server row as the form holds it. For HTTPS, `address` holds the URL. */
export type DnsServerRow = { wire: ProxyDnsWire; address: string; tlsName: string };
export type DnsForm = { who: ProxyDnsWho; path: ProxyDnsPath; trust: ProxyDnsTrust; servers: DnsServerRow[] };

export const blankServer = (): DnsServerRow => ({ wire: "PLAIN", address: "", tlsName: "" });

/**
 * The older two fields in the explicit shape: a resolver with no servers is
 * the host or the proxy answering, and with servers it is those servers, asked
 * from the egress for Weaver and through the proxy for the proxy.
 */
export function legacyDns(resolver: "WEAVER" | "PROXY", dnsServers: readonly string[]): ProxyDns {
  if (dnsServers.length === 0) {
    return { who: resolver === "WEAVER" ? "HOST" : "PROXY", path: "THROUGH_PROXY", trust: "AS_SENT", servers: [] };
  }
  return {
    who: "SERVERS",
    path: resolver === "WEAVER" ? "DIRECT" : "THROUGH_PROXY",
    trust: "AS_SENT",
    servers: dnsServers.map((address) => ({ wire: "PLAIN", address, tlsName: null, url: null })),
  };
}

/** A stored proxy's DNS in the form. The list always has a row to type in. */
export function dnsForm(dns: ProxyDns): DnsForm {
  const servers = dns.servers.map((server) => ({
    wire: server.wire,
    address: server.wire === "HTTPS" ? (server.url ?? "") : server.address,
    tlsName: server.wire === "TLS" ? (server.tlsName ?? "") : "",
  }));
  return { who: dns.who, path: dns.path, trust: dns.trust, servers: servers.length ? servers : [blankServer()] };
}

/** A new proxy's DNS: the host answers. */
export const NEW_DNS: DnsForm = dnsForm(legacyDns("WEAVER", []));

/** A `wg-quick` file's DNS servers answer inside the tunnel: plain queries through it, as sent. */
export function wireguardDns(servers: readonly string[]): DnsForm {
  return dnsForm(legacyDns("PROXY", servers));
}

/** Whether the proxy can be handed a name. A WireGuard peer has nobody to hand it to. */
export const proxyAnswers = (kind: ProxyKind) => kind !== "WIRE_GUARD";

/**
 * The DNS a proxy of `kind` can have. A WireGuard proxy cannot hand its peer a
 * name, so "the proxy" there means its servers. Where those are asked from is
 * the operator's: a new one asks them through the tunnel, and a stored one
 * keeps the path it was saved with.
 */
export function effectiveDns(form: DnsForm, kind: ProxyKind): DnsForm {
  if (kind !== "WIRE_GUARD" || form.who !== "PROXY") return form;
  return { ...form, who: "SERVERS" };
}

/** The rows that name a server; a row left blank is not one. */
const namedRows = (form: DnsForm) => form.servers.filter((row) => row.address.trim() !== "");

/** The named rows as the save input sends them, in the order they are asked. */
function serversInput(form: DnsForm) {
  return namedRows(form).map((row) => {
    const value = row.address.trim();
    switch (row.wire) {
      case "HTTPS":
        return { wire: row.wire, address: "", tlsName: null, url: value };
      case "TLS":
        return { wire: row.wire, address: value, tlsName: row.tlsName.trim() || null, url: null };
      case "PLAIN":
        return { wire: row.wire, address: value, tlsName: null, url: null };
    }
  });
}

/** The form's DNS as the save input sends it. Only SERVERS carries a path, a trust and servers. */
export function dnsInput(form: DnsForm, kind: ProxyKind) {
  const dns = effectiveDns(form, kind);
  if (dns.who !== "SERVERS") {
    return { who: dns.who, path: "THROUGH_PROXY" as ProxyDnsPath, trust: "AS_SENT" as ProxyDnsTrust, servers: [] };
  }
  return { who: dns.who, path: dns.path, trust: dns.trust, servers: serversInput(dns) };
}

/**
 * Whether the authenticated mark can be forged: a plain query can be altered
 * on the way, mark and all, whichever way it goes. Validating here checks the
 * signatures, not the mark, and as sent asks for no mark, so neither is at risk.
 */
function plainFakes(form: DnsForm): boolean {
  return form.who === "SERVERS" && form.trust === "REQUIRE_AD" && namedRows(form).some((row) => row.wire === "PLAIN");
}

export function plainCanFakeAuthentication(form: DnsForm, kind: ProxyKind): boolean {
  return plainFakes(effectiveDns(form, kind));
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

export type DohProblem = "url" | "hostname";

/**
 * What is wrong with a DNS over HTTPS address: an https URL whose host is an
 * IP, with a path. The path, its query included, holds only the characters a
 * URI path and query may carry unescaped (letters, digits and
 * `-._~!$&'()*+,;=:@/%?`), at most 2048 of them: the query is sent on a URI
 * that takes nothing else.
 */
export function dohProblem(value: string): DohProblem | null {
  const match = /^https:\/\/(\[[^\]\s]*\]|[^/:?#\s[\]@]+)(?::([^/]*))?(\/[^\s#]*)?$/i.exec(value);
  if (!match) return "url";
  const host = match[1]!;
  if (match[2] !== undefined && !isPort(match[2])) return "url";
  if (host.startsWith("[")) {
    if (!isIpv6(host.slice(1, -1))) return "url";
  } else if (!isIpv4(host)) {
    // A name, as against something in no address form at all.
    return /[A-Za-z]/.test(host) ? "hostname" : "url";
  }
  const path = match[3];
  return path === undefined || path.length > MAX_DOH_PATH || !/^\/[A-Za-z0-9\-._~!$&'()*+,;=:@/%?]*$/.test(path) ? "url" : null;
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
 * judges it; the same address over another wire is another server.
 */
function serverKey(row: DnsServerRow): string {
  const value = row.address.trim();
  if (row.wire === "HTTPS") {
    const match = /^https:\/\/(\[[^\]]*\]|[^/:]+)(?::(\d+))?(\/.*)$/i.exec(value)!;
    return `HTTPS ${socketKey(match[1]!, match[2], 443)} ${match[3]}`;
  }
  const bracketed = /^\[([^\]]+)\](?::(\d+))?$/.exec(value);
  const [host, port] = bracketed ? [`[${bracketed[1]}]`, bracketed[2]] : isIpv6(value) ? [value, undefined] : value.split(":");
  const address = socketKey(host!, port, row.wire === "TLS" ? 853 : 53);
  return row.wire === "TLS" ? `TLS ${address} ${row.tlsName.trim()}` : `PLAIN ${address}`;
}

export type DnsProblem = { key: string; values?: Record<string, number> };

/** The first thing wrong with the form's DNS, as a dictionary key and its values. */
export function dnsProblem(form: DnsForm, kind: ProxyKind): DnsProblem | null {
  return serversProblem(effectiveDns(form, kind));
}

/** The first thing wrong with listed servers, the same for a proxy and an egress. */
function serversProblem(dns: DnsForm): DnsProblem | null {
  if (dns.who !== "SERVERS") return null;
  const rows = dns.servers.map((row, index) => ({ row, number: index + 1 })).filter(({ row }) => row.address.trim() !== "");
  if (rows.length === 0) return { key: "next.proxies.needsDns" };
  if (rows.length > MAX_DNS_SERVERS) return { key: "next.proxies.dnsTooMany", values: { count: MAX_DNS_SERVERS } };
  // A name an encrypted server failed to answer must not then go out in the clear.
  const firstEncrypted = rows.findIndex(({ row }) => row.wire !== "PLAIN");
  const plainAfter = firstEncrypted < 0 ? undefined : rows.slice(firstEncrypted).find(({ row }) => row.wire === "PLAIN");
  // The daemon's own sentence for it, so the form and a refused save read alike.
  if (plainAfter !== undefined) return { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" };
  for (const { row, number } of rows) {
    const value = row.address.trim();
    if (row.wire === "HTTPS") {
      const problem = dohProblem(value);
      if (problem === "hostname") return { key: "next.proxies.dnsUrlHostname", values: { number } };
      if (problem === "url") return { key: "next.proxies.dnsBadUrl", values: { number } };
      continue;
    }
    if (!isIpAddress(value)) return { key: "next.proxies.dnsBadAddress", values: { number } };
    const tlsName = row.tlsName.trim();
    if (row.wire === "TLS" && tlsName !== "" && !isTlsName(tlsName)) {
      return { key: "next.proxies.dnsBadTlsName", values: { number } };
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

/** Who answers a name an egress dials with no proxy below it: the host, or the servers listed. */
export type EgressDnsWho = "HOST" | "SERVERS";
/** An egress's DNS, as the daemon holds it. Trust and servers mean something only for SERVERS. */
export type EgressDns = { who: EgressDnsWho; trust: ProxyDnsTrust; servers: ProxyDnsServer[] };

/** The egress setting when nothing is listed: the host answers. */
export const HOST_EGRESS_DNS: EgressDns = { who: "HOST", trust: "AS_SENT", servers: [] };

/**
 * A stored egress's DNS in the form. Its servers are always asked straight
 * from the egress, so the path is fixed at DIRECT.
 */
export function egressDnsForm(dns: EgressDns | null | undefined): DnsForm {
  const stored = dns ?? HOST_EGRESS_DNS;
  return dnsForm({ who: stored.who, path: "DIRECT", trust: stored.trust, servers: stored.servers });
}

/** The egress form's DNS as the save input sends it. Only SERVERS carries a trust and servers. */
export function egressDnsInput(form: DnsForm): EgressDns {
  if (form.who !== "SERVERS") return { ...HOST_EGRESS_DNS };
  return { who: "SERVERS", trust: form.trust, servers: serversInput(form) };
}

/** The first thing wrong with an egress's DNS. */
export function egressDnsProblem(form: DnsForm): DnsProblem | null {
  return serversProblem(form);
}

/** Whether an egress's listed plain servers could forge the authenticated mark. */
export function egressPlainCanFakeAuthentication(form: DnsForm): boolean {
  return plainFakes(form);
}

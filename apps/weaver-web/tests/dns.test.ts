// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  MAX_DNS_SERVERS,
  capabilityTable,
  dnsProfileName,
  dnsWhoKey,
  dnsRouteProblem,
  dnsRouteProblemKey,
  wireGuardWithoutDns,
  dohProblem,
  isIpAddress,
  isTlsName,
  listForm,
  listInput,
  listProblem,
  probeRequest,
  profileForm,
  profileInput,
  profileProblem,
  profileSummary,
  routeCaption,
  serverRows,
  serverRowsKey,
  serversProblem,
  testRefusalRow,
  trustChoices,
  whoChoices,
  withWho,
  type DnsProbe,
  type DnsServerCapability,
  type DnsServerRow,
  type DnsTestRun,
} from "../src/lib/dns.ts";
import type { NetworkRoute } from "../src/lib/networking.ts";
import { Kind, type DocumentNode, type FieldNode } from "graphql";
import { DNS_PROFILE_CHOICES_QUERY, DNS_PROFILE_NAMES_QUERY, DNS_QUERY } from "../src/graphql/dns.ts";

/** Each `dnsProfiles` read of a document, by the name it answers under: the kind it asks for, and the fields it selects. */
function profileReads(document: DocumentNode): Record<string, { kind: string | null; fields: string[] }> {
  const definition = document.definitions[0];
  assert.ok(definition?.kind === Kind.OPERATION_DEFINITION);
  const reads: Record<string, { kind: string | null; fields: string[] }> = {};
  for (const selection of definition.selectionSet.selections) {
    const field = selection as FieldNode;
    if (field.name.value !== "dnsProfiles") continue;
    const kind = field.arguments?.find((argument) => argument.name.value === "kind")?.value;
    reads[field.alias?.value ?? field.name.value] = {
      kind: kind?.kind === Kind.ENUM ? kind.value : null,
      fields: (field.selectionSet?.selections ?? []).map((inner) => (inner as FieldNode).name.value),
    };
  }
  return reads;
}

test("each DNS read asks for one kind of profile, and only the fields its caller shows", () => {
  assert.deepEqual(profileReads(DNS_QUERY), {
    nameProfiles: { kind: "PROXY_NAME", fields: ["id", "name", "builtIn", "who", "trust", "serverListId", "usedByProxies"] },
    routeProfiles: {
      kind: "ROUTE",
      fields: ["id", "name", "builtIn", "who", "transit", "trust", "serverListId", "systemFallback", "usedByRoutes"],
    },
  });
  assert.deepEqual(profileReads(DNS_PROFILE_CHOICES_QUERY), {
    dnsProfiles: { kind: "ROUTE", fields: ["id", "name", "who", "trust", "systemFallback"] },
  });
  assert.deepEqual(profileReads(DNS_PROFILE_NAMES_QUERY), { dnsProfiles: { kind: "PROXY_NAME", fields: ["id", "name"] } });
});

const rows = (...list: Partial<DnsServerRow>[]): DnsServerRow[] => list.map((row) => ({ wire: "PLAIN", address: "", tlsName: "", ...row }));
const t = (key: string) => key;

test("a plain or TLS address is an IP, with a port only in its own form", () => {
  // The daemon reads a port with leading zeros as the number it spells.
  for (const good of ["192.0.2.53", "192.0.2.53:5353", "192.0.2.53:05353", "2001:db8::1", "[2001:db8::1]", "[2001:db8::1]:853", "[2001:db8::1]:0853", "::1", "::ffff:192.0.2.1", "1:2:3:4:5:6:7:8"]) {
    assert.equal(isIpAddress(good), true, good);
  }
  for (const bad of ["", "dns.example", "192.0.2", "192.0.2.256", "192.0.2.53:0", "192.0.2.53:65536", "192.0.2.53:", "[2001:db8::1]:", "1::2::3", "1:2:3:4:5:6:7:8:9", "fe80::1%en0", "[192.0.2.53]:53", "192.0.2.53 "]) {
    assert.equal(isIpAddress(bad), false, bad);
  }
});

test("a DNS over HTTPS address is an https URL to an IP, with a path", () => {
  const longest = `/${"a".repeat(2047)}`;
  for (const good of ["https://192.0.2.53/dns-query", "https://192.0.2.53:8443/dns-query", "https://192.0.2.53:0443/q", "https://[2001:db8::1]/dns-query", "https://[2001:db8::1]:443/q", "HTTPS://192.0.2.53/", "https://192.0.2.53/dns-query?dns=x&ct=1", `https://192.0.2.53${longest}`]) {
    assert.equal(dohProblem(good), null, good);
  }
  for (const bad of ["http://192.0.2.53/dns-query", "192.0.2.53/dns-query", "https://192.0.2.53", "https://192.0.2.53:99999/q", "https://192.0.2.53:0/q", "https://[2001:db8::zz]/q", "https://192.0.2/q", "https://1-2-3/q", "https://user@192.0.2.53/q", "https://192.0.2.53/q#frag", "https://192.0.2.53/dns-qüery", "https://192.0.2.53/q\u0001", `https://192.0.2.53${longest}a`]) {
    assert.equal(dohProblem(bad), "url", bad);
  }
  for (const char of ["<", ">", "`", '"', "{", "}", "|", "\\", "^", "[", "]", " ", "#"]) {
    assert.equal(dohProblem(`https://192.0.2.53/a${char}b`), "url", char);
  }
  assert.equal(dohProblem("https://192.0.2.53/a-._~!$&'()*+,;=:@/%41?dns=x&ct=1"), null);
  for (const name of ["https://dns.example/dns-query", "https://dns.example:443/dns-query"]) {
    assert.equal(dohProblem(name), "hostname", name);
  }
});

test("a TLS name is a plausible DNS name", () => {
  for (const good of ["dns.example", "dns.example.", "one", "a-b.c_d.example"]) assert.equal(isTlsName(good), true, good);
  for (const bad of ["", "dns example", "https://dns.example", "dns..example", "-dns.example", `${"a".repeat(64)}.example`, `${"a.".repeat(127)}ab`]) {
    assert.equal(isTlsName(bad), false, bad);
  }
  assert.equal(isTlsName(`${"a".repeat(63)}.${"b".repeat(63)}.${"c".repeat(63)}.${"d".repeat(61)}`), true);
});

test("a list's first fault is named by the server's place in it", () => {
  assert.deepEqual(serversProblem(rows({ address: " " })), { key: "next.dns.needsServer" });
  assert.deepEqual(serversProblem(rows({ address: "dns.example" })), { key: "next.dns.badAddress", values: { number: 1 } });
  assert.deepEqual(serversProblem(rows({ address: "192.0.2.53" }, { address: "192.0.2.256" })), { key: "next.dns.badAddress", values: { number: 2 } });
  assert.deepEqual(serversProblem(rows({ wire: "HTTPS", address: "https://dns.example/dns-query" })), { key: "next.dns.urlHostname", values: { number: 1 } });
  assert.deepEqual(serversProblem(rows({ wire: "HTTPS", address: "192.0.2.53" })), { key: "next.dns.badUrl", values: { number: 1 } });
  assert.deepEqual(serversProblem(rows({ wire: "TLS", address: "192.0.2.53", tlsName: "not a name" })), { key: "next.dns.badTlsName", values: { number: 1 } });
  // A TLS name is left out, or written on a TLS row only.
  assert.equal(serversProblem(rows({ wire: "TLS", address: "192.0.2.53" })), null);
  assert.equal(serversProblem(rows({ address: "192.0.2.53", tlsName: "not a name" })), null);
  const listed = (count: number) => rows(...Array.from({ length: count }, (_, at) => ({ address: `192.0.2.${at + 1}` })));
  assert.equal(MAX_DNS_SERVERS, 10);
  assert.equal(serversProblem(listed(10)), null);
  assert.deepEqual(serversProblem(listed(11)), { key: "next.dns.tooMany", values: { count: 10 } });
});

test("a plain server may come only before every encrypted one", () => {
  const tls = { wire: "TLS" as const, address: "192.0.2.53" };
  const https = { wire: "HTTPS" as const, address: "https://192.0.2.53/dns-query" };
  const plain = { address: "192.0.2.54" };
  const refused = { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" };
  assert.deepEqual(serversProblem(rows(tls, plain)), refused);
  assert.deepEqual(serversProblem(rows(https, plain)), refused);
  assert.equal(serversProblem(rows(plain, tls)), null);
  assert.equal(serversProblem(rows(tls, https)), null);
  // A blank row between them is not a server, so it neither breaks nor hides the order.
  assert.deepEqual(serversProblem(rows(tls, { address: " " }, plain)), refused);
  assert.equal(serversProblem(rows(tls, { address: "" })), null);
});

test("a server is listed once, however its address is spelled, and another wire is another server", () => {
  const duplicate = { key: "next.refusal.PROXY_DNS_SERVER_DUPLICATE" };
  const repeated: Partial<DnsServerRow>[][] = [
    [{ address: "192.0.2.53" }, { address: " 192.0.2.53:53" }],
    [{ address: "2001:db8::1" }, { address: "[2001:0db8:0:0::1]:53" }],
    [{ wire: "TLS", address: "192.0.2.53", tlsName: "dns.example" }, { wire: "TLS", address: "192.0.2.53:853", tlsName: "dns.example " }],
    [{ wire: "HTTPS", address: "https://192.0.2.53/dns-query" }, { wire: "HTTPS", address: "https://192.0.2.53:443/dns-query" }],
  ];
  for (const list of repeated) assert.deepEqual(serversProblem(rows(...list)), duplicate, JSON.stringify(list));
  const distinct: Partial<DnsServerRow>[][] = [
    [{ address: "192.0.2.53" }, { wire: "TLS", address: "192.0.2.53" }],
    [{ address: "192.0.2.53" }, { address: "192.0.2.53:5353" }],
    [{ wire: "TLS", address: "192.0.2.53", tlsName: "a.example" }, { wire: "TLS", address: "192.0.2.53", tlsName: "b.example" }],
    [{ wire: "HTTPS", address: "https://192.0.2.53/dns-query" }, { wire: "HTTPS", address: "https://192.0.2.53/other" }],
    [{ address: "192.0.2.53" }, { address: "" }, { address: " " }],
  ];
  for (const list of distinct) assert.equal(serversProblem(rows(...list)), null, JSON.stringify(list));
});

test("a list round trips through its form, in order, with blank rows dropped", () => {
  const stored = {
    id: 4,
    name: "Quad9",
    usedByProfiles: [],
    servers: [
      { wire: "PLAIN" as const, address: "192.0.2.53", tlsName: null, url: null },
      { wire: "TLS" as const, address: "192.0.2.54", tlsName: "dns.example", url: null },
      { wire: "HTTPS" as const, address: "", tlsName: null, url: "https://192.0.2.55/dns-query" },
    ],
  };
  const form = listForm(stored);
  assert.deepEqual(listInput(form), { id: 4, name: "Quad9", servers: stored.servers });
  assert.deepEqual(listInput({ ...form, id: null, servers: [...form.servers, { wire: "PLAIN", address: " ", tlsName: "" }] }), {
    name: "Quad9",
    servers: stored.servers,
  });
  // A new list starts with one row to type in, and needs a name before its servers are judged.
  assert.deepEqual(listForm(null), { id: null, name: "", servers: [{ wire: "PLAIN", address: "", tlsName: "" }] });
  assert.deepEqual(serverRows([]), [{ wire: "PLAIN", address: "", tlsName: "" }]);
  assert.deepEqual(listProblem(listForm(null)), { key: "next.dns.lists.nameRequired" });
  assert.deepEqual(listProblem({ ...listForm(null), name: "x" }), { key: "next.dns.needsServer" });
});

test("each who allows its own trusts, and a profile sends only what its who uses", () => {
  assert.deepEqual(trustChoices("SYSTEM"), ["AS_SENT"]);
  assert.deepEqual(trustChoices("PROXY"), ["AS_SENT", "VALIDATE"]);
  assert.deepEqual(trustChoices("WEAVER"), ["AS_SENT", "VALIDATE"]);
  const fresh = profileForm(null, [{ id: 7 }, { id: 8 }]);
  assert.deepEqual(fresh, { id: null, kind: "ROUTE", name: "", who: "WEAVER", transit: "PROXY", trust: "AS_SENT", serverListId: 7, systemFallback: false });
  const weaver = { ...fresh, id: 3, name: " Checked ", transit: "EGRESS" as const, trust: "VALIDATE" as const };
  assert.deepEqual(profileInput(weaver), { id: 3, kind: "ROUTE", name: "Checked", who: "WEAVER", transit: "EGRESS", trust: "VALIDATE", serverListId: 7, systemFallback: false });
  // Changing who keeps a trust it allows and drops one it does not.
  assert.equal(withWho(weaver, "PROXY").trust, "VALIDATE");
  assert.equal(withWho(weaver, "SYSTEM").trust, "AS_SENT");
  assert.deepEqual(profileInput({ ...weaver, who: "PROXY", trust: "VALIDATE" }), {
    id: 3, kind: "ROUTE", name: "Checked", who: "PROXY", transit: null, trust: "VALIDATE", serverListId: null, systemFallback: false,
  });
  assert.deepEqual(profileInput({ ...weaver, who: "SYSTEM" }), {
    id: 3, kind: "ROUTE", name: "Checked", who: "SYSTEM", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false,
  });
  // Asking the system on a leg with no proxy is sent for the proxy alone.
  assert.equal(profileInput({ ...weaver, who: "PROXY", systemFallback: true }).systemFallback, true);
  assert.equal(profileInput({ ...weaver, who: "SYSTEM", systemFallback: true }).systemFallback, false);
  assert.equal(profileInput({ ...weaver, systemFallback: true }).systemFallback, false);
  const stored = { id: 4, name: "T", builtIn: false, kind: "ROUTE" as const, who: "PROXY" as const, transit: null, trust: "AS_SENT" as const, serverListId: null, systemFallback: true, usedByRoutes: 0, usedByProxies: 0 };
  assert.equal(profileForm(stored, []).systemFallback, true);
  assert.deepEqual(profileInput({ ...fresh, name: "New" }).id, undefined);
  assert.deepEqual(profileProblem(fresh), { key: "next.dns.profiles.nameRequired" });
  assert.deepEqual(profileProblem({ ...fresh, name: "x", serverListId: null }), { key: "next.refusal.DNS_PROFILE_LIST_REQUIRED" });
  assert.equal(profileProblem({ ...fresh, name: "x", who: "PROXY", serverListId: null }), null);
});

test("a proxy name profile has the system or Weaver answer, asks from the egress, and never falls back", () => {
  assert.deepEqual(whoChoices("ROUTE"), ["SYSTEM", "PROXY", "WEAVER"]);
  assert.deepEqual(whoChoices("PROXY_NAME"), ["SYSTEM", "WEAVER"]);
  const fresh = profileForm(null, [{ id: 7 }], "PROXY_NAME");
  assert.deepEqual(fresh, { id: null, kind: "PROXY_NAME", name: "", who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: 7, systemFallback: false });
  // A new profile sends its kind; a transit or fallback left in the form is never sent.
  assert.deepEqual(profileInput({ ...fresh, name: " Names ", transit: "PROXY", trust: "VALIDATE", systemFallback: true }), {
    kind: "PROXY_NAME", name: "Names", who: "WEAVER", transit: "EGRESS", trust: "VALIDATE", serverListId: 7, systemFallback: false,
  });
  assert.deepEqual(profileInput({ ...fresh, id: 6, name: "Names", who: "SYSTEM", trust: "VALIDATE" }), {
    id: 6, kind: "PROXY_NAME", name: "Names", who: "SYSTEM", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false,
  });
  // The proxy cannot answer for its own address.
  assert.equal(profileInput({ ...fresh, name: "x", who: "PROXY", systemFallback: true }).who, "SYSTEM");
  const stored = { id: 6, name: "Names", builtIn: false, kind: "PROXY_NAME" as const, who: "WEAVER" as const, transit: "EGRESS" as const, trust: "AS_SENT" as const, serverListId: 7, systemFallback: false, usedByRoutes: 0, usedByProxies: 2 };
  assert.equal(profileForm(stored, []).kind, "PROXY_NAME");
});

test("a proxy name profile read without transit or fallback opens in its editor as one that has neither", () => {
  const form = profileForm({ id: 6, name: "Names", kind: "PROXY_NAME", who: "WEAVER", trust: "AS_SENT", serverListId: 7 }, [{ id: 7 }]);
  assert.equal(form.systemFallback, false);
  assert.deepEqual(profileInput(form), { id: 6, kind: "PROXY_NAME", name: "Names", who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: 7, systemFallback: false });
});

test("finding a proxy, the system is the host, and the built-in profile is named for it", () => {
  assert.equal(dnsWhoKey("SYSTEM", "PROXY_NAME"), "next.dns.who.HOST");
  assert.equal(dnsWhoKey("SYSTEM", "ROUTE"), "next.dns.who.SYSTEM");
  assert.equal(dnsWhoKey("WEAVER", "PROXY_NAME"), "next.dns.who.WEAVER");
  const builtIn = { id: 0, name: "System" };
  assert.equal(dnsProfileName(t, builtIn, "PROXY_NAME"), "next.dns.profiles.hostName");
  assert.equal(dnsProfileName(t, builtIn, "ROUTE"), "System");
  assert.equal(dnsProfileName(t, { id: 6, name: "Proxy names" }, "PROXY_NAME"), "Proxy names");
  assert.equal(
    profileSummary(t, { kind: "PROXY_NAME", who: "SYSTEM", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false }, []),
    "next.dns.who.HOST",
  );
});

test("a profile reads in one line", () => {
  const lists = [{
    id: 1,
    name: "Quad9",
    servers: [
      { wire: "TLS" as const, address: "192.0.2.53", tlsName: null, url: null },
      { wire: "HTTPS" as const, address: "", tlsName: null, url: "https://192.0.2.53/q" },
    ],
  }];
  assert.equal(profileSummary(t, { kind: "ROUTE", who: "SYSTEM", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false }, lists), "next.dns.who.SYSTEM");
  assert.equal(
    profileSummary(t, { kind: "ROUTE", who: "PROXY", transit: null, trust: "VALIDATE", serverListId: null, systemFallback: false }, lists),
    "next.dns.who.PROXY · next.dns.trust.VALIDATE",
  );
  assert.equal(
    profileSummary(t, { kind: "ROUTE", who: "PROXY", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: true }, lists),
    "next.dns.who.PROXY · next.dns.trust.AS_SENT · next.dns.systemFallbackOn",
  );
  assert.equal(
    profileSummary(t, { kind: "ROUTE", who: "WEAVER", transit: "EGRESS", trust: "VALIDATE", serverListId: 1, systemFallback: false }, lists),
    "next.dns.who.WEAVER · Quad9 next.dns.wireShort.TLS next.dns.wireShort.HTTPS · next.dns.transit.EGRESS · next.dns.trust.VALIDATE",
  );
  assert.equal(
    profileSummary(t, { kind: "ROUTE", who: "WEAVER", transit: "PROXY", trust: "AS_SENT", serverListId: 9, systemFallback: false }, lists),
    "next.dns.who.WEAVER · next.dns.listMissing · next.dns.transit.PROXY · next.dns.trust.AS_SENT",
  );
  // A proxy's name is always asked from the egress, so its line names no transit.
  assert.equal(
    profileSummary(t, { kind: "PROXY_NAME", who: "WEAVER", transit: "EGRESS", trust: "VALIDATE", serverListId: 1, systemFallback: false }, lists),
    "next.dns.who.WEAVER · Quad9 next.dns.wireShort.TLS next.dns.wireShort.HTTPS · next.dns.trust.VALIDATE",
  );
});

test("a route takes a proxy-answered profile only where every leg and rung has a proxy, and validation only through WireGuard", () => {
  const known = {
    proxies: [
      { id: 1, kind: "SOCKS5" as const, dnsServers: [] },
      { id: 2, kind: "WIRE_GUARD" as const, dnsServers: ["10.64.0.1"] },
      { id: 3, kind: "WIRE_GUARD" as const, dnsServers: [] },
      { id: 4, kind: "WIRE_GUARD" as const },
    ],
    pools: [{ id: 5, kind: "WIRE_GUARD" as const, memberIds: [2] }, { id: 6, kind: "WIRE_GUARD" as const, memberIds: [2, 3] }],
  };
  const ladder = (rungs: NetworkRoute["legs"][number]["path"]["rungs"], directFallback = false): NetworkRoute => ({
    failover: "REDISTRIBUTE",
    legs: [{ egressId: 0, weight: 100, path: { kind: "LADDER", rungs, directFallback } }],
  });
  const socks = { kind: "PROXY" as const, proxyId: 1, poolId: null, chainIds: [] };
  const wireguard = { kind: "PROXY" as const, proxyId: 2, poolId: null, chainIds: [] };
  const pool = { kind: "POOL" as const, proxyId: null, poolId: 5, chainIds: [] };
  const chain = { kind: "CHAIN" as const, proxyId: null, poolId: null, chainIds: [1, 2] };
  const direct: NetworkRoute = { failover: "REDISTRIBUTE", legs: [{ egressId: 0, weight: 100, path: { kind: "DIRECT", rungs: [], directFallback: false } }] };
  const proxy = { who: "PROXY" as const, trust: "AS_SENT" as const, systemFallback: false };
  const validate = { who: "PROXY" as const, trust: "VALIDATE" as const, systemFallback: false };
  const fallback = { ...proxy, systemFallback: true };
  assert.equal(dnsRouteProblem(direct, proxy, known), "needsProxy");
  assert.equal(dnsRouteProblem(ladder([socks], true), proxy, known), "needsProxy");
  assert.equal(dnsRouteProblem(ladder([socks]), proxy, known), null);
  // With the system asked on a leg with no proxy, a direct leg or fallback is no problem.
  assert.equal(dnsRouteProblem(direct, fallback, known), null);
  assert.equal(dnsRouteProblem(ladder([socks], true), fallback, known), null);
  assert.equal(dnsRouteProblem(ladder([socks]), { ...validate, systemFallback: true }, known), "needsWireGuard");
  assert.equal(dnsRouteProblem(ladder([socks]), validate, known), "needsWireGuard");
  assert.equal(dnsRouteProblem(ladder([wireguard, pool, chain]), validate, known), null);
  // Validating takes every stage to WireGuard, so a direct leg or fallback is refused even where the system answers.
  assert.equal(dnsRouteProblem(direct, { ...validate, systemFallback: true }, known), "needsWireGuard");
  assert.equal(dnsRouteProblem(ladder([wireguard], true), { ...validate, systemFallback: true }, known), "needsWireGuard");
  assert.equal(dnsRouteProblem(ladder([wireguard], true), validate, known), "needsProxy");
  // A WireGuard proxy that answers needs its tunnel's DNS servers, a pool's members among them.
  const silent = { kind: "PROXY" as const, proxyId: 3, poolId: null, chainIds: [] };
  assert.equal(dnsRouteProblem(ladder([silent]), proxy, known), "needsTunnelDns");
  assert.equal(dnsRouteProblem(ladder([silent]), validate, known), "needsTunnelDns");
  assert.equal(dnsRouteProblem(ladder([{ ...pool, poolId: 6 }]), proxy, known), "needsTunnelDns");
  assert.deepEqual(wireGuardWithoutDns(ladder([{ ...pool, poolId: 6 }, silent, { ...chain, chainIds: [1, 3] }]), known), [3]);
  // Servers this page has not loaded are not taken for none.
  assert.equal(dnsRouteProblem(ladder([{ ...wireguard, proxyId: 4 }]), proxy, known), null);
  // A rung whose proxy or pool is not loaded is not taken for WireGuard.
  assert.equal(dnsRouteProblem(ladder([{ ...wireguard, proxyId: 9 }]), validate, known), "needsWireGuard");
  assert.equal(dnsRouteProblem(ladder([{ ...pool, poolId: 9 }]), validate, known), "needsWireGuard");
  // Any other who, or no profile, sets nothing on the route.
  assert.equal(dnsRouteProblem(direct, { who: "WEAVER", trust: "VALIDATE", systemFallback: false }, known), null);
  assert.equal(dnsRouteProblem(direct, undefined, known), null);
  assert.equal(dnsRouteProblemKey("needsProxy"), "next.dns.routeNeedsProxy");
  assert.equal(dnsRouteProblemKey("needsWireGuard"), "next.dns.routeNeedsWireGuard");
  assert.equal(dnsRouteProblemKey("needsTunnelDns"), "next.dns.routeNeedsTunnelDns");
});

const yes = (millis: number): DnsProbe => ({ supported: true, millis, error: null });
const no = (code: string): DnsProbe => ({ supported: false, millis: 0, error: { code, detail: "" } });

test("a test sends the named rows and maps each result back to its form row", () => {
  const form = rows({ address: "192.0.2.53" }, { address: " " }, { wire: "TLS", address: "192.0.2.54" });
  const request = probeRequest(form);
  assert.deepEqual(request.rows, [0, 2]);
  assert.deepEqual(request.servers.map((server) => server.address), ["192.0.2.53", "192.0.2.54"]);
  const results: DnsServerCapability[] = [
    { index: 1, plain: yes(3), tls: no("TLS_HANDSHAKE"), https: no("REFUSED"), dnssec: yes(12) },
    { index: 0, plain: yes(2.5), tls: yes(9), https: no("REFUSED"), dnssec: no("BOGUS") },
  ];
  const run: DnsTestRun = { key: serverRowsKey(form), rows: request.rows, route: "eth0 → direct", results };
  const table = capabilityTable(run, form)!;
  assert.equal(table.route, "eth0 → direct");
  assert.equal(table.pending, false);
  assert.deepEqual(table.rows.map((line) => [line.row, line.address]), [[0, "192.0.2.53"], [2, "192.0.2.54"]]);
  const tlsRow = table.rows[1]!;
  // The row is set to TLS, so its TLS cell is its own wire, and that one failed.
  assert.deepEqual(tlsRow.cells.tls, { supported: false, millis: 0, error: { code: "TLS_HANDSHAKE", detail: "" }, own: true });
  assert.equal(tlsRow.cells.plain.own, false);
  assert.equal(tlsRow.cells.dnssec.supported, true);
  assert.equal(table.rows[0]!.cells.dnssec.supported, false);
  assert.deepEqual(Object.keys(tlsRow.cells), ["plain", "tls", "https", "dnssec"]);
});

test("a test in flight shows every tested row pending", () => {
  const form = rows({ address: "192.0.2.53" });
  const table = capabilityTable({ key: serverRowsKey(form), rows: [0], route: "r", results: null }, form)!;
  assert.equal(table.pending, true);
  assert.deepEqual(table.rows[0]!.cells.plain, { supported: null, millis: null, error: null, own: true });
});

test("any change to the rows clears the results", () => {
  const form = rows({ address: "192.0.2.53" }, { address: "192.0.2.54" });
  const run: DnsTestRun = {
    key: serverRowsKey(form),
    rows: [0, 1],
    route: "r",
    results: [{ index: 0, plain: yes(1), tls: no("REFUSED"), https: no("REFUSED"), dnssec: no("REFUSED") }],
  };
  assert.notEqual(capabilityTable(run, form), null);
  assert.equal(capabilityTable(run, rows({ address: "192.0.2.53" }, { address: "192.0.2.55" })), null);
  assert.equal(capabilityTable(run, [...form].reverse()), null);
  assert.equal(capabilityTable(run, [...form, { wire: "PLAIN", address: "", tlsName: "" }]), null);
  assert.equal(capabilityTable(run, [{ ...form[0]!, wire: "TLS" }, form[1]!]), null);
  assert.equal(capabilityTable(null, form), null);
});

test("a refused test names the form row of the server sent, and is dropped once the rows change", () => {
  const form = rows({ address: "" }, { address: "192.0.2.99" }, { address: "192.0.2.53" });
  const request = probeRequest(form);
  const sent = { key: serverRowsKey(form), rows: request.rows };
  assert.deepEqual(testRefusalRow(sent, 0, form), { row: 1 });
  assert.deepEqual(testRefusalRow(sent, 1, form), { row: 2 });
  assert.deepEqual(testRefusalRow(sent, undefined, form), { row: null });
  assert.deepEqual(testRefusalRow(sent, 7, form), { row: null });
  // Filling the blank row moves the refused server's place among those sent onto another row.
  assert.equal(testRefusalRow(sent, 0, [{ ...form[0]!, address: "192.0.2.1" }, form[1]!, form[2]!]), null);
  assert.equal(testRefusalRow(sent, 0, [form[0]!, form[2]!, form[1]!]), null);
  assert.equal(testRefusalRow(sent, 0, form.slice(0, 2)), null);
  // A refusal that names no server, such as one of the route, stands whatever the rows became.
  assert.deepEqual(testRefusalRow(sent, undefined, [...form, { wire: "PLAIN", address: "", tlsName: "" }]), { row: null });
  assert.deepEqual(testRefusalRow(sent, undefined, [form[0]!, form[2]!, form[1]!]), { row: null });
});

test("the results caption names each leg's egress and the proxies it goes through", () => {
  const names = {
    egresses: [{ id: 0, name: "System" }, { id: 3, name: "wlan0" }],
    proxies: [{ id: 1, name: "Mullvad" }, { id: 2, name: "Exit" }],
    pools: [{ id: 5, name: "Pool A" }],
  };
  const route: NetworkRoute = {
    failover: "REDISTRIBUTE",
    legs: [
      { egressId: 0, weight: 50, path: { kind: "LADDER", rungs: [{ kind: "CHAIN", proxyId: null, poolId: null, chainIds: [1, 2] }, { kind: "POOL", proxyId: null, poolId: 5, chainIds: [] }], directFallback: false } },
      { egressId: 3, weight: 50, path: { kind: "DIRECT", rungs: [], directFallback: false } },
    ],
  };
  assert.equal(routeCaption(route, names, "Direct"), "System → Mullvad + Exit → Pool A, wlan0 → Direct");
  // A ladder's direct fallback comes last.
  const fallback: NetworkRoute = { ...route, legs: [{ ...route.legs[0]!, path: { ...route.legs[0]!.path, directFallback: true } }] };
  assert.equal(routeCaption(fallback, names, "Direct"), "System → Mullvad + Exit → Pool A → Direct");
});

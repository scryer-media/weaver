// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  HOST_EGRESS_DNS,
  MAX_DNS_SERVERS,
  NEW_DNS,
  dnsForm,
  dnsInput,
  dnsProblem,
  dohProblem,
  effectiveDns,
  egressDnsForm,
  egressDnsInput,
  egressDnsProblem,
  egressPlainCanFakeAuthentication,
  isIpAddress,
  isTlsName,
  legacyDns,
  plainCanFakeAuthentication,
  proxyAnswers,
  wireguardDns,
  type DnsForm,
  type DnsServerRow,
} from "../src/lib/proxy-dns.ts";

const plain = (address: string) => ({ wire: "PLAIN" as const, address, tlsName: null, url: null });
const servers = (...rows: Partial<DnsServerRow>[]): DnsForm => ({
  who: "SERVERS",
  path: "THROUGH_PROXY",
  trust: "AS_SENT",
  servers: rows.map((row) => ({ wire: "PLAIN", address: "", tlsName: "", ...row })),
});

test("the older resolver and server list map onto who answers and the path", () => {
  assert.deepEqual(legacyDns("WEAVER", []), { who: "HOST", path: "THROUGH_PROXY", trust: "AS_SENT", servers: [] });
  assert.deepEqual(legacyDns("PROXY", []), { who: "PROXY", path: "THROUGH_PROXY", trust: "AS_SENT", servers: [] });
  assert.deepEqual(legacyDns("WEAVER", ["192.0.2.53", "192.0.2.54"]), {
    who: "SERVERS", path: "DIRECT", trust: "AS_SENT", servers: [plain("192.0.2.53"), plain("192.0.2.54")],
  });
  assert.deepEqual(legacyDns("PROXY", ["192.0.2.53"]), {
    who: "SERVERS", path: "THROUGH_PROXY", trust: "AS_SENT", servers: [plain("192.0.2.53")],
  });
});

test("a new proxy has the host answer, and a WireGuard file's servers are plain ones asked through the tunnel", () => {
  assert.equal(NEW_DNS.who, "HOST");
  assert.deepEqual(wireguardDns(["10.6.0.1", "fd00::1"]), {
    who: "SERVERS",
    path: "THROUGH_PROXY",
    trust: "AS_SENT",
    servers: [
      { wire: "PLAIN", address: "10.6.0.1", tlsName: "" },
      { wire: "PLAIN", address: "fd00::1", tlsName: "" },
    ],
  });
});

test("a stored proxy's servers fill the form, an HTTPS row by its URL, and the list always has a row", () => {
  const form = dnsForm({
    who: "SERVERS",
    path: "DIRECT",
    trust: "REQUIRE_AD",
    servers: [
      { wire: "TLS", address: "192.0.2.53", tlsName: "dns.example", url: null },
      { wire: "HTTPS", address: "192.0.2.54", tlsName: null, url: "https://192.0.2.54/dns-query" },
    ],
  });
  assert.deepEqual(form.servers, [
    { wire: "TLS", address: "192.0.2.53", tlsName: "dns.example" },
    { wire: "HTTPS", address: "https://192.0.2.54/dns-query", tlsName: "" },
  ]);
  assert.deepEqual(dnsForm(legacyDns("WEAVER", [])).servers, [{ wire: "PLAIN", address: "", tlsName: "" }]);
});

test("each wire is sent in its own shape, and blank rows are not sent", () => {
  const form = servers(
    { wire: "PLAIN", address: " 192.0.2.53:5353 ", tlsName: "ignored.example" },
    { wire: "TLS", address: "[2001:db8::1]:853", tlsName: " dns.example " },
    { wire: "TLS", address: "192.0.2.55", tlsName: "" },
    { wire: "HTTPS", address: "https://192.0.2.54/dns-query", tlsName: "ignored.example" },
    { wire: "PLAIN", address: "  " },
  );
  assert.deepEqual(dnsInput({ ...form, path: "DIRECT", trust: "REQUIRE_AD" }, "SOCKS5"), {
    who: "SERVERS",
    path: "DIRECT",
    trust: "REQUIRE_AD",
    servers: [
      { wire: "PLAIN", address: "192.0.2.53:5353", tlsName: null, url: null },
      { wire: "TLS", address: "[2001:db8::1]:853", tlsName: "dns.example", url: null },
      { wire: "TLS", address: "192.0.2.55", tlsName: null, url: null },
      { wire: "HTTPS", address: "", tlsName: null, url: "https://192.0.2.54/dns-query" },
    ],
  });
});

test("the host or the proxy answering sends no servers, whatever the form still holds", () => {
  const form = { ...servers({ address: "192.0.2.53" }), path: "DIRECT" as const, trust: "REQUIRE_AD" as const };
  for (const who of ["HOST", "PROXY"] as const) {
    assert.deepEqual(dnsInput({ ...form, who }, "SOCKS5"), { who, path: "THROUGH_PROXY", trust: "AS_SENT", servers: [] });
  }
});

test("a WireGuard proxy is never answered by the proxy, and its servers are asked from where they were saved", () => {
  assert.equal(proxyAnswers("WIRE_GUARD"), false);
  for (const kind of ["SOCKS5", "HTTP_CONNECT", "HTTP3_CONNECT", "SSH"] as const) assert.equal(proxyAnswers(kind), true);
  const direct = { ...servers({ address: "10.6.0.1" }), path: "DIRECT" as const };
  assert.equal(effectiveDns(direct, "WIRE_GUARD").path, "DIRECT");
  assert.equal(effectiveDns(direct, "SOCKS5").path, "DIRECT");
  // A form left on the proxy answering, then made WireGuard, means its servers.
  const proxy = { ...direct, who: "PROXY" as const };
  assert.equal(effectiveDns(proxy, "WIRE_GUARD").who, "SERVERS");
  assert.equal(dnsInput(proxy, "WIRE_GUARD").who, "SERVERS");
  assert.equal(effectiveDns({ ...direct, who: "HOST" }, "WIRE_GUARD").who, "HOST");
});

test("a save that leaves a WireGuard proxy's DNS alone sends back what is stored", () => {
  // An older "Weaver + servers" profile is migrated to its servers asked directly.
  const stored = {
    who: "SERVERS" as const,
    path: "DIRECT" as const,
    trust: "AS_SENT" as const,
    servers: [{ wire: "PLAIN" as const, address: "10.6.0.1", tlsName: null, url: null }],
  };
  assert.deepEqual(dnsInput(dnsForm(stored), "WIRE_GUARD"), stored);
  const inTunnel = { ...stored, path: "THROUGH_PROXY" as const };
  assert.deepEqual(dnsInput(dnsForm(inTunnel), "WIRE_GUARD"), inTunnel);
  // A new one, and one read from a wg-quick file, ask their servers through the tunnel.
  assert.equal(dnsInput({ ...NEW_DNS, who: "SERVERS", servers: [{ wire: "PLAIN", address: "10.6.0.1", tlsName: "" }] }, "WIRE_GUARD").path, "THROUGH_PROXY");
  assert.equal(dnsInput(wireguardDns(["10.6.0.1"]), "WIRE_GUARD").path, "THROUGH_PROXY");
});

test("the plain-query warning needs authenticated answers and a plain server, on either path", () => {
  const base = { ...servers({ wire: "PLAIN", address: "192.0.2.53" }, { wire: "TLS", address: "192.0.2.54" }), path: "DIRECT" as const, trust: "REQUIRE_AD" as const };
  assert.equal(plainCanFakeAuthentication(base, "SOCKS5"), true);
  assert.equal(plainCanFakeAuthentication({ ...base, trust: "AS_SENT" }, "SOCKS5"), false);
  // A plain query through the proxy can be altered past it just the same.
  assert.equal(plainCanFakeAuthentication({ ...base, path: "THROUGH_PROXY" }, "SOCKS5"), true);
  assert.equal(plainCanFakeAuthentication({ ...base, servers: base.servers.map((row) => ({ ...row, wire: "TLS" as const })) }, "SOCKS5"), false);
  // A WireGuard proxy's plain servers are open to the same forgery, asked directly or through the tunnel.
  assert.equal(plainCanFakeAuthentication(base, "WIRE_GUARD"), true);
  assert.equal(plainCanFakeAuthentication({ ...base, path: "THROUGH_PROXY" }, "WIRE_GUARD"), true);
  assert.equal(plainCanFakeAuthentication({ ...base, who: "HOST" }, "SOCKS5"), false);
  // Validating here checks the signatures, not the mark, so nothing is faked that way.
  assert.equal(plainCanFakeAuthentication({ ...base, trust: "VALIDATE" }, "SOCKS5"), false);
  assert.equal(plainCanFakeAuthentication({ ...base, trust: "VALIDATE" }, "WIRE_GUARD"), false);
});

test("validating here is sent with its servers, and dropped when no servers answer", () => {
  const form = { ...servers({ wire: "TLS", address: "192.0.2.53" }), path: "THROUGH_PROXY" as const, trust: "VALIDATE" as const };
  assert.deepEqual(dnsInput(form, "SOCKS5"), {
    who: "SERVERS",
    path: "THROUGH_PROXY",
    trust: "VALIDATE",
    servers: [{ wire: "TLS", address: "192.0.2.53", tlsName: null, url: null }],
  });
  assert.equal(dnsInput(form, "WIRE_GUARD").trust, "VALIDATE");
  assert.equal(dnsForm({ ...dnsInput(form, "SOCKS5"), servers: [] }).trust, "VALIDATE");
  for (const who of ["HOST", "PROXY"] as const) {
    assert.deepEqual(dnsInput({ ...form, who }, "SOCKS5"), { who, path: "THROUGH_PROXY", trust: "AS_SENT", servers: [] });
  }
  // Validating needs a server to ask, as any servers answering does.
  assert.deepEqual(dnsProblem({ ...form, servers: [{ wire: "PLAIN", address: "", tlsName: "" }] }, "SOCKS5"), { key: "next.proxies.needsDns" });
});

test("the plain-query warning looks only at the servers that will be sent", () => {
  const direct = { path: "DIRECT" as const, trust: "REQUIRE_AD" as const };
  // "Add server" leaves a blank plain row, which is not a server.
  assert.equal(plainCanFakeAuthentication({ ...servers({ wire: "TLS", address: "192.0.2.1" }, { wire: "PLAIN", address: "" }), ...direct }, "SOCKS5"), false);
  assert.equal(plainCanFakeAuthentication({ ...servers({ wire: "PLAIN", address: " " }), ...direct }, "SOCKS5"), false);
  assert.equal(plainCanFakeAuthentication({ ...servers({ wire: "PLAIN", address: "192.0.2.2" }), ...direct }, "SOCKS5"), true);
  // A plain server after an encrypted one is refused before it could be sent.
  const refused = { ...servers({ wire: "TLS", address: "192.0.2.1" }, { wire: "PLAIN", address: "192.0.2.2" }), ...direct };
  assert.deepEqual(dnsProblem(refused, "SOCKS5"), { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" });
});

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
  // The path holds only what a URI path and query carry unescaped, 2048 characters at most; a host with no letter in it is no name.
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

test("the form's first DNS fault is named by the server's place in the list", () => {
  assert.equal(dnsProblem(servers({ address: "dns.example" }), "SOCKS5")?.key, "next.proxies.dnsBadAddress");
  assert.equal(dnsProblem({ ...servers({ address: "dns.example" }), who: "HOST" }, "SOCKS5"), null);
  assert.deepEqual(dnsProblem(servers({ address: " " }), "SOCKS5"), { key: "next.proxies.needsDns" });
  assert.deepEqual(dnsProblem(servers({ address: "192.0.2.53" }, { address: "192.0.2.256" }), "SOCKS5"), {
    key: "next.proxies.dnsBadAddress", values: { number: 2 },
  });
  assert.deepEqual(dnsProblem(servers({ wire: "HTTPS", address: "https://dns.example/dns-query" }), "SOCKS5"), {
    key: "next.proxies.dnsUrlHostname", values: { number: 1 },
  });
  assert.deepEqual(dnsProblem(servers({ wire: "HTTPS", address: "192.0.2.53" }), "SOCKS5"), {
    key: "next.proxies.dnsBadUrl", values: { number: 1 },
  });
  assert.deepEqual(dnsProblem(servers({ wire: "TLS", address: "192.0.2.53", tlsName: "not a name" }), "SOCKS5"), {
    key: "next.proxies.dnsBadTlsName", values: { number: 1 },
  });
  // A TLS name is left out, or written on a TLS row only.
  assert.equal(dnsProblem(servers({ wire: "TLS", address: "192.0.2.53", tlsName: "" }), "SOCKS5"), null);
  assert.equal(dnsProblem(servers({ wire: "PLAIN", address: "192.0.2.53", tlsName: "not a name" }), "SOCKS5"), null);
  const listed = (count: number) => servers(...Array.from({ length: count }, (_, at) => ({ address: `192.0.2.${at + 1}` })));
  assert.equal(MAX_DNS_SERVERS, 10);
  assert.equal(dnsProblem(listed(10), "SOCKS5"), null);
  assert.deepEqual(dnsProblem(listed(11), "SOCKS5"), { key: "next.proxies.dnsTooMany", values: { count: 10 } });
  // On WireGuard the proxy answering means its servers, which must be named.
  assert.deepEqual(dnsProblem({ ...servers({ address: "" }), who: "PROXY" }, "WIRE_GUARD"), { key: "next.proxies.needsDns" });
});

test("a plain server may come only before every encrypted one, as the daemon refuses otherwise", () => {
  const tls = { wire: "TLS" as const, address: "192.0.2.53" };
  const https = { wire: "HTTPS" as const, address: "https://192.0.2.53/dns-query" };
  const plain = { wire: "PLAIN" as const, address: "192.0.2.54" };
  const refused = { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" };
  assert.deepEqual(dnsProblem(servers(tls, plain), "SOCKS5"), refused);
  assert.deepEqual(dnsProblem(servers(https, plain), "SOCKS5"), refused);
  assert.equal(dnsProblem(servers(plain, tls), "SOCKS5"), null);
  assert.equal(dnsProblem(servers(plain, { ...plain, address: "192.0.2.55" }), "SOCKS5"), null);
  assert.equal(dnsProblem(servers(tls, https), "SOCKS5"), null);
  // A blank row between them is not a server, so it neither breaks nor hides the order.
  assert.deepEqual(dnsProblem(servers(tls, { wire: "PLAIN", address: " " }, plain), "SOCKS5"), refused);
  assert.equal(dnsProblem(servers(tls, { wire: "PLAIN", address: "" }), "SOCKS5"), null);
});

test("an egress's DNS is the host or its servers, asked straight from it, checked as a proxy's are", () => {
  // A stored egress with no DNS yet, and a new one, are the host answering.
  assert.deepEqual(egressDnsForm(undefined), { who: "HOST", path: "DIRECT", trust: "AS_SENT", servers: [{ wire: "PLAIN", address: "", tlsName: "" }] });
  assert.deepEqual(egressDnsInput(egressDnsForm(HOST_EGRESS_DNS)), HOST_EGRESS_DNS);
  const stored = {
    who: "SERVERS" as const,
    trust: "REQUIRE_AD" as const,
    servers: [plain("192.0.2.53"), { wire: "TLS" as const, address: "192.0.2.54", tlsName: "dns.fixture.invalid", url: null }],
  };
  const form = egressDnsForm(stored);
  assert.equal(form.path, "DIRECT");
  // The order of the rows is the order the servers are asked in, and it round trips.
  assert.deepEqual(egressDnsInput(form), stored);
  // Host drops the servers and the trust, as a proxy's host does.
  assert.deepEqual(egressDnsInput({ ...form, who: "HOST" }), HOST_EGRESS_DNS);
  assert.equal(egressDnsProblem(form), null);
  assert.equal(egressDnsProblem({ ...form, who: "HOST", servers: [{ wire: "PLAIN", address: "nonsense", tlsName: "" }] }), null);
  assert.deepEqual(egressDnsProblem({ ...form, servers: [{ wire: "PLAIN", address: " ", tlsName: "" }] }), { key: "next.proxies.needsDns" });
  const rows = (count: number) => Array.from({ length: count }, (_, at) => ({ wire: "PLAIN" as const, address: `192.0.2.${at + 1}`, tlsName: "" }));
  assert.equal(egressDnsProblem({ ...form, servers: rows(10) }), null);
  assert.deepEqual(egressDnsProblem({ ...form, servers: rows(11) }), { key: "next.proxies.dnsTooMany", values: { count: 10 } });
  assert.deepEqual(egressDnsProblem({ ...form, servers: [...form.servers].reverse() }), { key: "next.refusal.PROXY_DNS_PLAIN_AFTER_ENCRYPTED" });
});

test("an egress's plain servers warn under required authentication only, and blank rows never do", () => {
  const form = egressDnsForm({ who: "SERVERS", trust: "REQUIRE_AD", servers: [plain("192.0.2.53")] });
  assert.equal(egressPlainCanFakeAuthentication(form), true);
  assert.equal(egressPlainCanFakeAuthentication({ ...form, trust: "VALIDATE" }), false);
  assert.equal(egressPlainCanFakeAuthentication({ ...form, trust: "AS_SENT" }), false);
  assert.equal(egressPlainCanFakeAuthentication({ ...form, who: "HOST" }), false);
  assert.equal(egressPlainCanFakeAuthentication({ ...form, servers: [{ wire: "PLAIN", address: "", tlsName: "" }] }), false);
  assert.equal(egressPlainCanFakeAuthentication({ ...form, servers: [{ wire: "TLS", address: "192.0.2.53", tlsName: "" }] }), false);
});

test("a server is listed once, however its address is spelled, and another wire is another server", () => {
  const form = (rows: { wire: "PLAIN" | "TLS" | "HTTPS"; address: string; tlsName?: string }[]) => ({
    who: "SERVERS" as const,
    path: "DIRECT" as const,
    trust: "AS_SENT" as const,
    servers: rows.map((row) => ({ tlsName: "", ...row })),
  });
  const duplicate = { key: "next.refusal.PROXY_DNS_SERVER_DUPLICATE" };
  const repeated = [
    [{ wire: "PLAIN" as const, address: "192.0.2.53" }, { wire: "PLAIN" as const, address: " 192.0.2.53:53" }],
    [{ wire: "PLAIN" as const, address: "2001:db8::1" }, { wire: "PLAIN" as const, address: "[2001:0db8:0:0::1]:53" }],
    [{ wire: "TLS" as const, address: "192.0.2.53", tlsName: "dns.example" }, { wire: "TLS" as const, address: "192.0.2.53:853", tlsName: "dns.example " }],
    [{ wire: "HTTPS" as const, address: "https://192.0.2.53/dns-query" }, { wire: "HTTPS" as const, address: "https://192.0.2.53:443/dns-query" }],
  ];
  for (const rows of repeated) {
    assert.deepEqual(dnsProblem(form(rows), "SOCKS5"), duplicate, JSON.stringify(rows));
    assert.deepEqual(egressDnsProblem(form(rows)), duplicate, JSON.stringify(rows));
  }
  const distinct = [
    [{ wire: "PLAIN" as const, address: "192.0.2.53" }, { wire: "TLS" as const, address: "192.0.2.53" }],
    [{ wire: "PLAIN" as const, address: "192.0.2.53" }, { wire: "PLAIN" as const, address: "192.0.2.53:5353" }],
    [{ wire: "TLS" as const, address: "192.0.2.53", tlsName: "a.example" }, { wire: "TLS" as const, address: "192.0.2.53", tlsName: "b.example" }],
    [{ wire: "TLS" as const, address: "192.0.2.53" }, { wire: "TLS" as const, address: "192.0.2.53", tlsName: "a.example" }],
    [{ wire: "HTTPS" as const, address: "https://192.0.2.53/dns-query" }, { wire: "HTTPS" as const, address: "https://192.0.2.53/other" }],
    // A blank row names no server.
    [{ wire: "PLAIN" as const, address: "192.0.2.53" }, { wire: "PLAIN" as const, address: "" }, { wire: "PLAIN" as const, address: " " }],
  ];
  for (const rows of distinct) {
    assert.equal(dnsProblem(form(rows), "SOCKS5"), null, JSON.stringify(rows));
    assert.equal(egressDnsProblem(form(rows)), null, JSON.stringify(rows));
  }
});

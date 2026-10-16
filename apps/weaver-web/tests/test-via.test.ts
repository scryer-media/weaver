// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import type { ProxyPool } from "../src/lib/networking.ts";
import {
  draftRoute,
  lookupErrorKey,
  resolutionParts,
  resolutionText,
  routeChoices,
  testSummary,
  type HostResolution,
  type PathTestResult,
} from "../src/lib/test-via.ts";
import { nextEn } from "../src/next/i18n/en.ts";

const english = (key: string, values: Record<string, unknown> = {}) =>
  Object.entries(values).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), nextEn[key] ?? key);

const profiles = [{ id: 1 }, { id: 2 }, { id: 3 }];
const pools: ProxyPool[] = [
  { id: 10, name: "Exits", kind: "SOCKS5", memberIds: [3], enabled: true },
  { id: 11, name: "Hops", kind: "WIRE_GUARD", memberIds: [1], enabled: true },
];

const path = (result: Partial<PathTestResult>): PathTestResult => ({
  leg: 0,
  egressId: 0,
  carrierIds: [],
  proxyId: null,
  success: true,
  code: "CONNECTED",
  message: "",
  params: {},
  failingProxyId: null,
  sourceAddress: null,
  connectMillis: 0,
  resolution: null,
  ...result,
});

test("a test draft starts direct, on the egress it was opened from or else the first enabled one", () => {
  const egresses = [
    { id: 4, enabled: false },
    { id: 7, enabled: true },
  ];
  assert.deepEqual(draftRoute(egresses).legs.map((leg) => [leg.egressId, leg.weight, leg.path.kind]), [[7, 100, "DIRECT"]]);
  assert.equal(draftRoute(egresses, 4).legs[0]?.egressId, 4);
  assert.equal(draftRoute(egresses).legs.length, 1);
});

test("a proxy under test is not offered to the route it is tested through, nor is a pool holding it", () => {
  const forProxy = routeChoices(profiles, pools, { proxy: 3 });
  assert.deepEqual(forProxy.profiles.map((profile) => profile.id), [1, 2]);
  assert.deepEqual(forProxy.pools.map((pool) => pool.id), [11]);

  const forPool = routeChoices(profiles, pools, { pool: 11 });
  assert.deepEqual(forPool.profiles.map((profile) => profile.id), [2, 3]);
  assert.deepEqual(forPool.pools.map((pool) => pool.id), [10]);

  const routeAlone = routeChoices(profiles, pools, null);
  assert.equal(routeAlone.profiles.length, 3);
  assert.equal(routeAlone.pools.length, 2);
});

test("a run reads as its first failing path, else its first path", () => {
  assert.equal(testSummary(english, []), null);
  assert.equal(
    testSummary(english, [path({ code: "FORWARDED" }), path({ code: "CONNECTED" })]),
    "Connected and forwarded through the proxy.",
  );
  assert.equal(
    testSummary(english, [
      path({ code: "FORWARDED" }),
      path({
        success: false,
        code: "TIMED_OUT",
        params: { seconds: 10 },
        message: "No answer within 10 seconds.",
        failingProxyId: 2,
      }),
    ]),
    "No answer within 10 seconds. Check the address, the port and any firewall.",
  );
});

const resolution = (fields: Partial<HostResolution>): HostResolution => ({
  host: "news.example.com",
  resolvedBy: "HOST",
  proxyId: null,
  dnsServers: [],
  addresses: [],
  cached: false,
  connectedAddress: null,
  error: null,
  ...fields,
});
const names = { egress: "WAN", proxy: (id: number) => `proxy ${id}` };
const line = (fields: Partial<HostResolution>) => resolutionText(resolutionParts(english, resolution(fields), names));

test("a resolution line names what the host resolved to, who resolved it, and whether the answer was held", () => {
  assert.equal(
    line({ addresses: ["192.0.2.1"], connectedAddress: "192.0.2.1", cached: true }),
    "news.example.com → 192.0.2.1 · host DNS · cached",
  );
  assert.equal(
    line({ resolvedBy: "SERVERS", dnsServers: ["192.0.2.53", "192.0.2.54"], addresses: ["192.0.2.1"] }),
    "news.example.com → 192.0.2.1 · DNS 192.0.2.53, 192.0.2.54 via WAN",
  );
  assert.equal(
    line({ resolvedBy: "PROXY_SERVERS", proxyId: 4, dnsServers: ["192.0.2.53"], addresses: ["192.0.2.1"] }),
    "news.example.com → 192.0.2.1 · DNS 192.0.2.53 via proxy 4",
  );
  assert.equal(line({ resolvedBy: "PROXY", proxyId: 2 }), "news.example.com · resolved by proxy 2");
  assert.equal(
    line({ host: "192.0.2.9", resolvedBy: "LITERAL", addresses: ["192.0.2.9"], connectedAddress: "192.0.2.9" }),
    "192.0.2.9 · address",
  );
});

test("the connected address is marked only among several", () => {
  const parts = (connectedAddress: string, addresses: string[]) =>
    resolutionParts(english, resolution({ addresses, connectedAddress }), names).addresses;
  assert.deepEqual(parts("192.0.2.1", ["192.0.2.1"]), [{ address: "192.0.2.1", connected: false }]);
  assert.deepEqual(parts("192.0.2.2", ["192.0.2.1", "192.0.2.2"]), [
    { address: "192.0.2.1", connected: false },
    { address: "192.0.2.2", connected: true },
  ]);
});

test("a failed lookup reads as its code, keeping the resolver's words as the detail", () => {
  const failed = resolutionParts(
    english,
    resolution({ resolvedBy: "SERVERS", dnsServers: ["192.0.2.53"], error: { code: "NO_SUCH_NAME", detail: "192.0.2.53: NXDOMAIN" } }),
    names,
  );
  assert.equal(resolutionText(failed), "news.example.com · DNS 192.0.2.53 via WAN: no such name");
  assert.equal(failed.detail, "192.0.2.53: NXDOMAIN");
  // A code this page does not know reads as the resolver's words.
  assert.equal(
    line({ error: { code: "SOMETHING_NEW", detail: "the resolver said no" } }),
    "news.example.com · host DNS: the resolver said no",
  );
});

test("every lookup error code the daemon sends has words here", () => {
  const rust = readFileSync(new URL("../../../engines/weaver-tunnel/src/resolution_record.rs", import.meta.url), "utf8");
  const start = rust.indexOf("pub fn code(self)");
  assert.ok(start >= 0, "the lookup error codes were not found where they are defined");
  const codes = [...rust.slice(start, rust.indexOf("\n    }\n", start)).matchAll(/=> "([A-Z][A-Z0-9_]*)"/g)].map((match) => match[1]!).sort();
  assert.ok(codes.includes("NO_SUCH_NAME"), "the lookup error codes were not found where they are defined");
  const prefix = lookupErrorKey("");
  const worded = Object.keys(nextEn).filter((key) => key.startsWith(prefix)).map((key) => key.slice(prefix.length)).sort();
  assert.deepEqual(worded, codes);
});

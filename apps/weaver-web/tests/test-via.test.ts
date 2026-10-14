import assert from "node:assert/strict";
import { test } from "node:test";
import type { ProxyPool } from "../src/lib/networking.ts";
import { draftRoute, routeChoices, testSummary, type PathTestResult } from "../src/lib/test-via.ts";
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

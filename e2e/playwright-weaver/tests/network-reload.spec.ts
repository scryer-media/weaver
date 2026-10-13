import fs from "node:fs";
import path from "node:path";
import { expect, graphql, nntpConnectionMetrics, test } from "./helpers";
import { datastoreKind, execute, literal, query } from "./support/datastore";
import {
  type NetworkFlow, consumerOf, createPool, deleteEgress, deletePool, deleteProxyProfile, directLeg, egressInterfaces,
  flowAfter, flowMark, graphqlErrors, interfaceEgress, ladderLeg, legOn, legsOf, networkRoutes, rssKey, rung,
  saveProxyProfile, saveRoute, serverKey, stage,
} from "./support/network-flow";
import { NetworkWorld, PROXIED_HOST } from "./support/network-scenario";

/** Reload, persistence, consumers. */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const carrying = (flow: NetworkFlow, server: number, egresses: number[]) =>
  egresses.every(id => { const leg = legOn(flow, serverKey(server), id); return leg?.target === 2 && leg.open === 2; });

test("R01 saving an unrelated profile leaves running legs and their connections alone", async ({ request }) => {
  test.setTimeout(0);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const server = await world.server({ route: { legs: [directLeg(a.id, 50), directLeg(b.id, 50)] } });
  const download = await world.pacedDownload("r01-unrelated");
  // The baseline waits for both legs to have pinned an address with nothing
  // left opening, so the dials that settle each leg's address race are counted
  // before it rather than after the save.
  await flowAfter(request, await flowMark(request), flow => carrying(flow, server, [a.id, b.id]) && [a.id, b.id].every(id => {
    const leg = legOn(flow, serverKey(server), id);
    return !!leg?.pinnedAddress && leg.opening === 0;
  }), "legs 2/2 open on a pinned address");
  const before = (await nntpConnectionMetrics()).accepted;
  const saved = await flowMark(request);
  await world.profile({ name: `r01-unrelated-${Date.now()}`, kind: "HTTP_CONNECT", enabled: true, host: PROXIED_HOST, port: 8101, username: "fixture", password: "fixture" });
  const after = await flowAfter(request, saved, () => true, "a sample after the save");
  expect(carrying(after, server, [a.id, b.id])).toBe(true);
  // The only new session is this second metrics probe.
  expect((await nntpConnectionMetrics()).accepted - before).toBe(1);
  expect(await download.release()).toBe("COMPLETED");
});

test("R02 removing a server and a feed drops their consumers and routes", async ({ request }) => {
  const a = await world.egress("a");
  const route = { legs: [directLeg(a.id, 100)] };
  const server = await world.server({ route });
  const feed = await world.feed({ url: "http://feed.proxy.test:8089/redirect", route });
  const keys = [serverKey(server), rssKey(feed)];
  expect((await networkRoutes(request)).map(entry => entry.consumer)).toEqual(expect.arrayContaining(keys));
  await graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id: server });
  world.servers.splice(world.servers.indexOf(server), 1);
  await graphql(request, "mutation($id: Int!) { deleteRssFeed(id: $id) }", { id: feed });
  world.feeds.splice(world.feeds.indexOf(feed), 1);
  const flow = await flowAfter(request, await flowMark(request), sample => keys.every(key => consumerOf(sample, key) === undefined), "consumers gone from the flow");
  expect(keys.flatMap(key => legsOf(flow, key))).toEqual([]);
  expect((await networkRoutes(request)).map(entry => entry.consumer).filter(consumer => keys.includes(consumer))).toEqual([]);
});

test("R03 routes for missing consumers and double route inputs are rejected", async ({ request }) => {
  const missing = await graphqlErrors(request,
    "mutation($input: RouteInput!) { saveNetworkRoute(kind: SERVER, id: 999999, input: $input) { consumer } }",
    { input: { legs: [directLeg(0, 100)] } });
  expect(missing.join("\n")).toContain("routing consumer no longer exists");
  const both = await graphqlErrors(request, "mutation($input: ServerInput!) { addServer(input: $input) { id } }", {
    input: {
      host: "nntp", port: 119, tls: false, username: "e2e-user", password: "e2e-pass", connections: 1, active: false,
      priority: 0, backfill: false, retentionDays: 0, routing: { proxyIds: [], allowDirect: true }, route: { legs: [directLeg(0, 100)] },
    },
  });
  expect(both.join("\n")).toContain("specify route or routing, not both");
});

test("R04 the legacy routing policy still saves as a one-leg route", async ({ request }) => {
  // proxy-routing.spec.ts carries the full legacy behaviour and stays in the gate.
  expect(fs.existsSync(path.join(__dirname, "proxy-routing.spec.ts"))).toBe(true);
  const connect1 = await world.connect("connect1");
  const id = (await graphql<{ addServer: { id: number } }>(request, "mutation($input: ServerInput!) { addServer(input: $input) { id } }", {
    input: {
      host: PROXIED_HOST, port: 119, tls: false, username: "e2e-user", password: "e2e-pass", connections: 1, active: false,
      priority: 0, backfill: false, retentionDays: 0, routing: { proxyIds: [connect1.id], allowDirect: true },
    },
  })).addServer.id;
  world.track("server", id);
  const route = (await networkRoutes(request)).find(entry => entry.consumer === serverKey(id))!;
  expect(route.legs).toHaveLength(1);
  expect(route.legs[0]!.path).toMatchObject({ kind: "LADDER", directFallback: true });
  expect(route.legs[0]!.path.rungs.map(entry => entry.proxyId)).toEqual([connect1.id]);
});

type R05Saved = { egresses: number[]; doomed: number; profiles: number[]; pool: number; server: number; route: unknown };
const r05File = () => path.join(process.env.PLAYWRIGHT_ARTIFACTS_DIR || "artifacts", `r05-${datastoreKind()}.json`);

test("R05 routes, pools and ladders survive a restart; a vanished egress is reported at boot @restart", async ({ request }) => {
  test.setTimeout(0);
  if (stage() === "initial") {
    // Built outside the per-test world: this configuration must outlive the test.
    const a = await interfaceEgress(request, `r05-a-${Date.now()}`, "a");
    const b = await interfaceEgress(request, `r05-b-${Date.now()}`, "b");
    const doomed = await interfaceEgress(request, `r05-doomed-${Date.now()}`, "b");
    const profiles = [];
    for (const port of [8101, 8102, 8103]) {
      profiles.push((await saveProxyProfile(request, { name: `r05-connect-${port}-${Date.now()}`, kind: "HTTP_CONNECT", enabled: true, host: PROXIED_HOST, port, username: "fixture", password: "fixture", dnsServers: [] })).id);
    }
    const pool = (await createPool(request, { name: `r05-pool-${Date.now()}`, kind: "HTTP_CONNECT", memberIds: profiles.slice(0, 2) })).id;
    const server = (await graphql<{ addServer: { id: number } }>(request, "mutation($input: ServerInput!) { addServer(input: $input) { id } }", {
      input: { host: PROXIED_HOST, port: 119, tls: false, username: "e2e-user", password: "e2e-pass", connections: 3, active: false, priority: 0, backfill: false, retentionDays: 0 },
    })).addServer.id;
    const saved = await saveRoute(request, "SERVER", server, {
      legs: [ladderLeg(a.id, [rung.pool(pool), rung.proxy(profiles[2]!)], 40, true), directLeg(b.id, 30), directLeg(doomed.id, 30)],
      failover: "HOLD",
    });
    const record: R05Saved = { egresses: [a.id, b.id], doomed: doomed.id, profiles, pool, server, route: { legs: saved.legs, failover: saved.failover } };
    fs.mkdirSync(path.dirname(r05File()), { recursive: true });
    fs.writeFileSync(r05File(), JSON.stringify(record));
    // Remove the egress row behind Weaver's back; only the next boot notices.
    await execute(`DELETE FROM egress_interfaces WHERE id = ${literal(doomed.id)}`);
    expect(await query(`SELECT id FROM egress_interfaces WHERE id = ${literal(doomed.id)}`)).toEqual([]);
    return;
  }
  const record = JSON.parse(fs.readFileSync(r05File(), "utf8")) as R05Saved;
  try {
    const restored = (await networkRoutes(request)).find(entry => entry.consumer === serverKey(record.server));
    expect(restored && { legs: restored.legs, failover: restored.failover }).toEqual(record.route);
    const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(record.server), record.doomed) !== undefined, "restored legs in the flow");
    expect(legOn(flow, serverKey(record.server), record.doomed)?.reason)
      .toBe(`Restored egress ${record.doomed} is missing; this leg is Down until its egress is repaired.`);
    expect(legOn(flow, serverKey(record.server), record.doomed)?.state).toBe("DOWN");
    expect(flow.proxyPools.find(pool => pool.id === record.pool)?.memberIds).toEqual(record.profiles.slice(0, 2));
  } finally {
    await graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id: record.server });
    await deletePool(request, record.pool);
    for (const id of record.profiles) await deleteProxyProfile(request, id);
    for (const id of record.egresses) await deleteEgress(request, id);
  }
});

test("R06 the phase's datastore is the one Weaver persists networking to @restart", async ({ request }) => {
  // The datastore matrix runs every network spec (E01, P01, F01, R05 included)
  // once on SQLite and once on Postgres; this pins which store the phase used.
  const egress = await world.egress("a");
  const rows = await query(`SELECT name, binding_kind FROM egress_interfaces WHERE id = ${literal(egress.id)}`);
  expect(rows, `egress ${egress.id} in ${datastoreKind()}`).toEqual([{ name: egress.name, binding_kind: "interface" }]);
  expect((await egressInterfaces(request)).map(entry => entry.id)).toContain(egress.id);
});

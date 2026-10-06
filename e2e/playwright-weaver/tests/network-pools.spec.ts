import { expect, test } from "./helpers";
import { waitTerminal } from "./support/downloads";
import {
  type NetworkFlow, type PoolFlow, type ProxyProfile, extended, flowAfter, flowMark, graphqlErrors, ladderLeg, legOn,
  platformNetworking, poolOn, proxyProfiles, rung, serverKey, testPool, updatePool,
} from "./support/network-flow";
import { NetworkWorld, PROXIED_HOST, saveEvidence, serverHealth } from "./support/network-scenario";
import { fixtureEvents, fixtureMark, waitFixtureEvents } from "./support/proxy-fixture";
import { addBandwidth, addLatency, addToxic, removeToxic } from "./support/toxiproxy";
import { controlTunnel } from "./support/tunnel-fixture";

/** Pools: fastest preference and re-pin. */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const CREATE_POOL = `mutation($input: ProxyPoolInput!) { createProxyPool(input: $input) { id } }`;
const PINNED_STATES = ["LEADER", "PINNED"];
const FAILED_STATES = ["FAILING", "SUSPECT"];

const member = (pool: PoolFlow | undefined, id: number) => pool?.members.find(candidate => candidate.id === id);

/** P01's setup: CONNECT pool {connect1, connect2, connect3} at 0 / 150 / 300 ms. */
async function latencyPool(request: Parameters<typeof flowMark>[0], options: { connections?: number } = {}) {
  const a = await world.egress("a");
  const members = [await world.connect("connect1"), await world.connect("connect2"), await world.connect("connect3")];
  await addLatency(request, "connect2", 150);
  await addLatency(request, "connect3", 300);
  const pool = await world.pool("HTTP_CONNECT", members.map(profile => profile.id));
  const server = await world.server({ host: PROXIED_HOST, connections: options.connections ?? 4, route: { legs: [ladderLeg(a.id, [rung.pool(pool)], 100)] } });
  const view = (flow: NetworkFlow) => poolOn(flow, pool, a.id);
  const [connect1, connect2, connect3] = members as [ProxyProfile, ProxyProfile, ProxyProfile];
  return { a, pool, server, view, connect1, connect2, connect3 };
}

async function pinned(request: Parameters<typeof flowMark>[0], view: (flow: NetworkFlow) => PoolFlow | undefined, id: number, describe: string, after?: number) {
  return flowAfter(request, after ?? await flowMark(request), flow => view(flow)?.pinnedMember === id, describe);
}

test("P01 a pool pins its fastest member and prewarms the rest", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  const { view, connect1, connect2, connect3 } = await latencyPool(request);
  const download = await world.pacedDownload("p01-fastest", { parts: 64, slowMs: 500 });
  const flow = await flowAfter(request, await flowMark(request), sample => {
    const pool = view(sample);
    return pool?.pinnedMember === connect1.id && [connect1, connect2, connect3].every(profile => member(pool, profile.id)?.warmed && member(pool, profile.id)?.connectMs !== null);
  }, "connect1 pinned, every member warmed and measured");
  const pool = view(flow)!;
  await saveEvidence(info, "P01", pool);
  const ms = [connect1, connect2, connect3].map(profile => member(pool, profile.id)!.connectMs!);
  expect(ms[0]!).toBeLessThan(ms[1]!);
  expect(ms[1]!).toBeLessThan(ms[2]!);
  expect(PINNED_STATES).toContain(member(pool, connect1.id)!.state);
  for (const other of [connect2, connect3]) expect([...FAILED_STATES, "BLOCKED"]).not.toContain(member(pool, other.id)!.state);
  expect(await download.release()).toBe("COMPLETED");
});

test("P02 a throttled pin loses to a faster member once that member has delivery evidence", async ({ request }, info) => {
  test.setTimeout(30 * 60_000);
  const { view, connect1, connect2 } = await latencyPool(request);
  await world.holdChaosSession();
  const first = await world.pacedDownload("p02-warm", { parts: 32, slowMs: 200 });
  await pinned(request, view, connect1.id, "connect1 pinned");
  expect(await first.release()).toBe("COMPLETED");
  // 200 MiB with connection churn, so the pool keeps re-dialling and racing.
  await world.setChaos("drop_conn=30");
  const mark = await fixtureMark(request);
  const flowStart = await flowMark(request);
  const download = await world.download("p02-throttled", { parts: 3200, partBytes: 64 * 1024 });
  await addBandwidth(request, "connect1", 64);
  const switched = await pinned(request, view, connect2.id, "connect2 pinned", flowStart);
  expect(member(view(switched), connect2.id)!.samples).toBeGreaterThanOrEqual(16);
  const events = await waitFixtureEvents(request, mark, list => list.some(event => event.kind === "nntp-bytes" && event.route === "connect2"), "article bytes through connect2");
  await saveEvidence(info, "P02", { pool: view(switched), bytes: events.filter(event => event.kind === "nntp-bytes").map(event => event.route) });
  await world.setChaos("off");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
});

test("P03 a black-holed pin fails over to the next fastest member", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { view, connect1, connect2 } = await latencyPool(request);
  const download = await world.pacedDownload("p03-suspect");
  await pinned(request, view, connect1.id, "connect1 pinned");
  const flowStart = await flowMark(request);
  await addToxic(request, "connect1", { name: "p03-blackhole", type: "timeout", attributes: { timeout: 0 } });
  const moved = await flowAfter(request, flowStart, flow => {
    const pool = view(flow);
    return pool?.pinnedMember !== connect1.id && (member(pool, connect1.id)?.failures ?? 0) >= 2;
  }, "pin moved off connect1 after two failures");
  expect(view(moved)!.pinnedMember).toBe(connect2.id);
  expect(FAILED_STATES).toContain(member(view(moved), connect1.id)!.state);
  await removeToxic(request, "connect1", "p03-blackhole");
  expect(await download.release()).toBe("COMPLETED");
});

test("P04 @extended the interval race re-pins the fastest member once it recovers", async ({ request }) => {
  test.skip(!extended(), "extended lane only");
  test.setTimeout(45 * 60_000);
  const { view, connect1 } = await latencyPool(request);
  await world.holdChaosSession();
  const warm = await world.pacedDownload("p04-warm", { parts: 32, slowMs: 200 });
  await pinned(request, view, connect1.id, "connect1 pinned");
  await addToxic(request, "connect1", { name: "p04-blackhole", type: "timeout", attributes: { timeout: 0 } });
  await flowAfter(request, await flowMark(request), flow => view(flow)?.pinnedMember !== connect1.id, "pin moved off connect1");
  expect(await warm.release()).toBe("COMPLETED");
  await removeToxic(request, "connect1", "p04-blackhole");
  // Keep traffic flowing until the ten-minute interval race runs and re-pins connect1.
  for (let round = 0; ; round += 1) {
    const job = await world.download(`p04-round-${round}`, { parts: 64 });
    expect(await waitTerminal(request, job.jobId)).toBe("COMPLETED");
    const flow = await flowAfter(request, await flowMark(request), () => true, "a sample after the round");
    if (view(flow)?.pinnedMember === connect1.id) break;
  }
});

test("P05 a race runs once a server over-limit clears and the pin holds", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { view, connect1 } = await latencyPool(request);
  await world.holdChaosSession();
  const download = await world.pacedDownload("p05-over-limit");
  const before = await pinned(request, view, connect1.id, "connect1 pinned");
  // The held control session takes one slot of max_conns=2, leaving Weaver one.
  const healthBefore = (await serverHealth(request, PROXIED_HOST))[0]?.failureCount ?? 0;
  await world.setChaos("slow_body=1500,max_conns=2");
  await expect.poll(async () => (await serverHealth(request, PROXIED_HOST))[0]?.failureCount ?? 0, { timeout: 0, message: "over-limit refusals recorded" }).toBeGreaterThan(healthBefore);
  const atOff = await flowAfter(request, await flowMark(request), () => true, "a sample at the over-limit");
  await world.setChaos("slow_body=1500");
  const raced = await flowAfter(request, atOff.sampledAt, flow => view(flow)!.members.some(candidate => {
    const earlier = member(view(atOff), candidate.id);
    return candidate.connectMs !== earlier?.connectMs || candidate.samples > (earlier?.samples ?? 0);
  }), "member evidence refreshed after the limit cleared");
  expect(view(raced)!.pinnedMember).toBe(view(before)!.pinnedMember);
  expect(await download.release()).toBe("COMPLETED");
});

test("P06 an SSH member with a changed host key is blocked and the pool pins another", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const ssh1 = await world.ssh("ssh1");
  const mismatch = await world.ssh("ssh-switch");
  const ssh3 = await world.ssh("ssh3");
  const pool = await world.pool("SSH", [ssh1.id, mismatch.id, ssh3.id]);
  const server = await world.server({ host: "nntp", route: { legs: [ladderLeg(a.id, [rung.pool(pool)], 100)] } });
  const view = (flow: NetworkFlow) => poolOn(flow, pool, a.id);
  const first = await world.download("p06-pin");
  expect(await waitTerminal(request, first.jobId)).toBe("COMPLETED");
  // Prewarming connects every member, which pins ssh-switch's primary key.
  await expect.poll(async () => (await proxyProfiles(request)).find(profile => profile.id === mismatch.id)?.hostKeyFingerprint ?? null,
    { timeout: 0, message: "ssh-switch pinned on first use" }).not.toBeNull();
  await controlTunnel(request, { endpoint: "ssh-switch", hostKey: "other", cut: true });
  const mark = await flowMark(request);
  const download = await world.download("p06-mismatch");
  const flow = await flowAfter(request, mark, sample => member(view(sample), mismatch.id)?.state === "BLOCKED", "ssh-switch Blocked");
  expect(member(view(flow), mismatch.id)!.blocked).toContain("host key changed");
  expect([ssh1.id, ssh3.id]).toContain(view(flow)!.pinnedMember);
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  expect(legOn(flow, serverKey(server), a.id)?.state).not.toBe("BLOCKED");
});

test("P07 @extended idle non-pinned members cool while the pin stays warm", async ({ request }) => {
  test.skip(!extended(), "extended lane only");
  test.setTimeout(30 * 60_000);
  const { view, connect1, connect2, connect3 } = await latencyPool(request);
  const download = await world.download("p07-idle");
  await pinned(request, view, connect1.id, "connect1 pinned");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const flow = await flowAfter(request, await flowMark(request), sample => [connect2, connect3].every(profile => member(view(sample), profile.id)?.warmed === false), "non-pinned members cooled");
  expect(member(view(flow), connect1.id)!.warmed).toBe(true);
});

test("P08 a pool test reports every member, and needs a target for CONNECT", async ({ request }) => {
  const { a, pool, connect1, connect2, connect3 } = await latencyPool(request);
  const results = await testPool(request, pool, a.id, PROXIED_HOST, 119);
  expect(results.map(result => result.proxyId).sort()).toEqual([connect1.id, connect2.id, connect3.id].sort());
  expect(results.every(result => result.success), JSON.stringify(results)).toBe(true);
  const errors = await graphqlErrors(request, "mutation($id: Int!, $egressId: Int!) { testProxyPool(id: $id, egressId: $egressId) { success } }", { id: pool, egressId: a.id });
  expect(errors.join("\n")).toContain("host and port are required for a SOCKS5 or HTTP CONNECT pool test");
});

test("P09 pool shape rules reject undersized, oversized, duplicated and mixed pools", async ({ request }) => {
  const connects = [];
  for (const name of ["connect1", "connect2", "connect3", "connect4", "connect5", "connect6"]) connects.push(await world.connect(name));
  const socks1 = await world.socks("socks1");
  const many = [];
  for (let index = 0; index < 17; index += 1) many.push((await world.connect(`connect${(index % 6) + 1}`)).id);
  const cases: Array<[string, number[], "HTTP_CONNECT", string]> = [
    ["one member", [connects[0]!.id], "HTTP_CONNECT", "a pool requires between two and sixteen members"],
    ["seventeen members", many, "HTTP_CONNECT", "a pool requires between two and sixteen members"],
    ["a duplicated member", [connects[0]!.id, connects[0]!.id], "HTTP_CONNECT", "a proxy cannot appear twice in a pool"],
    ["mixed kinds", [connects[0]!.id, socks1.id], "HTTP_CONNECT", "all pool members must have the pool's proxy kind"],
  ];
  for (const [label, memberIds, kind, message] of cases) {
    expect((await graphqlErrors(request, CREATE_POOL, { input: { name: `p09-${Date.now()}`, kind, memberIds } })).join("\n"), label).toContain(message);
  }
});

test("P10 an RSS fetch through a pool resolves and fetches on one member", async ({ request }) => {
  const { a, pool } = await latencyPool(request);
  const url = await world.armFeed(`p10-${Date.now()}`);
  const feed = await world.feed({ url, route: { legs: [ladderLeg(a.id, [rung.pool(pool)], 100)] } });
  const mark = await fixtureMark(request);
  expect((await world.sync(feed)).errors).toEqual([]);
  const events = await fixtureEvents(request, mark);
  expect(events.some(event => event.kind === "routed-dns")).toBe(true);
  expect(events.some(event => event.kind === "routed-http")).toBe(true);
  // DNS and HTTP leave through the same leased member.
  const routes = new Set(events.filter(event => event.kind === "connected").map(event => event.route));
  expect([...routes]).toHaveLength(1);
});

test("P11 changing a pool's members prewarms the newcomer and drops the leaver", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { pool, view, connect1, connect2, connect3 } = await latencyPool(request);
  const connect4 = await world.connect("connect4");
  const download = await world.pacedDownload("p11-membership");
  await pinned(request, view, connect1.id, "connect1 pinned");
  const mark = await flowMark(request);
  await updatePool(request, pool, { name: `pool-p11-${Date.now()}`, kind: "HTTP_CONNECT", memberIds: [connect2.id, connect3.id, connect4.id] });
  const flow = await flowAfter(request, mark, sample => {
    const current = view(sample);
    return !!current && member(current, connect1.id) === undefined && member(current, connect4.id)?.warmed === true;
  }, "connect1 gone, connect4 warmed");
  expect([connect2.id, connect3.id, connect4.id]).toContain(view(flow)!.pinnedMember);
  expect(await download.release()).toBe("COMPLETED");
});

test("P12 a slower handshake on the pin raises its connect time while delivery holds", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { view, connect1 } = await latencyPool(request);
  const download = await world.pacedDownload("p12-slow-handshake");
  const before = await pinned(request, view, connect1.id, "connect1 pinned");
  const baseline = member(view(before), connect1.id)!.connectMs!;
  await addLatency(request, "connect1", 500);
  // Churn makes Weaver dial connect1 again so its connect time is re-measured.
  await world.setChaos("slow_body=1500,drop_conn=20");
  const slower = await flowAfter(request, before.sampledAt, flow => (member(view(flow), connect1.id)?.connectMs ?? 0) > baseline, "connect1 connect time rose");
  // Until a race runs (P03, P04) the pin holds and delivery continues.
  expect(view(slower)!.pinnedMember).toBe(connect1.id);
  expect(await download.release()).toBe("COMPLETED");
});

test("P13 SOCKS5 and WireGuard pools deliver within the WireGuard budget", async ({ request }, info) => {
  test.setTimeout(15 * 60_000);
  const a = await world.egress("a");
  const socks = [];
  for (const name of ["socks1", "socks2", "socks3", "socks4"]) socks.push(await world.socks(name));
  const socksPool = await world.pool("SOCKS5", socks.map(profile => profile.id));
  const socksServer = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.pool(socksPool)], 100)] } });
  const mark = await fixtureMark(request);
  const socksJob = await world.download("p13-socks");
  const socksFlow = await flowAfter(request, await flowMark(request), flow => poolOn(flow, socksPool, a.id)?.pinnedMember !== null && poolOn(flow, socksPool, a.id)?.pinnedMember !== undefined, "SOCKS pool pinned");
  expect(await waitTerminal(request, socksJob.jobId)).toBe("COMPLETED");
  const pinnedProfile = socks.find(profile => profile.id === poolOn(socksFlow, socksPool, a.id)!.pinnedMember)!;
  const pinnedRoute = pinnedProfile.name.split("-")[0]!;
  const events = await fixtureEvents(request, mark);
  expect(events.some(event => event.kind === "connected" && event.route === pinnedRoute)).toBe(true);
  await world.updateServer(socksServer, { active: false });

  // WireGuard: the instance budget follows the memory Weaver can see, so the
  // pool takes as many members as fit (two where the overlay's limit holds)
  // and a route that would need one instance more is refused at save.
  const budget = (await platformNetworking(request)).maxWireguardInstances;
  expect(budget).toBeGreaterThanOrEqual(1);
  const wg1 = await world.wireguard("wg1");
  const wg2 = budget >= 2 ? await world.wireguard("wg2") : null;
  const wgMembers = wg2 ? [wg1, wg2] : [wg1];
  const wgPool = await world.pool("WIRE_GUARD", wgMembers.map(profile => profile.id));
  const wgServer = await world.server({ host: "nntp.proxy.test", route: { legs: [ladderLeg(a.id, [rung.pool(wgPool)], 100)] } });
  const wgJob = await world.download("p13-wireguard");
  const wgFlow = await flowAfter(request, await flowMark(request), flow => {
    const pool = poolOn(flow, wgPool, a.id);
    return !!pool && wgMembers.every(profile => member(pool, profile.id)?.connectMs !== null && member(pool, profile.id)?.connectMs !== undefined);
  }, "every WireGuard member measured");
  expect(await waitTerminal(request, wgJob.jobId)).toBe("COMPLETED");
  expect(legOn(wgFlow, serverKey(wgServer), a.id)?.state).not.toBe("DOWN");
  // Enough further profiles (each its own instance) to need budget + 1 in all.
  const endpoints = ["wg-rss", "wg1", "wg2"] as const;
  const extra: ProxyProfile[] = [];
  for (let index = 0; extra.length < budget + 1 - wgMembers.length; index += 1) extra.push(await world.wireguard(endpoints[index % endpoints.length]!));
  const errors = await graphqlErrors(request,
    "mutation($kind: NetworkConsumerKind!, $id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: $kind, id: $id, input: $input) { consumer } }",
    { kind: "SERVER", id: socksServer, input: { legs: [ladderLeg(a.id, extra.map(profile => rung.proxy(profile.id)), 100)] } });
  await saveEvidence(info, "P13", { budget, members: wgMembers.length, extra: extra.length, wireguardPool: poolOn(wgFlow, wgPool, a.id), errors });
  expect(errors.join("\n")).toMatch(/WireGuard instances/);
  test.info().annotations.push({ type: "deviation", description: "P13: an over-budget WireGuard route is refused when it is saved rather than skipped at dial time, so the over-budget case is asserted as a save rejection. HTTP/3 pool members are not built: no HTTP/3 fixture endpoint presents a certificate Weaver trusts (see S08)." });
});

test("P14 a disabled pool's rung is skipped and the next rung carries the leg", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const members = [await world.connect("connect1"), await world.connect("connect2")];
  const pool = await world.pool("HTTP_CONNECT", members.map(profile => profile.id));
  await updatePool(request, pool, { name: `pool-p14-${Date.now()}`, kind: "HTTP_CONNECT", memberIds: members.map(profile => profile.id), enabled: false });
  const connect3 = await world.connect("connect3");
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.pool(pool), rung.proxy(connect3.id)], 100)] } });
  const mark = await fixtureMark(request);
  const download = await world.download("p14-disabled-pool");
  const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(server), a.id)?.selectedRung === 1, "leg on rung 1");
  expect(legOn(flow, serverKey(server), a.id)?.selectedProxyId).toBe(connect3.id);
  expect(legOn(flow, serverKey(server), a.id)?.rungStates[0]?.trim()).toBeTruthy();
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const events = await fixtureEvents(request, mark);
  expect(events.filter(event => event.kind === "connected").every(event => event.route === "connect3")).toBe(true);
});

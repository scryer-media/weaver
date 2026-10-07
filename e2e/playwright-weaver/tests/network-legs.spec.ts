import { expect, graphql, resetNntpMetrics, test } from "./helpers";
import { ifaceFor, packetCount, startCapture, stopCapture } from "./support/capture";
import { waitTerminal } from "./support/downloads";
import {
  FlowSubscription, type NetworkFlow, consumerOf, directLeg, egressAddress, flowAfter, flowMark,
  graphqlErrors, ladderLeg, legOn, legsOf, openOn, proxyProfiles, resetProxyHostKey, rssKey, rung, saveProxyProfile, serverKey,
} from "./support/network-flow";
import { DIRECT_HOST, NetworkWorld, PROXIED_HOST, expectNoFailureEvents, saveEvidence, serverHealth } from "./support/network-scenario";
import { attemptsOn, controlRoute, fixtureEvents, fixtureMark, waitFixtureEvents } from "./support/proxy-fixture";
import { addToxic, removeToxic } from "./support/toxiproxy";
import { controlTunnel, tunnelState } from "./support/tunnel-fixture";

/** Legs, weights and failover. */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const SAVE_ROUTE = `mutation($id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: SERVER, id: $id, input: $input) { consumer } }`;

/** Both legs open at their targets, with the targets given. */
function carrying(flow: NetworkFlow, server: number, expected: Array<[number, number]>): boolean {
  return expected.every(([egressId, target]) => {
    const leg = legOn(flow, serverKey(server), egressId);
    return leg !== undefined && leg.target === target && leg.open === target;
  });
}

async function twoLegServer(host = DIRECT_HOST, weights: [number, number] = [50, 50], connections = 4) {
  const a = await world.egress("a");
  const b = await world.egress("b");
  const server = await world.server({ host, connections, route: { legs: [directLeg(a.id, weights[0]), directLeg(b.id, weights[1])] } });
  return { a, b, server };
}

test("L01 an even split opens two connections on each egress from its own address", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer();
  await world.holdChaosSession();
  await resetNntpMetrics();
  await startCapture(request, "l01");
  const download = await world.pacedDownload("l01-split");
  const flow = await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  expect(legOn(flow, serverKey(server), a.id)?.sourceAddress?.replace(/:\d+$/, "")).toBe(egressAddress("a"));
  expect(legOn(flow, serverKey(server), b.id)?.sourceAddress?.replace(/:\d+$/, "")).toBe(egressAddress("b"));
  expect(await download.release()).toBe("COMPLETED");
  await stopCapture(request);
  const counts = {
    a: await packetCount(request, "l01", await ifaceFor(request, egressAddress("a")), "tcp and dst port 119"),
    b: await packetCount(request, "l01", await ifaceFor(request, egressAddress("b")), "tcp and dst port 119"),
  };
  await saveEvidence(info, "L01", { flow, counts });
  expect(counts.a).toBeGreaterThan(0);
  expect(counts.b).toBeGreaterThan(0);
});

test("L02 largest-remainder targets for 75/25 and 70/30", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer(DIRECT_HOST, [75, 25]);
  const download = await world.pacedDownload("l02-weights");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 3], [b.id, 1]]), "75/25 gives 3/1");
  await world.route("SERVER", server, { legs: [directLeg(a.id, 70), directLeg(b.id, 30)] });
  const reloaded = await flowMark(request);
  const flow = await flowAfter(request, reloaded, sample => legsOf(sample, serverKey(server)).map(leg => leg.weight).join("/") === "70/30", "route reloaded as 70/30");
  expect(legsOf(flow, serverKey(server)).map(leg => leg.target)).toEqual([3, 1]);
  expect(await download.release()).toBe("COMPLETED");
});

test("L03 route shape rules reject every malformed route", async ({ request }) => {
  const server = await world.server();
  const p = (id: number) => rung.proxy(id);
  // Shape is validated before references, so the proxy ids need not exist.
  const cases: Array<[string, unknown, string]> = [
    ["weights that do not sum to 100", { legs: [directLeg(0, 60), directLeg(0, 50)] }, "leg weights must sum to exactly 100"],
    ["nine legs", { legs: Array.from({ length: 9 }, (_, index) => directLeg(0, index === 0 ? 92 : 1)) }, "a route requires between one and eight legs"],
    ["a zero weight", { legs: [directLeg(0, 0), directLeg(0, 100)] }, "leg weights must be between 1 and 100"],
    ["ten rungs", { legs: [ladderLeg(0, Array.from({ length: 10 }, (_, index) => p(index + 1)), 100)] }, "a ladder requires between one and eight rungs"],
    ["a chain of one", { legs: [ladderLeg(0, [rung.chain([1])], 100)] }, "a chain requires two or three proxies"],
    ["a chain of four", { legs: [ladderLeg(0, [rung.chain([1, 2, 3, 4])], 100)] }, "a chain requires two or three proxies"],
  ];
  for (const [label, input, message] of cases) {
    const errors = await graphqlErrors(request, SAVE_ROUTE, { id: server, input });
    expect(errors.join("\n"), label).toContain(message);
  }
});

async function proxiedSecondLeg(failover: "REDISTRIBUTE" | "HOLD") {
  const a = await world.egress("a");
  const b = await world.egress("b");
  const connect1 = await world.connect("connect1", { network: "b" });
  const server = await world.server({
    host: PROXIED_HOST, connections: 4,
    route: { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(connect1.id)], 50)], failover },
  });
  return { a, b, connect1, server };
}

test("L04 Redistribute gives a dead proxied leg's share to the direct leg", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await proxiedSecondLeg("REDISTRIBUTE");
  const download = await world.pacedDownload("l04-redistribute");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const cut = await flowMark(request);
  await controlRoute(request, { route: "connect1", up: false, cut: true });
  const flow = await flowAfter(request, cut, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN" && legOn(sample, serverKey(server), a.id)?.target === 4, "leg B Down, leg A target 4");
  expect(legOn(flow, serverKey(server), b.id)).toMatchObject({ target: 0 });
  expect(legOn(flow, serverKey(server), b.id)?.reason?.trim()).toBeTruthy();
  expect(await download.release()).toBe("COMPLETED");
  await expectNoFailureEvents(request, download.jobId);
});

test("L05 Hold parks a dead leg's share instead of moving it", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await proxiedSecondLeg("HOLD");
  const download = await world.pacedDownload("l05-hold");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const cut = await flowMark(request);
  const subscription = await FlowSubscription.open();
  try {
    await controlRoute(request, { route: "connect1", up: false, cut: true });
    const flow = await flowAfter(request, cut, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN", "leg B Down");
    expect(legOn(flow, serverKey(server), a.id)?.target).toBe(2);
    expect(legOn(flow, serverKey(server), b.id)?.target).toBe(0);
    const downSince = flow.sampledAt;
    await subscription.next(downSince, () => true, "a sample after leg B went Down");
    const after = subscription.since(downSince).map(sample => openOn(sample, serverKey(server)));
    expect(after.filter(open => open > 2), "open connections while leg B is held Down").toEqual([]);
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("L06 a dead leg goes Down, probes with one connection, and returns Up once its proxy does", async ({ request }, info) => {
  test.setTimeout(15 * 60_000);
  const { a, b, server } = await proxiedSecondLeg("REDISTRIBUTE");
  // The fixture paces every body line, so 10 ms lets a 64 KiB article finish in
  // about five seconds, inside the per-article soft timeout: a recovery probe can
  // succeed while the job is still paced. 480 parts outlast the leg cooldowns
  // and the 60 s cap on server recovery backoff several times over.
  const download = await world.pacedDownload("l06-probing", { parts: 480, slowMs: 10 });
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
    await controlRoute(request, { route: "connect1", up: false, cut: true });
    const stateB = (sample: NetworkFlow) => legOn(sample, serverKey(server), b.id);
    const firstDown = await subscription.next(start, sample => stateB(sample)?.state === "DOWN", "leg B Down");
    const probing = await subscription.next(firstDown.sampledAt, sample => stateB(sample)?.state === "PROBING", "leg B probing after its cooldown");
    expect(stateB(probing)?.target).toBe(1);
    const downAgain = await subscription.next(probing.sampledAt, sample => stateB(sample)?.state === "DOWN", "failed probe puts leg B Down again");
    await controlRoute(request, { route: "connect1", up: true });
    const probingAgain = await subscription.next(downAgain.sampledAt, sample => stateB(sample)?.state === "PROBING", "leg B probing again");
    await subscription.next(probingAgain.sampledAt, sample => stateB(sample)?.state === "UP" && carrying(sample, server, [[a.id, 2], [b.id, 2]]), "leg B Up at 2/2");
    await saveEvidence(info, "L06-states", subscription.since(start).map(sample => ({ at: sample.sampledAt, b: stateB(sample)?.state, targets: legsOf(sample, serverKey(server)).map(leg => leg.target) })));
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("L07 a changed SSH host key blocks the leg until the key is reset", async ({ request }) => {
  test.setTimeout(15 * 60_000);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const ssh = await world.ssh("ssh-switch", { network: "b" });
  const server = await world.server({ host: DIRECT_HOST, route: { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(ssh.id)], 50)] } });
  // First use pins the primary key.
  const first = await world.download("l07-pin");
  expect(await waitTerminal(request, first.jobId)).toBe("COMPLETED");
  await expect.poll(async () => (await proxyProfiles(request)).find(profile => profile.id === ssh.id)?.hostKeyFingerprint ?? null, { timeout: 0, message: "the first SSH session pins the host key" })
    .not.toBeNull();
  await controlTunnel(request, { endpoint: "ssh-switch", hostKey: "other" });
  const switched = await flowMark(request);
  const download = await world.pacedDownload("l07-mismatch", { parts: 64, slowMs: 500 });
  const blocked = await flowAfter(request, switched, sample => legOn(sample, serverKey(server), b.id)?.state === "BLOCKED", "leg B Blocked on the changed key");
  expect(legOn(blocked, serverKey(server), b.id)?.reason).toContain("host key changed");
  expect(await download.release()).toBe("COMPLETED");
  const later = await flowAfter(request, await flowMark(request), () => true, "a sample after the job finished");
  expect(legOn(later, serverKey(server), b.id)?.state).toBe("BLOCKED");
  const reset = await resetProxyHostKey(request, ssh.id);
  expect(reset.hostKeyFingerprint).toBeNull();
  const client = (await tunnelState(request)).sshClient;
  await saveProxyProfile(request, { name: ssh.name, kind: "SSH", enabled: true, host: ssh.host, port: ssh.port, username: client.username, password: client.password, timeoutSeconds: 5 }, ssh.id);
  const rebuilt = await flowMark(request);
  const third = await world.download("l07-reset");
  await flowAfter(request, rebuilt, sample => legOn(sample, serverKey(server), b.id)?.state === "UP", "leg B Up after the reset");
  expect(await waitTerminal(request, third.jobId)).toBe("COMPLETED");
});

test("L08 refused connections are not path evidence: the leg never goes Down", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  // Both legs reach the NNTP server through Toxiproxy's nntp1, each from its
  // own egress, so a reset_peer toxic refuses every connection either leg opens.
  const a = await world.egress("a");
  const b = await world.egress("b");
  const server = await world.server({ host: "toxiproxy", port: 3119, connections: 4, route: { legs: [directLeg(a.id, 50), directLeg(b.id, 50)] } });
  // Paced so each article still finishes inside the per-article soft timeout:
  // the refusals quarantine the server, and its recovery probe must be able to
  // complete an article before the pacing is lifted.
  const download = await world.pacedDownload("l08-refused", { parts: 320, slowMs: 10 });
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const before = (await serverHealth(request, "toxiproxy"))[0]!;
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
    await addToxic(request, "nntp1", { name: "l08-reset", type: "reset_peer", attributes: { timeout: 0 } });
    await expect.poll(async () => (await serverHealth(request, "toxiproxy"))[0]!.failureCount - before.failureCount, { timeout: 0 }).toBeGreaterThanOrEqual(4);
    const window = subscription.since(start);
    expect(window.flatMap(sample => legsOf(sample, serverKey(server))).filter(leg => leg.state === "DOWN" || leg.state === "BLOCKED"), "legs Down on refusals").toEqual([]);
    const removedAt = await flowMark(request);
    await removeToxic(request, "nntp1", "l08-reset");
    // Both legs hold their share again; the paced download completing below
    // proves the connections carry articles, not just that they are open.
    await subscription.next(removedAt, sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open after the refusals end");
    await saveEvidence(info, "L08", { before, after: (await serverHealth(request, "toxiproxy"))[0], samples: window.length });
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("L09 a black-holed proxy times out, takes its leg Down and cools its rung", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const connect2 = await world.connect("connect2", { network: "b", timeoutSeconds: 3 });
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(connect2.id)], 50)] } });
  const download = await world.pacedDownload("l09-timeout");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const mark = await flowMark(request);
  await addToxic(request, "connect2", { name: "l09-blackhole", type: "timeout", attributes: { timeout: 0 } });
  await controlRoute(request, { route: "connect2", cut: true });
  const flow = await flowAfter(request, mark, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN" && legOn(sample, serverKey(server), b.id)?.rungStates[0] === "COOLDOWN", "leg B Down with rung 0 cooling");
  expect(legOn(flow, serverKey(server), b.id)?.reason ?? "").toMatch(/tim(ed|e) ?out/i);
  await removeToxic(request, "connect2", "l09-blackhole");
  expect(await download.release()).toBe("COMPLETED");
});

test("L10 an RSS route uses one connection on its first leg", async ({ request }) => {
  const a = await world.egress("a");
  const b = await world.egress("b");
  const connect1 = await world.connect("connect1", { network: "a" });
  const connect2 = await world.connect("connect2", { network: "b" });
  const url = await world.armFeed(`l10-${Date.now()}`);
  const feed = await world.feed({ url, route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 50), ladderLeg(b.id, [rung.proxy(connect2.id)], 50)] } });
  const mark = await fixtureMark(request);
  const flowStart = await flowMark(request);
  expect((await world.sync(feed)).errors).toEqual([]);
  const events = await fixtureEvents(request, mark);
  expect(events.some(event => event.kind === "routed-http")).toBe(true);
  expect(attemptsOn(events, "connect1").length).toBeGreaterThan(0);
  expect(attemptsOn(events, "connect2")).toEqual([]);
  const flow = await flowAfter(request, flowStart, sample => consumerOf(sample, rssKey(feed)) !== undefined, "RSS consumer in the flow");
  expect(consumerOf(flow, rssKey(feed))?.cap).toBe(1);
  expect(legsOf(flow, rssKey(feed)).map(leg => leg.target)).toEqual([1, 0]);
  // The feed's accepted item became a job no server on this route can carry; it must not outlive the test.
  for (const item of (await graphql<{ rssSeenItems: Array<{ jobId: number | null }> }>(request,
    "query($feedId: Int) { rssSeenItems(feedId: $feedId) { jobId } }", { feedId: feed })).rssSeenItems) {
    if (item.jobId !== null) await graphql(request, "mutation($id: Int!) { cancelJob(id: $id) }", { id: item.jobId });
  }
});

/**
 * A news server accepting a connection: its SYN-ACK. A reload also races each
 * leg's addresses, and a SYN to an address the leg's network cannot reach is
 * retried without ever being accepted, so SYNs alone overcount.
 */
const NNTP_ACCEPTED = "tcp src port 119 and (tcp[tcpflags] & tcp-syn) != 0 and (tcp[tcpflags] & tcp-ack) != 0";

test("L11 reweighting a running route moves connections without revoking them", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer();
  // Paced so each article finishes inside the per-article soft timeout; a body
  // that never finishes times out and its lane reconnects, which this test
  // would count as a new connection.
  const download = await world.pacedDownload("l11-reweight", { parts: 320, slowMs: 10 });
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  // Connections the news server accepted from Weaver, counted in Weaver's
  // namespace: the server's own accept counter also sees its container
  // health check and this test's probes.
  const ifaceA = await ifaceFor(request, egressAddress("a"));
  const ifaceB = await ifaceFor(request, egressAddress("b"));
  await startCapture(request, "l11");
  let flow: NetworkFlow;
  try {
    await world.route("SERVER", server, { legs: [directLeg(a.id, 25), directLeg(b.id, 75)] });
    flow = await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 1], [b.id, 3]]), "legs 1/3 open");
    // The flow counts a lane while it is still connecting, so the capture
    // runs until leg B's new connection has been accepted.
    await expect.poll(() => packetCount(request, "l11", ifaceB, NNTP_ACCEPTED), { message: "leg B's new connection accepted", timeout: 0 }).toBeGreaterThanOrEqual(1);
  } finally {
    await stopCapture(request);
  }
  const accepted = {
    a: await packetCount(request, "l11", ifaceA, NNTP_ACCEPTED),
    b: await packetCount(request, "l11", ifaceB, NNTP_ACCEPTED),
  };
  await saveEvidence(info, "L11", { flow, accepted });
  // One more connection on leg B, and none to replace a revoked one.
  expect(accepted.a + accepted.b).toBeLessThanOrEqual(1);
  expect(await download.release()).toBe("COMPLETED");
});

test("L12 changing one leg's path revokes only that leg", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer(PROXIED_HOST);
  const connect1 = await world.connect("connect1", { network: "b" });
  const download = await world.pacedDownload("l12-revoke");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
    const mark = await fixtureMark(request);
    await world.route("SERVER", server, { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(connect1.id)], 50)] });
    const events = await waitFixtureEvents(request, mark, list => list.filter(event => event.kind === "connected" && event.route === "connect1").length >= 2, "two connections through connect1");
    const settled = await subscription.next(start, sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]) && legOn(sample, serverKey(server), b.id)?.selectedProxyId === connect1.id, "leg B open through connect1");
    const legA = subscription.since(start).filter(sample => sample.sampledAt <= settled.sampledAt).map(sample => legOn(sample, serverKey(server), a.id)?.open);
    await saveEvidence(info, "L12", { legA, connected: events.filter(event => event.kind === "connected").length });
    expect(legA.filter(open => open !== 2), "leg A open count across the reload").toEqual([]);
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("L13 the flow subscription ticks with rising stamps and live rates on both legs", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer();
  const download = await world.pacedDownload("l13-subscribe", { slowMs: 200, parts: 320 });
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  const subscription = await FlowSubscription.open();
  try {
    await expect.poll(() => subscription.samples.length, { timeout: 0 }).toBeGreaterThanOrEqual(5);
    const stamps = subscription.samples.slice(0, 5).map(sample => sample.sampledAt);
    for (let index = 1; index < stamps.length; index += 1) expect(stamps[index]!).toBeGreaterThan(stamps[index - 1]!);
    await expect.poll(() => [a.id, b.id].every(egress => subscription.samples.some(sample => (legOn(sample, serverKey(server), egress)?.bytesPerSecond ?? 0) > 0)), { timeout: 0 }).toBe(true);
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("L14 raising the server's connections recomputes targets and the consumer cap", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, b, server } = await twoLegServer();
  const download = await world.pacedDownload("l14-cap");
  await flowAfter(request, await flowMark(request), sample => carrying(sample, server, [[a.id, 2], [b.id, 2]]), "legs 2/2 open");
  await world.updateServer(server, { connections: 6 });
  const flow = await flowAfter(request, await flowMark(request), sample => consumerOf(sample, serverKey(server))?.cap === 6, "consumer cap 6");
  expect(legsOf(flow, serverKey(server)).map(leg => leg.target)).toEqual([3, 3]);
  expect(await download.release()).toBe("COMPLETED");
});

import { expect, metricValue, metrics, test } from "./helpers";
import { waitTerminal } from "./support/downloads";
import {
  FlowSubscription, type LegFlow, type NetworkFlow, type RouteInput, extended, flowAfter, flowMark, graphqlErrors,
  ladderLeg, directLeg, legOn, networkHosts, platformNetworking, proxyProfiles, rung, serverKey, testEgress, testProxyProfile,
} from "./support/network-flow";
import { NetworkWorld, PROXIED_HOST, expectNoFailureEvents, jobEvents, saveEvidence, serverHealth, verifyOutput } from "./support/network-scenario";
import { type FixtureEvent, attemptsOn, controlRoute, directEvents, fixtureEvents, fixtureMark, waitFixtureEvents } from "./support/proxy-fixture";
import { setEnabled } from "./support/toxiproxy";
import { controlTunnel, tunnelEvents, tunnelMark, tunnelState } from "./support/tunnel-fixture";

/** Ladders and live hop kills. */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const SAVE_ROUTE = `mutation($kind: NetworkConsumerKind!, $id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: $kind, id: $id, input: $input) { consumer } }`;
const connectedOn = (events: FixtureEvent[], route: string) =>
  events.filter(event => event.kind === "connected" && event.route === route);

/** One leg on egress-a whose ladder is [connect1, connect2] (F01's setup). */
async function twoRungLadder(options: { directFallback?: boolean; connect1Password?: string } = {}) {
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1", { password: options.connect1Password });
  const connect2 = await world.connect("connect2");
  const server = await world.server({
    host: PROXIED_HOST, connections: 4,
    route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id), rung.proxy(connect2.id)], 100, options.directFallback ?? false)] },
  });
  const leg = (flow: NetworkFlow) => legOn(flow, serverKey(server), a.id);
  return { a, connect1, connect2, server, leg };
}

/** Wait until the leg is carrying traffic through `rungIndex`. */
async function onRung(request: Parameters<typeof flowMark>[0], leg: (flow: NetworkFlow) => LegFlow | undefined, rungIndex: number, describe: string, after?: number) {
  return flowAfter(request, after ?? await flowMark(request), flow => leg(flow)?.selectedRung === rungIndex && (leg(flow)?.open ?? 0) > 0, describe);
}

/**
 * Wait for rung 0's cooldown to lapse, then cut the rung-1 connections so the
 * leg redials: the next dial is allowed on rung 0 again and must carry the leg
 * there. A lapsed rung reads Failing only until a dial succeeds on it, and a
 * leg with traffic can redial within one flow sample, so the lapse is any
 * state but Cooldown: Failing, or Standby once rung 0 has already taken a
 * connection back.
 */
async function rungZeroReturns(request: Parameters<typeof flowMark>[0], leg: (flow: NetworkFlow) => LegFlow | undefined, after: number) {
  const lapsed = await flowAfter(request, after, flow => {
    const state = leg(flow)?.rungStates[0];
    return state !== undefined && state !== "COOLDOWN";
  }, "rung 0 cooldown lapsed");
  await controlRoute(request, { route: "connect2", cut: true });
  return onRung(request, leg, 0, "leg back on rung 0", lapsed.sampledAt);
}

/** F02's state: connect1 failed mid-download, leg moved to rung 1. */
async function failFirstRung(request: Parameters<typeof flowMark>[0], name: string) {
  const ladder = await twoRungLadder();
  const download = await world.pacedDownload(name);
  await onRung(request, ladder.leg, 0, "leg on rung 0");
  const mark = await fixtureMark(request);
  const flowStart = await flowMark(request);
  await controlRoute(request, { route: "connect1", up: false, cut: true });
  const moved = await onRung(request, ladder.leg, 1, "leg moved to rung 1", flowStart);
  return { ...ladder, download, mark, moved };
}

test("F01 a ladder uses its first rung while that rung works", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { connect1, leg } = await twoRungLadder();
  const mark = await fixtureMark(request);
  const download = await world.pacedDownload("f01-first-rung", { parts: 64, slowMs: 500 });
  const flow = await onRung(request, leg, 0, "leg on rung 0");
  expect(leg(flow)).toMatchObject({ selectedRung: 0, selectedProxyId: connect1.id, rungStates: ["STANDBY", "STANDBY"] });
  expect(await download.release()).toBe("COMPLETED");
  const events = await fixtureEvents(request, mark);
  expect(connectedOn(events, "connect1").length).toBeGreaterThan(0);
  expect(connectedOn(events, "connect2")).toEqual([]);
});

test("F02 killing rung 0 mid-download moves the leg to rung 1 and the file still completes", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  const { leg, connect2, download, mark, moved } = await failFirstRung(request, "f02-hop-kill");
  expect(leg(moved)?.rungStates[0]).toBe("COOLDOWN");
  expect(leg(moved)?.selectedProxyId).toBe(connect2.id);
  const events = await fixtureEvents(request, mark);
  await saveEvidence(info, "F02", events);
  // connect1 answered every attempt after the kill with 502 (route down: no connected event).
  expect(attemptsOn(events, "connect1").length).toBeGreaterThan(0);
  expect(connectedOn(events, "connect1")).toEqual([]);
  expect(connectedOn(events, "connect2").length).toBeGreaterThan(0);
  expect(await download.release()).toBe("COMPLETED");
  await verifyOutput(request, download.jobId, "f02-hop-kill", download.bytes);
});

test("F03 the leg returns to rung 0 once its cooldown ends and it works again", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg, download, moved } = await failFirstRung(request, "f03-return");
  await controlRoute(request, { route: "connect1", up: true });
  const returned = await rungZeroReturns(request, leg, moved.sampledAt);
  expect(leg(returned)?.rungStates[0]).toBe("STANDBY");
  expect(await download.release()).toBe("COMPLETED");
});

test("F04 with every rung down, direct fallback carries the leg", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg } = await twoRungLadder({ directFallback: true });
  const download = await world.pacedDownload("f04-direct-fallback", { parts: 64, slowMs: 500 });
  await onRung(request, leg, 0, "leg on rung 0");
  const mark = await fixtureMark(request);
  const flowStart = await flowMark(request);
  await controlRoute(request, { route: "connect1", up: false, cut: true });
  await controlRoute(request, { route: "connect2", up: false, cut: true });
  const flow = await onRung(request, leg, 2, "leg on the direct fallback rung", flowStart);
  expect(leg(flow)?.selectedProxyId).toBeNull();
  await waitFixtureEvents(request, mark, events => directEvents(events).some(event => event.kind === "direct-nntp"), "direct NNTP sessions");
  expect(await download.release()).toBe("COMPLETED");
});

test("F05 a leg whose rungs are all down goes Down and the other leg takes its share", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const connect1 = await world.connect("connect1", { network: "b" });
  const connect2 = await world.connect("connect2", { network: "b" });
  const server = await world.server({
    host: PROXIED_HOST, connections: 4,
    route: { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(connect1.id), rung.proxy(connect2.id)], 50)], failover: "REDISTRIBUTE" },
  });
  const download = await world.pacedDownload("f05-all-rungs");
  await flowAfter(request, await flowMark(request), flow => (legOn(flow, serverKey(server), b.id)?.open ?? 0) === 2, "leg B open");
  const mark = await flowMark(request);
  await controlRoute(request, { route: "connect1", up: false, cut: true });
  await controlRoute(request, { route: "connect2", up: false, cut: true });
  const flow = await flowAfter(request, mark, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN", "leg B Down");
  expect(legOn(flow, serverKey(server), a.id)?.target).toBe(4);
  expect(await download.release()).toBe("COMPLETED");
  await expectNoFailureEvents(request, download.jobId);
});

test("F06 a 503 from rung 0 is hop evidence and the leg recovers once it clears", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg, connect2 } = await twoRungLadder();
  const download = await world.pacedDownload("f06-503");
  await onRung(request, leg, 0, "leg on rung 0");
  const flowStart = await flowMark(request);
  await controlRoute(request, { route: "connect1", connectStatus: 503, cut: true });
  const moved = await onRung(request, leg, 1, "leg moved to rung 1", flowStart);
  expect(leg(moved)?.rungStates[0]).toBe("COOLDOWN");
  expect(leg(moved)?.selectedProxyId).toBe(connect2.id);
  await controlRoute(request, { route: "connect1", connectStatus: null });
  const returned = await rungZeroReturns(request, leg, moved.sampledAt);
  expect(leg(returned)?.rungStates[0]).toBe("STANDBY");
  expect(await download.release()).toBe("COMPLETED");
});

test("F07 a rejected proxy password cools rung 0 and the profile test reports it", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg, connect1 } = await twoRungLadder({ connect1Password: "wrong" });
  const download = await world.pacedDownload("f07-auth", { parts: 64, slowMs: 500 });
  const flow = await onRung(request, leg, 1, "leg on rung 1 after the 407");
  expect(leg(flow)?.rungStates[0]).toBe("COOLDOWN");
  const result = await testProxyProfile(request, connect1.id);
  expect(result.success).toBe(false);
  expect(result.message.trim()).not.toBe("");
  expect(await download.release()).toBe("COMPLETED");
});

test("F08 a refused proxy endpoint cools rung 0 and the leg recovers once it is back", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  // Toxiproxy's connect1 proxy is disabled rather than a container stopped:
  // the endpoint refuses and closes exactly as a stopped member would, and
  // the harness never runs docker itself.
  const { leg } = await twoRungLadder();
  const download = await world.pacedDownload("f08-refused");
  await onRung(request, leg, 0, "leg on rung 0");
  const flowStart = await flowMark(request);
  await setEnabled(request, "connect1", false);
  const moved = await onRung(request, leg, 1, "leg moved to rung 1", flowStart);
  expect(leg(moved)?.rungStates[0]).toBe("COOLDOWN");
  await setEnabled(request, "connect1", true);
  const returned = await rungZeroReturns(request, leg, moved.sampledAt);
  expect(leg(returned)?.rungStates[0]).toBe("STANDBY");
  expect(await download.release()).toBe("COMPLETED");
});

test("F09 a hop cut mid-body refetches the article without demoting the job", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  await twoRungLadder();
  const mark = await fixtureMark(request);
  await controlRoute(request, { route: "connect1", hold: true });
  const download = await world.download("f09-held-body");
  const held = await waitFixtureEvents(request, mark, events => events.some(event => event.kind === "held-body" && event.route === "connect1"), "a body held on connect1");
  const heldAt = held.find(event => event.kind === "held-body")!.sequence;
  await controlRoute(request, { route: "connect1", hold: false, cut: true });
  await waitFixtureEvents(request, heldAt, events => events.some(event => event.kind === "connected"), "a new hop connection after the cut");
  await waitFixtureEvents(request, heldAt, events => events.some(event => event.kind === "nntp-bytes"), "article bytes flowing again");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  await verifyOutput(request, download.jobId, "f09-held-body", download.bytes);
  const demotions = (await jobEvents(request, download.jobId)).filter(event => /demot/i.test(`${event.kind} ${event.message}`));
  expect(demotions).toEqual([]);
});

test("F10 an egress test through a cooling rung's proxy bypasses the cooldown", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { a, leg, connect1, download, moved } = await failFirstRung(request, "f10-probe-bypass");
  expect(leg(moved)?.rungStates[0]).toBe("COOLDOWN");
  await controlRoute(request, { route: "connect1", up: true });
  // The probe dials through connect1 while the live leg still holds that
  // rung in cooldown; a probe is never blocked by a cooldown.
  const result = await testEgress(request, a.id, PROXIED_HOST, 119, connect1.id);
  expect(result.success, result.message).toBe(true);
  expect(await download.release()).toBe("COMPLETED");
});

test("F11 server over-limit refusals never fail the leg or its rung", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg, server } = await twoRungLadder();
  // The held control session takes one of max_conns=3, leaving Weaver two.
  await world.holdChaosSession();
  const download = await world.pacedDownload("f11-over-limit", { chaos: "max_conns=3" });
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
    // A provider's over-limit refusal holds off new connects without counting as a server failure;
    // Weaver counts it in its own provider-refusal counter.
    await expect.poll(async () => metricValue(await metrics(request), "weaver_server_provider_refusals_total", { server_id: String(server) }) ?? 0,
      { timeout: 0, message: "over-limit refusals recorded" }).toBeGreaterThan(0);
    const window = subscription.since(start);
    expect(window.map(leg).filter(flow => flow && (flow.state === "DOWN" || flow.rungStates.includes("COOLDOWN")))).toEqual([]);
    expect(Math.max(...window.map(flow => leg(flow)?.open ?? 0))).toBeLessThanOrEqual(2);
    await world.setChaos("slow_body=1500");
    await subscription.next(start, flow => leg(flow)?.open === 4, "leg reaches 4 once the limit lifts");
  } finally {
    subscription.close();
  }
  expect(await download.release()).toBe("COMPLETED");
});

test("F12 server greeting failures never fail the leg or its rung", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const { leg } = await twoRungLadder();
  await world.holdChaosSession();
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
    const before = (await serverHealth(request, PROXIED_HOST))[0]?.failureCount ?? 0;
    const download = await world.download("f12-greet-400", { beforeSubmit: () => world.setChaos("greet_400=100") });
    await expect.poll(async () => (await serverHealth(request, PROXIED_HOST))[0]?.failureCount ?? 0, { timeout: 0, message: "greeting failures recorded" }).toBeGreaterThan(before);
    const window = subscription.since(start);
    expect(window.map(leg).filter(flow => flow && (flow.state === "DOWN" || flow.rungStates.includes("COOLDOWN")))).toEqual([]);
    await world.setChaos("off");
    expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
    expect(subscription.since(start).map(leg).filter(flow => flow && (flow.state === "DOWN" || flow.rungStates.includes("COOLDOWN")))).toEqual([]);
  } finally {
    subscription.close();
  }
});

test("F13 a chain dials SSH first and CONNECT through it", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const ssh1 = await world.ssh("ssh1");
  // The SSH server forwards from the tunnel fixture's own network, so the
  // CONNECT hop above it addresses the proxy fixture directly there.
  const connect1 = await world.connect("connect1", { direct: true });
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.chain([ssh1.id, connect1.id])], 100)] } });
  const mark = await fixtureMark(request);
  const download = await world.download("f13-chain");
  const flow = await flowAfter(request, await flowMark(request), sample => (legOn(sample, serverKey(server), a.id)?.open ?? 0) > 0, "chain leg open");
  expect(legOn(flow, serverKey(server), a.id)?.selectedProxyId).toBe(connect1.id);
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const forwarded = (await tunnelState(request)).ssh.ssh1!.forwarded;
  expect(forwarded).toContainEqual([connect1.host, connect1.port]);
  const connected = connectedOn(await fixtureEvents(request, mark), "connect1");
  expect(connected.length).toBeGreaterThan(0);
  const tunnelAddresses = networkHosts()["tunnel-fixture"] ?? [];
  expect(connected.every(event => tunnelAddresses.includes(event.client ?? ""))).toBe(true);
});

test("F14 reference rules reject unsound ladders", async ({ request }) => {
  const a = await world.egress("a");
  const server = await world.server();
  const connect1 = await world.connect("connect1");
  const wg1 = await world.wireguard("wg1");
  const noDns = await world.profile({ name: `nodns-${Date.now()}`, kind: "HTTP_CONNECT", enabled: true, host: connect1.host, port: connect1.port, username: "fixture", password: "fixture", dnsServers: [] });
  const feed = await world.feed({ url: "http://feed.proxy.test:8089/redirect" });
  const cases: Array<[string, "SERVER" | "RSS", number, RouteInput, string]> = [
    ["the same proxy twice", "SERVER", server, { legs: [ladderLeg(a.id, [rung.proxy(connect1.id), rung.proxy(connect1.id)], 100)] }, "a proxy cannot appear twice in a ladder"],
    ["WireGuard above CONNECT", "SERVER", server, { legs: [ladderLeg(a.id, [rung.chain([connect1.id, wg1.id])], 100)] }, "WireGuard and HTTP/3 must be the first proxy in a chain"],
    ["RSS through a proxy without DNS", "RSS", feed, { legs: [ladderLeg(a.id, [rung.proxy(noDns.id)], 100)] }, "every RSS proxy and pool member requires DNS servers"],
  ];
  for (const [label, kind, id, input, message] of cases) {
    expect((await graphqlErrors(request, SAVE_ROUTE, { kind, id, input })).join("\n"), label).toContain(message);
  }
  const h3 = await world.profile({ name: `h3-${Date.now()}`, kind: "HTTP3_CONNECT", enabled: true, host: connect1.host, port: 8443, username: "fixture", password: "fixture", dnsServers: connect1.dnsServers });
  expect((await graphqlErrors(request, SAVE_ROUTE, { kind: "SERVER", id: server, input: { legs: [ladderLeg(a.id, [rung.chain([connect1.id, h3.id])], 100)] } })).join("\n"), "HTTP/3 above CONNECT")
    .toContain("WireGuard and HTTP/3 must be the first proxy in a chain");
  // Only WireGuard rides on WireGuard; HTTP/3 stays directly above the egress.
  expect((await graphqlErrors(request, SAVE_ROUTE, { kind: "SERVER", id: server, input: { legs: [ladderLeg(a.id, [rung.chain([wg1.id, h3.id])], 100)] } })).join("\n"), "HTTP/3 above WireGuard")
    .toContain("WireGuard and HTTP/3 must be the first proxy in a chain");
});

test("F15 a pool whose members are all blocked blocks the leg until its path changes", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  // Two SSH members pinned to the primary key on ssh-switch, then the key changes.
  const first = await world.ssh("ssh-switch");
  const second = await world.ssh("ssh-switch", { auth: "key" });
  const pool = await world.pool("SSH", [first.id, second.id]);
  const connect3 = await world.connect("connect3");
  const server = await world.server({ host: "nntp", route: { legs: [ladderLeg(a.id, [rung.pool(pool)], 100)] } });
  const pin = await world.download("f15-pin");
  expect(await waitTerminal(request, pin.jobId)).toBe("COMPLETED");
  await expect.poll(async () => (await proxyProfiles(request)).filter(profile => [first.id, second.id].includes(profile.id) && profile.hostKeyFingerprint !== null).length,
    { timeout: 0, message: "both members pinned" }).toBe(2);
  await controlTunnel(request, { endpoint: "ssh-switch", hostKey: "other", cut: true });
  const mark = await flowMark(request);
  const blockedJob = await world.download("f15-blocked");
  const blocked = await flowAfter(request, mark, flow => legOn(flow, serverKey(server), a.id)?.state === "BLOCKED", "leg Blocked");
  expect(legOn(blocked, serverKey(server), a.id)?.reason).toContain("all pool members are blocked");
  // A changed path rebuilds the leg. A blocked pool is fatal to every rung after
  // it, so the new path leaves the pool out. The host and route change in one
  // save because an edited endpoint is probed over the route it is saved with.
  const changed = await flowMark(request);
  await world.updateServer(server, { host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.proxy(connect3.id)], 100)] } });
  await flowAfter(request, changed, flow => legOn(flow, serverKey(server), a.id)?.state === "UP", "leg Up on the changed path");
  expect(await waitTerminal(request, blockedJob.jobId)).toBe("COMPLETED");
});

test("F16 @extended an eight-rung ladder walks every rung as each dies", async ({ request }, info) => {
  test.skip(!extended(), "extended lane only");
  test.setTimeout(45 * 60_000);
  const a = await world.egress("a");
  const members = ["connect1", "connect2", "connect3", "connect4", "connect5", "connect6", "socks1", "socks2"];
  const profiles = [];
  for (const member of members) profiles.push(member.startsWith("socks") ? await world.socks(member) : await world.connect(member));
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, profiles.map(profile => rung.proxy(profile.id)), 100)] } });
  const leg = (flow: NetworkFlow) => legOn(flow, serverKey(server), a.id);
  const download = await world.pacedDownload("f16-eight-rungs", { parts: 480, slowMs: 1500 });
  let flow = await onRung(request, leg, 0, "leg on rung 0");
  const sequence = [0];
  for (let index = 0; index < 7; index += 1) {
    const mark = await flowMark(request);
    await controlRoute(request, { route: members[index]!, up: false, cut: true });
    flow = await onRung(request, leg, index + 1, `leg on rung ${index + 1}`, mark);
    sequence.push(leg(flow)!.selectedRung!);
    for (let earlier = 0; earlier <= index; earlier += 1) expect(leg(flow)?.rungStates[earlier], `rung ${earlier}`).toBe("COOLDOWN");
  }
  await saveEvidence(info, "F16", sequence);
  expect(sequence).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  expect(await download.release()).toBe("COMPLETED");
});

test("F17 a chain carries WireGuard inside WireGuard", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  // Each WireGuard hop in the chain is its own instance.
  test.skip((await platformNetworking(request)).maxWireguardInstances < 2, "a WireGuard-in-WireGuard chain needs two WireGuard instances");
  const a = await world.egress("a");
  const wg1 = await world.wireguard("wg1");
  // wg2 addressed where wg1 relays to it, inside wg1's tunnel.
  const wg2 = await world.wireguard("wg2", { carried: true });
  const mark = await tunnelMark(request);
  const server = await world.server({ host: "nntp.proxy.test", route: { legs: [ladderLeg(a.id, [rung.chain([wg1.id, wg2.id])], 100)] } });
  const download = await world.download("f17-wireguard-chain");
  const flow = await flowAfter(request, await flowMark(request), sample => (legOn(sample, serverKey(server), a.id)?.open ?? 0) > 0, "chain leg open");
  expect(legOn(flow, serverKey(server), a.id)?.selectedProxyId).toBe(wg2.id);
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const peers = (await tunnelState(request)).wireguard;
  // wg2's endpoint name resolved inside wg1, the server's name inside wg2.
  expect(peers.wg1!.dnsQueries).toContain(peers.wg2!.carriedBy!.host);
  expect(peers.wg2!.dnsQueries).toContain("nntp.proxy.test");
  // wg1 relayed to wg2 nothing but WireGuard, all of it from wg1's own
  // client address: wg2 saw only the tunnel riding inside wg1.
  const clientAddress = peers.wg1!.clientAddress.split("/")[0];
  const forwarded = Object.entries(peers.wg1!.udpForwarded);
  expect(forwarded.length).toBeGreaterThan(0);
  for (const [source, seen] of forwarded) {
    expect(source.slice(0, source.lastIndexOf(":")), source).toBe(clientAddress);
    expect(seen.datagrams, source).toBeGreaterThan(0);
    expect(seen.wireguardDatagrams, source).toBe(seen.datagrams);
  }
  // Weaver reached wg1's published endpoint and never wg2's.
  const connects = (await tunnelEvents(request, mark)).filter(event => event.kind === "connect");
  expect(connects.some(event => event.endpoint === "wg1")).toBe(true);
  expect(connects.filter(event => event.endpoint === "wg2")).toEqual([]);
  test.info().annotations.push({ type: "deviation", description: "F17: the chain [wg1, wg2, ssh1] is not run; wg2 forwards only to the NNTP relay, so no SSH endpoint is reachable inside wg2." });
});

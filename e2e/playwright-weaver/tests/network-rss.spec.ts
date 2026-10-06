import { expect, graphql, test } from "./helpers";
import { type NetworkFlow, flowAfter, flowMark, ladderLeg, legOn, rssKey, rung } from "./support/network-flow";
import { NetworkWorld, saveEvidence } from "./support/network-scenario";
import { type FixtureEvent, controlRoute, directEvents, fixtureEvents, fixtureMark } from "./support/proxy-fixture";
import { clearScriptRecords, removeFixtureScripts, scriptBodies, scriptRecords, writeFixtureScript } from "./support/script-fixtures";
import { useScripts } from "./support/script-settings";
import { tunnelState } from "./support/tunnel-fixture";

/** RSS through routes (checkpoint 5.7). */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const connectedOn = (events: FixtureEvent[], route: string) => events.filter(event => event.kind === "connected" && event.route === route);

async function syncedEvents(request: Parameters<typeof fixtureMark>[0], feed: number) {
  const mark = await fixtureMark(request);
  const report = await world.sync(feed);
  return { report, events: await fixtureEvents(request, mark) };
}

test("RS01 a feed fetched through a CONNECT ladder resolves and fetches through it", async ({ request }, info) => {
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  const url = await world.armFeed(`rs01-${Date.now()}`);
  const feed = await world.feed({ url, route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 100)] } });
  const { report, events } = await syncedEvents(request, feed);
  await saveEvidence(info, "RS01", { report, events });
  expect(report.errors).toEqual([]);
  expect(report.itemsFetched).toBeGreaterThan(0);
  expect(events.some(event => event.kind === "routed-dns")).toBe(true);
  expect(events.some(event => event.kind === "routed-http")).toBe(true);
  expect(connectedOn(events, "connect1").length).toBeGreaterThan(0);
  expect(directEvents(events)).toEqual([]);
});

test("RS02 a feed moves to its second rung while the first is down and returns after the cooldown", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  const connect2 = await world.connect("connect2");
  const feed = await world.feed({ url: await world.armFeed(`rs02-${Date.now()}`), route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id), rung.proxy(connect2.id)], 100)] } });
  const leg = (flow: NetworkFlow) => legOn(flow, rssKey(feed), a.id);
  await controlRoute(request, { route: "connect1", up: false });
  const down = await syncedEvents(request, feed);
  expect(down.report.errors).toEqual([]);
  expect(connectedOn(down.events, "connect1")).toEqual([]);
  expect(connectedOn(down.events, "connect2").length).toBeGreaterThan(0);
  const cooling = await flowAfter(request, await flowMark(request), flow => leg(flow)?.rungStates[0] === "COOLDOWN", "rung 0 cooling");
  await controlRoute(request, { route: "connect1", up: true });
  await flowAfter(request, cooling.sampledAt, flow => leg(flow)?.rungStates[0] === "STANDBY", "rung 0 cooldown over");
  await world.armFeed(`rs02-back-${Date.now()}`);
  const back = await syncedEvents(request, feed);
  expect(back.report.errors).toEqual([]);
  expect(connectedOn(back.events, "connect1").length).toBeGreaterThan(0);
  expect(connectedOn(back.events, "connect2")).toEqual([]);
});

test("RS03 with every rung down, direct fallback resolves and fetches directly", async ({ request }) => {
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  const connect2 = await world.connect("connect2");
  const feed = await world.feed({ url: await world.armFeed(`rs03-${Date.now()}`), route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id), rung.proxy(connect2.id)], 100, true)] } });
  await controlRoute(request, { route: "connect1", up: false });
  await controlRoute(request, { route: "connect2", up: false });
  const { report, events } = await syncedEvents(request, feed);
  expect(report.errors).toEqual([]);
  expect(events.some(event => event.kind === "direct-dns")).toBe(true);
  expect(events.some(event => event.kind === "direct-http")).toBe(true);
});

test("RS04 a FEED script rewrites the routed feed and the rewritten item is queued", async ({ request }) => {
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  // A FEED script must exit 93 (NZBGet's success) or the feed is not used;
  // scripts only run with execution switched on.
  const script = writeFixtureScript(`rs04-feed-${Date.now()}.sh`, { kinds: ["FEED"], body: scriptBodies.rewriteFeedTitles("rewritten-"), exitCode: 93 });
  clearScriptRecords();
  const restoreScripts = await useScripts(request, {});
  try {
    const token = `rs04-${Date.now()}`;
    const feed = await world.feed({ url: await world.armFeed(token), route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 100)] }, scripts: [script] });
    const { report, events } = await syncedEvents(request, feed);
    expect(report.errors).toEqual([]);
    expect(connectedOn(events, "connect1").length).toBeGreaterThan(0);
    const records = scriptRecords(script);
    expect(records).toHaveLength(1);
    expect(records[0]!.env.NZBFP_FILENAME?.trim()).toBeTruthy();
    expect(report.itemsSubmitted).toBe(1);
    const jobs = (await graphql<{ jobs: Array<{ id: number; name: string; originalTitle: string }> }>(request, "query { jobs { id name originalTitle } }")).jobs;
    expect(jobs.filter(job => `${job.name} ${job.originalTitle}`.includes(`rewritten-proxy-${token}`))).toHaveLength(1);
  } finally {
    await restoreScripts();
    removeFixtureScripts([script]);
  }
});

test("RS05 a feed fetched through WireGuard resolves its host inside the tunnel", async ({ request }) => {
  const a = await world.egress("a");
  const wg = await world.wireguard("wg-rss");
  const feed = await world.feed({ url: await world.armFeed(`rs05-${Date.now()}`, "rss.proxy.test"), route: { legs: [ladderLeg(a.id, [rung.proxy(wg.id)], 100)] } });
  const report = await world.sync(feed);
  expect(report.errors).toEqual([]);
  expect(report.itemsFetched).toBeGreaterThan(0);
  expect((await tunnelState(request)).wireguard["wg-rss"]!.dnsQueries).toContain("rss.proxy.test");
});

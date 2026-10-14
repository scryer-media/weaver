import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, postMultipartProbeArticle, submitProbeNzb, test, updateConfiguredServer } from "./helpers";

type Event = { sequence: number; at: number; kind: string; route?: string; host?: string; port?: number; path?: string; name?: string };
type Evidence = { ip: string; ports: Record<string, number>; events: Event[]; active: Record<string, number> };
type Policy = { proxyIds: number[]; allowDirect: boolean };
type Status = { state: string; selectedProxyId: number | null; failures: Array<{ proxyId: number }> };
const controlUrl = process.env.PROXY_FIXTURE_URL || "http://proxy-fixture:8090";
const names = ["primary", "secondary", "tertiary"] as const;

test("proxy ladder: ordered recovery, host fallback, fail closed and active revocation", async ({ request }, info) => {
  test.setTimeout(8 * 60_000);
  const evidence = async (): Promise<Evidence> => {
    const response = await request.get(controlUrl);
    expect(response.ok()).toBeTruthy();
    return await response.json();
  };
  const control = async (data: Record<string, unknown>) => {
    const response = await request.post(controlUrl, { data });
    expect(response.ok(), await response.text()).toBeTruthy();
  };
  const mark = async () => (await evidence()).events.at(-1)?.sequence ?? 0;
  const since = async (sequence: number) => (await evidence()).events.filter(event => event.sequence > sequence);
  const noDirect = async (sequence: number) => {
    expect((await since(sequence)).filter(event => event.kind.startsWith("direct-")), "host destination connections and DNS must remain absent").toEqual([]);
  };
  const checkpoints: Array<{ name: string; sequence: number }> = [];
  const step = async (name: string, action: () => Promise<void>) => {
    checkpoints.push({ name, sequence: await mark() });
    await test.step(name, action);
  };
  try {
    const fixture = await evidence();
    await updateConfiguredServer(request, "nntp", { active: false });
    await updateConfiguredServer(request, "nntp2", { active: false });
    const profiles = names.map((name, index) => ({
      name: `e2e-${name}`, kind: index === 1 ? "SOCKS5" : "HTTP_CONNECT", resolver: "PROXY",
      enabled: true, host: fixture.ip, port: fixture.ports[name],
      username: "fixture", password: "fixture", dnsServers: [fixture.ip], timeoutSeconds: 2,
    }));
    const ids: number[] = [];
    for (const input of profiles) {
      const data = await graphql<{ saveProxyProfile: { id: number } }>(request,
        `mutation($input: ProxyProfileInput!) { saveProxyProfile(input: $input) { id } }`, { input });
      ids.push(data.saveProxyProfile.id);
    }
    const input = {
      host: "news.proxy.test", port: fixture.ports.nntp, tls: false,
      username: "e2e-user", password: "e2e-pass", connections: 1, active: false,
    };
    const initial: Policy = { proxyIds: ids, allowDirect: false };
    const added = await graphql<{ addServer: { id: number } }>(request,
      `mutation($input: ServerInput!) { addServer(input: $input) { id } }`, { input: { ...input, routing: initial } });
    const serverId = added.addServer.id;
    const configure = async (routing: Policy, overrides: Record<string, unknown> = {}) => {
      await graphql(request, `mutation($id: Int!, $input: ServerInput!) { updateServer(id: $id, input: $input) { id routing { proxyIds allowDirect } } }`,
        { id: serverId, input: { ...input, ...overrides, routing } });
    };
    const probe = async () => (await graphql<{ testConfiguredServerConnection: { success: boolean; message: string } }>(request,
      `mutation($id: Int!) { testConfiguredServerConnection(id: $id) { success message } }`, { id: serverId })).testConfiguredServerConnection;
    const status = async () => (await graphql<{ servers: Array<{ id: number; routingStatus: Status }> }>(request,
      `query { servers { id routingStatus { state selectedProxyId failures { proxyId } } } }`)).servers.find(server => server.id === serverId)!.routingStatus;
    // These proxies resolve with the fixture's DNS server, asked through the
    // proxy: a hop dials it once per lookup, A then AAAA, before the server.
    // A route that refuses the first lookup is left at once.
    const attempts = async (sequence: number) => (await since(sequence)).filter(event => event.kind === "attempt")
      .map(event => event.port === fixture.ports.dns ? `${event.route} dns` : event.route);
    const allDown = async () => { for (const route of names) await control({ route, up: false, cut: true, hold: false }); };
    const allUp = async () => { for (const route of names) await control({ route, up: true, hold: false }); };

    await step("NNTP connection tests prefer primary and fail over sequentially", async () => {
      const start = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(start)).toEqual(["primary dns", "primary dns", "primary"]);
      await control({ route: "primary", up: false, cut: true });
      const failover = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(failover)).toEqual(["primary dns", "secondary dns", "secondary dns", "secondary"]);
      await noDirect(start);
    });

    await step("third route and persisted ordering", async () => {
      await control({ route: "primary", up: false });
      await control({ route: "secondary", up: false });
      const start = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(start)).toEqual(["primary dns", "secondary dns", "tertiary dns", "tertiary dns", "tertiary"]);
      await configure({ proxyIds: [ids[2]!, ids[1]!, ids[0]!], allowDirect: false });
      const order = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(order)).toEqual(["tertiary dns", "tertiary dns", "tertiary"]);
      await noDirect(start);
    });

    await step("exhausted ladder: host disabled means unable to connect", async () => {
      await allDown();
      await configure(initial);
      const start = await mark();
      expect(await probe()).toMatchObject({ success: false });
      await noDirect(start);
    });

    await step("exhausted ladder: explicit host fallback succeeds and proves canaries", async () => {
      await configure({ proxyIds: ids, allowDirect: true }, { host: "direct-control.proxy.test" });
      const start = await mark();
      expect(await probe()).toMatchObject({ success: true });
      const events = await since(start);
      expect(events.some(event => event.kind === "direct-nntp")).toBeTruthy();
      expect(events.some(event => event.kind === "direct-dns" && event.name === "direct-control.proxy.test")).toBeTruthy();
      await configure(initial);
      const blocked = await mark();
      expect(await probe()).toMatchObject({ success: false });
      await noDirect(blocked);
    });

    await step("disabled profiles and empty blocked ladders never enable host access", async () => {
      for (const [index, profile] of profiles.entries()) await graphql(request,
        `mutation($id: Int!, $input: ProxyProfileInput!) { saveProxyProfile(id: $id, input: $input) { id } }`,
        { id: ids[index], input: { ...profile, enabled: false } });
      const start = await mark();
      expect(await probe()).toMatchObject({ success: false });
      expect(await attempts(start)).toEqual([]);
      await configure({ proxyIds: [], allowDirect: false });
      expect(await probe()).toMatchObject({ success: false });
      await noDirect(start);
      for (const [index, profile] of profiles.entries()) await graphql(request,
        `mutation($id: Int!, $input: ProxyProfileInput!) { saveProxyProfile(id: $id, input: $input) { id } }`, { id: ids[index], input: profile });
    });

    await step("active NNTP download retries through secondary after primary loss", async () => {
      await allUp();
      await control({ route: "primary", hold: true });
      await configure(initial, { active: true });
      const start = await mark();
      const jobId = await startDownload(request, "proxy-active-failover");
      await expect.poll(async () => (await since(start)).some(event => event.kind === "held-body" && event.route === "primary")).toBeTruthy();
      await control({ route: "primary", up: false, cut: true });
      await expect.poll(async () => (await since(start)).some(event => event.kind === "nntp-bytes" && event.route === "secondary"), { timeout: 60_000 }).toBeTruthy();
      await expect.poll(async () => await jobState(request, jobId), { timeout: 60_000 }).toBe("COMPLETED");
      await noDirect(start);
      await configure(initial);
    });

    await step("RSS redirects and inherited NZB requests fail over without host DNS", async () => {
      await allUp();
      await control({ route: "primary", up: false });
      await control({ nzb: probeNzb("proxy-rss"), feedToken: "routed" });
      const feedInput = { name: "Proxy RSS", url: `http://feed.proxy.test:${fixture.ports.http}/redirect`, enabled: false, pollIntervalSecs: 86400, routing: initial };
      const added = await graphql<{ addRssFeed: { id: number } }>(request,
        `mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }`, { input: feedInput });
      const feedId = added.addRssFeed.id;
      await graphql(request, `mutation($id: Int!) { addRssRule(feedId: $id, input: { sortOrder: 0, action: ACCEPT }) { id } }`, { id: feedId });
      const sync = async () => (await graphql<{ runRssSync: { itemsSubmitted: number; errors: string[] } }>(request,
        `mutation($id: Int!) { runRssSync(feedId: $id) { itemsSubmitted errors } }`, { id: feedId })).runRssSync;
      const update = async (routing: Policy, url = feedInput.url) => { await graphql(request,
        `mutation($id: Int!, $input: RssFeedInput!) { updateRssFeed(id: $id, input: $input) { id } }`, { id: feedId, input: { ...feedInput, url, routing } }); };
      const start = await mark();
      expect(await sync()).toMatchObject({ itemsSubmitted: 1, errors: [] });
      await control({ route: "primary", up: true });
      const cooldown = await mark();
      expect(await sync()).toMatchObject({ itemsSubmitted: 0, errors: [] });
      expect((await attempts(cooldown)).filter(route => route?.startsWith("primary"))).toEqual([]);
      expect(await attempts(cooldown)).toContain("secondary");
      // Configured-server tests intentionally use disposable routes. RSS
      // polling exercises the real consumer cooldown and monotonic deadline.
      await new Promise(resolve => setTimeout(resolve, 30_100));
      const recovery = await mark();
      expect(await sync()).toMatchObject({ itemsSubmitted: 0, errors: [] });
      expect(await attempts(recovery)).toContain("primary");
      expect((await attempts(recovery)).filter(route => route?.startsWith("secondary"))).toEqual([]);
      const events = await since(start);
      expect(events.some(event => event.kind === "routed-dns" && event.name === "feed.proxy.test")).toBeTruthy();
      expect(events.some(event => event.kind === "routed-dns" && event.name === "download.proxy.test")).toBeTruthy();
      expect(events.filter(event => event.kind === "routed-http" && event.path === "/probe.nzb")).toHaveLength(1);
      expect(events.filter(event => event.kind === "attempt").slice(0, 2).map(event => event.route)).toEqual(["primary", "secondary"]);
      await noDirect(start);
      await allDown();
      await update(initial);
      const blocked = await mark();
      expect((await sync()).errors.length).toBeGreaterThan(0);
      await noDirect(blocked);
      await update({ proxyIds: ids, allowDirect: true }, `http://rss-direct-control.proxy.test:${fixture.ports.http}/redirect`);
      const direct = await mark();
      expect((await sync()).errors).toEqual([]);
      expect((await since(direct)).some(event => event.kind === "direct-http")).toBeTruthy();
      expect((await since(direct)).some(event => event.kind === "direct-dns")).toBeTruthy();
      await update(initial);
      const revoked = await mark();
      expect((await sync()).errors.length).toBeGreaterThan(0);
      await noDirect(revoked);
      await graphql(request, `mutation($id: Int!) { deleteRssFeed(id: $id) }`, { id: feedId });
    });

    await step("disabling host fallback revokes an active direct download", async () => {
      await allDown();
      await control({ route: "direct", hold: true });
      await configure({ proxyIds: ids, allowDirect: true }, { active: true });
      const start = await mark();
      await startDownload(request, "proxy-direct-revocation");
      await expect.poll(async () => (await since(start)).some(event => event.kind === "held-body" && event.route === "direct")).toBeTruthy();
      await configure(initial, { active: true });
      await expect.poll(async () => (await evidence()).active.direct).toBe(0);
      const revoked = await mark();
      expect(await probe()).toMatchObject({ success: false });
      // A connection test dials a throwaway route of its own so that trying a
      // server never disturbs the downloads already running through it, which
      // also means it never moves the live route this status reads. The live
      // route is walked to exhaustion by the revoked download's own retry, so
      // poll for the verdict instead of reading it the instant the probe
      // returns — and let that same window prove no retry reached the host.
      await expect.poll(async () => (await status()).state, { timeout: 30_000 }).toBe("BLOCKED");
      await noDirect(revoked);
      await configure(initial);
    });
  } finally {
    const body = JSON.stringify({ checkpoints, ...await evidence() }, null, 2);
    const root = process.env.PLAYWRIGHT_ARTIFACTS_DIR || "artifacts";
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "proxy-routing-evidence.json"), body);
    await info.attach("proxy-routing-evidence", { body, contentType: "application/json" });
  }
});

// Weaver resolves the server's name with the host's resolver, here the
// fixture's canary, and hands the proxy the address it got back.
test("a proxy Weaver resolves for is handed the address and carries a download", async ({ request }) => {
  test.setTimeout(4 * 60_000);
  const events = async (after: number) => {
    const response = await request.get(`${controlUrl}/events?after=${after}`);
    expect(response.ok()).toBeTruthy();
    return await response.json() as { sequence: number; events: Event[] };
  };
  const response = await request.get(controlUrl);
  expect(response.ok()).toBeTruthy();
  const fixture: Evidence = await response.json();
  const control = await request.post(controlUrl, { data: { route: "primary", up: true, hold: false, connectStatus: null } });
  expect(control.ok(), await control.text()).toBeTruthy();
  await updateConfiguredServer(request, "nntp", { active: false });
  await updateConfiguredServer(request, "nntp2", { active: false });
  const saved = await graphql<{ saveProxyProfile: { id: number; resolver: string; dnsServers: string[] } }>(request,
    `mutation($input: ProxyProfileInput!) { saveProxyProfile(input: $input) { id resolver dnsServers } }`,
    { input: {
      name: "e2e-weaver-resolves", kind: "HTTP_CONNECT", resolver: "WEAVER", enabled: true,
      host: fixture.ip, port: fixture.ports.primary, username: "fixture", password: "fixture", dnsServers: [], timeoutSeconds: 2,
    } });
  expect(saved.saveProxyProfile).toMatchObject({ resolver: "WEAVER", dnsServers: [] });
  const name = "weaver-resolves.proxy.test";
  const input = {
    host: name, port: fixture.ports.nntp, tls: false, username: "e2e-user", password: "e2e-pass", connections: 1,
    routing: { proxyIds: [saved.saveProxyProfile.id], allowDirect: false },
  };
  const start = (await events(Number.MAX_SAFE_INTEGER)).sequence;
  const added = await graphql<{ addServer: { id: number } }>(request,
    `mutation($input: ServerInput!) { addServer(input: $input) { id } }`, { input: { ...input, active: true } });
  try {
    const jobId = await startDownload(request, "proxy-weaver-resolves");
    await expect.poll(async () => await jobState(request, jobId), { timeout: 60_000 }).toBe("COMPLETED");
    const seen = (await events(start)).events;
    expect(seen.some(event => event.kind === "direct-dns" && event.name === name), "Weaver asks the host's resolver").toBeTruthy();
    expect(seen.filter(event => event.kind === "routed-dns" && event.name === name), "the proxy is never asked for the name").toEqual([]);
    const attempts = seen.filter(event => event.kind === "attempt" && event.route === "primary");
    expect(attempts.length).toBeGreaterThan(0);
    expect(attempts.filter(event => event.host !== fixture.ip), "the proxy is handed only the address").toEqual([]);
    expect(seen.some(event => event.kind === "nntp-bytes" && event.route === "primary")).toBeTruthy();
    expect(seen.filter(event => event.kind === "direct-nntp"), "no host destination connection").toEqual([]);
  } finally {
    await graphql(request, `mutation($id: Int!, $input: ServerInput!) { updateServer(id: $id, input: $input) { id } }`,
      { id: added.addServer.id, input: { ...input, active: false } });
  }
});

async function startDownload(request: APIRequestContext, name: string): Promise<number> {
  const count = 32;
  const size = 64 * 1024;
  const articles = Array.from({ length: count }, (_, index) => ({ messageId: `${name}-${index}@e2e.invalid`, bytes: size }));
  for (const [index, article] of articles.entries()) await postMultipartProbeArticle(article.messageId, size, {
    filename: `${name}.bin`, number: index + 1, total: count, begin: index * size + 1,
    end: (index + 1) * size, totalBytes: count * size,
  });
  const result = await submitProbeNzb(request, name, articles, {}, "single-multipart-file");
  expect(result).toMatchObject({ accepted: true });
  expect(result.jobId).not.toBeNull();
  return result.jobId!;
}

async function jobState(request: APIRequestContext, id: number): Promise<string> {
  return (await graphql<{ historyItem: { state: string } | null }>(request,
    `query($id: Int!) { historyItem(id: $id) { state } }`, { id })).historyItem?.state ?? "PENDING";
}

function probeNzb(name: string): string {
  return `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="fixture" date="1700000000" subject="${name}.bin"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="1" number="1">${name}@e2e.invalid</segment></segments></file></nzb>`;
}

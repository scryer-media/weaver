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
      name: `e2e-${name}`, kind: index === 1 ? "SOCKS5" : "HTTP_CONNECT",
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
    const attempts = async (sequence: number) => (await since(sequence)).filter(event => event.kind === "attempt").map(event => event.route);
    const allDown = async () => { for (const route of names) await control({ route, up: false, cut: true, hold: false }); };
    const allUp = async () => { for (const route of names) await control({ route, up: true, hold: false }); };

    await step("NNTP connection tests prefer primary and fail over sequentially", async () => {
      const start = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(start)).toEqual(["primary"]);
      await control({ route: "primary", up: false, cut: true });
      const failover = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(failover)).toEqual(["primary", "secondary"]);
      await noDirect(start);
    });

    await step("third route and persisted ordering", async () => {
      await control({ route: "primary", up: false });
      await control({ route: "secondary", up: false });
      const start = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(start)).toEqual(["primary", "secondary", "tertiary"]);
      await configure({ proxyIds: [ids[2]!, ids[1]!, ids[0]!], allowDirect: false });
      const order = await mark();
      expect(await probe()).toMatchObject({ success: true });
      expect(await attempts(order)).toEqual(["tertiary"]);
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
      expect(await attempts(cooldown)).not.toContain("primary");
      expect(await attempts(cooldown)).toContain("secondary");
      // Configured-server tests intentionally use disposable routes. RSS
      // polling exercises the real consumer cooldown and monotonic deadline.
      await new Promise(resolve => setTimeout(resolve, 30_100));
      const recovery = await mark();
      expect(await sync()).toMatchObject({ itemsSubmitted: 0, errors: [] });
      expect(await attempts(recovery)).toContain("primary");
      expect(await attempts(recovery)).not.toContain("secondary");
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
      expect(await status()).toMatchObject({ state: "BLOCKED" });
      // Observe retries, not just the instant at which the mutation returns.
      await new Promise(resolve => setTimeout(resolve, 3000));
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

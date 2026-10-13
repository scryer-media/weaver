import type { APIRequestContext, Locator, Page } from "@playwright/test";
import { expect, graphql, submitProbeNzb, test, weaverRoute } from "./helpers";
import { postProbeFile, waitTerminal } from "./support/downloads";
import { ladderLeg, rung } from "./support/network-flow";
import { NetworkWorld, PROXIED_HOST } from "./support/network-scenario";
import { type FixtureEvent, directEvents, fixtureEvents, fixtureMark, fixtureState, setFixtureNzb } from "./support/proxy-fixture";
import { queueState, resumeAll, waitPaused } from "./support/schedules";

// The speed test (plan 169) driven from Networking -> Speed test: a Usenet
// run over a laddered route, the same run with the rung taken down, the run
// against a real job on the same route, and the queue gate around a run.
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({ request }, info) => {
  // A run still going would hold the queue paused into the next test.
  for (const run of await speedTestRuns(request)) {
    if (run.status === "RUNNING") await graphql(request, "mutation($id: Int!) { cancelSpeedTest(id: $id) }", { id: run.id });
  }
  await expect.poll(async () => (await speedTestRuns(request)).filter(run => run.status === "RUNNING").length,
    { timeout: 0, message: "no speed test running" }).toBe(0);
  if ((await queueState(request)).isPaused) await resumeAll(request);
  await world.cleanup(info);
});

type DialPath = { egressId: number; rung: number | null; poolId: number | null; memberId: number | null; proxyIds: number[] };
type SpeedTestRun = {
  id: number; consumer: string; consumerName: string; kind: string; status: string; phase: string | null; error: string | null;
  tested: { legs: Array<{ position: number; down: boolean }>; overrides: { downRungs: Array<{ leg: number; rung: number }> } };
  result: {
    bytes: number; averageBps: number; peakBps: number;
    connections: Array<{ leg: number; path: DialPath; bytes: number; error: string | null }>;
    usenet: { articlesRequested: number; delivered: number; missing: number; failed: number; declaredBytes: number; decodedBytes: number } | null;
    bandwidth: { providerId: string | null; url: string; streams: number } | null;
  } | null;
};

const RUN_FIELDS = `id consumer consumerName kind status phase error
  tested { legs { position down } overrides { downRungs { leg rung } } }
  result { bytes averageBps peakBps
    connections { leg bytes error path { egressId rung poolId memberId proxyIds } }
    usenet { articlesRequested delivered missing failed declaredBytes decodedBytes }
    bandwidth { providerId url streams } }`;

async function speedTestRun(request: APIRequestContext, id: number): Promise<SpeedTestRun> {
  const run = (await graphql<{ speedTestRun: SpeedTestRun | null }>(request,
    `query($id: Int!) { speedTestRun(id: $id) { ${RUN_FIELDS} } }`, { id })).speedTestRun;
  expect(run, `speed test run ${id}`).not.toBeNull();
  return run!;
}

async function speedTestRuns(request: APIRequestContext): Promise<Array<{ id: number; status: string }>> {
  return (await graphql<{ speedTestRuns: Array<{ id: number; status: string }> }>(request, "query { speedTestRuns { id status } }")).speedTestRuns;
}

// The pipeline's wire-byte counter, which a Usenet run books into exactly as
// a download does.
async function bytesDownloaded(request: APIRequestContext): Promise<number> {
  return (await graphql<{ systemMetrics: { bytesDownloaded: number } }>(request, "query { systemMetrics { bytesDownloaded } }")).systemMetrics.bytesDownloaded;
}

// The counter is read from a published snapshot. Wait until it carries the
// expected bytes on top of `before`, then require that it carries no more.
async function expectBooked(request: APIRequestContext, before: number, bytes: number, describe: string): Promise<number> {
  await expect.poll(async () => await bytesDownloaded(request) - before, { timeout: 0, message: `${describe} booked` }).toBeGreaterThanOrEqual(bytes);
  const after = await bytesDownloaded(request);
  expect(after - before, `${describe} booked exactly its bytes`).toBe(bytes);
  return after;
}

// The web client's byte and rate labels (splitBytes: binary steps, one
// decimal under 100), so a figure read from the API can be found on the page.
const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];
function sizeLabel(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const index = Math.min(Math.max(Math.floor(Math.log(bytes) / Math.log(1024)), 0), UNITS.length - 1);
  const scaled = bytes / 1024 ** index;
  return `${scaled.toFixed(index === 0 ? 0 : scaled >= 100 ? 0 : 1)} ${UNITS[index]}`;
}
const rateLabel = (bytesPerSecond: number) => `${sizeLabel(bytesPerSecond)}/s`;

// One multipart file in the same layout submitProbeNzb writes, so the test NZB
// and the job name the same articles.
function probeNzb(name: string, articles: Array<{ messageId: string; bytes: number }>): string {
  const segments = articles.map(({ messageId, bytes }, index) => `<segment bytes="${bytes}" number="${index + 1}">${messageId}</segment>`).join("");
  return `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="weaver-e2e" date="1700000000" subject="${name}.bin"><groups><group>alt.binaries.test</group></groups><segments>${segments}</segments></file></nzb>`;
}

// Post the articles, serve their NZB from the proxy fixture's HTTP port, and
// return its URL. download.proxy.test resolves to the fixture both directly
// and through the fixture's proxies.
async function servedNzb(request: APIRequestContext, name: string, parts: number) {
  const articles = await postProbeFile(name, { count: parts });
  await setFixtureNzb(request, probeNzb(name, articles));
  const port = (await fixtureState(request)).ports.http;
  return { articles, url: `http://download.proxy.test:${port}/probe.nzb` };
}

// One leg on egress-a whose ladder is [connect1] with a direct fallback.
async function ladderedServer() {
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  const server = await world.server({
    host: PROXIED_HOST, connections: 4,
    route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 100, true)] },
  });
  return { a, connect1, server };
}

const connectedOn = (events: FixtureEvent[], route: string) => events.filter(event => event.kind === "connected" && event.route === route);

class SpeedPage {
  readonly route: Locator;
  readonly test: Locator;
  readonly runs: Locator;

  constructor(readonly page: Page) {
    this.route = page.getByRole("region", { name: "Route", exact: true });
    this.test = page.getByRole("region", { name: "Test", exact: true });
    this.runs = page.getByRole("region", { name: "Runs", exact: true });
  }

  async open(server: number): Promise<void> {
    await this.page.goto(weaverRoute(`/settings/networking/speed?consumer=server:${server}`));
    await expect(this.route.getByRole("button", { name: "Route", exact: true })).toContainText(PROXIED_HOST);
    await expect(this.test.getByRole("radio", { name: "Usenet", exact: true })).toHaveAttribute("aria-checked", "true");
  }

  // A Usenet run of the NZB at `url`, allowed long enough to drain it.
  async useNzbUrl(url: string): Promise<void> {
    await this.test.getByRole("button", { name: "NZB", exact: true }).click();
    await this.page.getByRole("menu", { name: "NZB", exact: true }).getByRole("menuitemradio", { name: "URL", exact: true }).click();
    await this.test.getByRole("textbox", { name: "NZB URL", exact: true }).fill(url);
    const duration = this.test.getByRole("spinbutton", { name: "Max duration", exact: true });
    await duration.fill("900");
    await duration.press("Tab");
    await expect(duration).toHaveValue("900");
  }

  // A bandwidth run against Cloudflare, `streams` streams of the largest
  // size it serves, allowed long enough to finish on a slow uplink.
  async useCloudflare(streams: number): Promise<void> {
    await this.test.getByRole("radio", { name: "Bandwidth", exact: true }).click();
    await expect(this.test.getByRole("radio", { name: "Bandwidth", exact: true })).toHaveAttribute("aria-checked", "true");
    await expect(this.test.getByRole("button", { name: "Provider", exact: true })).toContainText("Cloudflare");
    const count = this.test.getByRole("spinbutton", { name: "Streams", exact: true });
    await count.fill(String(streams));
    await count.press("Tab");
    await expect(count).toHaveValue(String(streams));
    const duration = this.test.getByRole("spinbutton", { name: "Max duration", exact: true });
    await duration.fill("900");
    await duration.press("Tab");
    await expect(duration).toHaveValue("900");
  }

  // Press Run and return the id the start answered with.
  async run(): Promise<number> {
    const started = this.page.waitForResponse(response =>
      new URL(response.url()).pathname.endsWith("/graphql") && (response.request().postData() ?? "").includes("startSpeedTest"));
    await this.test.getByRole("button", { name: "Run · pauses downloads", exact: true }).click();
    const body = await (await started).json() as { data?: { startSpeedTest?: { id: number; status: string } } };
    expect(body.data?.startSpeedTest?.status).toBe("RUNNING");
    return body.data!.startSpeedTest!.id;
  }

  row(id: number): Locator {
    return this.runs.getByRole("button").filter({ has: this.page.getByRole("checkbox", { name: `Compare run ${id}`, exact: true }) });
  }

  // Wait for run `id` to reach `status` in the Runs table.
  async finished(id: number, status: "Completed" | "Cancelled"): Promise<Locator> {
    const row = this.row(id);
    await expect(row.getByText(/^(Completed|Cancelled|Failed)$/)).toBeVisible({ timeout: 0 });
    await expect(row).toContainText(status);
    return row;
  }

  // Open run `id`'s detail and return it.
  async detail(id: number): Promise<Locator> {
    const row = this.row(id);
    await row.click();
    await expect(row).toHaveAttribute("aria-expanded", "true");
    return this.page.locator(`#speed-runs-detail-${id}`);
  }
}

// The Downloads page's queue control, in a tab of its own beside the speed page.
async function downloadsTab(page: Page): Promise<Locator> {
  const tab = await page.context().newPage();
  await tab.goto(weaverRoute("/"));
  return tab.getByRole("banner").getByRole("button", { name: /^(Pause all|Resume all)$/ });
}

const diagram = (detail: Locator) =>
  detail.getByRole("group", { name: "Egress interfaces connect through route legs and proxies to endpoints", exact: true });
const connectionRows = (detail: Locator) =>
  detail.getByRole("table", { name: "Connections", exact: true }).getByRole("row").filter({ has: detail.page().getByRole("cell") });

test("ST01 a Usenet run measures through the proxy rung, the same run with that rung down goes direct, and a job on the route moves the same bytes", async ({ cleanPage: page, request }) => {
  test.setTimeout(0);
  const parts = 32;
  const { a, connect1, server } = await ladderedServer();
  const nzb = await servedNzb(request, "st01-agreement", parts);
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(nzb.url);

  // Run 1: the saved route, on its proxy rung.
  const before = await bytesDownloaded(request);
  const mark1 = await fixtureMark(request);
  const first = await speed.run();
  const firstRow = await speed.finished(first, "Completed");
  const run1 = await speedTestRun(request, first);
  expect(run1).toMatchObject({ status: "COMPLETED", kind: "USENET", consumer: `server:${server}`, error: null });
  const result1 = run1.result!;
  expect(result1.averageBps).toBeGreaterThan(0);
  expect(result1.bytes).toBeGreaterThan(0);
  expect(result1.usenet).toMatchObject({ articlesRequested: parts, delivered: parts, missing: 0, failed: 0 });
  expect(result1.usenet!.decodedBytes).toBeGreaterThan(0);
  expect(result1.connections.length).toBeGreaterThan(0);
  for (const connection of result1.connections) {
    expect(connection.path, "every run-1 connection dials the proxy rung").toMatchObject({ egressId: a.id, rung: 0, proxyIds: [connect1.id] });
  }
  expect(connectedOn(await fixtureEvents(request, mark1), "connect1").length).toBeGreaterThan(0);

  const result = speed.test.getByRole("region", { name: "Result", exact: true });
  await expect(result.getByText("Downloaded", { exact: true })).toBeVisible();
  await expect(result.getByText("Decoded", { exact: true })).toBeVisible();
  await expect(firstRow).toContainText(PROXIED_HOST);
  await expect(firstRow).toContainText("Usenet · URL");
  await expect(firstRow).toContainText(rateLabel(result1.averageBps));
  await expect(firstRow).toContainText(sizeLabel(result1.bytes));
  await expect(firstRow).toContainText(sizeLabel(result1.usenet!.decodedBytes));
  const firstDetail = await speed.detail(first);
  await expect(connectionRows(firstDetail)).toHaveCount(result1.connections.length);
  for (const row of await connectionRows(firstDetail).all()) {
    await expect(row).toContainText(`${a.name} > ${connect1.name}`);
  }
  await expect(diagram(firstDetail).getByText("Down", { exact: true })).toHaveCount(0);

  // The run gave the queue back, and booked exactly its bytes.
  expect((await queueState(request)).isPaused).toBe(false);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  const afterRun1 = await expectBooked(request, before, result1.bytes, "run 1");

  // Run 2: the same test with the proxy rung taken down.
  const down = speed.route.getByRole("switch", { name: "Leg 1 rung 1 down", exact: true });
  await down.click();
  await expect(down).toHaveAttribute("aria-checked", "true");
  const mark2 = await fixtureMark(request);
  const second = await speed.run();
  const secondRow = await speed.finished(second, "Completed");
  const run2 = await speedTestRun(request, second);
  expect(run2).toMatchObject({ status: "COMPLETED", error: null });
  expect(run2.tested.overrides.downRungs).toEqual([{ leg: 0, rung: 0 }]);
  expect(run2.tested.legs).toMatchObject([{ position: 0, down: false }]);
  const result2 = run2.result!;
  expect(result2.usenet).toMatchObject({ articlesRequested: parts, delivered: parts });
  expect(result2.connections.length).toBeGreaterThan(0);
  for (const connection of result2.connections) {
    expect(connection.path.proxyIds, "no run-2 connection goes through the downed rung").toEqual([]);
    expect(connection.path.rung).not.toBe(0);
  }
  const events2 = await fixtureEvents(request, mark2);
  expect(connectedOn(events2, "connect1")).toEqual([]);
  expect(directEvents(events2).some(event => event.kind === "direct-nntp")).toBe(true);

  await expect(secondRow).toContainText("leg 1 rung 1");
  const secondDetail = await speed.detail(second);
  await expect(diagram(secondDetail).getByText("Down", { exact: true })).toHaveCount(1);
  await expect(connectionRows(secondDetail)).toHaveCount(result2.connections.length);
  for (const row of await connectionRows(secondDetail).all()) {
    await expect(row).toContainText(a.name);
    await expect(row).not.toContainText(connect1.name);
  }
  expect((await queueState(request)).isPaused).toBe(false);
  const afterRun2 = await expectBooked(request, afterRun1, result2.bytes, "run 2");

  // Ruling 6: the same NZB as a job on the same route moves run 1's bytes.
  const submitted = await submitProbeNzb(request, "st01-agreement", nzb.articles, {}, "single-multipart-file");
  expect(submitted).toMatchObject({ accepted: true });
  const job = submitted.jobId!;
  world.jobs.push(job);
  expect(await waitTerminal(request, job)).toBe("COMPLETED");
  await expectBooked(request, afterRun2, result1.bytes, "the job");
  type Contribution = { serverId: number; serverHost: string | null; articles: number; wireBytes: number };
  let credited: Contribution[] = [];
  await expect.poll(async () => {
    credited = (await graphql<{ jobDetailSnapshot: { serverAttribution: Contribution[] } }>(request,
      "query($id: Int!) { jobDetailSnapshot(jobId: $id) { serverAttribution { serverId serverHost articles wireBytes } } }", { id: job }))
      .jobDetailSnapshot.serverAttribution;
    return credited.reduce((sum, entry) => sum + entry.articles, 0);
  }, { timeout: 0, message: "every article of the job credited to its server" }).toBe(parts);
  expect(credited).toEqual([{ serverId: server, serverHost: PROXIED_HOST, articles: parts, wireBytes: result1.bytes }]);
  await page.goto(weaverRoute(`/jobs/${job}`));
  const providers = page.locator("#providers");
  await expect(providers).toContainText(PROXIED_HOST);
  await expect(providers).toContainText(sizeLabel(result1.bytes));
});

test("ST02 a run started while the queue is paused leaves it paused", async ({ cleanPage: page, request }) => {
  test.setTimeout(0);
  const { server } = await ladderedServer();
  const nzb = await servedNzb(request, "st02-paused", 16);
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  await queueControl.click();
  await expect(queueControl).toHaveText("Resume all", { timeout: 0 });
  await waitPaused(request, true, "queue paused from the Downloads page");

  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(nzb.url);
  const id = await speed.run();
  await speed.finished(id, "Completed");
  expect(await speedTestRun(request, id)).toMatchObject({ status: "COMPLETED", error: null });
  // The run did not pause the queue, so it does not resume it.
  expect((await queueState(request)).isPaused).toBe(true);
  await expect(queueControl).toHaveText("Resume all", { timeout: 0 });

  await queueControl.click();
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  await waitPaused(request, false, "queue resumed from the Downloads page");
});

test("ST03 cancelling a run ends it Cancelled and resumes the queue it paused", async ({ cleanPage: page, request }) => {
  test.setTimeout(0);
  const { server } = await ladderedServer();
  const nzb = await servedNzb(request, "st03-cancel", 64);
  // Slow every article body so the run is still measuring when it is cancelled.
  await world.holdChaosSession();
  await world.setChaos("slow_body=1500");
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });

  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(nzb.url);
  const id = await speed.run();
  const progress = speed.test.getByRole("region", { name: "Progress", exact: true });
  await expect(progress.getByText("Measuring", { exact: true })).toBeVisible({ timeout: 0 });
  // The run paused the queue to measure.
  await expect(queueControl).toHaveText("Resume all", { timeout: 0 });
  await progress.getByRole("button", { name: "Cancel test", exact: true }).click();

  await speed.finished(id, "Cancelled");
  await expect(speed.test.getByRole("region", { name: "Result", exact: true }).getByRole("status")).toHaveText("Cancelled");
  expect(await speedTestRun(request, id)).toMatchObject({ status: "CANCELLED", error: null });
  expect((await queueState(request)).isPaused).toBe(false);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
});

// The largest download Cloudflare serves in one request; a larger one is
// refused with 403, so the provider caps the size at it.
const CLOUDFLARE_MAX_BYTES = 99_999_999;
const CLOUDFLARE_HOST = "speed.cloudflare.com";

test("ST04 a bandwidth run against Cloudflare measures through the proxy rung and books nothing as downloaded", async ({ cleanPage: page, request }) => {
  test.setTimeout(0);
  const streams = 1;
  const { a, connect1, server } = await ladderedServer();
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useCloudflare(streams);

  const before = await bytesDownloaded(request);
  const mark = await fixtureMark(request);
  const id = await speed.run();
  const row = await speed.finished(id, "Completed");
  const run = await speedTestRun(request, id);
  expect(run).toMatchObject({ status: "COMPLETED", kind: "BANDWIDTH", consumer: `server:${server}`, error: null });
  const result = run.result!;
  expect(result.bandwidth).toMatchObject({ providerId: "cloudflare", streams });
  expect(result.bandwidth!.url).toContain(CLOUDFLARE_HOST);
  expect(result.usenet).toBeNull();
  expect(result.bytes).toBe(CLOUDFLARE_MAX_BYTES * streams);
  expect(result.averageBps).toBeGreaterThan(0);
  expect(result.connections.length).toBeGreaterThan(0);
  for (const connection of result.connections) {
    expect(connection.path, "every connection dials the proxy rung").toMatchObject({ egressId: a.id, rung: 0, proxyIds: [connect1.id] });
    expect(connection.error).toBeNull();
  }
  // The rung carried the TLS session to Cloudflare, asked for by name or by
  // the address the fixture resolved it to, and nothing went direct.
  const events = await fixtureEvents(request, mark);
  const carried = connectedOn(events, "connect1").filter(event => event.port === 443
    && (event.host === CLOUDFLARE_HOST || /^\d+\.\d+\.\d+\.\d+$/.test(event.host ?? "")));
  expect(carried.length).toBeGreaterThan(0);
  expect(directEvents(events)).toEqual([]);

  await expect(row).toContainText(PROXIED_HOST);
  await expect(row).toContainText("Cloudflare");
  await expect(row).toContainText(rateLabel(result.averageBps));
  await expect(row).toContainText(sizeLabel(result.bytes));
  const detail = await speed.detail(id);
  await expect(connectionRows(detail)).toHaveCount(result.connections.length);
  for (const connectionRow of await connectionRows(detail).all()) {
    await expect(connectionRow).toContainText(`${a.name} > ${connect1.name}`);
  }

  // The run gave the queue back and booked none of its bytes as downloaded.
  expect((await queueState(request)).isPaused).toBe(false);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  expect(await bytesDownloaded(request) - before).toBe(0);
});

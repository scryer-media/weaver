// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext, Locator, Page } from "@playwright/test";
import { expect, graphql, submitProbeNzb, test, weaverRoute } from "./helpers";
import { postProbeFile, waitTerminal } from "./support/downloads";
import { advanceClock, clockFile } from "./support/e2e-clock";
import { directLeg, ladderLeg, rung } from "./support/network-flow";
import { DIRECT_HOST, NetworkWorld, PROXIED_HOST } from "./support/network-scenario";
import { type FixtureEvent, directEvents, fixtureEvents, fixtureMark, fixtureState, setFixtureNzb } from "./support/proxy-fixture";
import { queueState, resumeAll, waitPaused } from "./support/schedules";

// The speed test (plan 169) driven from Networking -> Speed test: Usenet and
// bandwidth runs over laddered and split routes, the rung and leg overrides,
// the refusals, the queue gate, cancelling, the deadline, quotas, and a run
// of the seeded reference posting.
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
  id: number; consumer: string; consumerName: string; kind: string; status: string; phase: string | null;
  failure: { code: string; message: string } | null;
  tested: { legs: Array<{ position: number; down: boolean }>; overrides: { downRungs: Array<{ leg: number; rung: number }> } };
  result: {
    bytes: number; averageBps: number; peakBps: number;
    samples: Array<{ atMs: number; leg: number; bytes: number }>;
    connections: Array<{ leg: number; path: DialPath; bytes: number; error: string | null }>;
    usenet: { articlesRequested: number; delivered: number; missing: number; failed: number; declaredBytes: number; decodedBytes: number } | null;
    bandwidth: { providerId: string | null; url: string; connections: number } | null;
  } | null;
};

const RUN_FIELDS = `id consumer consumerName kind status phase failure { code message }
  tested { legs { position down } overrides { downRungs { leg rung } } }
  result { bytes averageBps peakBps samples { atMs leg bytes }
    connections { leg bytes error path { egressId rung poolId memberId proxyIds } }
    usenet { articlesRequested delivered missing failed declaredBytes decodedBytes }
    bandwidth { providerId url connections } }`;

// The sampler closes one bin per leg each second, the first and the final
// partial one included, and each sample's atMs is where its bin ends. A bin's
// width is read in whole milliseconds, so its true width is at most one
// millisecond more and the rate computed with that width is a floor under the
// rate the server saw. The run's peak must reach the first and the last bin.
function expectPeakCoversFirstAndLastBins(result: NonNullable<SpeedTestRun["result"]>, describe: string) {
  const bins = new Map<number, number>();
  for (const sample of result.samples) bins.set(sample.atMs, (bins.get(sample.atMs) ?? 0) + sample.bytes);
  const ends = [...bins.keys()].sort((a, b) => a - b);
  expect(ends.length, `${describe} closed at least one sample bin`).toBeGreaterThan(0);
  expect.soft([...bins.values()].reduce((sum, bytes) => sum + bytes, 0), `${describe} bins carry every byte`).toBe(result.bytes);
  const floorRate = (index: number) => bins.get(ends[index])! / ((ends[index] - (index === 0 ? 0 : ends[index - 1]) + 1) / 1000);
  expect(result.peakBps, `${describe} peak reaches the first bin`).toBeGreaterThanOrEqual(floorRate(0));
  expect(result.peakBps, `${describe} peak reaches the last bin`).toBeGreaterThanOrEqual(floorRate(ends.length - 1));
  expect(result.peakBps, `${describe} peak is no less than the average`).toBeGreaterThanOrEqual(result.averageBps);
}

async function speedTestRun(request: APIRequestContext, id: number): Promise<SpeedTestRun> {
  const run = (await graphql<{ speedTestRun: SpeedTestRun | null }>(request,
    `query($id: Int!) { speedTestRun(id: $id) { ${RUN_FIELDS} } }`, { id })).speedTestRun;
  expect(run, `speed test run ${id}`).not.toBeNull();
  return run!;
}

async function speedTestRuns(request: APIRequestContext): Promise<Array<{ id: number; status: string }>> {
  return (await graphql<{ speedTestRuns: Array<{ id: number; status: string }> }>(request, "query { speedTestRuns { id status } }")).speedTestRuns;
}

// Start a Usenet run of `nzbUrl` on `server` from the API and return the
// refusal it is answered with, as the page would show it.
async function startRefusal(request: APIRequestContext, server: number, nzbUrl: string): Promise<{ code: string; message: string }> {
  // The session is already established by the calls before this one.
  const response = await request.post(weaverRoute("/graphql"), { data: {
    query: "mutation($input: SpeedTestInput!) { startSpeedTest(input: $input) { id } }",
    variables: { input: { kind: "SERVER", id: server, usenet: { nzbUrl, maxDurationSeconds: 900 } } },
  } });
  const body = await response.json() as { errors?: Array<{ message: string; extensions?: { code?: string } }> };
  expect(body.errors ?? [], "one refusal").toHaveLength(1);
  return { code: body.errors![0].extensions?.code ?? "", message: body.errors![0].message };
}

// The server's download quota as the API reports it.
async function serverQuotaUsed(request: APIRequestContext, id: number): Promise<{ usedBytes: number; blocked: boolean }> {
  return (await graphql<{ server: { downloadQuota: { usedBytes: number; blocked: boolean } } }>(request,
    "query($id: Int!) { server(id: $id) { downloadQuota { usedBytes blocked } } }", { id })).server.downloadQuota;
}

const serverQuota = (limitBytes: number) => ({
  enabled: true, limitBytes, period: "ONE_TIME", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
});

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
// A label as text content when its number and unit are separate elements.
const figure = (label: string) => new RegExp(label.replace(/[.*+?^${}()|[\]\\/]/g, "\\$&").replace(" ", "\\s*"));

// One multipart file in the same layout submitProbeNzb writes, so the test NZB
// and the job name the same articles.
function probeNzb(name: string, articles: Array<{ messageId: string; bytes: number }>): string {
  const segments = articles.map(({ messageId, bytes }, index) => `<segment bytes="${bytes}" number="${index + 1}">${messageId}</segment>`).join("");
  return `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="weaver-e2e" date="1700000000" subject="${name}.bin"><groups><group>alt.binaries.test</group></groups><segments>${segments}</segments></file></nzb>`;
}

// Post the articles, serve their NZB from the proxy fixture's HTTP port, and
// return its URL. download.proxy.test resolves to the fixture both directly
// and through the fixture's proxies.
async function servedNzb(request: APIRequestContext, name: string, parts: number, partBytes?: number) {
  const articles = await postProbeFile(name, { count: parts, partBytes });
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
    this.test = page.getByRole("region", { name: "Speed test", exact: true });
    this.runs = page.getByRole("region", { name: "Runs", exact: true });
  }

  async open(server: number, host = PROXIED_HOST): Promise<void> {
    await this.page.goto(weaverRoute(`/settings/networking/speed?consumer=server:${server}`));
    await expect(this.test.getByRole("button", { name: "Route", exact: true })).toContainText(host);
    await expect(this.test.getByRole("radio", { name: "Bandwidth", exact: true })).toHaveAttribute("aria-checked", "true");
  }

  // A Usenet run of the NZB at `url`, allowed `maxDuration` seconds: by
  // default long enough to drain it.
  async useNzbUrl(url: string, maxDuration = 900): Promise<void> {
    await this.test.getByRole("radio", { name: "Usenet", exact: true }).click();
    await expect(this.test.getByRole("radio", { name: "Usenet", exact: true })).toHaveAttribute("aria-checked", "true");
    await this.test.getByRole("button", { name: "NZB", exact: true }).click();
    await this.page.getByRole("menu", { name: "NZB", exact: true }).getByRole("menuitemradio", { name: "URL", exact: true }).click();
    await this.test.getByRole("textbox", { name: "NZB URL", exact: true }).fill(url);
    const duration = this.test.getByRole("spinbutton", { name: "Max duration", exact: true });
    await duration.fill(String(maxDuration));
    await duration.press("Tab");
    await expect(duration).toHaveValue(String(maxDuration));
  }

  get runButton(): Locator {
    return this.test.getByRole("button", { name: "Run", exact: true });
  }

  // The Up/Down switch that takes leg `position` (1-based) down for the next run.
  async takeLegDown(position: number): Promise<void> {
    const down = this.route
      .getByRole("radiogroup", { name: `Leg ${position}`, exact: true })
      .getByRole("radio", { name: "Down", exact: true });
    await down.click();
    await expect(down).toHaveAttribute("aria-checked", "true");
  }

  // Press Run and read the one-line refusal it is answered with.
  async runRefused(message: string): Promise<void> {
    await this.runButton.click();
    await expect(this.test.getByRole("alert")).toHaveText(message);
  }

  // A bandwidth run against Cloudflare. The run picks its own connections
  // and length.
  async useCloudflare(): Promise<void> {
    await this.test.getByRole("radio", { name: "Bandwidth", exact: true }).click();
    await expect(this.test.getByRole("radio", { name: "Bandwidth", exact: true })).toHaveAttribute("aria-checked", "true");
    await expect(this.test.getByRole("button", { name: "Provider", exact: true })).toContainText("Cloudflare");
  }

  // Press Run and return the id the start answered with.
  async run(): Promise<number> {
    const started = this.page.waitForResponse(response =>
      new URL(response.url()).pathname.endsWith("/graphql") && (response.request().postData() ?? "").includes("startSpeedTest"));
    await this.runButton.click();
    const body = await (await started).json() as { data?: { startSpeedTest?: { id: number; status: string } } };
    expect(body.data?.startSpeedTest?.status).toBe("RUNNING");
    return body.data!.startSpeedTest!.id;
  }

  row(id: number): Locator {
    return this.runs.locator(`[data-row-id="${id}"]`);
  }

  // Wait for run `id` to reach `status` in the Runs table.
  async finished(id: number, status: "Completed" | "Cancelled" | "Failed"): Promise<Locator> {
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
  expect(run1).toMatchObject({ status: "COMPLETED", kind: "USENET", consumer: `server:${server}`, failure: null });
  const result1 = run1.result!;
  expect(result1.averageBps).toBeGreaterThan(0);
  expect(result1.bytes).toBeGreaterThan(0);
  expect(result1.usenet).toMatchObject({ articlesRequested: parts, delivered: parts, missing: 0, failed: 0 });
  expect(result1.usenet!.decodedBytes).toBeGreaterThan(0);
  expectPeakCoversFirstAndLastBins(result1, "run 1");
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
  const down = speed.route
    .getByRole("radiogroup", { name: "Leg 1 rung 1", exact: true })
    .getByRole("radio", { name: "Down", exact: true });
  await down.click();
  await expect(down).toHaveAttribute("aria-checked", "true");
  const mark2 = await fixtureMark(request);
  const second = await speed.run();
  const secondRow = await speed.finished(second, "Completed");
  const run2 = await speedTestRun(request, second);
  expect(run2).toMatchObject({ status: "COMPLETED", failure: null });
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
  expect(await speedTestRun(request, id)).toMatchObject({ status: "COMPLETED", failure: null });
  // The run did not pause the queue, so it does not resume it.
  expect((await queueState(request)).isPaused).toBe(true);
  await expect(queueControl).toHaveText("Resume all", { timeout: 0 });

  await queueControl.click();
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  await waitPaused(request, false, "queue resumed from the Downloads page");
});

test("ST03 cancelling a run ends it Cancelled and resumes the queue it paused", async ({ cleanPage: page, request }) => {
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
  // One run at a time: the page cannot start another, and a start from the
  // API is refused naming the one running.
  await expect(speed.runButton).toBeDisabled();
  expect(await startRefusal(request, server, nzb.url)).toEqual({
    code: "SPEED_TEST_ALREADY_RUNNING", message: `speed test ${id} is still running; cancel it or wait for it to finish`,
  });
  await progress.getByRole("button", { name: "Cancel test", exact: true }).click();

  await speed.finished(id, "Cancelled");
  await expect(speed.test.getByRole("region", { name: "Result", exact: true }).getByRole("status")).toHaveText("Cancelled");
  expect(await speedTestRun(request, id)).toMatchObject({ status: "CANCELLED", failure: null });
  expect((await queueState(request)).isPaused).toBe(false);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
});

const CLOUDFLARE_HOST = "speed.cloudflare.com";

test("ST04 a bandwidth run against Cloudflare measures through the proxy rung and books nothing as downloaded", async ({ cleanPage: page, request }) => {
  const { a, connect1, server } = await ladderedServer();
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useCloudflare();

  const before = await bytesDownloaded(request);
  const mark = await fixtureMark(request);
  const id = await speed.run();
  const row = await speed.finished(id, "Completed");
  const run = await speedTestRun(request, id);
  expect(run).toMatchObject({ status: "COMPLETED", kind: "BANDWIDTH", consumer: `server:${server}`, failure: null });
  const result = run.result!;
  expect(result.bandwidth).toMatchObject({ providerId: "cloudflare" });
  expect(result.bandwidth!.url).toContain(CLOUDFLARE_HOST);
  expect(result.bandwidth!.connections).toBeGreaterThanOrEqual(1);
  expect(result.usenet).toBeNull();
  expect(result.bytes).toBeGreaterThan(0);
  expect(result.averageBps).toBeGreaterThan(0);
  // One row per connection the run opened.
  expect(result.connections).toHaveLength(result.bandwidth!.connections);
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

type RunConnection = NonNullable<SpeedTestRun["result"]>["connections"][number];
const byLeg = (connections: RunConnection[], leg: number) => connections.filter(connection => connection.leg === leg);

test("ST05 a two-leg route splits the connections, a leg taken down gives them all to the other, both down is refused, and two runs compare", async ({ cleanPage: page, request }) => {
  const parts = 32;
  const a = await world.egress("a");
  const b = await world.egress("b");
  const connect1 = await world.connect("connect1");
  // Leg 1: egress-a through connect1. Leg 2: egress-b direct. Equal weights
  // and 4 connections, so the run splits them two and two.
  const server = await world.server({
    host: PROXIED_HOST, connections: 4,
    route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 50), directLeg(b.id, 50)] },
  });
  const nzb = await servedNzb(request, "st05-split", parts);
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(nzb.url);

  // Run 1: both legs, two connections each.
  const first = await speed.run();
  await speed.finished(first, "Completed");
  const run1 = await speedTestRun(request, first);
  expect(run1).toMatchObject({ status: "COMPLETED", failure: null });
  expect(run1.tested.legs).toMatchObject([{ position: 0, down: false }, { position: 1, down: false }]);
  const result1 = run1.result!;
  expect(result1.usenet).toMatchObject({ articlesRequested: parts, delivered: parts });
  expect(result1.connections).toHaveLength(4);
  const leg1 = byLeg(result1.connections, 0);
  const leg2 = byLeg(result1.connections, 1);
  expect(leg1).toHaveLength(2);
  expect(leg2).toHaveLength(2);
  for (const connection of leg1) expect(connection.path).toMatchObject({ egressId: a.id, rung: 0, proxyIds: [connect1.id] });
  for (const connection of leg2) expect(connection.path).toMatchObject({ egressId: b.id, proxyIds: [] });
  const firstDetail = await speed.detail(first);
  await expect(connectionRows(firstDetail)).toHaveCount(4);
  await expect(connectionRows(firstDetail).filter({ hasText: `${a.name} > ${connect1.name}` })).toHaveCount(2);
  await expect(connectionRows(firstDetail).filter({ hasText: b.name }).filter({ hasNotText: connect1.name })).toHaveCount(2);

  // Run 2: leg 2 taken down, so every connection is leg 1's.
  await speed.takeLegDown(2);
  const second = await speed.run();
  const secondRow = await speed.finished(second, "Completed");
  const run2 = await speedTestRun(request, second);
  expect(run2).toMatchObject({ status: "COMPLETED", failure: null });
  expect(run2.tested.legs).toMatchObject([{ position: 0, down: false }, { position: 1, down: true }]);
  const result2 = run2.result!;
  expect(result2.usenet).toMatchObject({ articlesRequested: parts, delivered: parts });
  expect(result2.connections).toHaveLength(4);
  for (const connection of result2.connections) {
    expect(connection).toMatchObject({ leg: 0, path: { egressId: a.id, rung: 0, proxyIds: [connect1.id] } });
  }
  await expect(secondRow).toContainText("leg 2");
  const secondDetail = await speed.detail(second);
  await expect(connectionRows(secondDetail)).toHaveCount(4);
  await expect(connectionRows(secondDetail).filter({ hasText: b.name })).toHaveCount(0);
  // The detail draws leg 2 down, and its direct way with it.
  await expect(diagram(secondDetail)).toContainText("Down · 0/0 open");
  await expect(diagram(secondDetail)).toContainText("Direct · Down");

  // Both legs down: the start is refused and nothing runs.
  const runsBefore = (await speedTestRuns(request)).length;
  await speed.takeLegDown(1);
  await speed.runRefused("Leave at least one leg up.");
  expect((await speedTestRuns(request)).length).toBe(runsBefore);
  expect((await queueState(request)).isPaused).toBe(false);

  // Compare mode: two runs ticked in the Runs table are shown side by side.
  await page.getByRole("button", { name: "Compare", exact: true }).click();
  await speed.runs.getByRole("checkbox", { name: `Compare run ${first}`, exact: true }).check();
  await speed.runs.getByRole("checkbox", { name: `Compare run ${second}`, exact: true }).check();
  const compare = page.getByRole("region", { name: "Compare", exact: true });
  await expect(compare).toBeVisible();
  // The compared results set the figure and its unit in separate elements.
  await expect(compare).toContainText(figure(rateLabel(result1.averageBps)));
  await expect(compare).toContainText(figure(rateLabel(result2.averageBps)));
  await expect(compare.getByRole("table", { name: "Connections", exact: true })).toHaveCount(2);
});

// The seeded reference posting, the shape of the public 100 MB speed test
// NZB, mounted into this container by the gate.
const FIXTURE_NZB = "/work/fixtures/speed-test-100mb/speed-test-100mb.nzb";

test("ST06 a Usenet run of the seeded 100 MB posting over a plaintext direct leg delivers every article and books its wire bytes", async ({ cleanPage: page, request }) => {
  const a = await world.egress("a");
  const server = await world.server({ host: DIRECT_HOST, connections: 4, route: { legs: [directLeg(a.id, 100)] } });
  const xml = fs.readFileSync(FIXTURE_NZB, "utf8");
  const declared = [...xml.matchAll(/<segment bytes="(\d+)"/g)].map(match => Number(match[1]));
  expect(declared.length).toBeGreaterThan(0);
  await setFixtureNzb(request, xml);
  const url = `http://download.proxy.test:${(await fixtureState(request)).ports.http}/probe.nzb`;
  const speed = new SpeedPage(page);
  await speed.open(server, DIRECT_HOST);
  await speed.useNzbUrl(url);

  const before = await bytesDownloaded(request);
  const id = await speed.run();
  const row = await speed.finished(id, "Completed");
  const run = await speedTestRun(request, id);
  expect(run).toMatchObject({ status: "COMPLETED", kind: "USENET", consumer: `server:${server}`, failure: null });
  const result = run.result!;
  expect(result.usenet).toMatchObject({
    articlesRequested: declared.length, delivered: declared.length, missing: 0, failed: 0,
    declaredBytes: declared.reduce((sum, bytes) => sum + bytes, 0),
  });
  // The posting carries the 100 MB file and its explanation inside RAR and
  // PAR2 volumes; the wire carries their yEnc encoding.
  expect(result.usenet!.decodedBytes).toBeGreaterThan(100_000_000);
  expect(result.bytes).toBeGreaterThan(result.usenet!.decodedBytes);
  expect(result.averageBps).toBeGreaterThan(0);
  expect(result.peakBps).toBeGreaterThanOrEqual(result.averageBps);
  expect(result.connections).toHaveLength(4);
  for (const connection of result.connections) {
    expect(connection.path).toMatchObject({ egressId: a.id, proxyIds: [] });
    expect(connection.error).toBeNull();
  }
  await expect(row).toContainText(DIRECT_HOST);
  await expect(row).toContainText(sizeLabel(result.bytes));
  const detail = await speed.detail(id);
  await expect(detail.getByText("Plaintext", { exact: true })).toBeVisible();
  await expect(connectionRows(detail)).toHaveCount(4);
  expect((await queueState(request)).isPaused).toBe(false);
  await expectBooked(request, before, result.bytes, "the fixture run");
});

test("ST07 a run whose NZB URL does not serve an NZB fails, says why, and leaves the queue running", async ({ cleanPage: page, request }) => {
  const { server } = await ladderedServer();
  const port = (await fixtureState(request)).ports.http;
  const queueControl = await downloadsTab(page);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(`http://download.proxy.test:${port}/not-an-nzb.nzb`);

  const before = await bytesDownloaded(request);
  const id = await speed.run();
  const row = await speed.finished(id, "Failed");
  const run = await speedTestRun(request, id);
  expect(run).toMatchObject({ status: "FAILED", kind: "USENET" });
  // A cause with no sentence of its own is shown in the daemon's words.
  expect(run.failure, "the failure names its cause").toMatchObject({ code: "OTHER" });
  expect(run.failure!.message, "the failure names its cause").toBeTruthy();
  const result = speed.test.getByRole("region", { name: "Result", exact: true });
  await expect(result.getByRole("alert")).toHaveText(`Failed · ${run.failure!.message}`);
  await expect(row).toContainText("Failed");
  expect((await queueState(request)).isPaused).toBe(false);
  await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
  expect(await bytesDownloaded(request) - before).toBe(0);
});

test("ST08 a run reaching its maximum duration completes with what it measured", async ({ cleanPage: page, request }) => {
  // Hold between articles, then advance the explicit E2E deadline only after
  // this run has delivered and accounted for data.
  const parts = 128;
  const maxDuration = 10;
  const { server } = await ladderedServer();
  const nzb = await servedNzb(request, "st08-deadline", parts, 16 * 1024);
  const clock = path.parse(clockFile());
  const hold = path.join(clock.dir, `${clock.name}.speed-test-hold`);
  fs.writeFileSync(hold, "");
  try {
    const queueControl = await downloadsTab(page);
    await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
    const speed = new SpeedPage(page);
    await speed.open(server);
    await speed.useNzbUrl(nzb.url, maxDuration);

    const before = await bytesDownloaded(request);
    const id = await speed.run();
    await expect.poll(async () => (await speedTestRun(request, id)).result?.usenet?.delivered ?? 0,
      { timeout: 0, message: "this run delivered an article before advancing its deadline" }).toBeGreaterThan(0);
    advanceClock(maxDuration * 1000);
    await speed.finished(id, "Completed");
    const run = await speedTestRun(request, id);
    expect(run).toMatchObject({ status: "COMPLETED", failure: null });
    const result = run.result!;
    expect(result.bytes).toBeGreaterThan(0);
    expect(result.usenet!.delivered).toBeGreaterThan(0);
    expect(result.usenet!.delivered, "the deadline ended the run before the NZB was drained").toBeLessThan(parts);
    expect(result.usenet!.articlesRequested).toBeLessThan(parts);
    expect(result.usenet!.missing).toBe(0);
    expect((await queueState(request)).isPaused).toBe(false);
    await expect(queueControl).toHaveText("Pause all", { timeout: 0 });
    // Only delivered articles are booked; the one cut off by the deadline is not.
    await expectBooked(request, before, result.bytes, "the deadline run");
  } finally {
    fs.rmSync(hold, { force: true });
  }
});

test("ST09 a run books against the server's download quota, stops at it, and is refused once it is used up", async ({ cleanPage: page, request }) => {
  const parts = 32;
  const { server } = await ladderedServer();
  const nzb = await servedNzb(request, "st09-quota", parts);
  // Room for half the articles as declared; the rest are refused a reservation.
  const limit = nzb.articles.slice(0, parts / 2).reduce((sum, article) => sum + article.bytes, 0);
  await world.updateServer(server, { downloadQuota: serverQuota(limit) });
  const speed = new SpeedPage(page);
  await speed.open(server);
  await speed.useNzbUrl(nzb.url);

  const before = await bytesDownloaded(request);
  const id = await speed.run();
  await speed.finished(id, "Completed");
  const run = await speedTestRun(request, id);
  expect(run).toMatchObject({ status: "COMPLETED", failure: null });
  const result = run.result!;
  expect(result.bytes).toBeGreaterThan(0);
  expect(result.usenet!.delivered).toBeGreaterThan(0);
  expect(result.usenet!.delivered, "the quota stopped the run short").toBeLessThan(parts);
  expect(result.connections.some(connection => connection.error?.includes("has used its download quota")),
    `a connection ended on the quota: ${JSON.stringify(result.connections)}`).toBe(true);
  const quota = await serverQuotaUsed(request, server);
  expect(quota.usedBytes, "the run's bytes are the quota's").toBe(result.bytes);
  await expectBooked(request, before, result.bytes, "the quota run");
  const detail = await speed.detail(id);
  await expect(detail.getByText("has used its download quota").first()).toBeVisible();

  // With the quota used up, a run is refused before it starts.
  await world.updateServer(server, { downloadQuota: serverQuota(quota.usedBytes) });
  await expect.poll(async () => (await serverQuotaUsed(request, server)).blocked, { timeout: 0, message: "quota blocked" }).toBe(true);
  const runsBefore = (await speedTestRuns(request)).length;
  await speed.runRefused(`${PROXIED_HOST} has used its download quota.`);
  expect((await speedTestRuns(request)).length).toBe(runsBefore);
  expect(await startRefusal(request, server, nzb.url)).toMatchObject({ code: "SPEED_TEST_QUOTA_BLOCKED" });
});

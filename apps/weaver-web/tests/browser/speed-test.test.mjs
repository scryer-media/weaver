import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-speed-test", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });
async function open(query, shown) {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1800 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/speed-test.html${query}`);
  await page.getByText(shown, { exact: true }).first().waitFor();
  return { page, errors };
}
/**
 * With `SPEED_TEST_SCREENSHOT_DIR` set to a directory, the page as it stands, or only `part` of it, is saved
 * there as `name`.png, once every transition under way has finished.
 */
async function shot(page, name, part) {
  if (process.env.SPEED_TEST_SCREENSHOT_DIR) {
    await page.waitForFunction(() => document.getAnimations().every((animation) => !(animation instanceof CSSTransition)));
    const path = join(process.env.SPEED_TEST_SCREENSHOT_DIR, `${name}.png`);
    await (part ? part.screenshot({ path }) : page.screenshot({ path, fullPage: true }));
  }
}
/** How far, at most, a chart's leading dot sits off its line as drawn, over the next sixty frames, in pixels. */
const offLine = (chart) =>
  chart.evaluate(async (element) => {
    const line = element.querySelector("polyline");
    const dot = element.querySelector("circle[r='3.5']");
    let worst = 0;
    for (let frame = 0; frame < 60; frame += 1) {
      await new Promise((resolve) => requestAnimationFrame(resolve));
      const box = dot.getBoundingClientRect();
      const [x, y] = [box.x + box.width / 2, box.y + box.height / 2];
      const matrix = line.getScreenCTM();
      const corners = Array.from(line.points, (point) => new DOMPoint(point.x, point.y).matrixTransform(matrix));
      const after = corners.findIndex((corner) => corner.x >= x);
      const [a, b] = after > 0 ? [corners[after - 1], corners[after]] : [corners.at(after), corners.at(after)];
      const drawn = b.x > a.x ? a.y + ((b.y - a.y) * (x - a.x)) / (b.x - a.x) : b.y;
      worst = Math.max(worst, Math.abs(drawn - y));
    }
    return worst;
  });
const sent = (page, name) => page.evaluate((name) => window.speedFixture.requests.filter((request) => request.name === name).map((request) => request.variables), name);
const finish = (page, id) => page.evaluate((id) => window.speedFixture.finish(id), id);
/** The stat cell under `eyebrow` in `scope`: its eyebrow, figure and unit as one string. */
const stat = (scope, eyebrow) => scope.getByText(eyebrow, { exact: true }).first();

test("a leg taken down is left out of the test, the run is followed to its result, and the result is kept beside the runs before it", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    const plan = page.locator("main");
    const legSwitch = plan.getByRole("switch", { name: "Leg 2 down", exact: true });
    await legSwitch.waitFor();
    assert.equal(await legSwitch.getAttribute("aria-checked"), "false");
    // Every box the route passes through has its switch: both egresses, both legs, the pool's rung and each of its members.
    for (const name of ["WAN Fiber down", "System down", "Leg 1 down", "Leg 1 rung 1 down", "Amsterdam down", "Frankfurt down"]) {
      await plan.getByRole("switch", { name, exact: true }).waitFor();
    }
    await legSwitch.click();
    await plan.getByRole("switch", { name: "Leg 2 down", checked: true, exact: true }).waitFor();

    // A server is tested over Usenet by default; a feed could not be. The route's flow ends at the server.
    await page.getByRole("radio", { name: "Usenet", checked: true, exact: true }).waitFor();
    await plan.locator("svg").getByText("Usenet · 100 MB", { exact: true }).waitFor();
    await page.getByRole("spinbutton", { name: "Max duration", exact: true }).waitFor();
    await page.getByRole("button", { name: "Run", exact: true }).click();
    const progress = page.getByRole("region", { name: "Progress", exact: true });
    await progress.getByText("Draining 3", { exact: true }).waitFor();
    await progress.getByText("Leg 1", { exact: true }).waitFor();
    assert.equal(await progress.getByText("Leg 2", { exact: true }).count(), 0);
    // Once it measures, the lines carrying a leg move and the endpoint says how fast.
    assert.equal(await plan.locator("svg animate").count(), 0);
    // Before it measures there is nothing to chart: the figures say so, and the charts are empty.
    await progress.getByText("—", { exact: true }).first().waitFor();
    await page.evaluate((id) => window.speedFixture.measure(id, 12), 2);
    await plan.locator("svg animate").first().waitFor({ state: "attached" });
    // The endpoint and the large figure both give the newest second's rate: 12 MiB on the one leg up.
    await plan.locator("svg").getByText("Active · 12.0 MB/s", { exact: true }).waitFor();
    // A Usenet run charts what it downloads and what that decodes to, each chasing its newest second.
    for (const name of ["Speed", "Decode"]) {
      const chart = progress.getByRole("img", { name, exact: true });
      await chart.locator("polyline").waitFor({ state: "attached" });
      // The leading dot rides the line as drawn on every frame while the chart glides on.
      assert.ok((await offLine(chart)) < 1, name);
      assert.ok((await chart.locator("polyline").getAttribute("points")).split(" ").length >= 12, name);
    }
    await progress.getByText("12.0", { exact: true }).waitFor();
    await shot(page, "speed-running");
    await shot(page, "speed-progress", progress);
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input, {
      kind: "SERVER", id: 1,
      overrides: { downLegs: [1], downRungs: [], downProxies: [], downEgresses: [] },
      usenet: { sabSize: "MB_100", connections: 20, maxDurationSeconds: 120 },
    });

    await finish(page, 2);
    const result = page.getByRole("region", { name: "Result", exact: true }).first();
    await result.waitFor();
    for (const eyebrow of ["Average", "Peak", "Downloaded", "Decoded"]) await stat(result, eyebrow).waitFor();
    await page.getByRole("button", { name: "Run", exact: true }).and(page.locator(":enabled")).waitFor();

    // The runs table holds this route's runs; the feed's run before it shows under All routes.
    const rows = page.locator("main").getByRole("button").filter({ hasText: "Usenet · 100 MB" });
    await rows.filter({ hasText: "Completed" }).waitFor();
    assert.equal(await page.locator("main").getByRole("button").filter({ hasText: "Feed mirror" }).count(), 0);
    await page.getByRole("checkbox", { name: "All routes", exact: true }).click();
    await page.locator("main").getByRole("button").filter({ hasText: "Feed mirror" }).waitFor();
    await rows.filter({ hasText: "leg 2" }).first().waitFor();

    // A run opens on its own detail, and two runs checked are compared side by side.
    await rows.first().click();
    await page.getByRole("table", { name: "Legs", exact: true }).first().waitFor();
    await page.getByRole("table", { name: "Articles", exact: true }).first().waitFor();
    await page.getByRole("checkbox", { name: "Compare run 1", exact: true }).click();
    await page.getByRole("checkbox", { name: "Compare run 2", exact: true }).click();
    await page.getByText("Compare", { exact: true }).waitFor();
    // Each figure one run beat the other on carries the margin; the Usenet run's decoded bytes have nothing to beat.
    const compare = page.getByRole("region", { name: "Compare", exact: true });
    for (const margin of ["+100%", "+150%", "+140%"]) await compare.getByText(margin, { exact: true }).waitFor();
    assert.equal(await compare.getByText(/^\+/).count(), 3);
    await shot(page, "speed-compare", compare);

    // Run again sends the route as it was measured, with the same leg down.
    await page.getByRole("button", { name: "Run again", exact: true }).first().click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const again = (await sent(page, "StartSpeedTest"))[1];
    assert.deepEqual(again.input.overrides.downLegs, [1]);
    assert.deepEqual(again.input.usenet, { sabSize: "MB_100", nzbUrl: null, connections: 20, maxDurationSeconds: 120 });
    assert.deepEqual(again.input.route.legs.map((leg) => leg.weight), [60, 40]);
    await page.getByRole("button", { name: "Cancel test", exact: true }).click();
    await page.waitForFunction(() => window.speedFixture.requests.some((request) => request.name === "CancelSpeedTest"));
    assert.deepEqual((await sent(page, "CancelSpeedTest"))[0], { id: 3 });
    // Cancelled before it measured, it has a status and no result.
    await page.locator("main").getByRole("button").filter({ hasText: "Cancelled" }).waitFor();

    // A run started elsewhere runs again exactly as the daemon kept its request.
    await page.getByRole("checkbox", { name: "Compare run 2", exact: true }).click();
    await rows.filter({ hasText: "leg 2" }).filter({ hasText: "Completed" }).click();
    await page.getByRole("table", { name: "Articles", exact: true }).waitFor({ state: "detached" });
    const feed = page.locator("main").getByRole("button").filter({ hasText: "Feed mirror" });
    await feed.click();
    // The run's flow ends at the host it measured against.
    await page.locator("main svg").getByText("speed.fixture.invalid", { exact: true }).waitFor();
    const runAgain = page.getByRole("button", { name: "Run again", exact: true });
    await runAgain.and(page.locator(":enabled")).waitFor();
    assert.equal(await runAgain.count(), 1);
    await runAgain.click();
    await page.waitForFunction(() => window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === 3);
    const stored = (await sent(page, "StartSpeedTest"))[2];
    assert.deepEqual(stored.input.bandwidth, { providerId: "cloudflare", server: null, customUrl: null });
    assert.equal(stored.input.kind, "RSS");
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("a feed is tested for bandwidth only, and a custom URL must be HTTPS before it can run", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    await page.getByRole("button", { name: "Route", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Feed mirror", exact: true }).click();
    await page.getByRole("radio", { name: "Bandwidth", checked: true, exact: true }).waitFor();
    assert.equal(await page.getByRole("radio", { name: "Usenet", exact: true }).isDisabled(), true);
    // A bandwidth test asks only what to measure against, and runs as it opens; the flow ends there.
    const flow = page.getByRole("group", { name: "Egress interfaces connect through route legs and proxies to endpoints", exact: true });
    await flow.getByText("Cloudflare", { exact: true }).waitFor();
    assert.equal(await page.getByRole("alert").count(), 0);
    for (const name of ["Size per stream", "Streams", "Max duration"]) {
      assert.equal(await page.getByLabel(name, { exact: true }).count(), 0, name);
    }
    await page.getByRole("button", { name: "Provider", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Custom URL", exact: true }).click();
    await page.getByRole("alert").filter({ hasText: "Enter an HTTPS URL." }).waitFor();
    const run = page.getByRole("button", { name: "Run", exact: true });
    assert.equal(await run.isDisabled(), true);
    await page.getByLabel("URL", { exact: true }).fill("https://files.fixture.invalid/blob");
    await flow.getByText("files.fixture.invalid", { exact: true }).waitFor();
    await run.and(page.locator(":enabled")).click();
    const progress = page.getByRole("region", { name: "Progress", exact: true });
    await progress.waitFor();
    // A bandwidth run has the one chart: there is nothing to decode.
    await page.evaluate(() => window.speedFixture.measure(2, 12));
    await progress.getByText("Measuring", { exact: true }).waitFor();
    await progress.getByRole("img", { name: "Speed", exact: true }).locator("polyline").waitFor({ state: "attached" });
    assert.equal(await progress.getByRole("img", { name: "Decode", exact: true }).count(), 0);
    await shot(page, "speed-progress-bandwidth", progress);
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input, {
      kind: "RSS", id: 1,
      overrides: { downLegs: [], downRungs: [], downProxies: [], downEgresses: [] },
      bandwidth: { customUrl: "https://files.fixture.invalid/blob" },
    });
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("the route editor tests the draft as it stands, unsaved", async () => {
  const { page, errors } = await open("?routes", "news.fixture.invalid");
  try {
    await page.getByRole("button", { name: "Edit route", exact: true }).first().click();
    const editor = page.getByRole("dialog", { name: "Route for news.fixture.invalid", exact: true });
    await editor.waitFor();
    await editor.getByRole("button", { name: "Speed test", exact: true }).click();
    const test = editor.getByRole("region", { name: "Speed test", exact: true });
    await test.getByRole("button", { name: "Run", exact: true }).click();
    await test.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const [start] = await sent(page, "StartSpeedTest");
    assert.equal(start.input.kind, "SERVER");
    assert.equal(start.input.id, 1);
    assert.deepEqual(start.input.route.legs.map((leg) => leg.weight), [60, 40]);
    assert.deepEqual(start.input.usenet, { sabSize: "MB_100", connections: 20, maxDurationSeconds: 120 });
    await finish(page, 2);
    const result = test.getByRole("region", { name: "Result", exact: true });
    await stat(result, "Decoded").waitFor();
    await shot(page, "speed-dialog");
    await shot(page, "speed-result", result);
    // Nothing was saved: the test ran the draft, not a saved route.
    assert.equal((await sent(page, "SaveRoute")).length, 0);
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("a Usenet test takes one .nzb file, sent with the start, and runs it again from the run without sending it twice", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    const plan = page.locator("main");
    const run = page.getByRole("button", { name: "Run", exact: true });
    await page.getByRole("button", { name: "NZB", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "File", exact: true }).click();
    // Until a .nzb file is chosen there is nothing to run.
    await page.getByRole("alert").filter({ hasText: "Choose an .nzb file." }).waitFor();
    assert.equal(await run.isDisabled(), true);
    const picker = page.locator('input[type="file"][aria-label="NZB file"]');
    assert.equal(await picker.getAttribute("accept"), ".nzb");
    assert.equal(await picker.getAttribute("multiple"), null);
    await picker.setInputFiles({ name: "notes.txt", mimeType: "text/plain", buffer: Buffer.from("not an nzb") });
    await page.getByText("notes.txt", { exact: true }).waitFor();
    await page.getByRole("alert").filter({ hasText: "Choose an .nzb file." }).waitFor();
    assert.equal(await run.isDisabled(), true);

    const nzb = '<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"></nzb>';
    await picker.setInputFiles({ name: "fixture.nzb", mimeType: "application/x-nzb", buffer: Buffer.from(nzb) });
    await page.getByText("fixture.nzb", { exact: true }).first().waitFor();
    // The route's flow ends at the server, measured with the file.
    await plan.locator("svg").getByText("Usenet · fixture.nzb", { exact: true }).waitFor();
    assert.equal(await page.getByRole("alert").count(), 0);
    await shot(page, "speed-nzb-file");
    await run.and(page.locator(":enabled")).click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();

    // The file goes up as the one part of a multipart start, where the input names it.
    const uploads = await page.evaluate(() => window.speedFixture.uploads);
    assert.deepEqual(uploads, [{ map: { 0: ["variables.input.usenet.nzbUpload"] }, name: "fixture.nzb", text: nzb }]);
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input.usenet, { nzbUpload: { upload: "fixture.nzb", text: nzb }, connections: 20, maxDurationSeconds: 120 });

    // Run again names the run, whose upload the daemon still holds, and sends no file.
    await finish(page, 2);
    const rows = plan.getByRole("button").filter({ hasText: "Usenet · fixture.nzb" });
    await rows.filter({ hasText: "Completed" }).click();
    await page.getByRole("button", { name: "Run again", exact: true }).first().click();
    await page.waitForFunction(() => window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === 2);
    const again = (await sent(page, "StartSpeedTest"))[1];
    assert.deepEqual(again.input.usenet, { nzbRunId: 2, connections: 20, maxDurationSeconds: 120 });
    assert.equal((await page.evaluate(() => window.speedFixture.uploads)).length, 1);
    await rows.filter({ hasText: "Running" }).waitFor();
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("while a job downloads, Run and Run again ask before pausing downloads, and Cancel starts nothing", async () => {
  const { page, errors } = await open("?downloading", "news.fixture.invalid");
  try {
    const confirm = page.getByRole("dialog", { name: "Pause downloads?", exact: true });
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await confirm.getByText("Downloads pause while the test runs and resume when it ends.", { exact: true }).waitFor();
    await shot(page, "speed-pause-confirm");
    // Nothing starts until the pause is agreed to.
    assert.equal((await sent(page, "StartSpeedTest")).length, 0);
    await confirm.getByRole("button", { name: "Cancel", exact: true }).click();
    await confirm.waitFor({ state: "detached" });
    assert.equal((await sent(page, "StartSpeedTest")).length, 0);

    await page.getByRole("button", { name: "Run", exact: true }).click();
    await confirm.getByRole("button", { name: "Pause and run", exact: true }).click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    assert.equal((await sent(page, "StartSpeedTest")).length, 1);

    // Run again asks the same way.
    await finish(page, 2);
    const rows = page.locator("main").getByRole("button").filter({ hasText: "Usenet · 100 MB" });
    await rows.filter({ hasText: "Completed" }).click();
    await page.getByRole("button", { name: "Run again", exact: true }).first().click();
    await confirm.getByRole("button", { name: "Pause and run", exact: true }).click();
    await page.waitForFunction(() => window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === 2);
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("with no job downloading, a rate on the header gauge does not make Run or Run again ask", async () => {
  const { page, errors } = await open("?gauge", "news.fixture.invalid");
  try {
    const confirm = page.getByRole("dialog", { name: "Pause downloads?", exact: true });
    const started = (count) =>
      page.waitForFunction(
        (count) => document.querySelector('[role="dialog"]') !== null
          || window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === count,
        count,
      );
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await started(1);
    assert.equal(await confirm.count(), 0);
    assert.equal((await sent(page, "StartSpeedTest")).length, 1);

    await finish(page, 2);
    const rows = page.locator("main").getByRole("button").filter({ hasText: "Usenet · 100 MB" });
    await rows.filter({ hasText: "Completed" }).click();
    await page.getByRole("button", { name: "Run again", exact: true }).first().click();
    await started(2);
    assert.equal(await confirm.count(), 0);
    assert.equal((await sent(page, "StartSpeedTest")).length, 2);
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

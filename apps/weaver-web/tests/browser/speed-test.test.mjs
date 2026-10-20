// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
    const legSwitch = plan.getByRole("radiogroup", { name: "Leg 2", exact: true });
    await legSwitch.waitFor();
    // The route's one earlier run is listed, and there is nothing to compare it with yet.
    const runRow = (id) => page.locator(`main [data-row-id="${id}"]`);
    await runRow(1).waitFor();
    assert.equal(await page.getByRole("button", { name: "Compare", exact: true }).count(), 0);
    // Every box the route passes through has its Up/Down switch, Up until it is turned: both egresses, both legs, the pool's rung and each of its members.
    for (const name of ["WAN Fiber", "System", "Leg 1", "Leg 2", "Leg 1 rung 1", "Amsterdam", "Frankfurt"]) {
      const choices = plan.getByRole("radiogroup", { name, exact: true }).getByRole("radio");
      assert.deepEqual(await choices.allTextContents(), ["Up", "Down"], name);
      assert.deepEqual(await choices.evaluateAll((all) => all.map((one) => one.getAttribute("aria-checked"))), ["true", "false"], name);
    }
    const [up, down] = [legSwitch.getByRole("radio", { name: "Up", exact: true }), legSwitch.getByRole("radio", { name: "Down", exact: true })];
    // Up is green and Down red, each only while it is the one chosen.
    const fill = (radio) => radio.locator("rect").first().getAttribute("fill");
    const [upOn, downOff] = [await fill(up), await fill(down)];
    await down.click();
    await legSwitch.getByRole("radio", { name: "Down", checked: true, exact: true }).waitFor();
    const [upOff, downOn] = [await fill(up), await fill(down)];
    assert.equal(upOff, downOff);
    assert.notEqual(upOn, upOff);
    assert.notEqual(downOn, downOff);
    assert.notEqual(upOn, downOn);
    // A click leaves no focus ring behind.
    assert.equal(await down.evaluate((radio) => radio === document.activeElement), false);
    // The keyboard moves between the two and shows where it is.
    await down.focus();
    await page.keyboard.press("ArrowLeft");
    await up.and(page.locator(":focus-visible")).waitFor();
    await legSwitch.getByRole("radio", { name: "Up", checked: true, exact: true }).waitFor();
    await page.keyboard.press("ArrowRight");
    await legSwitch.getByRole("radio", { name: "Down", checked: true, exact: true }).waitFor();
    await shot(page, "speed-switches", plan.locator("svg").first());

    // A run measures bandwidth until Usenet is picked; a feed could not pick it. The route's flow ends at the server.
    await page.getByRole("radio", { name: "Bandwidth", checked: true, exact: true }).waitFor();
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
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
    // The pool member the leg's connection went through says so, with what it carries now; the other stands by.
    const members = plan.locator("svg").getByRole("group", { name: "Europe pool members", exact: true });
    await members.getByText("Frankfurt · Active", { exact: true }).waitFor();
    await members.getByText("1 open · 12.00 MiB/s", { exact: true }).waitFor();
    await members.getByText("Amsterdam · Standby", { exact: true }).waitFor();
    assert.equal(await plan.locator("svg").getByText(/Unmeasured/).count(), 0);
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

    // The runs table holds this route's runs: the new one beside the one before it.
    const rows = page.locator("main").getByRole("button").filter({ hasText: "Usenet · 100 MB" });
    await rows.filter({ hasText: "Completed" }).waitFor();
    await runRow(1).waitFor();
    await rows.filter({ hasText: "leg 2" }).first().waitFor();

    // A run opens on its own detail; until Compare is pressed a row has no box to check.
    await rows.first().click();
    await page.getByRole("table", { name: "Legs", exact: true }).first().waitFor();
    await page.getByRole("table", { name: "Articles", exact: true }).first().waitFor();
    assert.equal(await page.getByRole("checkbox", { name: "Compare run 2", exact: true }).count(), 0);
    // Compare closes every open row and holds two sides open, each asking for a result.
    await page.getByRole("button", { name: "Compare", exact: true }).click();
    await page.getByRole("button", { name: "Stop", exact: true }).waitFor();
    await page.getByRole("table", { name: "Articles", exact: true }).waitFor({ state: "detached" });
    const compare = page.getByRole("region", { name: "Compare", exact: true });
    const hint = "Pick a result from above";
    assert.equal(await compare.getByText(hint, { exact: true }).count(), 2);
    await shot(page, "speed-compare-empty");
    // The whole row is the box's target, and a row no longer opens: the first pick takes the left side, the next the right.
    await runRow(2).click();
    await page.getByRole("checkbox", { name: "Compare run 2", checked: true, exact: true }).waitFor();
    assert.equal(await runRow(2).getAttribute("aria-expanded"), null);
    assert.deepEqual(await compare.locator(":scope > *").evaluateAll((sides) => sides.map((side) => side.textContent === "Pick a result from above")), [false, false, true]);
    await page.getByRole("checkbox", { name: "Compare run 1", exact: true }).click();
    await page.getByRole("checkbox", { name: "Compare run 1", checked: true, exact: true }).waitFor();
    assert.equal(await compare.getByText(hint, { exact: true }).count(), 0);
    // Each figure one run beat the other on carries the margin; the Usenet run's decoded bytes have nothing to beat.
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

    // Stop puts the boxes away and the rows open again.
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    await compare.waitFor({ state: "detached" });
    assert.equal(await page.getByRole("checkbox", { name: "Compare run 2", exact: true }).count(), 0);
    // A run from before the page opened runs again exactly as the daemon kept its request.
    await runRow(1).click();
    // The run's flow ends at the host it measured against.
    await page.locator("main svg").getByText("speed.fixture.invalid", { exact: true }).waitFor();
    // A bandwidth result is its rates; how much it downloaded is not one of its figures.
    const feedResult = page.getByRole("region", { name: "Result", exact: true }).filter({ has: stat(page, "Peak") }).last();
    await stat(feedResult, "Average").waitFor();
    assert.equal(await feedResult.getByText("Downloaded", { exact: true }).count(), 0);
    const runAgain = page.getByRole("button", { name: "Run again", exact: true });
    await runAgain.and(page.locator(":enabled")).waitFor();
    assert.equal(await runAgain.count(), 1);
    await runAgain.click();
    await page.waitForFunction(() => window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === 3);
    const stored = (await sent(page, "StartSpeedTest"))[2];
    assert.deepEqual(stored.input.bandwidth, { providerId: "cloudflare", server: null, customUrl: null, uploadMib: null });
    assert.equal(stored.input.kind, "SERVER");
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
    // The server's run stays with the server: the feed has none of its own.
    await page.locator("main").getByText("No runs yet.", { exact: true }).waitFor();
    assert.equal(await page.locator('main [data-row-id="1"]').count(), 0);
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
    // The URL box keeps a bounded width rather than spanning the page.
    assert.ok((await page.getByLabel("URL", { exact: true }).boundingBox()).width <= 480);
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
    assert.equal(await progress.getByText("Downloaded", { exact: true }).count(), 0);
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

test("picking another route keeps the test as it was set, and a feed takes it as a bandwidth test", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    const route = async (name) => {
      await page.getByRole("button", { name: "Route", exact: true }).click();
      await page.getByRole("menuitemradio", { name, exact: true }).click();
    };
    const connections = page.getByRole("spinbutton", { name: "Connections", exact: true });
    const duration = page.getByRole("spinbutton", { name: "Max duration", exact: true });
    // The route is picked in the Speed test block, above the test it runs.
    await page.getByRole("region", { name: "Speed test", exact: true }).getByRole("button", { name: "Route", exact: true }).waitFor();
    assert.equal(await page.getByRole("region", { name: "Route", exact: true }).getByRole("button", { name: "Route", exact: true }).count(), 0);
    await page.getByRole("button", { name: "Provider", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Custom URL", exact: true }).click();
    await page.getByLabel("URL", { exact: true }).fill("https://files.fixture.invalid/blob");
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
    await page.getByRole("button", { name: "NZB", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "1 GB", exact: true }).click();
    await connections.fill("7");
    await connections.press("Tab");
    await duration.fill("300");
    await duration.press("Tab");

    // A feed has no Usenet test: the test turns to bandwidth, as it was set.
    await route("Feed mirror");
    await page.getByRole("radio", { name: "Bandwidth", checked: true, exact: true }).waitFor();
    assert.equal(await page.getByLabel("URL", { exact: true }).inputValue(), "https://files.fixture.invalid/blob");

    // Back on the server, the Usenet test is as it was left.
    await route("news.fixture.invalid");
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
    assert.match(await page.getByRole("button", { name: "NZB", exact: true }).innerText(), /1 GB/);
    assert.equal(await connections.inputValue(), "7");
    assert.equal(await duration.inputValue(), "300");
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("the route editor has no speed test of its own: the speed test page runs every test", async () => {
  const { page, errors } = await open("?routes", "news.fixture.invalid");
  try {
    await page.locator('main [data-row-id="server:1"]').getByRole("button", { name: "Edit", exact: true }).click();
    const editor = page.getByRole("dialog", { name: "Route for news.fixture.invalid", exact: true });
    await editor.waitFor();
    // A server's route splits its connections, and says so above the legs and on each one.
    await editor.getByText(/^Split up to 20 connections/).waitFor();
    await editor.getByText(/^Target: /).first().waitFor();
    assert.equal(await editor.getByRole("button", { name: "Speed test", exact: true }).count(), 0);
    assert.equal(await editor.getByRole("button", { name: "Run", exact: true }).count(), 0);
    assert.equal(await editor.getByRole("region", { name: "Speed test", exact: true }).count(), 0);
    await shot(page, "speed-dialog");
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("Default outbound's route comes first and is edited with nothing said about connections", async () => {
  const { page, errors } = await open("?routes", "Default outbound");
  try {
    // Its name says what it is, and one leg splits nothing, so its summary is empty.
    assert.equal(await page.locator("main [data-row-id]").first().getAttribute("data-row-id"), "outbound");
    const row = page.locator('main [data-row-id="outbound"]');
    await row.getByRole("button", { name: "Edit", exact: true }).click();
    assert.equal(await row.getByText(/\bleg/).count(), 0);
    const editor = page.getByRole("dialog", { name: "Route for Default outbound", exact: true });
    await editor.getByRole("button", { name: "Egress", exact: true }).waitFor();
    // It splits nothing, so it has one leg, shown as the route itself: no leg
    // box or number, no word above it, no target, no weight, preview or share
    // to hold, and no leg to add, move or remove.
    assert.equal(await editor.getByRole("group", { name: /^Leg / }).count(), 0);
    assert.equal(await editor.getByText(/^Leg \d|\bleg\b/).count(), 0);
    assert.equal(await editor.getByText(/^Split up to|^Feeds try/).count(), 0);
    assert.equal(await editor.getByText(/^Target: /).count(), 0);
    assert.equal(await editor.getByRole("slider").count(), 0);
    assert.equal(await editor.getByLabel("Weight", { exact: true }).count(), 0);
    assert.equal(await editor.getByText("Simulate leg down", { exact: true }).count(), 0);
    assert.equal(await editor.getByText("When a leg goes down", { exact: true }).count(), 0);
    assert.equal(await editor.getByRole("button", { name: /^(Add leg|Remove leg)$/ }).count(), 0);
    await shot(page, "speed-outbound-dialog");
    await editor.getByRole("button", { name: "Save route", exact: true }).click();
    await editor.waitFor({ state: "detached" });
    const [saved] = await sent(page, "SaveNetworkRoute");
    assert.equal(saved.kind, "OUTBOUND");
    assert.equal(saved.id, 0);
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

test("Default outbound is tested for bandwidth only, and its run is named for it", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    await page.getByRole("button", { name: "Route", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Default outbound", exact: true }).click();
    await page.getByRole("radio", { name: "Bandwidth", checked: true, exact: true }).waitFor();
    assert.equal(await page.getByRole("radio", { name: "Usenet", exact: true }).isDisabled(), true);
    await page.getByRole("button", { name: "Run", exact: true }).and(page.locator(":enabled")).click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input, {
      kind: "OUTBOUND", id: 0,
      overrides: { downLegs: [], downRungs: [], downProxies: [], downEgresses: [] },
      bandwidth: { providerId: "cloudflare" },
    });
    await page.locator('main [data-row-id="2"]').filter({ hasText: "Default outbound" }).waitFor();
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
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
    // However wide the page, the source keeps its own width and Connections follows it one gap on.
    const connections = page.getByRole("spinbutton", { name: "Connections", exact: true });
    const cells = async (control) =>
      page.evaluate(([source, after]) => {
        let row = source.parentElement;
        while (!row.contains(after)) row = row.parentElement;
        const cell = (element) => [...row.children].find((child) => child.contains(element)).getBoundingClientRect();
        return { width: Math.round(cell(source).width), gap: Math.round(cell(after).left - cell(source).right) };
      }, [await control.elementHandle(), await connections.elementHandle()]);
    await page.getByRole("button", { name: "NZB", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "URL", exact: true }).click();
    assert.deepEqual(await cells(page.getByRole("textbox", { name: "NZB URL", exact: true })), { width: 480, gap: 12 });
    await shot(page, "speed-nzb-url");
    await page.getByRole("button", { name: "NZB", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "File", exact: true }).click();
    assert.equal((await cells(page.getByRole("button", { name: "Choose file", exact: true }))).gap, 12);
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
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
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
    await page.getByRole("radio", { name: "Usenet", exact: true }).click();
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

test("a bandwidth run the provider rate limits says so in our words, with the daemon's own on hover", async () => {
  const { page, errors } = await open("", "news.fixture.invalid");
  try {
    await page.getByRole("button", { name: "Route", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Feed mirror", exact: true }).click();
    await page.getByRole("radio", { name: "Bandwidth", checked: true, exact: true }).waitFor();
    await page.getByRole("button", { name: "Run", exact: true }).and(page.locator(":enabled")).click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const message = "GET https://speed.cloudflare.com/__down?bytes=99999999 answered HTTP 429 Too Many Requests";
    await page.evaluate(
      (message) => window.speedFixture.fail(2, { code: "RATE_LIMITED", message, params: { url: "https://speed.cloudflare.com/__down?bytes=99999999" } }),
      message,
    );
    // The line is our sentence alone; the URL and status the daemon gave are its title.
    const sentence = "Failed · The provider is rate limiting your IP right now; choose another provider.";
    const failed = page.getByRole("region", { name: "Result", exact: true }).getByRole("alert").filter({ hasText: sentence }).first();
    await failed.waitFor();
    assert.equal(await failed.textContent(), sentence);
    assert.equal(await failed.getAttribute("title"), message);
    await shot(page, "speed-rate-limited");
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

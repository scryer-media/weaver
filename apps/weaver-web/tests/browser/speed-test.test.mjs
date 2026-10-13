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
/** With `SPEED_TEST_SCREENSHOT_DIR` set to a directory, the page as it stands is saved there as `name`.png. */
async function shot(page, name) {
  if (process.env.SPEED_TEST_SCREENSHOT_DIR) {
    await page.screenshot({ path: join(process.env.SPEED_TEST_SCREENSHOT_DIR, `${name}.png`), fullPage: true });
  }
}
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

    // A server is tested over Usenet by default; a feed could not be.
    await page.getByRole("radio", { name: "Usenet", checked: true, exact: true }).waitFor();
    await page.getByRole("button", { name: "Run · pauses downloads", exact: true }).click();
    const progress = page.getByRole("region", { name: "Progress", exact: true });
    await progress.getByText("Draining 3", { exact: true }).waitFor();
    await progress.getByText("Leg 1", { exact: true }).waitFor();
    assert.equal(await progress.getByText("Leg 2", { exact: true }).count(), 0);
    await shot(page, "speed-running");
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input, {
      kind: "SERVER", id: 1,
      overrides: { downLegs: [1], downRungs: [], downProxies: [], downEgresses: [] },
      maxDurationSeconds: 120,
      usenet: { sabSize: "MB_100", connections: 20 },
    });

    await finish(page, 2);
    const result = page.getByRole("region", { name: "Result", exact: true }).first();
    await result.waitFor();
    for (const eyebrow of ["Average", "Peak", "Downloaded", "Decoded"]) await stat(result, eyebrow).waitFor();
    await page.getByRole("button", { name: "Run · pauses downloads", exact: true }).and(page.locator(":enabled")).waitFor();

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
    await shot(page, "speed-compare");

    // Run again sends the route as it was measured, with the same leg down.
    await page.getByRole("button", { name: "Run again", exact: true }).first().click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const again = (await sent(page, "StartSpeedTest"))[1];
    assert.deepEqual(again.input.overrides.downLegs, [1]);
    assert.deepEqual(again.input.usenet, { sabSize: "MB_100", nzbUrl: null, connections: 20 });
    assert.equal(again.input.maxDurationSeconds, 120);
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
    const requested = page.getByLabel("Requested", { exact: true }).first();
    await requested.getByText(/^Total /).waitFor();
    assert.match(await requested.textContent(), /Streams 4/);
    const runAgain = page.getByRole("button", { name: "Run again", exact: true });
    await runAgain.and(page.locator(":enabled")).waitFor();
    assert.equal(await runAgain.count(), 1);
    await runAgain.click();
    await page.waitForFunction(() => window.speedFixture.requests.filter((request) => request.name === "StartSpeedTest").length === 3);
    const stored = (await sent(page, "StartSpeedTest"))[2];
    assert.deepEqual(stored.input.bandwidth, { providerId: "cloudflare", server: null, customUrl: null, bytes: 99999999, streams: 4 });
    assert.equal(stored.input.kind, "RSS");
    assert.equal(stored.input.maxDurationSeconds, 60);
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
    // Cloudflare is capped below 100 MiB, so its one size is the cap, and the form runs as it opens.
    await page.getByRole("button", { name: "Size per stream", exact: true }).click();
    assert.equal(await page.getByRole("menuitemradio").count(), 1);
    await page.keyboard.press("Escape");
    assert.equal(await page.getByRole("alert").count(), 0);
    await page.getByRole("button", { name: "Provider", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "LibreSpeed", exact: true }).click();
    await page.getByRole("button", { name: "Size per stream", exact: true }).click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["100 MB", "1.0 GB"]);
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Provider", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Custom URL", exact: true }).click();
    await page.getByRole("alert").filter({ hasText: "Enter an HTTPS URL." }).waitFor();
    const run = page.getByRole("button", { name: "Run · pauses downloads", exact: true });
    assert.equal(await run.isDisabled(), true);
    // Each stream asks for the size, so the total is the two multiplied.
    await page.getByRole("button", { name: "Size per stream", exact: true }).waitFor();
    assert.match(await page.getByRole("status", { name: "Total", exact: true }).textContent(), /^400(\.0)? M/);
    await page.getByLabel("URL", { exact: true }).fill("https://files.fixture.invalid/blob");
    await run.and(page.locator(":enabled")).click();
    await page.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const [start] = await sent(page, "StartSpeedTest");
    assert.deepEqual(start.input, {
      kind: "RSS", id: 1,
      overrides: { downLegs: [], downRungs: [], downProxies: [], downEgresses: [] },
      maxDurationSeconds: 120,
      bandwidth: { customUrl: "https://files.fixture.invalid/blob", bytes: 104857600, streams: 4 },
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
    await test.getByRole("button", { name: "Run · pauses downloads", exact: true }).click();
    await test.getByRole("region", { name: "Progress", exact: true }).waitFor();
    const [start] = await sent(page, "StartSpeedTest");
    assert.equal(start.input.kind, "SERVER");
    assert.equal(start.input.id, 1);
    assert.deepEqual(start.input.route.legs.map((leg) => leg.weight), [60, 40]);
    assert.deepEqual(start.input.usenet, { sabSize: "MB_100", connections: 20 });
    await finish(page, 2);
    const result = test.getByRole("region", { name: "Result", exact: true });
    await stat(result, "Decoded").waitFor();
    await shot(page, "speed-dialog");
    // Nothing was saved: the test ran the draft, not a saved route.
    assert.equal((await sent(page, "SaveRoute")).length, 0);
    assert.equal(errors.length, 0, errors.join("\n"));
  } finally {
    await page.close();
  }
});

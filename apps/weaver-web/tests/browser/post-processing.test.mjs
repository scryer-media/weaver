import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-post-processing", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });
async function open(query = "") {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  page.setDefaultTimeout(0);
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/post-processing.html${query}`);
  return page;
}
// What the fixture's daemon holds, read the way the panel reads it.
const stored = (page) => page.evaluate(() => fetch("/graphql", {
  method: "POST", body: JSON.stringify({ operationName: "PostProcessingSettings", variables: {} }),
}).then((response) => response.json()).then((payload) => payload.data.postProcessingSettings));

test("event script limits are a section of the panel and save with the rest of it", async () => {
  const page = await open();
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    await events.getByText("queue events run one at a time", { exact: true }).waitFor();
    const limit = (name) => events.getByRole("spinbutton", { name, exact: true });
    for (const [name, value] of [
      ["Concurrent event scripts", "1"], ["Default event timeout", "300"], ["File event interval", "0"],
      ["Captured output per run", "1048576"], ["Retained runs per job", "32"],
      ["Compressed output budget", "67108864"], ["Compressed output cap per run", "2097152"],
    ]) assert.equal(await limit(name).inputValue(), value);
    // The execution limit keeps a name of its own beside the event one.
    assert.equal(await page.getByRole("spinbutton", { name: "Concurrent scripts", exact: true }).count(), 1);
    const save = page.getByRole("button", { name: "Save changes", exact: true });
    assert.equal(await save.isDisabled(), true);
    // A number field commits what was typed when focus leaves it, held to the range the daemon accepts.
    await limit("File event interval").fill("-1");
    await limit("File event interval").blur();
    await limit("Concurrent event scripts").fill("12");
    await limit("Concurrent event scripts").blur();
    await save.click();
    await page.getByRole("contentinfo").getByText("Saved", { exact: true }).waitFor();
    const settings = await stored(page);
    assert.equal(settings.fileDownloadedEventInterval, -1);
    assert.equal(settings.eventScriptConcurrency, 8);
    assert.equal(await limit("Concurrent event scripts").inputValue(), "8");
  } finally { await page.close(); }
});

test("the panel search reaches an event script limit", async () => {
  const page = await open("?search=retained");
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    await events.getByRole("spinbutton", { name: "Retained runs per job", exact: true }).waitFor();
    assert.equal(await events.getByRole("spinbutton").count(), 1);
    assert.equal(await page.getByRole("region").count(), 1);
  } finally { await page.close(); }
});

const runSwitch = (runList, script) => runList.getByRole("switch", { name: `Run ${script}`, exact: true });

test("a run list row carries its timeout, its switch and its order controls", async () => {
  const page = await open();
  try {
    const runList = page.getByRole("region", { name: "Run list", exact: true });
    await runList.getByRole("switch").first().and(runSwitch(runList, "notify.py")).waitFor();
    const timeout = (script) => runList.getByRole("spinbutton", { name: `Timeout for ${script}`, exact: true });
    assert.equal(await timeout("notify.py").inputValue(), "0");
    assert.equal(await timeout("cleanup.sh").inputValue(), "600");
    assert.equal(await runSwitch(runList, "notify.py").isChecked(), true);
    assert.equal(await runSwitch(runList, "cleanup.sh").isChecked(), false);
    const control = (name) => runList.getByRole("button", { name, exact: true });
    for (const name of ["Move up", "Move down", "Remove"]) assert.equal(await control(name).count(), 3);
    assert.equal(await control("Move up").first().isDisabled(), true);
    assert.equal(await control("Move down").last().isDisabled(), true);
    await runList.getByText("missing", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("a run list control shows its edit at once and saves it", async () => {
  // The panel saves a run-list edit behind what it shows, so each edit gets a
  // page of its own: no earlier save or refetch is in flight when it is made.
  for (const [edit, saved] of [
    [async (runList) => {
      const timeout = runList.getByRole("spinbutton", { name: "Timeout for notify.py", exact: true });
      await timeout.fill("90");
      await timeout.blur();
    }, (list) => assert.deepEqual(list[0], { script: "notify.py", enabled: true, timeoutSeconds: 90 })],
    [async (runList) => {
      await runList.getByRole("button", { name: "Move down", exact: true }).first().click();
      await runList.getByRole("switch").first().and(runSwitch(runList, "cleanup.sh")).waitFor();
    }, (list) => assert.deepEqual(list.map((entry) => entry.script), ["cleanup.sh", "notify.py", "retired.py"])],
    [async (runList) => {
      await runSwitch(runList, "cleanup.sh").click();
      await runList.locator('[role="switch"][aria-checked="true"]').and(runSwitch(runList, "cleanup.sh")).waitFor();
    }, (list) => assert.deepEqual(list[1], { script: "cleanup.sh", enabled: true, timeoutSeconds: 600 })],
    [async (runList) => {
      await runList.getByRole("button", { name: "Remove", exact: true }).last().click();
      await runSwitch(runList, "retired.py").waitFor({ state: "detached" });
    }, (list) => assert.deepEqual(list.map((entry) => entry.script), ["notify.py", "cleanup.sh"])],
  ]) {
    const page = await open();
    try {
      const runList = page.getByRole("region", { name: "Run list", exact: true });
      await runSwitch(runList, "notify.py").waitFor();
      await edit(runList);
      await page.getByRole("contentinfo").getByText("Run list saved", { exact: true }).waitFor();
      saved((await stored(page)).lists.global);
    } finally { await page.close(); }
  }
});

test("discovered scripts name their kinds, declared events and task times", async () => {
  const page = await open();
  try {
    const discovered = page.getByRole("region", { name: "Discovered scripts", exact: true });
    const entry = (name) => discovered.getByRole("button").filter({ has: page.getByText(name, { exact: true }) });
    for (const [name, lines] of [
      ["Notify", ["Post-processing", "Queue", "Declared events: NZB_ADDED, NZB_DOWNLOADED"]],
      ["Cleanup", ["Post-processing"]],
      ["Nightly report", ["Scheduler", "Task times: 04:00, *:20"]],
      ["Intake filter", ["Scan", "Feed", "Queue", "Declared events: None recognised"]],
    ]) {
      for (const line of lines) await entry(name).getByText(line, { exact: true }).waitFor();
    }
    assert.equal(await entry("Cleanup").getByText("Declared events:").count(), 0);
    assert.equal(await entry("Notify").getByText("Task times:").count(), 0);
  } finally { await page.close(); }
});

test("a job's script runs group by event, mark each status and open the retained output", async () => {
  const page = await open("?job");
  try {
    const runs = page.getByRole("region", { name: "Script runs", exact: true });
    await runs.getByText("4 runs", { exact: true }).waitFor();
    const group = (label) => page.getByRole("group").filter({ has: page.getByText(label, { exact: true }) });
    await group("queue:NZB_ADDED (1)").getByText("queued fixture job", { exact: true }).waitFor();
    const post = group("post_processing (3)");
    for (const status of ["SUCCEEDED", "WARNING", "FAILED"]) await post.getByText(status, { exact: true }).waitFor();
    await post.getByText("script is no longer in the scripts directory", { exact: true }).waitFor();
    await post.getByText("Capture limit reached; output was truncated.", { exact: true }).waitFor();
    await post.getByText("[REDACTED]").waitFor();

    // The excerpt stands until the retained output arrives, and comes back on request.
    await post.getByRole("button", { name: "Show retained output", exact: true }).click();
    await post.getByText("resolved 1 recipient").waitFor();
    await post.getByRole("button", { name: "Show excerpt", exact: true }).click();
    await post.getByText("resolved 1 recipient").waitFor({ state: "detached" });
    await post.getByText("[REDACTED]").waitFor();

    const disclosure = post.getByRole("button", { name: "post_processing (3)", exact: true });
    assert.equal(await disclosure.getAttribute("aria-expanded"), "true");
    await disclosure.click();
    await post.getByText("FAILED", { exact: true }).waitFor({ state: "detached" });
    assert.equal(await disclosure.getAttribute("aria-expanded"), "false");
  } finally { await page.close(); }
});

test("a job no script ran for says so", async () => {
  const page = await open("?job&empty");
  try {
    const runs = page.getByRole("region", { name: "Script runs", exact: true });
    await runs.getByText("No script runs recorded.", { exact: true }).waitFor();
    await runs.getByText("0 runs", { exact: true }).waitFor();
    assert.equal(await page.getByRole("group").count(), 0);
  } finally { await page.close(); }
});

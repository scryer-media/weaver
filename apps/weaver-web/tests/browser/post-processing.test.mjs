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

test("configuration and scripts are two screens that share nothing but the settings behind them", async () => {
  const region = (page, name) => page.getByRole("region", { name, exact: true });
  const configuration = ["Execution", "Event scripts and output retention", "Interpreters", "Scripts directory"];
  const scripts = ["Run list", "Discovered scripts", "Scripts that could not be read"];
  for (const [query, shown, absent] of [["", configuration, scripts], ["?scripts", scripts, configuration]]) {
    const page = await open(query);
    try {
      for (const name of shown) await region(page, name).waitFor();
      assert.equal(await page.getByRole("region").count(), shown.length);
      for (const name of absent) assert.equal(await region(page, name).count(), 0);
      // Which downloads a run list is for is chosen only where the run list is.
      assert.equal(await page.locator("#controls").getByRole("button").count(), query === "" ? 0 : 1);
    } finally { await page.close(); }
  }
});

test("event script limits are a section of the configuration and save with the rest of it", async () => {
  const page = await open();
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    await events.getByText("queue events run one at a time", { exact: true }).waitFor();
    const limit = (name) => events.getByRole("spinbutton", { name, exact: true });
    for (const [name, value] of [
      ["Concurrent event scripts", "1"], ["Default event timeout", "300"], ["File event interval", "0"],
      ["Captured output per run", "1024"], ["Retained runs per job", "32"],
      ["Compressed output budget", "64"], ["Compressed output cap per run", "2048"],
    ]) assert.equal(await limit(name).inputValue(), value);
    // A size is read in the unit beside it and stored in bytes.
    for (const [name, unit] of [["Captured output per run", "KB"], ["Compressed output budget", "MB"], ["Compressed output cap per run", "KB"]]) {
      await events.locator("label").filter({ has: page.getByRole("spinbutton", { name, exact: true }) }).getByText(unit, { exact: true }).waitFor();
    }
    // The execution limit keeps a name of its own beside the event one.
    assert.equal(await page.getByRole("spinbutton", { name: "Concurrent scripts", exact: true }).count(), 1);
    const save = page.getByRole("button", { name: "Save changes", exact: true });
    assert.equal(await save.isDisabled(), true);
    // A number field commits what was typed when focus leaves it, held to the range the daemon accepts.
    await limit("File event interval").fill("-1");
    await limit("File event interval").blur();
    await limit("Concurrent event scripts").fill("12");
    await limit("Concurrent event scripts").blur();
    await limit("Compressed output budget").fill("128");
    await limit("Compressed output budget").blur();
    await save.click();
    await page.getByRole("contentinfo").getByText("Saved", { exact: true }).waitFor();
    const settings = await stored(page);
    assert.equal(settings.fileDownloadedEventInterval, -1);
    assert.equal(settings.eventScriptConcurrency, 8);
    assert.equal(settings.scriptOutputRingBytes, 128 * 1024 * 1024);
    assert.equal(await limit("Concurrent event scripts").inputValue(), "8");
  } finally { await page.close(); }
});

test("a size its unit only rounds is stored as it was unless its field is changed", async () => {
  const page = await open("?uneven");
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    const limit = (name) => events.getByRole("spinbutton", { name, exact: true });
    await limit("Compressed output cap per run").waitFor();
    assert.equal(await limit("Compressed output cap per run").inputValue(), "2048");
    // Leaving the field commits what it shows, which is not what is stored.
    await limit("Compressed output cap per run").focus();
    await limit("Compressed output cap per run").blur();
    await limit("Retained runs per job").fill("16");
    await limit("Retained runs per job").blur();
    await page.getByRole("button", { name: "Save changes", exact: true }).click();
    await page.getByRole("contentinfo").getByText("Saved", { exact: true }).waitFor();
    const settings = await stored(page);
    assert.equal(settings.scriptOutputRunsPerJob, 16);
    assert.equal(settings.scriptOutputRunCapBytes, 2097000);
  } finally { await page.close(); }
});

test("the settings search reaches an event script limit", async () => {
  const page = await open("?search=retained");
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    await events.getByRole("spinbutton", { name: "Retained runs per job", exact: true }).waitFor();
    assert.equal(await events.getByRole("spinbutton").count(), 1);
    assert.equal(await page.getByRole("region").count(), 1);
  } finally { await page.close(); }
});

const runSwitch = (runList, script) => runList.getByRole("switch", { name: `Run ${script}`, exact: true });
const runMode = (runList, script) => runList.getByRole("button", { name: `Run mode for ${script}`, exact: true });

test("a run list row carries its timeout, its run mode, its switch and its order controls", async () => {
  const page = await open("?scripts");
  try {
    const runList = page.getByRole("region", { name: "Run list", exact: true });
    await runList.getByRole("switch").first().and(runSwitch(runList, "notify.py")).waitFor();
    const timeout = (script) => runList.getByRole("spinbutton", { name: `Timeout for ${script}`, exact: true });
    assert.equal(await timeout("notify.py").inputValue(), "0");
    assert.equal(await timeout("cleanup.sh").inputValue(), "600");
    // The section says what the two run modes mean, after which downloads the list is for.
    await runList.getByText(
      "runs for every download · blocking is waited for · fire and forget is not · scan and feed scripts always are",
      { exact: true },
    ).waitFor();
    assert.equal(await runList.getByText("Run mode", { exact: true }).count(), 1);
    assert.equal(await runMode(runList, "notify.py").getByText("Blocking", { exact: true }).count(), 1);
    assert.equal(await runMode(runList, "cleanup.sh").getByText("Fire and forget", { exact: true }).count(), 1);
    await runMode(runList, "retired.py").click();
    assert.equal(await page.getByRole("menuitemradio").count(), 2);
    for (const [mode, chosen] of [["Blocking", "true"], ["Fire and forget", "false"]]) {
      assert.equal(await page.getByRole("menuitemradio", { name: mode, exact: true }).getAttribute("aria-checked"), chosen);
    }
    await page.keyboard.press("Escape");
    await page.getByRole("menu").waitFor({ state: "detached" });
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
  // The screen saves a run-list edit behind what it shows, so each edit gets a
  // page of its own: no earlier save or refetch is in flight when it is made.
  for (const [edit, saved] of [
    [async (runList) => {
      const timeout = runList.getByRole("spinbutton", { name: "Timeout for notify.py", exact: true });
      await timeout.fill("90");
      await timeout.blur();
    }, (list) => assert.deepEqual(list[0], { script: "notify.py", enabled: true, timeoutSeconds: 90, blocking: true })],
    [async (runList, page) => {
      await runMode(runList, "notify.py").click();
      await page.getByRole("menuitemradio", { name: "Fire and forget", exact: true }).click();
      await runMode(runList, "notify.py").getByText("Fire and forget", { exact: true }).waitFor();
    }, (list) => assert.deepEqual(list[0], { script: "notify.py", enabled: true, timeoutSeconds: null, blocking: false })],
    [async (runList, page) => {
      await runMode(runList, "cleanup.sh").click();
      await page.getByRole("menuitemradio", { name: "Blocking", exact: true }).click();
      await runMode(runList, "cleanup.sh").getByText("Blocking", { exact: true }).waitFor();
    }, (list) => assert.deepEqual(list[1], { script: "cleanup.sh", enabled: false, timeoutSeconds: 600, blocking: true })],
    [async (runList) => {
      await runList.getByRole("button", { name: "Move down", exact: true }).first().click();
      await runList.getByRole("switch").first().and(runSwitch(runList, "cleanup.sh")).waitFor();
    }, (list) => assert.deepEqual(list.map((entry) => entry.script), ["cleanup.sh", "notify.py", "retired.py"])],
    [async (runList) => {
      await runSwitch(runList, "cleanup.sh").click();
      await runList.locator('[role="switch"][aria-checked="true"]').and(runSwitch(runList, "cleanup.sh")).waitFor();
    }, (list) => assert.deepEqual(list[1], { script: "cleanup.sh", enabled: true, timeoutSeconds: 600, blocking: false })],
    [async (runList) => {
      await runList.getByRole("button", { name: "Remove", exact: true }).last().click();
      await runSwitch(runList, "retired.py").waitFor({ state: "detached" });
    }, (list) => assert.deepEqual(list.map((entry) => entry.script), ["notify.py", "cleanup.sh"])],
  ]) {
    const page = await open("?scripts");
    try {
      const runList = page.getByRole("region", { name: "Run list", exact: true });
      await runSwitch(runList, "notify.py").waitFor();
      await edit(runList, page);
      await page.getByRole("contentinfo").getByText("Run list saved", { exact: true }).waitFor();
      saved((await stored(page)).lists.global);
    } finally { await page.close(); }
  }
});

test("discovered scripts name their kinds, declared events and task times", async () => {
  const page = await open("?scripts");
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
    // Only the run nothing waited for is marked.
    assert.equal(await runs.getByText("Fire and forget", { exact: true }).count(), 1);
    assert.equal(await post.getByText("Fire and forget", { exact: true }).count(), 1);

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

// The runs screen: its one table, a run's row in it, and the rows holding a given text.
// "Load more" is a button under the rows and not one of them.
const runsTable = (page) => page.getByRole("region", { name: "Script runs", exact: true });
const runRows = (page) => runsTable(page).locator('[role="button"]');
const rowsHolding = (page, text) => runRows(page).filter({ has: page.getByText(text, { exact: true }) });
const loadMore = (page) => runsTable(page).getByRole("button", { name: "Load more", exact: true });
// The pages of runs the screen has asked the fixture's daemon for, oldest request first.
const runRequests = (page) => page.evaluate(() => fetch("/graphql", {
  method: "POST", body: JSON.stringify({ operationName: "ScriptRunRequests", variables: {} }),
}).then((response) => response.json()).then((payload) => payload.data.scriptRunRequests));

test("the runs screen is one table of every run, newest first, with no way to create one", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    assert.equal(await page.getByRole("region").count(), 1);
    for (const header of ["Finished", "Script", "Trigger", "Job", "Status", "Exit code", "Duration"]) {
      assert.equal(await runsTable(page).getByText(header, { exact: true }).count(), 1);
    }
    await runsTable(page).getByText("newest first", { exact: true }).waitFor();
    for (const [index, script, trigger, status, exitCode, duration] of [
      [0, "nightly.py", "Schedule", "SUCCEEDED", "0", "1m 35s"],
      [1, "intake.py", "Scan", "SUCCEEDED", "0", "1.5s"],
      [2, "notify.py", "Post-processing", "SUCCEEDED", "0", "240 ms"],
      [3, "retired.py", "Post-processing", "FAILED", "—", "240 ms"],
      [4, "notify.py", "Queue · NZB_ADDED", "SUCCEEDED", "0", "240 ms"],
      [5, "cleanup.sh", "Post-processing", "WARNING", "3", "240 ms"],
      [6, "intake.py", "Feed", "SUCCEEDED", "0", "240 ms"],
      [7, "sweep.sh", "Queue · FILE_DOWNLOADED", "SUCCEEDED", "0", "240 ms"],
    ]) {
      for (const text of [script, trigger, status, exitCode, duration]) {
        assert.equal(await rows.nth(index).getByText(text, { exact: true }).count(), 1, `row ${index}: ${text}`);
      }
    }
    // Only the run nothing waited for is marked.
    assert.equal(await runsTable(page).getByText("Fire and forget", { exact: true }).count(), 1);
    assert.equal(await rows.nth(5).getByText("Fire and forget", { exact: true }).count(), 1);
    // The top bar filters and refreshes; nothing on the screen adds a run.
    const controls = page.locator("#controls").getByRole("button");
    assert.equal(await controls.count(), 2);
    for (const name of ["Filter by trigger", "Refresh"]) {
      assert.equal(await page.locator("#controls").getByRole("button", { name, exact: true }).count(), 1);
    }
    assert.equal(await page.getByRole("button", { name: /^(Add|Create|New)\b/ }).count(), 0);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 50, before: null, kind: null });
  } finally { await page.close(); }
});

test("a run links to its job, and a run no job owns links nowhere", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    // The scheduled task and the scan belong to no job.
    for (const index of [0, 1]) {
      assert.equal(await rows.nth(index).getByRole("link").count(), 0);
      assert.equal(await rows.nth(index).getByText("—", { exact: true }).count(), 1);
    }
    const named = rows.nth(2).getByRole("link", { name: "fixture.release.one", exact: true });
    assert.equal(await named.getAttribute("href"), "/jobs/7");
    // A job whose name is not known is still reached by its number.
    assert.equal(await rows.nth(5).getByRole("link", { name: "#9", exact: true }).getAttribute("href"), "/jobs/9");
    // The link is the job's own: following it does not open the run it sits in.
    await named.click();
    assert.equal(await page.getByRole("dialog").count(), 0);
  } finally { await page.close(); }
});

test("the trigger filter asks the daemon for one kind and lists what comes back", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    await page.locator("#controls").getByRole("button", { name: "Filter by trigger", exact: true }).click();
    const triggers = ["All triggers", "Post-processing", "Queue", "Scan", "Schedule", "Feed"];
    assert.equal(await page.getByRole("menuitemradio").count(), triggers.length);
    for (const name of triggers) {
      assert.equal(await page.getByRole("menuitemradio", { name, exact: true }).count(), 1);
    }
    await page.getByRole("menuitemradio", { name: "Post-processing", exact: true }).click();
    await rows.first().and(rowsHolding(page, "Post-processing")).waitFor();
    assert.equal(await rows.count(), 3);
    assert.equal(await rowsHolding(page, "Post-processing").count(), 3);
    assert.equal(await loadMore(page).count(), 0);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 50, before: null, kind: "POST_PROCESSING" });
  } finally { await page.close(); }
});

test("load more appends the next page of runs and goes away at the end", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await loadMore(page).waitFor();
    assert.equal(await rows.count(), 50);
    assert.equal(await rows.last().getByRole("link", { name: "fixture.batch.11", exact: true }).count(), 1);
    await loadMore(page).click();
    await rows.last().and(rowsHolding(page, "fixture.batch.1")).waitFor();
    assert.equal(await rows.count(), 60);
    // The second page follows the first; the first is not asked for again.
    assert.equal(await rows.nth(49).getByRole("link", { name: "fixture.batch.11", exact: true }).count(), 1);
    assert.equal(await rows.nth(50).getByRole("link", { name: "fixture.batch.10", exact: true }).count(), 1);
    assert.equal(await loadMore(page).count(), 0);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 50, before: "run-11", kind: null });
  } finally { await page.close(); }
});

test("opening a run shows what it did and fetches its retained output", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    const record = (script) => page.getByRole("dialog", { name: script, exact: true });
    const shut = async (dialog) => {
      await dialog.getByRole("button", { name: "Close", exact: true }).click();
      await dialog.waitFor({ state: "detached" });
    };
    const retained = (dialog) => dialog.getByRole("button", { name: "Show retained output", exact: true });

    await rows.nth(2).getByText("Post-processing", { exact: true }).click();
    const notify = record("notify.py");
    await notify.waitFor();
    for (const text of ["post_processing", "Post-processing", "Blocking", "SUCCEEDED", "0", "240 ms", "NZBGet"]) {
      assert.equal(await notify.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await notify.getByRole("link", { name: "fixture.release.one", exact: true }).getAttribute("href"), "/jobs/7");
    // The excerpt stands until the retained output arrives, and comes back on request.
    await notify.getByText("notified 2 recipients").waitFor();
    assert.equal(await notify.getByText("resolved 2 recipients").count(), 0);
    await retained(notify).click();
    await notify.getByText("resolved 2 recipients").waitFor();
    await notify.getByRole("button", { name: "Show excerpt", exact: true }).click();
    await notify.getByText("resolved 2 recipients").waitFor({ state: "detached" });
    await notify.getByText("notified 2 recipients").waitFor();
    await shut(notify);

    // A failed run says why, and has neither an exit code nor output to fetch.
    await rows.nth(3).getByText("FAILED", { exact: true }).click();
    const retired = record("retired.py");
    await retired.waitFor();
    for (const text of ["script is no longer in the scripts directory", "FAILED", "—", "Full output is no longer retained."]) {
      assert.equal(await retired.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await retained(retired).count(), 0);
    await shut(retired);

    // A scan belongs to no job; its excerpt outlives the output it was cut from.
    await rows.nth(1).getByText("Scan", { exact: true }).click();
    const intake = record("intake.py");
    await intake.waitFor();
    for (const text of ["scan", "Scan", "—", "1.5s", "intake.py finished", "Full output is no longer retained."]) {
      assert.equal(await intake.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await intake.getByRole("link").count(), 0);
    assert.equal(await retained(intake).count(), 0);
    await shut(intake);

    // A run nothing waited for says so, and one cut at the capture limit says that.
    await rows.nth(5).getByText("WARNING", { exact: true }).click();
    const cleanup = record("cleanup.sh");
    await cleanup.waitFor();
    for (const text of ["Fire and forget", "WARNING", "3", "SABnzbd", "Capture limit reached; output was truncated."]) {
      assert.equal(await cleanup.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await cleanup.getByRole("link", { name: "#9", exact: true }).getAttribute("href"), "/jobs/9");
    assert.equal(await retained(cleanup).count(), 1);
  } finally { await page.close(); }
});

test("the settings search narrows the loaded runs by script", async () => {
  const page = await open("?runs&search=cleanup");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "cleanup.sh")).waitFor();
    assert.equal(await rows.count(), 1);
  } finally { await page.close(); }
});

test("a daemon that recorded no script run says so and offers nothing to add", async () => {
  const page = await open("?runs&empty");
  try {
    await runsTable(page).getByText("No script runs recorded.", { exact: true }).waitFor();
    assert.equal(await runsTable(page).getByRole("button").count(), 0);
    assert.equal(await page.locator("#controls").getByRole("button").count(), 2);
    assert.equal(await page.getByRole("button", { name: /^(Add|Create|New)\b/ }).count(), 0);
  } finally { await page.close(); }
});

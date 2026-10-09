import assert from "node:assert/strict";
import { join } from "node:path";
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
/** With `SCRIPTS_SCREENSHOT_DIR` set to a directory, the page as it stands is saved there as `name`.png. */
async function shot(page, name) {
  if (!process.env.SCRIPTS_SCREENSHOT_DIR) return;
  // The table and a dialog's body scroll inside the window; a taller one holds the whole of either.
  await page.setViewportSize({ width: 1600, height: 1500 });
  await page.screenshot({ path: join(process.env.SCRIPTS_SCREENSHOT_DIR, `${name}.png`) });
  await page.setViewportSize({ width: 1600, height: 1000 });
}
// What the fixture's daemon holds, read the way the panel reads it.
const stored = (page) => page.evaluate(() => fetch("/graphql", {
  method: "POST", body: JSON.stringify({ operationName: "PostProcessingSettings", variables: {} }),
}).then((response) => response.json()).then((payload) => payload.data.postProcessingSettings));
// The fixture's daemon itself: its instances with their secrets, every instance
// mutation it was sent, and every test run it was asked to start.
const daemon = (page) => page.evaluate(() => window.scriptsFixture);
const status = (page, text) => page.getByRole("contentinfo").getByText(text, { exact: true });
const action = (scope, name) => scope.getByRole("button", { name, exact: true });
const controls = (page) => page.locator("#controls");
/** Chooses `option` in the select labelled `label`. */
async function pick(page, scope, label, option) {
  await action(scope, label).click();
  await page.getByRole("menuitemradio", { name: option, exact: true }).click();
}

test("configuration and scripts are two screens that share nothing but the settings behind them", async () => {
  const region = (page, name) => page.getByRole("region", { name, exact: true });
  const configuration = ["Execution", "Event scripts and output retention", "Interpreters", "Scripts directory"];
  const scripts = ["Script instances", "Scripts that could not be read"];
  for (const [query, shown, absent, buttons] of [
    ["", configuration, scripts, []],
    ["?scripts", scripts, configuration, ["Refresh", "Create instance"]],
  ]) {
    const page = await open(query);
    try {
      for (const name of shown) await region(page, name).waitFor();
      assert.equal(await page.locator("main > section").count(), shown.length);
      for (const name of absent) assert.equal(await region(page, name).count(), 0);
      // Instances are created only where they are listed.
      assert.deepEqual(await controls(page).getByRole("button").allTextContents(), buttons);
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

test("when instances for every category run is a setting saved with the rest of the configuration", async () => {
  const page = await open();
  try {
    const always = "For every download";
    const onlyWithout = "Only when the category has none";
    const execution = page.getByRole("region", { name: "Execution", exact: true });
    const choice = action(execution, "Global scripts run");
    await execution.getByText(
      "A global script is an instance with no category of its own, and runs ahead of a category's own. This decides whether it also runs for a download whose category has scripts.",
      { exact: true },
    ).waitFor();
    await choice.getByText(always, { exact: true }).waitFor();
    await choice.click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [always, onlyWithout]);
    await page.getByRole("menuitemradio", { name: onlyWithout, exact: true }).click();
    await choice.getByText(onlyWithout, { exact: true }).waitFor();
    await page.getByRole("button", { name: "Save changes", exact: true }).click();
    await status(page, "Saved").waitFor();
    assert.equal((await stored(page)).globalScriptsRun, "ONLY_WITHOUT_CATEGORY_SCRIPTS");
    await choice.getByText(onlyWithout, { exact: true }).waitFor();
    await shot(page, "configuration");
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

// The scripts screen: its one table, the heading a trigger's instances sit under,
// the rows of either, and the row holding a given text. A row is a button that
// opens its editor, and the controls inside it are buttons of their own.
const table = (page) => page.getByRole("region", { name: "Script instances", exact: true });
const group = (page, title) => table(page).getByRole("region", { name: title, exact: true });
const rows = (scope) => scope.locator('[role="button"]');
const row = (page, name) => rows(table(page)).filter({ has: page.getByText(name, { exact: true }) });
const headings = (page) => table(page).getByRole("region").evaluateAll((sections) => sections.map((section) => section.getAttribute("aria-label")));
// The scripts no instance runs, which close the table, and the row of one of them by its file.
const unused = (page) => group(page, "Scripts with no instance");
const unusedRow = (page, file) => rows(unused(page)).filter({ has: page.getByText(file, { exact: true }) });
/** Opens the editor of the instance called `name`, which the dialog is then titled by. */
async function edit(page, name) {
  await row(page, name).getByText(name, { exact: true }).first().click();
  const editor = page.getByRole("dialog", { name, exact: true });
  await editor.waitFor();
  return editor;
}
const creator = (page) => page.getByRole("dialog", { name: "Create instance", exact: true });
const field = (editor, name) => editor.getByRole("textbox", { name, exact: true });
// A secret's field is a password input, which has no role to be found by.
const secretField = (editor, name) => editor.getByLabel(name, { exact: true });

test("the scripts screen is one table of instances, under a heading for each thing that starts them", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    await unused(page).waitFor();
    for (const header of ["Name", "Script", "Categories", "Run mode", "Timeout", "Enabled"]) {
      assert.equal(await table(page).getByText(header, { exact: true }).count(), 1, header);
    }
    await table(page).getByText("instances for every category run ahead of a category's own", { exact: true }).waitFor();
    assert.deepEqual(await headings(page), [
      "Post-processing", "Queue · NZB_ADDED", "Queue · NZB_DELETED", "Schedule", "Feed", "Scripts with no instance",
    ]);
    // Each row: its name, script, categories, run mode and timeout, and whether it is on.
    for (const [title, entries] of [
      ["Post-processing", [
        ["Notify", "notify.py", "Every category", "Blocking", "Default", true],
        ["Tidy tv", "cleanup.sh", "tv", "Fire and forget", "10m", false],
        ["Retired", "retired.py", "Every category", "Blocking", "Default", true],
      ]],
      ["Queue · NZB_ADDED", [["Announce", "notify.py", "Every category", "Blocking", "1m 30s", true]]],
      ["Queue · NZB_DELETED", [["Log removal", "notify.py", "Every category", "Blocking", "Default", true]]],
      // Only a download has a category to be narrowed to.
      ["Schedule", [["Nightly report", "nightly.py", "—", "Blocking", "1h", true]]],
      ["Feed", [["Feed intake", "intake.py", "—", "Fire and forget", "Default", true]]],
    ]) {
      const listed = rows(group(page, title));
      assert.equal(await listed.count(), entries.length, title);
      for (const [index, [name, script, categories, mode, timeout, enabled]] of entries.entries()) {
        for (const text of [name, script, categories, mode, timeout]) {
          assert.equal(await listed.nth(index).getByText(text, { exact: true }).count(), 1, `${name}: ${text}`);
        }
        assert.equal(await listed.nth(index).getByRole("switch", { name: `${name} enabled`, exact: true }).isChecked(), enabled, name);
      }
    }
    await group(page, "Schedule").getByText("runs when a schedule rule names it", { exact: true }).waitFor();
    await group(page, "Feed").getByText("runs on the feeds it is attached to", { exact: true }).waitFor();
    // An instance whose script has gone says so, and one its header has moved away from says that.
    for (const [name, line] of [
      ["Retired", "the script is no longer in the scripts directory"],
      ["Announce", "Inputs differ from the script's header"],
    ]) {
      assert.equal(await row(page, name).getByText(line, { exact: true }).count(), 1, name);
      assert.equal(await table(page).getByText(line, { exact: true }).count(), 1, line);
    }
    // Every instance can be moved, tested and deleted; only the one that has drifted can be re-applied.
    for (const name of ["Move up", "Move down", "Test", "Delete"]) assert.equal(await action(table(page), name).count(), 7, name);
    assert.equal(await action(table(page), "Re-apply from header").count(), 1);
    assert.equal(await action(row(page, "Announce"), "Re-apply from header").count(), 1);
    const post = rows(group(page, "Post-processing"));
    for (const [index, up, down] of [[0, true, false], [1, false, false], [2, false, true]]) {
      assert.equal(await action(post.nth(index), "Move up").isDisabled(), up);
      assert.equal(await action(post.nth(index), "Move down").isDisabled(), down);
    }
    // An instance alone under its heading has nowhere to go.
    for (const name of ["Move up", "Move down"]) assert.equal(await action(row(page, "Feed intake"), name).isDisabled(), true);
    // One Create for the whole list, in the top bar.
    assert.deepEqual(await controls(page).getByRole("button").allTextContents(), ["Refresh", "Create instance"]);
    assert.equal(await action(controls(page), "Create instance").isDisabled(), false);
    await shot(page, "scripts-table");
  } finally { await page.close(); }
});

test("a script nothing is wired to closes the table, with what its header declares and a way to start", async () => {
  const page = await open("?scripts");
  try {
    await unused(page).getByText("/fixture/scripts", { exact: true }).waitFor();
    assert.equal(await rows(unused(page)).count(), 2);
    for (const line of [
      "Archive", "archive.py", "NZBGet · 0.9", "Post-processing", "Queue", "Schedule",
      "Declared events: None recognised", "Task times: 03:30",
    ]) assert.equal(await unusedRow(page, "archive.py").getByText(line, { exact: true }).count(), 1, line);
    for (const name of ["Set up from header", "Create instance"]) {
      assert.equal(await action(unusedRow(page, "archive.py"), name).count(), 1, name);
    }
    // A script whose header declares nothing has nothing to be set up from.
    for (const line of ["SABnzbd", "Post-processing"]) {
      assert.equal(await unusedRow(page, "plain.sh").getByText(line, { exact: true }).count(), 1, line);
    }
    assert.equal(await unusedRow(page, "plain.sh").getByText("Declared events:").count(), 0);
    assert.equal(await action(unusedRow(page, "plain.sh"), "Set up from header").count(), 0);
    assert.equal(await action(unusedRow(page, "plain.sh"), "Create instance").count(), 1);
    // Neither is an instance, so neither has a switch, an order or a test.
    assert.equal(await unused(page).getByRole("switch").count(), 0);
    for (const name of ["Move up", "Test", "Delete"]) assert.equal(await action(unused(page), name).count(), 0, name);
    // What could not be read at all is listed under the table.
    const problems = page.getByRole("region", { name: "Scripts that could not be read", exact: true });
    for (const text of ["broken.py", "option 3 declares an unknown type"]) await problems.getByText(text, { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("the table says which way the instances for every category run", async () => {
  const page = await open("?scripts&cascade");
  try {
    await table(page).getByText("instances for every category run only when the category has none of its own", { exact: true }).waitFor();
    assert.equal(await table(page).getByText("instances for every category run ahead of a category's own", { exact: true }).count(), 0);
  } finally { await page.close(); }
});

test("a scripts directory that cannot be listed still shows every instance, and why none can run", async () => {
  const page = await open("?scripts&unlistable");
  try {
    const problem = "could not read /fixture/scripts: permission denied";
    await row(page, "Notify").waitFor();
    await status(page, problem).waitFor();
    assert.equal(await rows(table(page)).count(), 7);
    assert.equal(await table(page).getByText(problem, { exact: true }).count(), 7);
    assert.equal(await unused(page).count(), 0);
    assert.equal(await page.getByRole("region", { name: "Scripts that could not be read", exact: true }).count(), 0);
    // With no script known, there is nothing a new instance could run.
    assert.equal(await action(controls(page), "Create instance").isDisabled(), true);
  } finally { await page.close(); }
});

test("a directory with no scripts and no instances says so", async () => {
  const page = await open("?scripts&empty");
  try {
    await table(page).getByText("Nothing here. Put a script in the scripts directory, then reload.", { exact: true }).waitFor();
    assert.equal(await page.getByRole("region").count(), 1);
    assert.equal(await rows(table(page)).count(), 0);
    assert.equal(await action(controls(page), "Create instance").isDisabled(), true);
  } finally { await page.close(); }
});

test("refresh reads the instances again", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    // Renamed behind the screen's back, as another session would.
    await page.evaluate(() => { window.scriptsFixture.instances[0].name = "Renamed elsewhere"; });
    await action(controls(page), "Refresh").click();
    await rows(group(page, "Post-processing")).first().and(row(page, "Renamed elsewhere")).waitFor();
    assert.equal(await row(page, "Notify").count(), 0);
  } finally { await page.close(); }
});

test("a new instance starts from what the chosen script's header declares", async () => {
  const page = await open("?scripts");
  try {
    await unused(page).waitFor();
    await action(controls(page), "Create instance").click();
    const editor = creator(page);
    await editor.getByText("new instance", { exact: true }).waitFor();
    // Nothing can be saved until there is a script to run.
    await action(editor, "Script").getByText("Choose a script", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await editor.getByText("This instance has no inputs.", { exact: true }).waitFor();
    await action(editor, "Script").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "Choose a script", "Notify · notify.py", "Cleanup · cleanup.sh", "Nightly report · nightly.py",
      "Intake filter · intake.py", "Archive · archive.py", "plain.sh",
    ]);
    await page.getByRole("menuitemradio", { name: "Archive · archive.py", exact: true }).click();

    // The first trigger the header declares, and every input it declares at its default.
    await action(editor, "Trigger").getByText("Post-processing · declared", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), false);
    assert.equal(await editor.getByText("This instance has no inputs.", { exact: true }).count(), 0);
    assert.equal(await field(editor, "Target").inputValue(), "/fixture/archive");
    await editor.getByText("Where a finished download is copied. Default /fixture/archive.", { exact: true }).waitFor();
    // A secret is never filled in from the header, and nothing is saved for it yet.
    assert.equal(await secretField(editor, "Key").getAttribute("type"), "password");
    assert.equal(await secretField(editor, "Key").inputValue(), "");
    assert.equal(await secretField(editor, "Key").getAttribute("placeholder"), null);
    await editor.getByText("Once saved, the value is never shown again.", { exact: true }).waitFor();
    // The triggers the header declares are marked among all there are.
    await action(editor, "Trigger").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "Post-processing · declared", "Queue", "Scan", "Schedule · declared", "Feed",
    ]);
    await page.getByRole("menuitemradio", { name: "Post-processing · declared", exact: true }).click();

    await field(editor, "Name").fill("Archive movies");
    await secretField(editor, "Key").fill("fixture-key-1");
    const categories = editor.getByRole("group", { name: "Categories", exact: true });
    assert.deepEqual(await categories.getByRole("button").allTextContents(), ["movies", "tv"]);
    await action(categories, "movies").click();
    await categories.locator('[aria-pressed="true"]').and(action(categories, "movies")).waitFor();
    await editor.getByRole("radio", { name: "Fire and forget", exact: true }).click();
    const timeout = editor.getByRole("spinbutton", { name: "Timeout", exact: true });
    await timeout.fill("120");
    await timeout.blur();
    await shot(page, "create-dialog");
    await action(editor, "Save").click();
    await status(page, "Archive movies created").waitFor();
    await editor.waitFor({ state: "detached" });

    // It runs last under its trigger's heading, and its script has left the ones with no instance.
    const created = rows(group(page, "Post-processing")).nth(3);
    await created.and(row(page, "Archive movies")).waitFor();
    for (const text of ["archive.py", "movies", "Fire and forget", "2m"]) {
      assert.equal(await created.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await unusedRow(page, "archive.py").count(), 0);
    assert.equal(await rows(unused(page)).count(), 1);
    assert.equal(await page.getByText("fixture-key-1").count(), 0);
    assert.deepEqual((await daemon(page)).requests, [{ name: "CreateScriptInstance", variables: { input: {
      name: "Archive movies", script: "archive.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [{ name: "Target", value: "/fixture/archive", secret: false }, { name: "Key", value: "fixture-key-1", secret: true }],
      categories: ["movies"], enabled: true, blocking: false, timeoutSeconds: 120,
    } } }]);
  } finally { await page.close(); }
});

test("an instance can be started from its script's row, and a queue instance names its event", async () => {
  const page = await open("?scripts");
  try {
    await action(unusedRow(page, "archive.py"), "Create instance").click();
    const editor = creator(page);
    // The row's script is already chosen.
    await action(editor, "Script").getByText("Archive · archive.py", { exact: true }).waitFor();
    assert.equal(await action(editor, "Queue event").count(), 0);
    await pick(page, editor, "Trigger", "Queue");
    // The header declares no queue event, so none of them is marked.
    await action(editor, "Queue event").getByText("NZB_ADDED", { exact: true }).waitFor();
    await action(editor, "Queue event").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "FILE_DOWNLOADED", "URL_COMPLETED", "NZB_MARKED", "NZB_ADDED", "NZB_NAMED", "NZB_DOWNLOADED", "NZB_DELETED",
    ]);
    await page.getByRole("menuitemradio", { name: "NZB_NAMED", exact: true }).click();
    // Saved with no name and no secret: it takes its script's name, and the secret is not stored at all.
    await action(editor, "Save").click();
    await status(page, "archive.py created").waitFor();
    await rows(group(page, "Queue · NZB_NAMED")).first().and(row(page, "archive.py")).waitFor();
    assert.deepEqual(await headings(page), [
      "Post-processing", "Queue · NZB_ADDED", "Queue · NZB_NAMED", "Queue · NZB_DELETED", "Schedule", "Feed", "Scripts with no instance",
    ]);
    let held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), { name: "CreateScriptInstance", variables: { input: {
      name: "", script: "archive.py", trigger: "QUEUE", queueEvent: "NZB_NAMED",
      inputs: [{ name: "Target", value: "/fixture/archive", secret: false }, { name: "Key", value: null, secret: true }],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
    } } });
    assert.equal(held.instances.at(-1).name, "archive.py");
    assert.deepEqual(held.instances.at(-1).inputs, [{ name: "Target", value: "/fixture/archive", secret: false }]);

    // Each input the header declares draws the control its header asks for.
    await action(controls(page), "Create instance").click();
    await pick(page, editor, "Script", "Notify · notify.py");
    await action(editor, "Trigger").getByText("Post-processing · declared", { exact: true }).waitFor();
    assert.equal(await field(editor, "Label").inputValue(), "");
    await editor.getByText("Shown in the notification title. Required.", { exact: true }).waitFor();
    await action(editor, "Mode").getByText("quiet", { exact: true }).waitFor();
    assert.equal(await editor.getByRole("switch", { name: "Attach the log", exact: true }).isChecked(), false);
    assert.equal(await secretField(editor, "Token").getAttribute("type"), "password");
    await field(editor, "Label").fill("downloads");
    await pick(page, editor, "Mode", "verbose");
    await editor.getByRole("switch", { name: "Attach the log", exact: true }).click();
    // The events the header declares are marked, and the first of them is the one offered.
    await pick(page, editor, "Trigger", "Queue · declared");
    await action(editor, "Queue event").getByText("NZB_ADDED · declared", { exact: true }).waitFor();
    await action(editor, "Queue event").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "FILE_DOWNLOADED", "URL_COMPLETED", "NZB_MARKED", "NZB_ADDED · declared", "NZB_NAMED", "NZB_DOWNLOADED · declared", "NZB_DELETED",
    ]);
    await page.getByRole("menuitemradio", { name: "NZB_DOWNLOADED · declared", exact: true }).click();
    // Only a download has a category, so only its triggers can be narrowed to one.
    const categories = editor.getByRole("group", { name: "Categories", exact: true });
    assert.equal(await categories.count(), 1);
    await pick(page, editor, "Trigger", "Schedule");
    await categories.waitFor({ state: "detached" });
    assert.equal(await action(editor, "Queue event").count(), 0);
    await pick(page, editor, "Trigger", "Queue · declared");
    await action(editor, "Queue event").getByText("NZB_DOWNLOADED · declared", { exact: true }).waitFor();
    await action(editor, "Save").click();
    await status(page, "notify.py created").waitFor();
    await rows(group(page, "Queue · NZB_DOWNLOADED")).first().and(row(page, "notify.py")).waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), { name: "CreateScriptInstance", variables: { input: {
      name: "", script: "notify.py", trigger: "QUEUE", queueEvent: "NZB_DOWNLOADED",
      inputs: [
        { name: "Label", value: "downloads", secret: false }, { name: "Token", value: null, secret: true },
        { name: "Mode", value: "verbose", secret: false }, { name: "Attach", value: "yes", secret: false },
      ],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
    } } });
  } finally { await page.close(); }
});

test("what the daemon refuses is said inside the editor, which stays open", async () => {
  const page = await open("?scripts");
  try {
    const editor = await edit(page, "Notify");
    await field(editor, "Label").fill("x".repeat(65));
    await action(editor, "Save").click();
    await editor.getByText("input value is invalid", { exact: true }).waitFor();
    assert.equal(await page.locator("#status").textContent(), "");
    // Changing the form takes back what was said about the last attempt.
    await field(editor, "Label").fill("shorter");
    await editor.getByText("input value is invalid", { exact: true }).waitFor({ state: "detached" });
    await action(editor, "Save").click();
    await status(page, "Notify saved").waitFor();
    await editor.waitFor({ state: "detached" });
    assert.equal((await daemon(page)).instances[0].inputs[0].value, "shorter");
  } finally { await page.close(); }
});

test("a saved secret is never shown, is kept when its field is left blank and replaced when it is not", async () => {
  const page = await open("?scripts");
  try {
    let editor = await edit(page, "Notify");
    // The instance's trigger sits beside its name, and its script and trigger are the ones saved.
    await editor.getByText("Post-processing", { exact: true }).waitFor();
    await action(editor, "Script").getByText("Notify · notify.py", { exact: true }).waitFor();
    await action(editor, "Trigger").getByText("Post-processing · declared", { exact: true }).waitFor();
    assert.equal(await field(editor, "Name").inputValue(), "Notify");
    assert.equal(await field(editor, "Label").inputValue(), "fixture");
    // The secret's field is blank, and says that something is saved behind it.
    assert.equal(await secretField(editor, "Token").inputValue(), "");
    assert.equal(await secretField(editor, "Token").getAttribute("placeholder"), "Saved · leave blank to keep");
    await editor.getByText("The service's access token. Leave blank to keep the saved value.", { exact: true }).waitFor();
    await field(editor, "Label").fill("renamed");
    await field(editor, "Name").fill("Notify all");
    await action(editor, "Save").click();
    await status(page, "Notify all saved").waitFor();
    // The row under its new name is the instance as the daemon now holds it.
    await row(page, "Notify all").waitFor();
    let held = await daemon(page);
    assert.deepEqual(held.requests, [{ name: "UpdateScriptInstance", variables: { id: "1", input: {
      name: "Notify all", script: "notify.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [
        { name: "Label", value: "renamed", secret: false }, { name: "Token", value: null, secret: true },
        { name: "Mode", value: "quiet", secret: false }, { name: "Attach", value: "no", secret: false },
      ],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
    } } }]);
    assert.deepEqual(held.instances[0].inputs[1], { name: "Token", value: "fixture-token-1", secret: true });

    // Typed into, the field replaces what was saved.
    editor = await edit(page, "Notify all");
    assert.equal(await field(editor, "Label").inputValue(), "renamed");
    assert.equal(await secretField(editor, "Token").inputValue(), "");
    await secretField(editor, "Token").fill("fixture-token-2");
    await action(editor, "Save").click();
    await editor.waitFor({ state: "detached" });
    await status(page, "Notify all saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs[1], { name: "Token", value: "fixture-token-2", secret: true });
    assert.deepEqual(held.instances[0].inputs[1], { name: "Token", value: "fixture-token-2", secret: true });
    assert.equal(await page.getByText("fixture-token").count(), 0);

    // A secret the instance was never given is still offered, and left blank it stays unset.
    editor = await edit(page, "Log removal");
    await action(editor, "Queue event").getByText("NZB_DELETED", { exact: true }).waitFor();
    assert.equal(await secretField(editor, "Token").getAttribute("placeholder"), null);
    await editor.getByText("The service's access token. Once saved, the value is never shown again.", { exact: true }).waitFor();
    await action(editor, "Save").click();
    await editor.waitFor({ state: "detached" });
    await status(page, "Log removal saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs.at(-1), { name: "Token", value: null, secret: true });
    assert.deepEqual(held.instances.find((entry) => entry.id === "5").inputs.map((input) => input.name), ["Label", "Mode", "Attach"]);
  } finally { await page.close(); }
});

test("an input the header does not declare can be added, and taken away again", async () => {
  const page = await open("?scripts");
  try {
    const editor = await edit(page, "Announce");
    await editor.getByText("Queue · NZB_ADDED", { exact: true }).waitFor();
    await editor.getByText(
      "This instance's inputs no longer match what the script's header declares. Re-apply from header to bring them in line; saved values are kept.",
      { exact: true },
    ).waitFor();
    assert.equal(await field(editor, "Legacy").inputValue(), "1");
    await editor.getByText("Not declared by the script's header.", { exact: true }).waitFor();
    // Only what the header does not ask for can be removed.
    assert.equal(await editor.getByRole("button", { name: /^Remove / }).count(), 1);
    const name = field(editor, "New input name");
    const secret = editor.getByRole("checkbox", { name: "The new input is a secret", exact: true });
    assert.equal(await action(editor, "Add input").isDisabled(), true);
    await name.fill("bad name");
    await action(editor, "Add input").click();
    await editor.getByRole("alert").filter({ hasText: "Use letters, digits, - and _, starting with a letter. Dots may join such parts." }).waitFor();
    // A name is taken whatever its case, and Enter adds as the button does.
    await name.fill("label");
    await editor.getByRole("alert").waitFor({ state: "detached" });
    await name.press("Enter");
    await editor.getByRole("alert").filter({ hasText: "This instance already has an input with that name." }).waitFor();
    await name.fill("Extra.key");
    await secret.click();
    await action(editor, "Add input").click();
    await secretField(editor, "Extra.key").waitFor();
    assert.equal(await secretField(editor, "Extra.key").getAttribute("type"), "password");
    assert.equal(await name.inputValue(), "");
    assert.equal(await secret.isChecked(), false);
    await secretField(editor, "Extra.key").fill("fixture-extra-1");
    await action(editor, "Remove Legacy").click();
    await field(editor, "Legacy").waitFor({ state: "detached" });
    await action(editor, "Save").click();
    await status(page, "Announce saved").waitFor();
    // The secret the header declares and the instance was never given goes along unset, between what was saved and what was added.
    assert.deepEqual((await daemon(page)).requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "queued", secret: false }, { name: "Token", value: null, secret: true },
      { name: "Extra.key", value: "fixture-extra-1", secret: true },
    ]);
    assert.deepEqual((await daemon(page)).instances.find((entry) => entry.id === "4").inputs, [
      { name: "Label", value: "queued", secret: false }, { name: "Extra.key", value: "fixture-extra-1", secret: true },
    ]);
  } finally { await page.close(); }
});

test("an instance trades places with its neighbour, and a second move builds on the first", async () => {
  const page = await open("?scripts");
  try {
    const post = rows(group(page, "Post-processing"));
    await post.first().and(row(page, "Notify")).waitFor();
    await action(row(page, "Notify"), "Move down").click();
    await post.nth(1).and(row(page, "Notify")).waitFor();
    await action(row(page, "Notify"), "Move down").click();
    await post.nth(2).and(row(page, "Notify")).waitFor();
    await action(row(page, "Retired"), "Move up").click();
    await post.first().and(row(page, "Retired")).waitFor();
    // The daemon orders a trigger's instances together, so every one of them is named each time.
    assert.deepEqual((await daemon(page)).requests, [
      { name: "ReorderScriptInstances", variables: { trigger: "POST_PROCESSING", ids: ["2", "1", "3"] } },
      { name: "ReorderScriptInstances", variables: { trigger: "POST_PROCESSING", ids: ["2", "3", "1"] } },
      { name: "ReorderScriptInstances", variables: { trigger: "POST_PROCESSING", ids: ["3", "2", "1"] } },
    ]);
    // Moving a row does not open it.
    assert.equal(await page.getByRole("dialog").count(), 0);
  } finally { await page.close(); }
});

test("the switch in a row turns its instance on or off without opening it, and keeps what is saved in it", async () => {
  // Each edit gets a page of its own: no earlier save or refetch is in flight when it is made.
  for (const [name, enabled, sent] of [
    ["Tidy tv", true, { id: "2", input: {
      name: "Tidy tv", script: "cleanup.sh", trigger: "POST_PROCESSING", queueEvent: null, inputs: [],
      categories: ["tv"], enabled: true, blocking: false, timeoutSeconds: 600,
    } }],
    // A secret goes back without a value, which keeps the one saved.
    ["Notify", false, { id: "1", input: {
      name: "Notify", script: "notify.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [
        { name: "Label", value: "fixture", secret: false }, { name: "Token", value: null, secret: true },
        { name: "Mode", value: "quiet", secret: false }, { name: "Attach", value: "no", secret: false },
      ],
      categories: [], enabled: false, blocking: true, timeoutSeconds: null,
    } }],
  ]) {
    const page = await open("?scripts");
    try {
      const toggle = table(page).getByRole("switch", { name: `${name} enabled`, exact: true });
      await toggle.click();
      await table(page).locator(`[role="switch"][aria-checked="${enabled}"]`).and(toggle).waitFor();
      assert.equal(await page.getByRole("dialog").count(), 0);
      const held = await daemon(page);
      assert.deepEqual(held.requests, [{ name: "UpdateScriptInstance", variables: sent }]);
      assert.equal(held.instances[0].inputs[1].value, "fixture-token-1");
    } finally { await page.close(); }
  }
});

test("deleting an instance asks first, and says what goes with it", async () => {
  const page = await open("?scripts");
  try {
    const confirm = page.getByRole("dialog", { name: "Delete instance", exact: true });
    await action(row(page, "Feed intake"), "Delete").click();
    await confirm.getByText("Feed intake", { exact: true }).waitFor();
    await confirm.getByText(
      "The instance is removed, along with any schedule rule that runs it and its place on any feed. The script file is not touched.",
      { exact: true },
    ).waitFor();
    await action(confirm, "Cancel").click();
    await confirm.waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests, []);

    await action(row(page, "Feed intake"), "Delete").click();
    await action(confirm, "Delete").click();
    await status(page, "Feed intake deleted").waitFor();
    await row(page, "Feed intake").waitFor({ state: "detached" });
    // Its heading goes with its last instance, and its script is back among those with none.
    assert.equal(await group(page, "Feed").count(), 0);
    await unusedRow(page, "intake.py").waitFor();
    assert.deepEqual((await daemon(page)).requests, [{ name: "DeleteScriptInstance", variables: { id: "7" } }]);

    // The editor's own Delete asks the same question, and the editor goes with the instance.
    const editor = await edit(page, "Retired");
    await editor.getByText("the script is no longer in the scripts directory", { exact: true }).waitFor();
    await action(editor, "Script").getByText("retired.py · missing", { exact: true }).waitFor();
    await action(editor, "Delete").click();
    await confirm.getByText("Retired", { exact: true }).waitFor();
    await action(confirm, "Delete").click();
    await status(page, "Retired deleted").waitFor();
    await editor.waitFor({ state: "detached" });
    await confirm.waitFor({ state: "detached" });
    await row(page, "Retired").waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests.at(-1), { name: "DeleteScriptInstance", variables: { id: "3" } });
  } finally { await page.close(); }
});

test("a delete or a re-apply the daemon refuses says so inside the question", async () => {
  const page = await open("?scripts&stale");
  try {
    for (const [name, button, title] of [
      ["Feed intake", "Delete", "Delete instance"],
      ["Announce", "Re-apply from header", "Re-apply from header"],
    ]) {
      await action(row(page, name), button).click();
      const confirm = page.getByRole("dialog", { name: title, exact: true });
      // A question asked again starts without the last one's answer.
      await confirm.getByText(name, { exact: true }).waitFor();
      assert.equal(await confirm.getByRole("alert").count(), 0);
      await action(confirm, button).click();
      await confirm.getByRole("alert").filter({ hasText: "script instance does not exist" }).waitFor();
      assert.equal(await page.locator("#status").textContent(), "");
      await action(confirm, "Cancel").click();
      await confirm.waitFor({ state: "detached" });
      assert.equal(await row(page, name).count(), 1);
    }
  } finally { await page.close(); }
});

test("an instance its header has moved away from is brought back in line, from its row or its editor", async () => {
  for (const fromEditor of [false, true]) {
    const page = await open("?scripts");
    try {
      const drift = table(page).getByText("Inputs differ from the script's header", { exact: true });
      await drift.waitFor();
      if (fromEditor) {
        await action(await edit(page, "Announce"), "Re-apply from header").click();
      } else {
        await action(row(page, "Announce"), "Re-apply from header").click();
      }
      const confirm = page.getByRole("dialog", { name: "Re-apply from header", exact: true });
      await confirm.getByText("Announce", { exact: true }).waitFor();
      await confirm.getByText(
        "Saved values are kept, new inputs are added at their defaults, and inputs the header no longer declares are dropped. The trigger does not change.",
        { exact: true },
      ).waitFor();
      await action(confirm, "Re-apply from header").click();
      await status(page, "Announce re-applied from its header").waitFor();
      // The note and the action go once the instance matches its header again, and the editor with them.
      await drift.waitFor({ state: "detached" });
      assert.equal(await page.getByRole("dialog").count(), 0);
      assert.equal(await action(table(page), "Re-apply from header").count(), 0);
      const held = await daemon(page);
      assert.deepEqual(held.requests, [{ name: "ReapplyScriptHeader", variables: { id: "4" } }]);
      // The saved value is kept, the inputs it lacked arrive at their defaults, and the one never declared is gone.
      assert.deepEqual(held.instances.find((entry) => entry.id === "4").inputs, [
        { name: "Label", value: "queued", secret: false }, { name: "Mode", value: "quiet", secret: false },
        { name: "Attach", value: "no", secret: false },
      ]);
    } finally { await page.close(); }
  }
});

test("set up from header creates an instance for each trigger the header declares that has none", async () => {
  const page = await open("?scripts");
  try {
    await action(unusedRow(page, "archive.py"), "Set up from header").click();
    await status(page, "2 instances created for Archive").waitFor();
    // One under each heading the header names, last in its order and named after the file.
    for (const [title, index] of [["Post-processing", 3], ["Schedule", 1]]) {
      const created = rows(group(page, title)).nth(index);
      await created.waitFor();
      assert.equal(await created.getByText("archive.py", { exact: true }).count(), 2, title);
    }
    assert.equal(await unusedRow(page, "archive.py").count(), 0);
    let held = await daemon(page);
    assert.deepEqual(held.requests, [{ name: "SetUpScriptFromHeader", variables: { script: "archive.py" } }]);
    // The header's defaults are saved; a secret has no value to copy.
    assert.deepEqual(held.instances.slice(-2).map((entry) => [entry.trigger, entry.inputs]), [
      ["POST_PROCESSING", [{ name: "Target", value: "/fixture/archive", secret: false }]],
      ["SCHEDULER", [{ name: "Target", value: "/fixture/archive", secret: false }]],
    ]);

    // The editor of a new instance offers the same, while its script has a declared trigger with no instance.
    await action(controls(page), "Create instance").click();
    const editor = creator(page);
    await action(editor, "Script").getByText("Choose a script", { exact: true }).waitFor();
    assert.equal(await action(editor, "Set up from header").count(), 0);
    await pick(page, editor, "Script", "Nightly report · nightly.py");
    await action(editor, "Trigger").getByText("Schedule · declared", { exact: true }).waitFor();
    assert.equal(await action(editor, "Set up from header").count(), 0);
    await pick(page, editor, "Script", "Notify · notify.py");
    await action(editor, "Set up from header").click();
    await status(page, "1 instance created for Notify").waitFor();
    await editor.waitFor({ state: "detached" });
    const created = rows(group(page, "Queue · NZB_DOWNLOADED")).first();
    await created.waitFor();
    assert.equal(await created.getByText("notify.py", { exact: true }).count(), 2);
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), { name: "SetUpScriptFromHeader", variables: { script: "notify.py" } });
    const made = held.instances.at(-1);
    assert.deepEqual([made.name, made.trigger, made.queueEvent], ["notify.py", "QUEUE", "NZB_DOWNLOADED"]);
  } finally { await page.close(); }
});

// A test run does nothing until the fixture's daemon is told to move it along;
// the dialog sees each step the next time it reads the run.
const advance = (page, step) => page.evaluate((flag) => { window.scriptsFixture.tests.at(-1)[flag] = true; }, step);
const testDialog = (page, name) => page.getByRole("dialog", { name: `Test ${name}`, exact: true });

test("testing an instance runs it once against made-up inputs and applies nothing it asks for", async () => {
  const page = await open("?scripts");
  try {
    await action(row(page, "Notify"), "Test").click();
    const dialog = testDialog(page, "Notify");
    const section = (name) => dialog.getByRole("region", { name, exact: true });
    await dialog.getByText("Running", { exact: true }).waitFor();
    for (const text of [
      "notify.py", "The instance as saved, run once against made-up inputs. Nothing the script asks for is applied.",
      "Post-processing", "5m", "NZBGet",
    ]) assert.equal(await dialog.getByText(text, { exact: true }).count(), 1, text);
    // It has neither an exit code nor a duration until it ends.
    assert.equal(await dialog.getByText("—", { exact: true }).count(), 2);
    for (const [name, value] of [
      ["NZBPP_CATEGORY", "tv"], ["NZBPP_DIRECTORY", "/fixture/scratch/test-1"], ["NZBPP_NZBNAME", "weaver-test-download"],
    ]) {
      for (const text of [name, value]) assert.equal(await section("Simulated inputs").getByText(text, { exact: true }).count(), 1, text);
    }
    await section("Simulated inputs").getByText("made up for this run · the instance's own inputs are left out", { exact: true }).waitFor();
    for (const text of ["/fixture/scratch/test-1", "weaver-test-download.nzb"]) {
      assert.equal(await section("Arguments").getByText(text, { exact: true }).count(), 1, text);
    }
    await section("Run log").getByText("The script has printed nothing yet.", { exact: true }).waitFor();
    assert.equal(await section("What the script asked for").count(), 0);
    // While it runs it can be cancelled, and not started again.
    assert.equal(await action(dialog, "Cancel test").count(), 1);
    assert.equal(await action(dialog, "Run again").count(), 0);

    // What the script prints arrives while it is still running.
    await advance(page, "printed");
    await section("Run log").getByText("resolved 1 recipient").waitFor();
    await section("Run log").getByText("[REDACTED]").waitFor();
    assert.equal(await action(dialog, "Cancel test").count(), 1);

    await advance(page, "finished");
    await dialog.getByText("SUCCEEDED", { exact: true }).waitFor();
    for (const text of ["0", "1.5s"]) assert.equal(await dialog.getByText(text, { exact: true }).count(), 1, text);
    await section("Run log").getByText("notified 1 recipient").waitFor();
    for (const text of ["[NZB] FINALDIR=/fixture/final", "[NZB] MARK=GOOD", "none of it was applied"]) {
      assert.equal(await section("What the script asked for").getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await action(dialog, "Cancel test").count(), 0);
    assert.equal(await action(dialog, "Run again").isDisabled(), false);
    await shot(page, "test-dialog");
    // One run was started, and nothing the script asked for was sent on to the instance.
    let held = await daemon(page);
    assert.deepEqual(held.tests.map((entry) => [entry.run.id, entry.run.instanceId, entry.run.status]), [["test-1", "1", "SUCCEEDED"]]);
    assert.deepEqual(held.requests, []);

    // A second run replaces the first, and can be cancelled while it is going.
    await action(dialog, "Run again").click();
    await action(dialog, "Cancel test").waitFor();
    assert.equal(await dialog.getByText("SUCCEEDED", { exact: true }).count(), 0);
    assert.equal(await section("What the script asked for").count(), 0);
    await action(dialog, "Cancel test").click();
    await dialog.getByText("CANCELLED", { exact: true }).waitFor();
    await action(dialog, "Run again").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.tests.map((entry) => [entry.run.id, entry.cancelled]), [["test-1", false], ["test-2", true]]);
    await action(dialog, "Close").click();
    await dialog.waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests, []);
  } finally { await page.close(); }
});

test("closing the test of a run still going ends the run", async () => {
  const page = await open("?scripts");
  try {
    await action(row(page, "Tidy tv"), "Test").click();
    const dialog = testDialog(page, "Tidy tv");
    await dialog.getByText("Running", { exact: true }).waitFor();
    // The instance's own timeout bounds its test.
    for (const text of ["cleanup.sh", "10m", "SABnzbd"]) assert.equal(await dialog.getByText(text, { exact: true }).count(), 1, text);
    await action(dialog, "Close").click();
    await dialog.waitFor({ state: "detached" });
    await page.waitForFunction(() => window.scriptsFixture.tests.at(-1).cancelled);
    assert.deepEqual((await daemon(page)).tests.map((entry) => [entry.run.instanceId, entry.run.status]), [["2", "CANCELLED"]]);
  } finally { await page.close(); }
});

test("a test run the daemon no longer has says so, and can be run again", async () => {
  const page = await open("?scripts");
  try {
    await action(row(page, "Nightly report"), "Test").click();
    const dialog = testDialog(page, "Nightly report");
    await dialog.getByText("Running", { exact: true }).waitFor();
    await advance(page, "forgotten");
    await dialog.getByRole("alert").filter({ hasText: "The daemon no longer has this test run." }).waitFor();
    // It is no longer running, and it has no outcome either.
    assert.equal(await dialog.getByText("Running", { exact: true }).count(), 0);
    assert.equal(await dialog.getByText("—", { exact: true }).count(), 3);
    assert.equal(await action(dialog, "Cancel test").count(), 0);
    await action(dialog, "Run again").click();
    await action(dialog, "Cancel test").waitFor();
    assert.equal(await dialog.getByRole("alert").count(), 0);
    assert.equal((await daemon(page)).tests.length, 2);
  } finally { await page.close(); }
});

test("a job's script runs group by event, name the instance that ran and open the retained output", async () => {
  const page = await open("?job");
  try {
    const runs = page.getByRole("region", { name: "Script runs", exact: true });
    await runs.getByText("4 runs", { exact: true }).waitFor();
    const group = (label) => page.getByRole("group").filter({ has: page.getByText(label, { exact: true }) });
    await group("queue:NZB_ADDED (1)").getByText("queued fixture job", { exact: true }).waitFor();
    const post = group("post_processing (3)");
    for (const status of ["SUCCEEDED", "WARNING", "FAILED"]) await post.getByText(status, { exact: true }).waitFor();
    // A run is called by its instance, with the script file beside it; one whose instance is gone, by its script alone.
    for (const [scope, texts] of [
      [group("queue:NZB_ADDED (1)"), ["Announce", "notify.py"]],
      [post, ["Notify", "notify.py", "Tidy tv", "cleanup.sh", "retired.py"]],
    ]) {
      for (const text of texts) assert.equal(await scope.getByText(text, { exact: true }).count(), 1, text);
    }
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
    for (const header of ["Finished", "Instance", "Trigger", "Job", "Status", "Exit code", "Duration"]) {
      assert.equal(await runsTable(page).getByText(header, { exact: true }).count(), 1);
    }
    await runsTable(page).getByText("newest first", { exact: true }).waitFor();
    // A run is called by its instance, with the script file beside it.
    for (const [index, instance, script, trigger, status, exitCode, duration] of [
      [0, "Nightly report", "nightly.py", "Schedule", "SUCCEEDED", "0", "1m 35s"],
      // An instance named after its file reads as the file alone, as does a run with no instance.
      [1, null, "intake.py", "Scan", "SUCCEEDED", "0", "1.5s"],
      [2, "Notify", "notify.py", "Post-processing", "SUCCEEDED", "0", "240 ms"],
      [3, null, "retired.py", "Post-processing", "FAILED", "—", "240 ms"],
      [4, "Announce", "notify.py", "Queue · NZB_ADDED", "SUCCEEDED", "0", "240 ms"],
      [5, "Tidy tv", "cleanup.sh", "Post-processing", "WARNING", "3", "240 ms"],
      [6, "Feed intake", "intake.py", "Feed", "SUCCEEDED", "0", "240 ms"],
      [7, null, "sweep.sh", "Queue · FILE_DOWNLOADED", "SUCCEEDED", "0", "240 ms"],
    ]) {
      for (const text of [instance, script, trigger, status, exitCode, duration]) {
        if (text !== null) assert.equal(await rows.nth(index).getByText(text, { exact: true }).count(), 1, `row ${index}: ${text}`);
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
    // A run's record is titled by its instance, or by its script when it has none.
    const record = (name) => page.getByRole("dialog", { name, exact: true });
    const shut = async (dialog) => {
      await dialog.getByRole("button", { name: "Close", exact: true }).click();
      await dialog.waitFor({ state: "detached" });
    };
    const retained = (dialog) => dialog.getByRole("button", { name: "Show retained output", exact: true });

    await rows.nth(2).getByText("Post-processing", { exact: true }).click();
    const notify = record("Notify");
    await notify.waitFor();
    for (const text of ["notify.py", "post_processing", "Post-processing", "Blocking", "SUCCEEDED", "0", "240 ms", "NZBGet"]) {
      assert.equal(await notify.getByText(text, { exact: true }).count(), 1, text);
    }
    // The instance is named in the title and again beside the script it ran.
    assert.equal(await notify.getByText("Notify", { exact: true }).count(), 2);
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

    // A failed run says why, and has neither an exit code nor output to fetch. Its instance is gone too.
    await rows.nth(3).getByText("FAILED", { exact: true }).click();
    const retired = record("retired.py");
    await retired.waitFor();
    for (const text of ["script is no longer in the scripts directory", "FAILED", "Full output is no longer retained."]) {
      assert.equal(await retired.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await retired.getByText("—", { exact: true }).count(), 2);
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
    const cleanup = record("Tidy tv");
    await cleanup.waitFor();
    for (const text of ["cleanup.sh", "Fire and forget", "WARNING", "3", "SABnzbd", "Capture limit reached; output was truncated."]) {
      assert.equal(await cleanup.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await cleanup.getByRole("link", { name: "#9", exact: true }).getAttribute("href"), "/jobs/9");
    assert.equal(await retained(cleanup).count(), 1);
  } finally { await page.close(); }
});

test("the settings search narrows the loaded runs by script or by instance", async () => {
  for (const search of ["cleanup", "tidy"]) {
    const page = await open(`?runs&search=${search}`);
    try {
      const rows = runRows(page);
      await rows.first().and(rowsHolding(page, "cleanup.sh")).waitFor();
      assert.equal(await rows.count(), 1);
    } finally { await page.close(); }
  }
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

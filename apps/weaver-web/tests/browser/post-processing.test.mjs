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
async function shot(page, name, { resize = true } = {}) {
  if (!process.env.SCRIPTS_SCREENSHOT_DIR) return;
  const path = join(process.env.SCRIPTS_SCREENSHOT_DIR, `${name}.png`);
  // An open menu is taken as it stands: resizing the window would close it.
  if (!resize) {
    await page.screenshot({ path });
    return;
  }
  // The table and a dialog's body scroll inside the window; a taller one holds the whole of either.
  await page.setViewportSize({ width: 1600, height: 1500 });
  await page.screenshot({ path });
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
  const scripts = ["Jobs"];
  for (const [query, shown, absent, buttons] of [
    ["", configuration, scripts, []],
    ["?scripts", scripts, configuration, ["Refresh", "Add job"]],
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
      ["Retained runs per download", "32"], ["Retain failed runs", "8"],
    ]) assert.equal(await limit(name).inputValue(), value);
    assert.deepEqual(await events.getByRole("spinbutton").evaluateAll((fields) => fields.map((field) => field.getAttribute("aria-label"))),
      ["Concurrent event scripts", "Default event timeout", "File event interval", "Retained runs per download", "Retain failed runs"]);
    // The execution limit keeps a name of its own beside the event one.
    assert.equal(await page.getByRole("spinbutton", { name: "Concurrent scripts", exact: true }).count(), 1);
    const save = page.getByRole("button", { name: "Save changes", exact: true });
    assert.equal(await save.isDisabled(), true);
    // A number field commits what was typed when focus leaves it, held to the range the daemon accepts.
    await limit("File event interval").fill("-1");
    await limit("File event interval").blur();
    await limit("Concurrent event scripts").fill("12");
    await limit("Concurrent event scripts").blur();
    await limit("Retain failed runs").fill("200");
    await limit("Retain failed runs").blur();
    await save.click();
    await page.getByRole("contentinfo").getByText("Saved", { exact: true }).waitFor();
    const settings = await stored(page);
    assert.equal(settings.fileDownloadedEventInterval, -1);
    assert.equal(settings.eventScriptConcurrency, 8);
    assert.equal(settings.scriptOutputFailedRunsPerJob, 128);
    assert.equal(await limit("Concurrent event scripts").inputValue(), "8");
    assert.equal(await limit("Retain failed runs").inputValue(), "128");
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
      "A global script is a job with no category of its own, and runs ahead of a category's own. This decides whether it also runs for a download whose category has scripts.",
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

test("a retention limit lowered below what is saved warns that saving deletes runs", async () => {
  const page = await open();
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    const limit = (name) => events.getByRole("spinbutton", { name, exact: true });
    const warning = "Lowering this deletes the older runs of every download as soon as you save.";
    const warnings = events.getByText(warning, { exact: true });
    await events.getByText("How many of the newest runs are kept for each download, and for each scan, schedule or feed.", { exact: true }).waitFor();
    await events.getByText("Failed runs kept past that count for each download, so failures stay inspectable.", { exact: true }).waitFor();
    assert.equal(await warnings.count(), 0);
    const set = async (name, value) => { await limit(name).fill(value); await limit(name).blur(); };

    // Raising a limit deletes nothing, so it says nothing.
    await set("Retained runs per download", "64");
    assert.equal(await warnings.count(), 0);
    await set("Retained runs per download", "16");
    await events.getByTestId("scriptOutputRunsPerJob-lowered").getByText(warning, { exact: true }).waitFor();
    await set("Retain failed runs", "2");
    await events.getByTestId("scriptOutputFailedRunsPerJob-lowered").waitFor();
    assert.equal(await warnings.count(), 2);
    await shot(page, "retention-lowered");

    // Back to what is saved, the warning goes.
    await set("Retained runs per download", "32");
    await events.getByTestId("scriptOutputRunsPerJob-lowered").waitFor({ state: "detached" });
    assert.equal(await warnings.count(), 1);
    await set("Retain failed runs", "8");
    await warnings.first().waitFor({ state: "detached" });

    // Once a lower limit is saved it is what is kept, and nothing is lowered any more.
    await set("Retained runs per download", "16");
    await page.getByRole("button", { name: "Save changes", exact: true }).click();
    await status(page, "Saved").waitFor();
    assert.equal((await stored(page)).scriptOutputRunsPerJob, 16);
    await warnings.first().waitFor({ state: "detached" });
  } finally { await page.close(); }
});

test("the settings search reaches an event script limit", async () => {
  const page = await open("?search=retained");
  try {
    const events = page.getByRole("region", { name: "Event scripts and output retention", exact: true });
    await events.getByRole("spinbutton", { name: "Retained runs per download", exact: true }).waitFor();
    assert.equal(await events.getByRole("spinbutton").count(), 1);
    assert.equal(await page.getByRole("region").count(), 1);
  } finally { await page.close(); }
});

// The scripts screen: its one table, the heading a trigger's instances sit under,
// the rows of either, and the row holding a given text. A row is a button that
// opens its editor, and the controls inside it are buttons of their own.
const table = (page) => page.getByRole("region", { name: "Jobs", exact: true });
const group = (page, title) => table(page).getByRole("region", { name: title, exact: true });
const rows = (scope) => scope.locator('[role="button"]');
const row = (page, name) => rows(table(page)).filter({ has: page.getByText(name, { exact: true }) });
const headings = (page) => table(page).getByRole("region").evaluateAll((sections) => sections.map((section) => section.getAttribute("aria-label")));
/** Opens the editor of the instance called `name`, which the dialog is then titled by. */
async function edit(page, name) {
  await row(page, name).getByText(name, { exact: true }).first().click();
  const editor = page.getByRole("dialog", { name, exact: true });
  await editor.waitFor();
  return editor;
}
const creator = (page) => page.getByRole("dialog", { name: "Add job", exact: true });
const field = (editor, name) => editor.getByRole("textbox", { name, exact: true });
// A password input has no role to be found by.
const secretField = (scope, name) => scope.getByLabel(name, { exact: true });
const secretBox = (editor, name) => editor.getByRole("checkbox", { name: `${name} is a secret`, exact: true });
const secretDialog = (page, name) => page.getByRole("dialog", { name, exact: true });

test("the scripts screen is one table of instances, under a heading for each thing that starts them", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    for (const header of ["Job name", "Script", "Categories", "Run mode", "Timeout", "Enabled"]) {
      assert.equal(await table(page).getByText(header, { exact: true }).count(), 1, header);
    }
    await table(page).getByText("jobs for every category run ahead of a category's own", { exact: true }).waitFor();
    assert.deepEqual(await headings(page), [
      "Post-processing", "Queue · NZB_ADDED", "Queue · NZB_DELETED", "Schedule", "Feed",
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
    assert.deepEqual(await controls(page).getByRole("button").allTextContents(), ["Refresh", "Add job"]);
    assert.equal(await action(controls(page), "Add job").isDisabled(), false);
    assert.equal(await action(table(page), "Add job").count(), 0);
    // It is the list of jobs and nothing else: no script without one, and no file that could not be read.
    for (const file of ["archive.py", "plain.sh", "broken.py"]) assert.equal(await page.getByText(file, { exact: true }).count(), 0, file);
    assert.equal(await page.getByRole("main").getByRole("region").count(), 6);
    await shot(page, "scripts-table");
  } finally { await page.close(); }
});

test("a file that could not be read as a script is named where a script is chosen, not among the jobs", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    const unreadable = (scope) => scope.getByRole("group", { name: "Scripts that could not be read", exact: true });
    assert.equal(await unreadable(page).count(), 0);
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    for (const text of ["broken.py", "option 3 declares an unknown type"]) await unreadable(editor).getByText(text, { exact: true }).waitFor();
    // There are scripts to choose from, so nothing says there are none.
    assert.equal(await editor.getByText("No scripts found. Put one in the scripts directory, then refresh.", { exact: true }).count(), 0);
    await shot(page, "create-dialog-unreadable");
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
    // A saved job's editor is about its own script.
    assert.equal(await unreadable(await edit(page, "Notify")).count(), 0);
  } finally { await page.close(); }
});

test("the table says which way the instances for every category run", async () => {
  const page = await open("?scripts&cascade");
  try {
    await table(page).getByText("jobs for every category run only when the category has none of its own", { exact: true }).waitFor();
    assert.equal(await table(page).getByText("jobs for every category run ahead of a category's own", { exact: true }).count(), 0);
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
    // A job can still be started; its editor has no script to offer, so nothing can be saved.
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    await editor.getByText("No scripts found. Put one in the scripts directory, then refresh.", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), true);
  } finally { await page.close(); }
});

test("with no jobs the table says so and offers the first, as the top bar always does", async () => {
  const page = await open("?scripts&empty");
  try {
    await table(page).getByText("No jobs yet.", { exact: true }).waitFor();
    assert.equal(await page.getByRole("region").count(), 1);
    assert.equal(await rows(table(page)).count(), 0);
    assert.deepEqual(await table(page).getByRole("button").allTextContents(), ["Add job"]);
    assert.equal(await action(controls(page), "Add job").isDisabled(), false);
    await shot(page, "scripts-empty");
    // Either opens the new job's editor, which says there is no script for one to run yet.
    await action(table(page), "Add job").click();
    const editor = creator(page);
    await editor.getByText("No scripts found. Put one in the scripts directory, then refresh.", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
    await action(controls(page), "Add job").click();
    await editor.waitFor();
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
    await row(page, "Notify").waitFor();
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    await editor.getByText("new job", { exact: true }).waitFor();
    // Nothing can be saved until there is a script to run.
    await action(editor, "Script").getByText("Choose a script", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await editor.getByText("This job has no inputs.", { exact: true }).waitFor();
    await action(editor, "Script").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "Choose a script", "Notify · notify.py", "Cleanup · cleanup.sh", "Nightly report · nightly.py",
      "Intake filter · intake.py", "Archive · archive.py", "plain.sh",
    ]);
    await page.getByRole("menuitemradio", { name: "Archive · archive.py", exact: true }).click();

    // The first trigger the header declares, and every input it declares at its default.
    await action(editor, "Trigger").getByText("Post-processing", { exact: true }).waitFor();
    assert.equal(await action(editor, "Save").isDisabled(), false);
    assert.equal(await editor.getByText("This job has no inputs.", { exact: true }).count(), 0);
    assert.equal(await field(editor, "Target").inputValue(), "/fixture/archive");
    await editor.getByText("Where a finished download is copied. Default /fixture/archive.", { exact: true }).waitFor();
    // A secret is never filled in from the header: it links one of the saved secrets, and none is chosen yet.
    await action(editor, "Key").getByText("Choose a secret", { exact: true }).waitFor();
    assert.equal(await secretBox(editor, "Key").isChecked(), true);
    // What the header calls plain is a value, with no box to make it a secret.
    assert.equal(await secretBox(editor, "Target").count(), 0);
    await editor.getByText("Linked from Settings · Scripts · Secrets. The script is given its value when it runs; it is never shown here.", { exact: true }).waitFor();
    await action(editor, "Key").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "Choose a secret", "Notify token", "Spare key", "Create new secret…",
    ]);
    await page.getByRole("menuitemradio", { name: "Choose a secret", exact: true }).click();
    // Every trigger is offered by its plain name, whatever the header asks for.
    await action(editor, "Trigger").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "Post-processing", "Queue", "Scan", "Schedule", "Feed",
    ]);
    await page.getByRole("menuitemradio", { name: "Post-processing", exact: true }).click();

    await field(editor, "Job name").fill("Archive movies");
    await pick(page, editor, "Key", "Spare key");
    // Categories are one dropdown: it says every category until some are ticked, then names them.
    const categories = action(editor, "Categories");
    await categories.getByText("Every category", { exact: true }).waitFor();
    await categories.click();
    const choices = page.getByRole("menuitemcheckbox");
    assert.deepEqual(await choices.allTextContents(), ["movies", "tv"]);
    assert.deepEqual(await choices.evaluateAll((all) => all.map((one) => one.getAttribute("aria-checked"))), ["false", "false"]);
    // Ticking leaves the menu open, so several can be chosen in one go, and a tick comes off again.
    await choices.nth(1).click();
    await choices.nth(0).click();
    await categories.getByText("movies, tv", { exact: true }).waitFor();
    await shot(page, "instance-editor-categories", { resize: false });
    await choices.nth(1).click();
    await categories.getByText("movies", { exact: true }).waitFor();
    assert.deepEqual(await choices.evaluateAll((all) => all.map((one) => one.getAttribute("aria-checked"))), ["true", "false"]);
    await page.keyboard.press("Escape");
    await choices.first().waitFor({ state: "detached" });
    await editor.getByRole("radio", { name: "Fire and forget", exact: true }).click();
    const timeout = editor.getByRole("spinbutton", { name: "Timeout", exact: true });
    await timeout.fill("120");
    await timeout.blur();
    await shot(page, "create-dialog");
    await action(editor, "Save").click();
    await status(page, "Archive movies created").waitFor();
    await editor.waitFor({ state: "detached" });

    // It runs last under its trigger's heading.
    const created = rows(group(page, "Post-processing")).nth(3);
    await created.and(row(page, "Archive movies")).waitFor();
    for (const text of ["archive.py", "movies", "Fire and forget", "2m"]) {
      assert.equal(await created.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await page.getByText("fixture-token").count(), 0);
    assert.deepEqual((await daemon(page)).requests, [{ name: "CreateScriptInstance", variables: { input: {
      name: "Archive movies", script: "archive.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [{ name: "Target", value: "/fixture/archive" }, { name: "Key", secretId: "s2" }],
      categories: ["movies"], enabled: true, blocking: false, timeoutSeconds: 120,
      schedule: { days: [], times: [], runAtStartup: false },
    } } }]);
  } finally { await page.close(); }
});

test("a queue instance names its event, and each input the header declares draws its own control", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    await pick(page, editor, "Script", "Archive · archive.py");
    await action(editor, "Script").getByText("Archive · archive.py", { exact: true }).waitFor();
    assert.equal(await action(editor, "Queue event").count(), 0);
    await pick(page, editor, "Trigger", "Queue");
    // The header declares no queue event, so the first of them all is the one offered.
    await action(editor, "Queue event").getByText("NZB_ADDED", { exact: true }).waitFor();
    await action(editor, "Queue event").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "FILE_DOWNLOADED", "URL_COMPLETED", "NZB_MARKED", "NZB_ADDED", "NZB_NAMED", "NZB_DOWNLOADED", "NZB_DELETED",
    ]);
    await page.getByRole("menuitemradio", { name: "NZB_NAMED", exact: true }).click();
    // Saved with no name and no secret chosen: it takes its script's name, and the secret is not sent at all.
    await action(editor, "Save").click();
    await status(page, "archive.py created").waitFor();
    await rows(group(page, "Queue · NZB_NAMED")).first().and(row(page, "archive.py")).waitFor();
    assert.deepEqual(await headings(page), [
      "Post-processing", "Queue · NZB_ADDED", "Queue · NZB_NAMED", "Queue · NZB_DELETED", "Schedule", "Feed",
    ]);
    let held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), { name: "CreateScriptInstance", variables: { input: {
      name: "", script: "archive.py", trigger: "QUEUE", queueEvent: "NZB_NAMED",
      inputs: [{ name: "Target", value: "/fixture/archive" }],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
      schedule: { days: [], times: [], runAtStartup: false },
    } } });
    assert.equal(held.instances.at(-1).name, "archive.py");
    assert.deepEqual(held.instances.at(-1).inputs, [{ name: "Target", value: "/fixture/archive", secretId: null }]);

    // Each input the header declares draws the control its header asks for.
    await action(controls(page), "Add job").click();
    await pick(page, editor, "Script", "Notify · notify.py");
    await action(editor, "Trigger").getByText("Post-processing", { exact: true }).waitFor();
    assert.equal(await field(editor, "Label").inputValue(), "");
    await editor.getByText("Shown in the notification title. Required.", { exact: true }).waitFor();
    await action(editor, "Mode").getByText("quiet", { exact: true }).waitFor();
    assert.equal(await editor.getByRole("switch", { name: "Attach the log", exact: true }).isChecked(), false);
    await action(editor, "Token").getByText("Choose a secret", { exact: true }).waitFor();
    await field(editor, "Label").fill("downloads");
    await pick(page, editor, "Mode", "verbose");
    await editor.getByRole("switch", { name: "Attach the log", exact: true }).click();
    // The first event the header declares is the one offered, among all of them by their plain names.
    await pick(page, editor, "Trigger", "Queue");
    await action(editor, "Queue event").getByText("NZB_ADDED", { exact: true }).waitFor();
    await action(editor, "Queue event").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), [
      "FILE_DOWNLOADED", "URL_COMPLETED", "NZB_MARKED", "NZB_ADDED", "NZB_NAMED", "NZB_DOWNLOADED", "NZB_DELETED",
    ]);
    await page.getByRole("menuitemradio", { name: "NZB_DOWNLOADED", exact: true }).click();
    // Only a download has a category, so only its triggers can be narrowed to one.
    const categories = action(editor, "Categories");
    assert.equal(await categories.count(), 1);
    await pick(page, editor, "Trigger", "Schedule");
    await categories.waitFor({ state: "detached" });
    assert.equal(await action(editor, "Queue event").count(), 0);
    await pick(page, editor, "Trigger", "Queue");
    await action(editor, "Queue event").getByText("NZB_DOWNLOADED", { exact: true }).waitFor();
    await action(editor, "Save").click();
    await status(page, "notify.py created").waitFor();
    await rows(group(page, "Queue · NZB_DOWNLOADED")).first().and(row(page, "notify.py")).waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), { name: "CreateScriptInstance", variables: { input: {
      name: "", script: "notify.py", trigger: "QUEUE", queueEvent: "NZB_DOWNLOADED",
      inputs: [
        { name: "Label", value: "downloads" }, { name: "Mode", value: "verbose" }, { name: "Attach", value: "yes" },
      ],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
      schedule: { days: [], times: [], runAtStartup: false },
    } } });
  } finally { await page.close(); }
});

test("a job for downloads says there are no categories when none are defined", async () => {
  const page = await open("?scripts&nocategories");
  try {
    const editor = await edit(page, "Notify");
    await editor.getByText("No categories defined", { exact: true }).waitFor();
    assert.equal(await action(editor, "Categories").count(), 0);
    assert.equal(await editor.getByText("Every category", { exact: true }).count(), 0);
    await shot(page, "instance-editor-no-categories");
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
    // A category a job was saved with is still offered, so it can be taken off.
    const narrowed = await edit(page, "Tidy tv");
    await action(narrowed, "Categories").getByText("tv", { exact: true }).waitFor();
    await action(narrowed, "Categories").click();
    assert.deepEqual(await page.getByRole("menuitemcheckbox").allTextContents(), ["tv"]);
    assert.equal(await page.getByRole("menuitemcheckbox").getAttribute("aria-checked"), "true");
  } finally { await page.close(); }
});

test("a new job on the schedule is given when it runs, starting from the times its header asks for", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    const times = field(editor, "Run times");
    const days = editor.getByRole("group", { name: "Days", exact: true });
    const startup = editor.getByRole("switch", { name: "Also run at startup", exact: true });
    // Only a job on the schedule has times.
    await pick(page, editor, "Script", "Notify · notify.py");
    assert.equal(await times.count(), 0);
    await pick(page, editor, "Script", "Nightly report · nightly.py");
    await action(editor, "Trigger").getByText("Schedule", { exact: true }).waitFor();
    assert.equal(await times.inputValue(), "04:00, *:20");
    assert.deepEqual(await days.getByRole("button").allTextContents(), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]);
    assert.equal(await days.locator('[aria-pressed="true"]').count(), 0);
    assert.equal(await startup.isChecked(), false);
    await editor.getByText("Local time. HH:MM, or *:MM for every hour; separate several with commas.", { exact: true }).waitFor();
    await shot(page, "create-dialog-schedule");

    // What is not a time, and no time at all, are said before anything is created.
    await times.fill("04:00, 25:00");
    await action(editor, "Save").click();
    const invalid = editor.getByText("25:00 is not a time. Use HH:MM, or *:MM for every hour.", { exact: true });
    await invalid.waitFor();
    await times.fill("");
    await invalid.waitFor({ state: "detached" });
    await action(editor, "Save").click();
    await editor.getByText("Give this job a time to run at, or turn on running at startup.", { exact: true }).waitFor();
    assert.deepEqual((await daemon(page)).requests, []);

    await times.fill("06:30, *:15");
    await action(days, "Sat").click();
    await action(days, "Sun").click();
    assert.equal(await days.locator('[aria-pressed="true"]').count(), 2);
    await startup.click();
    await field(editor, "Job name").fill("Weekend report");
    await action(editor, "Save").click();
    await status(page, "Weekend report created").waitFor();
    await editor.waitFor({ state: "detached" });
    await rows(group(page, "Schedule")).nth(1).and(row(page, "Weekend report")).waitFor();
    // The job carries its run times; no schedule rule is made for it.
    assert.deepEqual((await daemon(page)).requests, [
      { name: "CreateScriptInstance", variables: { input: {
        name: "Weekend report", script: "nightly.py", trigger: "SCHEDULER", queueEvent: null,
        inputs: [], categories: [], enabled: true, blocking: true, timeoutSeconds: null,
        schedule: { days: ["sat", "sun"], times: ["06:30", "*:15"], runAtStartup: true },
      } } },
    ]);

    // Saved, it opens on the run times it was given, which are changed here.
    const saved = await edit(page, "Weekend report");
    assert.equal(await field(saved, "Run times").inputValue(), "06:30, *:15");
    const savedDays = saved.getByRole("group", { name: "Days", exact: true });
    assert.equal(await savedDays.locator('[aria-pressed="true"]').count(), 2);
    assert.equal(await saved.getByRole("switch", { name: "Also run at startup", exact: true }).isChecked(), true);
  } finally { await page.close(); }
});

test("a script whose header asks for no time starts with none, and a job may run at startup alone", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    // The header's times belong to its script: a script with none starts with none.
    await pick(page, editor, "Script", "Cleanup · cleanup.sh");
    await pick(page, editor, "Trigger", "Schedule");
    assert.equal(await field(editor, "Run times").inputValue(), "");
    await editor.getByRole("switch", { name: "Also run at startup", exact: true }).click();
    await field(editor, "Job name").fill("Startup tidy");
    await action(editor, "Save").click();
    await status(page, "Startup tidy created").waitFor();
    const sent = (await daemon(page)).requests.at(-1);
    assert.equal(sent.name, "CreateScriptInstance");
    assert.deepEqual(sent.variables.input.schedule, { days: [], times: [], runAtStartup: true });
    const saved = await edit(page, "Startup tidy");
    assert.equal(await saved.getByRole("switch", { name: "Also run at startup", exact: true }).isChecked(), true);
  } finally { await page.close(); }
});

test("a saved job on the schedule shows its run times to change, and a job of another trigger has none", async () => {
  const page = await open("?scripts");
  try {
    let editor = await edit(page, "Nightly report");
    const times = field(editor, "Run times");
    assert.equal(await times.inputValue(), "*:20, 04:00");
    const days = editor.getByRole("group", { name: "Days", exact: true });
    assert.deepEqual(await days.locator('[aria-pressed="true"]').allTextContents(), ["Sat", "Sun"]);
    assert.equal(await editor.getByRole("switch", { name: "Also run at startup", exact: true }).isChecked(), true);
    await shot(page, "instance-editor-schedule");
    // A changed time is saved on the job itself.
    await times.fill("05:00");
    await action(days, "Sat").click();
    await action(editor, "Save").click();
    await status(page, "Nightly report saved").waitFor();
    const requests = (await daemon(page)).requests;
    assert.deepEqual(requests.map((request) => request.name), ["UpdateScriptInstance"]);
    assert.deepEqual(requests[0].variables.input.schedule, { days: ["sun"], times: ["05:00"], runAtStartup: true });
    editor = await edit(page, "Nightly report");
    assert.equal(await field(editor, "Run times").inputValue(), "05:00");
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
    // A job of another trigger has no run times.
    assert.equal(await (await edit(page, "Notify")).getByText("Run times", { exact: true }).count(), 0);
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

test("a secret input shows the secret it links and never its value, and can link another or a new one", async () => {
  const page = await open("?scripts");
  try {
    let editor = await edit(page, "Notify");
    // The instance's trigger sits beside its name, and its script and trigger are the ones saved.
    await action(editor, "Script").getByText("Notify · notify.py", { exact: true }).waitFor();
    await action(editor, "Trigger").getByText("Post-processing", { exact: true }).waitFor();
    assert.equal(await editor.getByText("Post-processing", { exact: true }).count(), 2);
    assert.equal(await field(editor, "Job name").inputValue(), "Notify");
    assert.equal(await field(editor, "Label").inputValue(), "fixture");
    // The secret input names the secret it links; nothing can be typed into it.
    await action(editor, "Token").getByText("Notify token", { exact: true }).waitFor();
    await editor.getByText("The service's access token. Linked from Settings · Scripts · Secrets. The script is given its value when it runs; it is never shown here.", { exact: true }).waitFor();
    await shot(page, "instance-editor-secret-linked");
    await field(editor, "Label").fill("renamed");
    await field(editor, "Job name").fill("Notify all");
    await action(editor, "Save").click();
    await status(page, "Notify all saved").waitFor();
    // The row under its new name is the instance as the daemon now holds it.
    await row(page, "Notify all").waitFor();
    let held = await daemon(page);
    assert.deepEqual(held.requests, [{ name: "UpdateScriptInstance", variables: { id: "1", input: {
      name: "Notify all", script: "notify.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [
        { name: "Label", value: "renamed" }, { name: "Token", secretId: "s1" },
        { name: "Mode", value: "quiet" }, { name: "Attach", value: "no" },
      ],
      categories: [], enabled: true, blocking: true, timeoutSeconds: null,
      schedule: { days: [], times: [], runAtStartup: false },
    } } }]);
    assert.deepEqual(held.instances[0].inputs[1], { name: "Token", value: "", secretId: "s1" });

    // A linked input offers no secret first, then the other secrets and a new one.
    editor = await edit(page, "Notify all");
    await action(editor, "Token").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["No secret", "Notify token", "Spare key", "Create new secret…"]);
    await shot(page, "instance-editor-secret-linked-picker", { resize: false });
    await page.getByRole("menuitemradio", { name: "Spare key", exact: true }).click();
    await action(editor, "Save").click();
    await editor.waitFor({ state: "detached" });
    await status(page, "Notify all saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs[1], { name: "Token", secretId: "s2" });

    // Choosing no secret unlinks it: the slot stays, and nothing is sent for it.
    editor = await edit(page, "Notify all");
    await pick(page, editor, "Token", "No secret");
    await action(editor, "Token").getByText("Choose a secret", { exact: true }).waitFor();
    await shot(page, "instance-editor-secret-unlinked");
    await action(editor, "Save").click();
    await editor.waitFor({ state: "detached" });
    await status(page, "Notify all saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "renamed" }, { name: "Mode", value: "quiet" }, { name: "Attach", value: "no" },
    ]);

    // A secret the instance never linked is offered unchosen, and one can be created on the spot.
    editor = await edit(page, "Log removal");
    await action(editor, "Queue event").getByText("NZB_DELETED", { exact: true }).waitFor();
    await action(editor, "Token").getByText("Choose a secret", { exact: true }).waitFor();
    await pick(page, editor, "Token", "Create new secret…");
    const created = secretDialog(page, "Add secret");
    await created.waitFor();
    await action(created, "Save").click();
    await created.getByText("Give the secret a name of up to 128 bytes.", { exact: true }).waitFor();
    await field(created, "Name").fill("Notify token");
    await secretField(created, "Value").fill("fixture-token-3");
    await action(created, "Save").click();
    // A name is taken whatever its case, and the daemon's refusal is said inside the dialog.
    await created.getByText("a secret named 'Notify token' already exists", { exact: true }).waitFor();
    await shot(page, "instance-editor-create-secret");
    await field(created, "Name").fill("Mail token");
    await action(created, "Save").click();
    await created.waitFor({ state: "detached" });
    await action(editor, "Token").getByText("Mail token", { exact: true }).waitFor();
    await action(editor, "Save").click();
    await editor.waitFor({ state: "detached" });
    await status(page, "Log removal saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.slice(-2), [
      { name: "CreateSecret", variables: { name: "Mail token", value: "fixture-token-3" } },
      { name: "UpdateScriptInstance", variables: { id: "5", input: {
        name: "Log removal", script: "notify.py", trigger: "QUEUE", queueEvent: "NZB_DELETED",
        inputs: [
          { name: "Label", value: "removed" }, { name: "Mode", value: "verbose" }, { name: "Attach", value: "yes" },
          { name: "Token", secretId: "s3" },
        ],
        categories: [], enabled: true, blocking: true, timeoutSeconds: null,
        schedule: { days: [], times: [], runAtStartup: false },
      } } },
    ]);
    assert.equal(await page.getByText("fixture-token").count(), 0);
  } finally { await page.close(); }
});

test("only an input the header takes for a secret has the secret box, which gives it a blank field or its choice back", async () => {
  const page = await open("?scripts");
  try {
    const editor = await edit(page, "Notify");
    // The header says which inputs are secrets; one it calls plain has no box to make it one.
    for (const name of ["Label", "Mode", "Attach"]) assert.equal(await secretBox(editor, name).count(), 0, name);
    // The only other box is the one beside the input being added.
    assert.equal(await editor.getByRole("checkbox").count(), 2);
    assert.equal(await secretBox(editor, "Token").isChecked(), true);
    // It takes an input for a secret by its name, so that one can be made plain, and back.
    await secretBox(editor, "Token").click();
    assert.equal(await field(editor, "Token").inputValue(), "");
    await secretBox(editor, "Token").click();
    await action(editor, "Token").getByText("Choose a secret", { exact: true }).waitFor();
    assert.equal(await field(editor, "Token").count(), 0);
    await secretBox(editor, "Token").click();
    await field(editor, "Token").fill("typed");
    await action(editor, "Save").click();
    await status(page, "Notify saved").waitFor();
    assert.deepEqual((await daemon(page)).requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "fixture" }, { name: "Token", value: "typed" },
      { name: "Mode", value: "quiet" }, { name: "Attach", value: "no" },
    ]);
  } finally { await page.close(); }
});

test("an input the header does not have is added as a value or as the job's own secret, which is never read back", async () => {
  const page = await open("?scripts");
  try {
    const editor = await edit(page, "Announce");
    await editor.getByText("Queue · NZB_ADDED", { exact: true }).waitFor();
    await editor.getByText(
      "This job's inputs no longer match what the script's header declares. Re-apply from header to bring them in line; saved values are kept.",
      { exact: true },
    ).waitFor();
    assert.equal(await field(editor, "Legacy").inputValue(), "1");
    await editor.getByText("Not in the script's header.", { exact: true }).waitFor();
    await shot(page, "instance-editor-drift");
    // Only what the header does not ask for can be removed.
    assert.equal(await editor.getByRole("button", { name: /^Remove / }).count(), 1);
    // An input is added in one step: its name, its value, and a box that makes the value this job's own secret.
    const name = field(editor, "New input name");
    const value = secretField(editor, "New input value");
    const own = editor.getByRole("checkbox", { name: "The new input is a secret", exact: true });
    assert.equal(await editor.getByRole("radiogroup").count(), 1);
    assert.equal(await own.isChecked(), false);
    assert.equal(await value.getAttribute("type"), "text");
    assert.equal(await action(editor, "Add input").isDisabled(), true);
    await name.fill("bad name");
    await action(editor, "Add input").click();
    await editor.getByRole("alert").filter({ hasText: "Use letters, digits, - and _, starting with a letter. Dots may join such parts." }).waitFor();
    // A name is taken whatever its case, and Enter adds as the button does.
    await name.fill("label");
    await editor.getByRole("alert").waitFor({ state: "detached" });
    await name.press("Enter");
    await editor.getByRole("alert").filter({ hasText: "This job already has an input with that name." }).waitFor();
    await name.fill("Retries");
    await value.fill("3");
    await value.press("Enter");
    assert.equal(await field(editor, "Retries").inputValue(), "3");
    assert.equal(await name.inputValue(), "");
    assert.equal(await value.inputValue(), "");
    // Ticked, the value is typed masked, and with nothing typed there is nothing to add.
    await own.click();
    assert.equal(await value.getAttribute("type"), "password");
    await name.fill("Extra.key");
    assert.equal(await action(editor, "Add input").isDisabled(), true);
    await value.fill("fixture-own-secret");
    await shot(page, "instance-editor-add-input");
    await action(editor, "Add input").click();
    // It joins the inputs masked, as this job's own, and the box is clear for the next one.
    const added = secretField(editor, "Extra.key");
    assert.equal(await added.getAttribute("type"), "password");
    assert.equal(await added.inputValue(), "fixture-own-secret");
    assert.equal(await own.isChecked(), false);
    assert.equal(await value.getAttribute("type"), "text");
    // What was added is a value or the job's own secret, and stays what it was added as.
    for (const each of ["Retries", "Extra.key"]) assert.equal(await secretBox(editor, each).count(), 0, each);
    assert.equal(await editor.getByRole("button", { name: /^Remove / }).count(), 3);
    await action(editor, "Remove Legacy").click();
    await field(editor, "Legacy").waitFor({ state: "detached" });
    // Each save renames the job, so the row opened next is one the daemon has answered with since.
    await field(editor, "Job name").fill("Announce 2");
    await action(editor, "Save").click();
    await status(page, "Announce 2 saved").waitFor();
    // The secret the header declares and the instance never linked is not sent; the added ones go as a value and
    // as the job's own secret, which is no secret of the Secrets screen.
    let held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "queued" }, { name: "Retries", value: "3" },
      { name: "Extra.key", value: "fixture-own-secret", secret: true },
    ]);
    assert.deepEqual(held.instances.find((entry) => entry.id === "4").inputs, [
      { name: "Label", value: "queued", secretId: null }, { name: "Retries", value: "3", secretId: null },
      { name: "Extra.key", value: "", secretId: null, sealed: true },
    ]);
    assert.equal(held.requests.filter((request) => request.name === "CreateSecret").length, 0);

    // Opened again, the saved secret is not read back: its field is blank, and left blank it is kept.
    let again = await edit(page, "Announce 2");
    let kept = secretField(again, "Extra.key");
    assert.equal(await kept.getAttribute("type"), "password");
    assert.equal(await kept.inputValue(), "");
    assert.equal(await kept.getAttribute("placeholder"), "Saved. Type to replace.");
    assert.equal(await page.getByText("fixture-own-secret").count(), 0);
    await shot(page, "instance-editor-own-secret-saved");
    await field(again, "Job name").fill("Announce 3");
    await action(again, "Save").click();
    await status(page, "Announce 3 saved").waitFor();
    held = await daemon(page);
    assert.deepEqual(held.requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "queued" }, { name: "Retries", value: "3" }, { name: "Extra.key", secret: true },
    ]);
    assert.deepEqual(held.instances.find((entry) => entry.id === "4").inputs.at(-1), {
      name: "Extra.key", value: "", secretId: null, sealed: true,
    });

    // A new value typed over it replaces it, and it can be taken away like any input added here.
    again = await edit(page, "Announce 3");
    kept = secretField(again, "Extra.key");
    await kept.fill("fixture-own-secret-2");
    await field(again, "Job name").fill("Announce 4");
    await action(again, "Save").click();
    await status(page, "Announce 4 saved").waitFor();
    assert.deepEqual((await daemon(page)).requests.at(-1).variables.input.inputs.at(-1), {
      name: "Extra.key", value: "fixture-own-secret-2", secret: true,
    });
    again = await edit(page, "Announce 4");
    await action(again, "Remove Extra.key").click();
    await secretField(again, "Extra.key").waitFor({ state: "detached" });
    await field(again, "Job name").fill("Announce 5");
    await action(again, "Save").click();
    await status(page, "Announce 5 saved").waitFor();
    assert.deepEqual((await daemon(page)).requests.at(-1).variables.input.inputs, [
      { name: "Label", value: "queued" }, { name: "Retries", value: "3" },
    ]);
    assert.equal(await page.getByText("fixture-own-secret").count(), 0);
    assert.equal(await page.getByText("fixture-token").count(), 0);
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
      schedule: { days: [], times: [], runAtStartup: false },
    } }],
    // A secret goes back as the link it is.
    ["Notify", false, { id: "1", input: {
      name: "Notify", script: "notify.py", trigger: "POST_PROCESSING", queueEvent: null,
      inputs: [
        { name: "Label", value: "fixture" }, { name: "Token", secretId: "s1" },
        { name: "Mode", value: "quiet" }, { name: "Attach", value: "no" },
      ],
      categories: [], enabled: false, blocking: true, timeoutSeconds: null,
      schedule: { days: [], times: [], runAtStartup: false },
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
      assert.equal(held.instances[0].inputs[1].secretId, "s1");
    } finally { await page.close(); }
  }
});

test("deleting an instance asks first, and says what goes with it", async () => {
  const page = await open("?scripts");
  try {
    const confirm = page.getByRole("dialog", { name: "Delete job", exact: true });
    await action(row(page, "Feed intake"), "Delete").click();
    await confirm.getByText("Feed intake", { exact: true }).waitFor();
    await confirm.getByText(
      "The job is removed, along with its place on any feed. The script file is not touched.",
      { exact: true },
    ).waitFor();
    await action(confirm, "Cancel").click();
    await confirm.waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests, []);

    await action(row(page, "Feed intake"), "Delete").click();
    await action(confirm, "Delete").click();
    await status(page, "Feed intake deleted").waitFor();
    await row(page, "Feed intake").waitFor({ state: "detached" });
    // Its heading goes with its last instance, and its script is not listed in its place.
    assert.equal(await group(page, "Feed").count(), 0);
    assert.equal(await page.getByText("intake.py", { exact: true }).count(), 0);
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
      ["Feed intake", "Delete", "Delete job"],
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
        { name: "Label", value: "queued", secretId: null }, { name: "Mode", value: "quiet", secretId: null },
        { name: "Attach", value: "no", secretId: null },
      ]);
    } finally { await page.close(); }
  }
});

test("set up from header creates an instance for each trigger the header declares that has none", async () => {
  const page = await open("?scripts");
  try {
    await row(page, "Notify").waitFor();
    await action(controls(page), "Add job").click();
    const editor = creator(page);
    await pick(page, editor, "Script", "Archive · archive.py");
    await action(editor, "Set up jobs from header").click();
    await status(page, "2 jobs created for Archive").waitFor();
    await editor.waitFor({ state: "detached" });
    // One under each heading the header names, last in its order and named after the file.
    for (const [title, index] of [["Post-processing", 3], ["Schedule", 1]]) {
      const created = rows(group(page, title)).nth(index);
      await created.waitFor();
      assert.equal(await created.getByText("archive.py", { exact: true }).count(), 2, title);
    }
    let held = await daemon(page);
    assert.deepEqual(held.requests, [{ name: "SetUpScriptFromHeader", variables: { script: "archive.py" } }]);
    // The header's defaults are saved; a secret has no value to copy.
    assert.deepEqual(held.instances.slice(-2).map((entry) => [entry.trigger, entry.inputs]), [
      ["POST_PROCESSING", [{ name: "Target", value: "/fixture/archive", secretId: null }]],
      ["SCHEDULER", [{ name: "Target", value: "/fixture/archive", secretId: null }]],
    ]);

    // It is offered only while the chosen script has a declared trigger with no instance.
    await action(controls(page), "Add job").click();
    await action(editor, "Script").getByText("Choose a script", { exact: true }).waitFor();
    assert.equal(await action(editor, "Set up jobs from header").count(), 0);
    await pick(page, editor, "Script", "Nightly report · nightly.py");
    await action(editor, "Trigger").getByText("Schedule", { exact: true }).waitFor();
    assert.equal(await action(editor, "Set up jobs from header").count(), 0);
    await pick(page, editor, "Script", "Notify · notify.py");
    await action(editor, "Set up jobs from header").click();
    await status(page, "1 job created for Notify").waitFor();
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

// The secrets screen: its one table and the row holding a given name.
const secretsTable = (page) => page.getByRole("region", { name: "Secrets", exact: true });
const secretRow = (page, name) => rows(secretsTable(page)).filter({ has: page.getByText(name, { exact: true }) });

test("the secrets screen is one table of names and who links them, with one way to add", async () => {
  const page = await open("?secrets");
  try {
    await secretRow(page, "Notify token").waitFor();
    assert.equal(await page.getByRole("region").count(), 1);
    for (const header of ["Name", "Used by", "Updated"]) {
      assert.equal(await secretsTable(page).getByText(header, { exact: true }).count(), 1, header);
    }
    assert.equal(await rows(secretsTable(page)).count(), 2);
    assert.equal(await secretRow(page, "Notify token").getByText("Notify", { exact: true }).count(), 1);
    assert.equal(await secretRow(page, "Spare key").getByText("Not used", { exact: true }).count(), 1);
    // One Add for the whole list, in the top bar, and no value anywhere.
    assert.deepEqual(await controls(page).getByRole("button").allTextContents(), ["Add secret"]);
    assert.equal(await page.getByText("fixture-token").count(), 0);
    await shot(page, "secrets-table");
  } finally { await page.close(); }
});

test("a secret is added, renamed and given a new value without its value ever being shown", async () => {
  const page = await open("?secrets");
  try {
    await action(controls(page), "Add secret").click();
    let editor = secretDialog(page, "Add secret");
    assert.equal(await secretField(editor, "Value").getAttribute("type"), "password");
    assert.equal(await secretField(editor, "Value").getAttribute("placeholder"), null);
    await field(editor, "Name").fill("Mail token");
    // A new secret needs a value, and the dialog says so itself.
    await action(editor, "Save").click();
    await editor.getByText("Type the value to keep.", { exact: true }).waitFor();
    assert.equal(await page.locator("#status").textContent(), "");
    await shot(page, "secret-editor-create");
    await secretField(editor, "Value").fill("fixture-token-3");
    await action(editor, "Save").click();
    await status(page, "Secret Mail token created").waitFor();
    await editor.waitFor({ state: "detached" });
    await secretRow(page, "Mail token").getByText("Not used", { exact: true }).waitFor();

    // A row opens its secret: the name to change, and a value field that starts blank.
    await secretRow(page, "Mail token").getByText("Mail token", { exact: true }).click();
    editor = secretDialog(page, "Mail token");
    assert.equal(await field(editor, "Name").inputValue(), "Mail token");
    assert.equal(await secretField(editor, "Value").inputValue(), "");
    assert.equal(await secretField(editor, "Value").getAttribute("placeholder"), "Saved · leave blank to keep");
    await shot(page, "secret-editor-edit");
    await field(editor, "Name").fill("Mail key");
    await action(editor, "Save").click();
    await status(page, "Secret Mail key saved").waitFor();
    await secretRow(page, "Mail key").waitFor();

    await secretRow(page, "Mail key").getByText("Mail key", { exact: true }).click();
    editor = secretDialog(page, "Mail key");
    await secretField(editor, "Value").fill("fixture-token-4");
    await action(editor, "Save").click();
    await status(page, "Secret Mail key saved").waitFor();
    // A rename sends only the name; a blank value field is not sent, and a typed one is.
    const held = await daemon(page);
    assert.deepEqual(held.requests, [
      { name: "CreateSecret", variables: { name: "Mail token", value: "fixture-token-3" } },
      { name: "UpdateSecret", variables: { id: "s3", name: "Mail key", value: null } },
      { name: "UpdateSecret", variables: { id: "s3", name: null, value: "fixture-token-4" } },
    ]);
    assert.equal(held.secrets.find((entry) => entry.id === "s3").value, "fixture-token-4");
    assert.equal(await page.getByText("fixture-token").count(), 0);
  } finally { await page.close(); }
});

test("a secret an instance links cannot be deleted, and the refusal names the instance inside the question", async () => {
  const page = await open("?secrets");
  try {
    const confirm = page.getByRole("dialog", { name: "Delete secret", exact: true });
    await secretRow(page, "Notify token").getByText("Notify token", { exact: true }).click();
    let editor = secretDialog(page, "Notify token");
    await action(editor, "Delete").click();
    await confirm.getByText("Delete Notify token? A job that still links it keeps it from being deleted.", { exact: true }).waitFor();
    assert.equal(await confirm.getByRole("alert").count(), 0);
    await action(confirm, "Delete").click();
    // Said where it was asked, as an instance's refused delete is; the question stays open.
    await confirm.getByRole("alert").filter({ hasText: "secret 'Notify token' is used by Notify" }).waitFor();
    assert.equal(await page.locator("#status").textContent(), "");
    await shot(page, "secret-delete-refused");
    await action(confirm, "Cancel").click();
    await confirm.waitFor({ state: "detached" });
    // Asked again, the question starts without the last answer.
    await action(editor, "Delete").click();
    await confirm.getByText("Notify token", { exact: true }).waitFor();
    assert.equal(await confirm.getByRole("alert").count(), 0);
    await action(confirm, "Cancel").click();
    await confirm.waitFor({ state: "detached" });
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
    assert.equal(await secretRow(page, "Notify token").count(), 1);

    // One nothing links goes.
    await secretRow(page, "Spare key").getByText("Spare key", { exact: true }).click();
    editor = secretDialog(page, "Spare key");
    await action(editor, "Delete").click();
    await action(confirm, "Delete").click();
    await status(page, "Secret Spare key deleted").waitFor();
    await editor.waitFor({ state: "detached" });
    await secretRow(page, "Spare key").waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests, [
      { name: "DeleteSecret", variables: { id: "s1" } },
      { name: "DeleteSecret", variables: { id: "s2" } },
    ]);
  } finally { await page.close(); }
});

test("signed in, a secret is added without the password, and changing or deleting one asks for it", async () => {
  const page = await open("?secrets&signedin");
  try {
    const check = page.getByRole("dialog", { name: "Confirm your password", exact: true });

    // Adding one is never held back.
    await action(controls(page), "Add secret").click();
    let editor = secretDialog(page, "Add secret");
    await field(editor, "Name").fill("Mail token");
    await secretField(editor, "Value").fill("fixture-token-3");
    await action(editor, "Save").click();
    await status(page, "Secret Mail token created").waitFor();
    assert.equal(await check.count(), 0);

    // A change the daemon holds back asks for the password over the editor, and says nothing in it.
    await secretRow(page, "Mail token").getByText("Mail token", { exact: true }).click();
    editor = secretDialog(page, "Mail token");
    await field(editor, "Name").fill("Mail key");
    await action(editor, "Save").click();
    await check.waitFor();
    assert.equal(await editor.getByRole("alert").count(), 0);
    assert.equal(await page.getByText("recent password verification required").count(), 0);
    await shot(page, "secret-password-check");
    // A wrong password is said inside the question, and the change is not tried again.
    await secretField(check, "Password").fill("not-it");
    await action(check, "Continue").click();
    await check.getByRole("alert").filter({ hasText: "That password is not correct." }).waitFor();
    await secretField(check, "Password").fill("fixture-password");
    await action(check, "Continue").click();
    // The same change runs again by itself.
    await status(page, "Secret Mail key saved").waitFor();
    await check.waitFor({ state: "detached" });
    await editor.waitFor({ state: "detached" });
    await secretRow(page, "Mail key").waitFor();
    assert.deepEqual((await daemon(page)).requests, [
      { name: "CreateSecret", variables: { name: "Mail token", value: "fixture-token-3" } },
      { name: "UpdateSecret", variables: { id: "s3", name: "Mail key", value: null } },
      { name: "UpdateSecret", variables: { id: "s3", name: "Mail key", value: null } },
    ]);
  } finally { await page.close(); }
});

test("signed in, deleting a secret asks for the password over the question, and cancelling leaves it", async () => {
  const page = await open("?secrets&signedin");
  try {
    const check = page.getByRole("dialog", { name: "Confirm your password", exact: true });
    const confirm = page.getByRole("dialog", { name: "Delete secret", exact: true });
    await secretRow(page, "Spare key").getByText("Spare key", { exact: true }).click();
    const editor = secretDialog(page, "Spare key");
    await action(editor, "Delete").click();
    await action(confirm, "Delete").click();
    await check.waitFor();
    assert.equal(await confirm.getByRole("alert").count(), 0);
    // Cancelled, the question is still there and nothing was deleted.
    await action(check, "Cancel").click();
    await check.waitFor({ state: "detached" });
    assert.equal(await confirm.count(), 1);
    assert.equal(await confirm.getByRole("alert").count(), 0);
    // Asked again, it asks again; the password checked, the delete goes through.
    await action(confirm, "Delete").click();
    await secretField(check, "Password").fill("fixture-password");
    await action(check, "Continue").click();
    await status(page, "Secret Spare key deleted").waitFor();
    await secretRow(page, "Spare key").waitFor({ state: "detached" });
    assert.deepEqual((await daemon(page)).requests, [
      { name: "DeleteSecret", variables: { id: "s2" } },
      { name: "DeleteSecret", variables: { id: "s2" } },
      { name: "DeleteSecret", variables: { id: "s2" } },
    ]);
  } finally { await page.close(); }
});

test("no secrets says so, and the top bar's Add is the only one", async () => {
  const page = await open("?secrets&empty");
  try {
    await secretsTable(page).getByText("No secrets yet.", { exact: true }).waitFor();
    assert.equal(await rows(secretsTable(page)).count(), 0);
    assert.equal(await secretsTable(page).getByRole("button").count(), 0);
    await shot(page, "secrets-empty");
    await action(controls(page), "Add secret").click();
    await secretDialog(page, "Add secret").waitFor();
  } finally { await page.close(); }
});

test("a secret several instances link names every one of them", async () => {
  const page = await open("?secrets&shared");
  try {
    await secretRow(page, "Notify token").getByText("Notify, Log removal", { exact: true }).waitFor();
    assert.equal(await secretRow(page, "Spare key").getByText("Not used", { exact: true }).count(), 1);
    await shot(page, "secrets-table-shared");
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
      "notify.py", "The job as saved, run once against made-up inputs. Nothing the script asks for is applied.",
      "Post-processing", "5m", "NZBGet",
    ]) assert.equal(await dialog.getByText(text, { exact: true }).count(), 1, text);
    // It has neither an exit code nor a duration until it ends.
    assert.equal(await dialog.getByText("—", { exact: true }).count(), 2);
    for (const [name, value] of [
      ["NZBPP_CATEGORY", "tv"], ["NZBPP_DIRECTORY", "/fixture/scratch/test-1"], ["NZBPP_NZBNAME", "weaver-test-download"],
    ]) {
      for (const text of [name, value]) assert.equal(await section("Simulated inputs").getByText(text, { exact: true }).count(), 1, text);
    }
    await section("Simulated inputs").getByText("made up for this run · the job's own inputs are left out", { exact: true }).waitFor();
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

test("a test that prints more than its log keeps is marked, and says how much was kept", async () => {
  const page = await open("?scripts");
  try {
    await action(row(page, "Notify"), "Test").click();
    const dialog = testDialog(page, "Notify");
    const log = dialog.getByRole("region", { name: "Run log", exact: true });
    await log.getByText("The script has printed nothing yet.", { exact: true }).waitFor();
    assert.equal(await log.getByText("Truncated", { exact: true }).count(), 0);

    await advance(page, "overflowed");
    await log.getByText("Truncated", { exact: true }).waitFor();
    await log.getByText("Only the last 32 KB of output was kept.", { exact: true }).waitFor();
    await advance(page, "finished");
    await dialog.getByText("SUCCEEDED", { exact: true }).waitFor();
    assert.equal(await log.getByText("Truncated", { exact: true }).count(), 1);
    await shot(page, "test-dialog-truncated");
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
    await post.getByText("Only the last 32 KB of output was kept.", { exact: true }).waitFor();
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
// A row is a disclosure; the pagination bar sits under the table, outside it.
const runsTable = (page) => page.getByRole("region", { name: "Script runs", exact: true });
const runRows = (page) => runsTable(page).locator('[role="button"]');
const rowsHolding = (page, text) => runRows(page).filter({ has: page.getByText(text, { exact: true }) });
// The pages of runs the screen has asked the fixture's daemon for, oldest request first.
const runRequests = (page) => page.evaluate(() => fetch("/graphql", {
  method: "POST", body: JSON.stringify({ operationName: "ScriptRunRequests", variables: {} }),
}).then((response) => response.json()).then((payload) => payload.data.scriptRunRequests));
// The runs whose full output the screen has asked for, oldest first.
const outputRequests = async (page) => (await daemon(page)).outputRequests;
// The output of the run called `name`, as the open row shows it.
const outputOf = (page, name) => runsTable(page).getByRole("region", { name: `Output of ${name}`, exact: true });
// The pagination bar: the page on screen, the range it covers, and its steps.
const currentPage = (page) => page.locator('[aria-current="page"]');
const pageButton = (page, name) => page.getByRole("button", { name, exact: true });
const rowsPerPage = (page) => page.getByRole("radiogroup", { name: "Rows per page", exact: true });

test("the runs screen is one table of every run, newest first, with no way to create one", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    assert.equal(await page.getByRole("region").count(), 1);
    for (const header of ["Finished", "Script job", "Trigger", "Job", "Status", "Exit code", "Duration"]) {
      assert.equal(await runsTable(page).getByText(header, { exact: true }).count(), 1);
    }
    await runsTable(page).getByText("newest first", { exact: true }).waitFor();
    // A run is called by its script job, with the script file beside it.
    for (const [index, job, script, trigger, status, exitCode, duration] of [
      [0, "Nightly report", "nightly.py", "Schedule", "SUCCEEDED", "0", "1m 35s"],
      // A script job named after its file reads as the file alone, as does a run with no script job.
      [1, null, "intake.py", "Scan", "SUCCEEDED", "0", "1.5s"],
      [2, "Notify", "notify.py", "Post-processing", "SUCCEEDED", "0", "240 ms"],
      [3, null, "retired.py", "Post-processing", "FAILED", "—", "240 ms"],
      [4, "Announce", "notify.py", "Queue · NZB_ADDED", "SUCCEEDED", "0", "240 ms"],
      [5, "Tidy tv", "cleanup.sh", "Post-processing", "WARNING", "3", "240 ms"],
      [6, "Feed intake", "intake.py", "Feed", "SUCCEEDED", "0", "240 ms"],
      [7, null, "sweep.sh", "Queue · FILE_DOWNLOADED", "SUCCEEDED", "0", "240 ms"],
    ]) {
      for (const text of [job, script, trigger, status, exitCode, duration]) {
        if (text !== null) assert.equal(await rows.nth(index).getByText(text, { exact: true }).count(), 1, `row ${index}: ${text}`);
      }
    }
    // Every row starts shut, showing only its columns, and no output has been asked for.
    assert.equal(await rows.count(), 25);
    assert.deepEqual(new Set(await rows.evaluateAll((all) => all.map((row) => row.getAttribute("aria-expanded")))), new Set(["false"]));
    assert.equal(await runsTable(page).getByText("nightly.py finished", { exact: true }).count(), 0);
    assert.deepEqual(await outputRequests(page), []);
    // Only the run nothing waited for is marked.
    assert.equal(await runsTable(page).getByText("Fire and forget", { exact: true }).count(), 1);
    assert.equal(await rows.nth(5).getByText("Fire and forget", { exact: true }).count(), 1);
    // Only the run that printed more than was kept says so while shut.
    assert.equal(await runsTable(page).getByText("Truncated", { exact: true }).count(), 1);
    assert.equal(await rows.nth(5).getByText("Truncated", { exact: true }).count(), 1);
    // The top bar filters and refreshes; nothing on the screen adds a run.
    const controls = page.locator("#controls").getByRole("button");
    assert.equal(await controls.count(), 2);
    for (const name of ["Filter by trigger", "Refresh"]) {
      assert.equal(await page.locator("#controls").getByRole("button", { name, exact: true }).count(), 1);
    }
    assert.equal(await page.getByRole("button", { name: /^(Add|Create|New)\b/ }).count(), 0);
    // The first page of the smallest size, and where it sits in the whole.
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: null, kind: null, status: null });
    await page.getByText("1–25 of 60", { exact: true }).waitFor();
    assert.equal(await currentPage(page).textContent(), "1");
    assert.equal(await rowsPerPage(page).getByRole("radio", { checked: true }).textContent(), "25");
    await shot(page, "runs");
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
    assert.deepEqual(await outputRequests(page), []);
  } finally { await page.close(); }
});

test("opening a run reads its output once into the log viewer, and shutting it keeps what was read", async () => {
  const page = await open("?runs");
  try {
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"], { origin: baseUrl });
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    const nightly = rows.nth(0);

    await nightly.getByText("Schedule", { exact: true }).click();
    assert.equal(await nightly.getAttribute("aria-expanded"), "true");
    const viewer = outputOf(page, "Nightly report");
    await viewer.getByText("summary written ").waitFor();
    // Every line is numbered; the line the log parser reads as a record shows its stamp,
    // level and target, and tints its key=value pairs. The plain lines are left as they are.
    for (const number of ["1", "2", "3", "4", "5"]) {
      assert.equal(await viewer.getByText(number, { exact: true }).count(), 1, `line ${number}`);
    }
    assert.equal(await viewer.getByText("6", { exact: true }).count(), 0, "a trailing newline is not a line");
    for (const text of ["starting nightly report", "warning: 1 feed was unreachable", "done", "03:00:00.125", "nightly::report"]) {
      assert.equal(await viewer.getByText(text, { exact: true }).count(), 1, text);
    }
    assert.equal(await viewer.getByText(/^info$/i).count(), 1, "the record's level");
    for (const pair of ["rows=42", "path=/fixture/reports/nightly.txt"]) {
      const tinted = viewer.getByText(pair, { exact: true });
      assert.match(await tinted.getAttribute("class"), /text-wv-info/, pair);
    }
    // A long line wraps inside the viewer rather than widening it.
    const box = await viewer.boundingBox();
    const wide = await viewer.getByText(/^columns wide/).first().boundingBox();
    assert.ok(wide.width <= box.width, "the long line stays inside the viewer");
    assert.ok(wide.height > 30, "the long line wraps");
    // The detail says how the run went, and how it was started.
    await runsTable(page).getByText("nightly.py · Blocking · NZBGet · scheduler:4", { exact: true }).waitFor();
    assert.deepEqual(await outputRequests(page), ["run-60"]);
    await shot(page, "runs-row-open");

    // The whole output goes to the clipboard.
    await action(runsTable(page), "Copy output").click();
    await status(page, "Output copied to the clipboard").waitFor();
    assert.equal(
      await page.evaluate(() => navigator.clipboard.readText()),
      "starting nightly report\n2026-01-01T03:00:00.125Z INFO nightly::report: summary written rows=42 path=/fixture/reports/nightly.txt\n"
        + `columns ${"wide ".repeat(60)}end\nwarning: 1 feed was unreachable\ndone\n`,
    );

    // Shut, the row shows only its columns again; opened again, it does not ask again.
    await nightly.getByText("Schedule", { exact: true }).click();
    await viewer.waitFor({ state: "detached" });
    assert.equal(await nightly.getAttribute("aria-expanded"), "false");
    await nightly.getByText("Schedule", { exact: true }).click();
    await viewer.getByText("starting nightly report", { exact: true }).waitFor();
    assert.deepEqual(await outputRequests(page), ["run-60"]);

    // The row answers the keyboard as a button does: Enter and Space open and shut it.
    const notify = rows.nth(2);
    await notify.focus();
    await page.keyboard.press("Enter");
    await outputOf(page, "Notify").getByText("resolved 2 recipients", { exact: true }).waitFor();
    assert.equal(await notify.getAttribute("aria-expanded"), "true");
    await page.keyboard.press(" ");
    await outputOf(page, "Notify").waitFor({ state: "detached" });
    assert.equal(await notify.getAttribute("aria-expanded"), "false");
    // Only the rows that were opened were read, each once.
    assert.deepEqual(await outputRequests(page), ["run-60", "run-58"]);
  } finally { await page.close(); }
});

test("an open run says why it failed, and when its output is cut or no longer kept", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();

    // A failed run says why, and has neither output to read nor any to show.
    await rows.nth(3).getByText("FAILED", { exact: true }).click();
    for (const text of ["script is no longer in the scripts directory", "Full output is no longer retained."]) {
      await runsTable(page).getByText(text, { exact: true }).waitFor();
    }
    assert.equal(await outputOf(page, "retired.py").count(), 0);
    assert.equal(await action(runsTable(page), "Copy output").count(), 0);
    await rows.nth(3).getByText("FAILED", { exact: true }).click();
    await runsTable(page).getByText("script is no longer in the scripts directory", { exact: true }).waitFor({ state: "detached" });

    // A scan's excerpt outlives the output it was cut from, and nothing is asked for.
    await rows.nth(1).getByText("Scan", { exact: true }).click();
    await outputOf(page, "intake.py").getByText("intake.py finished", { exact: true }).waitFor();
    await runsTable(page).getByText("Full output is no longer retained.", { exact: true }).waitFor();
    await rows.nth(1).getByText("Scan", { exact: true }).click();
    await outputOf(page, "intake.py").waitFor({ state: "detached" });

    // A run that printed more than was kept is marked while shut and says how much was kept
    // once open; one whose output the daemon has since let go keeps the excerpt it ended with,
    // and is still marked.
    await rows.nth(5).getByText("Truncated", { exact: true }).waitFor();
    assert.equal(await runsTable(page).getByText("Only the last 32 KB of output was kept.", { exact: true }).count(), 0);
    await rows.nth(5).getByText("WARNING", { exact: true }).click();
    await runsTable(page).getByText("Only the last 32 KB of output was kept.", { exact: true }).waitFor();
    assert.equal(await rows.nth(5).getByText("Truncated", { exact: true }).count(), 1);
    await shot(page, "runs-truncated-open");
    await runsTable(page).getByText("Full output is no longer retained.", { exact: true }).waitFor();
    await outputOf(page, "Tidy tv").getByText("cleanup.sh finished", { exact: true }).waitFor();
    await runsTable(page).getByText("cleanup.sh · Fire and forget · SABnzbd · post_processing", { exact: true }).waitFor();
    assert.deepEqual(await outputRequests(page), ["run-55"]);
  } finally { await page.close(); }
});

test("the runs go a page at a time, forward and back, and a shut page leaves its rows shut", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    await rows.nth(0).getByText("Schedule", { exact: true }).click();
    await outputOf(page, "Nightly report").waitFor();

    // The next page starts below the last run of this one.
    await pageButton(page, "Next").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.35")).waitFor();
    assert.equal(await rows.count(), 25);
    assert.equal(await rows.last().getByRole("link", { name: "fixture.batch.11", exact: true }).count(), 1);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: "run-36", kind: null, status: null });
    await page.getByText("26–50 of 60", { exact: true }).waitFor();
    assert.equal(await currentPage(page).textContent(), "2");
    await shot(page, "runs-page-two");

    // The last page is short, and has nowhere further to go.
    await pageButton(page, "3").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.10")).waitFor();
    assert.equal(await rows.count(), 10);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: "run-11", kind: null, status: null });
    await page.getByText("51–60 of 60", { exact: true }).waitFor();
    assert.equal(await pageButton(page, "Next").isDisabled(), true);

    // Back goes to where the page before began.
    await pageButton(page, "Previous").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.35")).waitFor();
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: "run-36", kind: null, status: null });
    await pageButton(page, "1").click();
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: null, kind: null, status: null });
    assert.equal(await pageButton(page, "Previous").isDisabled(), true);
    // The row opened before the first page was left is shut on the way back.
    assert.equal(await rows.nth(0).getAttribute("aria-expanded"), "false");
    assert.equal(await outputOf(page, "Nightly report").count(), 0);
  } finally { await page.close(); }
});

test("a page further on than any reached yet is walked to, a page at a time", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    const asked = (await runRequests(page)).length;
    await pageButton(page, "3").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.10")).waitFor();
    assert.deepEqual((await runRequests(page)).slice(asked), [
      { limit: 25, before: "run-36", kind: null, status: null },
      { limit: 25, before: "run-11", kind: null, status: null },
    ]);
    assert.equal(await currentPage(page).textContent(), "3");
  } finally { await page.close(); }
});

test("a new page size starts again from the first page", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    await pageButton(page, "Next").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.35")).waitFor();
    await rowsPerPage(page).getByRole("radio", { name: "50", exact: true }).click();
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    assert.equal(await rows.count(), 50);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 50, before: null, kind: null, status: null });
    assert.equal(await currentPage(page).textContent(), "1");
    await page.getByText("1–50 of 60", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("the trigger filter asks the daemon for one kind from its first page", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    await pageButton(page, "Next").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.35")).waitFor();
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
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: null, kind: "POST_PROCESSING", status: null });
    assert.equal(await currentPage(page).textContent(), "1");
    await page.getByText("1–3 of 3", { exact: true }).waitFor();
    assert.equal(await pageButton(page, "Next").isDisabled(), true);
  } finally { await page.close(); }
});

// The tabs over the runs: one for every way a run can end, each with its count.
const statusTabs = (page) => page.locator("button[aria-pressed]");
const statusTab = (page, name, count) => statusTabs(page).filter({ hasText: new RegExp(`^${name}${count}$`) });

test("the status tabs ask the daemon for runs that ended one way, from the first page", async () => {
  const page = await open("?runs");
  try {
    const rows = runRows(page);
    await rows.first().and(rowsHolding(page, "nightly.py")).waitFor();
    // Every status has a tab, in one order, and says how many runs ended that way.
    const tabs = [["All", 60], ["Succeeded", 58], ["Warning", 1], ["Failed", 1], ["Timed out", 0], ["Skipped", 0], ["Cancelled", 0]];
    assert.deepEqual(
      await statusTabs(page).evaluateAll((all) => all.map((tab) => tab.textContent)),
      tabs.map(([name, count]) => `${name}${count}`),
    );
    assert.equal(await statusTab(page, "All", 60).getAttribute("aria-pressed"), "true");
    await pageButton(page, "Next").click();
    await rows.first().and(rowsHolding(page, "fixture.batch.35")).waitFor();

    await statusTab(page, "Failed", 1).click();
    await rows.first().and(rowsHolding(page, "retired.py")).waitFor();
    assert.equal(await rows.count(), 1);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: null, kind: null, status: "FAILED" });
    assert.equal(await statusTab(page, "Failed", 1).getAttribute("aria-pressed"), "true");
    assert.equal(await currentPage(page).textContent(), "1");
    await page.getByText("1–1 of 1", { exact: true }).waitFor();
    // The other tabs keep their own counts while one is open.
    assert.equal(await statusTab(page, "Succeeded", 58).count(), 1);
    await shot(page, "runs-status-failed");

    // A status no run ended with lists nothing, and says the filters are why.
    await statusTab(page, "Timed out", 0).click();
    await runsTable(page).getByText("No runs match these filters.", { exact: true }).waitFor();
    assert.equal(await rows.count(), 0);

    // A status narrows within the trigger, and the counts follow the trigger.
    await page.locator("#controls").getByRole("button", { name: "Filter by trigger", exact: true }).click();
    await page.getByRole("menuitemradio", { name: "Post-processing", exact: true }).click();
    await statusTab(page, "All", 3).waitFor();
    await statusTab(page, "Warning", 1).click();
    await rows.first().and(rowsHolding(page, "cleanup.sh")).waitFor();
    assert.equal(await rows.count(), 1);
    assert.deepEqual((await runRequests(page)).at(-1), { limit: 25, before: null, kind: "POST_PROCESSING", status: "WARNING" });
  } finally { await page.close(); }
});

test("the settings search narrows the loaded runs by script or by script job", async () => {
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
    await shot(page, "runs-empty");
  } finally { await page.close(); }
});

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-schedule-backup", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });
// The shared controls: a select is a button that opens a menu, and a toggle is a switch.
async function choose(page, scope, name, option) {
  await scope.getByRole("button", { name, exact: true }).click();
  await page.getByRole("menuitemradio", { name: option, exact: true }).click();
}
const toggle = (scope, name) => scope.getByRole("switch", { name, exact: true });
// The panel's own Add, which the settings shell puts in its top bar.
const addSchedule = (page) => page.locator("#controls").getByRole("button", { name: "Add schedule", exact: true });
async function open(query = "") {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  if (query.includes("automatic")) {
    await page.clock.install({ time: new Date("2026-01-03T03:00:00Z") });
    await page.clock.pauseAt(new Date("2026-01-03T03:00:00Z"));
  }
  page.setDefaultTimeout(0);
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/schedule-backup.html${query}`);
  return page;
}

test("backup settings, retained create, token download and delete use the rendered controls", async () => {
  const page = await open();
  try {
    const automatic = page.getByRole("region", { name: "Automatic backups", exact: true });
    await automatic.waitFor();
    await automatic.getByRole("switch", { name: "Enable automatic backups", exact: true }).click();
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    await page.getByRole("alert").filter({ hasText: "automatic backup key is required" }).waitFor();
    await automatic.getByLabel("Automatic backup key", { exact: true }).fill("fixture archive key");
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    await automatic.getByText("A key is stored. Enter a new key to replace it.", { exact: true }).waitFor();
    assert.equal(await automatic.getByLabel("Automatic backup key", { exact: true }).inputValue(), "");
    await automatic.getByRole("switch", { name: "Enable automatic backups", exact: true }).click();
    await automatic.getByRole("switch", { name: "Clear automatic backup key", exact: true }).click();
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    await automatic.getByText("Set a key with at least 8 non-whitespace characters before enabling.", { exact: true }).waitFor();
    const manual = page.getByRole("region", { name: "Backup", exact: true });
    await manual.getByLabel("Password", { exact: true }).fill("manual archive key");
    await manual.getByLabel("Confirm password", { exact: true }).fill("manual archive key");
    await page.getByRole("button", { name: "Create without downloading", exact: true }).click();
    const stored = page.getByRole("region", { name: "Stored backups", exact: true });
    await stored.getByText("Ready", { exact: true }).waitFor();
    const download = page.waitForEvent("download");
    await stored.getByRole("button", { name: "Download", exact: true }).click();
    assert.match((await download).suggestedFilename(), /^weaver_backup_fixture_/);
    await stored.getByRole("button", { name: "Delete backup", exact: true }).click();
    const confirmation = page.getByRole("dialog", { name: "Delete backup", exact: true });
    await confirmation.getByRole("button", { name: "Delete backup", exact: true }).click();
    await stored.getByText("Nothing configured yet.", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("all new schedule actions support disabled create, edit, draft and list toggles, and delete", async () => {
  const page = await open("?schedules");
  try {
    for (const [action, group] of [["Pause all intake", "Downloads"], ["Pause post-processing", "Post-processing"], ["Resume post-processing", "Post-processing"], ["Set server availability", "Servers"], ["Set quota metering", "Quota metering"], ["Scan watch folder", "One-shot actions"], ["Fetch RSS", "One-shot actions"], ["Prune history", "One-shot actions"]]) {
      await addSchedule(page).click();
      const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
      await form.getByLabel("Label", { exact: true }).fill(`fixture ${action}`);
      await form.getByRole("switch", { name: "Enabled", exact: true }).click();
      await form.getByRole("button", { name: "Action", exact: true }).click();
      await page.getByRole("menuitemradio", { name: action, exact: true }).click();
      if (action === "Set server availability") await choose(page, form, "Server", "fixture-provider");
      if (action === "Prune history") await toggle(form, "Completed").check();
      if (action === "Fetch RSS") {
        await toggle(form, "Every hour").click();
        // A number field commits what was typed when focus leaves it.
        const minute = form.getByRole("spinbutton", { name: "Minute of the hour", exact: true });
        await minute.fill("15");
        await minute.blur();
      }
      await form.getByRole("button", { name: "Save", exact: true }).click();
      await form.waitFor({ state: "hidden" });
      const row = page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(`fixture ${action}`, { exact: true }) });
      await row.waitFor();
      assert.equal(await row.getByRole("switch").isChecked(), false);
      if (action === "Scan watch folder") {
        // The fixture stores mutations without dispatching real schedule actions.
        await row.getByRole("switch").click();
        await row.locator('[role="switch"][aria-checked="true"]').waitFor();
        await row.getByRole("switch").click();
        await row.locator('[role="switch"][aria-checked="false"]').waitFor();
      }
      const description = {
        "Set server availability": "Set server availability: fixture-provider (On)",
        "Set quota metering": "Set quota metering: On",
        "Fetch RSS": "Fetch RSS: All enabled feeds",
        "Prune history": "Prune history: Completed (Delete files too: Off)",
      }[action];
      if (description) await row.getByText(description, { exact: true }).waitFor();
      await row.click();
      const edit = page.getByRole("dialog", { name: `fixture ${action}`, exact: true });
      if (action === "Set server availability") await edit.getByRole("button", { name: "Server", exact: true }).getByText("fixture-provider", { exact: true }).waitFor();
      if (action === "Fetch RSS") assert.equal(await edit.getByRole("spinbutton", { name: "Minute of the hour", exact: true }).inputValue(), "15");
      if (action === "Prune history") assert.equal(await toggle(edit, "Completed").isChecked(), true);
      await edit.getByLabel("Label", { exact: true }).fill(`edited ${action}`);
      await edit.getByRole("switch", { name: "Enabled", exact: true }).click();
      await edit.getByRole("switch", { name: "Enabled", exact: true }).click();
      await edit.getByRole("button", { name: "Save", exact: true }).click();
      await edit.waitFor({ state: "hidden" });
      const updated = page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(`edited ${action}`, { exact: true }) });
      await updated.waitFor();
      assert.equal(await updated.getByRole("switch").isChecked(), false);
      await updated.click();
      await page.getByRole("dialog", { name: `edited ${action}`, exact: true }).getByRole("button", { name: "Remove schedule", exact: true }).click();
      await page.getByRole("dialog", { name: "Remove schedule", exact: true }).getByRole("button", { name: "Remove schedule", exact: true }).click();
      await updated.waitFor({ state: "hidden" });
    }
  } finally { await page.close(); }
});

test("editing another action into server and quota rules saves the displayed boolean defaults", async () => {
  const page = await open("?schedules");
  try {
    for (const [action, group, option] of [
      ["Set server availability", "Servers", "Server active"],
      ["Set quota metering", "Quota metering", "Count traffic toward the quota"],
    ]) {
      const label = `converted ${group}`;
      await addSchedule(page).click();
      const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
      await form.getByLabel("Label", { exact: true }).fill(label);
      await form.getByRole("switch", { name: "Enabled", exact: true }).click();
      await form.getByRole("button", { name: "Save", exact: true }).click();
      await form.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: "Downloads", exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      const edit = page.getByRole("dialog", { name: label, exact: true });
      await edit.getByRole("button", { name: "Action", exact: true }).click();
      await page.getByRole("menuitemradio", { name: action, exact: true }).click();
      if (group === "Servers") await choose(page, edit, "Server", "fixture-provider");
      assert.equal(await toggle(edit, option).isChecked(), true);
      await edit.getByRole("button", { name: "Save", exact: true }).click();
      await edit.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      assert.equal(await toggle(edit, option).isChecked(), true);
      await toggle(edit, option).uncheck();
      await edit.getByRole("button", { name: "Save", exact: true }).click();
      await edit.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: group, exact: true }).getByText(group === "Servers" ? "Set server availability: fixture-provider (Off)" : "Set quota metering: Off", { exact: true }).waitFor();
      await page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      assert.equal(await toggle(edit, option).isChecked(), false);
      await edit.getByRole("button", { name: "Cancel", exact: true }).click();
    }
  } finally { await page.close(); }
});

test("the schedules are one list with one Add, headed only by the groups that hold a rule", async () => {
  const page = await open("?schedules");
  try {
    const list = page.getByRole("region", { name: "Schedules", exact: true });
    await list.getByText("No schedules yet. Weaver downloads whenever there is work.", { exact: true }).waitFor();
    assert.equal(await list.getByRole("region").count(), 0);
    // The top bar's Add is the only one, with rules listed or without.
    const everyAdd = page.getByRole("button", { name: "Add schedule", exact: true });
    assert.equal(await everyAdd.count(), 1);
    assert.equal(await list.getByRole("button").count(), 0);
    await addSchedule(page).click();
    const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
    await form.getByLabel("Label", { exact: true }).fill("fixture hold");
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await form.waitFor({ state: "hidden" });
    await list.getByRole("region", { name: "Downloads", exact: true }).getByText("fixture hold", { exact: true }).waitFor();
    assert.equal(await list.getByRole("region").count(), 1);
    assert.equal(await list.getByText("Time", { exact: true }).count(), 1);
    assert.equal(await everyAdd.count(), 1);
    assert.equal(await addSchedule(page).count(), 1);
  } finally { await page.close(); }
});

// The rules as the fixture's daemon holds them.
const storedSchedules = (page) => page.evaluate(() => fetch("/graphql", {
  method: "POST", body: JSON.stringify({ operationName: "Schedules", variables: {} }),
}).then((response) => response.json()).then((payload) => payload.data.schedules));
const oneShotRow = (page, label) => page.getByRole("region", { name: "One-shot actions", exact: true })
  .getByRole("button").filter({ has: page.getByText(label, { exact: true }) });

test("a rule that runs a script names a schedule instance, and is listed by that instance's name", async () => {
  const page = await open("?schedules");
  try {
    await addSchedule(page).click();
    const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
    await form.getByLabel("Label", { exact: true }).fill("fixture script");
    assert.equal(await form.getByRole("button", { name: "Script instance", exact: true }).count(), 0);
    await choose(page, form, "Action", "Run script");
    const instance = form.getByRole("button", { name: "Script instance", exact: true });
    await instance.getByText("Choose an instance", { exact: true }).waitFor();
    await form.getByText("A rule can only run an instance whose trigger is Schedule.", { exact: true }).waitFor();
    // A rule with no instance to run is refused, and the editor stays open to say so.
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await form.getByText("a run_script schedule needs a script instance", { exact: true }).waitFor();
    assert.deepEqual(await storedSchedules(page), []);
    // Only the instances a schedule starts are offered, each by its name with its script beside it.
    await instance.click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["Choose an instance", "Nightly report · nightly.py", "sweep.sh"]);
    await page.getByRole("menuitemradio", { name: "Nightly report · nightly.py", exact: true }).click();
    await instance.getByText("Nightly report · nightly.py", { exact: true }).waitFor();
    // A script rule's time takes the script evaluator's own notation.
    await form.getByText("Local time. HH:MM, or *:MM for every hour; separate several with commas.", { exact: true }).waitFor();
    await form.getByLabel("Time", { exact: true }).fill("*:20");
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await form.waitFor({ state: "hidden" });

    const list = page.getByRole("region", { name: "Schedules", exact: true });
    await list.getByRole("region", { name: "One-shot actions", exact: true }).getByText("local time · runs once each time", { exact: true }).waitFor();
    const row = oneShotRow(page, "fixture script");
    for (const text of ["*:20", "Run Nightly report"]) await row.getByText(text, { exact: true }).waitFor();
    // Every rule is the operator's own: none is read-only.
    assert.equal(await row.getByRole("switch").isDisabled(), false);
    const [rule] = await storedSchedules(page);
    assert.deepEqual(
      [rule.actionType, rule.instanceId, rule.runAtStartup, rule.time, rule.times, rule.everyHourAtMinute],
      ["run_script", "1", false, "*:20", [], null],
    );
    assert.equal("script" in rule, false);

    // Reopened, the rule shows the instance it runs, and a chosen instance cannot be unchosen.
    await row.getByText("fixture script", { exact: true }).click();
    const edit = page.getByRole("dialog", { name: "fixture script", exact: true });
    await edit.getByRole("button", { name: "Script instance", exact: true }).getByText("Nightly report · nightly.py", { exact: true }).waitFor();
    await edit.getByRole("button", { name: "Script instance", exact: true }).click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["Nightly report · nightly.py", "sweep.sh"]);
    await page.getByRole("menuitemradio", { name: "sweep.sh", exact: true }).click();
    await toggle(edit, "Also run at startup").click();
    await edit.getByRole("button", { name: "Save", exact: true }).click();
    await edit.waitFor({ state: "hidden" });
    await row.getByText("Run sweep.sh", { exact: true }).waitFor();
    const [updated] = await storedSchedules(page);
    assert.deepEqual([updated.instanceId, updated.runAtStartup], ["2", true]);
  } finally { await page.close(); }
});

test("a rule whose instance no longer runs on a schedule says so, and can be pointed at one that does", async () => {
  const page = await open("?schedules&stranded");
  try {
    const row = oneShotRow(page, "fixture stranded");
    // The instance is still there, but nothing about it is a schedule's any more.
    await row.getByText("Run script", { exact: true }).waitFor();
    assert.equal(await row.getByRole("switch").isDisabled(), false);
    await row.getByText("fixture stranded", { exact: true }).click();
    const edit = page.getByRole("dialog", { name: "fixture stranded", exact: true });
    const instance = edit.getByRole("button", { name: "Script instance", exact: true });
    await instance.getByText("No longer a schedule instance", { exact: true }).waitFor();
    await instance.click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["Nightly report · nightly.py", "sweep.sh", "No longer a schedule instance"]);
    await page.getByRole("menuitemradio", { name: "Nightly report · nightly.py", exact: true }).click();
    await edit.getByRole("button", { name: "Save", exact: true }).click();
    await edit.waitFor({ state: "hidden" });
    await row.getByText("Run Nightly report", { exact: true }).waitFor();
    assert.equal((await storedSchedules(page))[0].instanceId, "1");
  } finally { await page.close(); }
});

test("an automatic backup and next run update while the backup panel stays open", async () => {
  const page = await open("?automatic");
  try {
    const automatic = page.getByRole("region", { name: "Automatic backups", exact: true });
    const stored = page.getByRole("region", { name: "Stored backups", exact: true });
    await automatic.getByText("A key is stored. Enter a new key to replace it.", { exact: true }).waitFor();
    await stored.getByText("Nothing configured yet.", { exact: true }).waitFor();
    const before = await automatic.innerText();
    await page.evaluate(() => fetch("/fixture/complete-automatic-backup"));
    await page.clock.runFor(30_000);
    await stored.getByText("Ready", { exact: true }).waitFor();
    await stored.getByText("Automatic", { exact: true }).waitFor();
    assert.notEqual(await automatic.innerText(), before);
  } finally { await page.close(); }
});

test("an export downloads without creating a stored backup", async () => {
  const page = await open("");
  try {
    const manual = page.getByRole("region", { name: "Backup", exact: true });
    await manual.getByLabel("Password", { exact: true }).fill("fixture archive key");
    await manual.getByLabel("Confirm password", { exact: true }).fill("fixture archive key");
    const download = page.waitForEvent("download");
    await page.getByRole("button", { name: "Download backup", exact: true }).click();
    assert.match((await download).suggestedFilename(), /^weaver_backup_fixture_/);
    const payload = await page.evaluate(() => fetch("/graphql", {
      method: "POST", body: JSON.stringify({ operationName: "Backups", variables: {} }),
    }).then((response) => response.json()));
    assert.deepEqual(payload.data.backups, []);
    await page.getByRole("region", { name: "Stored backups", exact: true }).getByText("Nothing configured yet.", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("expired backup administration verifies the account password before replaying the mutation", async () => {
  const page = await open("?expired");
  try {
    const automatic = page.getByRole("region", { name: "Automatic backups", exact: true });
    await automatic.getByLabel("Automatic backup key", { exact: true }).fill("fixture archive key");
    await automatic.getByRole("switch", { name: "Enable automatic backups", exact: true }).click();
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    const verification = page.getByRole("dialog", { name: "Current password", exact: true });
    await verification.getByLabel("Current password", { exact: true }).fill("wrong password");
    await verification.getByRole("button", { name: "Continue", exact: true }).click();
    await verification.getByRole("alert").filter({ hasText: "invalid password" }).waitFor();
    assert.equal((await page.evaluate(() => fetch("/fixture/auth-state").then((response) => response.json()))).acceptedBackupMutations, 0);
    await verification.getByLabel("Current password", { exact: true }).fill("fixture account password");
    await verification.getByRole("button", { name: "Continue", exact: true }).click();
    await verification.waitFor({ state: "hidden" });
    await automatic.getByText("A key is stored. Enter a new key to replace it.", { exact: true }).waitFor();
    assert.deepEqual(await page.evaluate(() => fetch("/fixture/auth-state").then((response) => response.json())), { verificationRequests: 2, acceptedBackupMutations: 1, acceptedBackupCreates: 0 });
    assert.equal(await automatic.getByLabel("Automatic backup key", { exact: true }).inputValue(), "");
  } finally { await page.close(); }
});

test("expired REST backup creation verifies the account password and retries once", async () => {
  const page = await open("?expired");
  try {
    const manual = page.getByRole("region", { name: "Backup", exact: true });
    await manual.getByLabel("Password", { exact: true }).fill("fixture archive key");
    await manual.getByLabel("Confirm password", { exact: true }).fill("fixture archive key");
    await page.getByRole("button", { name: "Create without downloading", exact: true }).click();
    const verification = page.getByRole("dialog", { name: "Current password", exact: true });
    await verification.waitFor();
    assert.equal((await page.evaluate(() => fetch("/fixture/auth-state").then((response) => response.json()))).acceptedBackupCreates, 0);
    await verification.getByLabel("Current password", { exact: true }).fill("fixture account password");
    await verification.getByRole("button", { name: "Continue", exact: true }).click();
    await verification.waitFor({ state: "hidden" });
    await page.getByRole("region", { name: "Stored backups", exact: true }).getByText("Ready", { exact: true }).waitFor();
    assert.deepEqual(await page.evaluate(() => fetch("/fixture/auth-state").then((response) => response.json())), { verificationRequests: 1, acceptedBackupMutations: 0, acceptedBackupCreates: 1 });
  } finally { await page.close(); }
});

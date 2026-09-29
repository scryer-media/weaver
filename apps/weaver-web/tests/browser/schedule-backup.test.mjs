import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });
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
      await page.getByRole("button", { name: "Add schedule", exact: true }).first().click();
      const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
      await form.getByLabel("Label", { exact: true }).fill(`fixture ${action}`);
      await form.getByRole("switch", { name: "Enabled", exact: true }).click();
      await form.getByRole("button", { name: "Action", exact: true }).click();
      await page.getByRole("menuitemradio", { name: action, exact: true }).click();
      if (action === "Set server availability") await form.getByLabel("Server", { exact: true }).selectOption("1");
      if (action === "Prune history") await form.getByLabel("Completed", { exact: true }).check();
      if (action === "Fetch RSS") { await form.getByLabel("Every hour", { exact: true }).check(); await form.getByLabel("Minute of the hour", { exact: true }).fill("15"); }
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
      if (action === "Set server availability") {
        await edit.getByRole("option", { name: "fixture-provider", exact: true }).waitFor({ state: "attached" });
        assert.equal(await edit.getByLabel("Server", { exact: true }).inputValue(), "1");
      }
      if (action === "Fetch RSS") assert.equal(await edit.getByLabel("Minute of the hour", { exact: true }).inputValue(), "15");
      if (action === "Prune history") assert.equal(await edit.getByLabel("Completed", { exact: true }).isChecked(), true);
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
    for (const [action, group, checkbox] of [
      ["Set server availability", "Servers", "Server active"],
      ["Set quota metering", "Quota metering", "Count traffic toward the quota"],
    ]) {
      const label = `converted ${group}`;
      await page.getByRole("button", { name: "Add schedule", exact: true }).first().click();
      const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
      await form.getByLabel("Label", { exact: true }).fill(label);
      await form.getByRole("switch", { name: "Enabled", exact: true }).click();
      await form.getByRole("button", { name: "Save", exact: true }).click();
      await form.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: "Downloads", exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      const edit = page.getByRole("dialog", { name: label, exact: true });
      await edit.getByRole("button", { name: "Action", exact: true }).click();
      await page.getByRole("menuitemradio", { name: action, exact: true }).click();
      if (group === "Servers") await edit.getByLabel("Server", { exact: true }).selectOption("1");
      assert.equal(await edit.getByLabel(checkbox, { exact: true }).isChecked(), true);
      await edit.getByRole("button", { name: "Save", exact: true }).click();
      await edit.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      assert.equal(await edit.getByLabel(checkbox, { exact: true }).isChecked(), true);
      await edit.getByLabel(checkbox, { exact: true }).uncheck();
      await edit.getByRole("button", { name: "Save", exact: true }).click();
      await edit.waitFor({ state: "hidden" });
      await page.getByRole("region", { name: group, exact: true }).getByText(group === "Servers" ? "Set server availability: fixture-provider (Off)" : "Set quota metering: Off", { exact: true }).waitFor();
      await page.getByRole("region", { name: group, exact: true }).getByRole("button").filter({ has: page.getByText(label, { exact: true }) }).click();
      assert.equal(await edit.getByLabel(checkbox, { exact: true }).isChecked(), false);
      await edit.getByRole("button", { name: "Cancel", exact: true }).click();
    }
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

test("classic backup page exposes automatic settings and creates a retained backup", async () => {
  const page = await open("?classic");
  try {
    const automatic = page.getByRole("region", { name: "Automatic backups", exact: true });
    await automatic.getByLabel("Automatic backup key", { exact: true }).fill("classic fixture archive key");
    await automatic.getByRole("switch", { name: "Enable automatic backups", exact: true }).click();
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    await automatic.getByText("A key is stored. Enter a new key to replace it.", { exact: true }).waitFor();
    await page.locator("#backup-export-password").fill("classic manual archive key");
    await page.locator("#backup-export-password-confirm").fill("classic manual archive key");
    await page.getByRole("button", { name: "Create without downloading", exact: true }).click();
    const stored = page.getByRole("region", { name: "Stored backups", exact: true });
    await stored.getByText("Ready", { exact: true }).waitFor();
    const download = page.waitForEvent("download");
    await stored.getByRole("button", { name: "Download", exact: true }).click();
    assert.match((await download).suggestedFilename(), /^weaver_backup_fixture_/);
    await page.getByRole("region", { name: "Backup storage", exact: true }).waitFor();
    await automatic.getByRole("switch", { name: "Enable automatic backups", exact: true }).click();
    await automatic.getByRole("button", { name: "Save changes", exact: true }).click();
    await automatic.getByText("Not scheduled", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("both UIs export a download without creating a stored backup", async () => {
  for (const query of ["", "?classic"]) {
    const page = await open(query);
    try {
      if (query.includes("classic")) {
        await page.locator("#backup-export-password").fill("fixture archive key");
        await page.locator("#backup-export-password-confirm").fill("fixture archive key");
      } else {
        const manual = page.getByRole("region", { name: "Backup", exact: true });
        await manual.getByLabel("Password", { exact: true }).fill("fixture archive key");
        await manual.getByLabel("Confirm password", { exact: true }).fill("fixture archive key");
      }
      const download = page.waitForEvent("download");
      await page.getByRole("button", { name: query.includes("classic") ? "Download Backup" : "Download backup", exact: true }).click();
      assert.match((await download).suggestedFilename(), /^weaver_backup_fixture_/);
      const payload = await page.evaluate(() => fetch("/graphql", {
        method: "POST", body: JSON.stringify({ operationName: "Backups", variables: {} }),
      }).then((response) => response.json()));
      assert.deepEqual(payload.data.backups, []);
      await page.getByRole("region", { name: "Stored backups", exact: true }).getByText("Nothing configured yet.", { exact: true }).waitFor();
    } finally { await page.close(); }
  }
});

test("expired backup administration verifies the account password before replaying the mutation in both UIs", async () => {
  for (const query of ["?expired", "?classic&expired"]) {
    const page = await open(query);
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
  }
});


test("expired REST backup creation verifies the account password and retries once in both UIs", async () => {
  for (const query of ["?expired", "?classic&expired"]) {
    const page = await open(query);
    try {
      if (query.includes("classic")) {
        await page.locator("#backup-export-password").fill("fixture archive key");
        await page.locator("#backup-export-password-confirm").fill("fixture archive key");
      } else {
        const manual = page.getByRole("region", { name: "Backup", exact: true });
        await manual.getByLabel("Password", { exact: true }).fill("fixture archive key");
        await manual.getByLabel("Confirm password", { exact: true }).fill("fixture archive key");
      }
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
  }
});

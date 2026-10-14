import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-general-panel", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open() {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/general-panel.html`);
  return { page, errors };
}
/** With `GENERAL_PANEL_SCREENSHOT_DIR` set to a directory, `part` of the page is saved there as `name`.png. */
async function shot(part, name) {
  if (process.env.GENERAL_PANEL_SCREENSHOT_DIR) {
    await part.screenshot({ path: join(process.env.GENERAL_PANEL_SCREENSHOT_DIR, `${name}.png`) });
  }
}
const sent = (page, name) => page.evaluate((name) => window.generalPanelFixture.sent.filter((entry) => entry.name === name).map((entry) => entry.variables), name);
/** Wait until the panel has sent `count` mutations named `name`. */
const sentCount = (page, name, count) =>
  page.waitForFunction(([name, count]) => window.generalPanelFixture.sent.filter((entry) => entry.name === name).length >= count, [name, count]);
/** The panel never offered the top bar anything to save or revert. */
async function assertNeverDirty(page) {
  const { dirty, actions } = await page.evaluate(() => ({
    dirty: window.generalPanelFixture.flags.filter((flags) => flags.dirty).length,
    actions: window.generalPanelFixture.actions.current,
  }));
  assert.equal(dirty, 0);
  assert.equal(actions, null);
}

test("a toggle, a select and a committed number each save on their own, and the top bar never turns dirty", async () => {
  const { page, errors } = await open();
  try {
    const downloads = page.getByRole("region", { name: "Downloads", exact: true });
    const srrdb = downloads.getByRole("switch", { name: "SRRDB release lookup", exact: true });
    const retries = downloads.getByRole("spinbutton", { name: "Article retries", exact: true });
    const delay = downloads.getByRole("spinbutton", { name: "Propagation delay", exact: true });
    await srrdb.waitFor();

    // A toggle saves as it changes, and only its own field.
    await srrdb.click();
    await sentCount(page, "UpdateSettings", 1);
    assert.deepEqual(await sent(page, "UpdateSettings"), [{ input: { enableSrrdbLookup: true } }]);
    await downloads.locator('[role="switch"][aria-checked="true"]').waitFor();

    // A number saves when Enter commits it, not while it is typed.
    await retries.fill("5");
    assert.equal((await sent(page, "UpdateSettings")).length, 1);
    await retries.press("Enter");
    await sentCount(page, "UpdateSettings", 2);
    assert.deepEqual((await sent(page, "UpdateSettings"))[1], { input: { maxRetries: 5 } });

    // Committing what is already stored sends nothing: the next save is the delay's.
    await retries.press("Enter");
    await retries.blur();
    await delay.fill("120");
    await delay.blur();
    await sentCount(page, "UpdateSettings", 3);
    assert.deepEqual((await sent(page, "UpdateSettings")).slice(2), [{ input: { propagationDelaySecs: 120 } }]);

    // A duplicate rule saves alone, as its one field of the policy.
    const duplicates = page.getByRole("region", { name: "Duplicates", exact: true });
    await duplicates.getByRole("button", { name: "Overlapping article set", exact: true }).click();
    await page.getByRole("menu", { name: "Overlapping article set", exact: true }).getByRole("menuitemradio", { name: "Block", exact: true }).click();
    await sentCount(page, "UpdateSettings", 4);
    assert.deepEqual((await sent(page, "UpdateSettings"))[3], { input: { duplicatePolicy: { articleSet: "BLOCK" } } });
    await duplicates.getByRole("button", { name: "Overlapping article set", exact: true }).filter({ hasText: "Block" }).waitFor();

    // Each save went alone through its own mutation.
    assert.equal((await sent(page, "UpdateArchivePasswordSettings")).length, 0);
    assert.equal((await sent(page, "SetUnacceptableExtensions")).length, 0);
    await page.locator("#status").filter({ hasText: /^clean$/ }).waitFor();
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("a refused save keeps what was typed or chosen and shows the daemon's reason", async () => {
  const { page, errors } = await open();
  try {
    const downloads = page.getByRole("region", { name: "Downloads", exact: true });
    const extensions = downloads.getByRole("textbox", { name: "Unacceptable extensions", exact: true });
    const status = page.locator("#status");
    await extensions.waitFor();

    await page.evaluate(() => { window.generalPanelFixture.refuse.SetUnacceptableExtensions = "the extension list was refused"; });
    await extensions.fill("bat, iso");
    await extensions.press("Enter");
    await status.filter({ hasText: /^clean the extension list was refused$/ }).waitFor();
    assert.equal(await extensions.inputValue(), "bat, iso");
    assert.equal(await page.evaluate(() => window.generalPanelFixture.flags.at(-1).failed), true);

    // Committed again once the daemon takes it, it is stored and the reason goes.
    await page.evaluate(() => { delete window.generalPanelFixture.refuse.SetUnacceptableExtensions; });
    await extensions.press("Enter");
    await sentCount(page, "SetUnacceptableExtensions", 2);
    await status.filter({ hasText: /^clean$/ }).waitFor();
    assert.deepEqual(await sent(page, "SetUnacceptableExtensions"), [{ extensions: ["bat", "iso"] }, { extensions: ["bat", "iso"] }]);

    // A refused toggle keeps the choice in view beside the reason.
    const storage = page.getByRole("region", { name: "Storage", exact: true });
    const cleanup = storage.getByRole("switch", { name: "Clean up after unpacking", exact: true });
    await page.evaluate(() => { window.generalPanelFixture.refuse.UpdateSettings = "settings were refused"; });
    await cleanup.click();
    await status.filter({ hasText: /^clean settings were refused$/ }).waitFor();
    assert.equal(await cleanup.getAttribute("aria-checked"), "false");
    assert.deepEqual(await sent(page, "UpdateSettings"), [{ input: { cleanupAfterExtract: false } }]);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the password list stays shut until Edit passwords opens it, saves when left, and Clear removes the stored one", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", { name: "Archive passwords", exact: true });
    const edit = section.getByRole("button", { name: "Edit passwords", exact: true });
    const list = section.getByRole("textbox", { name: "Passwords (one per line)", exact: true });
    const clear = section.getByRole("button", { name: "Clear", exact: true });
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    await clear.waitFor();
    await shot(section, "archive-passwords");

    // Opened and left blank, it shuts again and keeps what is stored.
    await edit.click();
    await list.focus();
    await list.blur();
    await edit.waitFor();
    assert.equal(await list.count(), 0);

    // A typed list saves when the box is left, and the box shuts.
    await edit.click();
    await list.fill("first-secret\nsecond-secret");
    await list.blur();
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    assert.deepEqual(await sent(page, "UpdateArchivePasswordSettings"), [{ passwords: ["first-secret", "second-secret"] }]);

    // Clear removes the stored list at once.
    await clear.click();
    await clear.waitFor({ state: "detached" });
    assert.deepEqual(await sent(page, "UpdateArchivePasswordSettings"), [{ passwords: ["first-secret", "second-secret"] }, { passwords: [] }]);
    assert.equal(await list.count(), 0);
    // Nothing else was touched, so nothing else was sent.
    assert.equal((await sent(page, "SetUnacceptableExtensions")).length, 0);
    assert.equal((await sent(page, "UpdateSettings")).length, 0);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the unacceptable extension list is a Downloads row, and saves on its own as the daemon normalizes it", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", { name: "Downloads", exact: true });
    const extensions = section.getByRole("textbox", { name: "Unacceptable extensions", exact: true });
    await extensions.waitFor();
    assert.equal(await extensions.inputValue(), "bat, cmd, com, exe, js, lnk, msi, ps1, scr, vbs");
    // The whole list shows: the box is wide enough for the default ten without scrolling.
    assert.equal(await extensions.evaluate((input) => input.scrollWidth <= input.clientWidth), true);
    await shot(section, "downloads");

    await extensions.fill("bat, cmd, EXE, iso");
    await extensions.blur();
    await section.getByRole("textbox", { name: "Unacceptable extensions", exact: true }).and(page.locator('[value="bat, cmd, exe, iso"]')).waitFor();
    assert.deepEqual(await sent(page, "SetUnacceptableExtensions"), [{ extensions: ["bat", "cmd", "EXE", "iso"] }]);
    assert.equal((await sent(page, "UpdateSettings")).length, 0);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the hardware profile is a row like the others: its timing under the label, and no note in the band", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", { name: "Performance", exact: true });
    await section.getByRole("button", { name: "Hardware profile", exact: true }).waitFor();
    await section.getByText("Hardware profile", { exact: true }).waitFor();
    await section.getByText("Changes apply to the next download or extraction that starts; work already running finishes with the settings it started with.", { exact: true }).waitFor();
    assert.equal(await page.getByText("Choose your performance profile.", { exact: true }).count(), 0);
    await shot(section, "hardware-profile");
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

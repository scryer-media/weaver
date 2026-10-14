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

test("the password list stays shut until Edit passwords opens it, and shuts again on save and on revert", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", { name: "Archive passwords", exact: true });
    const edit = section.getByRole("button", { name: "Edit passwords", exact: true });
    const list = section.getByRole("textbox", { name: "Passwords (one per line)", exact: true });
    const status = page.locator("#status");
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    await section.getByRole("button", { name: "Clear", exact: true }).waitFor();
    await shot(section, "archive-passwords");

    // Opened, a new list is typed and saved; saving shuts the box again.
    await edit.click();
    await list.fill("first-secret\nsecond-secret");
    assert.equal(await edit.count(), 0);
    await page.evaluate(() => window.generalPanelFixture.actions.current.save());
    await status.filter({ hasText: /^clean Saved/ }).waitFor();
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    assert.deepEqual((await sent(page, "UpdateArchivePasswordSettings"))[0], { passwords: ["first-secret", "second-secret"], passwordFile: null });
    // The extension list was not touched, so it was not sent.
    assert.equal((await sent(page, "SetUnacceptableExtensions")).length, 0);

    // Opened and reverted, it shuts with nothing typed kept.
    await edit.click();
    await list.fill("third-secret");
    await status.filter({ hasText: /^dirty/ }).waitFor();
    await page.evaluate(() => window.generalPanelFixture.actions.current.revert());
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    await status.filter({ hasText: /^clean/ }).waitFor();

    // Clearing shows the box only to say the list goes on save; there is nothing in it to see.
    await section.getByRole("button", { name: "Clear", exact: true }).click();
    assert.equal(await list.inputValue(), "");
    assert.equal(await list.getAttribute("placeholder"), "Removed on save");
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
    await page.locator("#status").filter({ hasText: /^dirty/ }).waitFor();
    await page.evaluate(() => window.generalPanelFixture.actions.current.save());
    await page.locator("#status").filter({ hasText: /^clean Saved/ }).waitFor();
    assert.deepEqual(await sent(page, "SetUnacceptableExtensions"), [{ extensions: ["bat", "cmd", "EXE", "iso"] }]);
    await section.getByRole("textbox", { name: "Unacceptable extensions", exact: true }).and(page.locator('[value="bat, cmd, exe, iso"]')).waitFor();
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

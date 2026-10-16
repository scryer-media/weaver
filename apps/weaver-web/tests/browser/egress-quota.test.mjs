// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-egress-quota", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

const MIB = 1024 ** 2;
const GIB = 1024 ** 3;
const TIB = 1024 ** 4;
const shot = (name) => process.env.NETWORKING_SCREENSHOT?.replace(/\.png$/, `-${name}.png`);
// Dates and sizes read the same on every machine: one locale, one time zone.
async function open(screen, params = {}) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, locale: "en-GB", timezoneId: "UTC" });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/egress-quota.html?${new URLSearchParams({ screen, ...params })}`);
  return { page, errors };
}
const cells = (row) => row.locator(":scope > div").allInnerTexts();
const saved = (page) => page.evaluate(() => structuredClone(window.egressQuotaFixture.saved));
const sent = (page) => page.evaluate(() => structuredClone(window.egressQuotaFixture.sent));
const regions = (page) => page.locator("main section[aria-label]").evaluateAll((sections) => sections.map((section) => section.getAttribute("aria-label")));
const egressRows = (page) => page.getByRole("region", { name: "Egress interfaces", exact: true }).locator("[role=button][style*='grid-template-columns']");
const names = async (page) => (await egressRows(page).locator(":scope > div:first-child").allInnerTexts());

test("the egress table carries each egress's quota usage, System first", async () => {
  const { page, errors } = await open("egress");
  const row = (name) => page.getByRole("button", { name: new RegExp(`^${name}`) });
  await row("LTE standby").waitFor();
  // The daemon listed System last; the table leads with it.
  assert.deepEqual(await names(page), ["System", "WAN Fiber", "LTE standby"]);
  // The quota column: used against allowance, then what is left and when the window resets, and the refusal when there is one.
  assert.deepEqual((await cells(row("System"))).slice(4), ["—", "Edit"]);
  assert.deepEqual((await cells(row("WAN Fiber"))).slice(4), ["400 GB / 1.0 TB\n624 GB remaining · Resets 1 Nov, 00:00", "Edit"]);
  assert.deepEqual((await cells(row("LTE standby"))).slice(4), ["20.0 GB / 20.0 GB\nBLOCKED\n0 B remaining · Resets 1 Nov, 00:00", "Edit"]);
  assert.equal(await row("WAN Fiber").getByText("Blocked", { exact: true }).count(), 0);
  // One list of egresses: the usage is not drawn a second time.
  assert.deepEqual(await regions(page), ["Limits", "Egress interfaces"]);
  assert.equal(await page.getByText("Download quotas", { exact: true }).count(), 0);
  if (shot("egress-table")) await page.screenshot({ path: shot("egress-table"), fullPage: true });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the download ceiling sits above the egress table and saves through the settings", async () => {
  const { page, errors } = await open("egress");
  const status = page.locator("#status");
  const ceiling = page.getByRole("spinbutton", { name: "Download ceiling", exact: true });
  await ceiling.waitFor();
  assert.deepEqual(await regions(page), ["Limits", "Egress interfaces"]);
  assert.equal(await ceiling.inputValue(), "25");
  // Each egress's own limit is the table's column and its editor's field; there is no second place to set it.
  assert.equal(await page.getByRole("button", { name: "Save limit", exact: true }).count(), 0);
  // The ISP cap stays gone: the ceiling is the one global limit to edit here.
  assert.equal(await page.getByText("Enforce a data cap", { exact: true }).count(), 0);
  assert.deepEqual((await cells(page.getByRole("button", { name: /^WAN Fiber/ }))).slice(3, 4), ["Unlimited"]);

  await ceiling.fill("8");
  await ceiling.press("Tab");
  await status.filter({ hasText: /^dirty/ }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  assert.deepEqual((await sent(page)).filter((entry) => entry.name === "UpdateSettings").map((entry) => entry.variables), [
    { input: { maxDownloadSpeed: 8 * MIB } },
  ]);
  assert.equal(await ceiling.inputValue(), "8");
  if (shot("egress")) await page.screenshot({ path: shot("egress"), fullPage: true });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the settings search finds the ceiling and the quota terms on the Egress page", async () => {
  const visible = async (search) => {
    const { page, errors } = await open("egress", { search });
    await page.locator("main section[aria-label]").first().waitFor();
    const found = { regions: await regions(page), rows: await names(page) };
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
    return found;
  };
  assert.deepEqual(await visible("ceiling"), { regions: ["Limits"], rows: [] });
  assert.deepEqual(await visible("throttle"), { regions: ["Limits"], rows: [] });
  assert.deepEqual(await visible("allowance"), { regions: ["Egress interfaces"], rows: ["System", "WAN Fiber", "LTE standby"] });
  assert.deepEqual(await visible("quota"), { regions: ["Egress interfaces"], rows: ["System", "WAN Fiber", "LTE standby"] });
  assert.deepEqual(await visible("blocked"), { regions: ["Egress interfaces"], rows: ["LTE standby"] });
  assert.deepEqual(await visible("remaining"), { regions: ["Egress interfaces"], rows: ["WAN Fiber", "LTE standby"] });
});

test("the former Bandwidth addresses open the Egress page", async () => {
  for (const from of ["/settings/bandwidth", "/settings/networking/bandwidth"]) {
    const { page, errors } = await open("redirect", { from });
    await page.getByRole("status", { name: "Landed at", exact: true }).filter({ hasText: /^\/settings\/networking\/egress$/ }).waitFor();
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  }
});

test("the egress editor saves a download quota, and refuses one switched on with no allowance", async () => {
  const { page, errors } = await open("egress");
  const row = (name) => page.getByRole("button", { name: new RegExp(`^${name}`) });
  await row("LTE standby").waitFor();
  // The line opens its egress, and so does the button on it.
  const editor = page.getByRole("dialog", { name: "WAN Fiber", exact: true });
  await row("WAN Fiber").getByRole("button", { name: "Edit", exact: true }).click();
  await editor.getByRole("button", { name: "Cancel", exact: true }).click();
  await editor.waitFor({ state: "detached" });
  await page.getByText("WAN Fiber", { exact: true }).click();
  await editor.getByText("Download quota", { exact: true }).first().waitFor();
  assert.equal(await editor.getByRole("switch", { name: "Enforce a download quota", exact: true }).getAttribute("aria-checked"), "true");
  await editor.getByText("Used so far: 400 GB.", { exact: true }).waitFor();
  const allowance = editor.getByRole("textbox", { name: "Allowance", exact: true });
  assert.equal(await allowance.inputValue(), "1");
  await allowance.fill("2");
  if (shot("egress-editor")) await editor.screenshot({ path: shot("egress-editor") });
  await editor.getByRole("button", { name: "Save egress", exact: true }).click();
  await editor.waitFor({ state: "detached" });
  const [update] = await saved(page);
  assert.equal(update.name, "UpdateEgress");
  assert.equal(update.variables.id, 1);
  assert.deepEqual(update.variables.input.downloadQuota, {
    enabled: true, limitBytes: 2 * TIB, period: "MONTHLY", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
  });

  // System carries a quota like any other egress; switched on, it needs an allowance first.
  await row("System").click();
  const system = page.getByRole("dialog", { name: "System", exact: true });
  await system.getByRole("switch", { name: "Enforce a download quota", exact: true }).click();
  await system.getByRole("button", { name: "Save egress", exact: true }).click();
  await system.getByText("Set an allowance before switching the quota on.", { exact: true }).waitFor();
  assert.equal((await saved(page)).length, 1);
  await system.getByRole("button", { name: "Quota window", exact: true }).click();
  await page.getByRole("menuitemradio", { name: "Daily", exact: true }).click();
  await system.getByRole("textbox", { name: "Allowance", exact: true }).fill("500");
  await system.getByRole("button", { name: "Save egress", exact: true }).click();
  await system.waitFor({ state: "detached" });
  const [, created] = await saved(page);
  assert.equal(created.variables.id, 0);
  assert.deepEqual(created.variables.input.downloadQuota, {
    enabled: true, limitBytes: 500 * GIB, period: "DAILY", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
  });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the block banner names the egress whose quota is spent and when its window ends", async () => {
  const { page, errors } = await open("banner");
  await page.getByText("Download quota reached on LTE standby", { exact: true }).waitFor();
  await page.getByText("resumes 1 Nov, 00:00", { exact: true }).waitFor();
  if (shot("banner")) await page.locator("aside").screenshot({ path: shot("banner") });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a block with no window end points at the Egress page", async () => {
  const { page, errors } = await open("banner-open-ended");
  await page.getByText("Download quota reached on LTE standby", { exact: true }).waitFor();
  await page.getByText("Settings → Networking → Egress", { exact: true }).waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

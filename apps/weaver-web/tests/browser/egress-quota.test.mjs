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

test("the egress table carries each egress's limits and caps per direction, System first", async () => {
  const { page, errors } = await open("egress");
  const row = (name) => page.getByRole("button", { name: new RegExp(`^${name}`) });
  await row("LTE standby").waitFor();
  // The daemon listed System last; the table leads with it.
  assert.deepEqual(await names(page), ["System", "WAN Fiber", "LTE standby"]);
  // The limit column: one line per direction.
  assert.deepEqual((await cells(row("System"))).slice(3), ["↓ Unlimited\n↑ Unlimited", "—", "Edit"]);
  assert.deepEqual((await cells(row("WAN Fiber"))).slice(3), [
    "↓ Unlimited\n↑ 5.0 MiB/s",
    "↓\n400 GB / 1.0 TB\nResets 1 Nov, 00:00",
    "Edit",
  ]);
  // A direction its cap is holding back says so; the other direction carries on.
  assert.deepEqual((await cells(row("LTE standby"))).slice(3), [
    "↓ 2.0 MiB/s\n↑ Unlimited",
    "↓\n20.0 GB / 20.0 GB\nSTOPPED\n↑\n1.0 GB / 2.0 GB\nResets 1 Nov, 00:00",
    "Edit",
  ]);
  assert.equal(await row("WAN Fiber").getByText("Stopped", { exact: true }).count(), 0);
  // One list of egresses: the usage is not drawn a second time.
  assert.deepEqual(await regions(page), ["Limits", "Egress interfaces"]);
  if (shot("egress-table")) await page.screenshot({ path: shot("egress-table"), fullPage: true });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the global limits sit above the egress table, one per direction, and save together", async () => {
  const { page, errors } = await open("egress");
  const status = page.locator("#status");
  const limits = page.getByRole("region", { name: "Limits", exact: true });
  const download = limits.getByRole("spinbutton", { name: "Download limit", exact: true });
  await download.waitFor();
  assert.deepEqual(await regions(page), ["Limits", "Egress interfaces"]);
  assert.equal(await download.inputValue(), "25");
  // Off is no limit at all: the rate shows only while the limit is on.
  const upload = limits.getByRole("switch", { name: "Upload limit", exact: true });
  assert.equal(await upload.getAttribute("aria-checked"), "false");
  assert.equal(await limits.getByRole("spinbutton", { name: "Upload limit", exact: true }).count(), 0);

  await download.fill("8");
  await download.press("Tab");
  await upload.click();
  await limits.getByRole("spinbutton", { name: "Upload limit", exact: true }).fill("3");
  await limits.getByRole("spinbutton", { name: "Upload limit", exact: true }).press("Tab");
  await status.filter({ hasText: /^dirty/ }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  assert.deepEqual((await sent(page)).filter((entry) => entry.name === "SetBandwidth").map((entry) => entry.variables), [
    { input: { downloadBps: 8 * MIB, uploadBps: 3 * MIB, split: null } },
  ]);
  assert.equal(await download.inputValue(), "8");
  if (shot("egress")) await page.screenshot({ path: shot("egress"), fullPage: true });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the split shares the download limit and stays editable without one", async () => {
  const { page, errors } = await open("egress");
  const status = page.locator("#status");
  const limits = page.getByRole("region", { name: "Limits", exact: true });
  const split = limits.getByRole("switch", { name: "Bandwidth split", exact: true });
  const usenet = limits.getByRole("spinbutton", { name: "Usenet", exact: true });
  await split.waitFor();
  // No split: the shares show only once one is switched on.
  assert.equal(await split.getAttribute("aria-checked"), "false");
  assert.equal(await usenet.count(), 0);
  await split.click();
  assert.equal(await usenet.inputValue(), "50");
  await limits.getByText("Torrents 50%", { exact: true }).waitFor();
  await usenet.fill("80");
  await usenet.press("Tab");
  await limits.getByText("Torrents 20%", { exact: true }).waitFor();
  await status.filter({ hasText: /^dirty/ }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  // The rates each side is held to sit beside the split once it is in force.
  await limits.getByText("Usenet ↓ 20.0 MiB/s · Torrents ↓ 5.0 MiB/s", { exact: true }).waitFor();
  if (shot("split")) await limits.screenshot({ path: shot("split") });

  // Without a download limit the saved share stays editable and says what it needs.
  await limits.getByRole("switch", { name: "Download limit", exact: true }).click();
  await limits.getByText("Needs a global download limit", { exact: true }).waitFor();
  assert.equal(await split.isDisabled(), false);
  assert.equal(await usenet.isDisabled(), false);
  assert.equal(await usenet.inputValue(), "80");
  await usenet.fill("70");
  await usenet.press("Tab");
  await limits.getByText("Torrents 30%", { exact: true }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  await limits.getByText("Usenet ↓", { exact: false }).waitFor({ state: "detached" });

  // The split can be lifted and restored without setting a download limit.
  await split.click();
  await usenet.waitFor({ state: "detached" });
  await status.filter({ hasText: /^dirty/ }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  await split.click();
  await limits.getByText("Torrents 30%", { exact: true }).waitFor();
  await status.filter({ hasText: /^dirty/ }).waitFor();
  await page.evaluate(() => window.egressQuotaFixture.actions.current.save());
  await status.filter({ hasText: /^clean Saved$/ }).waitFor();
  assert.deepEqual((await sent(page)).filter((entry) => entry.name === "SetBandwidth").map((entry) => entry.variables), [
    { input: { downloadBps: 25 * MIB, uploadBps: null, split: { usenetPct: 80 } } },
    { input: { downloadBps: null, uploadBps: null, split: { usenetPct: 70 } } },
    { input: { downloadBps: null, uploadBps: null, split: null } },
    { input: { downloadBps: null, uploadBps: null, split: { usenetPct: 70 } } },
  ]);
  if (shot("split-needs-limit")) await limits.screenshot({ path: shot("split-needs-limit") });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the settings search finds the limits and the quota terms on the Egress page", async () => {
  const visible = async (search) => {
    const { page, errors } = await open("egress", { search });
    await page.locator("main section[aria-label]").first().waitFor();
    const found = { regions: await regions(page), rows: await names(page) };
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
    return found;
  };
  assert.deepEqual(await visible("throttle"), { regions: ["Limits"], rows: [] });
  assert.deepEqual(await visible("bandwidth"), { regions: ["Limits"], rows: [] });
  assert.deepEqual(await visible("quota"), { regions: ["Egress interfaces"], rows: ["System", "WAN Fiber", "LTE standby"] });
  assert.deepEqual(await visible("stopped"), { regions: ["Egress interfaces"], rows: ["LTE standby"] });
  assert.deepEqual(await visible("resets"), { regions: ["Egress interfaces"], rows: ["WAN Fiber", "LTE standby"] });
});

test("the former Bandwidth addresses open the Egress page", async () => {
  for (const from of ["/settings/bandwidth", "/settings/networking/bandwidth"]) {
    const { page, errors } = await open("redirect", { from });
    await page.getByRole("status", { name: "Landed at", exact: true }).filter({ hasText: /^\/settings\/networking\/egress$/ }).waitFor();
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  }
});

test("the egress editor saves its limits and caps, and refuses a cap switched on with no allowance", async () => {
  const { page, errors } = await open("egress");
  const row = (name) => page.getByRole("button", { name: new RegExp(`^${name}`) });
  await row("LTE standby").waitFor();
  // The line opens its egress, and so does the button on it.
  const editor = page.getByRole("dialog", { name: "WAN Fiber", exact: true });
  await row("WAN Fiber").getByRole("button", { name: "Edit", exact: true }).click();
  await editor.getByRole("button", { name: "Cancel", exact: true }).click();
  await editor.waitFor({ state: "detached" });
  await page.getByText("WAN Fiber", { exact: true }).click();
  await editor.getByText("Download cap", { exact: true }).first().waitFor();
  assert.equal(await editor.getByRole("switch", { name: "Download cap", exact: true }).getAttribute("aria-checked"), "true");
  assert.equal(await editor.getByRole("switch", { name: "Upload hard cap", exact: true }).getAttribute("aria-checked"), "false");
  await editor.getByText("Used so far: 400 GB.", { exact: true }).waitFor();
  const allowance = editor.getByRole("textbox", { name: "Download cap", exact: true });
  assert.equal(await allowance.inputValue(), "1");
  await allowance.fill("2");
  // The per-egress upload limit is the editor's field, beside the download one.
  assert.equal(await editor.getByRole("spinbutton", { name: "Upload limit", exact: true }).inputValue(), "5");
  await editor.getByRole("switch", { name: "Download limit", exact: true }).click();
  // A hard cap switched on with nothing typed is refused before anything is sent.
  await editor.getByRole("switch", { name: "Upload hard cap", exact: true }).click();
  await editor.getByRole("button", { name: "Save egress", exact: true }).click();
  await editor.getByText("Set an allowance before switching the quota on.", { exact: true }).waitFor();
  assert.equal((await saved(page)).length, 0);
  await editor.getByRole("textbox", { name: "Upload hard cap", exact: true }).fill("50");
  if (shot("egress-editor")) await editor.screenshot({ path: shot("egress-editor") });
  await editor.getByRole("button", { name: "Save egress", exact: true }).click();
  await editor.waitFor({ state: "detached" });
  const [update, quota] = await saved(page);
  assert.equal(update.name, "UpdateEgress");
  assert.equal(update.variables.id, 1);
  assert.equal(update.variables.input.downloadBps, 10 * MIB);
  assert.equal(update.variables.input.uploadBps, 5 * MIB);
  assert.equal(quota.name, "SetEgressQuota");
  assert.deepEqual(quota.variables, {
    id: 1,
    input: {
      downloadBytes: 2 * TIB, uploadSoftBytes: null, uploadTrickleBps: 2048 * 1024, uploadHardBytes: 50 * GIB,
      period: "MONTHLY", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
    },
  });

  // System carries caps like any other egress.
  await row("System").click();
  const system = page.getByRole("dialog", { name: "System", exact: true });
  await system.getByRole("switch", { name: "Download cap", exact: true }).click();
  await system.getByRole("button", { name: "Quota window", exact: true }).click();
  await page.getByRole("menuitemradio", { name: "Daily", exact: true }).click();
  await system.getByRole("textbox", { name: "Download cap", exact: true }).fill("500");
  await system.getByRole("button", { name: "Save egress", exact: true }).click();
  await system.waitFor({ state: "detached" });
  const [, , , created] = await saved(page);
  assert.equal(created.name, "SetEgressQuota");
  assert.equal(created.variables.id, 0);
  assert.deepEqual(created.variables.input, {
    downloadBytes: 500 * GIB, uploadSoftBytes: null, uploadTrickleBps: 2048 * 1024, uploadHardBytes: null,
    period: "DAILY", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
  });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("an egress's own names are answered by the host or by its servers, in the order listed, with no proxy and no path", async () => {
  const { page, errors } = await open("egress");
  const option = (scope, name) => scope.getByRole("radio", { name, exact: true });
  await page.getByText("WAN Fiber", { exact: true }).click();
  const editor = page.getByRole("dialog", { name: "WAN Fiber", exact: true });
  // An egress saved before it had DNS is the host answering.
  assert.equal(await option(editor, "The host").getAttribute("aria-checked"), "true");
  assert.equal(await option(editor, "The proxy").count(), 0);
  await option(editor, "These servers").click();
  // Its servers are always asked straight from it: there is no path to choose.
  assert.equal(await editor.getByTestId("field-dnsPath").count(), 0);
  // Servers chosen and none named is refused before anything is sent.
  await editor.getByRole("button", { name: "Save egress", exact: true }).click();
  await editor.getByText("Add at least one DNS server.", { exact: true }).waitFor();
  assert.equal((await saved(page)).length, 0);
  await editor.getByTestId("field-dnsServer-0-address").fill("192.0.2.53");
  const add = editor.getByTestId("field-dnsServerAdd");
  for (let row = 1; row < 10; row += 1) {
    await add.click();
    await editor.getByTestId(`field-dnsServer-${row}-address`).fill(`192.0.2.${53 + row}`);
  }
  // Ten servers are the most an egress keeps, as for a proxy.
  assert.equal(await add.isDisabled(), true);
  // The last row moves to the top: it is asked first.
  for (let row = 9; row > 0; row -= 1) {
    await editor.getByTestId(`field-dnsServer-${row}-moveUp`).click();
  }
  assert.equal(await editor.getByTestId("field-dnsServer-0-address").inputValue(), "192.0.2.62");
  for (let row = 9; row > 1; row -= 1) {
    await editor.getByTestId(`field-dnsServer-${row}-remove`).click();
  }
  assert.equal(await add.isDisabled(), false);
  await editor.getByTestId("field-dnsServer-1-wire").click();
  await page.getByRole("menuitemradio", { name: "DNS over TLS", exact: true }).click();
  // A plain server under required authentication can be faked, and that is said; validating here cannot.
  const warning = editor.getByTestId("help-dnsPlainWarning");
  await option(editor, "Require authenticated").click();
  await warning.waitFor();
  assert.equal(await warning.textContent(), "A plain query can be altered on the way, so the authenticated mark can be faked.");
  await option(editor, "Validate here").click();
  await warning.waitFor({ state: "detached" });
  await option(editor, "Require authenticated").click();
  if (shot("egress-dns")) await editor.screenshot({ path: shot("egress-dns") });
  await editor.getByRole("button", { name: "Save egress", exact: true }).click();
  await editor.waitFor({ state: "detached" });
  const [update] = await saved(page);
  assert.equal(update.name, "UpdateEgress");
  assert.deepEqual(update.variables.input.dns, {
    who: "SERVERS",
    trust: "REQUIRE_AD",
    servers: [
      { wire: "PLAIN", address: "192.0.2.62", tlsName: null, url: null },
      { wire: "TLS", address: "192.0.2.53", tlsName: null, url: null },
    ],
  });

  // The host answering sends no servers, whatever was typed before it was chosen.
  await page.getByText("LTE standby", { exact: true }).click();
  const standby = page.getByRole("dialog", { name: "LTE standby", exact: true });
  await option(standby, "These servers").click();
  await standby.getByTestId("field-dnsServer-0-address").fill("192.0.2.53");
  await option(standby, "The host").click();
  assert.equal(await standby.getByTestId("field-dnsServer-0-address").count(), 0);
  await standby.getByRole("button", { name: "Save egress", exact: true }).click();
  await standby.waitFor({ state: "detached" });
  const host = (await saved(page)).at(-1);
  assert.equal(host.variables.id, 2);
  assert.deepEqual(host.variables.input.dns, { who: "HOST", trust: "AS_SENT", servers: [] });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("an egress's DNS rows fit a phone, every control in reach", async () => {
  const page = await browser.newPage({ viewport: { width: 375, height: 812 }, locale: "en-GB", timezoneId: "UTC" });
  page.setDefaultTimeout(0);
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/egress-quota.html?screen=egress`);
  await page.getByText("WAN Fiber", { exact: true }).click();
  const editor = page.getByRole("dialog", { name: "WAN Fiber", exact: true });
  await editor.getByRole("radio", { name: "These servers", exact: true }).click();
  await editor.getByTestId("field-dnsServerAdd").click();
  const width = await page.evaluate(() => document.documentElement.clientWidth);
  for (const id of ["wire", "moveUp", "moveDown", "remove", "address"]) {
    const box = await editor.getByTestId(`field-dnsServer-1-${id}`).boundingBox();
    assert.ok(box && box.x >= 0 && box.x + box.width <= width, id);
  }
  if (shot("egress-dns-phone")) await page.screenshot({ path: shot("egress-dns-phone") });
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

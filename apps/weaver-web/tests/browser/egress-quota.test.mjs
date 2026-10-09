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

const GIB = 1024 ** 3;
const TIB = 1024 ** 4;
const shot = (name) => process.env.NETWORKING_SCREENSHOT?.replace(/\.png$/, `-${name}.png`);
// Dates and sizes read the same on every machine: one locale, one time zone.
async function open(screen) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, locale: "en-GB", timezoneId: "UTC" });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/egress-quota.html?screen=${screen}`);
  return { page, errors };
}
const cells = (row) => row.locator(":scope > div").allInnerTexts();
const saved = (page) => page.evaluate(() => structuredClone(window.egressQuotaFixture.saved));

test("the egress editor saves a download quota, and refuses one switched on with no allowance", async () => {
  const { page, errors } = await open("egress");
  const row = (name) => page.getByRole("button", { name: new RegExp(`^${name}`) });
  // The table's quota column: used against allowance, or a dash where there is none.
  assert.deepEqual((await cells(row("WAN Fiber"))).slice(4), ["400 GB / 1.0 TB"]);
  assert.deepEqual((await cells(row("LTE standby"))).slice(4), ["20.0 GB / 20.0 GB"]);
  assert.deepEqual((await cells(row("System"))).slice(4), ["—"]);

  await row("WAN Fiber").click();
  const editor = page.getByRole("dialog", { name: "WAN Fiber", exact: true });
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

test("the Bandwidth panel lists every egress's quota usage, System first", async () => {
  const { page, errors } = await open("bandwidth");
  await page.getByText("Download quotas", { exact: true }).waitFor();
  const rows = page.locator("div[style*='grid-template-columns']").filter({ has: page.locator(":scope > div") });
  const usage = async (name) => cells(rows.filter({ hasText: new RegExp(`^${name}`) }).first());
  await rows.filter({ hasText: /^LTE standby/ }).first().waitFor();
  assert.deepEqual(await usage("System"), ["System", "3.0 GB", "—", "—", "—", "—"]);
  assert.deepEqual(await usage("WAN Fiber"), ["WAN Fiber", "400 GB", "1.0 TB", "624 GB", "1 Nov, 00:00", "Open"]);
  assert.deepEqual(await usage("LTE standby"), ["LTE standby", "20.0 GB", "20.0 GB", "0 B", "1 Nov, 00:00", "BLOCKED"]);
  // The ISP cap is gone: the panel keeps the global ceiling and nothing else to edit.
  await page.getByText("Download ceiling", { exact: true }).waitFor();
  assert.equal(await page.getByText("Enforce a data cap", { exact: true }).count(), 0);
  if (shot("bandwidth")) await page.screenshot({ path: shot("bandwidth"), fullPage: true });
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

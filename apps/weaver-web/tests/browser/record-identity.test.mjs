import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
const RUNNER_BOUND = { timeout: 10 * 60_000 };
before(async () => {
  server = await createServer({
    cacheDir: "node_modules/.vite/browser-record-identity",
    server: { host: "127.0.0.1", port: 0 },
    plugins: [{ name: "record-identity-fixture", enforce: "pre", load(id) {
      // Keep the actual pages and urql hooks; omit unrelated application chrome.
      if (id.endsWith("/next/data/next-data.tsx")) return 'export const useNextData = () => ({ connection: { isDisconnected: false } });';
      if (id.endsWith("/next/shell/NextShell.tsx")) return 'export const NextShell = ({header, controls, children}) => <main>{header}{controls}{children}</main>; export const RailBlock = ({children}) => <aside>{children}</aside>;';
      if (process.env.RECORD_IDENTITY_BASELINE && id.includes("/src/")) {
        const relative = id.slice(id.indexOf("/src/") + 1);
        try { return readFileSync(resolve(process.env.RECORD_IDENTITY_BASELINE, relative), "utf8"); } catch { /* Only changed files are supplied. */ }
      }
    } }],
  });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open(screen = "providers") {
  const page = await browser.newPage({ viewport: { width: 1500, height: 1200 } });
  page.setDefaultTimeout(0);
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/record-identity.html?screen=${screen}`);
  await page.waitForFunction(() => window.recordFixture?.ready);
  return page;
}
const host = (page) => page.getByLabel(/^(Host|Hostname|next.providers.host)$/);
async function edit(page, id) {
  await page.getByText(`provider-${id}.example`, { exact: true }).click();
}
async function cancel(page) { await page.getByRole("button", { name: "Cancel", exact: true }).click(); }
async function pending(page, name) { await page.waitForFunction((n) => window.recordFixture.pending.some((r) => r.name === n), name); }
async function release(page, name) { await page.evaluate((n) => window.recordFixture.release(n), name); }

test(`switching equal-priority providers waits for the selected ID and saves its values`, RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    assert.equal(await host(page).inputValue(), "provider-1.example");
    await cancel(page);
    await edit(page, 2);
    await pending(page, "Server");
    assert.equal(await host(page).count(), 0, "old provider fields must not be editable during the fetch");
    await release(page, "Server");
    await host(page).waitFor();
    assert.equal(await host(page).inputValue(), "provider-2.example");
    await page.getByRole("button", { name: /^(Save|Save Changes|Save changes)$/ }).click();
    await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "UpdateServer"));
    const saved = await page.evaluate(() => window.recordFixture.mutations.find((m) => m.name === "UpdateServer").variables);
    assert.equal(saved.id, 2);
    assert.equal(saved.input.host, "provider-2.example");
    assert.equal(saved.input.username, "account-2");
    assert.equal(saved.input.connections, 20);
  } finally { await page.close(); }
});

test(`add after edit starts blank`, RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    await cancel(page);
    await page.getByRole("button", { name: /^(Add provider|next.providers.add)$/i }).click();
    await host(page).waitFor();
    assert.equal(await host(page).inputValue(), "");
  } finally { await page.close(); }
});

const SAVE = /^(Save|Save Changes|Save changes)$/;
const toggle = (page, name) => page.getByRole("switch", { name, exact: true });
/** With `RECORD_SCREENSHOT` set to a `.png` path, the editor as it stands is saved beside it. */
async function shot(page, name) {
  if (process.env.RECORD_SCREENSHOT) await page.screenshot({ path: process.env.RECORD_SCREENSHOT.replace(/\.png$/, `-${name}.png`) });
}
const sent = (page, name) => page.evaluate((n) => window.recordFixture.mutations.filter((m) => m.name === n).map((m) => m.variables), name);

test("a kill switch saves a new provider switched off behind a blocked route, untested", RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await page.getByRole("button", { name: "Add provider", exact: true }).click();
    await host(page).fill("held-provider.example");
    const enabled = toggle(page, "Enabled");
    const killSwitch = toggle(page, "Kill switch");
    const testConnection = page.getByRole("button", { name: "Test connection", exact: true });
    assert.equal(await enabled.getAttribute("aria-checked"), "true");
    assert.equal(await killSwitch.getAttribute("aria-checked"), "false");
    await page.getByText(/^Starts on the direct route/).waitFor();

    await killSwitch.click();
    await page.getByText(/^Starts with nothing allowed out/).waitFor();
    assert.equal(await enabled.getAttribute("aria-checked"), "false");
    assert.equal(await enabled.isDisabled(), true);
    assert.equal(await testConnection.isDisabled(), true);

    // Taking the kill switch back off gives the provider back as it was filled in.
    await killSwitch.click();
    await page.getByText(/^Starts on the direct route/).waitFor();
    assert.equal(await enabled.getAttribute("aria-checked"), "true");
    assert.equal(await enabled.isDisabled(), false);
    assert.equal(await testConnection.isDisabled(), false);

    await killSwitch.click();
    await shot(page, "provider-kill-switch");
    await page.getByRole("button", { name: SAVE }).click();
    await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "AddServer"));
    const [{ input }] = await sent(page, "AddServer");
    assert.equal(input.host, "held-provider.example");
    assert.equal(input.active, false);
    assert.deepEqual(input.routing, { proxyIds: [], allowDirect: false });
    assert.equal("route" in input, false);
    assert.deepEqual(await sent(page, "TestConnection"), []);
  } finally { await page.close(); }
});

test("a new provider without the kill switch is saved as filled in, on the default route", RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await page.getByRole("button", { name: "Add provider", exact: true }).click();
    await host(page).fill("open-provider.example");
    await page.getByRole("button", { name: SAVE }).click();
    await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "AddServer"));
    const [{ input }] = await sent(page, "AddServer");
    assert.equal(input.active, true);
    assert.equal("routing" in input, false);
    assert.equal("route" in input, false);
  } finally { await page.close(); }
});

test("a saved provider has no kill switch to set, and is tested by the route it has", RUNNER_BOUND, async () => {
  const page = await open();
  try {
    const testConnection = page.getByRole("button", { name: "Test connection", exact: true });
    await edit(page, 3);
    await host(page).waitFor();
    assert.equal(await toggle(page, "Kill switch").count(), 0);
    assert.equal(await toggle(page, "Enabled").isDisabled(), false);
    const tested = (count) => page.waitForFunction((n) => window.recordFixture.mutations.filter((m) => m.name === "TestConnection").length === n, count);
    await testConnection.click();
    await tested(1);
    await cancel(page);
    await edit(page, 1);
    await host(page).waitFor();
    await testConnection.click();
    await tested(2);
    const [held, direct] = await sent(page, "TestConnection");
    // Behind a kill switch the test is told so: left unsaid, it would go direct.
    assert.deepEqual(held.input.routing, { proxyIds: [], allowDirect: false });
    assert.equal("route" in held.input, false);
    assert.deepEqual(direct.input.route, { failover: "REDISTRIBUTE", legs: [{ egressId: 0, weight: 100, path: { direct: true } }] });
    assert.equal("routing" in direct.input, false);
  } finally { await page.close(); }
});

test("a kill switch saves a new feed switched off behind a blocked route", RUNNER_BOUND, async () => {
  const page = await open("feeds");
  try {
    await page.getByRole("button", { name: "Add feed", exact: true }).first().click();
    await page.getByLabel("Name", { exact: true }).fill("Held feed");
    await page.getByLabel("URL", { exact: true }).fill("https://feed.example/rss");
    const enabled = toggle(page, "Enabled");
    const killSwitch = toggle(page, "Kill switch");
    assert.equal(await enabled.getAttribute("aria-checked"), "true");
    await page.getByText(/^Starts on the direct route/).waitFor();
    await killSwitch.click();
    await page.getByText(/^Starts with nothing allowed out/).waitFor();
    assert.equal(await enabled.getAttribute("aria-checked"), "false");
    assert.equal(await enabled.isDisabled(), true);
    await shot(page, "feed-kill-switch");
    await page.getByRole("button", { name: SAVE }).click();
    await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "AddRssFeed"));
    const [{ input }] = await sent(page, "AddRssFeed");
    assert.equal(input.name, "Held feed");
    assert.equal(input.enabled, false);
    assert.deepEqual(input.routing, { proxyIds: [], allowDirect: false });
    assert.equal("route" in input, false);
  } finally { await page.close(); }
});

test("a new feed without the kill switch is saved as filled in, on the default route", RUNNER_BOUND, async () => {
  const page = await open("feeds");
  try {
    await page.getByRole("button", { name: "Add feed", exact: true }).first().click();
    await page.getByLabel("Name", { exact: true }).fill("Open feed");
    await page.getByLabel("URL", { exact: true }).fill("https://feed.example/rss");
    await page.getByRole("button", { name: SAVE }).click();
    await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "AddRssFeed"));
    const [{ input }] = await sent(page, "AddRssFeed");
    assert.equal(input.enabled, true);
    assert.equal("routing" in input, false);
    assert.equal("route" in input, false);
  } finally { await page.close(); }
});

test(`a late connection test cannot attach its certificate to a reopened editor`, RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    await page.evaluate(() => window.recordFixture.hold("TestConnection"));
    await page.getByRole("button", { name: /^(Test Connection|Test connection|next.providers.test)$/ }).click();
    await pending(page, "TestConnection");
    await page.keyboard.press("Escape");
    // Reopen the same ID to isolate response ownership from query identity.
    await edit(page, 1);
    await host(page).waitFor();
    await release(page, "TestConnection");
    assert.equal(await host(page).inputValue(), "provider-1.example");
    assert.equal(await page.getByText("Result for provider 1", { exact: true }).count(), 0);
    assert.equal(await page.getByText("old-fingerprint", { exact: false }).count(), 0);
  } finally { await page.close(); }
});

test(`connection changes hide a previous test certificate`, RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    await page.getByRole("button", { name: /^Test connection$/i }).click();
    await page.getByText("old-fingerprint", { exact: false }).waitFor();
    await host(page).fill("changed-provider.example");
    assert.equal(await page.getByText("old-fingerprint", { exact: false }).count(), 0);
    assert.equal(await page.getByText("Result for provider 1", { exact: true }).count(), 0);
  } finally { await page.close(); }
});

test(`a late save cannot close a reopened editor`, RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    await page.evaluate(() => window.recordFixture.hold("UpdateServer"));
    await page.getByRole("button", { name: /^(Save|Save Changes|Save changes)$/ }).click();
    await pending(page, "UpdateServer");
    await page.keyboard.press("Escape");
    await edit(page, 1);
    await host(page).waitFor();
    await host(page).fill("unsaved-edit.example");
    await release(page, "UpdateServer");
    assert.equal(await host(page).count(), 1);
    assert.equal(await host(page).inputValue(), "unsaved-edit.example");
  } finally { await page.close(); }
});

test("a late deletion cannot close another provider editor", RUNNER_BOUND, async () => {
  const page = await open();
  try {
    await edit(page, 1);
    await host(page).waitFor();
    await page.evaluate(() => window.recordFixture.hold("RemoveServer"));
    await page.getByRole("button", { name: /^(Delete|Remove provider)$/ }).click();
    await page.getByRole("dialog", { name: "Remove provider", exact: true }).getByRole("button", { name: "Remove provider", exact: true }).click();
    await pending(page, "RemoveServer");
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");
    await edit(page, 2);
    await pending(page, "Server");
    await release(page, "Server");
    await host(page).waitFor();
    await host(page).fill("unsaved-edit.example");
    await release(page, "RemoveServer");
    assert.equal(await host(page).count(), 1);
    assert.equal(await host(page).inputValue(), "unsaved-edit.example");
  } finally { await page.close(); }
});

test(`job navigation drops old details and delete confirmations`, RUNNER_BOUND, async () => {
  const page = await open("jobs");
  try {
    await page.getByRole("heading", { name: "Job-1", exact: true }).waitFor();
    await page.getByRole("button", { name: /next.job.deleteSaveFiles|Delete.*keep/i }).first().click();
    await page.getByRole("dialog").waitFor();
    await page.evaluate(() => window.recordFixture.navigate("/jobs/2"));
    await pending(page, "Job");
    assert.equal(await page.getByRole("heading", { name: "Job-1", exact: true }).count(), 0);
    assert.equal(await page.getByRole("dialog").count(), 0);
    await release(page, "Job");
    await page.getByRole("heading", { name: "Job-2", exact: true }).waitFor();
    assert.equal(await page.getByRole("dialog").count(), 0);
    assert.equal(await page.evaluate(() => window.recordFixture.mutations.length), 0);
  } finally { await page.close(); }
});

test(`a completed deletion cannot navigate away from another job`, RUNNER_BOUND, async () => {
  const page = await open("jobs");
  try {
    await page.getByRole("heading", { name: "Job-1", exact: true }).waitFor();
    await page.evaluate(() => window.recordFixture.hold("AcceptHistoryDelete"));
    await page.getByRole("button", { name: /Delete.*keep/i }).first().click();
    await page.getByRole("dialog").getByRole("button", { name: /Delete/i }).click();
    await pending(page, "AcceptHistoryDelete");
    await page.evaluate(() => window.recordFixture.navigate("/jobs/2"));
    await pending(page, "Job");
    await release(page, "Job");
    await page.getByRole("heading", { name: "Job-2", exact: true }).waitFor();
    await release(page, "AcceptHistoryDelete");
    assert.equal(await page.getByRole("heading", { name: "Job-2", exact: true }).count(), 1);
  } finally { await page.close(); }
});

test(`late folder creation cannot replace a reopened picker`, RUNNER_BOUND, async () => {
  const page = await open("folders");
  try {
    await page.evaluate(() => window.recordFixture.openFolder("/first"));
    await page.getByRole("dialog").waitFor();
    const path = page.getByLabel("Folder path");
    await path.waitFor();
    await page.waitForFunction(() => [...document.querySelectorAll("input")].some((input) => input.value === "/first"));
    await page.evaluate(() => window.recordFixture.hold("CreateDirectory"));
    const name = page.getByPlaceholder("new folder name", { exact: true });
    await name.fill("child");
    await page.getByRole("button", { name: /^Create folder$/i }).click();
    await pending(page, "CreateDirectory");
    await cancel(page);
    await page.evaluate(() => window.recordFixture.openFolder("/second"));
    await page.waitForFunction(() => [...document.querySelectorAll("input")].some((input) => input.value === "/second"));
    await release(page, "CreateDirectory");
    assert.equal(await path.inputValue(), "/second");
    await page.getByRole("button", { name: "Use this folder", exact: true }).click();
    assert.equal(await page.evaluate(() => window.recordFixture.chosen), "/second");
  } finally { await page.close(); }
});

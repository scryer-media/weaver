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

async function open(variant, screen = "providers") {
  const page = await browser.newPage({ viewport: { width: 1500, height: 1200 } });
  page.setDefaultTimeout(0);
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/record-identity.html?variant=${variant}&screen=${screen}`);
  await page.waitForFunction(() => window.recordFixture?.ready);
  return page;
}
const host = (page, variant) => variant === "next" ? page.getByLabel(/^(Host|Hostname|next.providers.host)$/) : page.locator("#server-host");
async function edit(page, variant, id) {
  if (variant === "next") await page.getByText(`provider-${id}.example`, { exact: true }).click();
  else await page.getByRole("row").filter({ hasText: `provider-${id}.example` }).getByRole("button", { name: "Edit", exact: true }).click();
}
async function cancel(page) { await page.getByRole("button", { name: "Cancel", exact: true }).click(); }
async function pending(page, name) { await page.waitForFunction((n) => window.recordFixture.pending.some((r) => r.name === n), name); }
async function release(page, name) { await page.evaluate((n) => window.recordFixture.release(n), name); }

for (const variant of ["legacy", "next"]) {
  test(`${variant}: switching equal-priority providers waits for the selected ID and saves its values`, RUNNER_BOUND, async () => {
    const page = await open(variant);
    try {
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      assert.equal(await host(page, variant).inputValue(), "provider-1.example");
      await cancel(page);
      await edit(page, variant, 2);
      await pending(page, "Server");
      assert.equal(await host(page, variant).count(), 0, "old provider fields must not be editable during the fetch");
      await release(page, "Server");
      await host(page, variant).waitFor();
      assert.equal(await host(page, variant).inputValue(), "provider-2.example");
      await page.getByRole("button", { name: /^(Save|Save Changes|Save changes)$/ }).click();
      await page.waitForFunction(() => window.recordFixture.mutations.some((m) => m.name === "UpdateServer"));
      const saved = await page.evaluate(() => window.recordFixture.mutations.find((m) => m.name === "UpdateServer").variables);
      assert.equal(saved.id, 2);
      assert.equal(saved.input.host, "provider-2.example");
      assert.equal(saved.input.username, "account-2");
      assert.equal(saved.input.connections, 20);
    } finally { await page.close(); }
  });

  test(`${variant}: add after edit starts blank`, RUNNER_BOUND, async () => {
    const page = await open(variant);
    try {
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await cancel(page);
      if (variant === "legacy") await page.getByTestId("add-server-button").click();
      else await page.getByRole("button", { name: /^(Add provider|next.providers.add)$/i }).click();
      await host(page, variant).waitFor();
      assert.equal(await host(page, variant).inputValue(), "");
    } finally { await page.close(); }
  });

  test(`${variant}: a late connection test cannot attach its certificate to a reopened editor`, RUNNER_BOUND, async () => {
    const page = await open(variant);
    try {
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await page.evaluate(() => window.recordFixture.hold("TestConnection"));
      await page.getByRole("button", { name: /^(Test Connection|Test connection|next.providers.test)$/ }).click();
      await pending(page, "TestConnection");
      if (variant === "next") await page.keyboard.press("Escape"); else await cancel(page);
      // Reopen the same ID to isolate response ownership from query identity.
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await release(page, "TestConnection");
      assert.equal(await host(page, variant).inputValue(), "provider-1.example");
      assert.equal(await page.getByText("Result for provider 1", { exact: true }).count(), 0);
      assert.equal(await page.getByText("old-fingerprint", { exact: false }).count(), 0);
    } finally { await page.close(); }
  });

  test(`${variant}: a late save cannot close a reopened editor`, RUNNER_BOUND, async () => {
    const page = await open(variant);
    try {
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await page.evaluate(() => window.recordFixture.hold("UpdateServer"));
      await page.getByRole("button", { name: /^(Save|Save Changes|Save changes)$/ }).click();
      await pending(page, "UpdateServer");
      if (variant === "next") await page.keyboard.press("Escape"); else await cancel(page);
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await host(page, variant).fill("unsaved-edit.example");
      await release(page, "UpdateServer");
      assert.equal(await host(page, variant).count(), 1);
      assert.equal(await host(page, variant).inputValue(), "unsaved-edit.example");
    } finally { await page.close(); }
  });

  if (variant === "next") test("next: a late deletion cannot close another provider editor", RUNNER_BOUND, async () => {
    const page = await open(variant);
    try {
      await edit(page, variant, 1);
      await host(page, variant).waitFor();
      await page.evaluate(() => window.recordFixture.hold("RemoveServer"));
      await page.getByRole("button", { name: /^(Delete|Remove provider)$/ }).click();
      await page.getByRole("dialog", { name: "Remove provider", exact: true }).getByRole("button", { name: "Remove provider", exact: true }).click();
      await pending(page, "RemoveServer");
      await page.keyboard.press("Escape");
      await page.keyboard.press("Escape");
      await edit(page, variant, 2);
      await pending(page, "Server");
      await release(page, "Server");
      await host(page, variant).waitFor();
      await host(page, variant).fill("unsaved-edit.example");
      await release(page, "RemoveServer");
      assert.equal(await host(page, variant).count(), 1);
      assert.equal(await host(page, variant).inputValue(), "unsaved-edit.example");
    } finally { await page.close(); }
  });

  test(`${variant}: job navigation drops old details and delete confirmations`, RUNNER_BOUND, async () => {
    const page = await open(variant, "jobs");
    try {
      await page.getByRole("heading", { name: "Job-1", exact: true }).waitFor();
      await page.getByRole("button", { name: variant === "next" ? /next.job.deleteSaveFiles|Delete.*keep/i : /^Delete$/ }).first().click();
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

  test(`${variant}: late folder creation cannot replace a reopened picker`, RUNNER_BOUND, async () => {
    const page = await open(variant, "folders");
    try {
      await page.evaluate(() => window.recordFixture.openFolder("/first"));
      await page.getByRole("dialog").waitFor();
      const path = variant === "next" ? page.getByLabel("Folder path") : page.locator("#directory-browser-path");
      await path.waitFor();
      await page.waitForFunction(() => [...document.querySelectorAll("input")].some((input) => input.value === "/first"));
      await page.evaluate(() => window.recordFixture.hold("CreateDirectory"));
      const name = variant === "next" ? page.getByPlaceholder("new folder name", { exact: true }) : page.getByPlaceholder("New folder name", { exact: true });
      await name.fill("child");
      await page.getByRole("button", { name: /^Create folder$/i }).click();
      await pending(page, "CreateDirectory");
      await cancel(page);
      await page.evaluate(() => window.recordFixture.openFolder("/second"));
      await page.waitForFunction(() => [...document.querySelectorAll("input")].some((input) => input.value === "/second"));
      await release(page, "CreateDirectory");
      assert.equal(await path.inputValue(), "/second");
      await page.getByRole("button", { name: variant === "next" ? "Use this folder" : "Use Current Folder", exact: true }).click();
      assert.equal(await page.evaluate(() => window.recordFixture.chosen), "/second");
    } finally { await page.close(); }
  });
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-api-key-copy", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

/** The panel with a key just minted by the deep link, on a LAN origin when `lan` is set. */
async function open({ lan }) {
  const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
  await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin: baseUrl });
  const page = await context.newPage();
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  const params = new URLSearchParams({ createApiKey: "1", name: "Sonarr", scope: "control", ...(lan ? { lan: "1" } : {}) });
  await page.goto(`${baseUrl}/tests/browser/api-key-copy.html?${params}`);
  const dialog = page.getByRole("dialog", { name: "API key created" });
  await dialog.waitFor();
  return { page, errors, dialog };
}
const commands = (page) => page.evaluate(() => structuredClone(window.copyFixture.commands));
const rawKey = (page) => page.evaluate(() => window.copyFixture.rawKey);
/** The key field keeps the whole key selected with focus, so the user's own copy keystroke still works. */
async function assertKeySelected(page) {
  const selection = await page.evaluate(() => {
    const field = document.activeElement;
    return field instanceof HTMLInputElement
      ? { value: field.value, start: field.selectionStart, end: field.selectionEnd }
      : null;
  });
  assert.deepEqual(selection, { value: await rawKey(page), start: 0, end: (await rawKey(page)).length });
}

test("on a plain-HTTP LAN origin, which has no asynchronous clipboard, the copy button still copies the key", async () => {
  const { page, errors, dialog } = await open({ lan: true });
  try {
    assert.equal(await page.evaluate(() => navigator.clipboard), undefined);
    await dialog.getByText("Copy key", { exact: true }).click();
    await dialog.getByText("Copied", { exact: true }).waitFor();
    assert.deepEqual(await commands(page), [{ command: "copy", text: await rawKey(page) }]);
    await assertKeySelected(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.context().close();
  }
});

test("the copy icon inside the field copies the key the same way", async () => {
  const { page, errors, dialog } = await open({ lan: true });
  try {
    await dialog.locator("button[title='Copy key']").click();
    await dialog.locator("button[title='Copied']").waitFor();
    assert.deepEqual(await commands(page), [{ command: "copy", text: await rawKey(page) }]);
    assert.deepEqual(errors, []);
  } finally {
    await page.context().close();
  }
});

test("on a secure origin the asynchronous clipboard takes the key and no selection command runs", async () => {
  const { page, errors, dialog } = await open({ lan: false });
  try {
    await dialog.getByText("Copy key", { exact: true }).click();
    await dialog.getByText("Copied", { exact: true }).waitFor();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), await rawKey(page));
    assert.deepEqual(await commands(page), []);
    await assertKeySelected(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.context().close();
  }
});

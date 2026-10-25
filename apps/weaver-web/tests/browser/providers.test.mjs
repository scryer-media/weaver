// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-providers", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open(params) {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/providers.html?${new URLSearchParams(params)}`);
  return { page, errors };
}
const EMPTY = "No providers yet. Add one to start downloading.";
const REFUSAL = "admin scope required";
const hosts = (count) => Array.from({ length: count }, (_, index) => `news${index + 1}.example.test`);

test("a refused list says so in place of the table and reads again on Retry", async () => {
  const { page, errors } = await open({ servers: "3", refuse: "" });
  try {
    const alert = page.getByRole("alert");
    await alert.getByText(REFUSAL, { exact: true }).waitFor();
    assert.equal(await page.getByText(EMPTY, { exact: true }).count(), 0);
    // The failure is drawn in the table, so the status line does not repeat it.
    assert.equal(await page.locator("#status").textContent(), "ok");

    const refusedReads = await page.evaluate(() => window.providersFixture.reads.length);
    await page.evaluate(() => { window.providersFixture.refusing = false; });
    await alert.getByRole("button", { name: "Retry", exact: true }).click();
    for (const host of hosts(3)) await page.getByText(host, { exact: true }).waitFor();
    assert.equal(await page.getByRole("alert").count(), 0);
    // The rows came from the read Retry asked for, not from an earlier one.
    assert.ok((await page.evaluate(() => window.providersFixture.reads.length)) > refusedReads);
    assert.deepEqual(errors, []);
  } finally { await page.close(); }
});

test("every configured server is listed, however many there are", async () => {
  const { page, errors } = await open({ servers: "40" });
  try {
    for (const host of hosts(40)) await page.getByText(host, { exact: true }).waitFor();
    assert.equal(await page.getByText(EMPTY, { exact: true }).count(), 0);
    assert.equal(await page.getByRole("alert").count(), 0);
    assert.deepEqual(errors, []);
  } finally { await page.close(); }
});

test("a failure beside rows keeps the rows and puts the failure on the status line", async () => {
  const { page, errors } = await open({ servers: "2", partial: "" });
  try {
    for (const host of hosts(2)) await page.getByText(host, { exact: true }).waitFor();
    await page.locator("#status").getByText(`failed ${REFUSAL}`, { exact: true }).waitFor();
    assert.equal(await page.getByRole("alert").count(), 0);
    assert.deepEqual(errors, []);
  } finally { await page.close(); }
});

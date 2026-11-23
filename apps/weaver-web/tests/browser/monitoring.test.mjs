// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-monitoring", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

test("switching dashboards keeps the shell and swaps only what the dashboard fills", async () => {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/monitoring.html?dashboard=engine`);
  // The Engine dashboard fills the title controls with its range.
  await page.getByRole("radiogroup", { name: "Time range" }).waitFor();
  // Mark the shell's footer, which a shell drawn again would replace.
  const footer = page.getByTestId("shell-footer");
  await footer.evaluate((node) => { node.dataset.marked = "kept"; });
  const engineAsked = await page.evaluate(() => window.monitoringFixture.asked.filter((name) => name === "MetricsHistory").length);

  await page.getByTestId("monitoring-tab-integrity").click();
  await page.waitForFunction(() => document.querySelector("[data-testid='monitoring-tab-integrity']")?.getAttribute("aria-pressed") === "true");
  // The Engine's range has left the title bar with its dashboard.
  await page.waitForFunction(() => document.querySelectorAll("[role='radiogroup'][aria-label='Time range']").length === 0);
  // The Integrity dashboard has drawn from its answer, so its mount has settled.
  await page.getByText("Repaired per day", { exact: false }).waitFor();
  assert.equal(await footer.getAttribute("data-marked"), "kept");
  // A mount asks each of its reads at most twice, and a read the shell
  // already holds is never asked again by a dashboard mounting beside it.
  const asked = await page.evaluate(() => window.monitoringFixture.asked);
  const counts = Object.groupBy(asked, (name) => name);
  for (const [name, list] of Object.entries(counts)) assert.ok(list.length <= 2, `${name} asked ${list.length} times`);
  assert.ok(counts.IntegrityHistory?.length >= 1);
  // The Engine's reads stay where they were once it has gone.
  assert.equal(asked.filter((name) => name === "MetricsHistory").length, engineAsked);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

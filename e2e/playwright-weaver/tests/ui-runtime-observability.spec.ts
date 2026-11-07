// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Page, WebSocketRoute } from "@playwright/test";
import { expect, openNavigation, test, weaverRoute } from "./helpers";
import { isOperation, operationResponse } from "./support/graphql-operations";
import { introspectPublicMutationNames } from "./support/runtime-introspection";
import { byTestIdPrefix, choose } from "./support/settings-locators";
import { seedRuntimeHistory } from "./support/setup/runtime-history";
import { runtimeIds, settingsIds, testIds } from "./support/testids";

type CoverageEntry = {
  name?: string;
  path?: string;
  owner: string;
  oracle: "browser" | "api-metrics" | "existing-pipeline" | "unit-only";
  rationale?: string;
};

type CoverageLedger = {
  version: number;
  routes: CoverageEntry[];
  mutations: CoverageEntry[];
  behaviors: CoverageEntry[];
};

const coverageLedger = JSON.parse(
  readFileSync(resolve(process.cwd(), "coverage-ledger.v1.json"), "utf8"),
) as CoverageLedger;

const staticBrowserRoutes = coverageLedger.routes
  .filter(({ oracle, path }) => oracle === "browser" && path && !path.includes(":"))
  .map(({ path }) => path as string);
const configuredBasePath = new URL(
  process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090/",
).pathname.replace(/\/+$/, "");

test("all user-facing routes render, refresh, and retain live connectivity", async ({ cleanPage: page }) => {
  for (const route of staticBrowserRoutes) {
    await test.step(route, async () => {
      const response = await page.goto(weaverRoute(route));
      expect(response?.ok(), `${route} returned ${response?.status()}`).toBeTruthy();
      const main = page.getByRole("main");
      await expect(main).not.toContainText(/application error|failed to fetch/i);
      await expect(main).toBeVisible();
      await page.reload();
      await expect(main).toBeVisible();
    });
  }
});

test("visible navigation routes are assigned to the Weaver coverage ledger", async ({ cleanPage: page }) => {
  await page.goto(weaverRoute("/"));
  await expect(page.getByRole("main")).toBeVisible();

  const navigation = await openNavigation(page);
  const navigationLinks = await navigation.getByRole("link").all();
  const navigationHrefs = await Promise.all(
    navigationLinks.map((link) => link.getAttribute("href")),
  );
  const currentUrl = new URL(page.url());
  const visibleNavigationPaths = Array.from(
    new Set(
      navigationHrefs
        .filter((href): href is string => href != null)
        .map((href) => new URL(href, currentUrl))
        .filter((url) => url.origin === currentUrl.origin)
        .map((url) => {
          const applicationPath = url.pathname.startsWith(configuredBasePath)
            ? url.pathname.slice(configuredBasePath.length)
            : url.pathname;
          return applicationPath.replace(/\/+$/, "") || "/";
        }),
    ),
  ).sort();

  expect(visibleNavigationPaths, "Weaver rendered no visible navigation links").not.toEqual([]);
  const ownedPaths = new Set(coverageLedger.routes.map(({ path }) => path));
  const missingOwners = visibleNavigationPaths.filter((path) => !ownedPaths.has(path));
  expect(
    missingOwners,
    `visible Weaver routes missing from coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
});

test("monitoring metrics and live logs expose real product state and controls", async ({ cleanPage: page }) => {
  await page.goto(weaverRoute("/monitoring"));
  await expect(page.getByTestId(runtimeIds.monitoringPipeline)).toBeVisible();
  await expect(page.getByTestId(runtimeIds.monitoringConnections)).toBeVisible();
  await expect(page.getByTestId(runtimeIds.monitoringDepths)).toBeVisible();
  // The provider list reads live server health, so the configured provider
  // shows up by host: the fixture's host, not the app's copy.
  const providerHost = "nntp";
  await expect(page.getByTestId(runtimeIds.monitoringProviders).getByText(providerHost, { exact: true })).toBeVisible();

  await page.goto(weaverRoute("/logs"));
  const following = page.getByTestId(runtimeIds.logsTail("following"));
  await expect(following).toBeVisible();
  await page.getByTestId(runtimeIds.logsPauseTail).click();
  await expect(page.getByTestId(runtimeIds.logsTail("paused"))).toBeVisible();
  await page.getByTestId(runtimeIds.logsResumeTail).click();
  await expect(following).toBeVisible();

  // The live log level: a preset applies at once, the field shows the
  // directives now in force, and Default hands back the startup filter.
  const logLevel = page.getByTestId(runtimeIds.logsLevel);
  const levelShown = (preset: string) => logLevel.getByTestId(testIds.selected(runtimeIds.logsLevel, preset));
  const directives = page.getByTestId(runtimeIds.logsDirectives);
  await expect(levelShown("default")).toBeVisible();
  const startupDirectives = await directives.inputValue();
  await chooseLogLevel(page, "download");
  await expect(directives).toHaveValue(
    "info,weaver_server_core::pipeline::download=debug,weaver_nntp=debug",
  );
  await expect(levelShown("download")).toBeVisible();
  await chooseLogLevel(page, "default");
  await expect(directives).toHaveValue(startupDirectives);
  await expect(levelShown("default")).toBeVisible();
});

/** Pick a log-level preset, by its preset key, and wait for the server to apply it. */
async function chooseLogLevel(page: Page, preset: string) {
  const applied = operationResponse(page, "SetLogFilter");
  await choose(page, page, runtimeIds.logsLevel, preset);
  const response = await applied;
  expect(response.ok()).toBeTruthy();
  const payload = await response.json();
  expect(payload.errors ?? [], JSON.stringify(payload.errors ?? [])).toEqual([]);
}

test("history pagination renders metadata-only seeded records", async ({ cleanPage: page, request }) => {
  const suffix = configuredBasePath.replaceAll("/", "-") || "root";
  const markers = await seedRuntimeHistory(request, `e2e-runtime-history-${suffix}`);
  await page.goto(weaverRoute("/history"));
  const historyRow = (name: string) =>
    byTestIdPrefix(page, testIds.historyRowPrefix).filter({ has: page.getByText(name, { exact: true }) });
  await expect(historyRow(markers.newest)).toBeVisible();
  await page
    .getByTestId(testIds.paginationPageSize)
    .getByTestId(testIds.option(testIds.paginationPageSize, "25"))
    .click();
  await expect(historyRow(markers.oldest)).toHaveCount(0);
  const next = page.getByTestId(testIds.paginationNext);
  await expect(next).toBeEnabled();
  await next.click();
  await expect(historyRow(markers.oldest)).toBeVisible();
});

test("subscription loss polls and reconnects without losing the visible application", async ({ cleanPage: page }) => {
  let blockReconnects = false;
  let socketConnections = 0;
  let liveBrowserSocket: WebSocketRoute | undefined;
  let liveServerSocket: WebSocketRoute | undefined;
  let blockedReconnectSocket: WebSocketRoute | undefined;
  // Settles when the client opens a live socket again after the interruption.
  let reconnected!: () => void;
  const reconnect = new Promise<void>((resolve) => {
    reconnected = resolve;
  });

  await page.routeWebSocket(/\/graphql\/ws(?:\?|$)/, (socket) => {
    if (blockReconnects) {
      // Return so Playwright can still observe the HTTP polling fallback.
      blockedReconnectSocket = socket;
      return;
    }
    liveBrowserSocket = socket;
    liveServerSocket = socket.connectToServer();
    socketConnections += 1;
    if (socketConnections > 1) reconnected();
  });

  await page.goto(weaverRoute("/"));
  await expect(page.getByRole("main")).toBeVisible();
  const status = page.getByTestId(testIds.shellConnection);
  const connected = status.getByTestId(testIds.shellConnectionState("connected"));
  await expect(connected).toBeVisible();
  await expect.poll(() => liveBrowserSocket).toBeTruthy();
  await expect.poll(() => liveServerSocket).toBeTruthy();
  expect(socketConnections).toBe(1);

  blockReconnects = true;
  // The HTTP fallback polls live metrics by name once the socket is gone.
  const polled = page.waitForRequest((request) => isOperation(request, "LiveMetrics"));
  try {
    await liveServerSocket!.close({ code: 1012, reason: "Weaver e2e subscription interruption" });
    await liveBrowserSocket!.close({ code: 1012, reason: "Weaver e2e subscription interruption" });
    await expect(status.getByTestId(/^shell-connection-(?:reconnecting|polling)$/)).toBeVisible();
    await polled;
    await expect(status.getByTestId(testIds.shellConnectionState("polling"))).toBeVisible();
    await expect(page.getByRole("main")).toBeVisible();
  } finally {
    blockReconnects = false;
    await blockedReconnectSocket?.close({
      code: 1012,
      reason: "Weaver e2e reconnect interruption released",
    });
  }
  await reconnect;
  await expect(connected).toBeVisible();
  await expect(page.getByRole("main")).toBeVisible();
});

test("the analyzer renders and downloads a redacted report without queueing", async ({ cleanPage: page }) => {
  // The NZB-only path still opens the analyzer.
  await page.goto(weaverRoute("/tools/nzb-analyzer"));
  const sentinel = "e2e-analyzer-private";
  const nzb = `<?xml version="1.0" encoding="UTF-8"?>
    <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
      <head><meta type="password">${sentinel}</meta></head>
      <file poster="${sentinel}@e2e.invalid" date="1700000000" subject="${sentinel}.bin">
        <groups><group>alt.binaries.${sentinel}</group></groups>
        <segments><segment bytes="1024" number="1">${sentinel}@e2e.invalid</segment></segments>
      </file>
    </nzb>`;
  const chooser = page.waitForEvent("filechooser");
  await page.getByTestId(runtimeIds.analyzerDrop).click();
  await (await chooser).setFiles({ name: "analysis-probe.nzb", mimeType: "application/x-nzb", buffer: Buffer.from(nzb) });
  const report = page.getByTestId(runtimeIds.analyzerReport);
  await expect(report).toContainText("fingerprint ");
  await expect(report).not.toContainText(sentinel);
  const downloadPromise = page.waitForEvent("download");
  await page.getByTestId(runtimeIds.analyzerDownloadJson).click();
  const download = await downloadPromise;
  const stream = await download.createReadStream();
  if (!stream) throw new Error("the analyzer did not retain its download");
  const chunks: Buffer[] = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  const json = Buffer.concat(chunks).toString("utf8");
  expect(json).not.toContain(sentinel);
  const parsed = JSON.parse(json);
  expect(parsed.nzb.shape.file_count).toBe(1);
  expect(parsed.job).toBeNull();

  const infohash = "0123456789abcdef0123456789abcdef01234567";
  await page.getByRole("textbox", { name: "Magnet link", exact: true }).fill(`magnet:?xt=urn:btih:${infohash}&dn=${sentinel}`);
  await page.getByRole("button", { name: "Analyze", exact: true }).click();
  await expect(report).toContainText("source magnet");
  await expect(report).not.toContainText(sentinel);
  await expect(report).not.toContainText(infohash);
  await page.goto(weaverRoute("/"));
  await expect(page.getByTestId(testIds.queueEmpty)).toBeVisible();
});

test("stored backup storage, download and deletion are browser-owned", async ({ cleanPage: page }) => {
  await page.goto(weaverRoute("/settings/backup"));
  const storage = page.getByTestId(testIds.section("backupStorage"));
  const path = storage.getByTestId(testIds.field("backupPath"));
  await expect(path).toBeVisible();
  const originalPath = await path.inputValue();
  const customPath = `/data/e2e-backups-${configuredBasePath.replaceAll("/", "-") || "root"}`;
  const savePath = async (value: string) => {
    // Focusing a path opens its picker; cancel it before typing a path directly.
    await path.click();
    const picker = page.getByTestId(testIds.dialog("picker"));
    await picker.getByTestId(testIds.pickerCancel).click();
    await expect(picker).toBeHidden();
    await path.fill(value);
    const saved = operationResponse(page, "UpdateBackupSettings");
    await storage.getByTestId(settingsIds.backupStorageSave).click();
    expect((await saved).ok()).toBeTruthy();
    await page.reload();
    await expect(path).toHaveValue(value);
  };
  await savePath(customPath);
  try {
    const stored = page.getByTestId(testIds.section("storedBackups"));
    const downloads = stored.getByTestId(settingsIds.storedBackupDownload);
    await expect(downloads).toHaveCount(0);
    const backup = page.getByTestId(testIds.section("export"));
    await backup.getByTestId(testIds.field("password")).fill("e2e-stored-backup-key");
    await backup.getByTestId(testIds.field("passwordConfirm")).fill("e2e-stored-backup-key");
    const created = page.waitForResponse((response) =>
      new URL(response.url()).pathname.endsWith("/api/backup/create")
      && response.request().method() === "POST",
    );
    await page.getByTestId(settingsIds.backupCreate).click();
    const response = await created;
    expect(response.status()).toBe(202);
    const info = await response.json() as { filename: string };
    const storedRow = stored.getByTestId(testIds.row("storedBackups", info.filename));
    await expect(storedRow).toBeVisible();
    const downloadButton = downloads;
    await expect(downloadButton).toHaveCount(1);
    await expect(downloadButton).toBeEnabled();
    await expect(storedRow.getByTestId(settingsIds.storedBackupStatus("ready"))).toBeVisible();
    const downloadPromise = page.waitForEvent("download");
    await downloadButton.click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toBe(info.filename);
    const stream = await download.createReadStream();
    if (!stream) throw new Error("the stored backup did not retain its download");
    let bytes = 0;
    for await (const chunk of stream) bytes += chunk.length;
    expect(bytes).toBeGreaterThan(0);
    await storedRow.getByTestId(settingsIds.storedBackupDelete).click();
    const confirm = page.getByTestId(testIds.dialog("stored-backup-delete"));
    await expect(confirm).toContainText(info.filename);
    await confirm.getByTestId(testIds.confirmAccept).click();
    await expect(confirm).toBeHidden();
    await expect(storedRow).toHaveCount(0);
    await page.reload();
    await expect(downloads).toHaveCount(0);
  } finally {
    await savePath(originalPath);
  }
});

test("every public GraphQL mutation has a release-gate owner", async ({ request }) => {
  const schemaMutations = await introspectPublicMutationNames(request);
  const ledgerMutations = coverageLedger.mutations
    .map(({ name }) => name)
    .filter((name): name is string => Boolean(name))
    .sort();

  expect(schemaMutations, "Weaver exposes no GraphQL mutation fields").not.toEqual([]);
  expect(
    schemaMutations.filter((name) => !ledgerMutations.includes(name)),
    `public mutations missing from coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
  expect(
    ledgerMutations.filter((name) => !schemaMutations.includes(name)),
    `stale mutation entries in coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
});

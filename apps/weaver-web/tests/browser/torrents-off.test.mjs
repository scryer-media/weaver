// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-torrents-off", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open(screen, presence) {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/torrents-off.html?screen=${screen}&presence=${presence}`);
  return { page, errors };
}
const sent = (page, name) => page.evaluate((name) => window.torrentsOffFixture.sent.filter((entry) => entry.name === name).map((entry) => entry.variables), name);
const sentCount = (page, name, count) =>
  page.waitForFunction(([name, count]) => window.torrentsOffFixture.sent.filter((entry) => entry.name === name).length >= count, [name, count]);

for (const [presence, title] of [
  ["off", "with torrents off and none left"],
  ["unavailable", "on a server without torrents, whatever is saved"],
]) test(`${title}, Downloads says nothing of torrents`, async () => {
  const { page, errors } = await open("downloads", presence);
  const empty = page.getByTestId("queue-empty");
  await empty.waitFor();
  await empty.getByText("Add an NZB, or drop one anywhere on this list.", { exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Pause torrents", exact: true }).count(), 0);
  assert.equal(await page.getByRole("button", { name: /^Torrent/ }).count(), 0);
  // The list opens on All, and Seeding is not among the tabs.
  assert.equal(await page.getByTestId("queue-tab-all").getAttribute("aria-pressed"), "true");
  for (const tab of ["downloading", "queued", "paused"]) await page.getByTestId(`queue-tab-${tab}`).waitFor();
  assert.deepEqual(
    await page.locator("[data-testid^='queue-tab-']").evaluateAll((nodes) => nodes.map((node) => node.dataset.testid)),
    ["queue-tab-all", "queue-tab-downloading", "queue-tab-queued", "queue-tab-paused"],
  );
  assert.equal(await page.getByTestId("queue-tab-seeding").count(), 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("with torrents off but some still seeding, Downloads shows those torrents' controls but asks for NZBs alone", async () => {
  const { page, errors } = await open("downloads", "offJobs");
  const empty = page.getByTestId("queue-empty");
  await empty.waitFor();
  await empty.getByText("Add an NZB, or drop one anywhere on this list.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Pause torrents", exact: true }).waitFor();
  await page.getByRole("button", { name: /^Torrent/ }).waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("with torrents on, Downloads asks for an NZB or a torrent", async () => {
  const { page, errors } = await open("downloads", "onIdle");
  const empty = page.getByTestId("queue-empty");
  await empty.waitFor();
  await empty.getByText("Add an NZB or torrent, or drop one anywhere on this list.", { exact: true }).waitFor();
  // Seeding sits among the tabs while torrents are on, after Downloading.
  await page.getByTestId("queue-tab-seeding").waitFor();
  assert.deepEqual(
    await page.locator("[data-testid^='queue-tab-']").evaluateAll((nodes) => nodes.map((node) => node.dataset.testid)),
    ["queue-tab-all", "queue-tab-downloading", "queue-tab-seeding", "queue-tab-queued", "queue-tab-paused"],
  );
  assert.equal(await page.getByTestId("queue-tab-all").getAttribute("aria-pressed"), "true");
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

for (const presence of ["off", "offJobs"]) {
  test(`with the switch off (${presence}), the Torrents panel shows the switch alone`, async () => {
    const { page, errors } = await open("panel", presence);
    await page.getByTestId("field-enabled").waitFor();
    const sections = await page.locator("section[data-testid^='section-']").evaluateAll((nodes) => nodes.map((node) => node.dataset.testid));
    assert.deepEqual(sections, ["section-enabled"]);
    // Torrents still seeding after the switch went off are said to be let finish.
    assert.equal(await page.getByTestId("note-enabled").count(), presence === "offJobs" ? 1 : 0);
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  });
}

test("with the switch on, the Torrents panel shows every section", async () => {
  const { page, errors } = await open("panel", "onIdle");
  await page.getByTestId("section-connection").waitFor();
  await page.getByTestId("section-seeding").waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

// Each box left on lets that part finish; the choice saves at once, with no Save.
for (const [cleared, mode] of [[[], "FINISH_ALL"], [["seeding"], "FINISH_DOWNLOADS"], [["downloads"], "FINISH_SEEDING"]]) {
  test(`turning torrents off while some run asks how they end and saves ${mode} at once`, async () => {
    const { page, errors } = await open("panel", "onRunning");
    await page.getByTestId("section-connection").waitFor();
    await page.getByTestId("field-enabled").click();
    const dialog = page.getByTestId("dialog-torrents-off");
    await dialog.waitFor();
    for (const key of ["downloads", "seeding"]) {
      assert.equal(await page.getByTestId(`torrents-off-option-${key}`).getAttribute("aria-checked"), "true");
    }
    for (const key of cleared) {
      await page.getByTestId(`torrents-off-option-${key}`).click();
    }
    assert.equal((await sent(page, "UpdateTorrentSettings")).length, 0);
    await page.getByTestId("confirm-accept").click();
    await sentCount(page, "UpdateTorrentSettings", 1);
    assert.deepEqual(await sent(page, "UpdateTorrentSettings"), [
      { input: { torrent: { enabled: false, whenDisabled: mode } } },
    ]);
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  });
}

test("clearing both boxes asks again, naming the hit and run, before every torrent stops now", async () => {
  const { page, errors } = await open("panel", "onRunning");
  await page.getByTestId("section-connection").waitFor();
  await page.getByTestId("field-enabled").click();
  await page.getByTestId("dialog-torrents-off").waitFor();
  await page.getByTestId("torrents-off-option-downloads").click();
  await page.getByTestId("torrents-off-option-seeding").click();
  await page.getByTestId("confirm-accept").click();
  const force = page.getByTestId("dialog-torrents-off-force");
  await force
    .getByText("This will forcibly cancel all torrents and may result in a hit and run on private trackers.", { exact: true })
    .waitFor();
  assert.equal((await sent(page, "UpdateTorrentSettings")).length, 0);
  await force.getByTestId("confirm-accept").click();
  await sentCount(page, "UpdateTorrentSettings", 1);
  assert.deepEqual(await sent(page, "UpdateTorrentSettings"), [
    { input: { torrent: { enabled: false, whenDisabled: "STOP_NOW" } } },
  ]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("cancelling the question leaves torrents on and sends nothing", async () => {
  const { page, errors } = await open("panel", "onRunning");
  await page.getByTestId("section-connection").waitFor();
  await page.getByTestId("field-enabled").click();
  const dialog = page.getByTestId("dialog-torrents-off");
  await dialog.waitFor();
  await page.getByTestId("confirm-dismiss").click();
  await dialog.waitFor({ state: "detached" });
  await page.getByTestId("section-connection").waitFor();
  assert.equal(await page.getByTestId("field-enabled").getAttribute("aria-checked"), "true");
  assert.equal((await sent(page, "UpdateTorrentSettings")).length, 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a wind-down under way can be cut short: Stop now sends torrents off to stop at once", async () => {
  const { page, errors } = await open("panel", "offJobs");
  await page.getByTestId("note-enabled").waitFor();
  await page.getByTestId("field-stopNow").click();
  await sentCount(page, "UpdateTorrentSettings", 1);
  assert.deepEqual(await sent(page, "UpdateTorrentSettings"), [
    { input: { torrent: { enabled: false, whenDisabled: "STOP_NOW" } } },
  ]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

const FORCED_LINE = "A forced torrent, or one with no seeding limit, seeds until Stop now.";

test("the question says a forced or goal-less torrent seeds on while seeding is let finish", async () => {
  const { page, errors } = await open("panel", "onRunning");
  await page.getByTestId("section-connection").waitFor();
  await page.getByTestId("field-enabled").click();
  await page.getByTestId("torrents-off-option-seeding-note").getByText(FORCED_LINE, { exact: true }).waitFor();
  assert.equal(await page.getByTestId("torrents-off-option-downloads-note").getByText(FORCED_LINE).count(), 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

for (const [presence, line] of [["offJobs", FORCED_LINE], ["offFinishDownloads", "Every torrent stops at once."]]) {
  test(`a wind-down under way (${presence}) says beside Stop now what it leaves running`, async () => {
    const { page, errors } = await open("panel", presence);
    await page.getByTestId("field-stopNow").waitFor();
    await page.getByTestId("section-enabled").getByText(line, { exact: true }).waitFor();
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  });
}

test("with no wind-down under way there is no Stop now", async () => {
  const { page, errors } = await open("panel", "off");
  await page.getByTestId("field-enabled").waitFor();
  assert.equal(await page.getByTestId("field-stopNow").count(), 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the switch waits to be turned off until the server says which torrents run", async () => {
  const { page, errors } = await open("panel", "unknown");
  const toggle = page.getByTestId("field-enabled");
  await toggle.waitFor();
  assert.equal(await toggle.getAttribute("aria-checked"), "true");
  assert.equal(await toggle.isDisabled(), true);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("turning torrents off with none running asks once, with no boxes, and saves at once", async () => {
  const { page, errors } = await open("panel", "onIdle");
  await page.getByTestId("section-connection").waitFor();
  await page.getByTestId("field-enabled").click();
  await page.getByTestId("dialog-torrents-off").waitFor();
  assert.equal(await page.getByTestId("torrents-off-option-downloads").count(), 0);
  await page.getByTestId("confirm-accept").click();
  await sentCount(page, "UpdateTorrentSettings", 1);
  assert.deepEqual(await sent(page, "UpdateTorrentSettings"), [{ input: { torrent: { enabled: false } } }]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

for (const control of ["field-enableTorrents", "field-enabled"]) {
  test(`with torrents off, ${control} asks about the beta and turns them on at once`, async () => {
    const { page, errors } = await open("panel", "off");
    await page.getByTestId(control).click();
    const dialog = page.getByTestId("dialog-enable-torrents");
    await dialog.getByText("Torrents are in early beta, use at your own risk.", { exact: true }).waitFor();
    assert.equal((await sent(page, "UpdateTorrentSettings")).length, 0);
    await dialog.getByTestId("confirm-accept").click();
    await sentCount(page, "UpdateTorrentSettings", 1);
    assert.deepEqual(await sent(page, "UpdateTorrentSettings"), [{ input: { torrent: { enabled: true } } }]);
    assert.equal(errors.length, 0, errors.join("\n"));
    await page.close();
  });
}

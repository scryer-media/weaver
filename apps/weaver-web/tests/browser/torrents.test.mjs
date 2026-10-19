// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-torrents", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open(screen) {
  const page = await browser.newPage({ viewport: { width: 1200, height: 1000 } });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/torrents.html?screen=${screen}`);
  return { page, errors };
}
const sent = (page) => page.evaluate(() => structuredClone(window.torrentsFixture.sent));
const METAINFO = "d4:infod4:name7:fixturee";

test("only a torrent carries a protocol chip", async () => {
  const { page, errors } = await open("tag");
  await page.getByLabel("torrent row", { exact: true }).waitFor();
  assert.equal(await page.getByLabel("torrent row", { exact: true }).getByLabel("Torrent", { exact: true }).count(), 1);
  assert.equal(await page.getByLabel("nzb row", { exact: true }).innerText(), "nzb job");
  assert.equal(await page.getByLabel("older row", { exact: true }).innerText(), "older job");
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a wait reason that is a sentence is shown as the daemon wrote it", async () => {
  const { page, errors } = await open("detail");
  const items = page.getByRole("list", { name: "details", exact: true }).getByRole("listitem");
  await items.first().waitFor();
  assert.deepEqual(await items.allInnerTexts(), ["Not seeding: route down", "propagation delay", "some new code"]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the add dialog takes a torrent file and a magnet link, each sent through submitTorrent", async () => {
  const { page, errors } = await open("add");
  const dialog = page.getByRole("dialog", { name: "Add", exact: true });
  await dialog.waitFor();
  // Torrent options show only once there is a torrent to add.
  assert.equal(await dialog.getByRole("switch", { name: "Add paused", exact: true }).count(), 0);
  await page.locator("input[type=file]").setInputFiles({ name: "fixture.torrent", mimeType: "application/x-bittorrent", buffer: Buffer.from(METAINFO) });
  await dialog.getByText("fixture.torrent", { exact: true }).waitFor();
  assert.equal(await dialog.getByText("Only .nzb", { exact: false }).count(), 0);
  await dialog.getByRole("switch", { name: "Add paused", exact: true }).click();
  await dialog.getByRole("switch", { name: "Force", exact: true }).click();

  // Text that is not a magnet link is named as such, and holds the submit back.
  const magnet = dialog.getByRole("textbox", { name: "Magnet link", exact: true });
  await magnet.fill("https://example.invalid/file");
  await dialog.getByText("Not a magnet link", { exact: true }).waitFor();
  const submit = dialog.getByRole("button", { name: /^Add/ });
  assert.equal(await submit.isDisabled(), true);
  await magnet.fill("magnet:?xt=urn:btih:duplicate");
  assert.equal(await submit.isDisabled(), false);
  await submit.click();

  // The daemon's refusal is shown as it gave it, and the dialog stays open for it.
  await dialog.getByText("This torrent is already in the queue.", { exact: true }).waitFor();
  const submitted = (await sent(page)).filter((entry) => entry.name === "SubmitTorrent").map((entry) => entry.variables.input);
  assert.deepEqual(submitted, [
    { category: null, paused: true, forced: true, metainfoBase64: Buffer.from(METAINFO).toString("base64") },
    { category: null, paused: true, forced: true, magnet: "magnet:?xt=urn:btih:duplicate" },
  ]);
  assert.equal(await page.locator("#status").innerText(), "");

  // Sent again, the file already in is not sent twice, and the dialog closes once nothing is refused.
  await magnet.fill("magnet:?xt=urn:btih:fresh");
  await submit.click();
  await page.locator("#status").filter({ hasText: /^closed$/ }).waitFor();
  const again = (await sent(page)).filter((entry) => entry.name === "SubmitTorrent").slice(2).map((entry) => entry.variables.input);
  assert.deepEqual(again, [{ category: null, paused: true, forced: true, magnet: "magnet:?xt=urn:btih:fresh" }]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a file that is neither an NZB nor a torrent is refused", async () => {
  const { page, errors } = await open("add");
  const dialog = page.getByRole("dialog", { name: "Add", exact: true });
  await dialog.waitFor();
  await page.locator("input[type=file]").setInputFiles({ name: "notes.txt", mimeType: "text/plain", buffer: Buffer.from("x") });
  await dialog.getByText(/\.nzb/).first().waitFor();
  assert.equal(await dialog.getByText("notes.txt", { exact: true }).count(), 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("removing a torrent short of its seeding goal asks again, and only then confirms", async () => {
  const { page, errors } = await open("remove");
  const first = page.getByRole("dialog", { name: "Remove torrent", exact: true });
  await first.waitFor();
  await first.getByRole("checkbox", { name: "Also delete files", exact: true }).click();
  await first.getByRole("button", { name: "Remove", exact: true }).click();
  const second = page.getByRole("dialog", { name: "Seeding goal not met", exact: true });
  await second.getByText("Ratio 0.40 of 1.00 goal.", { exact: true }).waitFor();
  assert.deepEqual((await sent(page)).map((entry) => entry.variables), [{ id: 5, deleteFiles: true, confirmUnmetGoal: false }]);
  await second.getByRole("button", { name: "Remove anyway", exact: true }).click();
  await page.locator("#status").filter({ hasText: /^removed 5$/ }).waitFor();
  assert.deepEqual((await sent(page)).map((entry) => entry.variables), [
    { id: 5, deleteFiles: true, confirmUnmetGoal: false },
    { id: 5, deleteFiles: true, confirmUnmetGoal: true },
  ]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("dismissing the second confirm sends nothing more", async () => {
  const { page, errors } = await open("remove");
  const first = page.getByRole("dialog", { name: "Remove torrent", exact: true });
  await first.getByRole("button", { name: "Remove", exact: true }).click();
  const second = page.getByRole("dialog", { name: "Seeding goal not met", exact: true });
  await second.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.locator("#status").filter({ hasText: /^dismissed$/ }).waitFor();
  assert.deepEqual((await sent(page)).map((entry) => entry.variables), [{ id: 5, deleteFiles: false, confirmUnmetGoal: false }]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("each torrent state is labelled in the queue by the shared status renderer", async () => {
  const { page, errors } = await open("queue");
  const labels = { FETCHING_METADATA: "Fetching metadata", CHECKING_FILES: "Checking files", SEEDING: "Seeding", STALLED: "Stalled" };
  for (const [state, label] of Object.entries(labels)) {
    const row = page.getByLabel(state, { exact: true });
    await row.getByText(label, { exact: true }).waitFor();
    assert.equal(await row.getByText(label, { exact: true }).count(), 1);
  }
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a torrent's detail names where its seeding policy comes from, and its swarm and file progress", async () => {
  const { page, errors } = await open("seeding");
  const seeding = (source) => page.getByLabel(source, { exact: true }).getByRole("region", { name: "Seeding", exact: true });
  await seeding("TORRENT").waitFor();
  assert.equal(await seeding("TORRENT").getByText("Override", { exact: true }).count(), 1);
  assert.equal(await seeding("CATEGORY").getByText("Category movies", { exact: true }).count(), 1);
  assert.equal(await seeding("GLOBAL").getByText("Global", { exact: true }).count(), 1);
  // The effective policy is the daemon's: the override's goal, then the category's.
  assert.equal(await seeding("TORRENT").getByText("2.50", { exact: true }).count(), 1);
  assert.equal(await seeding("CATEGORY").getByText("1.50", { exact: true }).count(), 1);

  const swarm = page.getByLabel("GLOBAL", { exact: true }).getByRole("region", { name: "Torrent", exact: true });
  assert.equal(await swarm.getByText("3 / 6", { exact: true }).count(), 1);
  assert.equal(await swarm.getByText("9", { exact: true }).count(), 1);
  const files = page.getByLabel("GLOBAL", { exact: true }).getByRole("region", { name: "Files", exact: true });
  assert.equal(await files.getByText("25.0%", { exact: true }).count(), 1);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the override dialog starts from the override, and Use inherited clears only a set one", async () => {
  const { page, errors } = await open("seeding");
  const dialog = page.getByRole("dialog", { name: "Override seeding", exact: true });

  // Inherited: the dialog starts from the policy in effect, with nothing to clear.
  await page.getByLabel("GLOBAL", { exact: true }).getByRole("button", { name: "Override seeding", exact: true }).click();
  await dialog.waitFor();
  assert.equal(await dialog.getByRole("spinbutton", { name: "Ratio goal", exact: true }).inputValue(), "1");
  assert.equal(await dialog.getByRole("button", { name: "Use inherited", exact: true }).isDisabled(), true);
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  await dialog.waitFor({ state: "detached" });

  await page.getByLabel("TORRENT", { exact: true }).getByRole("button", { name: "Override seeding", exact: true }).click();
  await dialog.waitFor();
  assert.equal(await dialog.getByRole("spinbutton", { name: "Ratio goal", exact: true }).inputValue(), "2.5");
  await dialog.getByRole("button", { name: "Use inherited", exact: true }).click();
  await page.locator("#status").filter({ hasText: /^changed 1$/ }).waitFor();
  assert.deepEqual((await sent(page)).filter((entry) => entry.name === "SetTorrentSeeding").map((entry) => entry.variables), [
    { id: 1, seeding: null },
  ]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

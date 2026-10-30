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

test("a tracker row says who named the tracker", async () => {
  const { page, errors } = await open("trackers");
  const row = (url) => page.getByTitle(url, { exact: true });
  await row("http://a.test/announce").waitFor();
  assert.equal(await row("http://a.test/announce").getByText("Torrent", { exact: true }).count(), 1);
  assert.equal(await row("http://b.test/announce").getByText("Added", { exact: true }).count(), 1);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the add dialog sends a torrent file and a magnet link through submit, read for what they are", async () => {
  const { page, errors } = await open("add");
  const dialog = page.getByRole("dialog", { name: "Add", exact: true });
  await dialog.waitFor();
  // The name says nothing: the daemon reads the bytes, and a torrent is not staged.
  await page.locator("input[type=file]").setInputFiles({ name: "fixture.bin", mimeType: "application/octet-stream", buffer: Buffer.from(METAINFO) });
  await dialog.getByText("fixture.bin", { exact: true }).waitFor();
  await dialog.getByText("staged", { exact: true }).waitFor();
  await dialog.getByRole("switch", { name: "Add paused", exact: true }).click();

  const link = dialog.getByRole("textbox", { name: "Link", exact: true });
  await link.fill("magnet:?xt=urn:btih:duplicate");
  const submit = dialog.getByRole("button", { name: /^Add/ });
  await submit.click();

  // The daemon's refusal is shown as it gave it, and the dialog stays open for it.
  await dialog.getByText("This torrent is already in the queue.", { exact: true }).waitFor();
  const input = { upload: null, category: null, paused: true, password: null, unpack: null };
  const submitted = (await sent(page)).filter((entry) => entry.name === "Submit").map((entry) => entry.variables);
  assert.deepEqual(submitted, [
    { input: { ...input, text: "magnet:?xt=urn:btih:duplicate" } },
    { input: { ...input, text: null }, file: "fixture.bin" },
  ]);
  assert.equal(await page.locator("#status").innerText(), "");

  // Sent again, the file already in is not sent twice, and the dialog closes once nothing is refused.
  await link.fill("magnet:?xt=urn:btih:fresh");
  await submit.click();
  await page.locator("#status").filter({ hasText: /^closed$/ }).waitFor();
  const again = (await sent(page)).filter((entry) => entry.name === "Submit").slice(2).map((entry) => entry.variables);
  assert.deepEqual(again, [{ input: { ...input, text: "magnet:?xt=urn:btih:fresh" } }]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a file the daemon cannot read shows its refusal on its row", async () => {
  const { page, errors } = await open("add");
  const dialog = page.getByRole("dialog", { name: "Add", exact: true });
  await dialog.waitFor();
  await page.locator("input[type=file]").setInputFiles({ name: "notes.txt", mimeType: "text/plain", buffer: Buffer.from("x") });
  await dialog.getByText("notes.txt", { exact: true }).waitFor();
  await dialog.getByText("not an NZB", { exact: true }).waitFor();
  assert.equal(await dialog.getByRole("button", { name: /^Add/ }).isDisabled(), true);
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

test("each queue row names its type, and a wide list draws the bars as a column of the row", async () => {
  const { page, errors } = await open("queue");
  const older = page.getByLabel("older", { exact: true });
  await older.getByText("Usenet", { exact: true }).waitFor();
  assert.equal(await page.getByLabel("SEEDING", { exact: true }).getByText("Torrent", { exact: true }).count(), 1);
  const name = older.getByText("older job", { exact: true });
  const bar = older.getByRole("progressbar");
  const middle = (box) => box.y + box.height / 2;
  // The rows lay out by the list's width, not the window's.
  const listWidth = (width) => page.locator("ul").evaluate((list, value) => { list.style.width = value; }, width);
  // A 1300px list: the bars sit level with the name, between it and the status.
  await listWidth("1300px");
  const [wideName, wideBar] = [await name.boundingBox(), await bar.boundingBox()];
  assert.ok(Math.abs(middle(wideName) - middle(wideBar)) < wideName.height, "the bars share the name's line");
  assert.ok(wideBar.x > wideName.x + wideName.width, "the bars follow the name");
  // A name that does not fit beside the column is never cut: the bars go under it, and it wraps.
  const long = page.getByLabel("long", { exact: true });
  const [longName, longBar] = [long.getByText(/Segment23$/), long.getByRole("progressbar")];
  const longBox = await longName.boundingBox();
  assert.ok((await longBar.boundingBox()).y >= longBox.y + longBox.height, "the bars sit under a long name");
  assert.ok(longBox.height > 2 * wideName.height, "a long name wraps rather than being cut");
  assert.ok(await longName.evaluate((text) => text.scrollWidth <= text.clientWidth), "no part of the name is hidden");
  // A 1700px list: the column widens to the bars' full length.
  await listWidth("1700px");
  assert.ok((await bar.boundingBox()).width > wideBar.width, "an ultrawide list draws longer bars");
  // A 900px list: the bars go back under the name.
  await listWidth("900px");
  const [narrowName, narrowBar] = [await name.boundingBox(), await bar.boundingBox()];
  assert.ok(narrowBar.y >= narrowName.y + narrowName.height, "the bars sit under the name");
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a torrent's detail names where each seeding term comes from, and its swarm and file progress", async () => {
  const { page, errors } = await open("seeding");
  const seeding = (source) => page.getByLabel(source, { exact: true }).getByRole("region", { name: "Seeding", exact: true });
  await seeding("TORRENT").waitFor();
  // The ratio goal comes from the level each torrent names; its five other terms from the global settings.
  assert.equal(await seeding("TORRENT").getByText("Override", { exact: true }).count(), 1);
  assert.equal(await seeding("TORRENT").getByText("Global", { exact: true }).count(), 5);
  assert.equal(await seeding("CATEGORY").getByText("Category movies", { exact: true }).count(), 1);
  assert.equal(await seeding("CATEGORY").getByText("Global", { exact: true }).count(), 5);
  assert.equal(await seeding("GLOBAL").getByText("Global", { exact: true }).count(), 6);
  // The terms in force are the daemon's: the override's goal, then the category's.
  assert.equal(await seeding("TORRENT").getByText("2.50", { exact: true }).count(), 1);
  assert.equal(await seeding("CATEGORY").getByText("1.50", { exact: true }).count(), 1);

  const swarm = page.getByLabel("GLOBAL", { exact: true }).getByRole("region", { name: "Transfer", exact: true });
  assert.equal(await swarm.getByText("3 (12 total)", { exact: true }).count(), 1);
  assert.equal(await swarm.getByText("6 (20 total)", { exact: true }).count(), 1);
  assert.equal(await swarm.getByText("9 (50 max)", { exact: true }).count(), 1);
  const files = page.getByLabel("GLOBAL", { exact: true }).getByRole("region", { name: "Files", exact: true });
  assert.equal(await files.getByText("25.0%", { exact: true }).count(), 1);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the override dialog starts from the override, and Use inherited clears only a set one", async () => {
  const { page, errors } = await open("seeding");
  const dialog = page.getByRole("dialog", { name: "Override seeding", exact: true });

  // Inherited: every term of the dialog inherits, with nothing to clear.
  await page.getByLabel("GLOBAL", { exact: true }).getByRole("button", { name: "Override seeding", exact: true }).click();
  await dialog.waitFor();
  assert.equal(await dialog.getByRole("spinbutton", { name: "Ratio goal", exact: true }).count(), 0);
  const ratio = dialog.getByRole("radiogroup", { name: "Ratio goal", exact: true });
  assert.equal(await ratio.getByRole("radio", { name: "Inherit", exact: true }).getAttribute("aria-checked"), "true");
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

// What the page has sent by one mutation, once it has sent `count` of them.
async function sentAs(page, name, count) {
  await page.waitForFunction(
    ([wanted, total]) => window.torrentsFixture.sent.filter((entry) => entry.name === wanted).length >= total,
    [name, count],
  );
  return (await sent(page)).filter((entry) => entry.name === name).map((entry) => entry.variables);
}
const publish = (page) => page.evaluate(() => window.torrentsFixture.publish());

test("a file's priority set on its row is sent for that file alone, and shown once the torrent pushes it", async () => {
  const { page, errors } = await open("files");
  const files = page.getByRole("region", { name: "Files", exact: true });
  const level = files.getByRole("button", { name: "Priority of set/a.mkv", exact: true });
  await level.getByText("Normal", { exact: true }).waitFor();
  await level.click();
  await page.getByRole("menu", { name: "Priority of set/a.mkv", exact: true }).getByRole("menuitemradio", { name: "High", exact: true }).click();
  assert.deepEqual(await sentAs(page, "SetTorrentFilePriorities", 1), [{ id: 4, priorities: [{ index: 0, priority: "HIGH" }] }]);
  await publish(page);
  await level.getByText("High", { exact: true }).waitFor();
  // The other files keep their levels.
  await files.getByRole("button", { name: "Priority of set/b.nfo", exact: true }).getByText("Normal", { exact: true }).waitFor();
  await files.getByRole("button", { name: "Priority of set/c.srt", exact: true }).getByText("Ignored", { exact: true }).waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a priority set over the ticked files is one request naming every one of them", async () => {
  const { page, errors } = await open("files");
  const files = page.getByRole("region", { name: "Files", exact: true });
  await files.getByRole("checkbox", { name: "Select set/b.nfo", exact: true }).click();
  await files.getByRole("checkbox", { name: "Select set/c.srt", exact: true }).click();
  await files.getByText("2 selected", { exact: true }).waitFor();
  await files.getByRole("button", { name: "Set priority", exact: true }).click();
  await page.getByRole("menu", { name: "Set priority", exact: true }).getByRole("menuitemradio", { name: "Maximum", exact: true }).click();
  assert.deepEqual(await sentAs(page, "SetTorrentFilePriorities", 1), [
    { id: 4, priorities: [{ index: 1, priority: "MAXIMUM" }, { index: 2, priority: "MAXIMUM" }] },
  ]);
  await publish(page);
  await files.getByRole("button", { name: "Priority of set/b.nfo", exact: true }).getByText("Maximum", { exact: true }).waitFor();
  await files.getByRole("button", { name: "Priority of set/c.srt", exact: true }).getByText("Maximum", { exact: true }).waitFor();
  await files.getByRole("button", { name: "Priority of set/a.mkv", exact: true }).getByText("Normal", { exact: true }).waitFor();

  // Select every file ticks the whole torrent, and its change goes as one request too.
  await files.getByRole("checkbox", { name: "Select every file", exact: true }).click();
  await files.getByText("3 selected", { exact: true }).waitFor();
  await files.getByRole("button", { name: "Set priority", exact: true }).click();
  await page.getByRole("menu", { name: "Set priority", exact: true }).getByRole("menuitemradio", { name: "Ignored", exact: true }).click();
  assert.deepEqual((await sentAs(page, "SetTorrentFilePriorities", 2))[1], {
    id: 4,
    priorities: [0, 1, 2].map((index) => ({ index, priority: "IGNORED" })),
  });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the picking switches send their mutations and show what the torrent pushes back", async () => {
  const { page, errors } = await open("files");
  const files = page.getByRole("region", { name: "Files", exact: true });
  const switches = [
    ["Sequential download", "SetTorrentSequential", { id: 4, sequential: true }],
    ["First and last pieces first", "SetTorrentFirstLastPieceFirst", { id: 4, firstLastPieceFirst: true }],
  ];
  for (const [label, name, variables] of switches) {
    const toggle = files.getByRole("switch", { name: label, exact: true });
    await toggle.and(page.locator('[aria-checked="false"]')).waitFor();
    await toggle.click();
    assert.deepEqual(await sentAs(page, name, 1), [variables]);
    // Nothing is pushed yet, so the switch still shows the torrent's own state.
    assert.equal(await toggle.getAttribute("aria-checked"), "false");
    await publish(page);
    await toggle.and(page.locator('[aria-checked="true"]')).waitFor();
  }
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

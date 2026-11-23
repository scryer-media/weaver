// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { readFileSync } from "node:fs";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectGraphqlErrors } from "./support/graphql-errors";
import { byTestIdPrefix, saveSettings, tableRow } from "./support/settings-locators";
import { jobIds, testIds, torrentIds } from "./support/testids";

/**
 * Torrents as an operator drives them in the browser: the settings panel and
 * a category's torrent section, adding a magnet and a .torrent through the
 * Add dialog, the queue row, the job's torrent blocks, the seeding override,
 * the header controls, and both removes.
 *
 * The lane has generated the fixtures, handed every one of them to the
 * loopback seeder and turned torrents on before this file runs.
 */

test.describe.configure({ mode: "serial" });

type Fixture = { slug: string; name: string; infohash: string; tracker: string; private: boolean };

const fixturesRoot = "/work/fixtures/torrents";
const manifest = JSON.parse(readFileSync(`${fixturesRoot}/manifest.json`, "utf8")) as {
  torrents: Record<string, Fixture>;
};

function fixture(slug: string): Fixture {
  const found = manifest.torrents[slug];
  if (!found) throw new Error(`torrent fixture ${slug} is not in the manifest`);
  return found;
}

const magnetFixture = fixture("ui-magnet");
const fileFixture = fixture("ui-file");
const category = "e2e-torrent-category";

/**
 * The queue row of a download, by the name it shows: the queue shows a
 * release name with its dots read as spaces.
 */
function queueRow(page: Page, name: string): Locator {
  const shown = name.replaceAll(".", " ").replace(/\s+/g, " ").trim();
  return byTestIdPrefix(page, torrentIds.queueRowPrefix).filter({
    has: page.getByTestId(torrentIds.queueRowName).and(page.getByText(shown, { exact: true })),
  });
}

/** Type a number into the field whose test ID is `fieldId`, inside `scope`. */
async function setNumber(scope: Locator, fieldId: string, value: string) {
  const field = scope.getByTestId(fieldId);
  await field.fill(value);
  await field.press("Enter");
  await expect(field).toHaveValue(value);
}

/** Open a torrent's own screen from its queue row and the inspector's link. */
async function openJob(page: Page, torrent: Fixture) {
  await page.goto("/");
  await queueRow(page, torrent.name).click();
  await page.getByTestId(torrentIds.inspectorOpenDetail).click();
  await expect(page).toHaveURL(/\/jobs\/\d+$/);
  await expect(page.getByTestId(torrentIds.field("infoHashV1"))).toContainText(torrent.infohash);
}

test("Settings → Torrents saves the connection and the global seeding policy", async ({ cleanPage: page }) => {
  await page.goto("/settings/torrents");
  // The on/off switch has its own section at the top of the page.
  const torrents = page.getByTestId(testIds.section("enabled"));
  const connection = page.getByTestId(testIds.section("connection"));
  const bittorrent = page.getByTestId(testIds.section("bittorrent"));
  const seeding = page.getByTestId(testIds.section("seeding"));
  await expect(torrents.getByTestId(testIds.field("enabled"))).toHaveAttribute("aria-checked", "true");

  await setNumber(bittorrent, testIds.field("maxActiveDownloads"), "4");
  await setNumber(connection, testIds.field("maxPeersTotal"), "150");
  await seeding.getByTestId(testIds.field("seeding-seedTimeGoalMinutes")).click();
  // The goal starts in days, the unit its one-day fallback divides into: 2 days is 2880 minutes.
  await setNumber(seeding, testIds.fieldValue("seeding-seedTimeGoalMinutes"), "2");
  await saveSettings(page);

  await page.reload();
  const reloadedTorrents = page.getByTestId(testIds.section("enabled"));
  const reloadedConnection = page.getByTestId(testIds.section("connection"));
  const reloadedBittorrent = page.getByTestId(testIds.section("bittorrent"));
  const reloadedSeeding = page.getByTestId(testIds.section("seeding"));
  await expect(reloadedBittorrent.getByTestId(testIds.field("maxActiveDownloads"))).toHaveValue("4");
  await expect(reloadedConnection.getByTestId(testIds.field("maxPeersTotal"))).toHaveValue("150");
  await expect(reloadedTorrents.getByTestId(testIds.field("enabled"))).toHaveAttribute("aria-checked", "true");
  await expect(reloadedSeeding.getByTestId(testIds.field("seeding-seedTimeGoalMinutes"))).toHaveAttribute("aria-checked", "true");
  await expect(reloadedSeeding.getByTestId(testIds.fieldValue("seeding-seedTimeGoalMinutes"))).toHaveValue("2");
  await expect(reloadedSeeding.getByTestId(testIds.field("seeding-ratioGoal"))).toHaveAttribute("aria-checked", "false");
});

test("a category's Torrents section saves its own route and seeding policy", async ({ cleanPage: page }) => {
  await page.goto("/settings/categories");
  await page.getByTestId(testIds.tableAdd("categories")).click();
  const form = page.getByTestId(testIds.dialog("category"));
  await form.getByTestId(testIds.field("name")).fill(category);
  const ratioGoal = testIds.field("categorySeeding-ratioGoal");
  await form.getByTestId(testIds.field("torrentRoute")).click();
  // A category's goal is set on its own once its state is Set; nothing else turns its policy on.
  await form.getByTestId(testIds.option(ratioGoal, "SET")).click();
  await setNumber(form, testIds.fieldValue("categorySeeding-ratioGoal"), "2.5");
  await form.getByTestId(testIds.editorSave).click();
  await expect(form).toBeHidden();

  await page.reload();
  await tableRow(page, "categories", category).click();
  const editor = page.getByTestId(testIds.dialog("category"));
  await expect(editor.getByTestId(testIds.field("torrentRoute"))).toHaveAttribute("aria-checked", "true");
  await expect(editor.getByTestId(testIds.option(ratioGoal, "SET"))).toHaveAttribute("aria-checked", "true");
  await expect(editor.getByTestId(testIds.fieldValue("categorySeeding-ratioGoal"))).toHaveValue("2.5");

  await editor.getByTestId(testIds.editorDelete).click();
  const confirm = page.getByTestId(testIds.dialog("category-remove"));
  await confirm.getByTestId(testIds.confirmAccept).click();
  await expect(confirm).toBeHidden();
  await expect(tableRow(page, "categories", category)).toHaveCount(0);
});

test("the Add dialog takes a magnet and a .torrent, and the queue tags and seeds them", async ({ cleanPage: page }) => {
  await page.goto("/");
  await page.getByTestId(torrentIds.addOpen).click();
  const dialog = page.getByTestId(testIds.dialog("add"));
  await dialog.getByTestId(torrentIds.addLink).fill(readFileSync(`${fixturesRoot}/ui-magnet.magnet`, "utf8").trim());
  const chooser = page.waitForEvent("filechooser");
  await dialog.getByTestId(torrentIds.addDrop).click();
  await (await chooser).setFiles(`${fixturesRoot}/ui-file.torrent`);
  await expect(dialog.getByTestId(torrentIds.addPaused)).toHaveAttribute("aria-checked", "false");
  const submit = dialog.getByTestId(torrentIds.addSubmit);
  // The button counts what it adds: the magnet and the file.
  await expect(submit).toContainText("2");
  await submit.click();
  await expect(dialog).toBeHidden();

  for (const name of [magnetFixture.name, fileFixture.name]) {
    const row = queueRow(page, name);
    await expect(row.getByTestId(torrentIds.queueRowProtocol("TORRENT"))).toBeVisible();
    // Seeding shows once the download is whole: the ratio and the upload rate.
    await expect(row.getByTestId(torrentIds.queueRowSeeding)).toHaveText(/^\d+\.\d{2} · ↑ /);
  }
});

test("a torrent job shows its swarm, trackers, files and policy, and takes a seeding override", async ({ cleanPage: page }) => {
  await openJob(page, fileFixture);
  await expect(page.getByTestId(torrentIds.field("infoHashV1"))).toContainText(fileFixture.infohash);
  // The swarm: seeds and peers each read as a count over the swarm's total, and the share ratio as a number.
  await expect(page.getByTestId(torrentIds.field("seeds"))).toContainText(/\d+ \((\d+|—) total\)/);
  await expect(page.getByTestId(torrentIds.field("peers"))).toContainText(/\d+ \((\d+|—) total\)/);
  await expect(page.getByTestId(torrentIds.field("shareRatio"))).toContainText(/\d+\.\d{2}/);

  const tracker = page.getByTestId(torrentIds.tracker(fileFixture.tracker));
  await expect(tracker).toBeVisible();
  await expect(tracker.getByTestId(torrentIds.trackerStatus("WORKING"))).toBeVisible();

  const filePaths = page.getByTestId(torrentIds.filePath);
  await expect(filePaths.filter({ hasText: /beryl\.bin$/ })).toBeVisible();
  await expect(filePaths.filter({ hasText: /notes\/beryl\.txt$/ })).toBeVisible();
  // The whole download arrived: the torrent reads complete, and so does each of its files.
  await expect(byTestIdPrefix(page, jobIds.jobProgressPrefix)).toHaveText(["100.0%"]);
  for (const name of [/beryl\.bin$/, /notes\/beryl\.txt$/]) {
    const file = page.getByTestId(torrentIds.file).filter({ has: filePaths.filter({ hasText: name }) });
    await expect(file.getByTestId(torrentIds.fileProgress)).toHaveText("100.0%");
  }

  await expect(page.getByTestId(torrentIds.seedingLevel("seedTimeGoalMinutes", "GLOBAL"))).toBeVisible();
  // 2880 minutes reads in the largest unit it divides into whole.
  await expect(page.getByTestId(torrentIds.seedingTerm("seedTimeGoalMinutes"))).toHaveText("2 d");

  await page.getByTestId(torrentIds.seedingOverride).click();
  const dialog = page.getByTestId(testIds.dialog("torrent-seeding"));
  await dialog.getByTestId(testIds.option(testIds.field("torrent-seeding-ratioGoal"), "SET")).click();
  await setNumber(dialog, testIds.fieldValue("torrent-seeding-ratioGoal"), "3");
  await dialog.getByTestId(testIds.editorSave).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByTestId(torrentIds.seedingLevel("ratioGoal", "TORRENT"))).toBeVisible();
  await expect(page.getByTestId(torrentIds.seedingTerm("ratioGoal"))).toHaveText("3.00");
});

test("a torrent job pauses, resumes, forces and unforces from its header", async ({ cleanPage: page }) => {
  await openJob(page, fileFixture);
  await page.getByTestId(torrentIds.jobPause).click();
  await expect(page.getByTestId(torrentIds.jobResume)).toBeVisible();
  await page.getByTestId(torrentIds.jobResume).click();
  await expect(page.getByTestId(torrentIds.jobPause)).toBeVisible();

  await page.getByTestId(torrentIds.jobForce).click();
  await expect(page.getByTestId(torrentIds.jobUnforce)).toBeVisible();
  await page.getByTestId(torrentIds.jobUnforce).click();
  await expect(page.getByTestId(torrentIds.jobForce)).toBeVisible();
});

test("Pause torrents on the Downloads page holds every torrent and leaves the queue's pause alone", async ({ cleanPage: page }) => {
  await page.goto("/");
  const rows = [queueRow(page, magnetFixture.name), queueRow(page, fileFixture.name)];
  for (const row of rows) await expect(row.getByTestId(torrentIds.queueRowStatus("SEEDING"))).toBeVisible();
  await expect(page.getByTestId(testIds.queuePauseAll)).toBeVisible();

  await page.getByTestId(torrentIds.pauseTorrents).click();
  await expect(page.getByTestId(torrentIds.resumeTorrents)).toBeVisible();
  for (const row of rows) {
    await expect(row.getByTestId(torrentIds.queueRowStatus("PAUSED"))).toBeVisible();
    await expect(row.getByTestId(torrentIds.queueRowResume)).toBeVisible();
  }
  // The torrents' hold is its own: the queue's pause still reads unpaused.
  await expect(page.getByTestId(testIds.queuePauseAll)).toBeVisible();
  await expect(page.getByTestId(testIds.queueResumeAll)).toHaveCount(0);

  await page.getByTestId(torrentIds.resumeTorrents).click();
  await expect(page.getByTestId(torrentIds.pauseTorrents)).toBeVisible();
  for (const row of rows) {
    await expect(row.getByTestId(torrentIds.queueRowStatus("SEEDING"))).toBeVisible();
    await expect(row.getByTestId(torrentIds.queueRowPause)).toBeVisible();
  }
  await expect(page.getByTestId(testIds.queuePauseAll)).toBeVisible();
});

test("removing a private torrent short of its goal asks about its files and then the goal", async ({ cleanPage: page }) => {
  // The daemon refuses the first remove with the goal it is short of; the dialog turns that into its second confirm.
  expectGraphqlErrors(page, /seeding goal is not met/);
  await openJob(page, fileFixture);
  await page.getByTestId(torrentIds.jobRemove).click();
  const first = page.getByTestId(testIds.dialog("torrent-remove"));
  await first.getByTestId(torrentIds.removeDeleteFiles).check();
  await first.getByTestId(testIds.confirmAccept).click();
  const second = page.getByTestId(testIds.dialog("torrent-unmet-goal"));
  await expect(second).toBeVisible();
  await second.getByTestId(testIds.confirmAccept).click();
  await expect(second).toBeHidden();
  await expect(page).toHaveURL(/\/$/);
  await expect(queueRow(page, fileFixture.name)).toHaveCount(0);
  await expect(queueRow(page, magnetFixture.name)).toBeVisible();
});

test("removing a public torrent from the queue inspector takes one confirm", async ({ cleanPage: page }) => {
  await page.goto("/");
  await queueRow(page, magnetFixture.name).click();
  await page.getByTestId(torrentIds.inspectorRemove).click();
  const confirm = page.getByTestId(testIds.dialog("torrent-remove"));
  await expect(confirm.getByTestId(torrentIds.removeDeleteFiles)).not.toBeChecked();
  await confirm.getByTestId(testIds.confirmAccept).click();
  await expect(confirm).toBeHidden();
  await expect(page.getByTestId(testIds.dialog("torrent-unmet-goal"))).toHaveCount(0);
  await expect(queueRow(page, magnetFixture.name)).toHaveCount(0);
});

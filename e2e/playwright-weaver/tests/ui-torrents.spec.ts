// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { readFileSync } from "node:fs";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectGraphqlErrors } from "./support/graphql-errors";

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

/** A settings table row, found by the exact text of one of its cells. */
function tableRow(page: Page, table: string, cellText: string): Locator {
  return page
    .getByRole("region", { name: table, exact: true })
    .getByRole("button")
    .filter({ has: page.getByText(cellText, { exact: true }) });
}

/**
 * The queue row of a download, by the name the row carries as its title: the
 * queue shows a release name with its dots read as spaces.
 */
function queueRow(page: Page, name: string): Locator {
  const shown = name.replaceAll(".", " ").replace(/\s+/g, " ").trim();
  return page.getByRole("main").getByTitle(shown, { exact: true });
}

async function setNumber(scope: Locator, label: string, value: string) {
  const field = scope.getByRole("spinbutton", { name: label, exact: true });
  await field.fill(value);
  await field.press("Enter");
  await expect(field).toHaveValue(value);
}

async function saveSettings(page: Page) {
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();
}

/** Open a torrent's own screen from its queue row and the inspector's link. */
async function openJob(page: Page, torrent: Fixture) {
  await page.goto("/");
  await queueRow(page, torrent.name).click();
  await page.getByRole("link", { name: "Open job detail" }).click();
  await expect(page).toHaveURL(/\/jobs\/\d+$/);
  await expect(page.locator("#torrent").getByText(torrent.infohash, { exact: true })).toBeVisible();
}

test("Settings → Torrents saves the connection and the global seeding policy", async ({ cleanPage: page }) => {
  await page.goto("/settings/torrents");
  const connection = page.getByRole("region", { name: "Connection", exact: true });
  const seeding = page.getByRole("region", { name: "Seeding", exact: true });
  await expect(connection.getByRole("switch", { name: "Enabled", exact: true })).toHaveAttribute("aria-checked", "true");

  await setNumber(connection, "Max active downloads", "4");
  await setNumber(connection, "Max peers", "150");
  await seeding.getByRole("switch", { name: "Seed time goal", exact: true }).click();
  await setNumber(seeding, "Seed time goal", "2880");
  await saveSettings(page);

  await page.reload();
  const reloadedConnection = page.getByRole("region", { name: "Connection", exact: true });
  const reloadedSeeding = page.getByRole("region", { name: "Seeding", exact: true });
  await expect(reloadedConnection.getByRole("spinbutton", { name: "Max active downloads", exact: true })).toHaveValue("4");
  await expect(reloadedConnection.getByRole("spinbutton", { name: "Max peers", exact: true })).toHaveValue("150");
  await expect(reloadedConnection.getByRole("switch", { name: "Enabled", exact: true })).toHaveAttribute("aria-checked", "true");
  await expect(reloadedSeeding.getByRole("switch", { name: "Seed time goal", exact: true })).toHaveAttribute("aria-checked", "true");
  await expect(reloadedSeeding.getByRole("spinbutton", { name: "Seed time goal", exact: true })).toHaveValue("2880");
  await expect(reloadedSeeding.getByRole("switch", { name: "Ratio goal", exact: true })).toHaveAttribute("aria-checked", "false");
});

test("a category's Torrents section saves its own route and seeding policy", async ({ cleanPage: page }) => {
  await page.goto("/settings/categories");
  await page.getByRole("main").getByRole("button", { name: "Add category", exact: true }).click();
  const form = page.getByRole("dialog", { name: "Add category", exact: true });
  await form.getByRole("textbox", { name: "Name", exact: true }).fill(category);
  // The editor's Torrents section: its fields are the only ones by these names in the dialog.
  const torrents = form;
  await torrents.getByRole("switch", { name: "Own torrent route", exact: true }).click();
  await torrents.getByRole("switch", { name: "Own seeding policy", exact: true }).click();
  await torrents.getByRole("switch", { name: "Ratio goal", exact: true }).click();
  await setNumber(torrents, "Ratio goal", "2.5");
  await form.getByRole("button", { name: "Save", exact: true }).click();
  await expect(form).toBeHidden();

  await page.reload();
  await tableRow(page, "Categories", category).click();
  const editor = page.getByRole("dialog", { name: category, exact: true });
  const saved = editor;
  await expect(saved.getByRole("switch", { name: "Own torrent route", exact: true })).toHaveAttribute("aria-checked", "true");
  await expect(saved.getByRole("switch", { name: "Own seeding policy", exact: true })).toHaveAttribute("aria-checked", "true");
  await expect(saved.getByRole("spinbutton", { name: "Ratio goal", exact: true })).toHaveValue("2.5");

  await editor.getByRole("button", { name: "Remove category", exact: true }).click();
  const confirm = page.getByRole("dialog", { name: "Remove category", exact: true });
  await confirm.getByRole("button", { name: "Remove category", exact: true }).click();
  await expect(confirm).toBeHidden();
  await expect(tableRow(page, "Categories", category)).toHaveCount(0);
});

test("the Add dialog takes a magnet and a .torrent, and the queue tags and seeds them", async ({ cleanPage: page }) => {
  await page.goto("/");
  await page.getByRole("banner").getByRole("button", { name: "Add", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Add", exact: true });
  await dialog
    .getByRole("textbox", { name: "Magnet link", exact: true })
    .fill(readFileSync(`${fixturesRoot}/ui-magnet.magnet`, "utf8").trim());
  const chooser = page.waitForEvent("filechooser");
  await dialog.getByRole("button", { name: /Drop NZB or torrent files here, or click to choose/ }).click();
  await (await chooser).setFiles(`${fixturesRoot}/ui-file.torrent`);
  await expect(dialog.getByRole("switch", { name: "Add paused", exact: true })).toHaveAttribute("aria-checked", "false");
  await expect(dialog.getByRole("switch", { name: "Force", exact: true })).toHaveAttribute("aria-checked", "false");
  await dialog.getByRole("button", { name: "Add 2 downloads", exact: true }).click();
  await expect(dialog).toBeHidden();

  for (const name of [magnetFixture.name, fileFixture.name]) {
    const row = queueRow(page, name);
    await expect(row.getByTitle("Torrent", { exact: true })).toHaveText("BT");
    // Seeding shows once the download is whole: the ratio and the upload rate.
    await expect(row.getByTitle("Ratio · upload", { exact: true })).toHaveText(/^\d+\.\d{2} · ↑ /);
  }
});

test("a torrent job shows its swarm, trackers, files and policy, and takes a seeding override", async ({ cleanPage: page }) => {
  await openJob(page, fileFixture);
  const swarm = page.locator("#torrent");
  await expect(swarm.getByText(fileFixture.infohash, { exact: true })).toBeVisible();
  await expect(swarm.getByText("100.0%", { exact: true })).toBeVisible();
  await expect(swarm.getByText("Seeds / leechers", { exact: true })).toBeVisible();
  await expect(swarm.getByText(/^\d+ \/ \d+$/)).toBeVisible();
  await expect(swarm.getByText("Ratio", { exact: true })).toBeVisible();

  const trackers = page.locator("#trackers");
  await expect(trackers.getByText(fileFixture.tracker, { exact: true })).toBeVisible();
  await expect(trackers.getByText("Working", { exact: true })).toBeVisible();

  const files = page.locator("#torrent-files");
  await expect(files.getByText(/beryl\.bin$/)).toBeVisible();
  await expect(files.getByText(/notes\/beryl\.txt$/)).toBeVisible();

  const seeding = page.locator("#seeding");
  await expect(seeding.getByText("Global", { exact: true })).toBeVisible();
  await expect(seeding.getByText("2880 min", { exact: true })).toBeVisible();

  await seeding.getByRole("button", { name: "Override seeding", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Override seeding", exact: true });
  await dialog.getByRole("switch", { name: "Ratio goal", exact: true }).click();
  await setNumber(dialog, "Ratio goal", "3");
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(seeding.getByText("Override", { exact: true })).toBeVisible();
  await expect(seeding.getByText("3.00", { exact: true })).toBeVisible();
});

test("a torrent job pauses, resumes, forces and unforces from its header", async ({ cleanPage: page }) => {
  await openJob(page, fileFixture);
  const header = page.getByRole("banner");
  await header.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(header.getByRole("button", { name: "Resume", exact: true })).toBeVisible();
  await header.getByRole("button", { name: "Resume", exact: true }).click();
  await expect(header.getByRole("button", { name: "Pause", exact: true })).toBeVisible();

  await header.getByRole("button", { name: "Force", exact: true }).click();
  await expect(header.getByRole("button", { name: "Unforce", exact: true })).toBeVisible();
  await header.getByRole("button", { name: "Unforce", exact: true }).click();
  await expect(header.getByRole("button", { name: "Force", exact: true })).toBeVisible();
});

test("Pause torrents on the Downloads page holds every torrent and leaves the queue's pause alone", async ({ cleanPage: page }) => {
  await page.goto("/");
  const header = page.getByRole("banner");
  const rows = [queueRow(page, magnetFixture.name), queueRow(page, fileFixture.name)];
  for (const row of rows) await expect(row.getByText("Seeding", { exact: true })).toBeVisible();
  await expect(header.getByRole("button", { name: "Pause all", exact: true })).toBeVisible();

  await header.getByRole("button", { name: "Pause torrents", exact: true }).click();
  await expect(header.getByRole("button", { name: "Resume torrents", exact: true })).toBeVisible();
  for (const row of rows) {
    await expect(row.getByText("Paused", { exact: true })).toBeVisible();
    await expect(row.getByRole("button", { name: "Resume", exact: true })).toBeVisible();
  }
  // The torrents' hold is its own: the queue's pause still reads unpaused.
  await expect(header.getByRole("button", { name: "Pause all", exact: true })).toBeVisible();
  await expect(header.getByRole("button", { name: "Resume all", exact: true })).toHaveCount(0);

  await header.getByRole("button", { name: "Resume torrents", exact: true }).click();
  await expect(header.getByRole("button", { name: "Pause torrents", exact: true })).toBeVisible();
  for (const row of rows) {
    await expect(row.getByText("Seeding", { exact: true })).toBeVisible();
    await expect(row.getByRole("button", { name: "Pause", exact: true })).toBeVisible();
  }
  await expect(header.getByRole("button", { name: "Pause all", exact: true })).toBeVisible();
});

test("removing a private torrent short of its goal asks about its files and then the goal", async ({ cleanPage: page }) => {
  // The daemon refuses the first remove with the goal it is short of; the dialog turns that into its second confirm.
  expectGraphqlErrors(page, /seeding goal is not met/);
  await openJob(page, fileFixture);
  await page.getByRole("banner").getByRole("button", { name: "Remove", exact: true }).click();
  const first = page.getByRole("dialog", { name: "Remove torrent", exact: true });
  await first.getByRole("checkbox", { name: "Also delete files", exact: true }).check();
  await first.getByRole("button", { name: "Remove", exact: true }).click();
  const second = page.getByRole("dialog", { name: "Seeding goal not met", exact: true });
  await expect(second).toBeVisible();
  await second.getByRole("button", { name: "Remove anyway", exact: true }).click();
  await expect(second).toBeHidden();
  await expect(page).toHaveURL(/\/$/);
  await expect(queueRow(page, fileFixture.name)).toHaveCount(0);
  await expect(queueRow(page, magnetFixture.name)).toBeVisible();
});

test("removing a public torrent from the queue inspector takes one confirm", async ({ cleanPage: page }) => {
  await page.goto("/");
  await queueRow(page, magnetFixture.name).click();
  await page.getByRole("button", { name: "Remove download", exact: true }).click();
  const confirm = page.getByRole("dialog", { name: "Remove torrent", exact: true });
  await expect(confirm.getByRole("checkbox", { name: "Also delete files", exact: true })).not.toBeChecked();
  await confirm.getByRole("button", { name: "Remove", exact: true }).click();
  await expect(confirm).toBeHidden();
  await expect(page.getByRole("dialog", { name: "Seeding goal not met", exact: true })).toHaveCount(0);
  await expect(queueRow(page, magnetFixture.name)).toHaveCount(0);
});

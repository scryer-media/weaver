// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import fs from "node:fs";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { operationResponse } from "./support/graphql-operations";
import { confirmDialog, saveSettings, tableRow } from "./support/settings-locators";
import { settingsIds, testIds } from "./support/testids";

const field = testIds.field;

async function ensureDownloadsPaused(page: Page): Promise<void> {
  const pauseAll = page.getByTestId(testIds.queuePauseAll);
  const resumeAll = page.getByTestId(testIds.queueResumeAll);
  const toggle = pauseAll.or(resumeAll);
  await expect(toggle).toBeVisible();

  // The control follows the live queue state, so wait for it to stop changing
  // before deciding which way to flip it.
  const shownState = async () => (await pauseAll.count()) > 0 ? "pause" : (await resumeAll.count()) > 0 ? "resume" : "";
  let previousState = "";
  let stableReads = 0;
  await expect
    .poll(async () => {
      const state = await shownState();
      stableReads = state === previousState ? stableReads + 1 : 1;
      previousState = state;
      return stableReads >= 3 ? state : "";
    }, { timeout: 5_000, intervals: [100, 100, 250] })
    .toMatch(/^(pause|resume)$/);

  if (previousState === "pause") {
    await pauseAll.click();
  }
  await expect(resumeAll).toBeVisible();
}

async function addFeed(page: Page, name: string, url: string): Promise<Locator> {
  await page.getByTestId(testIds.tableAdd("feeds")).click();
  const form = page.getByTestId(testIds.dialog("feed"));
  await form.getByTestId(field("name")).fill(name);
  await form.getByTestId(field("url")).fill(url);
  await form.getByTestId(testIds.editorSave).click();
  await expect(form).toBeHidden();
  const row = tableRow(page, "feeds", name);
  await expect(row).toHaveCount(1);
  return row;
}

async function pollFeed(page: Page, row: Locator) {
  const sync = operationResponse(page, "RunRssSync");
  await row.getByTestId(settingsIds.rssFeedPoll).click();
  await sync;
}

async function removeFeed(page: Page, name: string) {
  await tableRow(page, "feeds", name).click();
  const editor = page.getByTestId(testIds.dialog("feed"));
  await expect(editor).toContainText(name);
  await editor.getByTestId(testIds.editorDelete).click();
  await confirmDialog(page, "feed-remove");
  await expect(tableRow(page, "feeds", name)).toHaveCount(0);
}

test("watch-folder injection reports queued/error marker renames without pipeline assertions", async ({ cleanPage: page }) => {
  const inbox = "/watch-folder";
  const validSource = `${inbox}/e2e-watch-valid.nzb`;
  const invalidSource = `${inbox}/e2e-watch-invalid.nzb`;
  fs.mkdirSync(inbox, { recursive: true });
  for (const candidate of [
    validSource,
    `${validSource}.queued`,
    `${validSource}.error`,
    invalidSource,
    `${invalidSource}.queued`,
    `${invalidSource}.error`,
  ]) {
    fs.rmSync(candidate, { force: true });
  }
  await page.goto("/");
  await ensureDownloadsPaused(page);

  await page.goto("/settings/watch-folder");
  const section = page.getByTestId(testIds.section("folder"));
  await section.getByTestId(testIds.browse(field("path"))).click();
  const directoryDialog = page.getByTestId(testIds.dialog("picker"));
  // The dialog opens on a listing of its own; typing before it lands would be
  // overwritten by it.
  const useFolder = directoryDialog.getByTestId(testIds.pickerUse);
  await expect(useFolder).toBeEnabled();
  await directoryDialog.getByTestId(testIds.pickerPathInput).fill(inbox);
  await directoryDialog.getByTestId(testIds.pickerGo).click();
  // The dialog chooses the folder it is showing, so let the browse land first.
  await expect(directoryDialog.getByTestId(testIds.pickerPath)).toHaveAttribute("title", inbox);
  await useFolder.click();
  await expect(directoryDialog).toBeHidden();
  const pathInput = section.getByTestId(field("path"));
  await expect(pathInput).toHaveValue(inbox);
  await section.getByTestId(testIds.option(field("mode"), "polling")).click();
  const pollInterval = section.getByTestId(field("pollIntervalSecs"));
  await pollInterval.fill("45");
  await pollInterval.press("Tab");
  await saveSettings(page);

  const scanNow = page.getByTestId(settingsIds.watchScanNow);
  const scanReport = page.getByTestId(testIds.section("report"));
  const scanFound = scanReport.getByTestId(settingsIds.watchFound);
  const scanQueued = scanReport.getByTestId(settingsIds.watchQueued);
  // Saving polling mode starts the poller, and its first pass runs at once.
  // Scan the still-empty inbox so the files below are left for the scan the
  // test drives; the poller's next pass is a full interval away.
  const emptyScan = operationResponse(page, "ScanWatchFolder");
  await scanNow.click();
  await emptyScan;
  await expect(scanQueued).toHaveText("0");
  await expect(scanFound).toHaveText("0");

  fs.writeFileSync(
    validSource,
    `<?xml version="1.0" encoding="UTF-8"?>
      <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
        <file poster="weaver-e2e" date="1700000000" subject="watch-folder-behavior.bin">
          <groups><group>alt.binaries.test</group></groups>
          <segments><segment bytes="1" number="1">watch-folder-behavior@e2e.invalid</segment></segments>
        </file>
      </nzb>`,
  );
  fs.writeFileSync(invalidSource, "this is intentionally not an NZB");

  const firstScan = operationResponse(page, "ScanWatchFolder");
  await expect(scanNow).toBeEnabled();
  await scanNow.click();
  await firstScan;
  await expect(scanQueued).toHaveText("1");
  await expect(scanFound).toHaveText("2");
  await expect(scanReport.getByTestId(settingsIds.watchProblem("error"))).toContainText("e2e-watch-invalid.nzb");
  await expect.poll(() => fs.existsSync(`${validSource}.queued`)).toBeTruthy();
  await expect.poll(() => fs.existsSync(`${invalidSource}.error`)).toBeTruthy();

  const secondScan = operationResponse(page, "ScanWatchFolder");
  await expect(scanNow).toBeEnabled();
  await scanNow.click();
  await secondScan;
  await expect(scanQueued).toHaveText("0");
  expect(fs.readdirSync(inbox).filter((name) => name.startsWith("e2e-watch-valid"))).toEqual([
    "e2e-watch-valid.nzb.queued",
  ]);
  expect(fs.readdirSync(inbox).filter((name) => name.startsWith("e2e-watch-invalid"))).toEqual([
    "e2e-watch-invalid.nzb.error",
  ]);

  await page.reload();
  await expect(pathInput).toHaveValue(inbox);
  await expect(pollInterval).toHaveValue("45");
});

test("RSS payload ingestion, seen deduplication, and seen controls are visible behavior", async ({ cleanPage: page }) => {
  const feedName = "E2E Controlled Feed";
  const releaseTitle = "Weaver E2E RSS Behavior Probe";

  await page.goto("/");
  await ensureDownloadsPaused(page);

  await page.goto("/settings/rss");
  const feedRow = await addFeed(page, feedName, "http://rss-fixture:8089/feed.xml");

  await page.getByTestId(testIds.tableAdd("rules")).click();
  const ruleForm = page.getByTestId(testIds.dialog("rss-rule"));
  await expect(ruleForm.getByTestId(field("feedId"))).toHaveText(feedName);
  await ruleForm.getByTestId(field("titleRegex")).fill(releaseTitle);
  await ruleForm.getByTestId(testIds.editorSave).click();
  await expect(ruleForm).toBeHidden();
  await expect(tableRow(page, "rules", releaseTitle).getByTestId(settingsIds.rssRuleAction("ACCEPT"))).toBeVisible();

  // The report line's counts are what the poll did.
  const status = page.getByTestId(testIds.shellStatus);
  const seenEntry = tableRow(page, "seen", releaseTitle);

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 feed polled · 1 new · 1 queued");
  await expect(seenEntry).toBeVisible();

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 feed polled · 0 new · 0 queued");
  await expect(seenEntry).toHaveCount(1);

  await seenEntry.click();
  await expect(page.getByTestId(testIds.dialog("rss-seen-forget"))).toContainText(releaseTitle);
  await confirmDialog(page, "rss-seen-forget");
  await expect(seenEntry).toHaveCount(0);

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 new");
  await expect(seenEntry).toBeVisible();

  await page.getByTestId(testIds.section("seen")).getByTestId(settingsIds.rssSeenClear).click();
  await confirmDialog(page, "rss-seen-clear");
  await expect(page.getByTestId(testIds.tableEmpty("seen"))).toBeVisible();

  await removeFeed(page, feedName);
  await expect(tableRow(page, "rules", releaseTitle)).toHaveCount(0);
});

test("RSS feed CRUD and classified sync errors are visible behavior", async ({ cleanPage: page }) => {
  const feedName = "E2E Unreachable Feed";
  const feedUrl = "http://127.0.0.1:1/feed.xml";
  await page.goto("/settings/rss");
  const feedRow = await addFeed(page, feedName, feedUrl);
  await expect(feedRow.getByText(feedUrl, { exact: true })).toBeVisible();
  // No assertion on "not polled yet": the daemon polls a feed as soon as it
  // is added, so the row may already carry the first failure.

  await pollFeed(page, feedRow);
  // The report line carries the first classified error after the counts,
  // named for its feed; the feed's row shows that same error.
  const report = page.getByTestId(testIds.shellStatus);
  await expect(report).toHaveText(/^1 feed polled · 0 new · 0 queued · \S/);
  const reportText = (await report.textContent()) ?? "";
  const prefix = `0 queued · ${feedName}: `;
  expect(reportText).toContain(prefix);
  const feedError = reportText.slice(reportText.indexOf(prefix) + prefix.length);
  expect(feedError).not.toBe("");
  // The row's state is that same error, so it is no longer "not polled yet".
  await expect(feedRow.getByTestId(settingsIds.rssFeedState)).toHaveText(feedError);

  await removeFeed(page, feedName);
});

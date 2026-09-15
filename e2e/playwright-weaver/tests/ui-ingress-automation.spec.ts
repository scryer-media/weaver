import fs from "node:fs";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";

async function ensureDownloadsPaused(page: Page): Promise<void> {
  const toggle = page.getByRole("banner").getByRole("button", { name: /^(Pause all|Resume all)$/ });
  await expect(toggle).toBeVisible();

  // The label follows the live queue state, so wait for it to stop changing
  // before deciding which way to flip it.
  let previousLabel = "";
  let stableReads = 0;
  await expect
    .poll(async () => {
      const label = (await toggle.textContent())?.trim() ?? "";
      stableReads = label === previousLabel ? stableReads + 1 : 1;
      previousLabel = label;
      return stableReads >= 3 ? label : "";
    }, { timeout: 5_000, intervals: [100, 100, 250] })
    .toMatch(/^(Pause all|Resume all)$/);

  if (previousLabel === "Pause all") {
    await page.getByRole("banner").getByRole("button", { name: "Pause all", exact: true }).click();
  }
  await expect(page.getByRole("banner").getByRole("button", { name: "Resume all", exact: true })).toBeVisible();
}

/** A settings table row, found by the exact text of one of its cells. */
function tableRow(page: Page, table: string, cellText: string): Locator {
  return page
    .getByRole("region", { name: table, exact: true })
    .getByRole("button")
    .filter({ has: page.getByText(cellText, { exact: true }) });
}

async function confirmDialog(page: Page, title: string, confirmLabel: string) {
  const confirm = page.getByRole("dialog", { name: title, exact: true });
  await confirm.getByRole("button", { name: confirmLabel, exact: true }).click();
  await expect(confirm).toBeHidden();
}

function operationResponse(page: Page, operation: string) {
  return page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname.endsWith("/graphql")
      && (response.request().postData() ?? "").includes(operation),
  );
}

async function addFeed(page: Page, name: string, url: string): Promise<Locator> {
  await page.getByRole("banner").getByRole("button", { name: "Add feed", exact: true }).click();
  const form = page.getByRole("dialog", { name: "Add feed", exact: true });
  await form.getByRole("textbox", { name: "Name", exact: true }).fill(name);
  await form.getByRole("textbox", { name: "URL", exact: true }).fill(url);
  await form.getByRole("button", { name: "Save", exact: true }).click();
  await expect(form).toBeHidden();
  const row = tableRow(page, "Feeds", name);
  await expect(row).toHaveCount(1);
  return row;
}

async function pollFeed(page: Page, row: Locator) {
  const sync = operationResponse(page, "RunRssSync");
  await row.getByRole("button", { name: "Poll", exact: true }).click();
  await sync;
}

async function removeFeed(page: Page, name: string) {
  await tableRow(page, "Feeds", name).click();
  await page
    .getByRole("dialog", { name, exact: true })
    .getByRole("button", { name: "Remove feed", exact: true })
    .click();
  await confirmDialog(page, "Remove feed", "Remove feed");
  await expect(tableRow(page, "Feeds", name)).toHaveCount(0);
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
  const section = page.getByRole("region", { name: "Watch folder", exact: true });
  await section.getByRole("button", { name: "Browse", exact: true }).click();
  const directoryDialog = page.getByRole("dialog", { name: "Folder", exact: true });
  // The dialog opens on a listing of its own; typing before it lands would be
  // overwritten by it.
  const useFolder = directoryDialog.getByRole("button", { name: "Use this folder", exact: true });
  await expect(useFolder).toBeEnabled();
  await directoryDialog.getByRole("textbox", { name: "Folder path", exact: true }).fill(inbox);
  await directoryDialog.getByRole("button", { name: "Go", exact: true }).click();
  // The dialog chooses the folder it is showing, so let the browse land first.
  await expect(directoryDialog.getByTitle(inbox, { exact: true })).not.toHaveCount(0);
  await useFolder.click();
  await expect(directoryDialog).toBeHidden();
  const pathInput = section.getByRole("textbox", { name: "Folder", exact: true });
  await expect(pathInput).toHaveValue(inbox);
  await section
    .getByRole("radiogroup", { name: "Watching", exact: true })
    .getByRole("radio", { name: "Polling", exact: true })
    .click();
  const pollInterval = section.getByRole("spinbutton", { name: "Poll interval", exact: true });
  await pollInterval.fill("45");
  await pollInterval.press("Tab");
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();

  const scanNow = page.getByRole("banner").getByRole("button", { name: "Scan now", exact: true });
  const scanReport = page.getByRole("region", { name: "Last scan", exact: true });
  // Saving polling mode starts the poller, and its first pass runs at once.
  // Scan the still-empty inbox so the files below are left for the scan the
  // test drives; the poller's next pass is a full interval away.
  const emptyScan = operationResponse(page, "ScanWatchFolder");
  await scanNow.click();
  await emptyScan;
  await expect(scanReport).toContainText("0 queued of 0 found");

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
  await expect(scanReport).toContainText("1 queued of 2 found");
  await expect(scanReport).toContainText("Files found");
  await expect(scanReport).toContainText("Queued");
  await expect(scanReport).toContainText("e2e-watch-invalid.nzb");
  await expect.poll(() => fs.existsSync(`${validSource}.queued`)).toBeTruthy();
  await expect.poll(() => fs.existsSync(`${invalidSource}.error`)).toBeTruthy();

  const secondScan = operationResponse(page, "ScanWatchFolder");
  await expect(scanNow).toBeEnabled();
  await scanNow.click();
  await secondScan;
  await expect(scanReport).toContainText(/^Last scan0 queued of/);
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

  await page
    .getByRole("region", { name: "Rules", exact: true })
    .getByRole("button", { name: "Add rule", exact: true })
    .click();
  const ruleForm = page.getByRole("dialog", { name: "Add rule", exact: true });
  await expect(ruleForm.getByRole("button", { name: "Feed", exact: true })).toHaveText(feedName);
  await ruleForm.getByRole("textbox", { name: "Title matches", exact: true }).fill(releaseTitle);
  await ruleForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(ruleForm).toBeHidden();
  await expect(tableRow(page, "Rules", releaseTitle).getByText("Accept", { exact: true })).toBeVisible();

  const status = page.getByRole("contentinfo");
  const seenEntry = tableRow(page, "Recently seen", releaseTitle);

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 feed polled · 1 new · 1 queued");
  await expect(seenEntry).toBeVisible();

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 feed polled · 0 new · 0 queued");
  await expect(seenEntry).toHaveCount(1);

  await seenEntry.click();
  await expect(page.getByRole("dialog", { name: "Forget this item", exact: true })).toContainText(releaseTitle);
  await confirmDialog(page, "Forget this item", "Forget item");
  await expect(seenEntry).toHaveCount(0);

  await pollFeed(page, feedRow);
  await expect(status).toContainText("1 new");
  await expect(seenEntry).toBeVisible();

  await page
    .getByRole("region", { name: "Recently seen", exact: true })
    .getByRole("button", { name: "Clear history", exact: true })
    .click();
  await confirmDialog(page, "Clear seen history", "Clear history");
  await expect(
    page.getByText("Nothing seen yet. Items appear here once a feed has been polled.", { exact: true }),
  ).toBeVisible();

  await removeFeed(page, feedName);
  await expect(tableRow(page, "Rules", releaseTitle)).toHaveCount(0);
});

test("RSS feed CRUD and classified sync errors are visible behavior", async ({ cleanPage: page }) => {
  const feedName = "E2E Unreachable Feed";
  const feedUrl = "http://127.0.0.1:1/feed.xml";
  await page.goto("/settings/rss");
  const feedRow = await addFeed(page, feedName, feedUrl);
  await expect(feedRow.getByText(feedUrl, { exact: true })).toBeVisible();
  await expect(feedRow.getByText("not polled yet", { exact: true })).toBeVisible();

  await pollFeed(page, feedRow);
  // The report line carries the first classified error after the counts.
  await expect(page.getByRole("contentinfo")).toContainText(/1 feed polled · 0 new · 0 queued · \S/);
  await expect(feedRow.getByText("not polled yet", { exact: true })).toHaveCount(0);

  await removeFeed(page, feedName);
});

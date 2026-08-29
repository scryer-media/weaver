import fs from "node:fs";
import type { Page } from "@playwright/test";
import { expect, test } from "./helpers";

async function ensureDownloadsPaused(page: Page): Promise<void> {
  const toggle = page.getByRole("button", { name: /^(Pause All|Resume All)$/ });
  await expect(toggle).toBeVisible();

  let previousLabel = "";
  let stableReads = 0;
  await expect
    .poll(async () => {
      const label = (await toggle.textContent())?.trim() ?? "";
      stableReads = label === previousLabel ? stableReads + 1 : 1;
      previousLabel = label;
      return stableReads >= 3 ? label : "";
    }, { timeout: 5_000, intervals: [100, 100, 250] })
    .toMatch(/^(Pause All|Resume All)$/);

  if (previousLabel === "Pause All") {
    await page.getByRole("button", { name: "Pause All", exact: true }).click();
  }
  await expect(page.getByRole("button", { name: "Resume All", exact: true })).toBeVisible();
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

  await page.goto("/");
  await ensureDownloadsPaused(page);

  await page.goto("/settings/watch-folder");
  await page.getByRole("combobox", { name: "Mode" }).click();
  await page.getByRole("option", { name: "Polling" }).click();
  const pathInput = page.getByLabel("Folder", { exact: true });
  await pathInput.click();
  const directoryDialog = page.getByRole("dialog", { name: "Browse Server Directories" });
  await directoryDialog
    .getByRole("textbox", { name: "Current directory path" })
    .fill(inbox);
  await directoryDialog.getByRole("button", { name: "Browse" }).click();
  await directoryDialog.getByRole("button", { name: "Use Current Folder" }).click();
  await page.getByLabel("Poll Interval (seconds)").fill("45");
  await page.getByRole("button", { name: "Save" }).click();
  await expect(page.getByText(/settings saved/i)).toBeVisible();

  await page.getByRole("button", { name: "Scan Now" }).click();
  await expect(page.getByText(/scan complete/i)).toBeVisible();
  const scanReport = page.getByRole("region", { name: "Watch folder scan report" });
  await expect(scanReport).toContainText("Discovered");
  await expect(scanReport).toContainText("Issues");
  await expect(scanReport).toContainText("e2e-watch-valid.nzb");
  await expect(scanReport).toContainText("e2e-watch-invalid.nzb");
  await expect.poll(() => fs.existsSync(`${validSource}.queued`)).toBeTruthy();
  await expect.poll(() => fs.existsSync(`${invalidSource}.error`)).toBeTruthy();

  await page.getByRole("button", { name: "Scan Now" }).click();
  await expect(page.getByText(/scan complete/i)).toBeVisible();
  expect(fs.readdirSync(inbox).filter((name) => name.startsWith("e2e-watch-valid"))).toEqual([
    "e2e-watch-valid.nzb.queued",
  ]);
  expect(fs.readdirSync(inbox).filter((name) => name.startsWith("e2e-watch-invalid"))).toEqual([
    "e2e-watch-invalid.nzb.error",
  ]);

  await page.reload();
  await expect(pathInput).toHaveValue(inbox);
  await expect(page.getByLabel("Poll Interval (seconds)")).toHaveValue("45");
});

test("RSS payload ingestion, seen deduplication, and seen controls are visible behavior", async ({ cleanPage: page }) => {
  const feedName = "E2E Controlled Feed";
  const releaseTitle = "Weaver E2E RSS Behavior Probe";

  await page.goto("/");
  await ensureDownloadsPaused(page);

  await page.goto("/settings/rss");
  await page.getByTestId("rss-add-feed").click();
  const feedForm = page.getByRole("region", { name: "Add Feed", exact: true });
  await feedForm.getByTestId("rss-feed-name").fill(feedName);
  await feedForm
    .getByTestId("rss-feed-url")
    .fill("http://rss-fixture:8089/feed.xml");
  await feedForm.getByRole("button", { name: "Add Feed", exact: true }).click();

  const feedCard = page.getByRole("region", { name: new RegExp(feedName) });
  await expect(feedCard).toHaveCount(1);
  await feedCard.getByRole("button", { name: "Add Rule" }).click();
  const ruleForm = feedCard.getByRole("region", { name: "Add Rule", exact: true });
  await ruleForm.getByTestId("rss-rule-title-regex").fill(releaseTitle);
  await ruleForm.getByRole("button", { name: "Add Rule", exact: true }).click();
  await expect(feedCard.getByText("Accept", { exact: true })).toBeVisible();

  const firstSync = page.waitForResponse(
    (response) =>
      response.url().includes("/graphql")
      && /runRssSync/.test(response.request().postData() ?? ""),
  );
  await feedCard.getByRole("button", { name: "Run Feed" }).click();
  await firstSync;
  await expect(page.getByText("RSS sync completed.")).toBeVisible();
  const report = page.getByRole("region", { name: "Sync Report", exact: true });
  await expect(report.getByTestId("rss-sync-items-submitted")).toContainText("1");
  // The release title also appears as the rule's Title Regex value, so scope
  // seen-history assertions to the per-item group the product labels by title.
  const seenEntry = feedCard.getByRole("group", { name: releaseTitle, exact: true });
  await expect(seenEntry).toBeVisible();

  const secondSync = page.waitForResponse(
    (response) =>
      response.url().includes("/graphql")
      && /runRssSync/.test(response.request().postData() ?? ""),
  );
  await feedCard.getByRole("button", { name: "Run Feed" }).click();
  await secondSync;
  await expect(report.getByTestId("rss-sync-items-new")).toContainText("0");
  await expect(seenEntry).toHaveCount(1);

  await seenEntry.getByRole("button", { name: "Forget" }).click();
  await expect(page.getByText("Seen item removed.")).toBeVisible();
  await expect(seenEntry).toHaveCount(0);

  const thirdSync = page.waitForResponse(
    (response) =>
      response.url().includes("/graphql")
      && /runRssSync/.test(response.request().postData() ?? ""),
  );
  await feedCard.getByRole("button", { name: "Run Feed" }).click();
  await thirdSync;
  await expect(seenEntry).toBeVisible();
  await feedCard.getByRole("button", { name: "Clear Seen" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Clear Seen History" }).click();
  await expect(feedCard.getByText("No seen items yet.", { exact: true })).toBeVisible();

  // The feed card exposes two buttons with the exact accessible name "Delete":
  // the feed-level delete in the header action bar and a rule-level delete
  // inside each RuleCard. Target the feed delete by its product-owned test id
  // (rule deletes carry no such id), keeping the selector-quality audit happy.
  await feedCard.getByTestId("rss-delete-feed").click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete Feed" }).click();
  await expect(feedCard).toHaveCount(0);
});

test("RSS feed CRUD and classified sync errors are visible behavior", async ({ cleanPage: page }) => {
  const feedName = "E2E Unreachable Feed";
  await page.goto("/settings/rss");
  await page.getByTestId("rss-add-feed").click();
  const feedForm = page.getByRole("region", { name: "Add Feed", exact: true });
  await feedForm.getByTestId("rss-feed-name").fill(feedName);
  await feedForm
    .getByTestId("rss-feed-url")
    .fill("http://127.0.0.1:1/feed.xml");
  await feedForm.getByRole("button", { name: "Add Feed", exact: true }).click();

  const feedCard = page.getByRole("region", { name: new RegExp(feedName) });
  await expect(feedCard).toHaveCount(1);
  await expect(feedCard).toContainText("http://127.0.0.1:1/feed.xml");
  await feedCard.getByRole("button", { name: "Run Feed" }).click();
  await expect(page.getByText("RSS sync completed.")).toBeVisible();
  const report = page.getByRole("region", { name: "Sync Report", exact: true });
  const feedResult = report.getByRole("group", { name: feedName, exact: true });
  await expect(feedResult.getByRole("alert")).not.toBeEmpty();

  await feedCard.getByRole("button", { name: "Delete" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete Feed" }).click();
  await expect(feedCard).toHaveCount(0);
});

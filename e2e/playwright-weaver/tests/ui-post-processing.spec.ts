import type { APIRequestContext, Page } from "@playwright/test";

import { expect, graphql, postProbeArticle, submitProbeNzb, test } from "./helpers";
import {
  POST_PROCESSING_BROKEN_PACKAGE,
  POST_PROCESSING_FAILING_SCRIPT,
  POST_PROCESSING_MARKER,
  POST_PROCESSING_NOTIFY_SCRIPT,
  POST_PROCESSING_NZBGET_DISPLAY_NAME,
  POST_PROCESSING_NZBGET_PACKAGE,
  POST_PROCESSING_SECRET,
  postProcessingMarker,
  removePostProcessingScripts,
  seedPostProcessingScripts,
} from "./support/setup/post-processing-package";

type HistoryItem = { id: number; status: string; outputDir?: string | null };

/** Poll history until `jobId` is terminal, and return its row. */
async function waitForTerminalJob(
  request: APIRequestContext,
  jobId: number,
): Promise<HistoryItem> {
  let item: HistoryItem | null = null;
  await expect
    .poll(
      async () => {
        const data = await graphql<{ historyItem: HistoryItem | null }>(
          request,
          `query WeaverE2EPostProcessingHistory($id: Int!) {
            historyItem(id: $id) { id status outputDir }
          }`,
          { id: jobId },
        );
        item = data.historyItem;
        return item?.status ?? "PENDING";
      },
      { timeout: 120_000, intervals: [500, 1_000, 2_000] },
    )
    .toMatch(/COMPLETE|FAILED/);
  if (!item) throw new Error(`job ${jobId} never reached history`);
  return item;
}

/** Script results recorded on a job, whatever the UI is currently showing. */
async function scriptResults(
  request: APIRequestContext,
  jobId: number,
): Promise<Array<{ script: string; status: string; exitCode: number | null }>> {
  const data = await graphql<{
    postProcessingResults: Array<{ script: string; status: string; exitCode: number | null }>;
  }>(
    request,
    `query WeaverE2EPostProcessingResults($jobId: Int!) {
      postProcessingResults(jobId: $jobId) { script status exitCode }
    }`,
    { jobId },
  );
  return data.postProcessingResults;
}

/** Submit a one-article job that will run the configured scripts on completion. */
async function runJobThroughPostProcessing(
  request: APIRequestContext,
  name: string,
): Promise<HistoryItem> {
  const messageId = `${name}@post-processing.e2e.invalid`;
  await postProbeArticle(messageId, 1024);
  const submission = await submitProbeNzb(request, name, [{ messageId, bytes: 1024 }]);
  expect(submission.accepted, JSON.stringify(submission)).toBeTruthy();
  expect(submission.jobId).not.toBeNull();
  return waitForTerminalJob(request, submission.jobId!);
}

async function addScriptToGlobalList(page: Page, displayName: string): Promise<void> {
  await page.getByRole("button", { name: `Add ${displayName}`, exact: true }).click();
  await expect(page.getByText("Script list saved.")).toBeVisible();
}

test("post-processing settings, the live script list, and real script execution are browser-owned", async ({
  cleanPage: page,
  request,
}) => {
  removePostProcessingScripts();
  seedPostProcessingScripts();

  await page.goto("/settings/post-processing");

  // 1. The master switch is off until an operator turns it on.
  const executionToggle = page.getByRole("switch", { name: "Run post-processing scripts" });
  await expect(executionToggle).toBeVisible();
  if (!(await executionToggle.isChecked())) {
    await executionToggle.click();
    await expect(page.getByText("Post-processing settings saved.")).toBeVisible();
  }
  await page.locator("#pp-concurrency").fill("2");
  await page.locator("#pp-grace").fill("5");
  await page.getByRole("button", { name: "Save settings" }).click();
  await expect(page.getByText("Post-processing settings saved.")).toBeVisible();

  // 2. Scripts are listed live from the directory, with unreadable ones surfaced.
  const problems = page.getByRole("region", { name: "Script problems" });
  await expect(problems).toContainText(POST_PROCESSING_BROKEN_PACKAGE);

  // 3. Build the global list, in the order the scripts must run.
  await addScriptToGlobalList(page, POST_PROCESSING_NOTIFY_SCRIPT);
  await addScriptToGlobalList(page, POST_PROCESSING_FAILING_SCRIPT);
  await addScriptToGlobalList(page, POST_PROCESSING_NZBGET_DISPLAY_NAME);
  const list = page.getByRole("list", { name: "Script list" });
  await expect(list.getByRole("listitem")).toContainText([
    POST_PROCESSING_NOTIFY_SCRIPT,
    POST_PROCESSING_FAILING_SCRIPT,
    POST_PROCESSING_NZBGET_PACKAGE,
  ]);
  await expect(
    list.getByRole("listitem", { name: `Script ${POST_PROCESSING_NZBGET_PACKAGE}` }),
  ).toContainText("NZBGET");
  await expect(
    list.getByRole("listitem", { name: `Script ${POST_PROCESSING_NOTIFY_SCRIPT}` }),
  ).toContainText("SABNZBD");

  // 4. Manifest options, including a secret that must never come back in cleartext.
  await page.getByLabel("Script options").click();
  await page.getByRole("option", { name: POST_PROCESSING_NZBGET_DISPLAY_NAME }).click();
  const optionsGroup = page.getByRole("group", {
    name: `Options for ${POST_PROCESSING_NZBGET_DISPLAY_NAME}`,
  });
  await expect(optionsGroup.getByLabel("Label")).toHaveValue("default-label");
  await optionsGroup.getByLabel("Label").fill("e2e-label");
  await optionsGroup.getByLabel("Token").fill(POST_PROCESSING_SECRET);
  await page.getByRole("button", { name: "Save options" }).click();
  await expect(
    page.getByText(`Options for ${POST_PROCESSING_NZBGET_DISPLAY_NAME} saved.`),
  ).toBeVisible();

  await page.reload();
  await expect(page.locator("#pp-concurrency")).toHaveValue("2");
  await expect(page.locator("#pp-grace")).toHaveValue("5");
  await page.getByLabel("Script options").click();
  await page.getByRole("option", { name: POST_PROCESSING_NZBGET_DISPLAY_NAME }).click();
  await expect(optionsGroup.getByLabel("Label")).toHaveValue("e2e-label");
  await expect(optionsGroup.getByLabel("Token")).toHaveValue("[REDACTED]");

  // 5. A real job runs the list in order and records one result per script.
  const job = await runJobThroughPostProcessing(request, "weaver-e2e-post-processing");
  expect(job.outputDir, "a completed job must retain its output directory").toBeTruthy();

  await expect
    .poll(async () => (await scriptResults(request, job.id)).length, { timeout: 60_000 })
    .toBe(3);
  const results = await scriptResults(request, job.id);
  expect(results.map((result) => result.script)).toEqual([
    POST_PROCESSING_NOTIFY_SCRIPT,
    POST_PROCESSING_FAILING_SCRIPT,
    POST_PROCESSING_NZBGET_PACKAGE,
  ]);
  expect(results[0].status).toBe("SUCCEEDED");
  // A nonzero SABnzbd exit is a warning, and the list keeps going.
  expect(results[1].status).toBe("WARNING");
  expect(results[1].exitCode).toBe(3);
  // NZBGet's exit 93 is success.
  expect(results[2].status).toBe("SUCCEEDED");
  expect(results[2].exitCode).toBe(93);

  // The scripts really ran, in order, against the job's output directory.
  const marker = postProcessingMarker(job.outputDir!);
  expect(marker.trim().split("\n")).toEqual(["notify", "failing", "nzbget e2e-label"]);

  // 6. The job's event log shows what each script did, and never the secret.
  await page.goto(`/jobs/${job.id}`);
  const eventLog = page.getByText(POST_PROCESSING_NOTIFY_SCRIPT).first();
  await expect(eventLog).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(POST_PROCESSING_SECRET)).toHaveCount(0);
  await expect(page.getByText("[REDACTED]").first()).toBeVisible();

  // 7. Re-running executes the list again against the retained output.
  await page.getByRole("button", { name: "Re-run scripts" }).click();
  await expect
    .poll(() => postProcessingMarker(job.outputDir!).trim().split("\n").length, {
      timeout: 60_000,
      intervals: [500, 1_000],
    })
    .toBe(6);
  expect(await scriptResults(request, job.id)).toHaveLength(3);
});

test("a disabled entry stays in the list without running", async ({
  cleanPage: page,
  request,
}) => {
  removePostProcessingScripts();
  seedPostProcessingScripts();

  await page.goto("/settings/post-processing");
  const executionToggle = page.getByRole("switch", { name: "Run post-processing scripts" });
  await expect(executionToggle).toBeVisible();
  if (!(await executionToggle.isChecked())) {
    await executionToggle.click();
    await expect(page.getByText("Post-processing settings saved.")).toBeVisible();
  }

  const list = page.getByRole("list", { name: "Script list" });
  for (const entry of await list.getByRole("listitem").all()) {
    const remove = entry.getByRole("button", { name: "Remove" });
    if (await remove.isVisible()) {
      await remove.click();
      await expect(page.getByText("Script list saved.")).toBeVisible();
    }
  }
  await addScriptToGlobalList(page, POST_PROCESSING_NOTIFY_SCRIPT);
  await page
    .getByRole("switch", { name: `Enable ${POST_PROCESSING_NOTIFY_SCRIPT}` })
    .click();
  await expect(page.getByText("Script list saved.")).toBeVisible();

  const job = await runJobThroughPostProcessing(request, "weaver-e2e-post-processing-disabled");
  expect(await scriptResults(request, job.id)).toHaveLength(0);
  expect(postProcessingMarker(job.outputDir!)).toBe("");
  // The entry is still configured, just not enabled.
  await page.reload();
  await expect(
    list.getByRole("listitem", { name: `Script ${POST_PROCESSING_NOTIFY_SCRIPT}` }),
  ).toBeVisible();
  expect(POST_PROCESSING_MARKER).toBeTruthy();
});

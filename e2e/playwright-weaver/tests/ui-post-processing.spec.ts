import type { Page } from "@playwright/test";

import { expect, test } from "./helpers";
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
import {
  runJobThroughPostProcessing,
  scriptResults,
} from "./support/setup/post-processing-job";

async function waitForScriptListSave(page: Page, action: () => Promise<void>): Promise<void> {
  const response = page.waitForResponse(
    (candidate) =>
      candidate.url().includes("/graphql") &&
      candidate.request().method() === "POST" &&
      candidate.request().postData()?.includes("mutation SetScriptLists") === true,
  );
  await action();
  expect((await response).ok()).toBeTruthy();
  await expect(page.getByText("Script list saved.")).toBeVisible();
}

async function addScriptToGlobalList(page: Page, displayName: string): Promise<void> {
  await waitForScriptListSave(page, () =>
    page.getByRole("button", { name: `Add ${displayName}`, exact: true }).click(),
  );
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
  await page.getByRole("button", { name: "Event Log" }).click();
  // Counted rather than picked positionally: the log legitimately mentions a
  // script more than once, and which occurrence renders first is not something
  // this test should assert. Presence is the claim — the secret's absence
  // below is asserted the same way.
  await expect(page.getByText(POST_PROCESSING_NOTIFY_SCRIPT)).not.toHaveCount(0, {
    timeout: 30_000,
  });
  await expect(page.getByText(POST_PROCESSING_SECRET)).toHaveCount(0);
  await expect(page.getByText("[REDACTED]")).not.toHaveCount(0);

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
  const scriptEntryLabels = await list.getByRole("listitem").evaluateAll((items) =>
    items
      .map((item) => item.getAttribute("aria-label"))
      .filter((label): label is string => label?.startsWith("Script ") === true),
  );
  for (const label of scriptEntryLabels) {
    const entry = list.getByRole("listitem", { name: label, exact: true });
    const remove = entry.getByRole("button", { name: "Remove", exact: true });
    await waitForScriptListSave(page, () => remove.click());
  }
  await addScriptToGlobalList(page, POST_PROCESSING_NOTIFY_SCRIPT);
  await waitForScriptListSave(page, () =>
    page.getByRole("switch", { name: `Enable ${POST_PROCESSING_NOTIFY_SCRIPT}` }).click(),
  );

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

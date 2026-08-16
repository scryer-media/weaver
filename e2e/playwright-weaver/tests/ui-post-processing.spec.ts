import type { Page } from "@playwright/test";

import { expect, test } from "./helpers";
import { nntpBodyTransferCount } from "./support/external-control/nntp";
import {
  enqueuePostProcessingRuns,
  POST_PROCESSING_E2E_EXTENSION_ID,
  POST_PROCESSING_E2E_PROFILE,
  POST_PROCESSING_E2E_SECRET,
  POST_PROCESSING_FAILING_EXTENSION_NAME,
  POST_PROCESSING_QUIET_EXTENSION_NAME,
  POST_PROCESSING_SLOW_EXTENSION_NAME,
  preparePostProcessingJobs,
  seedPostProcessingRunsForProfile,
  seedPostProcessingExecutionPackages,
  seedPostProcessingPackage,
} from "./support/setup/post-processing-package";

test("post-processing settings, profiles, assignments, and real queue behavior are browser-owned", async ({
  cleanPage: page,
  request,
}) => {
  seedPostProcessingPackage();
  const nntpBefore = await nntpBodyTransferCount();
  await page.goto("/settings/post-processing");
  const discoveryToggle = page.getByRole("switch", {
    name: "Discover packages in the data-directory scripts folder",
  });
  if (!(await discoveryToggle.isChecked())) await discoveryToggle.click();
  const executionToggle = page.getByRole("switch", {
    name: "Execute approved post-processing plans",
  });
  if (!(await executionToggle.isChecked())) await executionToggle.click();
  await page.locator("#pp-concurrency").fill("2");
  await page.locator("#pp-grace").fill("15");
  await page.locator("#pp-roots").fill("/data");
  await page.getByRole("button", { name: "Save settings" }).click();
  await expect(page.getByText(/post-processing settings saved/i)).toBeVisible();

  await page.getByRole("button", { name: "Scan scripts folder" }).click();
  await expect(page.getByText(/discovery found 1 package/i)).toBeVisible();

  const revision = page.getByRole("group", {
    name: /^Extension revision E2E Lifecycle Extension 1\.0\.0 /,
  });
  await expect(revision).toContainText("UNAPPROVED");
  await revision.getByRole("button", { name: "Approve immutable revision" }).click();
  await expect(page.getByText("E2E Lifecycle Extension revision approved.")).toBeVisible();
  await expect(revision).toContainText("APPROVED");
  await revision.getByRole("button", { name: "Disable" }).click();
  await expect(page.getByText("E2E Lifecycle Extension revision disabled.")).toBeVisible();
  await expect(revision).toContainText("UNAPPROVED");
  await revision.getByRole("button", { name: "Approve immutable revision" }).click();
  await revision.getByRole("button", { name: "Revoke trust" }).click();
  await expect(page.getByText("E2E Lifecycle Extension revision revoked.")).toBeVisible();
  await expect(revision).toContainText("REVOKED");
  await revision.getByRole("button", { name: "Approve immutable revision" }).click();
  await expect(revision).toContainText("APPROVED");

  await page.locator("#pp-profile-id").fill(POST_PROCESSING_E2E_PROFILE);
  await page.locator("#pp-profile-name").fill("E2E Extension Profile");
  await page.getByRole("button", { name: "Add step" }).click();
  const profileStep = page.getByTestId("pp-profile-step-0");
  await expect(profileStep).toContainText("E2E Lifecycle Extension");
  await profileStep.getByLabel("Options JSON").fill(
    JSON.stringify([
      { name: "TOKEN", kind: "SECRET", value: POST_PROCESSING_E2E_SECRET },
    ]),
  );
  await page.getByRole("button", { name: "Save profile" }).click();
  await expect(page.getByText("Profile E2E Extension Profile saved.")).toBeVisible();
  const profilesSection = page.getByRole("region", { name: "Profiles" });
  await expect(
    profilesSection.getByText("E2E Extension Profile", { exact: true }),
  ).toBeVisible();

  await page.getByLabel("Global default").click();
  await page.getByRole("option", { name: "E2E Extension Profile" }).click();
  await page.getByRole("button", { name: "Save global default" }).click();
  await expect(page.getByText("Global default profile assignment saved.")).toBeVisible();

  await page.getByLabel("Category assignment").fill("movies");
  await page.getByLabel("Category profile").click();
  await page.getByRole("option", { name: "E2E Extension Profile" }).click();
  await page.getByRole("button", { name: "Save category assignment" }).click();
  await expect(page.getByText("Assignment for category movies saved.")).toBeVisible();

  const prepared = await preparePostProcessingJobs(request);
  expect(prepared.jobIds).toHaveLength(2);
  expect(prepared.runIds).toEqual([]);
  const [firstJobId, secondJobId] = prepared.jobIds;

  const jobIdInput = page.getByLabel("Job ID");
  await jobIdInput.fill(String(firstJobId));
  await page.getByRole("button", { name: "Inspect job plan" }).click();
  const frozenPlan = page.getByRole("region", {
    name: `Frozen plan for job ${firstJobId}`,
  });
  const frozenPlanDefinition = frozenPlan.getByRole("region", {
    name: "Frozen plan definition",
  });
  await expect(frozenPlan).toContainText("FROZEN");
  await expect(frozenPlanDefinition).toContainText(POST_PROCESSING_E2E_EXTENSION_ID);
  await expect(frozenPlanDefinition).toContainText("[REDACTED]");
  await expect(frozenPlanDefinition).not.toContainText(POST_PROCESSING_E2E_SECRET);

  const inspectorSection = page.getByRole("region", {
    name: "Job plan and run inspector",
  });
  const runsPanel = inspectorSection.getByRole("region", {
    name: "Post-processing runs",
  });
  await inspectorSection.getByLabel("Selection mode").click();
  await page.getByRole("option", { name: "Profile", exact: true }).click();
  await inspectorSection.getByLabel("Profile", { exact: true }).click();
  await page.getByRole("option", { name: "E2E Extension Profile", exact: true }).click();
  await inspectorSection
    .getByRole("button", { name: "Freeze replacement selection" })
    .click();
  await expect(page.getByText(`Frozen selection updated for job ${firstJobId}.`)).toBeVisible();
  await expect(frozenPlanDefinition).toContainText(POST_PROCESSING_E2E_EXTENSION_ID);

  await page.getByRole("button", { name: "Pause queue" }).click();
  await expect(page.getByText("Post-processing queue paused.")).toBeVisible();
  const queued = await enqueuePostProcessingRuns(request, prepared.jobIds);
  expect(queued.jobIds).toEqual(prepared.jobIds);
  expect(queued.runIds).toHaveLength(2);

  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  const queue = page.getByRole("list", { name: "Post-processing queue" });
  const queueRow = (jobId: number) =>
    queue.getByRole("listitem", { name: `Post-processing queue job ${jobId}` });
  await expect(queueRow(firstJobId)).toContainText("QUEUED");
  await expect(queueRow(secondJobId)).toContainText("QUEUED");

  await queueRow(secondJobId).getByRole("button", { name: "Move up" }).click();
  await expect(page.getByText("Post-processing queue order updated.")).toBeVisible();
  await expect(queue.getByRole("listitem")).toContainText([
    new RegExp(`Job ${secondJobId}[\\s\\S]*QUEUED`),
    new RegExp(`Job ${firstJobId}[\\s\\S]*QUEUED`),
  ]);

  await queueRow(firstJobId).getByRole("button", { name: "Cancel attempt" }).click();
  await expect(page.getByText(`Cancellation requested for job ${firstJobId}.`)).toBeVisible();
  await expect(queueRow(firstJobId)).toBeHidden({ timeout: 10_000 });
  await jobIdInput.fill(String(firstJobId));
  await page.getByRole("button", { name: "Inspect job plan" }).click();
  await expect
    .poll(async () =>
      runsPanel.getByRole("button", { name: /^Post-processing run .* CANCELLED$/ }).count(),
    )
    .toBeGreaterThan(0);

  await page.getByRole("button", { name: "Resume queue" }).click();
  await expect(page.getByText("Post-processing queue resumed.")).toBeVisible();
  // The second job's run generates the deliberately-huge bounded-log
  // output and legitimately takes 25-40s under full-suite load.
  await expect(queueRow(secondJobId)).toBeHidden({ timeout: 120_000 });

  await jobIdInput.fill(String(secondJobId));
  await page.getByRole("button", { name: "Inspect job plan" }).click();
  await expect(
    page.getByRole("region", { name: `Frozen plan for job ${secondJobId}` }),
  ).toBeVisible();
  const attempt = page.getByRole("button", { name: /Step 1/ });
  await expect(attempt).toContainText("SUCCEEDED", { timeout: 30_000 });
  await expect(attempt).toContainText("TRUNCATED");

  const boundedLog = page.getByRole("log", { name: "Redacted bounded log output" });
  await expect(boundedLog).toContainText("[REDACTED]");
  await expect(boundedLog).not.toContainText(POST_PROCESSING_E2E_SECRET);
  await expect(
    page.getByText(
      "Output exceeded the persisted cap; the header and rolling tail were retained.",
      { exact: true },
    ),
  ).toBeVisible();

  await page.getByRole("button", { name: "Rerun all scripts" }).click();
  await expect(page.getByText("Created full script-only rerun.")).toBeVisible();
  // The script-only rerun repeats the huge bounded-log run (25-40s under
  // full-suite load); both polls must outlast it.
  await expect
    .poll(
      async () => runsPanel.getByRole("button", { name: /Post-processing run/ }).count(),
      { timeout: 120_000 },
    )
    .toBeGreaterThanOrEqual(2);
  await expect
    .poll(
      async () =>
        runsPanel
          .getByRole("button", { name: /Post-processing run .* SUCCEEDED$/ })
          .count(),
      { timeout: 120_000 },
    )
    .toBeGreaterThanOrEqual(2);
  expect(await nntpBodyTransferCount()).toBe(nntpBefore);
  await page.getByRole("button", { name: "Rerun failed and later" }).click();
  await expect(page.getByText(/source run has no failed or interrupted attempt/i)).toBeVisible();

  await page.reload();
  await expect(page.locator("#pp-concurrency")).toHaveValue("2");
  await expect(page.locator("#pp-grace")).toHaveValue("15");
  await expect(page.locator("#pp-roots")).toHaveValue("/data");
  await expect(
    profilesSection.getByText("E2E Extension Profile", { exact: true }),
  ).toBeVisible();
});

/**
 * Bring the settings page up with discovery + execution enabled and every e2e
 * execution-behaviour package discovered and approved. Idempotent, so each
 * execution test is self-sufficient rather than depending on an earlier test.
 */
async function prepareExecutionExtensions(page: Page): Promise<void> {
  seedPostProcessingExecutionPackages();
  await page.goto("/settings/post-processing");

  // "Scan scripts folder" is disabled until discovery is enabled, and the
  // settings form renders from empty defaults until its query resolves. Reading
  // the toggles before that hydration reports "off" for settings that are
  // already on, so wait for the button's own enabled state instead — the first
  // test in this file turns discovery and execution on and saves them.
  const scanButton = page.getByRole("button", { name: "Scan scripts folder" });
  await expect(scanButton).toBeEnabled({ timeout: 60_000 });
  await scanButton.click();
  await expect(page.getByText(/discovery found \d+ package/i)).toBeVisible();
  // Each package is asserted visible below rather than relying on the notice's
  // count: if a package never reached weaver's scripts folder, the named
  // revision assertion says exactly which one instead of failing much later as
  // an unrelated-looking timeout.

  for (const name of [
    POST_PROCESSING_FAILING_EXTENSION_NAME,
    POST_PROCESSING_QUIET_EXTENSION_NAME,
    POST_PROCESSING_SLOW_EXTENSION_NAME,
  ]) {
    const revision = page.getByRole("group", {
      name: new RegExp(`^Extension revision ${name} 1\\.0\\.0 `),
    });
    await expect(revision).toBeVisible();
    // "APPROVED" is a substring of "UNAPPROVED", so the trust badge has to be
    // matched exactly — a substring check would treat an unapproved revision as
    // approved and only surface later as an unfindable revision option.
    const trustBadge = revision.getByText(/^(?:UNAPPROVED|APPROVED|REVOKED)$/);
    if ((await trustBadge.textContent())?.trim() !== "APPROVED") {
      await revision.getByRole("button", { name: "Approve immutable revision" }).click();
      await expect(page.getByText(`${name} revision approved.`)).toBeVisible();
    }
    await expect(trustBadge).toHaveText("APPROVED");
  }
}

function profileStep(page: Page, index: number) {
  return page.getByTestId(`pp-profile-step-${index}`);
}

/** Point profile step `index` at one of the approved e2e extensions. */
async function selectStepRevision(
  page: Page,
  index: number,
  extensionName: string,
): Promise<void> {
  await profileStep(page, index).getByLabel("Approved immutable revision").click();
  await page.getByRole("option", { name: new RegExp(`^${extensionName} `) }).click();
}

async function saveProfile(page: Page, id: string, name: string): Promise<void> {
  await page.locator("#pp-profile-id").fill(id);
  await page.locator("#pp-profile-name").fill(name);
  await page.getByRole("button", { name: "Save profile" }).click();
  await expect(page.getByText(`Profile ${name} saved.`)).toBeVisible();
}

/**
 * Run `seed` with the post-processing queue paused through the UI, then resume.
 *
 * The seed mutation refuses to enqueue unless the queue is paused, while a run
 * seeded during the pause parks in the admission loop until it resumes — so the
 * pair has to be bracketed. Resume runs in `finally` because the pause flag is
 * process-global: a mid-test failure that left it set would strand every later
 * test's runs at QUEUED.
 */
/**
 * Re-open a finished run so its attempt rows are fresh.
 *
 * The attempts query is keyed on the selected run id, so a run that was already
 * selected while it was still executing keeps rendering its attempts as RUNNING
 * even after the run reaches a terminal status. Reloading refetches everything,
 * then the run is re-inspected and re-selected.
 */
async function reopenFinishedRun(page: Page, jobId: number, status: RegExp) {
  await page.reload();
  const runsPanel = await runPlanForJob(page, jobId);
  const run = runsPanel.getByRole("button", { name: status });
  await expect(run).toBeVisible();
  await run.click();
  return run;
}

async function withPausedQueue<T>(page: Page, seed: () => Promise<T>): Promise<T> {
  await page.getByRole("button", { name: "Pause queue" }).click();
  await expect(page.getByText("Post-processing queue paused.")).toBeVisible();
  try {
    return await seed();
  } finally {
    await page.getByRole("button", { name: "Resume queue" }).click();
    await expect(page.getByText("Post-processing queue resumed.")).toBeVisible();
  }
}

/**
 * Open the inspector for `jobId` and return its runs panel, retrying the
 * inspect until the panel renders — the queue refreshes on its own timer, so a
 * single click can land before the seeded run is visible to the page.
 */
async function runPlanForJob(page: Page, jobId: number) {
  const inspectorSection = page.getByRole("region", {
    name: "Job plan and run inspector",
  });
  const runsPanel = inspectorSection.getByRole("region", {
    name: "Post-processing runs",
  });
  await expect
    .poll(
      async () => {
        await page.getByLabel("Job ID").fill(String(jobId));
        await page.getByRole("button", { name: "Inspect job plan" }).click();
        return runsPanel.getByRole("button", { name: /Post-processing run/ }).count();
      },
      { timeout: 60_000 },
    )
    .toBeGreaterThan(0);
  return runsPanel;
}

test("a failing step marked fail-job records a failed attempt and a failed run", async ({
  cleanPage: page,
  request,
}) => {
  await prepareExecutionExtensions(page);

  await page.getByRole("button", { name: "Add step" }).click();
  await selectStepRevision(page, 0, POST_PROCESSING_FAILING_EXTENSION_NAME);
  await profileStep(page, 0).getByLabel("Outcome impact").click();
  await page.getByRole("option", { name: "Fail successful job" }).click();
  await saveProfile(page, "e2e-failing-profile", "E2E Failing Profile");

  const jobId = await withPausedQueue(page, () =>
    seedPostProcessingRunsForProfile(request, "e2e-failing-profile"),
  );

  const runsPanel = await runPlanForJob(page, jobId);
  await expect
    .poll(
      async () =>
        runsPanel.getByRole("button", { name: /Post-processing run .* FAILED$/ }).count(),
      { timeout: 60_000 },
    )
    .toBeGreaterThan(0);

  await runsPanel.getByRole("button", { name: /Post-processing run .* FAILED$/ }).click();
  const attempt = page.getByRole("button", { name: /Step 1/ });
  await expect(attempt).toContainText("FAILED");
  await expect(attempt).toContainText("exit 3");
});

test("a failing step set to continue still runs later steps and warns", async ({
  cleanPage: page,
  request,
}) => {
  await prepareExecutionExtensions(page);

  await page.getByRole("button", { name: "Add step" }).click();
  await selectStepRevision(page, 0, POST_PROCESSING_FAILING_EXTENSION_NAME);
  await profileStep(page, 0).getByLabel("After failure").click();
  await page.getByRole("option", { name: "Continue", exact: true }).click();
  await page.getByRole("button", { name: "Add step" }).click();
  await selectStepRevision(page, 1, POST_PROCESSING_QUIET_EXTENSION_NAME);
  await saveProfile(page, "e2e-continue-profile", "E2E Continue Profile");

  const jobId = await withPausedQueue(page, () =>
    seedPostProcessingRunsForProfile(request, "e2e-continue-profile"),
  );

  // A failing step whose impact is WARN degrades the run summary to WARNING but
  // leaves the run itself SUCCEEDED, and the run button is named by status.
  const runsPanel = await runPlanForJob(page, jobId);
  const warnedRun = runsPanel.getByRole("button", {
    name: /Post-processing run .* SUCCEEDED$/,
  });
  await expect
    .poll(async () => warnedRun.count(), { timeout: 60_000 })
    .toBeGreaterThan(0);
  await expect(warnedRun).toContainText("WARNING");

  await reopenFinishedRun(page, jobId, /Post-processing run .* SUCCEEDED$/);
  // The second step must have run despite the first failing.
  await expect(page.getByRole("button", { name: /Step 1/ })).toContainText("FAILED");
  await expect(page.getByRole("button", { name: /Step 2/ })).toContainText("SUCCEEDED");
});

test("a step whose artifact condition is unmet is skipped without an attempt", async ({
  cleanPage: page,
  request,
}) => {
  await prepareExecutionExtensions(page);

  await page.getByRole("button", { name: "Add step" }).click();
  await selectStepRevision(page, 0, POST_PROCESSING_QUIET_EXTENSION_NAME);
  await profileStep(page, 0)
    .getByLabel("Required artifact suffixes (one per line)")
    .fill(".no-such-suffix");
  await profileStep(page, 0).getByLabel("Minimum artifact count").fill("1");
  await saveProfile(page, "e2e-condition-profile", "E2E Condition Profile");

  const jobId = await withPausedQueue(page, () =>
    seedPostProcessingRunsForProfile(request, "e2e-condition-profile"),
  );

  // No attempt executes, so the summary degrades to NOTRUN and the run is
  // recorded as SKIPPED.
  const runsPanel = await runPlanForJob(page, jobId);
  const skippedRun = runsPanel.getByRole("button", {
    name: /Post-processing run .* SKIPPED$/,
  });
  await expect
    .poll(async () => skippedRun.count(), { timeout: 60_000 })
    .toBeGreaterThan(0);
  await expect(skippedRun).toContainText("NOTRUN");

  await skippedRun.click();
  // The unmet condition must skip the step outright rather than spawn an attempt.
  await expect(page.getByRole("button", { name: /Step 1/ })).toBeHidden();
});

test("a step that outlives its timeout is recorded as timed out", async ({
  cleanPage: page,
  request,
}) => {
  await prepareExecutionExtensions(page);

  await page.getByRole("button", { name: "Add step" }).click();
  await selectStepRevision(page, 0, POST_PROCESSING_SLOW_EXTENSION_NAME);
  await profileStep(page, 0).getByLabel("Timeout seconds").fill("2");
  await saveProfile(page, "e2e-timeout-profile", "E2E Timeout Profile");

  const jobId = await withPausedQueue(page, () =>
    seedPostProcessingRunsForProfile(request, "e2e-timeout-profile"),
  );

  // The step keeps the default WARN impact, so the timeout degrades the summary
  // to WARNING without failing the run — the attempt itself is what records
  // TIMED_OUT.
  const runsPanel = await runPlanForJob(page, jobId);
  const timedOutRun = runsPanel.getByRole("button", {
    name: /Post-processing run .* SUCCEEDED$/,
  });
  await expect
    .poll(async () => timedOutRun.count(), { timeout: 90_000 })
    .toBeGreaterThan(0);
  await expect(timedOutRun).toContainText("WARNING");

  await reopenFinishedRun(page, jobId, /Post-processing run .* SUCCEEDED$/);
  await expect(page.getByRole("button", { name: /Step 1/ })).toContainText("TIMEDOUT");
});

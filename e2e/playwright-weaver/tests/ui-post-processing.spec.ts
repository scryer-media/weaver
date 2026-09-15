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

function operationResponse(page: Page, operation: string) {
  return page.waitForResponse(
    (candidate) =>
      new URL(candidate.url()).pathname.endsWith("/graphql")
      && candidate.request().method() === "POST"
      && candidate.request().postData()?.includes(`mutation ${operation}`) === true,
  );
}

async function waitForScriptListSave(page: Page, action: () => Promise<void>): Promise<void> {
  const response = operationResponse(page, "SetScriptLists");
  await action();
  expect((await response).ok()).toBeTruthy();
  await expect(page.getByRole("contentinfo")).toContainText("Run list saved");
}

async function addScriptToRunList(page: Page, displayName: string): Promise<void> {
  const label = "Add a script to the run list";
  await page.getByRole("region", { name: "Run list", exact: true }).getByRole("button", { name: label, exact: true }).click();
  await waitForScriptListSave(page, () =>
    page
      .getByRole("menu", { name: label, exact: true })
      .getByRole("menuitemradio", { name: displayName, exact: true })
      .click(),
  );
}

/** A discovered script's row, which opens its options. */
function discoveredScript(page: Page, displayName: string) {
  return page
    .getByRole("region", { name: "Discovered scripts", exact: true })
    .getByRole("button")
    .filter({ has: page.getByText(displayName, { exact: true }) });
}

async function saveSettings(page: Page) {
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

test("post-processing settings, the live script list, and real script execution are browser-owned", async ({
  cleanPage: page,
  request,
}) => {
  removePostProcessingScripts();
  seedPostProcessingScripts();

  await page.goto("/settings/post-processing");

  // 1. The master switch is off until an operator turns it on.
  const executionToggle = page.getByRole("switch", { name: "Run scripts", exact: true });
  await expect(executionToggle).toBeVisible();
  if (!(await executionToggle.isChecked())) {
    await executionToggle.click();
  }
  const concurrency = page.getByRole("spinbutton", { name: "Concurrent scripts", exact: true });
  await concurrency.fill("2");
  await concurrency.press("Tab");
  const grace = page.getByRole("spinbutton", { name: "Termination grace", exact: true });
  await grace.fill("5");
  await grace.press("Tab");
  const extensions = page.getByRole("textbox", { name: "Unacceptable extensions", exact: true });
  await extensions.fill("EXE, r??");
  await saveSettings(page);

  // 2. Scripts are listed live from the directory, with unreadable ones surfaced.
  const problems = page.getByRole("region", { name: "Scripts that could not be read", exact: true });
  await expect(problems).toContainText(POST_PROCESSING_BROKEN_PACKAGE);
  await expect(
    discoveredScript(page, POST_PROCESSING_NZBGET_DISPLAY_NAME).getByText("NZBGet", { exact: true }),
  ).toBeVisible();
  await expect(
    discoveredScript(page, POST_PROCESSING_NOTIFY_SCRIPT).getByText("SABnzbd", { exact: true }),
  ).toBeVisible();

  // 3. Build the global list, in the order the scripts must run.
  await addScriptToRunList(page, POST_PROCESSING_NOTIFY_SCRIPT);
  await addScriptToRunList(page, POST_PROCESSING_FAILING_SCRIPT);
  await addScriptToRunList(page, POST_PROCESSING_NZBGET_DISPLAY_NAME);
  const runList = page.getByRole("region", { name: "Run list", exact: true });
  await expect(runList).toContainText(
    new RegExp(
      [POST_PROCESSING_NOTIFY_SCRIPT, POST_PROCESSING_FAILING_SCRIPT, POST_PROCESSING_NZBGET_DISPLAY_NAME]
        .map(escapeRegExp)
        .join("[\\s\\S]*"),
    ),
  );
  for (const script of [POST_PROCESSING_NOTIFY_SCRIPT, POST_PROCESSING_FAILING_SCRIPT, POST_PROCESSING_NZBGET_PACKAGE]) {
    await expect(runList.getByRole("switch", { name: `Run ${script}`, exact: true })).toBeChecked();
  }

  // 4. Manifest options, including a secret that must never come back in cleartext.
  const options = page.getByRole("dialog", { name: POST_PROCESSING_NZBGET_DISPLAY_NAME, exact: true });
  await discoveredScript(page, POST_PROCESSING_NZBGET_DISPLAY_NAME).click();
  await expect(options.getByRole("textbox", { name: "Label", exact: true })).toHaveValue("default-label");
  await options.getByRole("textbox", { name: "Label", exact: true }).fill("e2e-label");
  await options.getByLabel("Token", { exact: true }).fill(POST_PROCESSING_SECRET);
  await options.getByRole("button", { name: "Save options", exact: true }).click();
  await expect(options).toBeHidden();
  await expect(page.getByRole("contentinfo")).toContainText(
    `Options for ${POST_PROCESSING_NZBGET_DISPLAY_NAME} saved`,
  );

  await page.reload();
  await expect(executionToggle).toBeChecked();
  await expect(concurrency).toHaveValue("2");
  await expect(grace).toHaveValue("5");
  await expect(extensions).toHaveValue("exe, r??");
  await discoveredScript(page, POST_PROCESSING_NZBGET_DISPLAY_NAME).click();
  await expect(options.getByRole("textbox", { name: "Label", exact: true })).toHaveValue("e2e-label");
  await expect(options.getByLabel("Token", { exact: true })).toHaveValue("[REDACTED]");
  await expect(page.getByRole("main")).not.toContainText(POST_PROCESSING_SECRET);
  await options.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(options).toBeHidden();

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
  const eventLog = page.getByRole("region", { name: "Event log", exact: true });
  // Counted rather than picked positionally: the log legitimately mentions a
  // script more than once, and which occurrence renders first is not something
  // this test should assert. Presence is the claim — the secret's absence
  // below is asserted the same way.
  await expect(eventLog.getByText(POST_PROCESSING_NOTIFY_SCRIPT)).not.toHaveCount(0, {
    timeout: 30_000,
  });
  await expect(page.getByText(POST_PROCESSING_SECRET)).toHaveCount(0);
  await expect(eventLog.getByText("[REDACTED]")).not.toHaveCount(0);

  // 7. Re-running executes the list again against the retained output.
  await page.getByRole("button", { name: "Re-run scripts", exact: true }).click();
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
  const executionToggle = page.getByRole("switch", { name: "Run scripts", exact: true });
  await expect(executionToggle).toBeVisible();
  if (!(await executionToggle.isChecked())) {
    await executionToggle.click();
    await saveSettings(page);
  }

  const runList = page.getByRole("region", { name: "Run list", exact: true });
  const notifyEntry = runList.getByRole("switch", { name: `Run ${POST_PROCESSING_NOTIFY_SCRIPT}`, exact: true });
  if ((await notifyEntry.count()) === 0) {
    await addScriptToRunList(page, POST_PROCESSING_NOTIFY_SCRIPT);
  }
  // Every entry is switched off, so the job has nothing enabled to run.
  for (const script of [POST_PROCESSING_NOTIFY_SCRIPT, POST_PROCESSING_FAILING_SCRIPT, POST_PROCESSING_NZBGET_PACKAGE]) {
    const entry = runList.getByRole("switch", { name: `Run ${script}`, exact: true });
    if ((await entry.count()) > 0 && (await entry.isChecked())) {
      await waitForScriptListSave(page, () => entry.click());
      await expect(entry).not.toBeChecked();
    }
  }

  const job = await runJobThroughPostProcessing(request, "weaver-e2e-post-processing-disabled");
  expect(await scriptResults(request, job.id)).toHaveLength(0);
  expect(postProcessingMarker(job.outputDir!)).toBe("");
  // The entry is still configured, just not enabled.
  await page.reload();
  await expect(notifyEntry).toBeVisible();
  await expect(notifyEntry).not.toBeChecked();
  expect(POST_PROCESSING_MARKER).toBeTruthy();
});

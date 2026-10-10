import { randomBytes } from "node:crypto";
import type { Locator, Page } from "@playwright/test";

import { expect, test } from "./helpers";
import {
  postProcessingRunCounts,
  removeFixtureScripts,
  removeScriptJobs,
  seedScriptRuns,
  submitScriptJob,
  useScriptDirectory,
  waitScriptJob,
  writeBareScript,
  writeFixtureScript,
} from "./support/setup/script-jobs";

/**
 * The Scripts screens, driven the way an operator does: Configuration,
 * Jobs (add, switch off, reorder, test, delete), Secrets and Runs, and the
 * script runs on a job's own page. Setup only writes script files, turns
 * execution on and submits jobs; every claim is read off the screens.
 */

const SCRIPT_CONFIGURATION = "/settings/scripts/configuration";
const SCRIPT_JOBS = "/settings/scripts/list";
const SCRIPT_SECRETS = "/settings/scripts/secrets";
const SCRIPT_RUNS = "/settings/scripts/runs";

const token = () => `${Date.now().toString(36)}${randomBytes(2).toString("hex")}`;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Text that holds `parts` in this order. */
const inOrder = (parts: string[]) => new RegExp(parts.map(escapeRegExp).join("[\\s\\S]*"));

async function saveSettings(page: Page) {
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();
}

/** Choose `option` in the select labelled `label`, inside `scope`. */
async function choose(page: Page, scope: Locator, label: string, option: string) {
  await scope.getByRole("button", { name: label, exact: true }).click();
  await page.getByRole("menu", { name: label, exact: true }).getByRole("menuitemradio", { name: option, exact: true }).click();
  await expect(scope.getByRole("button", { name: label, exact: true })).toHaveText(option);
}

/** A Runs status tab, named by its status and the count beside it. */
const statusTab = (page: Page, status: string, count: number) =>
  page.getByRole("button", { name: new RegExp(`^${status}\\s*${count}$`) });

/** The post-processing group of the Jobs table. */
const postProcessingJobs = (page: Page) =>
  page.getByRole("region", { name: "Jobs", exact: true }).getByRole("region", { name: "Post-processing", exact: true });

/** A job's row, which opens its editor. */
const jobRow = (page: Page, name: string) =>
  postProcessingJobs(page).getByRole("button").filter({ has: page.getByText(name, { exact: true }) });

async function addJob(page: Page, script: string, name: string, inputs: Record<string, string> = {}) {
  await page.getByRole("banner").getByRole("button", { name: "Add job", exact: true }).click();
  const editor = page.getByRole("dialog", { name: "Add job", exact: true });
  await choose(page, editor, "Script", script);
  await expect(editor.getByRole("button", { name: "Trigger", exact: true })).toHaveText("Post-processing");
  await editor.getByRole("textbox", { name: "Job name", exact: true }).fill(name);
  for (const [input, value] of Object.entries(inputs)) await editor.getByRole("textbox", { name: input, exact: true }).fill(value);
  await editor.getByRole("button", { name: "Save", exact: true }).click();
  await expect(editor).toBeHidden();
  await expect(page.getByRole("contentinfo")).toContainText(`${name} created`);
  await expect(jobRow(page, name)).toBeVisible();
}

test("Configuration saves execution, ordering policy, limits and interpreters", async ({ cleanPage: page, request }) => {
  const restore = await useScriptDirectory(request);
  try {
    await page.goto(SCRIPT_CONFIGURATION);
    await expect(page.getByRole("textbox", { name: "Scripts directory", exact: true })).toHaveValue("/data/scripts");
    const execution = page.getByRole("switch", { name: "Run scripts", exact: true });
    await expect(execution).toBeChecked();
    await execution.click();
    await expect(execution).not.toBeChecked();
    const settings = page.getByRole("region", { name: "Execution", exact: true });
    await choose(page, settings, "Global scripts run", "For every download");
    const concurrency = page.getByRole("spinbutton", { name: "Concurrent scripts", exact: true });
    await concurrency.fill("2");
    await concurrency.press("Tab");
    const grace = page.getByRole("spinbutton", { name: "Termination grace", exact: true });
    await grace.fill("5");
    await grace.press("Tab");
    const extensions = page.getByRole("textbox", { name: "Unacceptable extensions", exact: true });
    await extensions.fill("EXE, r??");
    const python = page.getByRole("textbox", { name: "Python", exact: true });
    await python.fill("/usr/bin/python3");
    await saveSettings(page);

    await page.reload();
    await expect(execution).not.toBeChecked();
    await expect(settings.getByRole("button", { name: "Global scripts run", exact: true })).toHaveText("For every download");
    await expect(concurrency).toHaveValue("2");
    await expect(grace).toHaveValue("5");
    await expect(extensions).toHaveValue("exe, r??");
    await expect(python).toHaveValue("/usr/bin/python3");

    // Back on, so the restore below and the other tests find execution as they left it.
    await execution.click();
    await saveSettings(page);
    await page.reload();
    await expect(execution).toBeChecked();
  } finally {
    await restore();
  }
});

test("Jobs are added, run in their order, switched off, reordered, tested and deleted from the Jobs table", async ({ cleanPage: page, request }) => {
  const tag = `uipp-${token()}`;
  const sab = writeBareScript(`${tag}-sab`, { body: "printf 'sab ran for %s\\n' \"$SAB_FINAL_NAME\"\n" });
  const nzb = writeFixtureScript(`${tag}-nzb`, {
    kinds: ["POST-PROCESSING"], exitCode: 93, headerOptions: ["Label=header-label"],
    body: "printf 'nzb label=%s\\n' \"$NZBPO_Label\"\n",
  });
  const first = `${tag}-first`;
  const second = `${tag}-second`;
  const restore = await useScriptDirectory(request);
  try {
    await page.goto(SCRIPT_JOBS);
    await expect(page.getByRole("region", { name: "Jobs", exact: true })).toContainText("No jobs yet.");
    await addJob(page, sab, first);
    await addJob(page, nzb, second, { Label: "screen-label" });
    await expect(postProcessingJobs(page)).toContainText(inOrder([first, second]));
    for (const name of [first, second]) {
      await expect(postProcessingJobs(page).getByRole("switch", { name: `${name} enabled`, exact: true })).toBeChecked();
    }

    // A job runs them in that order, and its page shows each run and what it printed.
    const run1 = await submitScriptJob(request, `${tag}-a`);
    await waitScriptJob(request, run1, 2);
    await page.goto(`/jobs/${run1}`);
    const runs1 = page.getByRole("group", { name: "post_processing (2)", exact: true });
    await expect(runs1).toContainText(inOrder([first, second]));
    await expect(runs1.getByText("SUCCEEDED", { exact: true })).toHaveCount(2);
    await expect(runs1).toContainText(`sab ran for ${tag}-a`);
    await expect(runs1).toContainText("nzb label=screen-label");

    // Switched off, the first job stays listed and does not run; moved up, the second runs first.
    await page.goto(SCRIPT_JOBS);
    const firstEnabled = postProcessingJobs(page).getByRole("switch", { name: `${first} enabled`, exact: true });
    await firstEnabled.click();
    await expect(firstEnabled).not.toBeChecked();
    await jobRow(page, second).getByRole("button", { name: "Move up", exact: true }).click();
    await expect(postProcessingJobs(page)).toContainText(inOrder([second, first]));
    await page.reload();
    await expect(postProcessingJobs(page)).toContainText(inOrder([second, first]));
    await expect(firstEnabled).not.toBeChecked();
    const run2 = await submitScriptJob(request, `${tag}-b`);
    await waitScriptJob(request, run2, 1);
    await page.goto(`/jobs/${run2}`);
    const runs2 = page.getByRole("group", { name: "post_processing (1)", exact: true });
    await expect(runs2).toContainText(second);
    await expect(runs2).not.toContainText(first);

    // Test runs the saved job against made-up inputs and shows how it went.
    await page.goto(SCRIPT_JOBS);
    await jobRow(page, second).getByRole("button", { name: "Test", exact: true }).click();
    const testDialog = page.getByRole("dialog", { name: `Test ${second}`, exact: true });
    await expect(testDialog.getByText("SUCCEEDED", { exact: true })).toBeVisible();
    await expect(testDialog.getByRole("region", { name: "Run log", exact: true })).toContainText("nzb label=screen-label");
    await expect(testDialog.getByRole("region", { name: "Arguments", exact: true })).toBeVisible();
    await expect(testDialog.getByRole("button", { name: "Run again", exact: true })).toBeVisible();
    await testDialog.getByRole("button", { name: "Close", exact: true }).click();
    await expect(testDialog).toBeHidden();

    // The editor reopens a job with what was saved in it.
    await jobRow(page, second).click();
    const editor = page.getByRole("dialog", { name: second, exact: true });
    await expect(editor.getByRole("textbox", { name: "Label", exact: true })).toHaveValue("screen-label");
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(editor).toBeHidden();

    for (const name of [first, second]) {
      await jobRow(page, name).getByRole("button", { name: "Delete", exact: true }).click();
      const confirm = page.getByRole("dialog", { name: "Delete job", exact: true });
      await confirm.getByRole("button", { name: "Delete", exact: true }).click();
      await expect(confirm).toBeHidden();
      await expect(page.getByRole("contentinfo")).toContainText(`${name} deleted`);
      await expect(jobRow(page, name)).toHaveCount(0);
    }
    await page.reload();
    await expect(page.getByRole("region", { name: "Jobs", exact: true })).toContainText("No jobs yet.");
  } finally {
    await removeScriptJobs(request, [sab, nzb]);
    await restore();
    removeFixtureScripts([sab, nzb]);
  }
});

test("Secrets are added with a value that is never shown again, and deleted", async ({ cleanPage: page }) => {
  const name = `uipp-secret-${token()}`;
  const value = `uipp-value-${token()}-never-shown`;
  await page.goto(SCRIPT_SECRETS);
  await page.getByRole("banner").getByRole("button", { name: "Add secret", exact: true }).click();
  const editor = page.getByRole("dialog", { name: "Add secret", exact: true });
  await editor.getByRole("textbox", { name: "Name", exact: true }).fill(name);
  await editor.getByLabel("Value", { exact: true }).fill(value);
  await editor.getByRole("button", { name: "Save", exact: true }).click();
  await expect(editor).toBeHidden();
  await expect(page.getByRole("contentinfo")).toContainText(`Secret ${name} created`);
  const secrets = page.getByRole("region", { name: "Secrets", exact: true });
  const row = secrets.getByRole("button").filter({ has: page.getByText(name, { exact: true }) });
  await expect(row).toContainText("Not used");

  await page.reload();
  await row.click();
  const opened = page.getByRole("dialog", { name, exact: true });
  await expect(opened.getByLabel("Value", { exact: true })).toHaveValue("");
  await expect(page.getByText(value)).toHaveCount(0);
  await opened.getByRole("button", { name: "Delete", exact: true }).click();
  const confirm = page.getByRole("dialog", { name: "Delete secret", exact: true });
  await confirm.getByRole("button", { name: "Delete", exact: true }).click();
  await expect(page.getByRole("contentinfo")).toContainText(`Secret ${name} deleted`);
  await expect(row).toHaveCount(0);
});

test("Runs lists every run newest first, filters by trigger and status, opens output, and pages forward without re-reading", async ({ cleanPage: page, request }) => {
  const tag = `uipp-runs-${token()}`;
  const script = writeBareScript(`${tag}-sab`, { body: `echo "${tag} output line"\n` });
  const failing = writeBareScript(`${tag}-fail`, { body: `echo "${tag} failing line"\n`, exitCode: 4 });
  const restore = await useScriptDirectory(request);
  try {
    // More runs than the first page holds, then one failed run on top, over
    // whatever runs earlier tests left.
    const RUNS = 27;
    const earlier = await postProcessingRunCounts(request);
    await seedScriptRuns(request, script, RUNS);
    await seedScriptRuns(request, failing, 1);
    const total = earlier.all + RUNS + 1;
    const failed = earlier.failed + 1;

    // Every ScriptRuns request the screen sends, in order, with its cursor.
    const cursors: Array<string | null> = [];
    page.on("request", sent => {
      const body = sent.postData();
      if (sent.method() !== "POST" || !body?.includes("query ScriptRuns")) return;
      const parsed = JSON.parse(body) as { variables?: { before?: string | null } };
      cursors.push(parsed.variables?.before ?? null);
    });

    await page.goto(SCRIPT_RUNS);
    const runs = page.getByRole("region", { name: "Script runs", exact: true });
    const runRow = (name: string) => runs.getByRole("button").filter({ has: page.getByText(name, { exact: true }) });
    await choose(page, page.getByRole("banner"), "Filter by trigger", "Post-processing");
    await expect(statusTab(page, "All", total)).toBeVisible();
    await expect(statusTab(page, "Failed", failed)).toBeVisible();
    await expect(page.getByText(`1–25 of ${total}`, { exact: true })).toBeVisible();
    await expect(runRow(`${failing}-01`), "the newest run first").toBeVisible();
    await expect(runs).toContainText(inOrder([`${failing}-01`, `${script}-${RUNS}`]));

    // A run opens to what it printed.
    await runRow(`${failing}-01`).click();
    await expect(runs.getByRole("region", { name: `Output of ${failing}-01`, exact: true })).toContainText(`${tag} failing line`);
    await expect(runs.getByRole("button", { name: "Copy output", exact: true })).toBeVisible();

    // The next page asks once, from where the first ended.
    const before = cursors.length;
    await page.getByRole("button", { name: "Next", exact: true }).click();
    await expect(page.getByText(`26–${Math.min(50, total)} of ${total}`, { exact: true })).toBeVisible();
    await expect(runRow(`${script}-01`), "the oldest run on the last page").toBeVisible();
    const sent = cursors.slice(before);
    expect(sent, "one request for the next page").toHaveLength(1);
    expect(sent[0], "the next page is asked for below the first").not.toBeNull();

    // Filtering by status counts and lists only those runs.
    await statusTab(page, "Failed", failed).click();
    await expect(page.getByText(`1–${Math.min(25, failed)} of ${failed}`, { exact: true })).toBeVisible();
    await expect(runRow(`${failing}-01`)).toContainText("FAILED");
    await expect(runRow(`${failing}-01`)).toContainText("4");
    await expect(runs.getByRole("button").filter({ has: page.getByText(`${script}-01`, { exact: true }) })).toHaveCount(0);

    await choose(page, page.getByRole("banner"), "Filter by trigger", "Scan");
    await expect(runs).toContainText("No runs match these filters.");
  } finally {
    await removeScriptJobs(request, [script, failing]);
    await restore();
    removeFixtureScripts([script, failing]);
  }
});

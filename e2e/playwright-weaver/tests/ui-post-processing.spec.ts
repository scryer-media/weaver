// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { randomBytes } from "node:crypto";
import type { Locator, Page } from "@playwright/test";

import { expect, test } from "./helpers";
import { isOperation } from "./support/graphql-operations";
import { saveSettings, tableRow } from "./support/settings-locators";
import { jobIds, settingsIds, testIds } from "./support/testids";
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

const field = testIds.field;

/** The settings report line, where each action says how it went. */
const statusLine = (page: Page) => page.getByTestId(testIds.shellStatus);

/** Choose `value` in the select `controlId`, inside `scope`, and see it chosen. */
async function choose(page: Page, scope: Page | Locator, controlId: string, value: string) {
  await scope.getByTestId(controlId).click();
  await page.getByTestId(testIds.menu(controlId)).getByTestId(testIds.option(controlId, value)).click();
  await expect(scope.getByTestId(controlId).getByTestId(testIds.selected(controlId, value))).toBeVisible();
}

/** A Runs status tab, by its status, showing `count` beside it. */
async function expectStatusTab(page: Page, status: string, count: number) {
  const owner = settingsIds.scriptRunsStatus;
  await expect(page.getByTestId(testIds.tab(owner, status))).toBeVisible();
  await expect(page.getByTestId(testIds.tabCount(owner, status))).toHaveText(String(count));
}

/** The pagination bar's range, `from`-`to` of `total`, whatever its wording. */
const pageRange = (from: number, to: number, total: number) => new RegExp(`^${from}–${to}\\D+${total}$`);

/** The post-processing group of the Jobs table. */
const postProcessingJobs = (page: Page) =>
  page.getByTestId(testIds.section("instances")).getByTestId(testIds.rowGroup("instances", "POST_PROCESSING"));

/** A job's row, which opens its editor. */
const jobRow = (page: Page, name: string) => tableRow(page, "instances", name, postProcessingJobs(page));

async function addJob(page: Page, script: string, name: string, inputs: Record<string, string> = {}) {
  await page.getByTestId(testIds.tableAdd("instances")).click();
  const editor = page.getByTestId(testIds.dialog("pp-job"));
  await choose(page, editor, field("script"), script);
  await expect(
    editor.getByTestId(field("trigger")).getByTestId(testIds.selected(field("trigger"), "POST_PROCESSING")),
  ).toBeVisible();
  await editor.getByTestId(field("name")).fill(name);
  for (const [input, value] of Object.entries(inputs)) await editor.getByTestId(field(`input:${input}`)).fill(value);
  await editor.getByTestId(testIds.editorSave).click();
  await expect(editor).toBeHidden();
  await expect(statusLine(page)).toContainText(name);
  await expect(jobRow(page, name)).toBeVisible();
}

test("Configuration saves execution, ordering policy, limits and interpreters", async ({ cleanPage: page, request }) => {
  const restore = await useScriptDirectory(request);
  try {
    await page.goto(SCRIPT_CONFIGURATION);
    await expect(page.getByTestId(jobIds.scriptDirectory).getByRole("textbox")).toHaveValue("/data/scripts");
    const execution = page.getByTestId(field("executionEnabled"));
    await expect(execution).toBeChecked();
    await execution.click();
    await expect(execution).not.toBeChecked();
    const settings = page.getByTestId(testIds.section("execution"));
    await choose(page, settings, field("globalScriptsRun"), "ALWAYS");
    const concurrency = page.getByTestId(field("concurrency"));
    await concurrency.fill("2");
    await concurrency.press("Tab");
    const grace = page.getByTestId(field("terminationGraceSeconds"));
    await grace.fill("5");
    await grace.press("Tab");
    // The unwanted extension list lives under General downloads, not here.
    await expect(page.getByTestId(field("unacceptableExtensions"))).toHaveCount(0);
    const python = page.getByTestId(field("pythonInterpreter"));
    await python.fill("/usr/bin/python3");
    await saveSettings(page);

    await page.reload();
    await expect(execution).not.toBeChecked();
    await expect(
      settings.getByTestId(field("globalScriptsRun")).getByTestId(testIds.selected(field("globalScriptsRun"), "ALWAYS")),
    ).toBeVisible();
    await expect(concurrency).toHaveValue("2");
    await expect(grace).toHaveValue("5");
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
    await expect(page.getByTestId(testIds.tableEmpty("instances"))).toBeVisible();
    await addJob(page, sab, first);
    await addJob(page, nzb, second, { Label: "screen-label" });
    await expect(postProcessingJobs(page)).toContainText(inOrder([first, second]));
    for (const name of [first, second]) {
      await expect(jobRow(page, name).getByTestId(settingsIds.ppJobEnabled)).toBeChecked();
    }

    // A job runs them in that order, and its page shows each run and what it printed.
    const run1 = await submitScriptJob(request, `${tag}-a`);
    await waitScriptJob(request, run1, 2);
    await page.goto(`/jobs/${run1}`);
    const runs1 = page.getByTestId(jobIds.scriptEventGroup("post_processing"));
    await expect(runs1.getByTestId(jobIds.scriptRun)).toHaveCount(2);
    await expect(runs1).toContainText(inOrder([first, second]));
    await expect(runs1.getByTestId(jobIds.scriptRunStatus("SUCCEEDED"))).toHaveCount(2);
    await expect(runs1).toContainText(`sab ran for ${tag}-a`);
    await expect(runs1).toContainText("nzb label=screen-label");

    // Switched off, the first job stays listed and does not run; moved up, the second runs first.
    await page.goto(SCRIPT_JOBS);
    const firstEnabled = jobRow(page, first).getByTestId(settingsIds.ppJobEnabled);
    await firstEnabled.click();
    await expect(firstEnabled).not.toBeChecked();
    await jobRow(page, second).getByTestId(settingsIds.ppJobMoveUp).click();
    await expect(postProcessingJobs(page)).toContainText(inOrder([second, first]));
    await page.reload();
    await expect(postProcessingJobs(page)).toContainText(inOrder([second, first]));
    await expect(firstEnabled).not.toBeChecked();
    const run2 = await submitScriptJob(request, `${tag}-b`);
    await waitScriptJob(request, run2, 1);
    await page.goto(`/jobs/${run2}`);
    const runs2 = page.getByTestId(jobIds.scriptEventGroup("post_processing"));
    await expect(runs2.getByTestId(jobIds.scriptRun)).toHaveCount(1);
    await expect(runs2).toContainText(second);
    await expect(runs2).not.toContainText(first);

    // Test runs the saved job against made-up inputs and shows how it went.
    await page.goto(SCRIPT_JOBS);
    await jobRow(page, second).getByTestId(settingsIds.ppJobTest).click();
    const testDialog = page.getByTestId(testIds.dialog("pp-test"));
    await expect(testDialog).toContainText(second);
    await expect(testDialog.getByTestId(jobIds.scriptRunStatus("SUCCEEDED"))).toBeVisible();
    await expect(testDialog.getByTestId(settingsIds.ppTestLog)).toContainText("nzb label=screen-label");
    await expect(testDialog.getByTestId(settingsIds.ppTestArguments)).toBeVisible();
    await expect(testDialog.getByTestId(settingsIds.ppTestAgain)).toBeVisible();
    await testDialog.getByTestId(settingsIds.ppTestClose).click();
    await expect(testDialog).toBeHidden();

    // The editor reopens a job with what was saved in it.
    await jobRow(page, second).click();
    const editor = page.getByTestId(testIds.dialog("pp-job"));
    await expect(editor).toContainText(second);
    await expect(editor.getByTestId(field("input:Label"))).toHaveValue("screen-label");
    await editor.getByTestId(testIds.editorCancel).click();
    await expect(editor).toBeHidden();

    for (const name of [first, second]) {
      await jobRow(page, name).getByTestId(settingsIds.ppJobDelete).click();
      const confirm = page.getByTestId(testIds.dialog("pp-job-delete"));
      await expect(confirm).toContainText(name);
      await confirm.getByTestId(testIds.confirmAccept).click();
      await expect(confirm).toBeHidden();
      await expect(statusLine(page)).toContainText(name);
      await expect(jobRow(page, name)).toHaveCount(0);
    }
    await page.reload();
    await expect(page.getByTestId(testIds.tableEmpty("instances"))).toBeVisible();
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
  await page.getByTestId(testIds.tableAdd("secrets")).click();
  const editor = page.getByTestId(testIds.dialog("secret"));
  await editor.getByTestId(field("name")).fill(name);
  await editor.getByTestId(field("value")).fill(value);
  await editor.getByTestId(testIds.editorSave).click();
  await expect(editor).toBeHidden();
  await expect(statusLine(page)).toContainText(name);
  const row = tableRow(page, "secrets", name);
  await expect(row.getByTestId(settingsIds.secretUsage("unused"))).toBeVisible();

  await page.reload();
  await row.click();
  const opened = page.getByTestId(testIds.dialog("secret"));
  await expect(opened).toContainText(name);
  await expect(opened.getByTestId(field("value"))).toHaveValue("");
  await expect(page.getByText(value)).toHaveCount(0);
  await opened.getByTestId(testIds.editorDelete).click();
  const confirm = page.getByTestId(testIds.dialog("secret-delete"));
  await confirm.getByTestId(testIds.confirmAccept).click();
  await expect(statusLine(page)).toContainText(name);
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
      if (!isOperation(sent, "ScriptRuns")) return;
      const parsed = sent.postDataJSON() as { variables?: { before?: string | null } };
      cursors.push(parsed.variables?.before ?? null);
    });

    await page.goto(SCRIPT_RUNS);
    const runs = page.getByTestId(testIds.section("runs"));
    const runRow = (name: string) => tableRow(page, "runs", name, runs);
    const range = page.getByTestId(testIds.paginationRange);
    await choose(page, page, settingsIds.scriptRunsTrigger, "POST_PROCESSING");
    await expectStatusTab(page, "ALL", total);
    await expectStatusTab(page, "FAILED", failed);
    await expect(range).toHaveText(pageRange(1, 25, total));
    await expect(runRow(`${failing}-01`), "the newest run first").toBeVisible();
    await expect(runs).toContainText(inOrder([`${failing}-01`, `${script}-${RUNS}`]));

    // A run opens to what it printed.
    await runRow(`${failing}-01`).click();
    // One run is open, so its output is the only one shown.
    await expect(runs.getByTestId(settingsIds.scriptRunOutput)).toContainText(`${tag} failing line`);
    await expect(runs.getByTestId(settingsIds.scriptRunCopy)).toBeVisible();

    // The next page asks once, from where the first ended.
    const before = cursors.length;
    await page.getByTestId(testIds.paginationNext).click();
    await expect(range).toHaveText(pageRange(26, Math.min(50, total), total));
    await expect(runRow(`${script}-01`), "the oldest run on the last page").toBeVisible();
    const sent = cursors.slice(before);
    expect(sent, "one request for the next page").toHaveLength(1);
    expect(sent[0], "the next page is asked for below the first").not.toBeNull();

    // Filtering by status counts and lists only those runs.
    await expectStatusTab(page, "FAILED", failed);
    await page.getByTestId(testIds.tab(settingsIds.scriptRunsStatus, "FAILED")).click();
    await expect(range).toHaveText(pageRange(1, Math.min(25, failed), failed));
    await expect(runRow(`${failing}-01`).getByTestId(jobIds.scriptRunStatus("FAILED"))).toBeVisible();
    await expect(runRow(`${failing}-01`).getByTestId(settingsIds.scriptRunExitCode)).toHaveText("4");
    await expect(runRow(`${script}-01`)).toHaveCount(0);

    await choose(page, page, settingsIds.scriptRunsTrigger, "SCAN");
    await expect(page.getByTestId(testIds.tableEmpty("runs"))).toBeVisible();
  } finally {
    await removeScriptJobs(request, [script, failing]);
    await restore();
    removeFixtureScripts([script, failing]);
  }
});

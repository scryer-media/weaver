// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { defineConfig } from "@playwright/test";

const artifactsDir = process.env.PLAYWRIGHT_ARTIFACTS_DIR || "artifacts";
const artifactStage = (process.env.E2E_WEAVER_ARTIFACT_STAGE ?? "")
  .trim()
  .replace(/[^a-zA-Z0-9._-]+/g, "-");
const runArtifactsDir = artifactStage
  ? `${artifactsDir}/${artifactStage}`
  : artifactsDir;

// The release gate kills a flow at its deadline, and a killed run keeps no
// trace, video or report. The gate passes the time Playwright may use; the run
// stops at that deadline through the global timeout, and the helpers fixture
// ends each test before it so a stuck test fails with its evidence. Workers
// load this file too and inherit the deadline the runner fixed.
const budgetMs = Number(process.env.E2E_WEAVER_PLAYWRIGHT_BUDGET_MS) || 0;
if (budgetMs > 0 && !process.env.E2E_WEAVER_PLAYWRIGHT_DEADLINE_MS) {
  process.env.E2E_WEAVER_PLAYWRIGHT_DEADLINE_MS = String(Date.now() + budgetMs);
}
const deadlineMs = Number(process.env.E2E_WEAVER_PLAYWRIGHT_DEADLINE_MS) || 0;

// Local iteration only: E2E_WEAVER_PLAYWRIGHT_TEST_TIMEOUT_MS caps every
// test's timeout, including those a test raises itself, so a targeted re-run
// fails fast. Unset (the gate and CI), every timeout is unchanged.
const iterationTestTimeoutMs = Number(process.env.E2E_WEAVER_PLAYWRIGHT_TEST_TIMEOUT_MS) || 0;

// Long-form scenarios (@extended) wait out ten-minute product timers. They
// run only when the operator asks for them, like the archive matrix; the
// npm scripts repeat this in --grep-invert, which replaces this setting.
const extended = process.env.E2E_WEAVER_EXTENDED === "1";

// A test's own bound is a hang detector, never a deadline: an hour is far
// above any real run of any test here on a loaded runner, the same order as
// the Rust suite's one-hour end. Tests set no timeout of their own.
const testHangMs = 60 * 60 * 1000;

export default defineConfig({
  testDir: "./tests",
  grepInvert: extended ? undefined : /@extended/,
  outputDir: `${runArtifactsDir}/test-results`,
  timeout: iterationTestTimeoutMs || testHangMs,
  globalTimeout: deadlineMs > 0 ? Math.max(1, deadlineMs - Date.now()) : 0,
  expect: { timeout: 20 * 1000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [
    ["list"],
    ["./reporters/progress-reporter", { outputDir: runArtifactsDir }],
    ["html", { outputFolder: `${runArtifactsDir}/html-report`, open: "never" }],
  ],
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    viewport: { width: 1440, height: 1200 },
    launchOptions: { args: ["--disable-dev-shm-usage"] },
  },
});

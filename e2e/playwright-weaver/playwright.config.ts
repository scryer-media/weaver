import { defineConfig } from "@playwright/test";

const artifactsDir = process.env.PLAYWRIGHT_ARTIFACTS_DIR || "artifacts";
const artifactStage = (process.env.E2E_WEAVER_ARTIFACT_STAGE ?? "")
  .trim()
  .replace(/[^a-zA-Z0-9._-]+/g, "-");
const runArtifactsDir = artifactStage
  ? `${artifactsDir}/${artifactStage}`
  : artifactsDir;

export default defineConfig({
  testDir: "./tests",
  outputDir: `${runArtifactsDir}/test-results`,
  timeout: 5 * 60 * 1000,
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

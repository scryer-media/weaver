import fs from "node:fs";
import path from "node:path";
import type { Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectHttpErrors } from "./support/http-errors";
import {
  makeRestoreTargetNonPristine,
  seedRestorableHistoryMetadata,
} from "./support/setup/restore-blocker";

type BackupStage = "source-export" | "target-blocked" | "target-restore" | "target-verify";
type Datastore = "sqlite" | "postgres";

type BackupMatrixState = {
  sourceDatastore: Datastore;
  targetDatastore: Datastore;
  categoryName: string;
  categoryPattern: string;
  apiKeyName: string;
  scheduleName: string;
  categorySourcePath: string;
  serverHost: string;
  rssFeedName: string;
  rssRuleTerm: string;
  watchPath: string;
  historyName: string;
  speedLimit: number;
};

const artifactsDir = "/artifacts";
const backupPath = path.join(artifactsDir, "weaver-backup.enc");
const tamperedBackupPath = path.join(artifactsDir, "weaver-backup-tampered.enc");
const matrixStatePath = path.join(artifactsDir, "weaver-backup-state.json");
const restoreStagedPath = path.join(artifactsDir, "weaver-backup-restore-staged.json");
const backupPassphrase = "weaver-e2e-backup-passphrase";
const loginUsername = "e2e-backup-admin";
const loginPassword = "e2e-backup-login-password";
const serverPassword = "e2e-backup-server-password";

const stage = process.env.E2E_WEAVER_BACKUP_STAGE ?? "";

async function signIn(page: Page): Promise<void> {
  await page.locator("#username").fill(loginUsername);
  await page.locator("#password").fill(loginPassword);
  const loginResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/login"
    && response.request().method() === "POST"
  );
  await page.getByRole("button", { name: "Sign In" }).click();
  expect((await loginResponse).status()).toBe(200);
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign In" })).toHaveCount(0);
}

test(`encrypted backup matrix stage: ${stage || "missing"}`, async ({ cleanPage: page, request }) => {
  const matrix = matrixFromEnvironment();

  switch (stage as BackupStage) {
    case "source-export":
      await runSourceExport(page, request, matrix);
      return;
    case "target-blocked":
      await runTargetBlocked(page, request, matrix);
      return;
    case "target-restore":
      await runTargetRestore(page, matrix);
      return;
    case "target-verify":
      await runTargetVerify(page, matrix);
      return;
    default:
      throw new Error(
        `E2E_WEAVER_BACKUP_STAGE must be source-export, target-blocked, target-restore, or target-verify; got ${JSON.stringify(stage)}`,
      );
  }
});

async function runSourceExport(
  page: Page,
  request: Parameters<typeof seedRestorableHistoryMetadata>[0],
  matrix: BackupMatrixState,
) {
  await createSourceMarkersThroughUI(page, request, matrix);
  await expectProductMarkers(page, matrix, true);

  await page.goto("/settings/backup");
  const exportPassword = page.locator("#backup-export-password");
  const exportPasswordConfirm = page.locator("#backup-export-password-confirm");
  const downloadButton = page.getByRole("button", { name: /download backup/i });
  await exportPassword.fill(backupPassphrase);
  await expect(exportPassword).toHaveAttribute("type", "password");
  await expect(exportPasswordConfirm).toHaveAttribute("type", "password");

  // A mismatched confirmation must block the export before any request fires.
  await exportPasswordConfirm.fill(`${backupPassphrase}-mismatch`);
  await expect(page.getByRole("alert").filter({ hasText: /do not match/i })).toBeVisible();
  await expect(downloadButton).toBeDisabled();
  await exportPasswordConfirm.fill(backupPassphrase);
  await expect(page.getByRole("alert").filter({ hasText: /do not match/i })).toHaveCount(0);
  await expect(downloadButton).toBeEnabled();

  const downloadPromise = page.waitForEvent("download");
  await downloadButton.click();
  const download = await downloadPromise;
  await download.saveAs(backupPath);
  await expect(page.getByText("Backup download started.", { exact: true })).toBeVisible();

  const backup = fs.readFileSync(backupPath);
  expect(backup.length, "encrypted backup artifact is empty").toBeGreaterThan(0);
  const tampered = Buffer.from(backup);
  tampered[Math.floor(tampered.length / 2)] ^= 0xff;
  fs.writeFileSync(tamperedBackupPath, tampered);
  fs.writeFileSync(matrixStatePath, `${JSON.stringify(matrix, null, 2)}\n`, "utf8");
}

async function runTargetRestore(page: Page, expectedMatrix: BackupMatrixState) {
  const matrix = readMatrixState();
  expect(matrix).toEqual(expectedMatrix);
  expectSharedFile(backupPath);
  expectSharedFile(tamperedBackupPath);

  await expectProductMarkers(page, matrix, false);

  expectHttpErrors(page, {
    method: "POST",
    pathname: "/api/backup/inspect",
    status: 400,
  });
  await analyzeBackup(page, backupPath, "wrong-passphrase");
  await expect(page.getByTestId("backup-restore-error")).toContainText(/password|decrypt|invalid/i);
  await expect(page.getByRole("button", { name: /restore backup/i })).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  expectHttpErrors(page, {
    method: "POST",
    pathname: "/api/backup/inspect",
    status: 400,
  });
  await analyzeBackup(page, tamperedBackupPath, backupPassphrase);
  await expect(page.getByTestId("backup-restore-error")).toContainText(
    /archive|corrupt|decrypt|integrity|invalid|password/i,
  );
  await expect(page.getByRole("button", { name: /restore backup/i })).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  await analyzeBackup(page, backupPath, backupPassphrase);
  const preview = page.getByTestId("backup-preview");
  await expect(preview).toBeVisible();
  await expect(preview).toContainText(`Source database: ${matrix.sourceDatastore}`);
  await fillRequiredCategoryRemaps(page);

  const restoreButton = page.getByRole("button", { name: /restore backup/i });
  await expect(restoreButton).toBeEnabled();
  await restoreButton.click();
  const confirmation = page.getByRole("dialog");
  await expect(confirmation).toBeVisible();
  await confirmation.getByRole("button", { name: /restore backup/i }).click();
  await expect(page.getByText(/restore staged with \d+ history jobs/i)).toBeVisible();
  await expect(page.getByText(/is staged\. restart weaver to apply it/i)).toBeVisible();

  // Restore is staged atomically: the live target remains unchanged until the
  // harness restarts Weaver for the target-verify stage.
  await expectProductMarkers(page, matrix, false);
  fs.writeFileSync(
    restoreStagedPath,
    `${JSON.stringify(
      {
        sourceDatastore: matrix.sourceDatastore,
        targetDatastore: matrix.targetDatastore,
        stagedAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
}

async function runTargetVerify(page: Page, expectedMatrix: BackupMatrixState) {
  const matrix = readMatrixState();
  expect(matrix).toEqual(expectedMatrix);
  expectSharedFile(restoreStagedPath);

  await page.goto("/");
  await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
  await signIn(page);
  await expectProductMarkers(page, matrix, true);
  await page.goto("/settings/backup");
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByText(/is staged\. restart weaver to apply it/i)).toHaveCount(0);
  await expect(page.locator("#backup-export-password")).toHaveAttribute("type", "password");
}

async function runTargetBlocked(
  page: Page,
  request: Parameters<typeof makeRestoreTargetNonPristine>[0],
  expectedMatrix: BackupMatrixState,
) {
  const matrix = readMatrixState();
  expect(matrix).toEqual(expectedMatrix);

  // Sanctioned setup only: a paused metadata probe makes the target
  // intentionally non-pristine without exercising article transfer.
  await page.goto("/");
  await page.getByRole("button", { name: "Pause All" }).click();
  await expect(page.getByRole("button", { name: "Resume All" })).toBeVisible();
  const result = await makeRestoreTargetNonPristine(
    request,
    `weaver-restore-blocked-${matrix.sourceDatastore}-to-${matrix.targetDatastore}`,
  );
  expect(result).toMatchObject({ accepted: true });

  await expectProductMarkers(page, matrix, false);
  await page.goto("/settings/backup");
  await analyzeBackup(page, backupPath, backupPassphrase);
  await expect(page.getByText("Backup Preview", { exact: true })).toBeVisible();
  await expect(page.getByTestId("backup-restore-availability")).toContainText(
    /no active jobs or job history/i,
  );
  await expect(page.getByRole("button", { name: /restore backup/i })).toBeDisabled();
  await expectProductMarkers(page, matrix, false);
}

async function createSourceMarkersThroughUI(
  page: Page,
  request: Parameters<typeof seedRestorableHistoryMetadata>[0],
  matrix: BackupMatrixState,
) {
  await page.goto("/settings/general");
  const speed = page.getByRole("slider", { name: "Speed Limit" });
  await expect(speed).toBeVisible();
  await speed.press("Home");
  for (let mib = 0; mib < matrix.speedLimit / (1024 * 1024); mib += 1) {
    await speed.press("ArrowRight");
  }
  await expect(speed).toHaveValue(String(matrix.speedLimit));
  await page.getByRole("button", { name: "Apply Now" }).click();
  await expect(page.getByText("Saved", { exact: true })).toBeVisible();

  await page.goto("/settings/categories");
  await page.getByTestId("add-category-button").click();
  const categoryForm = page.getByRole("region", { name: "Add Category" });
  await categoryForm.getByLabel("Name").fill(matrix.categoryName);
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.categorySourcePath)),
    { recursive: true },
  );
  await categoryForm.getByRole("button", { name: "Browse" }).click();
  let directoryDialog = page.getByRole("dialog", { name: "Browse Server Directories" });
  await directoryDialog
    .getByRole("textbox", { name: "Current directory path" })
    .fill(matrix.categorySourcePath);
  await directoryDialog.getByRole("button", { name: "Browse" }).click();
  await directoryDialog.getByRole("button", { name: "Use Current Folder" }).click();
  await categoryForm.getByLabel("Aliases").fill(matrix.categoryPattern);
  await categoryForm.getByRole("button", { name: "Add Category" }).click();
  await expect(page.getByRole("row").filter({ hasText: matrix.categoryName })).toContainText(
    matrix.categoryPattern,
  );

  await page.goto("/settings/schedules");
  await page.getByRole("button", { name: "Add Rule" }).click();
  await page.getByLabel("Time").fill("03:15");
  await page.getByLabel("Label").fill(matrix.scheduleName);
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByRole("group", { name: matrix.scheduleName })).toBeVisible();

  await page.goto("/settings/servers");
  await page.getByTestId("add-server-button").click();
  const serverForm = page.getByRole("region", { name: "Add Server" });
  await serverForm.getByLabel("Host").fill(matrix.serverHost);
  await serverForm.getByLabel("Port").fill("119");
  await serverForm.getByLabel("Username").fill("e2e-user");
  await serverForm.getByLabel("Password").fill(serverPassword);
  await serverForm.getByLabel("Connections").fill("1");
  const serverActive = serverForm.getByRole("checkbox", { name: "Active" });
  if (await serverActive.isChecked()) await serverActive.click();
  await serverForm.getByRole("button", { name: "Add Server" }).click();
  await expect(page.getByRole("row").filter({ hasText: `${matrix.serverHost}:119` })).toBeVisible();

  await page.goto("/settings/rss");
  await page.getByTestId("rss-add-feed").click();
  const feedForm = page.getByRole("region", { name: "Add Feed" });
  await feedForm.getByTestId("rss-feed-name").fill(matrix.rssFeedName);
  await feedForm
    .getByTestId("rss-feed-url")
    .fill("http://127.0.0.1:1/e2e-backup.xml");
  await feedForm.getByRole("button", { name: "Add Feed" }).click();
  const feedCard = page.getByRole("region", { name: matrix.rssFeedName });
  await expect(feedCard).toBeVisible();
  await feedCard.getByRole("button", { name: "Add Rule" }).click();
  const ruleForm = feedCard.getByRole("region", { name: "Add Rule" });
  await ruleForm.getByTestId("rss-rule-title-regex").fill(matrix.rssRuleTerm);
  await ruleForm.getByRole("button", { name: "Add Rule" }).click();
  await expect(feedCard.getByText("Accept", { exact: true })).toBeVisible();

  await page.goto("/settings/watch-folder");
  await page.getByRole("combobox", { name: "Mode" }).click();
  await page.getByRole("option", { name: "Polling" }).click();
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.watchPath)),
    { recursive: true },
  );
  await page.getByLabel("Folder", { exact: true }).click();
  directoryDialog = page.getByRole("dialog", { name: "Browse Server Directories" });
  await directoryDialog
    .getByRole("textbox", { name: "Current directory path" })
    .fill(matrix.watchPath);
  await directoryDialog.getByRole("button", { name: "Browse" }).click();
  await directoryDialog.getByRole("button", { name: "Use Current Folder" }).click();
  await page.getByLabel("Poll Interval (seconds)", { exact: true }).fill("45");
  await page.getByRole("button", { name: "Save" }).click();
  await expect(page.getByText(/settings saved/i)).toBeVisible();

  await seedRestorableHistoryMetadata(request, matrix.historyName);
  await page.goto("/history");
  await expect(page.getByRole("row").filter({ hasText: matrix.historyName })).toBeVisible();

  await page.goto("/settings/security");
  await page.getByTestId("api-key-name").fill(matrix.apiKeyName);
  await page.getByRole("combobox").click();
  await page.getByRole("option", { name: "Read" }).click();
  await page.getByRole("button", { name: "Create API Key" }).click();
  const createdDialog = page.getByRole("dialog", { name: "API Key Created" });
  await expect(createdDialog).toContainText(matrix.apiKeyName);
  await expect(createdDialog.getByTestId("raw-api-key")).not.toHaveValue("");
  await createdDialog.press("Escape");
  await expect(page.getByText(matrix.apiKeyName, { exact: true })).toBeVisible();
  await page.locator("#login-username").fill(loginUsername);
  await page.locator("#login-password").fill(loginPassword);
  await page.locator("#login-confirm").fill(loginPassword);
  // Enabling login mid-session 401s whichever polling queries are still in
  // flight before the client redirects to sign-in. At least one proves auth
  // is enforced; the exact number is scheduling noise.
  expectHttpErrors(page, {
    method: "POST",
    pathname: "/graphql",
    status: 401,
    count: 1,
    maxCount: 8,
  });
  await page.getByRole("button", { name: "Enable Login" }).click();
  await expect(page.getByText("Login protection enabled")).toBeVisible();
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
  await signIn(page);
}

async function expectProductMarkers(
  page: Page,
  matrix: BackupMatrixState,
  present: boolean,
) {
  await page.goto("/settings/general");
  const speed = page.getByRole("slider", { name: "Speed Limit" });
  await expect(speed).toBeVisible();
  if (present) {
    await expect(speed).toHaveValue(String(matrix.speedLimit));
  } else {
    await expect(speed).not.toHaveValue(String(matrix.speedLimit));
  }

  await page.goto("/settings/categories");
  const categoryRow = page.getByRole("row").filter({ hasText: matrix.categoryName });
  await expect(categoryRow).toHaveCount(present ? 1 : 0);
  if (present) await expect(categoryRow).toContainText(matrix.categoryPattern);

  await page.goto("/settings/schedules");
  const schedule = page.getByRole("group", { name: matrix.scheduleName });
  await expect(schedule).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/servers");
  const serverRow = page.getByRole("row").filter({ hasText: `${matrix.serverHost}:119` });
  await expect(serverRow).toHaveCount(present ? 1 : 0);
  if (present) {
    await serverRow.getByRole("button", { name: "Edit" }).click();
    const serverForm = page.getByRole("region", { name: "Edit Server" });
    await expect(serverForm.getByLabel("Password")).toHaveValue("");
    await expect(serverForm.getByLabel("Password")).toHaveAttribute(
      "placeholder",
      "Leave blank to keep",
    );
  }

  await page.goto("/settings/rss");
  const feedCard = page.getByRole("region", { name: matrix.rssFeedName });
  await expect(feedCard).toHaveCount(present ? 1 : 0);
  if (present) await expect(feedCard).toContainText(matrix.rssRuleTerm);

  await page.goto("/settings/watch-folder");
  const watchPath = page.getByLabel("Folder", { exact: true });
  if (present) {
    await expect(watchPath).toHaveValue(matrix.watchPath);
    await expect(
      page.getByLabel("Poll Interval (seconds)", { exact: true }),
    ).toHaveValue("45");
  } else {
    await expect(watchPath).not.toHaveValue(matrix.watchPath);
  }

  await page.goto("/history");
  const historyRow = page.getByRole("row").filter({ hasText: matrix.historyName });
  await expect(historyRow).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/security");
  await expect(page.getByText(matrix.apiKeyName, { exact: true })).toHaveCount(
    present ? 1 : 0,
  );
}

async function analyzeBackup(page: Page, file: string, password: string) {
  await page.goto("/settings/backup");
  await page.locator("#backup-restore-file").setInputFiles(file);
  const restorePassword = page.locator("#backup-restore-password");
  await restorePassword.fill(password);
  await expect(restorePassword).toHaveAttribute("type", "password");
  await page.getByRole("button", { name: /analyze backup/i }).click();
}

async function fillRequiredCategoryRemaps(page: Page) {
  const panel = page.getByTestId("backup-category-remaps");
  await expect(panel).toBeVisible();
  const inputs = await panel.getByRole("textbox").all();
  expect(inputs.length, "backup preview did not require a category path remap").toBeGreaterThan(0);
  for (const [index, input] of inputs.entries()) {
    const relativePath = `complete/restored-category-${index + 1}`;
    const destination = `/data/${relativePath}`;
    fs.mkdirSync(path.join("/weaver-data", relativePath), { recursive: true });
    await input.click();
    const directoryDialog = page.getByRole("dialog", {
      name: "Browse Server Directories",
    });
    await directoryDialog
      .getByRole("textbox", { name: "Current directory path" })
      .fill(destination);
    await directoryDialog.getByRole("button", { name: "Browse" }).click();
    await directoryDialog.getByRole("button", { name: "Use Current Folder" }).click();
    await expect(input).toHaveValue(destination);
  }
}

function matrixFromEnvironment(): BackupMatrixState {
  const sourceDatastore = parseDatastore(
    "E2E_WEAVER_BACKUP_SOURCE_DATASTORE",
    process.env.E2E_WEAVER_BACKUP_SOURCE_DATASTORE,
  );
  const targetDatastore = parseDatastore(
    "E2E_WEAVER_BACKUP_TARGET_DATASTORE",
    process.env.E2E_WEAVER_BACKUP_TARGET_DATASTORE,
  );
  const suffix = `${sourceDatastore}-to-${targetDatastore}`;
  return {
    sourceDatastore,
    targetDatastore,
    categoryName: `e2e-backup-category-${suffix}`,
    categoryPattern: `e2e-backup-${suffix}-*`,
    apiKeyName: `e2e-backup-key-${suffix}`,
    scheduleName: `e2e-backup-schedule-${suffix}`,
    categorySourcePath: `/data/e2e-backup-source-${suffix}`,
    serverHost: `e2e-backup-${suffix}.invalid`,
    rssFeedName: `E2E Backup Feed ${suffix}`,
    rssRuleTerm: `E2E Backup Release ${suffix}`,
    watchPath: `/data/e2e-backup-watch-${suffix}`,
    historyName: `e2e-backup-history-${suffix}`,
    speedLimit: 8 * 1024 * 1024,
  };
}

function parseDatastore(name: string, value: string | undefined): Datastore {
  if (value === "sqlite" || value === "postgres") return value;
  throw new Error(`${name} must be sqlite or postgres; got ${JSON.stringify(value)}`);
}

function readMatrixState(): BackupMatrixState {
  expectSharedFile(matrixStatePath);
  return JSON.parse(fs.readFileSync(matrixStatePath, "utf8")) as BackupMatrixState;
}

function expectSharedFile(file: string) {
  expect(fs.existsSync(file), `missing shared backup-flow artifact ${file}`).toBeTruthy();
  expect(fs.statSync(file).size, `shared backup-flow artifact ${file} is empty`).toBeGreaterThan(
    0,
  );
}

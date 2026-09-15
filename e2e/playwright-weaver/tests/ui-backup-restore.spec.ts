import fs from "node:fs";
import path from "node:path";
import type { Locator, Page } from "@playwright/test";
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

const signInButton = (page: Page) => page.getByRole("button", { name: "Sign in", exact: true });

async function signIn(page: Page): Promise<void> {
  await page.locator("#username").fill(loginUsername);
  await page.locator("#password").fill(loginPassword);
  const loginResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/login"
    && response.request().method() === "POST"
  );
  await signInButton(page).click();
  expect((await loginResponse).status()).toBe(200);
  await expect(page.getByRole("main")).toBeVisible();
  await expect(signInButton(page)).toHaveCount(0);
}

/** A settings table row, found by the exact text of one of its cells. */
function tableRow(page: Page, table: string, cellText: string): Locator {
  return page
    .getByRole("region", { name: table, exact: true })
    .getByRole("button")
    .filter({ has: page.getByText(cellText, { exact: true }) });
}

/** Choose a server folder through the directory dialog a path field opens. */
async function chooseFolder(page: Page, field: Locator, dialogName: string, folder: string) {
  await field.click();
  const dialog = page.getByRole("dialog", { name: dialogName, exact: true });
  // The dialog opens on a listing of its own; typing before it lands would be
  // overwritten by it.
  const useFolder = dialog.getByRole("button", { name: "Use this folder", exact: true });
  await expect(useFolder).toBeEnabled();
  await dialog.getByRole("textbox", { name: "Folder path", exact: true }).fill(folder);
  await dialog.getByRole("button", { name: "Go", exact: true }).click();
  // It chooses the folder it is showing, so let the browse land first.
  await expect(dialog.getByTitle(folder, { exact: true })).not.toHaveCount(0);
  await useFolder.click();
  await expect(dialog).toBeHidden();
  await expect(field).toHaveValue(folder);
}

async function saveSettings(page: Page) {
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();
}

const restoreButton = (page: Page) =>
  page
    .getByRole("region", { name: "Restore", exact: true })
    .getByRole("button", { name: "Restore from archive", exact: true });

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
  const exportSection = page.getByRole("region", { name: "Backup", exact: true });
  const exportPassword = exportSection.getByLabel("Password", { exact: true });
  const exportPasswordConfirm = exportSection.getByLabel("Confirm password", { exact: true });
  const downloadButton = page.getByRole("banner").getByRole("button", { name: "Download backup", exact: true });
  await exportPassword.fill(backupPassphrase);
  await expect(exportPassword).toHaveAttribute("type", "password");
  await expect(exportPasswordConfirm).toHaveAttribute("type", "password");

  // A mismatched confirmation must block the export before any request fires.
  const mismatch = exportSection.getByText("The two passwords do not match.", { exact: true });
  await exportPasswordConfirm.fill(`${backupPassphrase}-mismatch`);
  await expect(mismatch).toBeVisible();
  await expect(downloadButton).toBeDisabled();
  await exportPasswordConfirm.fill(backupPassphrase);
  await expect(mismatch).toHaveCount(0);
  await expect(downloadButton).toBeEnabled();

  const downloadPromise = page.waitForEvent("download");
  await downloadButton.click();
  const download = await downloadPromise;
  await download.saveAs(backupPath);
  await expect(page.getByRole("contentinfo")).toContainText(/Saved weaver_backup_\S+\.enc/);

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
  await expect(page.getByRole("contentinfo")).toContainText(/password|decrypt|invalid/i);
  await expect(restoreButton(page)).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  expectHttpErrors(page, {
    method: "POST",
    pathname: "/api/backup/inspect",
    status: 400,
  });
  await analyzeBackup(page, tamperedBackupPath, backupPassphrase);
  await expect(page.getByRole("contentinfo")).toContainText(
    /corrupt|decrypt|integrity|invalid|password/i,
  );
  await expect(restoreButton(page)).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  await analyzeBackup(page, backupPath, backupPassphrase);
  const preview = page.getByRole("region", { name: "What this archive holds", exact: true });
  await expect(preview).toBeVisible();
  await expect(preview).toContainText(`Source database${matrix.sourceDatastore}`);
  await fillRequiredCategoryRemaps(page);

  await expect(restoreButton(page)).toBeEnabled();
  await restoreButton(page).click();
  const confirmation = page.getByRole("dialog", { name: "Restore from archive", exact: true });
  await confirmation.getByRole("button", { name: "Stage restore", exact: true }).click();
  await expect(confirmation).toBeHidden();
  await expect(page.getByRole("contentinfo")).toContainText(
    /Restore staged · \d+ downloads? in history · restart weaver to apply it/,
  );
  await expect(page.getByRole("region", { name: "Staged restore", exact: true })).toContainText(
    "Waiting for a restart",
  );

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
  await expect(signInButton(page)).toBeVisible();
  await signIn(page);
  await expectProductMarkers(page, matrix, true);
  await page.goto("/settings/backup");
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("region", { name: "Staged restore", exact: true })).toHaveCount(0);
  await expect(
    page.getByRole("region", { name: "Backup", exact: true }).getByLabel("Password", { exact: true }),
  ).toHaveAttribute("type", "password");
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
  await page.getByRole("banner").getByRole("button", { name: "Pause all", exact: true }).click();
  await expect(page.getByRole("banner").getByRole("button", { name: "Resume all", exact: true })).toBeVisible();
  const result = await makeRestoreTargetNonPristine(
    request,
    `weaver-restore-blocked-${matrix.sourceDatastore}-to-${matrix.targetDatastore}`,
  );
  expect(result).toMatchObject({ accepted: true });

  await expectProductMarkers(page, matrix, false);
  await analyzeBackup(page, backupPath, backupPassphrase);
  await expect(page.getByRole("region", { name: "What this archive holds", exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "Restore", exact: true })).toContainText(
    /no active jobs or job history/i,
  );
  await expect(restoreButton(page)).toBeDisabled();
  await expectProductMarkers(page, matrix, false);
}

async function createSourceMarkersThroughUI(
  page: Page,
  request: Parameters<typeof seedRestorableHistoryMetadata>[0],
  matrix: BackupMatrixState,
) {
  await page.goto("/settings/bandwidth");
  const ceiling = page.getByRole("spinbutton", { name: "Download ceiling", exact: true });
  await ceiling.fill(downloadCeiling(matrix));
  await ceiling.press("Tab");
  await saveSettings(page);

  await page.goto("/settings/categories");
  await page.getByRole("banner").getByRole("button", { name: "Add category", exact: true }).click();
  const categoryForm = page.getByRole("dialog", { name: "Add category", exact: true });
  await categoryForm.getByRole("textbox", { name: "Name", exact: true }).fill(matrix.categoryName);
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.categorySourcePath)),
    { recursive: true },
  );
  await chooseFolder(
    page,
    categoryForm.getByRole("textbox", { name: "Destination", exact: true }),
    "Destination",
    matrix.categorySourcePath,
  );
  await categoryForm.getByRole("textbox", { name: "Also known as", exact: true }).fill(matrix.categoryPattern);
  await categoryForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(categoryForm).toBeHidden();
  await expect(
    tableRow(page, "Categories", matrix.categoryName).getByText(matrix.categoryPattern, { exact: true }),
  ).toBeVisible();

  await page.goto("/settings/schedules");
  await page.getByRole("banner").getByRole("button", { name: "Add schedule", exact: true }).click();
  const scheduleForm = page.getByRole("dialog", { name: "Add schedule", exact: true });
  await scheduleForm.getByLabel("Time", { exact: true }).fill("03:15");
  await scheduleForm.getByRole("textbox", { name: "Label", exact: true }).fill(matrix.scheduleName);
  await scheduleForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(scheduleForm).toBeHidden();
  await expect(tableRow(page, "Schedules", matrix.scheduleName)).toBeVisible();

  await page.goto("/settings/servers");
  await page.getByRole("banner").getByRole("button", { name: "Add provider", exact: true }).click();
  const serverForm = page.getByRole("dialog", { name: "Add provider", exact: true });
  await serverForm.getByRole("textbox", { name: "Host", exact: true }).fill(matrix.serverHost);
  const tls = serverForm.getByRole("switch", { name: "TLS", exact: true });
  if (await tls.isChecked()) await tls.click();
  await expect(serverForm.getByRole("spinbutton", { name: "Port", exact: true })).toHaveValue("119");
  await serverForm.getByLabel("Username", { exact: true }).fill("e2e-user");
  await serverForm.getByLabel("Password", { exact: true }).fill(serverPassword);
  const connections = serverForm.getByRole("spinbutton", { name: "Connections", exact: true });
  await connections.fill("1");
  await connections.press("Tab");
  const serverEnabled = serverForm.getByRole("switch", { name: "Enabled", exact: true });
  if (await serverEnabled.isChecked()) await serverEnabled.click();
  await serverForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(serverForm).toBeHidden();
  await expect(tableRow(page, "Servers", matrix.serverHost)).toBeVisible();

  await page.goto("/settings/rss");
  await page.getByRole("banner").getByRole("button", { name: "Add feed", exact: true }).click();
  const feedForm = page.getByRole("dialog", { name: "Add feed", exact: true });
  await feedForm.getByRole("textbox", { name: "Name", exact: true }).fill(matrix.rssFeedName);
  await feedForm.getByRole("textbox", { name: "URL", exact: true }).fill("http://127.0.0.1:1/e2e-backup.xml");
  await feedForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(feedForm).toBeHidden();
  await expect(tableRow(page, "Feeds", matrix.rssFeedName)).toBeVisible();
  await page
    .getByRole("region", { name: "Rules", exact: true })
    .getByRole("button", { name: "Add rule", exact: true })
    .click();
  const ruleForm = page.getByRole("dialog", { name: "Add rule", exact: true });
  await ruleForm.getByRole("textbox", { name: "Title matches", exact: true }).fill(matrix.rssRuleTerm);
  await ruleForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(ruleForm).toBeHidden();
  await expect(tableRow(page, "Rules", matrix.rssRuleTerm).getByText("Accept", { exact: true })).toBeVisible();

  await page.goto("/settings/watch-folder");
  const watchSection = page.getByRole("region", { name: "Watch folder", exact: true });
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.watchPath)),
    { recursive: true },
  );
  await chooseFolder(
    page,
    watchSection.getByRole("textbox", { name: "Folder", exact: true }),
    "Folder",
    matrix.watchPath,
  );
  await watchSection
    .getByRole("radiogroup", { name: "Watching", exact: true })
    .getByRole("radio", { name: "Polling", exact: true })
    .click();
  const pollInterval = watchSection.getByRole("spinbutton", { name: "Poll interval", exact: true });
  await pollInterval.fill("45");
  await pollInterval.press("Tab");
  await saveSettings(page);

  await seedRestorableHistoryMetadata(request, matrix.historyName);
  await page.goto("/history");
  await expect(historyRow(page, matrix.historyName)).toBeVisible();

  await page.goto("/settings/security");
  await page.getByRole("banner").getByRole("button", { name: "Add API key", exact: true }).click();
  const keyEditor = page.getByRole("dialog", { name: "New API key", exact: true });
  await keyEditor.getByRole("textbox", { name: "Name", exact: true }).fill(matrix.apiKeyName);
  await keyEditor.getByRole("button", { name: "Scope", exact: true }).click();
  await page
    .getByRole("menu", { name: "Scope", exact: true })
    .getByRole("menuitemradio", { name: "Read only", exact: true })
    .click();
  await keyEditor.getByRole("button", { name: "Create key", exact: true }).click();
  const createdDialog = page.getByRole("dialog", { name: "API key created", exact: true });
  await expect(createdDialog).toContainText(matrix.apiKeyName);
  await expect(createdDialog.getByRole("textbox", { name: "API key", exact: true })).not.toHaveValue("");
  await createdDialog.getByRole("button", { name: "Done", exact: true }).click();
  await expect(createdDialog).toBeHidden();
  await expect(apiKey(page, matrix)).toBeVisible();

  await page.getByRole("button", { name: "Set up a login", exact: true }).click();
  const setup = page.getByRole("dialog", { name: "Set up a login", exact: true });
  await setup.getByLabel("Username", { exact: true }).fill(loginUsername);
  await setup.getByLabel("Password", { exact: true }).fill(loginPassword);
  await setup.getByLabel("Repeat the password", { exact: true }).fill(loginPassword);
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
  await setup.getByRole("button", { name: "Turn on login", exact: true }).click();
  await expect(signInButton(page)).toBeVisible();
  await page.goto("/");
  await expect(signInButton(page)).toBeVisible();
  await signIn(page);
}

function downloadCeiling(matrix: BackupMatrixState): string {
  return String(matrix.speedLimit / (1024 * 1024));
}

function historyRow(page: Page, name: string): Locator {
  return page
    .getByRole("main")
    .getByRole("button")
    .filter({ has: page.getByText(name, { exact: true }) });
}

function apiKey(page: Page, matrix: BackupMatrixState): Locator {
  return page
    .getByRole("region", { name: "API keys", exact: true })
    .getByText(matrix.apiKeyName, { exact: true });
}

async function expectProductMarkers(
  page: Page,
  matrix: BackupMatrixState,
  present: boolean,
) {
  await page.goto("/settings/bandwidth");
  const ceiling = page.getByRole("spinbutton", { name: "Download ceiling", exact: true });
  await expect(ceiling).toBeVisible();
  if (present) {
    await expect(ceiling).toHaveValue(downloadCeiling(matrix));
  } else {
    await expect(ceiling).not.toHaveValue(downloadCeiling(matrix));
  }

  await page.goto("/settings/categories");
  await expect(page.getByRole("region", { name: "Categories", exact: true })).toBeVisible();
  const categoryRow = tableRow(page, "Categories", matrix.categoryName);
  await expect(categoryRow).toHaveCount(present ? 1 : 0);
  if (present) await expect(categoryRow).toContainText(matrix.categoryPattern);

  await page.goto("/settings/schedules");
  await expect(page.getByRole("region", { name: "Schedules", exact: true })).toBeVisible();
  await expect(tableRow(page, "Schedules", matrix.scheduleName)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/servers");
  await expect(tableRow(page, "Servers", "nntp")).toBeVisible();
  const serverRow = tableRow(page, "Servers", matrix.serverHost);
  await expect(serverRow).toHaveCount(present ? 1 : 0);
  if (present) {
    await serverRow.click();
    const serverForm = page.getByRole("dialog", { name: matrix.serverHost, exact: true });
    const password = serverForm.getByLabel("Password", { exact: true });
    await expect(password).toHaveValue("");
    await expect(password).toHaveAttribute("placeholder", "••••••••");
    await expect(serverForm.getByText("Leave blank to keep the stored password.", { exact: true })).toBeVisible();
    await serverForm.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(serverForm).toBeHidden();
  }

  await page.goto("/settings/rss");
  await expect(page.getByRole("region", { name: "Feeds", exact: true })).toBeVisible();
  await expect(tableRow(page, "Feeds", matrix.rssFeedName)).toHaveCount(present ? 1 : 0);
  await expect(tableRow(page, "Rules", matrix.rssRuleTerm)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/watch-folder");
  const watchSection = page.getByRole("region", { name: "Watch folder", exact: true });
  const watchPath = watchSection.getByRole("textbox", { name: "Folder", exact: true });
  await expect(watchPath).toBeVisible();
  if (present) {
    await expect(watchPath).toHaveValue(matrix.watchPath);
    await expect(
      watchSection.getByRole("spinbutton", { name: "Poll interval", exact: true }),
    ).toHaveValue("45");
  } else {
    await expect(watchPath).not.toHaveValue(matrix.watchPath);
  }

  await page.goto("/history");
  await expect(page.getByRole("main")).toBeVisible();
  await expect(historyRow(page, matrix.historyName)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/security");
  await expect(page.getByRole("region", { name: "API keys", exact: true })).toBeVisible();
  await expect(apiKey(page, matrix)).toHaveCount(present ? 1 : 0);
}

async function analyzeBackup(page: Page, file: string, password: string) {
  await page.goto("/settings/backup");
  const restoreSection = page.getByRole("region", { name: "Restore", exact: true });
  // Wait for the restore availability check before reading anything.
  await expect(restoreSection.getByText("checking…", { exact: true })).toHaveCount(0);
  const chooser = page.waitForEvent("filechooser");
  await restoreSection.getByRole("button", { name: "Choose file", exact: true }).click();
  await (await chooser).setFiles(file);
  await expect(restoreSection.getByText(path.basename(file), { exact: true })).toBeVisible();
  const restorePassword = restoreSection.getByLabel("Password", { exact: true });
  await restorePassword.fill(password);
  await expect(restorePassword).toHaveAttribute("type", "password");
  const inspected = page.waitForResponse(
    (response) => new URL(response.url()).pathname.endsWith("/api/backup/inspect"),
  );
  await restoreSection.getByRole("button", { name: "Read archive", exact: true }).click();
  await inspected;
  await expect(page.getByRole("contentinfo")).not.toContainText("Reading the archive…");
}

async function fillRequiredCategoryRemaps(page: Page) {
  const panel = page.getByRole("region", { name: "Category destinations", exact: true });
  await expect(panel).toBeVisible();
  const remapInputs = panel.getByRole("textbox");
  await expect(remapInputs).not.toHaveCount(0);
  const inputs = await remapInputs.all();
  for (const [index, input] of inputs.entries()) {
    // /data/complete is a separate volume this container cannot write, and
    // the directory picker only chooses folders that exist.
    const relativePath = `e2e-restored-category-${index + 1}`;
    const destination = `/data/${relativePath}`;
    fs.mkdirSync(path.join("/weaver-data", relativePath), { recursive: true });
    const label = await input.getAttribute("aria-label");
    expect(label, "a category destination field has no label").toBeTruthy();
    await chooseFolder(page, input, label!, destination);
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

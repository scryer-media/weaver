// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import fs from "node:fs";
import path from "node:path";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectHttpErrors } from "./support/http-errors";
import { byTestIdPrefix, choose, saveSettings, tableRow } from "./support/settings-locators";
import { settingsIds, testIds } from "./support/testids";
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

const field = testIds.field;
const fixtureServer = "nntp";

const signInButton = (page: Page) => page.getByTestId(settingsIds.loginSubmit);
/** The settings report line, where each backup step says how it went. */
const statusLine = (page: Page) => page.getByTestId(testIds.shellStatus);

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

/** A schedule row on the Downloads track. */
function downloadsSchedule(page: Page, label: string): Locator {
  return tableRow(page, "schedules", label, page.getByTestId(testIds.rowGroup("schedules", "DOWNLOADS")));
}

/** Choose a server folder through the directory dialog a path field opens. */
async function chooseFolder(page: Page, pathInput: Locator, folder: string) {
  await pathInput.click();
  const dialog = page.getByTestId(testIds.dialog("picker"));
  // The dialog opens on a listing of its own; typing before it lands would be
  // overwritten by it.
  const useFolder = dialog.getByTestId(testIds.pickerUse);
  await expect(useFolder).toBeEnabled();
  await dialog.getByTestId(testIds.pickerPathInput).fill(folder);
  await dialog.getByTestId(testIds.pickerGo).click();
  // It chooses the folder it is showing, so let the browse land first.
  await expect(dialog.getByTestId(testIds.pickerPath)).toHaveAttribute("title", folder);
  await useFolder.click();
  await expect(dialog).toBeHidden();
  await expect(pathInput).toHaveValue(folder);
}

const restoreButton = (page: Page) =>
  page.getByTestId(testIds.section("restore")).getByTestId(settingsIds.backupRestore);

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
  const exportSection = page.getByTestId(testIds.section("export"));
  const exportPassword = exportSection.getByTestId(field("password"));
  const exportPasswordConfirm = exportSection.getByTestId(field("passwordConfirm"));
  const downloadButton = page.getByTestId(settingsIds.backupDownload);
  await exportPassword.fill(backupPassphrase);
  await expect(exportPassword).toHaveAttribute("type", "password");
  await expect(exportPasswordConfirm).toHaveAttribute("type", "password");

  // A mismatched confirmation must block the export before any request fires.
  // The confirmation's help line appears only to say the two differ.
  const mismatch = exportSection.getByTestId(testIds.fieldHelp("passwordConfirm"));
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
  await expect(statusLine(page)).toContainText(/Saved weaver_backup_\S+\.enc/);

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
  await expect(statusLine(page)).toContainText(/password|decrypt|invalid/i);
  await expect(restoreButton(page)).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  expectHttpErrors(page, {
    method: "POST",
    pathname: "/api/backup/inspect",
    status: 400,
  });
  await analyzeBackup(page, tamperedBackupPath, backupPassphrase);
  await expect(statusLine(page)).toContainText(
    /corrupt|decrypt|integrity|invalid|password/i,
  );
  await expect(restoreButton(page)).toBeDisabled();
  await expectProductMarkers(page, matrix, false);

  await analyzeBackup(page, backupPath, backupPassphrase);
  const preview = page.getByTestId(testIds.section("manifest"));
  await expect(preview).toBeVisible();
  await expect(preview.getByTestId(settingsIds.backupSourceEngine)).toHaveText(matrix.sourceDatastore);
  await fillRequiredCategoryRemaps(page);

  await expect(restoreButton(page)).toBeEnabled();
  await restoreButton(page).click();
  const confirmation = page.getByTestId(testIds.dialog("backup-restore"));
  await confirmation.getByTestId(testIds.confirmAccept).click();
  await expect(confirmation).toBeHidden();
  await expect(statusLine(page)).toContainText(
    /Restore staged · \d+ downloads? in history · restart weaver to apply it/,
  );
  // The staged-restore section exists only while a restore waits for a restart.
  await expect(page.getByTestId(testIds.section("pending"))).toBeVisible();

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
  await expect(page.getByTestId(testIds.section("pending"))).toHaveCount(0);
  await expect(
    page.getByTestId(testIds.section("export")).getByTestId(field("password")),
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
  await page.getByTestId(testIds.queuePauseAll).click();
  await expect(page.getByTestId(testIds.queueResumeAll)).toBeVisible();
  const result = await makeRestoreTargetNonPristine(
    request,
    `weaver-restore-blocked-${matrix.sourceDatastore}-to-${matrix.targetDatastore}`,
  );
  expect(result).toMatchObject({ accepted: true });

  await expectProductMarkers(page, matrix, false);
  await analyzeBackup(page, backupPath, backupPassphrase);
  await expect(page.getByTestId(testIds.section("manifest"))).toBeVisible();
  await expect(page.getByTestId(testIds.section("restore")).getByTestId(settingsIds.backupStatus)).toContainText(
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
  await page.goto("/settings/networking/egress");
  const ceiling = page.getByTestId(field("maxDownloadSpeed"));
  await ceiling.fill(downloadCeiling(matrix));
  await ceiling.press("Tab");
  await saveSettings(page);

  await page.goto("/settings/categories");
  await page.getByTestId(testIds.tableAdd("categories")).click();
  const categoryForm = page.getByTestId(testIds.dialog("category"));
  await categoryForm.getByTestId(field("name")).fill(matrix.categoryName);
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.categorySourcePath)),
    { recursive: true },
  );
  await chooseFolder(page, categoryForm.getByTestId(field("destDir")), matrix.categorySourcePath);
  await categoryForm.getByTestId(field("aliases")).fill(matrix.categoryPattern);
  await categoryForm.getByTestId(testIds.editorSave).click();
  await expect(categoryForm).toBeHidden();
  await expect(
    tableRow(page, "categories", matrix.categoryName).getByText(matrix.categoryPattern, { exact: true }),
  ).toBeVisible();

  await page.goto("/settings/schedules");
  await page.getByTestId(testIds.tableAdd("schedules")).click();
  const scheduleForm = page.getByTestId(testIds.dialog("schedule"));
  await scheduleForm.getByTestId(field("time")).fill("03:15");
  await scheduleForm.getByTestId(field("label")).fill(matrix.scheduleName);
  await scheduleForm.getByTestId(field("enabled")).click();
  await scheduleForm.getByTestId(testIds.editorSave).click();
  await expect(scheduleForm).toBeHidden();
  await expect(downloadsSchedule(page, matrix.scheduleName)).toBeVisible();

  await page.goto("/settings/servers");
  await page.getByTestId(testIds.tableAdd("servers")).click();
  const serverForm = page.getByTestId(testIds.dialog("provider"));
  await serverForm.getByTestId(field("host")).fill(matrix.serverHost);
  const tls = serverForm.getByTestId(field("tls"));
  if (await tls.isChecked()) await tls.click();
  await expect(serverForm.getByTestId(field("port"))).toHaveValue("119");
  await serverForm.getByTestId(field("username")).fill("e2e-user");
  await serverForm.getByTestId(field("password")).fill(serverPassword);
  const connections = serverForm.getByTestId(field("connections"));
  await connections.fill("1");
  await connections.press("Tab");
  const serverEnabled = serverForm.getByTestId(field("active"));
  if (await serverEnabled.isChecked()) await serverEnabled.click();
  await serverForm.getByTestId(testIds.editorSave).click();
  await expect(serverForm).toBeHidden();
  await expect(tableRow(page, "servers", matrix.serverHost)).toBeVisible();

  await page.goto("/settings/rss");
  await page.getByTestId(testIds.tableAdd("feeds")).click();
  const feedForm = page.getByTestId(testIds.dialog("feed"));
  await feedForm.getByTestId(field("name")).fill(matrix.rssFeedName);
  await feedForm.getByTestId(field("url")).fill("http://127.0.0.1:1/e2e-backup.xml");
  await feedForm.getByTestId(testIds.editorSave).click();
  await expect(feedForm).toBeHidden();
  await expect(tableRow(page, "feeds", matrix.rssFeedName)).toBeVisible();
  await page.getByTestId(testIds.tableAdd("rules")).click();
  const ruleForm = page.getByTestId(testIds.dialog("rss-rule"));
  await ruleForm.getByTestId(field("titleRegex")).fill(matrix.rssRuleTerm);
  await ruleForm.getByTestId(testIds.editorSave).click();
  await expect(ruleForm).toBeHidden();
  await expect(
    tableRow(page, "rules", matrix.rssRuleTerm).getByTestId(settingsIds.rssRuleAction("ACCEPT")),
  ).toBeVisible();

  await page.goto("/settings/watch-folder");
  const watchSection = page.getByTestId(testIds.section("folder"));
  fs.mkdirSync(
    path.join("/weaver-data", path.basename(matrix.watchPath)),
    { recursive: true },
  );
  await chooseFolder(page, watchSection.getByTestId(field("path")), matrix.watchPath);
  await watchSection.getByTestId(testIds.option(field("mode"), "polling")).click();
  const pollInterval = watchSection.getByTestId(field("pollIntervalSecs"));
  await pollInterval.fill("45");
  await pollInterval.press("Tab");
  await saveSettings(page);

  await seedRestorableHistoryMetadata(request, matrix.historyName);
  await page.goto("/history");
  await expect(historyRow(page, matrix.historyName)).toBeVisible();

  await page.goto("/settings/security");
  await page.getByTestId(testIds.tableAdd("keys")).click();
  const keyEditor = page.getByTestId(testIds.dialog("key-new"));
  await keyEditor.getByTestId(field("name")).fill(matrix.apiKeyName);
  await choose(page, keyEditor, field("scope"), "READ");
  await keyEditor.getByTestId(testIds.editorSave).click();
  const createdDialog = page.getByTestId(testIds.dialog("key-created"));
  await expect(createdDialog).toContainText(matrix.apiKeyName);
  await expect(createdDialog.getByTestId(settingsIds.keyCreatedValue)).not.toHaveValue("");
  await createdDialog.getByTestId(settingsIds.keyCreatedDone).click();
  await expect(createdDialog).toBeHidden();
  await expect(apiKey(page, matrix)).toBeVisible();

  await page.getByTestId(settingsIds.loginSetup).click();
  const setup = page.getByTestId(testIds.dialog("login-setup"));
  await setup.getByTestId(field("username")).fill(loginUsername);
  await setup.getByTestId(field("password")).fill(loginPassword);
  await setup.getByTestId(field("confirm")).fill(loginPassword);
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
  await setup.getByTestId(testIds.editorSave).click();
  await expect(signInButton(page)).toBeVisible();
  await page.goto("/");
  await expect(signInButton(page)).toBeVisible();
  await signIn(page);
}

function downloadCeiling(matrix: BackupMatrixState): string {
  return String(matrix.speedLimit / (1024 * 1024));
}

function historyRow(page: Page, name: string): Locator {
  return byTestIdPrefix(page, testIds.historyRowPrefix).filter({ has: page.getByText(name, { exact: true }) });
}

function apiKey(page: Page, matrix: BackupMatrixState): Locator {
  return page.getByTestId(testIds.section("keys")).getByText(matrix.apiKeyName, { exact: true });
}

async function expectProductMarkers(
  page: Page,
  matrix: BackupMatrixState,
  present: boolean,
) {
  await page.goto("/settings/networking/egress");
  const ceiling = page.getByTestId(field("maxDownloadSpeed"));
  await expect(ceiling).toBeVisible();
  if (present) {
    await expect(ceiling).toHaveValue(downloadCeiling(matrix));
  } else {
    await expect(ceiling).not.toHaveValue(downloadCeiling(matrix));
  }

  await page.goto("/settings/categories");
  await expect(page.getByTestId(testIds.section("categories"))).toBeVisible();
  const categoryRow = tableRow(page, "categories", matrix.categoryName);
  await expect(categoryRow).toHaveCount(present ? 1 : 0);
  if (present) await expect(categoryRow).toContainText(matrix.categoryPattern);

  await page.goto("/settings/schedules");
  // The list heads only the groups that hold a rule, so the list itself is what loads.
  await expect(page.getByTestId(testIds.section("schedules"))).toBeVisible();
  await expect(downloadsSchedule(page, matrix.scheduleName)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/servers");
  await expect(tableRow(page, "servers", fixtureServer)).toBeVisible();
  const serverRow = tableRow(page, "servers", matrix.serverHost);
  await expect(serverRow).toHaveCount(present ? 1 : 0);
  if (present) {
    await serverRow.click();
    const serverForm = page.getByTestId(testIds.dialog("provider"));
    await expect(serverForm).toContainText(matrix.serverHost);
    // The stored password never comes back: the field is empty and shows the mask.
    const password = serverForm.getByTestId(field("password"));
    await expect(password).toHaveValue("");
    await expect(password).toHaveAttribute("placeholder", "••••••••");
    await serverForm.getByTestId(testIds.editorCancel).click();
    await expect(serverForm).toBeHidden();
  }

  await page.goto("/settings/rss");
  await expect(page.getByTestId(testIds.section("feeds"))).toBeVisible();
  await expect(tableRow(page, "feeds", matrix.rssFeedName)).toHaveCount(present ? 1 : 0);
  await expect(tableRow(page, "rules", matrix.rssRuleTerm)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/watch-folder");
  const watchSection = page.getByTestId(testIds.section("folder"));
  const watchPath = watchSection.getByTestId(field("path"));
  await expect(watchPath).toBeVisible();
  if (present) {
    await expect(watchPath).toHaveValue(matrix.watchPath);
    await expect(
      watchSection.getByTestId(field("pollIntervalSecs")),
    ).toHaveValue("45");
  } else {
    await expect(watchPath).not.toHaveValue(matrix.watchPath);
  }

  await page.goto("/history");
  await expect(page.getByRole("main")).toBeVisible();
  await expect(historyRow(page, matrix.historyName)).toHaveCount(present ? 1 : 0);

  await page.goto("/settings/security");
  await expect(page.getByTestId(testIds.section("keys"))).toBeVisible();
  await expect(apiKey(page, matrix)).toHaveCount(present ? 1 : 0);
}

async function analyzeBackup(page: Page, file: string, password: string) {
  await page.goto("/settings/backup");
  const restoreSection = page.getByTestId(testIds.section("restore"));
  // Wait for the restore availability check before reading anything.
  await expect(restoreSection.getByTestId(settingsIds.backupStatus)).toBeVisible();
  const chooser = page.waitForEvent("filechooser");
  await restoreSection.getByTestId(settingsIds.backupChooseFile).click();
  await (await chooser).setFiles(file);
  await expect(restoreSection.getByTestId(settingsIds.backupFileName)).toHaveText(path.basename(file));
  const restorePassword = restoreSection.getByTestId(field("restorePassword"));
  await restorePassword.fill(password);
  await expect(restorePassword).toHaveAttribute("type", "password");
  const inspected = page.waitForResponse(
    (response) => new URL(response.url()).pathname.endsWith("/api/backup/inspect"),
  );
  const readArchive = restoreSection.getByTestId(settingsIds.backupReadArchive);
  await readArchive.click();
  await inspected;
  // The panel is busy while it reads the archive; the button comes back once
  // the read has settled and the report line says how it went.
  await expect(readArchive).toBeEnabled();
}

async function fillRequiredCategoryRemaps(page: Page) {
  const panel = page.getByTestId(testIds.section("remaps"));
  await expect(panel).toBeVisible();
  const remapInputs = panel.getByTestId(settingsIds.backupRemap);
  await expect(remapInputs).not.toHaveCount(0);
  const inputs = await remapInputs.all();
  for (const [index, input] of inputs.entries()) {
    // /data/complete is a separate volume this container cannot write, and
    // the directory picker only chooses folders that exist.
    const relativePath = `e2e-restored-category-${index + 1}`;
    const destination = `/data/${relativePath}`;
    fs.mkdirSync(path.join("/weaver-data", relativePath), { recursive: true });
    await chooseFolder(page, input, destination);
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

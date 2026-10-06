import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, test } from "./helpers";
import { execute, literal, setting } from "./support/datastore";
import { readClock, setClock } from "./support/e2e-clock";
import { graphqlErrors, stage } from "./support/network-flow";
import { loadStageState, saveStageState } from "./support/script-settings";

/**
 * Automatic backups (checkpoint section 8, B01-B06). The daily scheduler
 * reads Weaver's e2e clock, so each test moves the clock across its backup
 * time and waits for the backup the crossing produces.
 *
 * B03 runs only in the DST phase (TZ=America/Denver). B05 spans the restarts:
 * the initial stage leaves automatic backups on with an older recorded
 * version, the restarted stage checks the pre-migration backup, and the
 * restarted-again stage checks that an absent key skips it.
 */

const AUTO_KEY = "e2e-automatic-backup-key";

type AutoSettings = { enabled: boolean; dailyTimeLocal: string; autoBackupKeyPresent: boolean; nextRunAt: string | null };
type Backup = {
  filename: string; trigger: "MANUAL" | "AUTO"; status: "CREATING" | "READY" | "FAILED";
  encrypted: boolean; sourceWeaverVersion: string; error: string | null;
};

const AUTO_FIELDS = "enabled dailyTimeLocal autoBackupKeyPresent nextRunAt";

function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

async function autoSettings(request: APIRequestContext): Promise<AutoSettings> {
  return (await graphql<{ autoBackupSettings: AutoSettings }>(request, `query { autoBackupSettings { ${AUTO_FIELDS} } }`)).autoBackupSettings;
}

async function saveAuto(
  request: APIRequestContext,
  input: { enabled: boolean; dailyTimeLocal: string; setAutoBackupKey?: string; clearAutoBackupKey?: boolean },
): Promise<AutoSettings> {
  return (await graphql<{ updateAutoBackupSettings: AutoSettings }>(request,
    `mutation($input: AutoBackupSettingsInput!) { updateAutoBackupSettings(input: $input) { ${AUTO_FIELDS} } }`,
    { input })).updateAutoBackupSettings;
}

async function saveAutoErrors(request: APIRequestContext, input: Record<string, unknown>): Promise<string> {
  return (await graphqlErrors(request,
    `mutation($input: AutoBackupSettingsInput!) { updateAutoBackupSettings(input: $input) { ${AUTO_FIELDS} } }`,
    { input })).join("\n");
}

async function backups(request: APIRequestContext): Promise<Backup[]> {
  return (await graphql<{ backups: Backup[] }>(request,
    "query { backups { filename trigger status encrypted sourceWeaverVersion error } }")).backups;
}

/** Turn automatic backups off and drop the key, whatever state a test left. */
async function disableAuto(request: APIRequestContext): Promise<void> {
  const current = await autoSettings(request);
  await saveAuto(request, { enabled: false, dailyTimeLocal: current.dailyTimeLocal, clearAutoBackupKey: current.autoBackupKeyPresent });
}

/** Midnight UTC a few days past the clock, so each test starts on a fresh day. */
function freshDay(): Date {
  const now = readClock();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 3));
}

const at = (day: Date, hours: number, minutes: number) => new Date(day.getTime() + (hours * 60 + minutes) * 60_000);
const instant = (value: string | null) => (value === null ? null : new Date(value).getTime());

/** Wait for one automatic backup beyond `before` to finish; returns it. */
async function nextAutoBackup(request: APIRequestContext, before: Set<string>, describe: string): Promise<{ backup: Backup; seen: string[] }> {
  const seen = new Set<string>();
  let created: Backup[] = [];
  await expect.poll(async () => {
    created = (await backups(request)).filter(backup => !before.has(backup.filename) && backup.trigger === "AUTO");
    for (const backup of created) seen.add(backup.status);
    return created.length === 1 && created[0]!.status !== "CREATING";
  }, { message: describe, timeout: 0 }).toBe(true);
  return { backup: created[0]!, seen: [...seen] };
}

const initialOnly = () => test.skip(stage() !== "initial", "runs in the initial stage");

test("B02 enabling without a key is refused and no backup runs", async ({ request }) => {
  initialOnly();
  note("discrepancy", "The checkpoint expects the save to succeed and simply produce no backup; the product refuses to enable automatic backups without a key, so nothing is ever scheduled.");
  await disableAuto(request);
  const day = freshDay();
  setClock(at(day, 2, 58));
  const before = new Set((await backups(request)).map(backup => backup.filename));
  expect(await saveAutoErrors(request, { enabled: true, dailyTimeLocal: "03:00" }))
    .toContain("an automatic backup key is required before enabling automatic backups");
  expect(await autoSettings(request)).toMatchObject({ enabled: false, autoBackupKeyPresent: false, nextRunAt: null });
  setClock(at(day, 3, 1));
  expect((await backups(request)).filter(backup => !before.has(backup.filename))).toEqual([]);
});

test("B01 an enabled daily backup runs at its local time, encrypted, and schedules the next day", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 2, 59));
  const before = new Set((await backups(request)).map(backup => backup.filename));
  try {
    const saved = await saveAuto(request, { enabled: true, dailyTimeLocal: "03:00", setAutoBackupKey: AUTO_KEY });
    expect(saved).toMatchObject({ enabled: true, dailyTimeLocal: "03:00", autoBackupKeyPresent: true });
    expect(instant(saved.nextRunAt)).toBe(at(day, 3, 0).getTime());

    setClock(at(day, 3, 0));
    const { backup, seen } = await nextAutoBackup(request, before, "B01 automatic backup finished");
    note("observed", `statuses seen while polling: ${seen.join(", ")} (CREATING is transient; nothing holds a backup in it)`);
    expect(backup).toMatchObject({ trigger: "AUTO", status: "READY", encrypted: true, error: null });
    expect(seen.every(status => status === "CREATING" || status === "READY")).toBe(true);
    await expect.poll(async () => instant((await autoSettings(request)).nextRunAt),
      { message: "B01 next run moved to the following day", timeout: 0 }).toBe(at(day, 24 + 3, 0).getTime());
    expect((await backups(request)).filter(entry => !before.has(entry.filename) && entry.trigger === "AUTO")).toHaveLength(1);
  } finally {
    await disableAuto(request);
  }
});

test("B03 a backup time inside the spring-forward gap runs at the next valid instant @dst", async ({ request }) => {
  initialOnly();
  // 2032-03-14 is the second Sunday of March: America/Denver jumps from
  // 02:00 MST to 03:00 MDT, so 02:30 local does not exist that day.
  setClock("2032-03-14T08:30:00Z"); // 01:30 MST
  const before = new Set((await backups(request)).map(backup => backup.filename));
  try {
    const saved = await saveAuto(request, { enabled: true, dailyTimeLocal: "02:30", setAutoBackupKey: AUTO_KEY });
    const next = instant(saved.nextRunAt);
    // 03:00 MDT, the first valid local instant after the gap.
    expect(next).toBe(Date.parse("2032-03-14T09:00:00Z"));
    expect(next! - Date.parse("2032-03-14T08:30:00Z")).toBeLessThanOrEqual(180 * 60_000);

    setClock("2032-03-14T09:30:00Z"); // 03:30 MDT
    const { backup } = await nextAutoBackup(request, before, "B03 automatic backup after the gap");
    expect(backup).toMatchObject({ trigger: "AUTO", status: "READY", encrypted: true });
    // 02:30 MDT the following day.
    await expect.poll(async () => instant((await autoSettings(request)).nextRunAt),
      { message: "B03 next run on the following day", timeout: 0 }).toBe(Date.parse("2032-03-15T08:30:00Z"));
    expect((await backups(request)).filter(entry => !before.has(entry.filename) && entry.trigger === "AUTO")).toHaveLength(1);
  } finally {
    await disableAuto(request);
  }
});

test.fixme("B04 a backup still running when the next one is due is not doubled", () => {
  // Missing observable: nothing holds a backup in CREATING (no e2e hook), and
  // manual and automatic backups take separate locks, so a held manual backup
  // would not block an automatic run anyway. The scheduler skips a due run
  // only while its own previous automatic run is still active.
});

test("B06 clearing the automatic key turns automatic backups off", async ({ request }) => {
  initialOnly();
  note("gap", "`--reset-automatic-backup-settings` is a startup flag (it also requires `--skip-upgrade-backup`); the harness starts Weaver with a fixed command, so only clearAutoBackupKey is driven here.");
  const day = freshDay();
  setClock(at(day, 1, 0));
  try {
    await saveAuto(request, { enabled: true, dailyTimeLocal: "03:00", setAutoBackupKey: AUTO_KEY });
    expect(await saveAutoErrors(request, { enabled: true, dailyTimeLocal: "03:00", clearAutoBackupKey: true }))
      .toContain("automatic backup key cannot be cleared while automatic backups are enabled");
    expect(await saveAutoErrors(request, { enabled: false, dailyTimeLocal: "03:00", setAutoBackupKey: AUTO_KEY, clearAutoBackupKey: true }))
      .toContain("choose either setting or clearing the automatic backup key");
    const cleared = await saveAuto(request, { enabled: false, dailyTimeLocal: "03:00", clearAutoBackupKey: true });
    expect(cleared).toEqual({ enabled: false, dailyTimeLocal: "03:00", autoBackupKeyPresent: false, nextRunAt: null });
    expect(await autoSettings(request)).toEqual(cleared);
  } finally {
    await disableAuto(request);
  }
});

/** A version older than any Weaver this flow runs. */
const OLD_VERSION = "0.1.0";

async function markOlderVersion(): Promise<void> {
  await execute(`UPDATE settings SET value = ${literal(OLD_VERSION)} WHERE key = 'last_started_version'`);
  expect(await setting("last_started_version")).toBe(OLD_VERSION);
}

test("B05 an upgrade takes a pre-migration backup only while the automatic key is set", async ({ request }) => {
  note("gap", "`--skip-upgrade-backup` is a startup flag the harness cannot pass; the key-absent path, which also leaves no backup, is driven instead.");
  const premigration = async () => (await backups(request)).filter(backup => backup.sourceWeaverVersion === OLD_VERSION);

  if (stage() === "initial") {
    // Deliberately left on: the restart must find the key and an older version.
    // The scheduling spec runs next and moves the clock across 03:00 several
    // times, so it also sees automatic backups run; none of them is labelled
    // with OLD_VERSION, so they cannot satisfy the restarted check.
    note("observed", "Automatic backups stay enabled through the rest of the initial stage, so later clock moves also produce daily AUTO backups.");
    await saveAuto(request, { enabled: true, dailyTimeLocal: "03:00", setAutoBackupKey: AUTO_KEY });
    expect(await setting("last_started_version")).toBeTruthy();
    await markOlderVersion();
    saveStageState("b05", { before: (await premigration()).map(backup => backup.filename) });
    return;
  }

  if (stage() === "restarted") {
    const { before } = loadStageState<{ before: string[] }>("b05");
    const created = (await premigration()).filter(backup => !before.includes(backup.filename));
    expect(created, "one pre-migration backup labelled with the version it was taken from").toHaveLength(1);
    expect(created[0]).toMatchObject({ trigger: "AUTO", status: "READY", encrypted: true });
    expect(await setting("pending_auto_backup_version") ?? "").toBe("");
    const recorded = await setting("last_started_version");
    expect(recorded).toBeTruthy();
    expect(recorded).not.toBe(OLD_VERSION);

    // Without the key the next upgrade must not attempt a backup.
    await saveAuto(request, { enabled: false, dailyTimeLocal: "03:00", clearAutoBackupKey: true });
    await markOlderVersion();
    saveStageState("b05", { before: (await premigration()).map(backup => backup.filename) });
    return;
  }

  const { before } = loadStageState<{ before: string[] }>("b05");
  expect((await premigration()).map(backup => backup.filename).sort()).toEqual([...before].sort());
  expect(await setting("pending_auto_backup_version") ?? "").toBe("");
  const recorded = await setting("last_started_version");
  expect(recorded).toBeTruthy();
  expect(recorded).not.toBe(OLD_VERSION);
});

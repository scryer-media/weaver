import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import {
  configuredServer, expect, graphql, nntpBodyMetrics, nntpConnectionMetrics, postProbeArticle,
  resetNntpMetrics, submitProbeNzb, test,
} from "./helpers";
import { literal, setting, waitRows } from "./support/datastore";
import { jobState, startDownload, waitTerminal } from "./support/downloads";
import { readClock, setClock } from "./support/e2e-clock";
import { graphqlErrors, stage } from "./support/network-flow";
import { loadStageState, nzbDocument, nzbgetRpc, saveStageState, withControlKey } from "./support/script-settings";

/**
 * Schedule rules (T01-T16). Weaver reads the e2e clock
 * on every evaluator tick, so each test moves the clock across its rule times
 * and waits for the effect the crossing produces.
 *
 * Every negative ("still paused", "not fetched", "not consumed") is asserted
 * only after `witnessTick`, which proves the evaluator ran a full tick that
 * saw the clock already moved. Until then such an assertion could only pass
 * falsely, never fail falsely.
 *
 * Hold rules apply as soon as they are created, through their occurrence on
 * the previous day, so the rule that should not be in force yet is always
 * created first: the later rule of a pair wins on the previous day too.
 *
 * T13 spans the Weaver restart. Every other test runs in the initial stage.
 */

const RSS_FIXTURE = "http://rss-fixture:8089";
const WATCH_ROOT = "/watch-folder";
const DAY_MS = 24 * 60 * 60_000;
const WEEKDAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;
const token = Date.now().toString(36);

function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

const initialOnly = () => test.skip(stage() !== "initial", "runs in the initial stage");

// ---------------------------------------------------------------------------
// Clock

/** Midnight UTC at least two days past the clock, optionally on a weekday. */
function freshDay(weekday?: (typeof WEEKDAYS)[number]): Date {
  const now = readClock();
  let day = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 2);
  while (weekday !== undefined && WEEKDAYS[new Date(day).getUTCDay()] !== weekday) day += DAY_MS;
  return new Date(day);
}

const at = (day: Date, hours: number, minutes: number) => new Date(day.getTime() + (hours * 60 + minutes) * 60_000);
const hhmm = (instant: Date) => instant.toISOString().slice(11, 16);

// ---------------------------------------------------------------------------
// Rules

type ScheduleRow = {
  id: string; label: string; enabled: boolean; implicit: boolean; days: string[]; time: string; times: string[];
  everyHourAtMinute: number | null; actionType: string; track: string; feedId: number | null;
  serverId: number | null; serverActive: boolean | null; speedLimitBytes: number | null; hardwareProfile: string | null;
};
type RuleInput = Record<string, unknown> & { actionType: string; time: string };

const SCHEDULE_FIELDS = "id label enabled implicit days time times everyHourAtMinute actionType track feedId serverId serverActive speedLimitBytes hardwareProfile";

async function schedules(request: APIRequestContext): Promise<ScheduleRow[]> {
  return (await graphql<{ schedules: ScheduleRow[] }>(request, `query { schedules { ${SCHEDULE_FIELDS} } }`)).schedules;
}

/** Rules a test creates, all removed again when it ends. */
class Rules {
  private readonly ids = new Set<string>();
  private serial = 0;

  constructor(private readonly request: APIRequestContext, private readonly tag: string) {}

  label(): string {
    this.serial += 1;
    return `${this.tag}-${this.serial}-${token}`;
  }

  async create(input: RuleInput): Promise<ScheduleRow> {
    const label = (input.label as string | undefined) ?? this.label();
    const rows = (await graphql<{ createSchedule: ScheduleRow[] }>(this.request,
      `mutation($input: ScheduleInput!) { createSchedule(input: $input) { ${SCHEDULE_FIELDS} } }`,
      { input: { enabled: true, days: [], ...input, label } })).createSchedule;
    const created = rows.filter(row => row.label === label);
    expect(created, `created rule ${label}`).toHaveLength(1);
    this.ids.add(created[0]!.id);
    return created[0]!;
  }

  async update(id: string, input: RuleInput): Promise<ScheduleRow[]> {
    return (await graphql<{ updateSchedule: ScheduleRow[] }>(this.request,
      `mutation($id: String!, $input: ScheduleInput!) { updateSchedule(id: $id, input: $input) { ${SCHEDULE_FIELDS} } }`,
      { id, input: { enabled: true, days: [], ...input } })).updateSchedule;
  }

  async toggle(id: string, enabled: boolean): Promise<ScheduleRow[]> {
    return (await graphql<{ toggleSchedule: ScheduleRow[] }>(this.request,
      `mutation($id: String!, $enabled: Boolean!) { toggleSchedule(id: $id, enabled: $enabled) { ${SCHEDULE_FIELDS} } }`,
      { id, enabled })).toggleSchedule;
  }

  async delete(id: string): Promise<ScheduleRow[]> {
    const rows = (await graphql<{ deleteSchedule: ScheduleRow[] }>(this.request,
      `mutation($id: String!) { deleteSchedule(id: $id) { ${SCHEDULE_FIELDS} } }`, { id })).deleteSchedule;
    this.ids.delete(id);
    return rows;
  }

  async clear(): Promise<void> {
    for (const id of [...this.ids]) await this.delete(id);
  }
}

/** Run `body` with a rule tracker whose rules are removed afterwards. */
async function withRules(request: APIRequestContext, tag: string, body: (rules: Rules) => Promise<void>): Promise<void> {
  const rules = new Rules(request, tag);
  try {
    await body(rules);
  } finally {
    await rules.clear();
  }
}

let witnessSerial = 0;

/**
 * Prove a whole evaluator tick ran after this call began. A speed rule with a
 * value nothing else uses must come into force, then the same rule edited to
 * the configured limit must too. The second change needs a later tick than
 * the first, and that tick read the rules (so the clock) after the clock was
 * last moved; every track, the Speed track included, has been processed by
 * the tick that published the first value.
 *
 * Not for tests that schedule speed rules of their own.
 */
async function witnessTick(request: APIRequestContext): Promise<void> {
  witnessSerial += 1;
  const marker = 2_000_000_000 + witnessSerial * 1_024 + (Date.now() % 1_000);
  const rules = new Rules(request, "witness");
  try {
    const rule = await rules.create({ actionType: "speed_limit", time: hhmm(readClock()), speedLimitBytes: marker });
    await expect.poll(async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit,
      { message: `witness ${marker} in force`, timeout: 0 }).toBe(marker);
    await rules.update(rule.id, { actionType: "configured_speed_limit", time: rule.time, label: rule.label });
    await expect.poll(async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit,
      { message: `witness ${marker} lifted`, timeout: 0 }).toBe(0);
  } finally {
    await rules.clear();
  }
}

// ---------------------------------------------------------------------------
// Queue, settings and jobs

type QueueState = {
  isPaused: boolean;
  speedLimitBytesPerSec: number;
  downloadBlock: { kind: string; usedBytes: number; scheduledSpeedLimit: number; scheduleHoldReason: string | null };
};

async function queueState(request: APIRequestContext): Promise<QueueState> {
  return (await graphql<{ globalQueueState: QueueState }>(request,
    "query { globalQueueState { isPaused speedLimitBytesPerSec downloadBlock { kind usedBytes scheduledSpeedLimit scheduleHoldReason } } }")).globalQueueState;
}

async function waitQueue(request: APIRequestContext, predicate: (state: QueueState) => boolean, message: string): Promise<QueueState> {
  let state: QueueState | undefined;
  await expect.poll(async () => predicate(state = await queueState(request)), { message, timeout: 0 }).toBe(true);
  return state!;
}

const waitPaused = (request: APIRequestContext, paused: boolean, message: string) =>
  waitQueue(request, state => state.isPaused === paused, message);

async function resumeAll(request: APIRequestContext): Promise<void> {
  await graphql(request, "mutation { resumeAll }");
}

type WatchFolder = {
  mode: string; path: string | null; pollIntervalSecs: number; stabilitySecs: number;
  categoryFromSubfolders: boolean; scanningPaused: boolean;
};

async function generalSettings(request: APIRequestContext): Promise<{ maxDownloadSpeed: number; watchFolder: WatchFolder }> {
  return (await graphql<{ settings: { maxDownloadSpeed: number; watchFolder: WatchFolder } }>(request,
    "query { settings { maxDownloadSpeed watchFolder { mode path pollIntervalSecs stabilitySecs categoryFromSubfolders scanningPaused } } }")).settings;
}

async function updateSettings(request: APIRequestContext, input: Record<string, unknown>): Promise<void> {
  await graphql(request, "mutation($input: GeneralSettingsInput!) { updateSettings(input: $input) { maxDownloadSpeed } }", { input });
}

async function watchFolderPaused(request: APIRequestContext): Promise<boolean> {
  return (await generalSettings(request)).watchFolder.scanningPaused;
}

/** A watch directory only this test uses, writable by Weaver. */
function watchDir(name: string): string {
  const directory = path.join(WATCH_ROOT, `scheduling-${name}-${token}`);
  fs.mkdirSync(directory, { recursive: true, mode: 0o777 });
  fs.chmodSync(directory, 0o777);
  return directory;
}

function dropFile(directory: string, name: string, contents: string): string {
  const target = path.join(directory, name);
  fs.writeFileSync(`${target}.part`, contents, { mode: 0o666 });
  fs.renameSync(`${target}.part`, target);
  return target;
}

/**
 * Poll `directory` once an hour, so only a scheduled scan or a resume can
 * consume what a test drops later, and wait for the first pass: it marks an
 * invalid sentinel file `.error`.
 */
async function armWatchFolder(request: APIRequestContext, directory: string): Promise<void> {
  const sentinel = dropFile(directory, "sentinel.nzb", "not an nzb\n");
  await updateSettings(request, { watchFolder: {
    mode: "polling", path: directory, pollIntervalSecs: 3600, stabilitySecs: 0,
    categoryFromSubfolders: false, scanningPaused: false,
  } });
  await expect.poll(() => fs.existsSync(`${sentinel}.error`), { message: `first watch pass over ${directory}`, timeout: 0 }).toBe(true);
}

async function disarmWatchFolder(request: APIRequestContext): Promise<void> {
  await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
}

/** Post one article and drop an NZB for it, returning the NZB's path. */
async function dropValidNzb(directory: string, name: string): Promise<string> {
  const article = { messageId: `${name}-${token}@e2e.invalid`, bytes: 16 * 1024 };
  await postProbeArticle(article.messageId, article.bytes);
  return dropFile(directory, `${name}.nzb`, nzbDocument(name, [article]));
}

async function addCountedFeed(request: APIRequestContext, key: string): Promise<number> {
  // Disabled, so only a scheduled fetch_rss reads it: the background poller
  // runs on wall-clock time and skips disabled feeds.
  return (await graphql<{ addRssFeed: { id: number } }>(request,
    "mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }",
    { input: { name: `counted ${key}`, url: `${RSS_FIXTURE}/counted/${key}.xml`, enabled: false } })).addRssFeed.id;
}

async function deleteFeed(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { deleteRssFeed(id: $id) }", { id });
}

async function feedCount(key: string): Promise<number> {
  const response = await fetch(`${RSS_FIXTURE}/count/${key}`);
  expect(response.ok, `count for ${key}`).toBe(true);
  return ((await response.json()) as { count: number }).count;
}

async function waitCounts(keys: Record<string, string>, expected: Record<string, number>, message: string): Promise<void> {
  let counts: Record<string, number> = {};
  await expect.poll(async () => {
    counts = {};
    for (const [name, key] of Object.entries(keys)) counts[name] = await feedCount(key);
    return Object.entries(expected).every(([name, count]) => counts[name]! >= count);
  }, { message, timeout: 0 }).toBe(true);
}

async function counts(keys: Record<string, string>): Promise<Record<string, number>> {
  const result: Record<string, number> = {};
  for (const [name, key] of Object.entries(keys)) result[name] = await feedCount(key);
  return result;
}

type QueueItem = { id: number; state: string; remainingFileCount: number; downloadedBytes: number };

async function queueItem(request: APIRequestContext, id: number): Promise<QueueItem | null> {
  return (await graphql<{ queueItem: QueueItem | null }>(request,
    "query($id: Int!) { queueItem(id: $id) { id state remainingFileCount downloadedBytes } }", { id })).queueItem;
}

async function historyItem(request: APIRequestContext, id: number): Promise<{ state: string; outputDir: string | null } | null> {
  return (await graphql<{ historyItem: { state: string; outputDir: string | null } | null }>(request,
    "query($id: Int!) { historyItem(id: $id) { state outputDir } }", { id })).historyItem;
}

/** BODY requests the provider served for articles whose id contains `prefix`. */
async function bodyCount(prefix: string, host = "nntp"): Promise<number> {
  const metrics = await nntpBodyMetrics(prefix, host);
  return Object.entries(metrics.body_counts ?? {})
    .filter(([messageId]) => messageId.includes(prefix))
    .reduce((sum, [, count]) => sum + count, 0);
}

async function completedJob(request: APIRequestContext, name: string, options: { nntpHost?: string } = {}): Promise<number> {
  const id = await startDownload(request, `${name}-${token}`, { count: 4, partBytes: 32 * 1024, ...options });
  expect(await waitTerminal(request, id), `${name} completes`).toBe("COMPLETED");
  return id;
}

/** A job none of whose articles exist on any provider. */
async function failedJob(request: APIRequestContext, name: string): Promise<number> {
  const articles = [{ messageId: `${name}-${token}-absent@e2e.invalid`, bytes: 16 * 1024 }];
  const result = await submitProbeNzb(request, `${name}-${token}`, articles);
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  expect(await waitTerminal(request, result.jobId!), `${name} fails`).toBe("FAILED");
  return result.jobId!;
}

async function cancelledJob(request: APIRequestContext, name: string): Promise<number> {
  await graphql(request, "mutation { pauseAll }");
  try {
    const articles = [{ messageId: `${name}-${token}-held@e2e.invalid`, bytes: 16 * 1024 }];
    await postProbeArticle(articles[0]!.messageId, articles[0]!.bytes);
    const result = await submitProbeNzb(request, `${name}-${token}`, articles);
    expect(result, `submit ${name}`).toMatchObject({ accepted: true });
    await graphql(request, "mutation($id: Int!) { cancelJob(id: $id) }", { id: result.jobId });
    await waitRows(`SELECT status FROM job_history WHERE job_id = ${literal(result.jobId!)}`,
      rows => rows[0]?.status === "cancelled", `${name} recorded as cancelled`);
    return result.jobId!;
  } finally {
    await resumeAll(request);
  }
}

const localOutput = (outputDir: string) => outputDir.replace(/^\/data\/complete/, "/weaver-downloads");

// ---------------------------------------------------------------------------
// Tests, in the order the evaluator state needs (T13 last).

test("T03 a legacy resume rule resumes downloads only", async ({ request }) => {
  initialOnly();
  const directory = watchDir("t03");
  await withRules(request, "t03", async rules => {
    try {
      await updateSettings(request, { watchFolder: {
        mode: "polling", path: directory, pollIntervalSecs: 3600, stabilitySecs: 0,
        categoryFromSubfolders: false, scanningPaused: true,
      } });
      expect(await watchFolderPaused(request)).toBe(true);
      await graphql(request, "mutation { pauseAll }");
      await waitPaused(request, true, "T03 manual pause");

      const rule = await rules.create({ actionType: "resume", time: hhmm(readClock()) });
      expect(rule.track).toBe("DOWNLOADS");
      await waitPaused(request, false, "T03 resume rule resumed downloads");
      await witnessTick(request);
      expect(await watchFolderPaused(request), "watch intake stays paused: no pause_all rule was ever seen").toBe(true);
      expect(await setting("schedule_pause_all_used")).toBeUndefined();
    } finally {
      await resumeAll(request);
      await disarmWatchFolder(request);
    }
  });
});

test("T01 pause and resume rules hold downloads for their window", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t01", async rules => {
    try {
      const resume = await rules.create({ actionType: "resume", time: "10:30" });
      const pause = await rules.create({ actionType: "pause", time: "10:00" });
      expect([resume.track, pause.track]).toEqual(["DOWNLOADS", "DOWNLOADS"]);
      await waitPaused(request, false, "T01 not paused before 10:00");

      setClock(at(day, 10, 0));
      const held = await waitQueue(request, state => state.isPaused && state.downloadBlock.kind === "SCHEDULED", "T01 paused at 10:00");
      expect(held.downloadBlock.kind).toBe("SCHEDULED");

      setClock(at(day, 10, 30));
      const released = await waitPaused(request, false, "T01 resumed at 10:30");
      expect(released.downloadBlock.kind).toBe("NONE");
    } finally {
      await resumeAll(request);
    }
  });
});

test("T02 pause_all holds downloads, watch intake and RSS until resume", async ({ request }) => {
  initialOnly();
  note("gap", "The RSS scheduled-pause flag has no observable of its own: a scheduled fetch inside the window must not reach the feed (asserted after a witnessed tick), and one after the resume must.");
  const directory = watchDir("t02");
  const feedKey = `t02-${token}`;
  const feedId = await addCountedFeed(request, feedKey);
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t02", async rules => {
    try {
      await armWatchFolder(request, directory);
      await rules.create({ actionType: "resume", time: "10:30" });
      const pauseAll = await rules.create({ actionType: "pause_all", time: "10:00" });
      expect(pauseAll.track).toBe("DOWNLOADS");
      await rules.create({ actionType: "fetch_rss", time: "10:10", feedId });
      await rules.create({ actionType: "fetch_rss", time: "10:31", feedId });
      await witnessTick(request);
      expect((await queueState(request)).isPaused).toBe(false);

      setClock(at(day, 10, 0));
      await waitPaused(request, true, "T02 downloads paused at 10:00");
      await expect.poll(() => watchFolderPaused(request), { message: "T02 watch intake paused", timeout: 0 }).toBe(true);

      setClock(at(day, 10, 10));
      await witnessTick(request);
      expect(await feedCount(feedKey), "the 10:10 fetch is skipped while RSS is paused").toBe(0);

      setClock(at(day, 10, 30));
      await waitPaused(request, false, "T02 downloads resumed at 10:30");
      await expect.poll(() => watchFolderPaused(request), { message: "T02 watch intake resumed", timeout: 0 }).toBe(false);

      setClock(at(day, 10, 31));
      let fetched = 0;
      await expect.poll(async () => (fetched = await feedCount(feedKey)) >= 1,
        { message: "T02 the 10:31 fetch reaches the feed", timeout: 0 }).toBe(true);
      expect(fetched).toBeGreaterThanOrEqual(1);
    } finally {
      await resumeAll(request);
      await disarmWatchFolder(request);
      await deleteFeed(request, feedId);
    }
  });
});

test("T04 post-processing pause defers completion until resume", async ({ request }) => {
  initialOnly();
  note("observed", "There is no API flag for a paused post-processing stage: the job finishes downloading and then stays in the queue until the resume rule fires.");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t04", async rules => {
    await rules.create({ actionType: "resume_post_processing", time: "10:30" });
    const pause = await rules.create({ actionType: "pause_post_processing", time: "10:00" });
    expect(pause.track).toBe("POST_PROCESSING");
    setClock(at(day, 10, 0));
    await witnessTick(request);

    const id = await startDownload(request, `t04-${token}`, { count: 4, partBytes: 32 * 1024 });
    let item: QueueItem | null = null;
    await expect.poll(async () => {
      item = await queueItem(request, id);
      return item !== null && item.remainingFileCount === 0;
    }, { message: "T04 download finished", timeout: 0 }).toBe(true);
    await witnessTick(request);
    expect(await queueItem(request, id), "still queued while post-processing is paused").not.toBeNull();
    expect(await jobState(request, id)).not.toBe("COMPLETED");

    setClock(at(day, 10, 30));
    expect(await waitTerminal(request, id)).toBe("COMPLETED");
  });
});

test("T05 watch-folder scanning pauses and resumes on schedule", async ({ request }) => {
  initialOnly();
  const directory = watchDir("t05");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t05", async rules => {
    try {
      await armWatchFolder(request, directory);
      const resume = await rules.create({ actionType: "resume_watch_folder_scanning", time: "10:30" });
      await rules.create({ actionType: "pause_watch_folder_scanning", time: "10:00" });
      expect(resume.track).toBe("WATCH_FOLDER");

      setClock(at(day, 10, 0));
      await expect.poll(() => watchFolderPaused(request), { message: "T05 paused at 10:00", timeout: 0 }).toBe(true);
      setClock(at(day, 10, 10));
      const dropped = await dropValidNzb(directory, "t05-dropped");
      setClock(at(day, 10, 29));
      await witnessTick(request);
      expect(fs.existsSync(dropped), "nothing consumes the file while scanning is paused").toBe(true);
      expect(fs.existsSync(`${dropped}.queued`)).toBe(false);

      setClock(at(day, 10, 30));
      await expect.poll(() => fs.existsSync(`${dropped}.queued`), { message: "T05 resume scans the folder", timeout: 0 }).toBe(true);
      expect(await watchFolderPaused(request)).toBe(false);
    } finally {
      await disarmWatchFolder(request);
    }
  });
});

test("T06 a scheduled speed limit binds for its window, then the configured limit returns", async ({ request }) => {
  initialOnly();
  note("observed", "The rate is the provider's BODY byte count over the job's wall time; per-leg samples are not exposed.");
  const limit = 1024 * 1024;
  const configured = (await generalSettings(request)).maxDownloadSpeed;
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t06", async rules => {
    await rules.create({ actionType: "configured_speed_limit", time: "10:30" });
    const speed = await rules.create({ actionType: "speed_limit", time: "10:00", speedLimitBytes: limit });
    expect(speed.track).toBe("SPEED");

    setClock(at(day, 10, 0));
    const limited = await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === limit, "T06 limit at 10:00");
    expect(limited.speedLimitBytesPerSec, "the configured limit is reported unchanged").toBe(configured);

    const partBytes = 512 * 1024;
    const name = `t06-${token}`;
    const articles = Array.from({ length: 6 }, (_, index) => ({ messageId: `${name}-${index}@e2e.invalid`, bytes: partBytes }));
    for (const article of articles) await postProbeArticle(article.messageId, article.bytes);
    await resetNntpMetrics();
    const startedAt = Date.now();
    const submitted = await submitProbeNzb(request, name, articles);
    expect(submitted).toMatchObject({ accepted: true });
    expect(await waitTerminal(request, submitted.jobId!)).toBe("COMPLETED");
    const elapsedSeconds = Math.max(0.001, (Date.now() - startedAt) / 1_000);
    const served = (await nntpBodyMetrics()).body_bytes;
    expect(served).toBeGreaterThanOrEqual(partBytes * articles.length);
    // Throughput can only fall on a slow host, so an upper bound is load-safe.
    expect(served / elapsedSeconds).toBeLessThan(limit * 1.8);

    setClock(at(day, 10, 30));
    const lifted = await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === 0, "T06 configured limit at 10:30");
    expect(lifted.speedLimitBytesPerSec).toBe(configured);
  });
});

type ProfileState = { active: string; scheduled: string | null; available: string[] };

async function profileState(request: APIRequestContext): Promise<ProfileState> {
  return (await graphql<{ hardwareProfile: ProfileState }>(request, "query { hardwareProfile { active scheduled available } }")).hardwareProfile;
}

async function waitProfile(request: APIRequestContext, scheduled: string, message: string): Promise<ProfileState> {
  let state: ProfileState | undefined;
  await expect.poll(async () => (state = await profileState(request)).scheduled === scheduled, { message, timeout: 0 }).toBe(true);
  expect(state!.active).toBe(scheduled);
  return state!;
}

test("T07 a scheduled hardware profile follows its rules across midnight", async ({ request }) => {
  initialOnly();
  note("observed", "A scheduled profile stays in force after its rules are deleted, until Weaver restarts; the test ends on the profile that was already active.");
  const before = await profileState(request);
  const home = before.active;
  const other = before.available.find(profile => profile !== home) ?? home;
  if (other === home) note("gap", `This machine honours only ${home}; the 09:00 change is not observable.`);
  const day = freshDay();
  setClock(at(day, 8, 59));
  await withRules(request, "t07", async rules => {
    const morning = await rules.create({ actionType: "hardware_profile", time: "09:00", hardwareProfile: other });
    await rules.create({ actionType: "hardware_profile", time: "10:00", hardwareProfile: home });
    expect(morning.track).toBe("PROFILE");
    expect(morning.hardwareProfile).toBe(other);
    await waitProfile(request, home, "T07 the previous day's 10:00 rule is in force");

    setClock(at(day, 9, 0));
    await waitProfile(request, other, "T07 09:00");
    setClock(at(day, 10, 0));
    await waitProfile(request, home, "T07 10:00");
    setClock(at(day, 24, 30));
    await witnessTick(request);
    await waitProfile(request, home, "T07 kept past midnight");
    setClock(at(day, 24 + 9, 0));
    await waitProfile(request, other, "T07 next day 09:00");
    setClock(at(day, 24 + 10, 0));
    await waitProfile(request, home, "T07 next day 10:00");
  });
});

async function serverActive(request: APIRequestContext, id: number): Promise<boolean | undefined> {
  return (await graphql<{ servers: Array<{ id: number; active: boolean }> }>(request, "query { servers { id active } }"))
    .servers.find(server => server.id === id)?.active;
}

test("T08 a server is out of rotation for its scheduled window", async ({ request }) => {
  initialOnly();
  note("observed", "nntp2 is the priority-1 backup, so each probe job's articles exist only there: the job can only finish through nntp2.");
  const backup = await configuredServer(request, "nntp2");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t08", async rules => {
    try {
      await rules.create({ actionType: "set_server_active", time: "10:30", serverId: backup.id, serverActive: true });
      const off = await rules.create({ actionType: "set_server_active", time: "10:00", serverId: backup.id, serverActive: false });
      expect(off.track).toBe("SERVER");

      setClock(at(day, 10, 0));
      await expect.poll(() => serverActive(request, backup.id), { message: "T08 nntp2 off at 10:00", timeout: 0 }).toBe(false);
      // The provider counts the session that reads its metrics, so one active connection is the probe itself.
      await expect.poll(async () => (await nntpConnectionMetrics("nntp2")).active - 1,
        { message: "T08 no Weaver sessions on nntp2", timeout: 0 }).toBe(0);
      const during = `t08-off-${token}`;
      const held = await startDownload(request, during, { count: 2, partBytes: 16 * 1024, nntpHost: "nntp2" });
      expect(await waitTerminal(request, held), "articles only nntp2 holds cannot be fetched while it is off").toBe("FAILED");
      expect(await bodyCount(during, "nntp2")).toBe(0);

      setClock(at(day, 10, 30));
      await expect.poll(() => serverActive(request, backup.id), { message: "T08 nntp2 back at 10:30", timeout: 0 }).toBe(true);
      const after = `t08-on-${token}`;
      await completedJob(request, "t08-on", { nntpHost: "nntp2" });
      expect(await bodyCount(after, "nntp2")).toBeGreaterThan(0);
    } finally {
      await graphql(request, "mutation($id: Int!, $input: ServerInput!) { updateServer(id: $id, input: $input) { id } }", {
        id: backup.id,
        input: { host: "nntp2", port: backup.port, tls: false, username: "e2e-user", password: "e2e-pass", connections: 4, active: true, priority: 1 },
      });
    }
  });
});

async function setIspCap(request: APIRequestContext, enabled: boolean): Promise<void> {
  await updateSettings(request, { ispBandwidthCap: {
    enabled, period: "DAILY", limitBytes: 2_000_000_000, resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1,
  } });
}

test("T09 quota metering pauses and resumes on schedule", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t09", async rules => {
    try {
      await setIspCap(request, true);
      await rules.create({ actionType: "set_quota_metering", time: "10:30", quotaMeteringEnabled: true });
      const off = await rules.create({ actionType: "set_quota_metering", time: "10:00", quotaMeteringEnabled: false });
      expect(off.track).toBe("QUOTA");

      setClock(at(day, 10, 0));
      await witnessTick(request);
      const before = (await queueState(request)).downloadBlock.usedBytes;
      await completedJob(request, "t09-unmetered");
      await witnessTick(request);
      expect((await queueState(request)).downloadBlock.usedBytes, "nothing metered while metering is off").toBe(before);

      setClock(at(day, 10, 30));
      await witnessTick(request);
      await completedJob(request, "t09-metered");
      await expect.poll(async () => (await queueState(request)).downloadBlock.usedBytes > before,
        { message: "T09 metered again after 10:30", timeout: 0 }).toBe(true);
    } finally {
      await setIspCap(request, false);
    }
  });
});

test("T10 one-shot rules fetch RSS, scan the watch folder and prune history", async ({ request }) => {
  initialOnly();
  const directory = watchDir("t10");
  const feedKey = `t10-${token}`;
  const feedId = await addCountedFeed(request, feedKey);
  const day = freshDay();
  setClock(at(day, 9, 30));
  await withRules(request, "t10", async rules => {
    try {
      await armWatchFolder(request, directory);
      const complete1 = await completedJob(request, "t10-c1");
      const complete1Dir = (await historyItem(request, complete1))!.outputDir!;
      expect(fs.existsSync(localOutput(complete1Dir))).toBe(true);
      const failed1 = await failedJob(request, "t10-f1");
      const cancelled1 = await cancelledJob(request, "t10-x1");
      const dropped = await dropValidNzb(directory, "t10-dropped");

      const fetchRule = await rules.create({ actionType: "fetch_rss", time: "10:00", feedId });
      expect(fetchRule.track).toBe("ONE_SHOT");
      await rules.create({ actionType: "scan_watch_folder", time: "10:00" });
      await rules.create({
        actionType: "prune_history", time: "10:00",
        pruneCompleted: { deleteFiles: true }, pruneFailed: { deleteFiles: false }, pruneCancelled: { deleteFiles: true },
      });
      await rules.create({
        actionType: "prune_history", time: "10:30",
        pruneCompleted: { deleteFiles: false }, pruneFailed: { deleteFiles: true }, pruneCancelled: { deleteFiles: false },
      });

      setClock(at(day, 9, 58));
      await witnessTick(request);
      expect(await feedCount(feedKey)).toBe(0);
      expect(fs.existsSync(dropped), "the hourly poll has not consumed the file").toBe(true);

      setClock(at(day, 10, 2));
      await expect.poll(() => feedCount(feedKey), { message: "T10 scheduled fetch", timeout: 0 }).toBe(1);
      await expect.poll(() => fs.existsSync(`${dropped}.queued`), { message: "T10 scheduled scan", timeout: 0 }).toBe(true);
      for (const [name, id] of [["completed", complete1], ["failed", failed1], ["cancelled", cancelled1]] as const) {
        await expect.poll(async () => await historyItem(request, id) === null, { message: `T10 round 1 prunes ${name}`, timeout: 0 }).toBe(true);
      }
      await expect.poll(() => fs.existsSync(localOutput(complete1Dir)), { message: "T10 round 1 deletes completed files", timeout: 0 }).toBe(false);

      const complete2 = await completedJob(request, "t10-c2");
      const complete2Dir = (await historyItem(request, complete2))!.outputDir!;
      const failed2 = await failedJob(request, "t10-f2");
      const cancelled2 = await cancelledJob(request, "t10-x2");

      setClock(at(day, 10, 30));
      for (const [name, id] of [["completed", complete2], ["failed", failed2], ["cancelled", cancelled2]] as const) {
        await expect.poll(async () => await historyItem(request, id) === null, { message: `T10 round 2 prunes ${name}`, timeout: 0 }).toBe(true);
      }
      await waitRows(`SELECT COUNT(*) AS n FROM async_operation_targets WHERE state IN ('queued', 'running')`,
        rows => rows[0]?.n === "0", "T10 round 2 delete operation finished");
      expect(fs.existsSync(localOutput(complete2Dir)), "round 2 keeps completed files").toBe(true);
      expect(await feedCount(feedKey), "one occurrence, one fetch").toBe(1);
    } finally {
      await disarmWatchFolder(request);
      await deleteFeed(request, feedId);
    }
  });
});

test("T11 every due one-shot runs even past the running cap", async ({ request }) => {
  initialOnly();
  note("gap", "At most 32 one-shots run at once and the rest wait; scheduled RSS fetches also serialise on the RSS lock, so how many ran concurrently is not observable. All 33 must still run exactly once.");
  const keys: Record<string, string> = {};
  const feeds: number[] = [];
  const day = freshDay();
  setClock(at(day, 9, 58));
  await withRules(request, "t11", async rules => {
    try {
      for (let index = 0; index < 33; index += 1) {
        const key = `t11-${index}-${token}`;
        keys[`f${index}`] = key;
        const feedId = await addCountedFeed(request, key);
        feeds.push(feedId);
        await rules.create({ actionType: "fetch_rss", time: "10:00", feedId });
      }
      await witnessTick(request);
      setClock(at(day, 10, 1));
      await waitCounts(keys, Object.fromEntries(Object.keys(keys).map(name => [name, 1])), "T11 all 33 fetched");
      await witnessTick(request);
      expect(Object.values(await counts(keys)).every(count => count === 1)).toBe(true);
    } finally {
      for (const id of feeds) await deleteFeed(request, id);
    }
  });
});

test("T12 time, times, hourly and weekday rules fire on their occurrences", async ({ request }) => {
  initialOnly();
  const keys = { A: `t12-a-${token}`, B: `t12-b-${token}`, C: `t12-c-${token}`, D: `t12-d-${token}` };
  const feeds: Record<string, number> = {};
  const monday = freshDay("mon");
  const tuesday = new Date(monday.getTime() + DAY_MS);
  setClock(at(monday, 9, 58));
  await withRules(request, "t12", async rules => {
    try {
      for (const [name, key] of Object.entries(keys)) feeds[name] = await addCountedFeed(request, key);
      await rules.create({ actionType: "fetch_rss", time: "10:00", feedId: feeds.A });
      await rules.create({ actionType: "fetch_rss", time: "10:00", times: ["10:00", "10:05"], feedId: feeds.B });
      const hourly = await rules.create({ actionType: "fetch_rss", time: "00:07", everyHourAtMinute: 7, feedId: feeds.C });
      expect(hourly.everyHourAtMinute).toBe(7);
      const weekly = await rules.create({ actionType: "fetch_rss", time: "10:00", days: ["mon"], feedId: feeds.D });
      expect(weekly.days).toEqual(["mon"]);
      await witnessTick(request);

      const step = async (instant: Date, expected: Record<string, number>, message: string) => {
        setClock(instant);
        await waitCounts(keys, expected, message);
        await witnessTick(request);
        expect(await counts(keys), message).toEqual(expected);
      };
      await step(at(monday, 10, 1), { A: 1, B: 1, C: 0, D: 1 }, "Monday 10:01");
      await step(at(monday, 10, 6), { A: 1, B: 2, C: 0, D: 1 }, "Monday 10:06");
      await step(at(monday, 10, 8), { A: 1, B: 2, C: 1, D: 1 }, "Monday 10:08");
      await step(at(monday, 11, 8), { A: 1, B: 2, C: 2, D: 1 }, "Monday 11:08");

      // More than 90 minutes forward only sets a new baseline.
      setClock(at(tuesday, 9, 58));
      await witnessTick(request);
      expect(await counts(keys), "a long jump fires nothing").toEqual({ A: 1, B: 2, C: 2, D: 1 });
      await step(at(tuesday, 10, 1), { A: 2, B: 3, C: 2, D: 1 }, "Tuesday 10:01");
    } finally {
      for (const id of Object.values(feeds)) await deleteFeed(request, id);
    }
  });
});

test("T14 a server rule is refused for a missing server and removed with its server", async ({ request }) => {
  initialOnly();
  note("observed", "The \"could not be applied\" hold for a server missing from the runtime configuration is reachable only when the database and the runtime disagree: saving refuses a missing server and removing a server removes its rules.");
  const servers = (await graphql<{ servers: Array<{ id: number }> }>(request, "query { servers { id } }")).servers;
  const missingId = Math.max(...servers.map(server => server.id)) + 1;
  const before = await schedules(request);
  const label = `t14-missing-${token}`;
  const refused = await graphqlErrors(request,
    `mutation($input: ScheduleInput!) { createSchedule(input: $input) { ${SCHEDULE_FIELDS} } }`,
    { input: { enabled: true, days: [], label, actionType: "set_server_active", time: hhmm(readClock()), serverId: missingId, serverActive: false } });
  expect(refused.join("\n"), "a rule for a missing server is refused").toContain(`server ${missingId} not found`);
  expect(await schedules(request), "the refused rule left the schedules unchanged").toEqual(before);

  // A disabled throwaway server whose rule never comes into force; removing the
  // server removes the rule with it.
  const serverId = (await graphql<{ addServer: { id: number } }>(request,
    "mutation($input: ServerInput!) { addServer(input: $input) { id } }",
    { input: { host: "nntp", port: 119, tls: false, username: "e2e-user", password: "e2e-pass", connections: 1, active: false, priority: 0, backfill: false, retentionDays: 0 } })).addServer.id;
  let removed = false;
  try {
    const ruleLabel = `t14-server-${token}`;
    const created = (await graphql<{ createSchedule: ScheduleRow[] }>(request,
      `mutation($input: ScheduleInput!) { createSchedule(input: $input) { ${SCHEDULE_FIELDS} } }`,
      { input: { enabled: false, days: [], label: ruleLabel, actionType: "set_server_active", time: hhmm(readClock()), serverId, serverActive: false } })).createSchedule
      .filter(row => row.label === ruleLabel);
    expect(created, "the rule for the throwaway server").toHaveLength(1);
    expect(created[0]).toMatchObject({ serverId, serverActive: false });

    await graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id: serverId });
    removed = true;
    const after = await schedules(request);
    expect(after.find(row => row.id === created[0]!.id), "the server's rule went with it").toBeUndefined();
    expect(after, "no other rule changed").toEqual(before);
  } finally {
    if (!removed) await graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id: serverId });
  }
});

test("T15 an armed NZBGet resume timer defers the scheduled resume", async ({ request }) => {
  initialOnly();
  note("observed", "scheduleresume arms a wall-clock timer, not an e2e-clock one; the test re-arms it for one second to let it fire.");
  const day = freshDay();
  setClock(at(day, 8, 59));
  await withRules(request, "t15", async rules => {
    try {
      await rules.create({ actionType: "resume", time: "10:00" });
      await rules.create({ actionType: "pause", time: "09:00" });
      setClock(at(day, 9, 55));
      await waitPaused(request, true, "T15 paused at 09:00");
      await withControlKey(request, async key => {
        await nzbgetRpc(request, key, "scheduleresume", [3600]);
        expect(Number(await setting("nzbget.scheduled_resume_at"))).toBeGreaterThan(0);

        setClock(at(day, 10, 0));
        await witnessTick(request);
        expect((await queueState(request)).isPaused, "the 10:00 resume waits for the armed timer").toBe(true);
        expect(Number(await setting("nzbget.scheduled_resume_at"))).toBeGreaterThan(0);

        await nzbgetRpc(request, key, "scheduleresume", [1]);
        await waitPaused(request, false, "T15 resumed once the timer fired");
        await expect.poll(() => setting("nzbget.scheduled_resume_at"), { message: "T15 timer setting cleared", timeout: 0 }).toBeUndefined();
      });
    } finally {
      await resumeAll(request);
    }
  });
});

test("T16 rules are created, edited, toggled and deleted through the API", async ({ request }) => {
  initialOnly();
  const keys = { E: `t16-e-${token}`, F: `t16-f-${token}` };
  const feeds = { E: await addCountedFeed(request, keys.E), F: await addCountedFeed(request, keys.F) };
  const day = freshDay();
  setClock(at(day, 9, 0));
  await withRules(request, "t16", async rules => {
    try {
      const created = await rules.create({ actionType: "fetch_rss", time: "10:00", feedId: feeds.E });
      expect(created).toMatchObject({ enabled: true, implicit: false, time: "10:00", actionType: "fetch_rss", track: "ONE_SHOT", feedId: feeds.E });
      expect(created.id).toMatch(/^sched-[0-9a-f]+$/);

      const renamed = `${created.label}-edited`;
      const updated = (await rules.update(created.id, { actionType: "fetch_rss", time: "10:05", feedId: feeds.E, label: renamed }))
        .find(row => row.id === created.id);
      expect(updated).toMatchObject({ label: renamed, time: "10:05", enabled: true });
      const toggled = (await rules.toggle(created.id, false)).find(row => row.id === created.id);
      expect(toggled?.enabled).toBe(false);
      const sibling = await rules.create({ actionType: "fetch_rss", time: "10:05", feedId: feeds.F });
      expect((await schedules(request)).find(row => row.id === created.id)).toMatchObject({ label: renamed, time: "10:05", enabled: false });

      setClock(at(day, 10, 3));
      await witnessTick(request);
      setClock(at(day, 10, 6));
      await expect.poll(() => feedCount(keys.F), { message: "T16 enabled sibling fires", timeout: 0 }).toBe(1);
      await witnessTick(request);
      expect(await feedCount(keys.E), "a disabled rule does not fire").toBe(0);

      expect((await rules.toggle(created.id, true)).find(row => row.id === created.id)?.enabled).toBe(true);
      const afterDelete = await rules.delete(created.id);
      expect(afterDelete.some(row => row.id === created.id)).toBe(false);
      expect(afterDelete.some(row => row.id === sibling.id)).toBe(true);
      await rules.delete(sibling.id);
      const remaining = await schedules(request);
      expect(remaining.some(row => row.id === created.id || row.id === sibling.id)).toBe(false);
    } finally {
      await deleteFeed(request, feeds.E);
      await deleteFeed(request, feeds.F);
    }
  });
});

test("T13 a hold rule replays its last occurrence at startup and only once after the clock moves back", async ({ request }) => {
  test.skip(stage() === "restarted-again", "covered by the first restart");

  if (stage() === "initial") {
    const day = freshDay();
    setClock(at(day, 9, 59));
    const rules = new Rules(request, "t13");
    const rule = await rules.create({ actionType: "pause", time: "10:00", label: `t13-lookback-${token}` });
    setClock(at(day, 10, 5));
    await waitPaused(request, true, "T13 paused at 10:00");
    // Deliberately left in place: the restart must find it.
    saveStageState("t13", { id: rule.id, day: day.toISOString() });
    return;
  }

  const { id, day: dayText } = loadStageState<{ id: string; day: string }>("t13");
  const day = new Date(dayText);
  try {
    await waitPaused(request, true, "T13 the 10:00 pause replays at startup");

    // Ten minutes back clears the applied state: the previous day's 10:00
    // pause is re-applied once, and a manual resume after that must stick.
    setClock(at(day, 9, 55));
    await witnessTick(request);
    await resumeAll(request);
    await waitPaused(request, false, "T13 manual resume");
    await witnessTick(request);
    expect((await queueState(request)).isPaused, "the replayed occurrence does not fire again").toBe(false);

    setClock(at(day, 10, 0));
    await waitPaused(request, true, "T13 10:00 fires again");
  } finally {
    await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id });
    await resumeAll(request);
  }
});

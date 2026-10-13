import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, nntpBodyMetrics, postProbeArticle, submitProbeNzb, test } from "../helpers";
import { literal, waitRows } from "./datastore";
import { startDownload, waitTerminal } from "./downloads";
import { clockFile, readClock } from "./e2e-clock";
import { nzbDocument, nzbgetRpc } from "./script-settings";

/**
 * Schedule rules through the public API, for the scheduling specs. Weaver
 * reads the e2e clock on every evaluator tick (250 ms in e2e mode), so a spec
 * moves the clock across a rule's time and waits for the effect the crossing
 * produces.
 *
 * Hold rules apply as soon as they are created, through their occurrence on
 * the previous day, so the rule that should not be in force yet is always
 * created first: the later rule of a pair wins on the previous day too.
 */

export const RSS_FIXTURE = "http://rss-fixture:8089";
const WATCH_ROOT = "/watch-folder";
export const DAY_MS = 24 * 60 * 60_000;
export const WEEKDAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;
export type WeekdayName = (typeof WEEKDAYS)[number];
export const token = Date.now().toString(36);

export function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

// ---------------------------------------------------------------------------
// Clock

/** Midnight UTC at least two days past the clock, optionally on a weekday. */
export function freshDay(weekday?: WeekdayName): Date {
  const now = readClock();
  let day = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 2);
  while (weekday !== undefined && WEEKDAYS[new Date(day).getUTCDay()] !== weekday) day += DAY_MS;
  return new Date(day);
}

export const at = (day: Date, hours: number, minutes: number) => new Date(day.getTime() + (hours * 60 + minutes) * 60_000);
export const hhmm = (instant: Date) => instant.toISOString().slice(11, 16);

/**
 * Leave an instant for the harness to put on the clock while Weaver is
 * stopped between stages. Only flows that ask the harness to move the clock
 * while Weaver is down consume it; the harness removes the file when it does.
 */
export function setClockWhileStopped(instant: Date): void {
  const file = clockFile();
  const owner = fs.statSync(file);
  const target = `${file}.while-stopped`;
  const pending = `${target}.${process.pid}.tmp`;
  fs.writeFileSync(pending, `${instant.toISOString()}\n`, { mode: 0o600 });
  fs.chownSync(pending, owner.uid, owner.gid);
  fs.chmodSync(pending, owner.mode & 0o777);
  fs.renameSync(pending, target);
}

/** Whether the harness consumed the instant left by `setClockWhileStopped`. */
export const clockWhileStoppedPending = () => fs.existsSync(`${clockFile()}.while-stopped`);

// ---------------------------------------------------------------------------
// Rules

export type SpeedLimitRow = { kind: "GLOBAL" | "EGRESS" | "SERVER"; id: number | null; bytesPerSec: number };
export type ScheduleRow = {
  id: string; label: string; enabled: boolean; days: string[]; time: string; times: string[];
  everyHourAtMinute: number | null; actionType: string; track: string;
  serverId: number | null; serverActive: boolean | null; speedLimitBytes: number | null; hardwareProfile: string | null;
  quotaMeteringEnabled: boolean | null; quotaEgressId: number | null;
  pruneFailed: { deleteFiles: boolean } | null; pruneCompleted: { deleteFiles: boolean } | null;
  pruneCancelled: { deleteFiles: boolean } | null;
  speedLimits: SpeedLimitRow[];
};
export type RuleInput = Record<string, unknown> & { actionType: string; time: string };

export const SCHEDULE_FIELDS = `id label enabled days time times everyHourAtMinute actionType track serverId serverActive
  speedLimitBytes hardwareProfile quotaMeteringEnabled quotaEgressId
  pruneFailed { deleteFiles } pruneCompleted { deleteFiles } pruneCancelled { deleteFiles }
  speedLimits { kind id bytesPerSec }`;

export async function schedules(request: APIRequestContext): Promise<ScheduleRow[]> {
  return (await graphql<{ schedules: ScheduleRow[] }>(request, `query { schedules { ${SCHEDULE_FIELDS} } }`)).schedules;
}

/** Rules a test creates, all removed again when it ends. */
export class Rules {
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

  /** Stop tracking a rule a later stage removes. */
  keep(id: string): void {
    this.ids.delete(id);
  }

  async clear(): Promise<void> {
    for (const id of [...this.ids]) await this.delete(id);
  }
}

/** Run `body` with a rule tracker whose rules are removed afterwards. */
export async function withRules(request: APIRequestContext, tag: string, body: (rules: Rules) => Promise<void>): Promise<void> {
  const rules = new Rules(request, tag);
  try {
    await body(rules);
  } finally {
    await rules.clear();
  }
}

let witnessSerial = 0;

/**
 * Prove a whole evaluator tick ran after this call began. A rule on a track
 * the test does not use must come into force, then the same rule edited to
 * the opposite action must too. The second change needs a later tick than the
 * first, and that tick read the rules (so the clock) after the clock was last
 * moved; every track has been processed by the tick that published the first
 * change, since a tick reads the rules once and walks every track.
 *
 * `speed` uses the global Speed track and ends with no scheduled limit; it is
 * not for tests that schedule a global speed rule. `watch` uses the
 * WatchFolder track and ends with scanning resumed; it is not for tests that
 * schedule watch-folder, pause_all or resume rules, or that leave scanning
 * paused.
 */
export async function witnessTick(request: APIRequestContext, via: "speed" | "watch" = "speed"): Promise<void> {
  witnessSerial += 1;
  const rules = new Rules(request, `witness-${via}`);
  try {
    const time = hhmm(readClock());
    if (via === "speed") {
      const marker = 2_000_000_000 + witnessSerial * 1_024 + (Date.now() % 1_000);
      const rule = await rules.create({ actionType: "speed_limit", time, speedLimitBytes: marker });
      await expect.poll(async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit,
        { message: `witness ${marker} in force`, timeout: 0 }).toBe(marker);
      // A global limit of 0 takes the scheduled limit away again.
      await rules.update(rule.id, { actionType: "speed_limit", time: rule.time, label: rule.label, speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 0 }] });
      await expect.poll(async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit,
        { message: `witness ${marker} lifted`, timeout: 0 }).toBe(0);
      return;
    }
    expect(await watchFolderPaused(request), "the watch witness starts with scanning running").toBe(false);
    const rule = await rules.create({ actionType: "pause_watch_folder_scanning", time });
    await expect.poll(() => watchFolderPaused(request), { message: `watch witness ${witnessSerial} paused`, timeout: 0 }).toBe(true);
    await rules.update(rule.id, { actionType: "resume_watch_folder_scanning", time: rule.time, label: rule.label });
    await expect.poll(() => watchFolderPaused(request), { message: `watch witness ${witnessSerial} resumed`, timeout: 0 }).toBe(false);
  } finally {
    await rules.clear();
  }
}

// ---------------------------------------------------------------------------
// Queue, settings and jobs

export type QueueState = {
  isPaused: boolean;
  speedLimitBytesPerSec: number;
  downloadBlock: { kind: string; usedBytes: number; scheduledSpeedLimit: number; scheduleHoldReason: string | null };
};

export async function queueState(request: APIRequestContext): Promise<QueueState> {
  return (await graphql<{ globalQueueState: QueueState }>(request,
    "query { globalQueueState { isPaused speedLimitBytesPerSec downloadBlock { kind usedBytes scheduledSpeedLimit scheduleHoldReason } } }")).globalQueueState;
}

export async function waitQueue(request: APIRequestContext, predicate: (state: QueueState) => boolean, message: string): Promise<QueueState> {
  let state: QueueState | undefined;
  await expect.poll(async () => predicate(state = await queueState(request)), { message, timeout: 0 }).toBe(true);
  return state!;
}

export const waitPaused = (request: APIRequestContext, paused: boolean, message: string) =>
  waitQueue(request, state => state.isPaused === paused, message);

export async function resumeAll(request: APIRequestContext): Promise<void> {
  await graphql(request, "mutation { resumeAll }");
}

export type WatchFolder = {
  mode: string; path: string | null; pollIntervalSecs: number; stabilitySecs: number;
  categoryFromSubfolders: boolean; scanningPaused: boolean;
};

export async function generalSettings(request: APIRequestContext): Promise<{ maxDownloadSpeed: number; watchFolder: WatchFolder }> {
  return (await graphql<{ settings: { maxDownloadSpeed: number; watchFolder: WatchFolder } }>(request,
    "query { settings { maxDownloadSpeed watchFolder { mode path pollIntervalSecs stabilitySecs categoryFromSubfolders scanningPaused } } }")).settings;
}

export async function updateSettings(request: APIRequestContext, input: Record<string, unknown>): Promise<void> {
  await graphql(request, "mutation($input: GeneralSettingsInput!) { updateSettings(input: $input) { maxDownloadSpeed } }", { input });
}

export async function watchFolderPaused(request: APIRequestContext): Promise<boolean> {
  return (await generalSettings(request)).watchFolder.scanningPaused;
}

/** A watch directory only this test uses, writable by Weaver. */
export function watchDir(name: string): string {
  const directory = path.join(WATCH_ROOT, `scheduling-${name}-${token}`);
  fs.mkdirSync(directory, { recursive: true, mode: 0o777 });
  fs.chmodSync(directory, 0o777);
  return directory;
}

export function dropFile(directory: string, name: string, contents: string): string {
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
export async function armWatchFolder(request: APIRequestContext, directory: string): Promise<void> {
  const sentinel = dropFile(directory, "sentinel.nzb", "not an nzb\n");
  await updateSettings(request, { watchFolder: {
    mode: "polling", path: directory, pollIntervalSecs: 3600, stabilitySecs: 0,
    categoryFromSubfolders: false, scanningPaused: false,
  } });
  await expect.poll(() => fs.existsSync(`${sentinel}.error`), { message: `first watch pass over ${directory}`, timeout: 0 }).toBe(true);
}

export async function disarmWatchFolder(request: APIRequestContext): Promise<void> {
  await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
}

/** Post one article and drop an NZB for it, returning the NZB's path. */
export async function dropValidNzb(directory: string, name: string): Promise<string> {
  const article = { messageId: `${name}-${token}@e2e.invalid`, bytes: 16 * 1024 };
  await postProbeArticle(article.messageId, article.bytes);
  return dropFile(directory, `${name}.nzb`, nzbDocument(name, [article]));
}

export type QueueItem = { id: number; state: string; remainingFileCount: number; downloadedBytes: number };

export async function queueItem(request: APIRequestContext, id: number): Promise<QueueItem | null> {
  return (await graphql<{ queueItem: QueueItem | null }>(request,
    "query($id: Int!) { queueItem(id: $id) { id state remainingFileCount downloadedBytes } }", { id })).queueItem;
}

export async function historyItem(request: APIRequestContext, id: number): Promise<{ state: string; outputDir: string | null } | null> {
  return (await graphql<{ historyItem: { state: string; outputDir: string | null } | null }>(request,
    "query($id: Int!) { historyItem(id: $id) { state outputDir } }", { id })).historyItem;
}

/** BODY requests the provider served for articles whose id contains `prefix`. */
export async function bodyCount(prefix: string, host = "nntp"): Promise<number> {
  const metrics = await nntpBodyMetrics(prefix, host);
  return Object.entries(metrics.body_counts ?? {})
    .filter(([messageId]) => messageId.includes(prefix))
    .reduce((sum, [, count]) => sum + count, 0);
}

export async function completedJob(request: APIRequestContext, name: string, options: { nntpHost?: string } = {}): Promise<number> {
  const id = await startDownload(request, `${name}-${token}`, { count: 4, partBytes: 32 * 1024, ...options });
  expect(await waitTerminal(request, id), `${name} completes`).toBe("COMPLETED");
  return id;
}

/** A job none of whose articles exist on any provider. */
export async function failedJob(request: APIRequestContext, name: string): Promise<number> {
  const articles = [{ messageId: `${name}-${token}-absent@e2e.invalid`, bytes: 16 * 1024 }];
  const result = await submitProbeNzb(request, `${name}-${token}`, articles);
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  expect(await waitTerminal(request, result.jobId!), `${name} fails`).toBe("FAILED");
  return result.jobId!;
}

/** A job cancelled while every download is paused by the operator. */
export async function cancelledJob(request: APIRequestContext, name: string): Promise<number> {
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

export const localOutput = (outputDir: string) => outputDir.replace(/^\/data\/complete/, "/weaver-downloads");

// ---------------------------------------------------------------------------
// Hardware profile, servers, egresses, post-processing, RSS

export type ProfileState = { active: string; scheduled: string | null; selected: string | null; available: string[] };

export async function profileState(request: APIRequestContext): Promise<ProfileState> {
  return (await graphql<{ hardwareProfile: ProfileState }>(request,
    "query { hardwareProfile { active scheduled selected available } }")).hardwareProfile;
}

export async function waitProfile(request: APIRequestContext, scheduled: string, message: string): Promise<ProfileState> {
  let state: ProfileState | undefined;
  await expect.poll(async () => (state = await profileState(request)).scheduled === scheduled, { message, timeout: 0 }).toBe(true);
  expect(state!.active).toBe(scheduled);
  return state!;
}

export type ServerRow = { id: number; host: string; port: number; active: boolean; maxDownloadSpeed: number; priority: number };

export async function serverRows(request: APIRequestContext): Promise<ServerRow[]> {
  return (await graphql<{ servers: ServerRow[] }>(request, "query { servers { id host port active maxDownloadSpeed priority } }")).servers;
}

export async function serverRow(request: APIRequestContext, host: string): Promise<ServerRow> {
  const found = (await serverRows(request)).find(server => server.host === host);
  expect(found, `configured server ${host}`).toBeTruthy();
  return found!;
}

export async function serverActive(request: APIRequestContext, id: number): Promise<boolean | undefined> {
  return (await serverRows(request)).find(server => server.id === id)?.active;
}

/** Put a configured fixture server back in rotation with no speed limit. */
export async function restoreServer(request: APIRequestContext, host: "nntp" | "nntp2"): Promise<void> {
  const server = await serverRow(request, host);
  await graphql(request, "mutation($id: Int!, $input: ServerInput!) { updateServer(id: $id, input: $input) { id } }", {
    id: server.id,
    input: {
      host, port: server.port, tls: false, username: "e2e-user", password: "e2e-pass", connections: 4,
      active: true, priority: host === "nntp" ? 0 : 1, maxDownloadSpeed: 0,
    },
  });
}

export async function egressSpeed(request: APIRequestContext, id: number): Promise<number | undefined> {
  return (await graphql<{ egressInterfaces: Array<{ id: number; maxDownloadSpeed: number }> }>(request,
    "query { egressInterfaces { id maxDownloadSpeed } }")).egressInterfaces.find(egress => egress.id === id)?.maxDownloadSpeed;
}

/** Whether post-processing is paused, as the NZBGet facade's status reports it. */
export async function postPaused(request: APIRequestContext, key: string): Promise<boolean> {
  const status = await nzbgetRpc(request, key, "status", []) as { PostPaused: boolean };
  return status.PostPaused;
}

/**
 * An enabled feed the fixture counts reads of. A new enabled feed is due at
 * once and then only after an hour of wall time, so after its first read only
 * a resume of a paused poller or a manual sync reads it again.
 */
export async function addCountedFeed(request: APIRequestContext, key: string): Promise<number> {
  return (await graphql<{ addRssFeed: { id: number } }>(request,
    "mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }",
    { input: { name: `counted ${key}`, url: `${RSS_FIXTURE}/counted/${key}.xml`, enabled: true, pollIntervalSecs: 3600 } })).addRssFeed.id;
}

export async function deleteFeed(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { deleteRssFeed(id: $id) }", { id });
}

export async function feedCount(key: string): Promise<number> {
  const response = await fetch(`${RSS_FIXTURE}/count/${key}`);
  expect(response.ok, `count for ${key}`).toBe(true);
  return ((await response.json()) as { count: number }).count;
}

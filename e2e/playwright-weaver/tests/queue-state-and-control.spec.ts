// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import fs from "node:fs";
import type { APIRequestContext } from "@playwright/test";
import {
  expect,
  graphql,
  metrics,
  metricValue,
  postProbeArticle,
  setNntpChaos,
  submitProbeNzb,
  test,
  updateConfiguredServer,
} from "./helpers";
import { nzbgetRpc, withControlKey } from "./support/script-settings";

// What the queue says about a job, and what the operator's controls do to
// it, read only through the API. Every job here is a probe of zero-filled
// articles the spec posts itself; the fixture's slow_body keeps a job in its
// download long enough to be read there, and every wait is on the job's own
// state.

// One worker runs these in order; a failure does not skip the rest.
test.describe.configure({ mode: "default" });

const HOSTS = ["nntp", "nntp2"] as const;
// 128 KiB is 1024 yEnc lines; at slow_body=10 an article takes about ten
// seconds to serve whatever the machine.
const ARTICLE_BYTES = 128 * 1024;
const SLOW = "slow_body=10";

type Item = {
  id: number;
  state: string;
  downloadState: string;
  waitReason: string | null;
  downloadWaitReason: string | null;
  progressPercent: number;
  outputDir: string | null;
};
type Snapshot = {
  items: Item[];
  summary: { pausedItems: number; activeItems: number; queuedItems: number };
  globalState: { isPaused: boolean };
};
type History = {
  id: number;
  state: string;
  error: string | null;
  attention: { code: string } | null;
};

const ITEM_FIELDS = "id state downloadState waitReason downloadWaitReason progressPercent outputDir";

async function snapshot(request: APIRequestContext): Promise<Snapshot> {
  return (await graphql<{ queueSnapshot: Snapshot }>(
    request,
    `query WeaverE2EQueueStateSnapshot {
      queueSnapshot {
        items { ${ITEM_FIELDS} }
        summary { pausedItems activeItems queuedItems }
        globalState { isPaused }
      }
    }`,
  )).queueSnapshot;
}

async function history(request: APIRequestContext, id: number): Promise<History | null> {
  return (await graphql<{ historyItem: History | null }>(
    request,
    "query WeaverE2EQueueStateHistory($id: Int!) { historyItem(id: $id) { id state error attention { code } } }",
    { id },
  )).historyItem;
}

// Mode C (Q9): no job reads PAUSED unless this spec paused it.
let pausedByUs = new Set<number>();
let globalPauseByUs = false;
function assertNoUnaskedPause(snap: Snapshot): void {
  for (const item of snap.items) {
    if (item.state !== "PAUSED") continue;
    expect(
      globalPauseByUs || pausedByUs.has(item.id),
      `job ${item.id} reads PAUSED without an operator pause (${JSON.stringify(item)})`,
    ).toBe(true);
  }
}

async function sample(request: APIRequestContext): Promise<Snapshot> {
  const snap = await snapshot(request);
  assertNoUnaskedPause(snap);
  return snap;
}

let sequence = 0;
async function probeJob(request: APIRequestContext, tag: string, articles: number): Promise<number> {
  sequence += 1;
  const name = `${tag}-${sequence}`;
  const ids = Array.from({ length: articles }, (_, index) => ({
    messageId: `${name}-${index + 1}@e2e.invalid`,
    bytes: ARTICLE_BYTES,
  }));
  for (const article of ids) {
    for (const host of HOSTS) await postProbeArticle(article.messageId, article.bytes, host);
  }
  const submitted = await submitProbeNzb(request, name, ids);
  expect(submitted).toMatchObject({ accepted: true });
  expect(submitted.jobId).not.toBeNull();
  return submitted.jobId!;
}

async function waitFor(
  request: APIRequestContext,
  message: string,
  predicate: (snap: Snapshot) => boolean,
): Promise<Snapshot> {
  let last: Snapshot | undefined;
  await expect.poll(async () => {
    last = await sample(request);
    return predicate(last);
  }, { message, timeout: 0 }).toBe(true);
  return last!;
}

function itemOf(snap: Snapshot, id: number): Item | undefined {
  return snap.items.find((item) => item.id === id);
}

async function waitTerminal(request: APIRequestContext, id: number): Promise<History> {
  let row: History | null = null;
  await expect.poll(async () => {
    await sample(request);
    row = await history(request, id);
    return row?.state ?? null;
  }, { message: `job ${id} reaches history`, timeout: 0 }).toMatch(/^(COMPLETED|FAILED)$/);
  return row!;
}

async function cancelAll(request: APIRequestContext, ids: number[]): Promise<void> {
  if (ids.length === 0) return;
  await graphql(request, "mutation WeaverE2EQueueStateCleanup($ids: [Int!]!) { cancelJobs(ids: $ids) { id } }", { ids });
}

let started: number[] = [];

test.beforeEach(async ({ request }) => {
  pausedByUs = new Set();
  globalPauseByUs = false;
  started = [];
  for (const host of HOSTS) await setNntpChaos(SLOW, host);
});

test.afterEach(async ({ request }) => {
  for (const host of HOSTS) {
    await setNntpChaos("off", host);
    await updateConfiguredServer(request, host, { active: true });
  }
  await updateConfiguredServer(request, "nntp", { connections: 4 });
  await graphql(request, "mutation WeaverE2EQueueStateResume { resumeAll }");
  const snap = await snapshot(request);
  await cancelAll(request, started.filter((id) => itemOf(snap, id)));
});

test("Q1 jobs past the connection capacity wait as Queued and every one completes", async ({ request }) => {
  // Weaver has no job-concurrency slot: maxConcurrentDownloads is article
  // capacity, and lanes pipeline bodies, so every job with a leased article
  // reads Downloading at once. What holds is that no job reads Paused or
  // Failed before it ends, a Queued job has made no progress, and all end
  // complete.
  await updateConfiguredServer(request, "nntp2", { active: false });
  await updateConfiguredServer(request, "nntp", { connections: 2 });
  const tag = `q1-${Date.now()}`;
  const jobs: number[] = [];
  for (let index = 0; index < 4; index += 1) jobs.push(await probeJob(request, tag, 1));
  started.push(...jobs);

  await expect.poll(async () => {
    const snap = await sample(request);
    for (const id of jobs) {
      const item = itemOf(snap, id);
      if (!item) continue;
      expect(["PAUSED", "FAILED"], `job ${id} reads ${item.state}`).not.toContain(item.state);
      if (item.state === "QUEUED") expect(item.progressPercent, `queued job ${id} has no progress`).toBe(0);
    }
    return jobs.every((id) => !itemOf(snap, id));
  }, { message: "every job leaves the queue", timeout: 0 }).toBe(true);

  for (const id of jobs) expect((await waitTerminal(request, id)).state).toBe("COMPLETED");
  await updateConfiguredServer(request, "nntp", { connections: 4 });
});

test("Q2 the global pause holds every download and counts them paused", async ({ request }) => {
  const tag = `q2-${Date.now()}`;
  const jobs = [await probeJob(request, tag, 6), await probeJob(request, tag, 6)];
  started.push(...jobs);
  await waitFor(request, "both jobs downloading", (snap) => jobs.every((id) => itemOf(snap, id)?.state === "DOWNLOADING"));

  globalPauseByUs = true;
  expect((await graphql<{ pauseAll: boolean }>(request, "mutation WeaverE2EQ2Pause { pauseAll }")).pauseAll).toBeTruthy();
  const paused = await waitFor(request, "every held download reads PAUSED", (snap) =>
    jobs.every((id) => itemOf(snap, id)?.state === "PAUSED"));
  expect(paused.globalState.isPaused).toBe(true);
  expect(paused.summary.pausedItems).toBe(paused.items.filter((item) => item.state === "PAUSED").length);
  expect(paused.summary.pausedItems).toBeGreaterThanOrEqual(jobs.length);

  expect((await graphql<{ resumeAll: boolean }>(request, "mutation WeaverE2EQ2Resume { resumeAll }")).resumeAll).toBeTruthy();
  const resumed = await waitFor(request, "every job leaves PAUSED", (snap) =>
    jobs.every((id) => ["DOWNLOADING", "QUEUED"].includes(itemOf(snap, id)?.state ?? "")));
  globalPauseByUs = false;
  expect(resumed.summary.pausedItems).toBe(0);
});

test("Q3 a pause or resume answers with the state it applied", async ({ request }) => {
  const tag = `q3-${Date.now()}`;
  const job = await probeJob(request, tag, 6);
  started.push(job);
  await waitFor(request, "job downloading", (snap) => itemOf(snap, job)?.state === "DOWNLOADING");

  pausedByUs.add(job);
  const pause = await graphql<{ pauseQueueItem: { success: boolean; item: { id: number; state: string } | null } }>(
    request,
    "mutation WeaverE2EQ3Pause($id: Int!) { pauseQueueItem(id: $id) { success item { id state } } }",
    { id: job },
  );
  expect(pause.pauseQueueItem).toMatchObject({ success: true, item: { id: job, state: "PAUSED" } });
  expect(itemOf(await sample(request), job)?.state).toBe("PAUSED");

  const resume = await graphql<{ resumeQueueItem: { success: boolean; item: { id: number; state: string } | null } }>(
    request,
    "mutation WeaverE2EQ3Resume($id: Int!) { resumeQueueItem(id: $id) { success item { id state } } }",
    { id: job },
  );
  expect(resume.resumeQueueItem.success).toBe(true);
  expect(["QUEUED", "DOWNLOADING"]).toContain(resume.resumeQueueItem.item?.state);
  pausedByUs.delete(job);
  expect(["QUEUED", "DOWNLOADING"]).toContain(itemOf(await sample(request), job)?.state);
});

test("Q4 and Q8 a started download with no server reads Queued with the reason, and the facade still counts it downloading", async ({ request }) => {
  // One connection and many articles, so the job is still mid-download when
  // the servers go off.
  await updateConfiguredServer(request, "nntp2", { active: false });
  await updateConfiguredServer(request, "nntp", { connections: 1 });
  const tag = `q4-${Date.now()}`;
  const job = await probeJob(request, tag, 64);
  started.push(job);
  // Started means at least one article landed, so the laneless job has
  // progress it must keep.
  const before = await waitFor(request, "job downloading with progress", (snap) => {
    const item = itemOf(snap, job);
    return item?.state === "DOWNLOADING" && item.progressPercent > 0;
  });
  const pausedBefore = before.summary.pausedItems;

  for (const host of HOSTS) await updateConfiguredServer(request, host, { active: false });
  const laneless = await waitFor(request, "job reads QUEUED with no_server", (snap) => {
    const item = itemOf(snap, job);
    expect(item, "the laneless job stays in the queue").toBeDefined();
    return item?.state === "QUEUED" && item.downloadWaitReason === "no_server";
  });
  expect(laneless.summary.pausedItems).toBe(pausedBefore);
  expect(itemOf(laneless, job)!.progressPercent).toBeGreaterThan(0);

  const [groups, status] = await withControlKey(request, async (key) => [
    await nzbgetRpc(request, key, "listgroups", [0]),
    await nzbgetRpc(request, key, "status", []),
  ]) as [Array<Record<string, unknown>>, Record<string, unknown>];
  // listgroups follows the item's state; only the facade's accounting keeps
  // counting the laneless started job as downloading.
  expect.soft(groups.find((group) => group.NZBID === job), "listgroups reads the laneless job as queued").toMatchObject({ Status: "QUEUED" });
  expect.soft(status, "status does not report server standby for the laneless job").toMatchObject({ ServerStandBy: false });

  await updateConfiguredServer(request, "nntp", { active: true, connections: 4 });
  await waitFor(request, "job downloads again", (snap) => itemOf(snap, job)?.state === "DOWNLOADING");
});

test("Q6 a cancelled job leaves the queue only once its history row says cancelled by operator", async ({ request }) => {
  await updateConfiguredServer(request, "nntp2", { active: false });
  await updateConfiguredServer(request, "nntp", { connections: 1 });
  // Both jobs are admitted under the global pause, so neither downloads
  // before the other is in the queue. Each has 64 articles on one connection,
  // so whichever the resume starts is far inside its download when the
  // cancel lands, and the other cannot get a lease until it ends.
  globalPauseByUs = true;
  await graphql(request, "mutation WeaverE2EQ6Pause { pauseAll }");
  const tag = `q6-${Date.now()}`;
  const jobs = [await probeJob(request, tag, 64), await probeJob(request, tag, 64)];
  started.push(...jobs);
  await waitFor(request, "both jobs admitted", (snap) => jobs.every((id) => itemOf(snap, id)));
  await graphql(request, "mutation WeaverE2EQ6Resume { resumeAll }");
  const before = await waitFor(request, "one job downloading, one queued", (snap) => {
    const states = jobs.map((id) => itemOf(snap, id)?.state).sort();
    return states[0] === "DOWNLOADING" && states[1] === "QUEUED";
  });
  globalPauseByUs = false;
  const [downloading, queued] = itemOf(before, jobs[0]!)!.state === "DOWNLOADING" ? jobs : [jobs[1]!, jobs[0]!];
  const outputDirs = [downloading, queued]
    .map((id) => itemOf(before, id)?.outputDir)
    .filter((dir): dir is string => Boolean(dir))
    .map((dir) => dir.replace(/^\/data\//, "/weaver-data/"));

  const cancelled = await graphql<{ cancelJobs: Array<{ id: number; cancelled: boolean }> }>(
    request,
    "mutation WeaverE2EQ6Cancel($ids: [Int!]!) { cancelJobs(ids: $ids) { id cancelled } }",
    { ids: [downloading, queued] },
  );
  expect(cancelled.cancelJobs).toEqual(expect.arrayContaining([
    { id: downloading, cancelled: true },
    { id: queued, cancelled: true },
  ]));

  await expect.poll(async () => {
    // The queue is read first: a job absent from it whose history row is
    // still missing afterwards left the queue before its row existed.
    const snap = await sample(request);
    const rows = await Promise.all([downloading, queued].map((id) => history(request, id)));
    [downloading, queued].forEach((id, index) => {
      if (!itemOf(snap, id)) expect(rows[index], `job ${id} left the queue before its history row`).not.toBeNull();
    });
    return [downloading, queued].every((id) => !itemOf(snap, id));
  }, { message: "both cancelled jobs leave the queue", timeout: 0 }).toBe(true);

  for (const id of [downloading, queued]) {
    expect(await history(request, id)).toMatchObject({
      state: "FAILED",
      error: "cancelled by operator",
      attention: { code: "CANCELLED" },
    });
  }
  await expect.poll(async () => metricValue(await metrics(request), "weaver_pipeline_active_downloads") ?? 0, {
    message: "no download stays booked to the cancelled jobs",
    timeout: 0,
  }).toBe(0);
  for (const dir of outputDirs) {
    await expect.poll(() => fs.existsSync(dir), { message: `${dir} removed`, timeout: 0 }).toBe(false);
  }
  await updateConfiguredServer(request, "nntp", { connections: 4 });
});

test("Q7 one history delete of 120 ids announces every id and leaves the rest", async ({ request }) => {
  test.setTimeout(10 * 60 * 1000);
  globalPauseByUs = true;
  await graphql(request, "mutation WeaverE2EQ7Pause { pauseAll }");
  const tag = `q7-${Date.now()}`;
  const keep = await probeJob(request, tag, 1);
  const jobs: number[] = [];
  for (let index = 0; index < 120; index += 1) {
    const submitted = await submitProbeNzb(request, `${tag}-bulk-${index}`, [
      { messageId: `${tag}-bulk-${index}@e2e.invalid`, bytes: 1 },
    ]);
    expect(submitted.jobId).not.toBeNull();
    jobs.push(submitted.jobId!);
  }
  started.push(keep, ...jobs);
  await cancelAll(request, [keep, ...jobs]);
  for (const id of [keep, ...jobs]) await waitTerminal(request, id);

  const gone = jobs[17]!;
  await graphql(request, "mutation WeaverE2EQ7DeleteOne($ids: [Int!]!) { removeHistoryItems(ids: $ids) { success } }", { ids: [gone] });
  await expect.poll(async () => history(request, gone), { message: `history ${gone} deleted`, timeout: 0 }).toBeNull();

  const removed = await graphql<{ removeHistoryItems: { success: boolean; removedIds: number[] } }>(
    request,
    "mutation WeaverE2EQ7DeleteBatch($ids: [Int!]!) { removeHistoryItems(ids: $ids) { success removedIds } }",
    { ids: jobs },
  );
  const announced = new Set(removed.removeHistoryItems.removedIds);
  const missing = jobs.filter((id) => id !== gone && !announced.has(id));
  expect(missing, "every deleted id is announced").toEqual([]);
  for (const id of jobs) {
    await expect.poll(async () => history(request, id), { message: `history ${id} deleted`, timeout: 0 }).toBeNull();
  }
  expect(await history(request, keep), "the row outside the batch is intact").not.toBeNull();
});

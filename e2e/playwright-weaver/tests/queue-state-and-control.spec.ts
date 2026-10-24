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
// articles the spec posts itself. A job that must be read mid-download is
// held there by the fixture's hold_body: the server serves one article and
// part of the next, then every body blocks until the test lifts the hold, so
// the job has progress and a download that cannot end on its own. Every wait
// is on the job's own state.

// One worker runs these in order; a failure does not skip the rest.
test.describe.configure({ mode: "default" });

const HOSTS = ["nntp", "nntp2"] as const;
const ARTICLE_BYTES = 128 * 1024;
// A probe article is 1024 yEnc lines of 128 bytes plus its =ybegin and
// =yend lines. The hold lets one article and half of the next through.
const ARTICLE_LINES = ARTICLE_BYTES / 128 + 2;
const HOLD = `hold_body=${Math.floor(ARTICLE_LINES * 1.5)}`;
// Articles per held job. A lane at its deepest rung (8) leases 18 articles a
// refill and asks for 18 more once one lands, so a held job can have 36
// leased; 64 keeps all of them inside the first job, and the job behind it
// stays Queued with nothing leased.
const HELD_ARTICLES = 64;
// Q1 needs its jobs to complete, so it slows the server instead of holding
// it: at slow_body=10 an article takes about ten seconds, long enough for
// the jobs past capacity to be read Queued.
const SLOW = "slow_body=10";

async function holdDownloads(request: APIRequestContext): Promise<void> {
  await updateConfiguredServer(request, "nntp2", { active: false });
  await updateConfiguredServer(request, "nntp", { connections: 1 });
  await setNntpChaos(HOLD, "nntp");
}

async function releaseDownloads(): Promise<void> {
  await setNntpChaos("off", "nntp");
}

function heldMidDownload(item: Item | undefined): boolean {
  return item?.state === "DOWNLOADING" && item.progressPercent > 0;
}

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

const BATCH = 10;
async function inBatches<T, R>(inputs: T[], run: (input: T) => Promise<R>): Promise<R[]> {
  const results: R[] = [];
  for (let offset = 0; offset < inputs.length; offset += BATCH) {
    results.push(...await Promise.all(inputs.slice(offset, offset + BATCH).map(run)));
  }
  return results;
}

let started: number[] = [];

test.beforeEach(async ({ request }) => {
  pausedByUs = new Set();
  globalPauseByUs = false;
  started = [];
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
  await setNntpChaos(SLOW, "nntp");
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
  // One lane and the hold: the hot job takes the lane, lands one article and
  // is held in its next when the pause lands, and the other waits Queued
  // behind it. The pause holds the one mid-download and counts both. Nothing
  // here needs the download to move again, so the hold lifts in afterEach.
  await holdDownloads(request);
  const tag = `q2-${Date.now()}`;
  const jobs = [await probeJob(request, tag, HELD_ARTICLES), await probeJob(request, tag, HELD_ARTICLES)];
  started.push(...jobs);
  await waitFor(request, "one job mid-download, the other queued", (snap) => {
    const items = jobs.map((id) => itemOf(snap, id));
    return items.some(heldMidDownload) && items.some((item) => item?.state === "QUEUED");
  });

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
  // The job is held in its download when the pause lands, and the hold is
  // still on when the resume does, so neither answer can race the job's end.
  await holdDownloads(request);
  const tag = `q3-${Date.now()}`;
  const job = await probeJob(request, tag, HELD_ARTICLES);
  started.push(job);
  await waitFor(request, "job mid-download", (snap) => heldMidDownload(itemOf(snap, job)));

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
  // The job is held in its download when the servers go off.
  await holdDownloads(request);
  const tag = `q4-${Date.now()}`;
  const job = await probeJob(request, tag, HELD_ARTICLES);
  started.push(job);
  // Started means at least one article landed, so the laneless job has
  // progress it must keep.
  const before = await waitFor(request, "job downloading with progress", (snap) => heldMidDownload(itemOf(snap, job)));
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

  // With the hold lifted and a server back, the laneless job's download
  // resumes and runs to the end.
  await releaseDownloads();
  await updateConfiguredServer(request, "nntp", { active: true, connections: 4 });
  expect((await waitTerminal(request, job)).state).toBe("COMPLETED");
});

test("Q6 a cancelled job leaves the queue only once its history row says cancelled by operator", async ({ request }) => {
  await holdDownloads(request);
  // Both jobs are admitted under the global pause, so neither downloads
  // before the other is in the queue. Whichever the resume starts is held in
  // its download when the cancel lands, and the other cannot get the lane
  // until it ends.
  globalPauseByUs = true;
  await graphql(request, "mutation WeaverE2EQ6Pause { pauseAll }");
  const tag = `q6-${Date.now()}`;
  const jobs = [await probeJob(request, tag, HELD_ARTICLES), await probeJob(request, tag, HELD_ARTICLES)];
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
  globalPauseByUs = true;
  await graphql(request, "mutation WeaverE2EQ7Pause { pauseAll }");
  const tag = `q7-${Date.now()}`;
  const keep = await probeJob(request, tag, 1);
  // Nothing below depends on the order the jobs were admitted in, so the
  // submissions and the polls go in bounded concurrent batches.
  const jobs = await inBatches(Array.from({ length: 120 }, (_, index) => index), async (index) => {
    const submitted = await submitProbeNzb(request, `${tag}-bulk-${index}`, [
      { messageId: `${tag}-bulk-${index}@e2e.invalid`, bytes: 1 },
    ]);
    expect(submitted.jobId).not.toBeNull();
    return submitted.jobId!;
  });
  started.push(keep, ...jobs);
  await cancelAll(request, [keep, ...jobs]);
  await inBatches([keep, ...jobs], (id) => waitTerminal(request, id));

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
  await inBatches(jobs, (id) =>
    expect.poll(async () => history(request, id), { message: `history ${id} deleted`, timeout: 0 }).toBeNull());
  expect(await history(request, keep), "the row outside the batch is intact").not.toBeNull();
});

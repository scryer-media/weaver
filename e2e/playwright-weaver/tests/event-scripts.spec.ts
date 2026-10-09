import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, postProbeArticle, submitProbeNzb, test } from "./helpers";
import { literal, query, waitRows } from "./support/datastore";
import { jobState, waitTerminal } from "./support/downloads";
import { advanceClock, readClock, setClock } from "./support/e2e-clock";
import { graphqlErrors, stage } from "./support/network-flow";
import { fixtureState, setFixtureNzb } from "./support/proxy-fixture";
import {
  type ScriptRecord, gateWaiting, releaseGate, removeFixtureScripts, removeStaleGate, scriptBodies, scriptRecords,
  waitingGates, writeBareScript, writeFixturePackage, writeFixtureScript,
} from "./support/script-fixtures";
import {
  type ListEntry, type ScriptResult, WEAVER_SCRIPTS_DIR, loadStageState, nzbDocument, nzbgetRpc, queueRows, saveStageState, scriptOutput,
  scriptResults, scriptSettings, setScriptLists, submitNzb, useScripts, waitJobLessResults, waitQueueRows, waitResults,
  withControlKey,
} from "./support/script-settings";

/**
 * Queue, scheduler and feed scripts. Every fixture
 * records the environment it was given; assertions read those records, the
 * public results and the durable `script_event_queue` rows.
 *
 * Records persist across stages, so each test filters them by a script name
 * unique to the test. Queue events drain one at a time across the whole
 * server, so a test that holds a run at its gate releases it (or removes the
 * gate of a run that is gone) before it ends.
 *
 * Q12 lives in script-restart.spec.ts, the last file of the stage: its
 * initial stage leaves a run started for the restart.
 */

const token = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

/** Post `files` single-article files and submit them as one job. */
async function job(request: APIRequestContext, name: string, files = 1, extraInput: Record<string, unknown> = {}): Promise<number> {
  const articles: Array<{ messageId: string; bytes: number }> = [];
  for (let index = 0; index < files; index += 1) {
    const messageId = `${name}-${index}@e2e.invalid`;
    await postProbeArticle(messageId, 4096);
    articles.push({ messageId, bytes: 4096 });
  }
  const result = await submitProbeNzb(request, name, articles, extraInput);
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  return result.jobId!;
}

/**
 * A queue run whose job has already left the queue is skipped (only
 * NZB_DELETED and NZB_MARKED run for a gone job), and a one-file probe job can
 * finish before its NZB_ADDED run is claimed. Tests that need that run hold
 * downloads paused until it has started.
 */
const forJob = (jobId: number) => (record: ScriptRecord) => (record.env.NZBNA_NZBID ?? record.env.NZBPP_NZBID) === String(jobId);
const forEvent = (event: string) => (record: ScriptRecord) => record.env.NZBNA_EVENT === event;

function recordsOf(script: string, ...filters: Array<(record: ScriptRecord) => boolean>): ScriptRecord[] {
  return scriptRecords(script).filter(record => filters.every(filter => filter(record)));
}

async function waitRecords(script: string, count: number, ...filters: Array<(record: ScriptRecord) => boolean>): Promise<ScriptRecord[]> {
  let records: ScriptRecord[] = [];
  await expect.poll(() => (records = recordsOf(script, ...filters)).length >= count,
    { message: `${count} records of ${script}`, timeout: 0 }).toBe(true);
  return records;
}

/** Gate keys of `script` runs that are blocked right now. */
function gateKeys(script: string): string[] {
  return waitingGates().filter(name => name.startsWith(`${script}-`)).map(name => name.slice(script.length + 1));
}

/** The gate key a fixture run blocks on, as its gate script derives it. */
const gateKeyOf = (record: ScriptRecord) => record.env.NZBNA_NZBID || record.env.NZBPP_NZBID || record.env.NZBNP_NZBNAME || "none";

/**
 * Release a live run and wait until it has consumed its gate, so a gate is
 * never written twice. The gate's path is keyed by script and job, not by run:
 * the job's next queue run can recreate it the moment this one removes it, so
 * a later run's record on the same key also proves this run passed (every run
 * writes its record before it creates its gate).
 */
async function openGate(script: string, key: string | number): Promise<void> {
  const sameKey = (record: ScriptRecord) => gateKeyOf(record) === String(key);
  const before = recordsOf(script, sameKey).length;
  await releaseGate(script, key);
  await expect.poll(() => !gateWaiting(script, key) || recordsOf(script, sameKey).length > before,
    { message: `${script} run ${key} passed its gate`, timeout: 0 }).toBe(true);
}

/** Release every gate `script` blocks on until `done` holds. */
async function drainGates(script: string, done: () => Promise<boolean>, describe: string): Promise<void> {
  await expect.poll(async () => {
    for (const key of gateKeys(script)) await openGate(script, key);
    return done();
  }, { message: describe, timeout: 0 }).toBe(true);
}

/**
 * A listed script also gets a post-processing result (SKIPPED when it does not
 * declare post-processing), so a queue run's result is matched on its event.
 */
const queueRun = (script: string, event: string) => (result: ScriptResult) => result.script === script && result.event === `queue:${event}`;

const terminal = (request: APIRequestContext, jobId: number) => async () => ["COMPLETED", "FAILED"].includes(await jobState(request, jobId));

async function pauseAll(request: APIRequestContext): Promise<void> {
  await graphql(request, "mutation { pauseAll }");
}

async function resumeAll(request: APIRequestContext): Promise<void> {
  await graphql(request, "mutation { resumeAll }");
}

async function cancelJob(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { cancelJob(id: $id) }", { id });
}

async function markGood(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { markDuplicateGood(id: $id) }", { id });
}

async function addCategory(request: APIRequestContext, name: string): Promise<number> {
  return (await graphql<{ addCategory: { id: number } }>(request,
    "mutation($input: CategoryInput!) { addCategory(input: $input) { id } }", { input: { name } })).addCategory.id;
}

async function removeCategory(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { removeCategory(id: $id) { id } }", { id });
}

async function outputDir(request: APIRequestContext, id: number): Promise<string> {
  const item = (await graphql<{ historyItem: { outputDir: string | null } | null }>(request,
    "query($id: Int!) { historyItem(id: $id) { outputDir } }", { id })).historyItem;
  expect(item?.outputDir, `job ${id} output directory`).toBeTruthy();
  return item!.outputDir!;
}

async function remainingFiles(request: APIRequestContext, id: number): Promise<number | null> {
  return (await graphql<{ queueItem: { remainingFileCount: number } | null }>(request,
    "query($id: Int!) { queueItem(id: $id) { remainingFileCount } }", { id })).queueItem?.remainingFileCount ?? null;
}

const base64 = (text: string) => Buffer.from(text).toString("base64");

/** The current lists plus `entries`, so scripts other tests left in place stay. */
async function withCurrentLists(request: APIRequestContext, entries: ListEntry[]) {
  const current = (await scriptSettings(request)).lists;
  const names = new Set(entries.map(entry => entry.script));
  return { global: [...current.global.filter(entry => !names.has(entry.script)), ...entries], categories: current.categories };
}

test("Q01 an NZB_ADDED script sees the job id, name and category", async ({ request }) => {
  const tag = `q01-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_ADDED"] });
  const categoryId = await addCategory(request, tag);
  const restore = await useScripts(request, { global: [{ script }] });
  await pauseAll(request);
  try {
    const jobId = await job(request, tag, 1, { category: tag });
    const [record] = await waitRecords(script, 1, forJob(jobId));
    expect(record!.env).toMatchObject({ NZBNA_EVENT: "NZB_ADDED", NZBNA_NZBID: String(jobId), NZBNA_CATEGORY: tag });
    expect(record!.env.NZBNA_NZBNAME).toContain(tag);
    const results = await waitResults(request, jobId, all => all.some(queueRun(script, "NZB_ADDED")), `Q01 result of ${script}`);
    expect(results.find(queueRun(script, "NZB_ADDED"))!.status).toBe("SUCCEEDED");
    await resumeAll(request);
    await waitTerminal(request, jobId);
  } finally {
    await resumeAll(request);
    await restore();
    removeFixtureScripts([script]);
    await removeCategory(request, categoryId);
  }
});

test("Q02 NZB_DOWNLOADED drops the job's queued FILE_DOWNLOADED events and runs once", async ({ request }) => {
  const tag = `q02-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["FILE_DOWNLOADED", "NZB_DOWNLOADED"], gate: true });
  const restore = await useScripts(request, { global: [{ script }] }, { fileDownloadedEventInterval: 0 });
  try {
    const jobId = await job(request, tag, 3);
    // The first FILE_DOWNLOADED run holds the serial queue at its gate.
    await expect.poll(() => gateWaiting(script, jobId), { message: "first FILE_DOWNLOADED run at its gate", timeout: 0 }).toBe(true);
    const rows = await waitQueueRows(jobId, all => all.some(row => row.event === "NZB_DOWNLOADED"), "NZB_DOWNLOADED admitted");
    note("observed", `rows when NZB_DOWNLOADED was admitted: ${JSON.stringify(rows)}`);
    expect(rows.filter(row => row.event === "FILE_DOWNLOADED" && row.state === "queued")).toEqual([]);
    await drainGates(script, terminal(request, jobId), `Q02 job ${jobId} terminal`);
    expect(recordsOf(script, forJob(jobId), forEvent("NZB_DOWNLOADED"))).toHaveLength(1);
    expect((await queueRows(jobId)).filter(row => row.state === "queued")).toEqual([]);
  } finally {
    await restore();
    removeFixtureScripts([script]);
    for (const key of gateKeys(script)) await openGate(script, key);
  }
});

test("Q03 FILE_DOWNLOADED admission follows the interval setting (0, -1, 30)", async ({ request }) => {
  note("discrepancy", "The interval window is measured on the wall clock (chrono::Utc::now in enqueue_script_event), not the e2e clock, so the test cannot drive it; it asserts the window on the recorded created_at values instead.");

  // -1: the event is never admitted.
  {
    const tag = `q03-off-${token()}`;
    const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["FILE_DOWNLOADED"] });
    const restore = await useScripts(request, { global: [{ script }] }, { fileDownloadedEventInterval: -1 });
    try {
      const jobId = await job(request, tag, 3);
      expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
      expect(await queueRows(jobId, "FILE_DOWNLOADED")).toEqual([]);
      expect(recordsOf(script, forJob(jobId))).toEqual([]);
    } finally {
      await restore();
      removeFixtureScripts([script]);
    }
  }

  // 0 and 30: the first run is held, so no row reaches `done` and none is pruned.
  for (const interval of [0, 30]) {
    const tag = `q03-${interval}-${token()}`;
    const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["FILE_DOWNLOADED"], gate: true });
    const restore = await useScripts(request, { global: [{ script }] }, { fileDownloadedEventInterval: interval });
    try {
      const jobId = await job(request, tag, 4);
      await expect.poll(() => gateWaiting(script, jobId), { message: `interval ${interval}: first run at its gate`, timeout: 0 }).toBe(true);
      await expect.poll(() => remainingFiles(request, jobId), { message: `interval ${interval}: every file downloaded`, timeout: 0 }).toBe(0);
      const rows = await queueRows(jobId, "FILE_DOWNLOADED");
      note("observed", `interval ${interval}: ${JSON.stringify(rows)}`);
      expect(rows.length).toBeGreaterThanOrEqual(1);
      expect(rows.filter(row => row.state === "queued").length).toBeLessThanOrEqual(1);
      if (interval === 0) {
        expect(rows.length).toBeLessThanOrEqual(2);
      } else {
        const created = rows.map(row => Number(row.created_at));
        for (let index = 1; index < created.length; index += 1) {
          expect(created[index]! - created[index - 1]!, "a second FILE_DOWNLOADED inside 30 s is suppressed").toBeGreaterThanOrEqual(30_000);
        }
      }
      await drainGates(script, terminal(request, jobId), `Q03 interval ${interval} job terminal`);
    } finally {
      await restore();
      removeFixtureScripts([script]);
      for (const key of gateKeys(script)) await openGate(script, key);
    }
  }
});

test("Q04 URL_COMPLETED reports SUCCESS, FAILURE and SCAN_FAILURE without a job and redacts URL secrets", async ({ request }) => {
  const tag = `q04-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["URL_COMPLETED"], body: "printf 'url=%s\\n' \"$NZBNA_URL\"\n" });
  const fixture = await fixtureState(request);
  const origin = `http://${fixture.ip}:${fixture.ports.http}`;
  const messageId = `${tag}@e2e.invalid`;
  await postProbeArticle(messageId, 4096);
  await setFixtureNzb(request, nzbDocument(tag, [{ messageId, bytes: 4096 }]));
  const secret = `secret${token()}`;
  const urls: Record<string, string> = {
    SUCCESS: `${origin}/probe.nzb?apikey=${secret}`,
    FAILURE: `${origin}/${tag}-missing.nzb`,
    SCAN_FAILURE: `${origin}/not-an-nzb.nzb`,
  };
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const good = await submitNzb(request, { url: urls.SUCCESS });
    expect(good.result?.accepted, JSON.stringify(good)).toBe(true);
    const missing = await submitNzb(request, { url: urls.FAILURE });
    expect(missing.result?.accepted ?? false, JSON.stringify(missing)).toBe(false);
    const notNzb = await submitNzb(request, { url: urls.SCAN_FAILURE });
    expect(notNzb.result?.accepted ?? false, JSON.stringify(notNzb)).toBe(false);

    const records = await waitRecords(script, 3, forEvent("URL_COMPLETED"));
    for (const [status, url] of Object.entries(urls)) {
      const record = records.find(candidate => candidate.env.NZBNA_URL === url);
      expect(record, `URL_COMPLETED for ${status}`).toBeTruthy();
      expect(record!.env.NZBNA_URLSTATUS).toBe(status);
      // No job: Weaver sends the id as 0 rather than leaving it out.
      expect(record!.env.NZBNA_NZBID ?? "0").toBe("0");
    }
    note("discrepancy", "URL_COMPLETED runs carry NZBNA_NZBID=0 (the job-less context) rather than no NZBNA_NZBID.");

    const results = await waitJobLessResults(script, 3);
    for (const result of results) {
      expect(result.outputTail).not.toContain(secret);
      const output = await scriptOutput(request, result.outputId ?? result.id);
      if (output !== null) expect(output).not.toContain(secret);
    }
    expect(results.some(result => result.outputTail.includes("[REDACTED]"))).toBe(true);
    await waitTerminal(request, good.result!.jobId!);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q05 NZB_MARKED runs in the output directory with MARKSTATUS GOOD", async ({ request }) => {
  const tag = `q05-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_MARKED"] });
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const directory = await outputDir(request, jobId);
    await markGood(request, jobId);
    const [record] = await waitRecords(script, 1, forJob(jobId));
    expect(record!.env).toMatchObject({ NZBNA_EVENT: "NZB_MARKED", NZBNA_MARKSTATUS: "GOOD", NZBNA_DIRECTORY: directory });
    expect(record!.cwd).toBe(directory);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q06 NZB_DELETED follows a user cancel; deleteHistory raises none", async ({ request }) => {
  note("discrepancy", "deleteHistory does not raise NZB_DELETED; only a user cancelJob of an active job (DELETESTATUS=MANUAL) or a health abort does. The test asserts that and uses a cancel for the positive case.");
  const tag = `q06-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_DELETED"] });
  const restore = await useScripts(request, { global: [{ script }] });
  await pauseAll(request);
  try {
    const first = await job(request, tag);
    await cancelJob(request, first);
    const [deleted] = await waitRecords(script, 1, forJob(first));
    expect(deleted!.env).toMatchObject({ NZBNA_EVENT: "NZB_DELETED", NZBNA_NZBID: String(first), NZBNA_DELETESTATUS: "MANUAL" });

    await graphql(request, "mutation($id: Int!) { deleteHistory(id: $id, deleteFiles: true) { id } }", { id: first });
    // Queue events drain one at a time in admission order, so once a later
    // sentinel's event has run, any event deleteHistory raised has run too.
    const sentinel = await job(request, `${tag}-sentinel`);
    await cancelJob(request, sentinel);
    await waitRecords(script, 1, forJob(sentinel));
    expect(recordsOf(script, forJob(first))).toHaveLength(1);
  } finally {
    await resumeAll(request);
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q07 NZB_NAMED is never raised", async ({ request }) => {
  const tag = `q07-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_NAMED"] });
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    await markGood(request, jobId);
    expect(await queueRows(jobId)).toEqual([]);
    expect(recordsOf(script)).toEqual([]);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q08 nothing is admitted without an enabled subscriber or with execution off", async ({ request }) => {
  const settings = await scriptSettings(request);
  note("coverage", `strict security cannot be switched by a test (strictSecurityRefusesExecution=${settings.strictSecurityRefusesExecution}); the refusal path is covered by execution switched off, which shares the admission check.`);
  const cases: Array<{ name: string; events: string[]; enabled: boolean; executionEnabled: boolean }> = [
    { name: "unsubscribed", events: ["NZB_MARKED"], enabled: true, executionEnabled: true },
    { name: "disabled-entry", events: ["NZB_ADDED", "NZB_DOWNLOADED"], enabled: false, executionEnabled: true },
    { name: "execution-off", events: ["NZB_ADDED", "NZB_DOWNLOADED"], enabled: true, executionEnabled: false },
  ];
  for (const testCase of cases) {
    const tag = `q08-${testCase.name}-${token()}`;
    const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: testCase.events });
    const restore = await useScripts(request, { global: [{ script, enabled: testCase.enabled }] }, { executionEnabled: testCase.executionEnabled });
    try {
      const jobId = await job(request, tag);
      expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
      expect(await queueRows(jobId), testCase.name).toEqual([]);
      expect(recordsOf(script), testCase.name).toEqual([]);
    } finally {
      await restore();
      removeFixtureScripts([script]);
    }
  }
});

test("Q09 queue events run one at a time; scan scripts honour eventScriptConcurrency", async ({ request }) => {
  note("discrepancy", "eventScriptConcurrency applies to scan, feed and scheduler runs; queue events always drain one at a time. The queue half asserts serial order, the scan half asserts the concurrency of 2.");
  const tag = `q09-${token()}`;
  const queueScript = writeFixtureScript(`${tag}-queue`, { kinds: ["QUEUE"], queueEvents: ["NZB_ADDED"], gate: true });
  const scanScript = writeFixtureScript(`${tag}-scan`, { kinds: ["SCAN"], gate: true });
  const restore = await useScripts(request, { global: [{ script: queueScript }] }, { eventScriptConcurrency: 2 });
  const jobs: number[] = [];
  await pauseAll(request);
  try {
    for (let index = 0; index < 5; index += 1) jobs.push(await job(request, `${tag}-${index}`));
    const rowsSql = `SELECT job_id, state, seq FROM script_event_queue WHERE event = 'NZB_ADDED' AND job_id IN (${jobs.map(literal).join(", ")}) ORDER BY seq`;
    const released: number[] = [];
    let violation = "";
    await expect.poll(async () => {
      const started = (await query(rowsSql)).filter(row => row.state === "started");
      const waiting = gateKeys(queueScript);
      if (started.length > 1 || waiting.length > 1) {
        violation = `started ${JSON.stringify(started)}, waiting ${JSON.stringify(waiting)}`;
        return true;
      }
      for (const key of waiting) {
        await openGate(queueScript, key);
        released.push(Number(key));
      }
      return released.length === jobs.length;
    }, { message: "five NZB_ADDED runs released one at a time", timeout: 0 }).toBe(true);
    expect(violation).toBe("");
    const rows = await waitRows(rowsSql, all => all.length === jobs.length && all.every(row => row.state === "done"), "every NZB_ADDED run done");
    expect(released).toEqual(rows.map(row => Number(row.job_id)));

    await setScriptLists(request, { global: [{ script: scanScript }] });
    const submissions = [0, 1, 2].map(index => {
      const name = `${tag}-scan-${index}`;
      return submitNzb(request, { nzbBase64: base64(nzbDocument(name, [{ messageId: `${name}@e2e.invalid`, bytes: 4096 }])), filename: `${name}.nzb` });
    });
    await expect.poll(() => {
      const waiting = gateKeys(scanScript);
      if (waiting.length > 2) violation = `scan gates ${JSON.stringify(waiting)}`;
      return waiting.length >= 2;
    }, { message: "two scan runs at their gates", timeout: 0 }).toBe(true);
    expect(violation).toBe("");
    expect(recordsOf(scanScript), "the third scan run waits for a slot").toHaveLength(2);
    const releasedScans = new Set<string>();
    await expect.poll(async () => {
      const waiting = gateKeys(scanScript);
      if (waiting.length > 2) violation = `scan gates ${JSON.stringify(waiting)}`;
      for (const key of waiting) {
        await openGate(scanScript, key);
        releasedScans.add(key);
      }
      return releasedScans.size === 3;
    }, { message: "three scan runs released", timeout: 0 }).toBe(true);
    expect(violation).toBe("");
    for (const submission of await Promise.all(submissions)) {
      expect(submission.result?.accepted, JSON.stringify(submission)).toBe(true);
      jobs.push(submission.result!.jobId!);
    }
  } finally {
    await restore();
    removeFixtureScripts([queueScript, scanScript]);
    for (const script of [queueScript, scanScript]) for (const key of gateKeys(script)) await openGate(script, key);
    for (const id of jobs) await cancelJob(request, id);
    await resumeAll(request);
  }
});

test("Q10 an event script past eventScriptTimeoutSeconds ends TIMED_OUT", async ({ request }) => {
  const tag = `q10-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_ADDED"], body: `echo q10-started\n${scriptBodies.sleepForever}` });
  const restore = await useScripts(request, { global: [{ script }] }, { eventScriptTimeoutSeconds: 2 });
  await pauseAll(request);
  try {
    const jobId = await job(request, tag);
    const results = await waitResults(request, jobId, all => all.some(queueRun(script, "NZB_ADDED")), `Q10 result of ${script}`);
    const result = results.find(queueRun(script, "NZB_ADDED"))!;
    expect(result.status).toBe("TIMED_OUT");
    expect(result.errorMessage).toBe("post-processing script timed out");
    expect(result.outputTail).toContain("q10-started");
    note("observed", `the termination is recorded in errorMessage; output tail: ${JSON.stringify(result.outputTail)}`);
    await resumeAll(request);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
  } finally {
    await resumeAll(request);
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q11 cancelling a job ends its blocked NZB_ADDED run", async ({ request }) => {
  const tag = `q11-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["QUEUE"], queueEvents: ["NZB_ADDED"], gate: true });
  const restore = await useScripts(request, { global: [{ script }] });
  await pauseAll(request);
  let jobId = 0;
  try {
    jobId = await job(request, tag);
    await expect.poll(() => gateWaiting(script, jobId), { message: "NZB_ADDED run at its gate", timeout: 0 }).toBe(true);
    await cancelJob(request, jobId);
    const results = await waitResults(request, jobId, all => all.some(queueRun(script, "NZB_ADDED")), `Q11 result of ${script}`);
    const result = results.find(queueRun(script, "NZB_ADDED"))!;
    note("observed", `cancelled run: status ${result.status}, error ${result.errorMessage}`);
    expect(["CANCELLED", "FAILED", "SKIPPED"]).toContain(result.status);
    await waitQueueRows(jobId, rows => rows.every(row => row.state !== "started"), "no NZB_ADDED row left started");
  } finally {
    // The cancelled run is gone; its gate has no reader any more.
    if (jobId) removeStaleGate(script, jobId);
    await resumeAll(request);
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q13 a per-job script override replaces the global and category lists", async ({ request }) => {
  const tag = `q13-${token()}`;
  const override = writeFixtureScript(`${tag}-override`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const categoryScript = writeFixtureScript(`${tag}-category`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const globalScript = writeFixtureScript(`${tag}-global`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const categoryId = await addCategory(request, tag);
  const restore = await useScripts(request, {
    global: [{ script: globalScript }],
    categories: [{ category: tag, entries: [{ script: categoryScript }] }],
  });
  try {
    note("coverage", "The override is written only by the NZBGet facade, from `<Script>:` append parameters (weaver.* attributes are reserved on submitNzb).");
    const messageId = `${tag}@e2e.invalid`;
    await postProbeArticle(messageId, 4096);
    const nzb = base64(nzbDocument(tag, [{ messageId, bytes: 4096 }]));
    const jobId = await withControlKey(request, key => nzbgetRpc(request, key, "append",
      [`${tag}.nzb`, nzb, tag, 0, false, false, "", 0, "SCORE", [{ Name: `${override}:`, Value: "yes" }]])) as number;
    expect(jobId).toBeGreaterThan(0);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const results = await waitResults(request, jobId, all => all.some(result => result.script === override), `Q13 result of ${override}`);
    expect(results.map(result => result.script).filter(name => [override, categoryScript, globalScript].includes(name))).toEqual([override]);
    expect(recordsOf(categoryScript)).toEqual([]);
    expect(recordsOf(globalScript)).toEqual([]);
  } finally {
    await restore();
    removeFixtureScripts([override, categoryScript, globalScript]);
    await removeCategory(request, categoryId);
  }
});

test("Q14 the output store truncates, keeps a 4 KiB excerpt and evicts beyond runsPerJob", async ({ request }) => {
  const tag = `q14-${token()}`;
  const script = writeFixtureScript(tag, {
    kinds: ["POST-PROCESSING", "QUEUE"], queueEvents: ["NZB_MARKED"], body: scriptBodies.output(9 * 1024 * 1024), exitCode: 93,
  });
  const restore = await useScripts(request, { global: [{ script }] }, {
    scriptOutputRunsPerJob: 2, scriptOutputCeilingBytes: 65_536, scriptOutputRunCapBytes: 65_536, scriptOutputRingBytes: 1024 * 1024,
  });
  try {
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const isMark = (event: string) => event.includes("NZB_MARKED");
    const first = (await waitResults(request, jobId, all => all.some(result => result.script === script && !isMark(result.event)), "post-processing result"))
      .find(result => result.script === script && !isMark(result.event))!;
    expect(first.outputTruncated).toBe(true);
    expect(Buffer.byteLength(first.outputTail)).toBeLessThanOrEqual(4096);
    expect(first.outputRetained).toBe(true);
    expect(first.outputId).toBeTruthy();

    for (const count of [1, 2]) {
      await markGood(request, jobId);
      await waitResults(request, jobId, all => all.filter(result => result.script === script && isMark(result.event)).length === count, `${count} NZB_MARKED runs`);
    }
    const after = (await scriptResults(request, jobId)).find(result => result.outputId === first.outputId);
    expect(after, "the post-processing result stays listed").toBeTruthy();
    expect(after!.outputRetained).toBe(false);
    expect(await scriptOutput(request, first.outputId!)).toBeNull();
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

const PLATFORM_ENV = new Set(["PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "TZ"]);
// The shell the fixture runs in sets these itself.
const SHELL_ENV = new Set(["PWD", "SHLVL", "OLDPWD", "_"]);

test("Q15 a post-processing script gets the NZBGet environment and nothing outside the allow-list", async ({ request }) => {
  const tag = `q15-${token()}`;
  const script = writeFixturePackage(tag, { kinds: ["POST-PROCESSING"], exitCode: 93, scriptOptions: [{ name: "Label", value: "q15-label" }] });
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const [record] = await waitRecords(script, 1, forJob(jobId));
    const env = record!.env;
    expect(env.NZBPP_NZBID).toBe(String(jobId));
    for (const key of ["NZBPP_DIRECTORY", "NZBPP_FINALDIR", "NZBPP_CATEGORY", "NZBPP_STATUS", "NZBPP_TOTALSTATUS", "NZBPP_PARSTATUS",
      "NZBPP_UNPACKSTATUS", "NZBPP_HEALTH", "NZBPP_CRITICALHEALTH", "NZBOP_VERSION"]) {
      expect(env, key).toHaveProperty(key);
    }
    expect(env.NZBPO_LABEL).toBe("q15-label");
    const outside = Object.keys(env).filter(key => !PLATFORM_ENV.has(key) && !SHELL_ENV.has(key) && !/^NZB(PP|PO|OP|PR)_/.test(key));
    expect(outside).toEqual([]);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("Q16 exit codes map to statuses per adapter", async ({ request }) => {
  note("discrepancy", "NZBGet exit 0 maps to FAILED (NZBGet requires 93 for success); the WARNING case is a SABnzbd-adapter script exiting non-zero, not exit 0 with warning output.");
  const tag = `q16-${token()}`;
  const cases = [
    { script: writeFixtureScript(`${tag}-93`, { kinds: ["POST-PROCESSING"], exitCode: 93 }), status: "SUCCEEDED" },
    { script: writeFixtureScript(`${tag}-94`, { kinds: ["POST-PROCESSING"], exitCode: 94 }), status: "FAILED" },
    { script: writeFixtureScript(`${tag}-95`, { kinds: ["POST-PROCESSING"], exitCode: 95 }), status: "SKIPPED" },
    { script: writeFixtureScript(`${tag}-0`, { kinds: ["POST-PROCESSING"], body: "echo '[WARNING] q16 warning'\n", exitCode: 0 }), status: "FAILED" },
    { script: writeBareScript(`${tag}-sab-1`, { body: "echo q16 warning\n", exitCode: 1 }), status: "WARNING" },
  ];
  try {
    for (const { script, status } of cases) {
      const restore = await useScripts(request, { global: [{ script }] });
      try {
        const jobId = await job(request, script);
        await waitTerminal(request, jobId);
        const results = await waitResults(request, jobId, all => all.some(result => result.script === script), `Q16 result of ${script}`);
        expect(results.find(result => result.script === script)!.status, script).toBe(status);
      } finally {
        await restore();
      }
    }
  } finally {
    removeFixtureScripts(cases.map(testCase => testCase.script));
  }
});

test("Q17 rerunPostProcessing appends a second run; cancelJobPostProcessing ends a running script", async ({ request }) => {
  note("coverage", "No upper bound on the cancel is asserted (time bounds are not allowed in tests); the run must end CANCELLED.");
  const tag = `q17-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const gated = writeFixtureScript(`${tag}-gated`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script }] });
  let jobId = 0;
  try {
    jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const runsSql = (name: string) => `SELECT id FROM script_outputs WHERE job_id = ${literal(jobId)} AND script = ${literal(name)} AND event = 'post_processing'`;
    await waitRows(runsSql(script), rows => rows.length === 1, "first post-processing run stored");
    await graphql(request, "mutation($jobId: Int!) { rerunPostProcessing(jobId: $jobId) }", { jobId });
    await waitRows(runsSql(script), rows => rows.length === 2, "rerun stored next to the first run");
    await waitRecords(script, 2, forJob(jobId));

    await setScriptLists(request, { global: [{ script: gated }] });
    await graphql(request, "mutation($jobId: Int!) { rerunPostProcessing(jobId: $jobId) }", { jobId });
    await expect.poll(() => gateWaiting(gated, jobId), { message: "rerun script at its gate", timeout: 0 }).toBe(true);
    await graphql(request, "mutation($jobId: Int!) { cancelJobPostProcessing(jobId: $jobId) }", { jobId });
    const results = await waitResults(request, jobId, all => all.some(result => result.script === gated), "cancelled rerun result");
    const result = results.find(candidate => candidate.script === gated)!;
    expect(result.status).toBe("CANCELLED");
    expect(result.errorMessage).toBe("post-processing script was cancelled");
  } finally {
    if (jobId) removeStaleGate(gated, jobId);
    await restore();
    removeFixtureScripts([script, gated]);
  }
});

const SS01_SUBJECT = "ss01-subject";
const SS01_WITNESS = "ss01-witness";
const SS01_RULE = "ss01-startup";
const subjectRuns = (implicit: boolean) => scriptRecords(SS01_SUBJECT).filter(record => (record.env.NZBSP_TASKID === "0") === implicit);
const witnessRuns = () => scriptRecords(SS01_WITNESS).length;

/** Occurrences of the subject's `*:15` and `03:30` times in (from, to], on UTC minutes. */
function subjectOccurrences(from: Date, to: Date): number {
  let count = 0;
  for (let minute = Math.floor(from.getTime() / 60_000) + 1; minute <= Math.floor(to.getTime() / 60_000); minute += 1) {
    const at = new Date(minute * 60_000);
    if (at.getUTCMinutes() === 15) count += 1;
    if (at.getUTCHours() === 3 && at.getUTCMinutes() === 30) count += 1;
  }
  return count;
}

type Schedule = { id: string; implicit: boolean; enabled: boolean; label: string; time: string; script: string | null; runAtStartup: boolean; actionType: string };
async function schedules(request: APIRequestContext): Promise<Schedule[]> {
  return (await graphql<{ schedules: Schedule[] }>(request,
    "query { schedules { id implicit enabled label time script runAtStartup actionType } }")).schedules;
}

test("SS01 SCHEDULER task times become implicit schedules on the e2e clock; Startup runs only by explicit rule", async ({ request }) => {
  note("discrepancy", "The implicit Startup (`*`) entry is never run (enabled false, run_at_startup false); a startup run needs an explicit run_script rule with runAtStartup, which this test creates and checks after the restart.");
  if (stage() !== "initial") {
    const saved = loadStageState<{ implicit: number; explicit: number }>("ss01");
    await expect.poll(() => subjectRuns(false).length, { message: "the explicit startup rule ran at boot", timeout: 0 }).toBeGreaterThanOrEqual(saved.explicit + 1);
    expect(subjectRuns(false)).toHaveLength(saved.explicit + 1);
    expect(subjectRuns(false).at(-1)!.env.NZBSP_TASKID).not.toBe("0");
    expect(subjectRuns(true), "the implicit Startup entry did not run").toHaveLength(saved.implicit);
    for (const rule of (await schedules(request)).filter(entry => !entry.implicit && entry.label === SS01_RULE)) {
      await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id: rule.id });
    }
    const current = (await scriptSettings(request)).lists;
    await setScriptLists(request, {
      global: current.global.filter(entry => entry.script !== SS01_SUBJECT && entry.script !== SS01_WITNESS), categories: current.categories,
    });
    removeFixtureScripts([SS01_SUBJECT, SS01_WITNESS]);
    return;
  }

  writeFixtureScript(SS01_SUBJECT, { kinds: ["SCHEDULER"], taskTimes: ["*", "*:15", "03:30"] });
  writeFixtureScript(SS01_WITNESS, { kinds: ["SCHEDULER"], taskTimes: Array.from({ length: 60 }, (_, minute) => `*:${String(minute).padStart(2, "0")}`) });
  // Deliberately not restored: the restarted stage needs both scripts listed.
  await useScripts(request, await withCurrentLists(request, [{ script: SS01_WITNESS }]));

  // A jump of more than 90 minutes only moves the evaluator's baseline, so
  // step a minute at a time until the witness proves it is firing again.
  const today = readClock();
  setClock(new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() + 2, 2, 0)));
  const synced = witnessRuns();
  await expect.poll(() => {
    if (witnessRuns() > synced) return true;
    advanceClock(60_000);
    return false;
  }, { message: "the schedule evaluator fires the witness on the e2e clock", timeout: 0 }).toBe(true);

  await setScriptLists(request, await withCurrentLists(request, [{ script: SS01_SUBJECT }, { script: SS01_WITNESS }]));
  const entries = (await schedules(request)).filter(entry => entry.implicit && entry.script === SS01_SUBJECT);
  expect(entries.map(entry => entry.time).sort()).toEqual(["*", "*:15", "03:30"].sort());
  for (const entry of entries) expect(entry.id).toMatch(/^implicit-script:/);
  expect(entries.find(entry => entry.time === "*")!.enabled).toBe(false);

  let at = readClock();
  const end = new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), at.getUTCDate(), 3, 34));
  expect(at.getTime(), "the witness sync left room to cross 03:30").toBeLessThan(end.getTime() - 30 * 60_000);
  let expected = subjectRuns(true).length;
  while (at < end) {
    const next = new Date(Math.min(at.getTime() + 40 * 60_000, end.getTime()));
    expected += subjectOccurrences(at, next);
    const witness = witnessRuns();
    setClock(next);
    await expect.poll(() => witnessRuns() > witness && subjectRuns(true).length >= expected,
      { message: `evaluator reached ${next.toISOString()} with ${expected} subject runs`, timeout: 0 }).toBe(true);
    expect(subjectRuns(true)).toHaveLength(expected);
    at = next;
  }
  expect(subjectRuns(true).every(record => record.env.NZBSP_TASKID === "0")).toBe(true);

  expect(await graphqlErrors(request, "mutation($input: ScheduleInput!) { createSchedule(input: $input) { id } }",
    { input: { actionType: "run_script", script: SS01_SUBJECT, time: "*", label: `${SS01_RULE}-refused` } }))
    .toEqual([expect.stringContaining("startup scripts require runAtStartup")]);
  for (const rule of (await schedules(request)).filter(entry => !entry.implicit && entry.label === SS01_RULE)) {
    await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id: rule.id });
  }
  await graphql(request, "mutation($input: ScheduleInput!) { createSchedule(input: $input) { id } }",
    { input: { actionType: "run_script", script: SS01_SUBJECT, time: "*", runAtStartup: true, label: SS01_RULE } });
  saveStageState("ss01", { implicit: subjectRuns(true).length, explicit: subjectRuns(false).length });
});

test("SS02 implicit schedules are read-only", async ({ request }) => {
  const script = writeFixtureScript(`ss02-${token()}`, { kinds: ["SCHEDULER"], taskTimes: ["05:00"] });
  const restore = await useScripts(request, await withCurrentLists(request, [{ script }]));
  try {
    const entry = (await schedules(request)).find(candidate => candidate.implicit && candidate.script === script);
    expect(entry, "implicit schedule for the task time").toBeTruthy();
    const refused = "manifest schedules are read-only; create an explicit rule to opt into startup";
    const variables = { id: entry!.id };
    expect(await graphqlErrors(request, "mutation($id: String!) { toggleSchedule(id: $id, enabled: false) { id } }", variables))
      .toEqual([expect.stringContaining(refused)]);
    expect(await graphqlErrors(request, "mutation($id: String!, $input: ScheduleInput!) { updateSchedule(id: $id, input: $input) { id } }",
      { ...variables, input: { actionType: "run_script", script, time: "06:00" } })).toEqual([expect.stringContaining(refused)]);
    expect(await graphqlErrors(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", variables))
      .toEqual([expect.stringContaining(refused)]);
    const after = (await schedules(request)).find(candidate => candidate.id === entry!.id);
    expect(after).toMatchObject({ enabled: entry!.enabled, time: "05:00" });
    note("observed", "toggle, update and delete on an implicit entry are refused; nothing persists.");
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

async function addFeed(request: APIRequestContext, name: string, url: string, scripts: string[]): Promise<number> {
  const id = (await graphql<{ addRssFeed: { id: number } }>(request, "mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }",
    { input: { name, url, enabled: false, pollIntervalSecs: 86400, scripts } })).addRssFeed.id;
  await graphql(request, "mutation($id: Int!) { addRssRule(feedId: $id, input: { sortOrder: 0, action: ACCEPT }) { id } }", { id });
  return id;
}

async function syncFeed(request: APIRequestContext, id: number): Promise<{ itemsFetched: number; errors: string[] }> {
  return (await graphql<{ runRssSync: { itemsFetched: number; errors: string[] } }>(request,
    "mutation($id: Int!) { runRssSync(feedId: $id) { itemsFetched errors } }", { id })).runRssSync;
}

async function seenItems(request: APIRequestContext, feedId: number): Promise<Array<{ itemTitle: string; jobId: number | null }>> {
  return (await graphql<{ rssSeenItems: Array<{ itemTitle: string; jobId: number | null }> }>(request,
    "query($feedId: Int) { rssSeenItems(feedId: $feedId) { itemTitle jobId } }", { feedId })).rssSeenItems;
}

async function deleteFeed(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { deleteRssFeed(id: $id) }", { id });
}

test("FS01 a feed refuses a script that does not handle feed events", async ({ request }) => {
  const tag = `fs01-${token()}`;
  const script = writeFixtureScript(tag, { kinds: ["POST-PROCESSING"] });
  const fixture = await fixtureState(request);
  try {
    const errors = await graphqlErrors(request, "mutation($input: RssFeedInput!) { addRssFeed(input: $input) { id } }",
      { input: { name: tag, url: `http://${fixture.ip}:${fixture.ports.http}/feed.xml`, enabled: false, scripts: [script] } });
    expect(errors).toEqual([expect.stringContaining(`script '${script}' does not support feed events`)]);
  } finally {
    removeFixtureScripts([script]);
  }
});

test("FS02 a FEED script rewrites item titles, and a rewritten feed over the ceiling is refused", async ({ request }) => {
  const tag = `fs02-${token()}`;
  const fixture = await fixtureState(request);
  const messageId = `${tag}@e2e.invalid`;
  await postProbeArticle(messageId, 4096);
  await setFixtureNzb(request, nzbDocument(tag, [{ messageId, bytes: 4096 }]), tag);
  // Weaver uses Docker DNS here, so the enclosure host is rewritten to the fixture's address too.
  const rewrite = writeFixtureScript(tag, {
    kinds: ["FEED"], exitCode: 93,
    body: `${scriptBodies.rewriteFeedTitles("fs02-")}sed -i -e 's#download.proxy.test#${fixture.ip}#g' "$NZBFP_FILENAME"\n`,
  });
  const oversize = writeFixtureScript(`${tag}-big`, {
    kinds: ["FEED"], exitCode: 93, body: `head -c ${17 * 1024 * 1024} /dev/zero | tr '\\0' ' ' >> "$NZBFP_FILENAME"\n`,
  });
  const restore = await useScripts(request, {});
  const feeds: number[] = [];
  try {
    const url = `http://${fixture.ip}:${fixture.ports.http}/feed.xml`;
    const feed = await addFeed(request, tag, url, [rewrite]);
    feeds.push(feed);
    const preview = (await graphql<{ previewRssFeed: Array<{ itemTitle: string; decision: string; jobId: number | null }> }>(
      request, "mutation($id: Int!) { previewRssFeed(id: $id) { itemTitle decision jobId } }", { id: feed },
    )).previewRssFeed;
    expect(preview).toEqual([expect.objectContaining({
      itemTitle: `fs02-proxy-${tag}`, decision: "accepted", jobId: null,
    })]);
    expect(await seenItems(request, feed), "preview must not record seen items").toEqual([]);
    const report = await syncFeed(request, feed);
    expect(report.errors).toEqual([]);
    const items = await seenItems(request, feed);
    expect(items.map(item => item.itemTitle)).toContain(`fs02-proxy-${tag}`);
    expect(items.map(item => item.itemTitle)).not.toContain(`proxy-${tag}`);
    for (const item of items) if (item.jobId !== null) await waitTerminal(request, item.jobId);

    const big = await addFeed(request, `${tag}-big`, url, [oversize]);
    feeds.push(big);
    const refused = await syncFeed(request, big);
    expect(refused.errors.join("\n")).toContain("rewritten RSS feed exceeds size limit");
    expect(await seenItems(request, big)).toEqual([]);
  } finally {
    for (const id of feeds) await deleteFeed(request, id);
    await restore();
    removeFixtureScripts([rewrite, oversize]);
  }
});

test("UI01 the settings and job pages show script kinds, declarations and run statuses", async ({ cleanPage: page, request }) => {
  const tag = `ui01-${token()}`;
  const queue = writeFixtureScript(`${tag}-queue`, { kinds: ["POST-PROCESSING", "QUEUE"], queueEvents: ["NZB_ADDED", "NZB_DOWNLOADED"], exitCode: 93 });
  const scheduler = writeFixtureScript(`${tag}-scheduler`, { kinds: ["SCHEDULER"], taskTimes: ["04:00", "*:20"] });
  const scan = writeFixtureScript(`${tag}-scan`, { kinds: ["SCAN"] });
  const feed = writeFixtureScript(`${tag}-feed`, { kinds: ["FEED"], exitCode: 93 });
  const restore = await useScripts(request, { global: [queue, scheduler, scan, feed].map(script => ({ script })) });
  try {
    await page.goto("/settings/scripts/configuration");
    await expect(page.getByRole("textbox", { name: "Scripts directory", exact: true })).toHaveValue(WEAVER_SCRIPTS_DIR);
    await expect(page.getByRole("region", { name: "Event scripts and output retention", exact: true })).toBeVisible();
    await page.goto("/settings/scripts/list");
    // A discovered script's row; its name cell carries the script's file name.
    const entry = (name: string) =>
      page.getByRole("region", { name: "Discovered scripts", exact: true })
        .getByRole("button").filter({ has: page.getByText(name, { exact: true }) });
    await expect(entry(queue)).toContainText("Post-processing");
    await expect(entry(queue)).toContainText("Queue");
    await expect(entry(queue)).toContainText("Declared events:");
    await expect(entry(queue)).toContainText("NZB_ADDED");
    await expect(entry(queue)).toContainText("NZB_DOWNLOADED");
    await expect(entry(scheduler)).toContainText("Scheduler");
    await expect(entry(scheduler)).toContainText("Task times: 04:00, *:20");
    await expect(entry(scan)).toContainText("Scan");
    await expect(entry(feed)).toContainText("Feed");

    await pauseAll(request);
    const jobId = await job(request, tag);
    await waitResults(request, jobId, all => all.some(queueRun(queue, "NZB_ADDED")), "the NZB_ADDED run of the queue script");
    await resumeAll(request);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const results = (await waitResults(request, jobId, all => all.filter(result => result.script === queue).length >= 3, "three runs of the queue script"))
      .filter(result => result.script === queue);
    await page.goto(`/jobs/${jobId}`);
    const groups = new Map<string, number>();
    for (const result of results) groups.set(result.event, (groups.get(result.event) ?? 0) + 1);
    const allResults = await scriptResults(request, jobId);
    for (const [event] of groups) {
      const count = allResults.filter(result => result.event === event).length;
      await expect(page.getByText(`${event} (${count})`, { exact: true })).toBeVisible();
    }
    for (const status of new Set(results.map(result => result.status))) {
      await expect(page.getByText(status, { exact: true }).first()).toBeVisible();
    }
  } finally {
    await resumeAll(request);
    await restore();
    removeFixtureScripts([queue, scheduler, scan, feed]);
  }
});

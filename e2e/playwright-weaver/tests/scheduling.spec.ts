import fs from "node:fs";
import type { APIRequestContext } from "@playwright/test";
import {
  configuredServer, expect, graphql, nntpBodyMetrics, nntpConnectionMetrics, postProbeArticle,
  resetNntpMetrics, submitProbeNzb, test,
} from "./helpers";
import { setting, waitRows } from "./support/datastore";
import { jobState, startDownload, waitTerminal } from "./support/downloads";
import { readClock, setClock } from "./support/e2e-clock";
import { graphqlErrors, setSystemEgressQuota, stage, systemEgressQuotaUsage } from "./support/network-flow";
import { loadStageState, nzbgetRpc, saveStageState, withControlKey } from "./support/script-settings";
import {
  Rules, SCHEDULE_FIELDS, type QueueItem, type ScheduleRow, armWatchFolder, at, bodyCount, cancelledJob, completedJob,
  disarmWatchFolder, dropValidNzb, failedJob, freshDay, generalSettings, historyItem, hhmm, localOutput, note,
  profileState, queueItem, queueState, resumeAll, schedules, serverActive, token, updateSettings, waitPaused,
  waitProfile, waitQueue, watchDir, watchFolderPaused, withRules, witnessTick,
} from "./support/schedules";

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
 * Occurrence rules (times, hourly, weekdays, one-shot catch-up) and the
 * per-action hold matrix live in schedule-tracks.spec.ts.
 */

const initialOnly = () => test.skip(stage() !== "initial", "runs in the initial stage");

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
  note("gap", "The RSS scheduled-pause flag has no observable of its own, and no rule fetches a feed on demand any more, so only downloads and watch intake are asserted.");
  const directory = watchDir("t02");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t02", async rules => {
    try {
      await armWatchFolder(request, directory);
      await rules.create({ actionType: "resume", time: "10:30" });
      const pauseAll = await rules.create({ actionType: "pause_all", time: "10:00" });
      expect(pauseAll.track).toBe("DOWNLOADS");
      await witnessTick(request);
      expect((await queueState(request)).isPaused).toBe(false);

      setClock(at(day, 10, 0));
      await waitPaused(request, true, "T02 downloads paused at 10:00");
      await expect.poll(() => watchFolderPaused(request), { message: "T02 watch intake paused", timeout: 0 }).toBe(true);

      setClock(at(day, 10, 30));
      await waitPaused(request, false, "T02 downloads resumed at 10:30");
      await expect.poll(() => watchFolderPaused(request), { message: "T02 watch intake resumed", timeout: 0 }).toBe(false);
    } finally {
      await resumeAll(request);
      await disarmWatchFolder(request);
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
    await rules.create({ actionType: "speed_limit", time: "10:30", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 0 }] });
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

async function setSystemQuota(request: APIRequestContext, enabled: boolean): Promise<void> {
  await setSystemEgressQuota(request, { enabled, period: "DAILY", limitBytes: 2_000_000_000 });
}

async function systemQuotaUsed(request: APIRequestContext): Promise<number> {
  return (await systemEgressQuotaUsage(request)).usedBytes;
}

test("T09 quota metering pauses and resumes on schedule", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "t09", async rules => {
    try {
      await setSystemQuota(request, true);
      await rules.create({ actionType: "set_quota_metering", time: "10:30", quotaMeteringEnabled: true });
      const off = await rules.create({ actionType: "set_quota_metering", time: "10:00", quotaMeteringEnabled: false });
      expect(off.track).toBe("QUOTA");

      setClock(at(day, 10, 0));
      await witnessTick(request);
      const before = await systemQuotaUsed(request);
      await completedJob(request, "t09-unmetered");
      await witnessTick(request);
      expect(await systemQuotaUsed(request), "nothing metered while metering is off").toBe(before);

      setClock(at(day, 10, 30));
      await witnessTick(request);
      await completedJob(request, "t09-metered");
      await expect.poll(async () => (await systemQuotaUsed(request)) > before,
        { message: "T09 metered again after 10:30", timeout: 0 }).toBe(true);    } finally {
      await setSystemQuota(request, false);
    }
  });
});

test("T10 one-shot rules prune history", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 30));
  await withRules(request, "t10", async rules => {
    {
      const complete1 = await completedJob(request, "t10-c1");
      const complete1Dir = (await historyItem(request, complete1))!.outputDir!;
      expect(fs.existsSync(localOutput(complete1Dir))).toBe(true);
      const failed1 = await failedJob(request, "t10-f1");
      const cancelled1 = await cancelledJob(request, "t10-x1");

      const first = await rules.create({
        actionType: "prune_history", time: "10:00",
        pruneCompleted: { deleteFiles: true }, pruneFailed: { deleteFiles: false }, pruneCancelled: { deleteFiles: true },
      });
      expect(first.track).toBe("ONE_SHOT");
      await rules.create({
        actionType: "prune_history", time: "10:30",
        pruneCompleted: { deleteFiles: false }, pruneFailed: { deleteFiles: true }, pruneCancelled: { deleteFiles: false },
      });

      setClock(at(day, 9, 58));
      await witnessTick(request);
      expect(await historyItem(request, complete1), "nothing is pruned before 10:00").not.toBeNull();

      setClock(at(day, 10, 2));
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
  note("gap", "No rule fetches a feed on demand any more, so a rule's firing is covered by the hold tests, not here.");
  const day = freshDay();
  setClock(at(day, 9, 0));
  await withRules(request, "t16", async rules => {
    const created = await rules.create({ actionType: "pause_rss", time: "10:00" });
    expect(created).toMatchObject({ enabled: true, time: "10:00", actionType: "pause_rss", track: "RSS" });
    expect(created.id).toMatch(/^sched-[0-9a-f]+$/);

    const renamed = `${created.label}-edited`;
    const updated = (await rules.update(created.id, { actionType: "pause_rss", time: "10:05", label: renamed }))
      .find(row => row.id === created.id);
    expect(updated).toMatchObject({ label: renamed, time: "10:05", enabled: true });
    const toggled = (await rules.toggle(created.id, false)).find(row => row.id === created.id);
    expect(toggled?.enabled).toBe(false);
    const sibling = await rules.create({ actionType: "resume_rss", time: "10:30" });
    expect(sibling.track).toBe("RSS");
    expect((await schedules(request)).find(row => row.id === created.id)).toMatchObject({ label: renamed, time: "10:05", enabled: false });

    expect((await rules.toggle(created.id, true)).find(row => row.id === created.id)?.enabled).toBe(true);
    const afterDelete = await rules.delete(created.id);
    expect(afterDelete.some(row => row.id === created.id)).toBe(false);
    expect(afterDelete.some(row => row.id === sibling.id)).toBe(true);
    await rules.delete(sibling.id);
    const remaining = await schedules(request);
    expect(remaining.some(row => row.id === created.id || row.id === sibling.id)).toBe(false);
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

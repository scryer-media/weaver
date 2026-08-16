import fs from "node:fs";
import {
  expect,
  expectSetting,
  graphql,
  metricValue,
  metrics,
  nntpBodyMetrics,
  postProbeArticle,
  resetNntpMetrics,
  submitProbeNzb,
  test,
  updateConfiguredServer,
} from "./helpers";

const globalLimit = 512 * 1024;
const serverLimit = 256 * 1024;
const scheduledLimit = 128 * 1024;
const scheduleLabel = "e2e scheduled rate persisted";
const stage = process.env.E2E_WEAVER_RATE_LIMIT_STAGE ?? "initial";

test(`global, scheduled, and per-server limits: ${stage}`, async ({ request }) => {
  const clockFile = process.env.E2E_WEAVER_CLOCK_FILE;
  expect(clockFile, "release harness must mount the deterministic Weaver clock").toBeTruthy();
  setClock(clockFile!, "2032-06-01T03:14:05Z");

  if (stage === "restart-verify") {
    await verifyPersistedLimits(request);
    const persistedRate = await runRateProbe(request, "restart-scheduled");
    expect(persistedRate).toBeGreaterThan(scheduledLimit * 0.3);
    expect(persistedRate).toBeLessThan(scheduledLimit * 1.8);
    await restoreUnlimited(request);
    return;
  }
  expect(stage).toBe("initial");

  await graphql(
    request,
    "mutation WeaverE2EGlobalRate($input: GeneralSettingsInput!) { updateSettings(input: $input) { maxDownloadSpeed } }",
    { input: { maxDownloadSpeed: globalLimit } },
  );
  await expectSetting(request, "maxDownloadSpeed", globalLimit);
  const scheduleId = await createSchedule(request);
  const server = await setServerLimit(request, serverLimit);
  await expectScheduledLimit(request, scheduledLimit);

  let body = await metrics(request);
  expect(
    metricValue(body, "weaver_server_download_rate_limit_bytes_per_second", {
      server_id: String(server.id),
    }),
  ).toBe(serverLimit);

  const scheduledRate = await runRateProbe(request, "scheduled");
  await deleteSchedule(request, scheduleId);
  await expectScheduledLimit(request, 0);

  const perServerRate = await runRateProbe(request, "per-server");
  await setServerLimit(request, 0);
  const globalRate = await runRateProbe(request, "global");

  expect(scheduledRate).toBeGreaterThan(scheduledLimit * 0.3);
  expect(scheduledRate).toBeLessThan(scheduledLimit * 1.8);
  expect(perServerRate).toBeGreaterThan(scheduledRate * 1.25);
  expect(perServerRate).toBeLessThan(serverLimit * 1.8);
  expect(globalRate).toBeGreaterThan(perServerRate * 1.25);
  expect(globalRate).toBeLessThan(globalLimit * 1.8);

  body = await metrics(request);
  expect(
    metricValue(body, "weaver_server_download_throttle_seconds_total", {
      server_id: String(server.id),
    }) ?? 0,
  ).toBeGreaterThan(0);
  expect(
    metricValue(body, "weaver_server_download_lifetime_bytes", {
      server_id: String(server.id),
    }) ?? 0,
  ).toBeGreaterThan(0);

  // Leave the full precedence chain configured so the harness restart proves
  // that global, scheduled, and per-server settings all persist.
  await setServerLimit(request, serverLimit);
  await createSchedule(request);
  await expectScheduledLimit(request, scheduledLimit);
});

function setClock(clockFile: string, instant: string): void {
  const owner = fs.statSync(clockFile);
  const pending = `${clockFile}.${process.pid}.tmp`;
  fs.writeFileSync(pending, `${instant}\n`, { mode: 0o600 });
  fs.chownSync(pending, owner.uid, owner.gid);
  fs.chmodSync(pending, owner.mode & 0o777);
  fs.renameSync(pending, clockFile);
}

async function runRateProbe(
  request: Parameters<typeof submitProbeNzb>[0],
  label: string,
): Promise<number> {
  const bytesPerArticle = 512 * 1024;
  // Six equal segments keep the workload flat while amortizing the global
  // and per-server token buckets' intentional one-second burst capacity.
  const articles = Array.from({ length: 6 }, (_, index) => ({
    messageId: `weaver-rate-${stage}-${label}-${index + 1}@e2e.invalid`,
    bytes: bytesPerArticle,
  }));
  for (const article of articles) {
    await postProbeArticle(article.messageId, article.bytes);
  }
  await resetNntpMetrics();
  const startedAtMS = Date.now();
  const submission = await submitProbeNzb(
    request,
    `weaver-rate-${stage}-${label}`,
    articles,
  );
  expect(submission.accepted).toBeTruthy();
  expect(submission.jobId).not.toBeNull();

  let observed = await nntpBodyMetrics();
  await expect
    .poll(async () => {
      observed = await nntpBodyMetrics();
      return observed.body_transfers;
    }, { timeout: 45_000, intervals: [250, 500, 1_000] })
    .toBe(articles.length);
  // Provider timestamps stop when bytes enter Weaver, before Weaver's shared
  // per-server limiter finishes draining the charged throttle debt. Wait only
  // for this probe to leave the public queue so the measured interval includes
  // that debt; this is synchronization, not a download-pipeline assertion.
  await expect
    .poll(async () => {
      const data = await graphql<{ queueItems: Array<{ id: number }> }>(
        request,
        "query WeaverE2ERateQueue { queueItems { id } }",
      );
      return data.queueItems.some(({ id }) => id === submission.jobId);
    }, { timeout: 30_000, intervals: [100, 250, 500] })
    .toBe(false);
  const elapsedSeconds = Math.max(0.001, (Date.now() - startedAtMS) / 1_000);
  // The fake provider reports NNTP BODY wire bytes. Keep this sanity check
  // broad enough for yEnc control lines and per-line CRLF framing; the wire
  // count itself is the numerator for the measured effective rate.
  const requestedBytes = bytesPerArticle * articles.length;
  expect(observed.body_bytes).toBeGreaterThanOrEqual(requestedBytes);
  expect(observed.body_bytes).toBeLessThan(requestedBytes * 1.03);
  return observed.body_bytes / elapsedSeconds;
}

async function verifyPersistedLimits(
  request: Parameters<typeof submitProbeNzb>[0],
): Promise<void> {
  await expectSetting(request, "maxDownloadSpeed", globalLimit);
  const data = await graphql<{
    servers: Array<{ host: string; maxDownloadSpeed: number }>;
    schedules: Array<{ id: string; label: string; speedLimitBytes: number }>;
  }>(
    request,
    `query WeaverE2EPersistedRates {
      servers { host maxDownloadSpeed }
      schedules { id label speedLimitBytes }
    }`,
  );
  expect(data.servers.find(({ host }) => host === "nntp")?.maxDownloadSpeed).toBe(serverLimit);
  expect(data.schedules).toContainEqual(
    expect.objectContaining({ label: scheduleLabel, speedLimitBytes: scheduledLimit }),
  );
  await expectScheduledLimit(request, scheduledLimit);
}

async function setServerLimit(
  request: Parameters<typeof submitProbeNzb>[0],
  maxDownloadSpeed: number,
) {
  return updateConfiguredServer(request, "nntp", {
    connections: 4,
    maxDownloadSpeed,
    downloadQuota: {
      enabled: false,
      limitBytes: 0,
      period: "ONE_TIME",
      resetTimeMinutesLocal: 0,
      weeklyResetWeekday: "MON",
      monthlyResetDay: 1,
    },
  });
}

async function createSchedule(
  request: Parameters<typeof submitProbeNzb>[0],
): Promise<string> {
  const data = await graphql<{
    createSchedule: Array<{ id: string; label: string }>;
  }>(
    request,
    `mutation WeaverE2EScheduledRate($input: ScheduleInput!) {
      createSchedule(input: $input) { id label }
    }`,
    {
      input: {
        enabled: true,
        label: scheduleLabel,
        days: [],
        time: "03:14",
        actionType: "speed_limit",
        speedLimitBytes: scheduledLimit,
      },
    },
  );
  const created = data.createSchedule.find(({ label }) => label === scheduleLabel);
  expect(created, `created schedule ${scheduleLabel}`).toBeTruthy();
  return created!.id;
}

async function deleteSchedule(
  request: Parameters<typeof submitProbeNzb>[0],
  id: string,
): Promise<void> {
  await graphql(
    request,
    "mutation WeaverE2EDeleteScheduledRate($id: String!) { deleteSchedule(id: $id) { id } }",
    { id },
  );
}

async function expectScheduledLimit(
  request: Parameters<typeof submitProbeNzb>[0],
  expected: number,
): Promise<void> {
  await expect
    .poll(async () => {
      const data = await graphql<{
        globalQueueState: { downloadBlock: { scheduledSpeedLimit: number } };
      }>(
        request,
        `query WeaverE2EScheduledRateState {
          globalQueueState { downloadBlock { scheduledSpeedLimit } }
        }`,
      );
      return data.globalQueueState.downloadBlock.scheduledSpeedLimit;
    }, { timeout: 10_000, intervals: [100, 250, 500] })
    .toBe(expected);
}

async function restoreUnlimited(
  request: Parameters<typeof submitProbeNzb>[0],
): Promise<void> {
  const data = await graphql<{ schedules: Array<{ id: string; label: string }> }>(
    request,
    "query WeaverE2ERateSchedules { schedules { id label } }",
  );
  for (const schedule of data.schedules.filter(({ label }) => label === scheduleLabel)) {
    await deleteSchedule(request, schedule.id);
  }
  await setServerLimit(request, 0);
  await graphql(
    request,
    "mutation WeaverE2ERestoreUnlimited($input: GeneralSettingsInput!) { updateSettings(input: $input) { maxDownloadSpeed } }",
    { input: { maxDownloadSpeed: 0 } },
  );
  await expectSetting(request, "maxDownloadSpeed", 0);
  await expectScheduledLimit(request, 0);
  const body = await metrics(request);
  const server = await graphql<{ servers: Array<{ id: number; host: string }> }>(
    request,
    "query WeaverE2ERateServer { servers { id host } }",
  );
  const serverId = String(server.servers.find(({ host }) => host === "nntp")!.id);
  expect(
    metricValue(body, "weaver_server_download_rate_limit_bytes_per_second", {
      server_id: serverId,
    }),
  ).toBe(0);
}

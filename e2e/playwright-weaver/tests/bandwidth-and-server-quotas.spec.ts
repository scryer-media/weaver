import fs from "node:fs";
import type { APIRequestContext } from "@playwright/test";
import {
  expect,
  graphql,
  metricValue,
  metrics,
  postProbeArticle,
  submitProbeNzb,
  test,
  updateConfiguredServer,
} from "./helpers";

type QuotaSnapshot = {
  enabled: boolean;
  period: string;
  limitBytes: number;
  lifetimeBytes: number;
  usedBytes: number;
  reservedBytes: number;
  remainingBytes: number;
  blocked: boolean;
  windowStartsAtEpochMs: number | null;
  windowEndsAtEpochMs: number | null;
  timezoneName: string;
};

type DownloadBlockSnapshot = {
  kind: string;
  capEnabled: boolean;
  period: string | null;
  usedBytes: number;
  limitBytes: number;
  remainingBytes: number;
  reservedBytes: number;
  windowStartsAtEpochMs: number | null;
  windowEndsAtEpochMs: number | null;
  timezoneName: string;
  scheduledSpeedLimit: number;
};

type QuotaInput = {
  enabled: boolean;
  limitBytes: number;
  period: "ONE_TIME" | "DAILY" | "WEEKLY" | "MONTHLY";
  resetTimeMinutesLocal: number;
  weeklyResetWeekday: "MON";
  monthlyResetDay: number;
};

const stage = process.env.E2E_WEAVER_QUOTA_STAGE ?? "initial";
const ispLimit = 256 * 1024;
const dailyServerLimit = 768 * 1024;
const initialProbeArticleBytes = 64 * 1024;
const initialProbeArticleCount = ispLimit / initialProbeArticleBytes;
const schedulePauseLabel = "e2e quota scheduled pause";
const scheduleLimitLabel = "e2e quota scheduled speed";
const scheduledLimit = 4 * 1024 * 1024;

test(`ISP and per-server quota behavior: ${stage}`, async ({ request }) => {
  expect(
    process.env.E2E_WEAVER_CLOCK_FILE,
    "release harness must mount the deterministic Weaver clock",
  ).toBeTruthy();

  if (stage === "initial") {
    await exerciseInitialAccounting(request);
    return;
  }
  expect(stage).toBe("restart-verify");
  await verifyRestartPersistenceAndDailyReset(request);
  await exerciseManualResetAndPeriodWindows(request);
  await exerciseScheduleAndManualPausePrecedence(request);
  await exerciseMultiServerFallbackAndGlobalBlock(request);
});

async function exerciseInitialAccounting(request: APIRequestContext): Promise<void> {
  setClock("2032-01-01T12:00:00Z");
  await configureIspCap(request, true, ispLimit);
  const primary = await configureServerQuota(
    request,
    "nntp",
    quota("DAILY", dailyServerLimit),
    { connections: 1, maxDownloadSpeed: 64 * 1024, priority: 0 },
  );
  await configureServerQuota(request, "nntp2", quota("ONE_TIME", 0, false), {
    active: false,
    priority: 1,
  });

  const initialQuota = await readServerQuota(request, primary.id);
  expect(initialQuota).toMatchObject({
    enabled: true,
    period: "DAILY",
    limitBytes: dailyServerLimit,
    lifetimeBytes: 0,
    usedBytes: 0,
    reservedBytes: 0,
    remainingBytes: dailyServerLimit,
    blocked: false,
  });
  expect(initialQuota.windowStartsAtEpochMs).not.toBeNull();
  expect(initialQuota.windowEndsAtEpochMs).not.toBeNull();
  expect(initialQuota.timezoneName).toMatch(/UTC|GMT/);

  const initialArticles = Array.from({ length: initialProbeArticleCount }, (_, index) => ({
    messageId: `weaver-quota-initial-${index + 1}@e2e.invalid`,
    bytes: initialProbeArticleBytes,
  }));
  for (const article of initialArticles) {
    await postProbeArticle(article.messageId, article.bytes);
  }
  const submitted = await submitProbeNzb(request, "weaver-quota-initial", initialArticles);
  expect(submitted).toMatchObject({ accepted: true });

  let body = "";
  await expect
    .poll(async () => {
      const [serverQuota, block, metricBody] = await Promise.all([
        readServerQuota(request, primary.id),
        readDownloadBlock(request),
        metrics(request),
      ]);
      body = metricBody;
      return serverQuota.reservedBytes > 0
        && block.reservedBytes > 0
        && (metricValue(body, "weaver_server_download_quota_reserved_bytes", {
          server_id: String(primary.id),
        }) ?? 0) > 0
        && (metricValue(body, "weaver_bandwidth_cap_reserved_bytes") ?? 0) > 0;
    }, { timeout: 15_000, intervals: [100, 250, 500] })
    .toBe(true);

  expect(
    metricValue(body, "weaver_server_download_quota_reserved_bytes", {
      server_id: String(primary.id),
    }) ?? 0,
  ).toBeGreaterThan(0);
  expect(metricValue(body, "weaver_bandwidth_cap_reserved_bytes") ?? 0).toBeGreaterThan(0);

  await expect
    .poll(async () => {
      const [serverQuota, block] = await Promise.all([
        readServerQuota(request, primary.id),
        readDownloadBlock(request),
      ]);
      return block.kind === "ISP_CAP"
        && block.usedBytes > 0
        && block.reservedBytes === 0
        && serverQuota.usedBytes > 0
        && serverQuota.reservedBytes === 0;
    }, { timeout: 30_000, intervals: [250, 500, 1_000] })
    .toBe(true);
  const [serverQuota, block] = await Promise.all([
    readServerQuota(request, primary.id),
    readDownloadBlock(request),
  ]);
  expect(serverQuota.usedBytes).toBeGreaterThan(0);
  expect(serverQuota.reservedBytes).toBe(0);
  expect(serverQuota.remainingBytes).toBe(dailyServerLimit - serverQuota.usedBytes);
  expect(serverQuota.blocked).toBe(false);
  expect(block).toMatchObject({
    kind: "ISP_CAP",
    capEnabled: true,
    period: "DAILY",
    limitBytes: ispLimit,
    reservedBytes: 0,
  });
  // Conservative pre-reservation parks the next article once the remaining
  // allowance is smaller than its estimate, so the cap trips with used at or
  // below the limit — never above it.
  expect(block.usedBytes).toBeGreaterThan(0);
  expect(block.usedBytes).toBeLessThanOrEqual(ispLimit);
  expect(block.remainingBytes).toBe(ispLimit - block.usedBytes);
  expect(block.remainingBytes).toBeLessThan(initialProbeArticleBytes);

  body = await metrics(request);
  expect(
    metricValue(body, "weaver_server_download_quota_used_bytes", {
      server_id: String(primary.id),
    }),
  ).toBe(serverQuota.usedBytes);
  expect(
    metricValue(body, "weaver_server_download_quota_remaining_bytes", {
      server_id: String(primary.id),
    }),
  ).toBe(serverQuota.remainingBytes);
  expect(metricValue(body, "weaver_bandwidth_cap_used_bytes")).toBe(block.usedBytes);
  expect(metricValue(body, "weaver_bandwidth_cap_remaining_bytes")).toBe(
    block.remainingBytes,
  );
  expect(
    metricValue(body, "weaver_pipeline_download_gate", { reason: "isp_cap" }),
  ).toBe(1);
}

async function verifyRestartPersistenceAndDailyReset(
  request: APIRequestContext,
): Promise<void> {
  const data = await graphql<{
    settings: {
      ispBandwidthCap: { enabled: boolean; period: string; limitBytes: number };
    };
    servers: Array<{
      id: number;
      host: string;
      maxDownloadSpeed: number;
      downloadQuota: QuotaSnapshot;
    }>;
  }>(
    request,
    `query WeaverE2EQuotaRestartState {
      settings { ispBandwidthCap { enabled period limitBytes } }
      servers {
        id host maxDownloadSpeed
        downloadQuota {
          enabled period limitBytes lifetimeBytes usedBytes reservedBytes remainingBytes blocked
          windowStartsAtEpochMs windowEndsAtEpochMs timezoneName
        }
      }
    }`,
  );
  expect(data.settings.ispBandwidthCap).toEqual({
    enabled: true,
    period: "DAILY",
    limitBytes: ispLimit,
  });
  const primary = data.servers.find(({ host }) => host === "nntp");
  expect(primary).toBeTruthy();
  expect(primary!.maxDownloadSpeed).toBe(64 * 1024);
  expect(primary!.downloadQuota).toMatchObject({
    enabled: true,
    period: "DAILY",
    limitBytes: dailyServerLimit,
    reservedBytes: 0,
  });
  expect(primary!.downloadQuota.usedBytes).toBeGreaterThan(0);

  // The parked-on-cap presentation is runtime state: after a restart it
  // re-arms on the first dispatch attempt that fails reservation, so poll
  // instead of asserting immediately.
  await expect
    .poll(async () => (await readDownloadBlock(request)).kind, {
      timeout: 30_000,
      intervals: [250, 500, 1_000],
    })
    .toBe("ISP_CAP");
  const persistedBlock = await readDownloadBlock(request);
  expect(persistedBlock).toMatchObject({
    kind: "ISP_CAP",
    capEnabled: true,
    period: "DAILY",
    limitBytes: ispLimit,
    reservedBytes: 0,
  });
  expect(persistedBlock.usedBytes).toBeGreaterThan(0);
  expect(persistedBlock.usedBytes).toBeLessThanOrEqual(ispLimit);
  expect(persistedBlock.remainingBytes).toBeLessThan(initialProbeArticleBytes);
  const persistedWindowEnd = persistedBlock.windowEndsAtEpochMs;
  const persistedServerWindowEnd = primary!.downloadQuota.windowEndsAtEpochMs;
  expect(persistedWindowEnd).not.toBeNull();
  expect(persistedServerWindowEnd).not.toBeNull();

  let body = await metrics(request);
  expect(metricValue(body, "weaver_bandwidth_cap_used_bytes")).toBe(
    persistedBlock.usedBytes,
  );
  expect(
    metricValue(body, "weaver_server_download_quota_used_bytes", {
      server_id: String(primary!.id),
    }),
  ).toBe(primary!.downloadQuota.usedBytes);

  // The initial stage saturates the cap, which parks the remaining article(s)
  // rather than failing them. Cancel the outstanding job so the daily-window
  // rollover is observed in isolation: otherwise the parked work correctly
  // resumes the instant the new window opens and re-accrues usage, which is
  // right behavior but not the clean-reset state this assertion targets.
  await cancelOutstandingJobs(request);
  setClock("2032-01-02T00:00:05Z");
  await expect
    .poll(async () => {
      const [quotaState, block] = await Promise.all([
        readServerQuota(request, primary!.id),
        readDownloadBlock(request),
      ]);
      return {
        serverUsed: quotaState.usedBytes,
        serverReserved: quotaState.reservedBytes,
        serverRemaining: quotaState.remainingBytes,
        ispUsed: block.usedBytes,
        ispReserved: block.reservedBytes,
        ispRemaining: block.remainingBytes,
        kind: block.kind,
      };
    }, { timeout: 15_000, intervals: [100, 250, 500] })
    .toEqual({
      serverUsed: 0,
      serverReserved: 0,
      serverRemaining: dailyServerLimit,
      ispUsed: 0,
      ispReserved: 0,
      ispRemaining: ispLimit,
      kind: "NONE",
    });
  const [resetQuota, resetBlock] = await Promise.all([
    readServerQuota(request, primary!.id),
    readDownloadBlock(request),
  ]);
  expect(resetQuota.windowStartsAtEpochMs).toBeGreaterThanOrEqual(
    persistedServerWindowEnd!,
  );
  expect(resetBlock.windowStartsAtEpochMs).toBeGreaterThanOrEqual(persistedWindowEnd!);

  body = await metrics(request);
  expect(metricValue(body, "weaver_bandwidth_cap_used_bytes")).toBe(0);
  expect(metricValue(body, "weaver_bandwidth_cap_reserved_bytes")).toBe(0);
  expect(metricValue(body, "weaver_bandwidth_cap_remaining_bytes")).toBe(ispLimit);
  expect(
    metricValue(body, "weaver_server_download_quota_used_bytes", {
      server_id: String(primary!.id),
    }),
  ).toBe(0);
}

async function exerciseManualResetAndPeriodWindows(
  request: APIRequestContext,
): Promise<void> {
  const primary = await configureServerQuota(
    request,
    "nntp",
    quota("DAILY", dailyServerLimit),
    { maxDownloadSpeed: 0, connections: 1, priority: 0 },
  );
  await runProbe(request, "manual-reset", 48 * 1024, ["nntp"]);
  await expect
    .poll(async () => (await readServerQuota(request, primary.id)).usedBytes, {
      timeout: 20_000,
      intervals: [100, 250, 500],
    })
    .toBeGreaterThan(0);
  const ispUsedBeforeReset = (await readDownloadBlock(request)).usedBytes;
  expect(ispUsedBeforeReset).toBeGreaterThan(0);
  const lifetimeBeforeReset = (await readServerQuota(request, primary.id)).lifetimeBytes;
  const reset = await resetServerQuota(request, primary.id);
  expect(reset).toMatchObject({
    usedBytes: 0,
    reservedBytes: 0,
    remainingBytes: dailyServerLimit,
    blocked: false,
  });
  expect(reset.lifetimeBytes).toBe(lifetimeBeforeReset);
  expect((await readDownloadBlock(request)).usedBytes).toBe(ispUsedBeforeReset);
  let body = await metrics(request);
  expect(
    metricValue(body, "weaver_server_download_quota_used_bytes", {
      server_id: String(primary.id),
    }),
  ).toBe(0);

  await configureIspCap(request, false, 0);
  await expect
    .poll(async () => await readDownloadBlock(request), {
      timeout: 10_000,
      intervals: [100, 250, 500],
    })
    .toMatchObject({ kind: "NONE", capEnabled: false });

  setClock("2032-01-02T12:00:00Z");
  await configureServerQuota(request, "nntp", quota("ONE_TIME", 256 * 1024), {
    maxDownloadSpeed: 0,
    connections: 1,
    priority: 0,
  });
  await resetServerQuota(request, primary.id);
  await runProbe(request, "one-time", 32 * 1024, ["nntp"]);
  await expect
    .poll(async () => (await readServerQuota(request, primary.id)).usedBytes, {
      timeout: 20_000,
      intervals: [100, 250, 500],
    })
    .toBeGreaterThan(0);
  const oneTime = await readServerQuota(request, primary.id);
  expect(oneTime).toMatchObject({
    period: "ONE_TIME",
    windowStartsAtEpochMs: null,
    windowEndsAtEpochMs: null,
  });
  setClock("2032-01-03T12:00:00Z");
  await new Promise((resolve) => setTimeout(resolve, 750));
  expect((await readServerQuota(request, primary.id)).usedBytes).toBe(oneTime.usedBytes);

  await exerciseResettingServerPeriod(
    request,
    primary.id,
    "WEEKLY",
    "2032-01-04T23:59:30Z",
    "2032-01-05T00:00:05Z",
    "weekly",
  );
  await exerciseResettingServerPeriod(
    request,
    primary.id,
    "MONTHLY",
    "2032-01-31T23:59:30Z",
    "2032-02-01T00:00:05Z",
    "monthly",
  );

  body = await metrics(request);
  expect(metricValue(body, "weaver_bandwidth_cap_enabled")).toBe(0);
  expect(
    metricValue(body, "weaver_server_download_quota_enabled", {
      server_id: String(primary.id),
    }),
  ).toBe(1);
}

async function exerciseResettingServerPeriod(
  request: APIRequestContext,
  serverId: number,
  period: "WEEKLY" | "MONTHLY",
  beforeBoundary: string,
  afterBoundary: string,
  label: string,
): Promise<void> {
  setClock(beforeBoundary);
  await configureServerQuota(request, "nntp", quota(period, 256 * 1024), {
    maxDownloadSpeed: 0,
    connections: 1,
    priority: 0,
  });
  await resetServerQuota(request, serverId);
  await runProbe(request, label, 32 * 1024, ["nntp"]);
  await expect
    .poll(async () => (await readServerQuota(request, serverId)).usedBytes, {
      timeout: 20_000,
      intervals: [100, 250, 500],
    })
    .toBeGreaterThan(0);
  const before = await readServerQuota(request, serverId);
  expect(before.period).toBe(period);
  expect(before.windowStartsAtEpochMs).not.toBeNull();
  expect(before.windowEndsAtEpochMs).not.toBeNull();

  setClock(afterBoundary);
  await expect
    .poll(async () => await readServerQuota(request, serverId), {
      timeout: 15_000,
      intervals: [100, 250, 500],
    })
    .toMatchObject({
      period,
      usedBytes: 0,
      reservedBytes: 0,
      remainingBytes: 256 * 1024,
      blocked: false,
    });
  const after = await readServerQuota(request, serverId);
  expect(after.windowStartsAtEpochMs).toBeGreaterThanOrEqual(before.windowEndsAtEpochMs!);
}

async function exerciseScheduleAndManualPausePrecedence(
  request: APIRequestContext,
): Promise<void> {
  setClock("2032-02-02T12:00:05Z");
  const pauseSchedule = await createSchedule(
    request,
    schedulePauseLabel,
    "pause",
    0,
    "12:00",
  );
  await expectBlockKind(request, "SCHEDULED");

  await graphql(request, "mutation WeaverE2EManualPause { pauseAll }");
  await expectBlockKind(request, "MANUAL_PAUSE");
  let body = await metrics(request);
  expect(
    metricValue(body, "weaver_pipeline_download_gate", { reason: "manual_pause" }),
  ).toBe(1);

  await graphql(request, "mutation WeaverE2EResumeAfterManualPause { resumeAll }");
  await expectBlockKind(request, "NONE");
  await deleteSchedule(request, pauseSchedule);

  const speedSchedule = await createSchedule(
    request,
    scheduleLimitLabel,
    "speed_limit",
    scheduledLimit,
    "12:00",
  );
  await expect
    .poll(async () => (await readDownloadBlock(request)).scheduledSpeedLimit, {
      timeout: 10_000,
      intervals: [100, 250, 500],
    })
    .toBe(scheduledLimit);
  expect((await readDownloadBlock(request)).kind).toBe("NONE");

  body = await metrics(request);
  expect(metricValue(body, "weaver_pipeline_download_gate", { reason: "none" })).toBe(1);
  expect(speedSchedule).toBeTruthy();
}

async function exerciseMultiServerFallbackAndGlobalBlock(
  request: APIRequestContext,
): Promise<void> {
  const articleBytes = 64 * 1024;
  // Weaver reserves a conservative estimate before each BODY (raw + raw/16 +
  // 1024; see bandwidth_reservation_estimate) and reconciles down to the
  // actual afterward, so a quota must clear one estimate to admit one article.
  // Size the per-server quotas in estimate units, not raw article bytes.
  const oneArticleQuota = reservationEstimate(articleBytes);
  const primary = await configureServerQuota(
    request,
    "nntp",
    quota("ONE_TIME", oneArticleQuota),
    { active: true, connections: 1, maxDownloadSpeed: 0, priority: 0 },
  );
  const secondary = await configureServerQuota(
    request,
    "nntp2",
    quota("ONE_TIME", 4 * oneArticleQuota),
    { active: true, connections: 1, maxDownloadSpeed: 0, priority: 1 },
  );
  await resetServerQuota(request, primary.id);
  await resetServerQuota(request, secondary.id);

  const fallbackJobId = await runMirroredProbe(request, "fallback", articleBytes, 4);
  await waitForCompletedJob(request, fallbackJobId);
  await expect
    .poll(async () => {
      const [primaryQuota, secondaryQuota] = await Promise.all([
        readServerQuota(request, primary.id),
        readServerQuota(request, secondary.id),
      ]);
      return primaryQuota.blocked
        && primaryQuota.reservedBytes === 0
        && secondaryQuota.usedBytes > 0
        && secondaryQuota.reservedBytes === 0
        && secondaryQuota.remainingBytes > 0
        && !secondaryQuota.blocked;
    }, { timeout: 30_000, intervals: [100, 250, 500] })
    .toBe(true);
  const [primaryBlocked, secondaryEligible, fallbackBlock] = await Promise.all([
    readServerQuota(request, primary.id),
    readServerQuota(request, secondary.id),
    readDownloadBlock(request),
  ]);
  expect(primaryBlocked).toMatchObject({
    blocked: true,
    reservedBytes: 0,
  });
  // Conservative reservation stops admitting once the remaining headroom is
  // smaller than one article's estimate, so the blocked primary settles with a
  // sub-article remainder rather than exactly zero.
  expect(primaryBlocked.remainingBytes).toBeLessThan(oneArticleQuota);
  expect(secondaryEligible.usedBytes).toBeGreaterThan(0);
  expect(secondaryEligible.remainingBytes).toBeGreaterThan(0);
  expect(secondaryEligible.blocked).toBe(false);
  expect(fallbackBlock.kind).not.toBe("SERVER_QUOTA");

  let body = await metrics(request);
  expect(
    metricValue(body, "weaver_server_download_quota_blocked", {
      server_id: String(primary.id),
    }),
  ).toBe(1);
  expect(
    metricValue(body, "weaver_server_download_quota_used_bytes", {
      server_id: String(secondary.id),
    }),
  ).toBe(secondaryEligible.usedBytes);

  const secondaryLimit = secondaryEligible.usedBytes + oneArticleQuota;
  await configureServerQuota(
    request,
    "nntp2",
    quota("ONE_TIME", secondaryLimit),
    { active: true, connections: 1, maxDownloadSpeed: 0, priority: 1 },
  );
  await runMirroredProbe(request, "all-exhausted", articleBytes, 2);
  await expect
    .poll(async () => {
      const [primaryQuota, secondaryQuota, block] = await Promise.all([
        readServerQuota(request, primary.id),
        readServerQuota(request, secondary.id),
        readDownloadBlock(request),
      ]);
      return {
        primaryBlocked: primaryQuota.blocked,
        secondaryBlocked: secondaryQuota.blocked,
        kind: block.kind,
        scheduledSpeedLimit: block.scheduledSpeedLimit,
      };
    }, { timeout: 30_000, intervals: [100, 250, 500] })
    .toEqual({
      primaryBlocked: true,
      secondaryBlocked: true,
      kind: "SERVER_QUOTA",
      scheduledSpeedLimit: scheduledLimit,
    });

  body = await metrics(request);
  expect(
    metricValue(body, "weaver_pipeline_download_gate", { reason: "server_quota" }),
  ).toBe(1);
  for (const id of [primary.id, secondary.id]) {
    expect(
      metricValue(body, "weaver_server_download_quota_blocked", {
        server_id: String(id),
      }),
    ).toBe(1);
    // Conservative reservation stops admitting once the remaining headroom is
    // smaller than one article's estimate, so a blocked server settles with a
    // sub-article remainder rather than exactly zero.
    const remaining = metricValue(body, "weaver_server_download_quota_remaining_bytes", {
      server_id: String(id),
    });
    expect(remaining).toBeLessThan(oneArticleQuota);
  }

  await graphql(request, "mutation WeaverE2EManualPauseOverServerQuota { pauseAll }");
  await expectBlockKind(request, "MANUAL_PAUSE");
  await graphql(request, "mutation WeaverE2EResumeIntoServerQuota { resumeAll }");
  await expectBlockKind(request, "SERVER_QUOTA");
}

// Mirror of weaver's bandwidth_reservation_estimate: the conservative raw-byte
// reservation held before a BODY (raw + raw/16 + 1024), reconciled to the
// actual afterward. Quotas that gate whole articles must be sized in these
// units, since admission checks the estimate, not the raw article size.
function reservationEstimate(rawBytes: number): number {
  return rawBytes + Math.floor(rawBytes / 16) + 1024;
}

function quota(
  period: QuotaInput["period"],
  limitBytes: number,
  enabled = true,
): QuotaInput {
  return {
    enabled,
    limitBytes,
    period,
    resetTimeMinutesLocal: 0,
    weeklyResetWeekday: "MON",
    monthlyResetDay: 1,
  };
}

async function cancelOutstandingJobs(request: APIRequestContext): Promise<void> {
  const data = await graphql<{ queueItems: Array<{ id: number }> }>(
    request,
    "query WeaverE2EQuotaOutstandingJobs { queueItems { id } }",
  );
  for (const item of data.queueItems) {
    await graphql<{ cancelJob: boolean }>(
      request,
      "mutation WeaverE2EQuotaCancel($id: Int!) { cancelJob(id: $id) }",
      { id: item.id },
    );
  }
  await expect
    .poll(
      async () =>
        (
          await graphql<{ queueItems: Array<{ id: number }> }>(
            request,
            "query WeaverE2EQuotaOutstandingJobsCheck { queueItems { id } }",
          )
        ).queueItems.length,
      { timeout: 15_000, intervals: [100, 250, 500] },
    )
    .toBe(0);
}

async function waitForCompletedJob(
  request: APIRequestContext,
  jobId: number,
): Promise<void> {
  await expect
    .poll(
      async () => {
        const data = await graphql<{ historyItem: { state: string } | null }>(
          request,
          `query WeaverE2EQuotaHistory($id: Int!) {
            historyItem(id: $id) { state }
          }`,
          { id: jobId },
        );
        return data.historyItem?.state ?? "PENDING";
      },
      { timeout: 30_000, intervals: [100, 250, 500] },
    )
    .toBe("COMPLETED");
}

function setClock(instant: string): void {
  const clockFile = process.env.E2E_WEAVER_CLOCK_FILE!;
  const owner = fs.statSync(clockFile);
  const pending = `${clockFile}.${process.pid}.tmp`;
  fs.writeFileSync(pending, `${instant}\n`, { mode: 0o600 });
  fs.chownSync(pending, owner.uid, owner.gid);
  fs.chmodSync(pending, owner.mode & 0o777);
  fs.renameSync(pending, clockFile);
}

async function configureIspCap(
  request: APIRequestContext,
  enabled: boolean,
  limitBytes: number,
): Promise<void> {
  const data = await graphql<{
    updateSettings: {
      ispBandwidthCap: { enabled: boolean; period: string; limitBytes: number };
    };
  }>(
    request,
    `mutation WeaverE2EIspCap($input: GeneralSettingsInput!) {
      updateSettings(input: $input) {
        ispBandwidthCap { enabled period limitBytes }
      }
    }`,
    {
      input: {
        ispBandwidthCap: {
          enabled,
          period: "DAILY",
          limitBytes,
          resetTimeMinutesLocal: 0,
          weeklyResetWeekday: "MON",
          monthlyResetDay: 1,
        },
      },
    },
  );
  expect(data.updateSettings.ispBandwidthCap).toEqual({
    enabled,
    period: "DAILY",
    limitBytes,
  });
}

async function configureServerQuota(
  request: APIRequestContext,
  host: string,
  downloadQuota: QuotaInput,
  overrides: Record<string, unknown> = {},
): Promise<{ id: number }> {
  return updateConfiguredServer(request, host, {
    maxDownloadSpeed: 0,
    downloadQuota,
    ...overrides,
  });
}

async function readServerQuota(
  request: APIRequestContext,
  id: number,
): Promise<QuotaSnapshot> {
  const data = await graphql<{ server: { downloadQuota: QuotaSnapshot } }>(
    request,
    `query WeaverE2EServerQuota($id: Int!) {
      server(id: $id) {
        downloadQuota {
          enabled period limitBytes lifetimeBytes usedBytes reservedBytes remainingBytes blocked
          windowStartsAtEpochMs windowEndsAtEpochMs timezoneName
        }
      }
    }`,
    { id },
  );
  return data.server.downloadQuota;
}

async function readDownloadBlock(
  request: APIRequestContext,
): Promise<DownloadBlockSnapshot> {
  const data = await graphql<{
    globalQueueState: { downloadBlock: DownloadBlockSnapshot };
  }>(
    request,
    `query WeaverE2EDownloadBlock {
      globalQueueState {
        downloadBlock {
          kind capEnabled period usedBytes limitBytes remainingBytes reservedBytes
          windowStartsAtEpochMs windowEndsAtEpochMs timezoneName scheduledSpeedLimit
        }
      }
    }`,
  );
  return data.globalQueueState.downloadBlock;
}

async function resetServerQuota(
  request: APIRequestContext,
  id: number,
): Promise<QuotaSnapshot> {
  const data = await graphql<{
    resetServerDownloadQuotaUsage: { downloadQuota: QuotaSnapshot };
  }>(
    request,
    `mutation WeaverE2EResetServerQuota($id: Int!) {
      resetServerDownloadQuotaUsage(id: $id) {
        downloadQuota {
          enabled period limitBytes lifetimeBytes usedBytes reservedBytes remainingBytes blocked
          windowStartsAtEpochMs windowEndsAtEpochMs timezoneName
        }
      }
    }`,
    { id },
  );
  return data.resetServerDownloadQuotaUsage.downloadQuota;
}

async function runProbe(
  request: APIRequestContext,
  label: string,
  bytes: number,
  hosts: string[],
): Promise<void> {
  const messageId = `weaver-quota-${label}@e2e.invalid`;
  for (const host of hosts) {
    await postProbeArticle(messageId, bytes, host);
  }
  expect(
    await submitProbeNzb(request, `weaver-quota-${label}`, [{ messageId, bytes }]),
  ).toMatchObject({ accepted: true });
}

async function runMirroredProbe(
  request: APIRequestContext,
  label: string,
  bytes: number,
  count: number,
): Promise<number> {
  const articles = Array.from({ length: count }, (_, index) => ({
    messageId: `weaver-quota-${label}-${index + 1}@e2e.invalid`,
    bytes,
  }));
  for (const article of articles) {
    await postProbeArticle(article.messageId, article.bytes, "nntp");
    await postProbeArticle(article.messageId, article.bytes, "nntp2");
  }
  const submission = await submitProbeNzb(request, `weaver-quota-${label}`, articles);
  expect(submission).toMatchObject({ accepted: true });
  expect(submission.jobId).not.toBeNull();
  return submission.jobId!;
}

async function createSchedule(
  request: APIRequestContext,
  label: string,
  actionType: "pause" | "speed_limit",
  speedLimitBytes: number,
  time: string,
): Promise<string> {
  const data = await graphql<{ createSchedule: Array<{ id: string; label: string }> }>(
    request,
    `mutation WeaverE2EQuotaSchedule($input: ScheduleInput!) {
      createSchedule(input: $input) { id label }
    }`,
    {
      input: {
        enabled: true,
        label,
        days: [],
        time,
        actionType,
        speedLimitBytes,
      },
    },
  );
  const created = data.createSchedule.find((schedule) => schedule.label === label);
  expect(created, `created schedule ${label}`).toBeTruthy();
  return created!.id;
}

async function deleteSchedule(
  request: APIRequestContext,
  id: string,
): Promise<void> {
  await graphql(
    request,
    "mutation WeaverE2EDeleteQuotaSchedule($id: String!) { deleteSchedule(id: $id) { id } }",
    { id },
  );
}

async function expectBlockKind(
  request: APIRequestContext,
  kind: string,
): Promise<void> {
  await expect
    .poll(async () => (await readDownloadBlock(request)).kind, {
      timeout: 10_000,
      intervals: [100, 250, 500],
    })
    .toBe(kind);
}

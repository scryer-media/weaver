import {
  configuredServer,
  expect,
  metricValue,
  metrics,
  nntpBodyMetrics,
  nntpConnectionMetrics,
  postMultipartProbeArticle,
  resetNntpMetrics,
  setNntpChaos,
  submitProbeNzb,
  test,
  updateConfiguredServer,
} from "./helpers";

type ProviderCapStage = "initial" | "restart-verify" | "recover";

const stage = (process.env.E2E_WEAVER_PROVIDER_CAP_STAGE || "initial") as ProviderCapStage;
const unlimitedQuota = {
  enabled: false,
  limitBytes: 0,
  period: "ONE_TIME",
  resetTimeMinutesLocal: 0,
  weeklyResetWeekday: "MON",
  monthlyResetDay: 1,
};
const labels = { server: "nntp:119" };
const providerConnectionLimit = 2;

test(`provider connection cap behavior: ${stage}`, async ({ request }) => {
  switch (stage) {
    case "initial":
      await configureAndProveReduction(request, "initial");
      return;
    case "restart-verify":
      await provePersistedConfigurationAndRelearning(request);
      return;
    case "recover":
      await proveProviderCapRecovery(request);
      return;
    default:
      throw new Error(`unsupported E2E_WEAVER_PROVIDER_CAP_STAGE ${JSON.stringify(stage)}`);
  }
});

async function configureAndProveReduction(
  request: Parameters<typeof metrics>[0],
  suffix: string,
) {
  await updateConfiguredServer(request, "nntp", {
    active: false,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  await updateConfiguredServer(request, "nntp2", {
    active: false,
    connections: 4,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  const articles = await createProbeArticles(`weaver-provider-cap-${suffix}`);
  await resetNntpMetrics();
  // Keep the admitted lanes occupied long enough for all 12 initial attempts
  // to observe the provider ceiling and converge deterministically.
  await setNntpChaos(`max_conns=${providerConnectionLimit},slow_body=5`);
  const lifetimeBefore = await serverLifetimeBytes(request);
  await updateConfiguredServer(request, "nntp", {
    active: true,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  expect(
    await submitProbeNzb(
      request,
      `weaver-provider-cap-${suffix}`,
      articles,
      {},
      "single-multipart-file",
    ),
  ).toMatchObject({ accepted: true });
  await expectReducedEffectiveCapacity(request);
  await expectWeaverProbeBytes(request, lifetimeBefore, articles);
  await assertBoundedProviderRejections(request);
  await expectProbeTransfers(articles.length);

  // Leave the server configured and active under the provider cap so the
  // harness restart exercises persisted configuration and relearning.
  await updateConfiguredServer(request, "nntp", {
    active: true,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
}

async function provePersistedConfigurationAndRelearning(
  request: Parameters<typeof metrics>[0],
) {
  await configuredServer(request, "nntp");
  const body = await metrics(request);
  expect(metricValue(body, "weaver_server_connections_configured", labels)).toBe(12);

  // Free one provider connection for controlled article injection, then let
  // the restarted Weaver relearn the same provider-side limit under traffic.
  await updateConfiguredServer(request, "nntp", {
    active: false,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  const articles = await createProbeArticles("weaver-provider-cap-restart");
  await resetNntpMetrics();
  const lifetimeBefore = await serverLifetimeBytes(request);
  await updateConfiguredServer(request, "nntp", {
    active: true,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  expect(
    await submitProbeNzb(
      request,
      "weaver-provider-cap-restart",
      articles,
      {},
      "single-multipart-file",
    ),
  ).toMatchObject({ accepted: true });
  await expectReducedEffectiveCapacity(request);
  await expectWeaverProbeBytes(request, lifetimeBefore, articles);
  await assertBoundedProviderRejections(request);
  await expectProbeTransfers(articles.length);
}

async function proveProviderCapRecovery(request: Parameters<typeof metrics>[0]) {
  await updateConfiguredServer(request, "nntp", {
    active: false,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  await setNntpChaos("slow_body=5");
  const articles = await createProbeArticles("weaver-provider-cap-recovered");
  await resetNntpMetrics();
  await updateConfiguredServer(request, "nntp", {
    active: true,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  expect(
    await submitProbeNzb(
      request,
      "weaver-provider-cap-recovered",
      articles,
      {},
      "single-multipart-file",
    ),
  ).toMatchObject({ accepted: true });

  await expect
    .poll(async () => {
      const body = await metrics(request);
      return metricValue(body, "weaver_server_connections_effective", labels);
    }, { timeout: 30_000, intervals: [500, 1_000, 2_000] })
    .toBe(12);
  await expectProbeTransfers(articles.length);
  const provider = await nntpConnectionMetrics();
  expect(provider.configured_limit).toBe(0);
  expect(provider.peak_active).toBeGreaterThan(providerConnectionLimit);
}

async function createProbeArticles(prefix: string) {
  const bytesPerArticle = 8 * 1024;
  const articles = Array.from({ length: 48 }, (_, index) => ({
    messageId: `${prefix}-${index}@e2e.invalid`,
    bytes: bytesPerArticle,
  }));
  for (const [index, article] of articles.entries()) {
    await postMultipartProbeArticle(article.messageId, article.bytes, {
      filename: `${prefix}.bin`,
      number: index + 1,
      total: articles.length,
      begin: index * bytesPerArticle + 1,
      end: (index + 1) * bytesPerArticle,
      totalBytes: articles.length * bytesPerArticle,
    });
  }
  return articles;
}

async function expectReducedEffectiveCapacity(request: Parameters<typeof metrics>[0]) {
  await expect
    .poll(async () => {
      const body = await metrics(request);
      const configured = metricValue(
        body,
        "weaver_server_connections_configured",
        labels,
      );
      const effective = metricValue(
        body,
        "weaver_server_connections_effective",
        labels,
      );
      const reductions = metricValue(
        body,
        "weaver_server_capacity_reductions_total",
        labels,
      );
      return configured === 12
        && effective === providerConnectionLimit
        && reductions !== undefined
        && reductions > 0;
    }, { timeout: 60_000, intervals: [500, 1_000, 2_000] })
    .toBeTruthy();
}

async function assertBoundedProviderRejections(request: Parameters<typeof metrics>[0]) {
  await updateConfiguredServer(request, "nntp", {
    active: false,
    connections: 12,
    maxDownloadSpeed: 0,
    downloadQuota: unlimitedQuota,
  });
  const provider = await nntpConnectionMetrics();
  expect(provider.configured_limit).toBe(providerConnectionLimit);
  expect(provider.rejected).toBeGreaterThan(0);
  expect(provider.peak_active).toBeLessThanOrEqual(providerConnectionLimit);
  expect(provider.attempted).toBeLessThan(500);
}

async function expectProbeTransfers(count: number) {
  await expect
    .poll(async () => (await nntpBodyMetrics()).body_transfers, {
      timeout: 60_000,
      intervals: [250, 500, 1_000, 2_000],
    })
    .toBe(count);
}

async function serverLifetimeBytes(
  request: Parameters<typeof metrics>[0],
): Promise<number> {
  const server = await configuredServer(request, "nntp");
  const body = await metrics(request);
  return metricValue(
    body,
    "weaver_server_download_lifetime_bytes",
    { server_id: String(server.id) },
  ) ?? 0;
}

async function expectWeaverProbeBytes(
  request: Parameters<typeof metrics>[0],
  baseline: number,
  articles: Array<{ bytes: number }>,
) {
  const advertisedBytes = articles.reduce((total, article) => total + article.bytes, 0);
  await expect
    .poll(
      async () => (await serverLifetimeBytes(request)) - baseline,
      { timeout: 60_000, intervals: [250, 500, 1_000, 2_000] },
    )
    .toBeGreaterThanOrEqual(advertisedBytes);
}

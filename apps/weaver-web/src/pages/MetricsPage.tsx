import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery, useSubscription } from "urql";
import { Loader2 } from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { SpeedDisplay, formatBytes } from "@/components/SpeedDisplay";
import { TimeSeriesChart } from "@/components/TimeSeriesChart";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { requestGraphqlClientRestart } from "@/graphql/client";
import { METRICS_PAGE_QUERY, METRICS_PAGE_SUBSCRIPTION } from "@/graphql/queries";
import {
  useLiveConnection,
  useLiveDownloadBlock,
  useLiveJobs,
  useLivePaused,
  useLiveSpeed,
} from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { useMetricsHistory } from "@/lib/hooks/use-metrics-history";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import {
  METRICS_RANGE_OPTIONS,
  type MetricsPageData,
  type MetricsHistoryRange,
} from "@/lib/metrics";

export function MetricsPage() {
  const t = useTranslate();
  const liveConnection = useLiveConnection();
  const liveDownloadBlock = useLiveDownloadBlock();
  const liveJobs = useLiveJobs();
  const livePaused = useLivePaused();
  const liveSpeed = useLiveSpeed();
  const [historyRange, setHistoryRange] = useState<MetricsHistoryRange>("ONE_HOUR");
  const [polledSnapshot, setPolledSnapshot] = useState<MetricsPageData | undefined>();
  const [{ data: queryData, fetching, error }] = useQuery<MetricsPageData>({
    query: METRICS_PAGE_QUERY,
  });
  const handleSubscription = (
    _prev: MetricsPageData | undefined,
    response: { systemMetricsUpdates: MetricsPageData },
  ) => response.systemMetricsUpdates;
  const [{ data: subscriptionData }] = useSubscription(
    { query: METRICS_PAGE_SUBSCRIPTION },
    handleSubscription,
  );
  useReconnectPolling<MetricsPageData>({
    enabled: liveConnection.isDisconnected,
    query: METRICS_PAGE_QUERY,
    onData: (nextSnapshot) => {
      setPolledSnapshot(nextSnapshot);
      requestGraphqlClientRestart();
    },
  });

  useEffect(() => {
    if (!liveConnection.isDisconnected) {
      setPolledSnapshot(undefined);
    }
  }, [liveConnection.isDisconnected]);

  const counts = useMemo(() => {
    const total = liveJobs.length;
    const active = liveJobs.filter(
      (job) => job.status !== "COMPLETE" && job.status !== "FAILED",
    ).length;
    const downloading = liveJobs.filter((job) => job.status === "DOWNLOADING").length;
    const queued = liveJobs.filter((job) => job.status === "QUEUED").length;
    const paused = liveJobs.filter((job) => job.status === "PAUSED").length;
    const failed = liveJobs.filter((job) => job.status === "FAILED").length;

    return { total, active, downloading, queued, paused, failed };
  }, [liveJobs]);

  const snapshot = subscriptionData ?? polledSnapshot ?? queryData;
  const metrics = snapshot?.metrics;
  const isPaused = snapshot?.globalState?.isPaused ?? livePaused;
  const downloadBlock = snapshot?.globalState?.downloadBlock ?? liveDownloadBlock;
  const capResetAt = downloadBlock.windowEndsAtEpochMs
    ? new Date(downloadBlock.windowEndsAtEpochMs).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })
    : "\u2014";
  const history = useMetricsHistory({
    range: historyRange,
    liveMetrics: metrics,
    liveJobs,
  });

  const jobStatusRows = history.jobStatusRows;
  const jobStatusTotal = useMemo(
    () => jobStatusRows.reduce((total, row) => total + row.count, 0),
    [jobStatusRows],
  );
  const chartEmptyLabel = history.isLoading ? t("label.loading") : t("metrics.chartNoData");
  const bandwidthProgress =
    downloadBlock.capEnabled && downloadBlock.limitBytes > 0
      ? Math.min(100, (downloadBlock.usedBytes / downloadBlock.limitBytes) * 100)
      : 0;
  const bandwidthCapLabel = downloadBlock.capEnabled && downloadBlock.limitBytes > 0
    ? formatBytes(downloadBlock.limitBytes)
    : t("label.disabled");

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("monitoring.title")}
        description={t("monitoring.description")}
      />

      {error ? (
        <Card>
          <CardContent className="py-6 text-sm text-destructive">{error.message}</CardContent>
        </Card>
      ) : null}

      {fetching && !metrics ? (
        <Card>
          <CardContent className="py-6 text-sm text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
      ) : null}

      {metrics ? (
        <>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricTile
              label={t("metrics.pipelineState")}
              value={isPaused ? t("status.paused") : t("metrics.activeState")}
            />
            <MetricTile
              label={t("metrics.downloadSpeed")}
              value={<SpeedDisplay bytesPerSec={liveSpeed} className="text-base" />}
            />
            <MetricTile label={t("metrics.activeJobs")} value={counts.active} />
            <MetricTile label={t("metrics.queuedJobs")} value={counts.queued} />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>{t("metrics.bandwidthCap")}</CardTitle>
              <CardDescription>{t("metrics.bandwidthCapDesc")}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="rounded-3xl border border-border/70 bg-field/55 p-4">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                  <div className="space-y-2">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                      {t("metrics.bandwidthCapUsage")}
                    </div>
                    <div className="text-3xl font-semibold text-foreground">
                      {formatBytes(downloadBlock.usedBytes)}
                    </div>
                    <div className="text-sm text-muted-foreground">
                      {downloadBlock.capEnabled && downloadBlock.limitBytes > 0
                        ? `${formatBytes(downloadBlock.usedBytes)} / ${formatBytes(downloadBlock.limitBytes)}`
                        : t("metrics.bandwidthCapDesc")}
                    </div>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <MetricTile label={t("metrics.bandwidthCapLimit")} value={bandwidthCapLabel} />
                    <MetricTile label={t("metrics.bandwidthCapReset")} value={capResetAt} />
                  </div>
                </div>
                <Progress
                  value={bandwidthProgress}
                  className="mt-4 h-3 bg-background/70"
                  indicatorClassName={
                    downloadBlock.kind === "ISP_CAP"
                      ? "bg-destructive"
                      : downloadBlock.capEnabled
                        ? "bg-primary"
                        : "bg-muted-foreground/40"
                  }
                />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div className="space-y-2">
                <CardTitle>{t("metrics.historySectionTitle")}</CardTitle>
                <CardDescription>{t("metrics.historySectionDesc")}</CardDescription>
                {history.error ? (
                  <div className="text-sm text-destructive">{t("metrics.historyUnavailable")}</div>
                ) : null}
              </div>

              <div className="inline-flex rounded-full border border-border/70 bg-background/80 p-1">
                {METRICS_RANGE_OPTIONS.map((option) => (
                  <Button
                    key={option.range}
                    type="button"
                    size="sm"
                    variant={historyRange === option.range ? "default" : "ghost"}
                    className="rounded-full px-4"
                    disabled={history.isLoading}
                    onClick={() => setHistoryRange(option.range)}
                  >
                    {t(option.labelKey)}
                  </Button>
                ))}
              </div>
            </CardHeader>
          </Card>

          <div className="grid gap-4 xl:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>{t("metrics.jobsByStatus")}</CardTitle>
                <CardDescription>{t("metrics.jobsByStatusDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {history.isLoading ? (
                  <MetricsDiagramLoader label={t("label.loading")} />
                ) : jobStatusRows.length ? (
                  jobStatusRows.map((row) => {
                    const percent = jobStatusTotal > 0 ? (row.count / jobStatusTotal) * 100 : 0;
                    return (
                      <div key={row.status} className="space-y-2">
                        <div className="flex items-center justify-between gap-3 text-sm">
                          <span className="font-medium text-foreground">{row.label}</span>
                          <span className="text-muted-foreground">{row.count}</span>
                        </div>
                        <div className="h-2.5 overflow-hidden rounded-full bg-muted/70">
                          <div
                            className="h-full rounded-full transition-[width] duration-300 motion-reduce:transition-none"
                            style={{
                              width: `${Math.max(percent, row.count > 0 ? 8 : 0)}%`,
                              backgroundColor: row.color,
                            }}
                          />
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="rounded-2xl border border-border/60 bg-background/60 px-4 py-8 text-center text-sm text-muted-foreground">
                    {t("metrics.jobsByStatusEmpty")}
                  </div>
                )}
              </CardContent>
            </Card>

            {history.charts.map((chart) => (
              <Card key={chart.definition.id}>
                <CardHeader>
                  <CardTitle>{t(chart.definition.titleKey)}</CardTitle>
                  <CardDescription>{t(chart.definition.descriptionKey)}</CardDescription>
                </CardHeader>
                <CardContent>
                  {history.isLoading ? (
                    <MetricsDiagramLoader label={t("label.loading")} />
                  ) : (
                    <TimeSeriesChart
                      data={chart.data}
                      series={chart.series.map((line) => ({
                        label: formatChartSeriesLabel(t, line.labelKey, line.variant),
                        colorToken: line.colorToken,
                        scale: line.scale,
                        format: line.format,
                        strokeStyle: line.variant === "peak" ? "dashed" : "solid",
                      }))}
                      leftAxisFormat={chart.definition.leftAxisFormat}
                      rightAxisFormat={chart.definition.rightAxisFormat}
                      emptyLabel={chartEmptyLabel}
                      height={240}
                    />
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </>
      ) : null}
    </div>
  );
}

function MetricsDiagramLoader({ label }: { label: string }) {
  return (
    <div className="flex h-[240px] items-center justify-center rounded-2xl border border-dashed border-border/60 bg-background/50">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        <span>{label}</span>
      </div>
    </div>
  );
}

function MetricTile({
  label,
  value,
}: {
  label: string;
  value: number | string | ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 text-base font-semibold text-foreground tabular-nums">{value}</div>
    </div>
  );
}

function formatChartSeriesLabel(
  t: (key: string) => string,
  labelKey: string,
  variant: "actual" | "avg" | "peak",
) {
  if (variant === "actual") {
    return t(labelKey);
  }

  const suffix = variant === "avg" ? t("metrics.seriesAvg") : t("metrics.seriesPeak");
  return `${t(labelKey)} ${suffix}`;
}

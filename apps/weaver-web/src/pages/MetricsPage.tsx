import { useEffect, useMemo, useState } from "react";
import { useQuery, useSubscription } from "urql";
import { Loader2 } from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { formatBytes, formatSpeed } from "@/components/SpeedDisplay";
import { StatTile } from "@/components/StatTile";
import { TimeSeriesChart } from "@/components/TimeSeriesChart";
import { Progress } from "@/components/ui/progress";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { requestGraphqlClientRestart } from "@/graphql/client";
import {
  DISK_USAGE_QUERY,
  METRICS_PAGE_QUERY,
  METRICS_PAGE_SUBSCRIPTION,
  SERVER_HEALTH_QUERY,
} from "@/graphql/queries";
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
import { STATUS_BG_CLASS, statusToken } from "@/lib/status-tokens";
import { cn } from "@/lib/utils";

interface DiskUsageEntry {
  label: string;
  path: string;
  totalBytes: number;
  usedBytes: number;
  freeBytes: number;
}

interface ServerHealthEntry {
  host: string;
  port: number;
  label: string;
  tier: string;
  state: string;
  connectionsActive: number;
  connectionsMax: number;
  latencyMs: number;
  successCount: number;
  failureCount: number;
  consecutiveFailures: number;
  prematureDeaths: number;
}

const SERVER_STATE_DOT_CLASS: Record<string, string> = {
  healthy: "bg-status-completed",
  degraded: "bg-status-paused",
  cooling_down: "bg-status-paused",
  disabled: "bg-status-failed",
};

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

  const [{ data: diskData }, reexecuteDiskUsage] = useQuery<{ diskUsage: DiskUsageEntry[] }>({
    query: DISK_USAGE_QUERY,
  });
  const [{ data: serverHealthData }, reexecuteServerHealth] = useQuery<{
    serverHealth: ServerHealthEntry[];
  }>({ query: SERVER_HEALTH_QUERY });

  useEffect(() => {
    if (liveConnection.isDisconnected) {
      return;
    }
    const id = window.setInterval(() => {
      reexecuteServerHealth({ requestPolicy: "network-only" });
      reexecuteDiskUsage({ requestPolicy: "network-only" });
    }, 5000);
    return () => window.clearInterval(id);
  }, [liveConnection.isDisconnected, reexecuteDiskUsage, reexecuteServerHealth]);

  const diskUsage = diskData?.diskUsage ?? [];
  const serverHealth = serverHealthData?.serverHealth ?? [];

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
    : "—";
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
  const bandwidthBarClass =
    downloadBlock.kind === "ISP_CAP"
      ? "bg-status-failed"
      : downloadBlock.capEnabled
        ? bandwidthProgress >= 90
          ? "bg-status-failed"
          : bandwidthProgress >= 70
            ? "bg-status-paused"
            : "bg-status-completed"
        : "bg-muted-foreground/40";

  const rangeOptions = useMemo(
    () =>
      METRICS_RANGE_OPTIONS.map((option) => ({
        value: option.range,
        label: t(option.labelKey),
      })),
    [t],
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("monitoring.title")}
        description={t("monitoring.description")}
      />

      {error ? (
        <div className="rounded-card border border-border bg-card p-5 sm:p-6 text-sm text-destructive">
          {error.message}
        </div>
      ) : null}

      {fetching && !metrics ? (
        <div className="rounded-card border border-border bg-card p-5 sm:p-6 text-sm text-muted-foreground">
          {t("label.loading")}
        </div>
      ) : null}

      {metrics ? (
        <>
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <StatTile
              label={t("metrics.pipelineState")}
              value={isPaused ? t("status.paused") : t("metrics.activeState")}
              valueClassName={isPaused ? "text-status-paused" : "text-status-completed"}
            />
            <StatTile
              label={t("metrics.downloadSpeed")}
              value={formatSpeed(liveSpeed)}
            />
            <StatTile label={t("metrics.activeJobs")} value={counts.active} />
            <StatTile label={t("metrics.queuedJobs")} value={counts.queued} />
          </div>

          <SectionCard
            title={t("metrics.bandwidthCap")}
            description={t("metrics.bandwidthCapDesc")}
          >
            <div className="rounded-inner border border-border bg-background/40 p-4">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div className="space-y-2">
                  <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
                    {t("metrics.bandwidthCapUsage")}
                  </div>
                  <div className="font-space-grotesk text-[38px] font-bold leading-none tracking-tight text-foreground">
                    {formatBytes(downloadBlock.usedBytes)}
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {downloadBlock.capEnabled && downloadBlock.limitBytes > 0
                      ? `${formatBytes(downloadBlock.usedBytes)} / ${formatBytes(downloadBlock.limitBytes)}`
                      : t("metrics.bandwidthCapDesc")}
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  <StatTile size="sm" label={t("metrics.bandwidthCapLimit")} value={bandwidthCapLabel} />
                  <StatTile size="sm" label={t("metrics.bandwidthCapReset")} value={capResetAt} />
                </div>
              </div>
              <Progress
                value={bandwidthProgress}
                className="mt-4 h-3 bg-background/70"
                indicatorClassName={bandwidthBarClass}
              />
            </div>
          </SectionCard>

          <div className="flex flex-col gap-4 rounded-card border border-border bg-card p-5 sm:flex-row sm:items-end sm:justify-between sm:p-6">
            <div className="space-y-1.5">
              <div className="font-space-grotesk text-lg font-bold leading-tight text-foreground">
                {t("metrics.historySectionTitle")}
              </div>
              <div className="text-[12.5px] leading-5 text-muted-foreground">
                {t("metrics.historySectionDesc")}
              </div>
              {history.error ? (
                <div className="text-sm text-destructive">{t("metrics.historyUnavailable")}</div>
              ) : null}
            </div>

            <SegmentedControl
              value={historyRange}
              onValueChange={setHistoryRange}
              options={rangeOptions}
              ariaLabel={t("metrics.historySectionTitle")}
            />
          </div>

          {diskUsage.length ? (
            <SectionCard title={t("metrics.diskUsage")} description={t("metrics.diskUsageDesc")}>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                {diskUsage.map((mount) => {
                  const pct = mount.totalBytes > 0 ? (mount.usedBytes / mount.totalBytes) * 100 : 0;
                  const barClass =
                    pct >= 85
                      ? "bg-status-failed"
                      : pct >= 65
                        ? "bg-status-paused"
                        : "bg-status-downloading";
                  return (
                    <div
                      key={mount.path}
                      className="rounded-inner border border-border bg-background/40 p-4"
                    >
                      <div className="flex items-baseline justify-between gap-3">
                        <span className="truncate text-[13px] font-semibold text-foreground">
                          {mount.label}
                        </span>
                        <span className="shrink-0 text-[12px] tabular-nums text-muted-foreground">
                          {Math.round(pct)}%
                        </span>
                      </div>
                      <div className="mt-1 truncate font-mono text-[11px] text-muted-foreground" title={mount.path}>
                        {mount.path}
                      </div>
                      <div className="mt-3 h-2 overflow-hidden rounded-pill bg-secondary">
                        <div
                          className={cn("h-full rounded-pill transition-[width] duration-500 motion-reduce:transition-none", barClass)}
                          style={{ width: `${Math.min(100, pct)}%` }}
                        />
                      </div>
                      <div className="mt-2.5 flex items-center justify-between text-[12px]">
                        <span className="font-medium text-foreground">
                          {formatBytes(mount.usedBytes)}{" "}
                          <span className="font-normal text-muted-foreground">
                            / {formatBytes(mount.totalBytes)}
                          </span>
                        </span>
                        <span className="text-muted-foreground">
                          {formatBytes(mount.freeBytes)} {t("metrics.diskFree")}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </SectionCard>
          ) : null}

          {serverHealth.length ? (
            <SectionCard
              title={t("metrics.newsServers")}
              description={t("metrics.newsServersDesc")}
            >
              <div className="flex flex-col">
                {serverHealth.map((server) => {
                  const dotClass = SERVER_STATE_DOT_CLASS[server.state] ?? "bg-muted-foreground";
                  const active = server.connectionsActive > 0 && server.state !== "disabled";
                  const connPct =
                    server.connectionsMax > 0
                      ? (server.connectionsActive / server.connectionsMax) * 100
                      : 0;
                  const latencyClass =
                    server.latencyMs < 80
                      ? "text-status-completed"
                      : server.latencyMs < 160
                        ? "text-status-paused"
                        : "text-status-failed";
                  const isPrimary = server.tier === "PRIMARY";
                  return (
                    <div
                      key={server.label}
                      className="flex items-center gap-4 border-b border-border/60 py-3 last:border-0"
                    >
                      <span
                        className={cn(
                          "size-2.5 shrink-0 rounded-pill",
                          dotClass,
                          active && "animate-status-pulse",
                        )}
                        title={server.state}
                      />
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2">
                          <span className="truncate text-[13px] font-semibold text-foreground">
                            {server.host}
                          </span>
                          <span
                            className={cn(
                              "shrink-0 rounded-chip px-2 py-px text-[10px] font-bold uppercase tracking-[0.06em]",
                              isPrimary ? "bg-primary/15 text-primary" : "bg-secondary text-muted-foreground",
                            )}
                          >
                            {isPrimary ? t("metrics.serverTierPrimary") : t("metrics.serverTierBackup")}
                          </span>
                        </div>
                        <div className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground">
                          {server.label}
                        </div>
                      </div>
                      <div className="hidden w-32 shrink-0 sm:block">
                        <div className="flex items-center justify-between text-[10.5px] text-muted-foreground">
                          <span>{t("metrics.serverConns")}</span>
                          <span className="font-semibold tabular-nums text-foreground">
                            {server.connectionsActive} / {server.connectionsMax}
                          </span>
                        </div>
                        <div className="mt-1.5 h-1.5 overflow-hidden rounded-pill bg-secondary">
                          <div
                            className="h-full rounded-pill bg-status-downloading transition-[width] duration-500 motion-reduce:transition-none"
                            style={{ width: `${Math.min(100, connPct)}%` }}
                          />
                        </div>
                      </div>
                      <div className="w-20 shrink-0 text-right">
                        <div className="text-[9px] font-semibold uppercase tracking-[0.1em] text-muted-foreground">
                          {t("metrics.serverLatency")}
                        </div>
                        <div className={cn("text-[13px] font-semibold tabular-nums", latencyClass)}>
                          {Math.round(server.latencyMs)} ms
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </SectionCard>
          ) : null}

          <div className="grid gap-4 lg:grid-cols-2">
            <div className="rounded-card border border-border bg-card p-5 sm:p-6">
              <div className="font-space-grotesk text-lg font-bold leading-tight text-foreground">
                {t("metrics.jobsByStatus")}
              </div>
              <div className="mt-1.5 text-[12.5px] leading-5 text-muted-foreground">
                {t("metrics.jobsByStatusDesc")}
              </div>
              <div className="mt-5 space-y-4">
                {history.isLoading ? (
                  <MetricsDiagramLoader label={t("label.loading")} />
                ) : jobStatusRows.length ? (
                  jobStatusRows.map((row) => {
                    const percent = jobStatusTotal > 0 ? (row.count / jobStatusTotal) * 100 : 0;
                    const token = statusToken(row.status);
                    return (
                      <div key={row.status} className="space-y-2">
                        <div className="flex items-center justify-between gap-3 text-sm">
                          <span className="font-medium text-foreground">{row.label}</span>
                          <span className="text-muted-foreground tabular-nums">{row.count}</span>
                        </div>
                        <div className="h-2.5 overflow-hidden rounded-pill bg-muted/70">
                          <div
                            className={cn(
                              "h-full rounded-pill transition-[width] duration-300 motion-reduce:transition-none",
                              STATUS_BG_CLASS[token],
                            )}
                            style={{
                              width: `${Math.max(percent, row.count > 0 ? 8 : 0)}%`,
                            }}
                          />
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="rounded-inner border border-border bg-background/40 px-4 py-8 text-center text-sm text-muted-foreground">
                    {t("metrics.jobsByStatusEmpty")}
                  </div>
                )}
              </div>
            </div>

            {history.charts.map((chart) => (
              <div key={chart.definition.id} className="rounded-card border border-border bg-card p-5 sm:p-6">
                <div className="font-space-grotesk text-lg font-bold leading-tight text-foreground">
                  {t(chart.definition.titleKey)}
                </div>
                <div className="mt-1.5 text-[12.5px] leading-5 text-muted-foreground">
                  {t(chart.definition.descriptionKey)}
                </div>
                <div className="mt-5">
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
                </div>
              </div>
            ))}
          </div>
        </>
      ) : null}
    </div>
  );
}

function MetricsDiagramLoader({ label }: { label: string }) {
  return (
    <div className="flex h-[240px] items-center justify-center rounded-inner border border-dashed border-border bg-background/40">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        <span>{label}</span>
      </div>
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

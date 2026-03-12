import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery, useSubscription } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SpeedDisplay, formatBytes } from "@/components/SpeedDisplay";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { requestGraphqlClientRestart } from "@/graphql/client";
import { METRICS_PAGE_QUERY, METRICS_PAGE_SUBSCRIPTION } from "@/graphql/queries";
import { useLiveData } from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";

type MetricsSnapshot = {
  bytesDownloaded: number;
  bytesDecoded: number;
  bytesCommitted: number;
  downloadQueueDepth: number;
  decodePending: number;
  commitPending: number;
  writeBufferedBytes: number;
  writeBufferedSegments: number;
  directWriteEvictions: number;
  segmentsDownloaded: number;
  segmentsDecoded: number;
  segmentsCommitted: number;
  articlesNotFound: number;
  decodeErrors: number;
  verifyActive: number;
  repairActive: number;
  extractActive: number;
  diskWriteLatencyUs: number;
  segmentsRetried: number;
  segmentsFailedPermanent: number;
  currentDownloadSpeed: number;
  crcErrors: number;
  recoveryQueueDepth: number;
  articlesPerSec: number;
  decodeRateMbps: number;
};

type MetricsPageData = {
  metrics: MetricsSnapshot;
  isPaused: boolean;
};

export function MetricsPage() {
  const t = useTranslate();
  const liveData = useLiveData();
  const [polledSnapshot, setPolledSnapshot] = useState<MetricsPageData | undefined>();
  const [{ data: queryData, fetching, error }] = useQuery<MetricsPageData>({
    query: METRICS_PAGE_QUERY,
  });
  const handleSubscription = (
    _prev: MetricsPageData | undefined,
    response: { jobUpdates: MetricsPageData },
  ) => response.jobUpdates;
  const [{ data: subscriptionData }] = useSubscription(
    { query: METRICS_PAGE_SUBSCRIPTION },
    handleSubscription,
  );
  useReconnectPolling<MetricsPageData>({
    enabled: liveData.connection.isDisconnected,
    query: METRICS_PAGE_QUERY,
    onData: (nextSnapshot) => {
      setPolledSnapshot(nextSnapshot);
      requestGraphqlClientRestart();
    },
  });

  useEffect(() => {
    if (!liveData.connection.isDisconnected) {
      setPolledSnapshot(undefined);
    }
  }, [liveData.connection.isDisconnected]);

  const counts = useMemo(() => {
    const total = liveData.jobs.length;
    const active = liveData.jobs.filter(
      (job) => job.status !== "COMPLETE" && job.status !== "FAILED",
    ).length;
    const downloading = liveData.jobs.filter((job) => job.status === "DOWNLOADING").length;
    const queued = liveData.jobs.filter((job) => job.status === "QUEUED").length;
    const paused = liveData.jobs.filter((job) => job.status === "PAUSED").length;
    const failed = liveData.jobs.filter((job) => job.status === "FAILED").length;

    return { total, active, downloading, queued, paused, failed };
  }, [liveData.jobs]);

  const snapshot = subscriptionData ?? polledSnapshot ?? queryData;
  const metrics = snapshot?.metrics;
  const isPaused = snapshot?.isPaused ?? liveData.isPaused;

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("metrics.title")}
        description={t("metrics.description")}
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
              value={<SpeedDisplay bytesPerSec={metrics.currentDownloadSpeed} className="text-base" />}
            />
            <MetricTile label={t("metrics.activeJobs")} value={counts.active} />
            <MetricTile label={t("metrics.queuedJobs")} value={counts.queued} />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>{t("metrics.runtimeSnapshot")}</CardTitle>
              <CardDescription>{t("metrics.runtimeSnapshotDesc")}</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <MetricTile label={t("metrics.totalJobs")} value={counts.total} />
              <MetricTile label={t("metrics.downloadingJobs")} value={counts.downloading} />
              <MetricTile label={t("metrics.pausedJobs")} value={counts.paused} />
              <MetricTile label={t("metrics.failedJobs")} value={counts.failed} />
              <MetricTile label={t("metrics.downloaded")} value={formatBytes(metrics.bytesDownloaded)} />
              <MetricTile label={t("metrics.decoded")} value={formatBytes(metrics.bytesDecoded)} />
              <MetricTile label={t("metrics.committed")} value={formatBytes(metrics.bytesCommitted)} />
              <MetricTile
                label={t("metrics.decodeRate")}
                value={`${metrics.decodeRateMbps.toFixed(1)} MiB/s`}
              />
              <MetricTile
                label={t("metrics.articlesPerSec")}
                value={`${metrics.articlesPerSec.toFixed(1)}/s`}
              />
              <MetricTile label={t("metrics.segmentsDownloaded")} value={metrics.segmentsDownloaded} />
              <MetricTile label={t("metrics.segmentsDecoded")} value={metrics.segmentsDecoded} />
              <MetricTile label={t("metrics.segmentsCommitted")} value={metrics.segmentsCommitted} />
            </CardContent>
          </Card>

          <div className="grid gap-4 xl:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>{t("metrics.queuePressure")}</CardTitle>
                <CardDescription>{t("metrics.queuePressureDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="grid gap-3 md:grid-cols-2">
                <MetricTile label={t("metrics.downloadQueue")} value={metrics.downloadQueueDepth} />
                <MetricTile label={t("metrics.decodePending")} value={metrics.decodePending} />
                <MetricTile label={t("metrics.commitPending")} value={metrics.commitPending} />
                <MetricTile label={t("metrics.recoveryQueue")} value={metrics.recoveryQueueDepth} />
                <MetricTile
                  label={t("metrics.writeBufferedBytes")}
                  value={formatBytes(metrics.writeBufferedBytes)}
                />
                <MetricTile
                  label={t("metrics.writeBufferedSegments")}
                  value={metrics.writeBufferedSegments}
                />
                <MetricTile
                  label={t("metrics.directWriteEvictions")}
                  value={metrics.directWriteEvictions}
                />
                <MetricTile
                  label={t("metrics.diskWriteLatency")}
                  value={formatLatency(metrics.diskWriteLatencyUs)}
                />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("metrics.errorsAndStages")}</CardTitle>
                <CardDescription>{t("metrics.errorsAndStagesDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="grid gap-3 md:grid-cols-2">
                <MetricTile label={t("metrics.articlesNotFound")} value={metrics.articlesNotFound} />
                <MetricTile label={t("metrics.decodeErrors")} value={metrics.decodeErrors} />
                <MetricTile label={t("metrics.crcErrors")} value={metrics.crcErrors} />
                <MetricTile label={t("metrics.segmentsRetried")} value={metrics.segmentsRetried} />
                <MetricTile
                  label={t("metrics.failedPermanent")}
                  value={metrics.segmentsFailedPermanent}
                />
                <MetricTile label={t("metrics.verifyActive")} value={metrics.verifyActive} />
                <MetricTile label={t("metrics.repairActive")} value={metrics.repairActive} />
                <MetricTile label={t("metrics.extractActive")} value={metrics.extractActive} />
              </CardContent>
            </Card>
          </div>
        </>
      ) : null}
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
      <div className="mt-2 text-base font-semibold text-foreground">{value}</div>
    </div>
  );
}

function formatLatency(microseconds: number) {
  if (microseconds >= 1000) {
    return `${(microseconds / 1000).toFixed(microseconds >= 10_000 ? 0 : 1)} ms`;
  }
  return `${microseconds} us`;
}

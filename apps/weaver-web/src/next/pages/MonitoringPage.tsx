import { useState } from "react";
import { Link } from "react-router";
import { useQuery } from "urql";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import type { MetricsHistoryRange } from "@/lib/metrics";
import { Bar, Eyebrow, KeyValueRow, MetricCell, SectionHeader, Square } from "../components/chrome";
import { Segmented } from "../components/controls";
import { Chart, type ChartSeries } from "../components/Chart";
import { columnStyle } from "../components/columns";
import { useNextData, type ProviderHealth } from "../data/next-data";
import {
  EM_DASH,
  formatClock,
  formatCompactCount,
  formatCount,
  formatDayClock,
  formatLatency,
  formatPerSecond,
  formatRate,
  formatSize,
  splitBytes,
  splitSpeed,
} from "../data/format";
import { useMetricsSeries, useMetricsSnapshot } from "../data/use-metrics-series";
import { WV, WV_FILL } from "../data/palette";
import { aroundSlot, countLabel } from "../i18n/labels";
import { NextShell } from "../shell/NextShell";
import { AttentionBlock, UptimeBlock, providerLoadPercent } from "../shell/rail-blocks";

type RangeId = "10m" | "1h" | "6h" | "24h" | "7d";

const RANGES: { value: RangeId; label: string; range: MetricsHistoryRange }[] = [
  { value: "10m", label: "10m", range: "TEN_MINUTES" },
  { value: "1h", label: "1h", range: "ONE_HOUR" },
  { value: "6h", label: "6h", range: "SIX_HOURS" },
  { value: "24h", label: "24h", range: "TWENTY_FOUR_HOURS" },
  { value: "7d", label: "7d", range: "SEVEN_DAYS" },
];

interface SystemInfoResponse {
  systemInfo: {
    compute: { decoderTier: string; logicalCores: number } | null;
  } | null;
}

function last(values: readonly number[]): number {
  return values.length === 0 ? 0 : values[values.length - 1]!;
}

/** Three labels, oldest to newest, from the series' own timestamps. */
function axisLabels(timestamps: readonly number[]): string[] {
  if (timestamps.length === 0) {
    return [EM_DASH, EM_DASH, EM_DASH];
  }
  const middle = timestamps[Math.floor((timestamps.length - 1) / 2)]!;
  return [
    formatClock(timestamps[0]!),
    formatClock(middle),
    formatClock(timestamps[timestamps.length - 1]!),
  ];
}

export function MonitoringPage() {
  const t = useTranslate();
  const { speed, peakSpeed, providers, queue, downloadBlock } = useNextData();
  const [rangeId, setRangeId] = useState<RangeId>("1h");
  const range = RANGES.find((entry) => entry.value === rangeId)!.range;

  const series = useMetricsSeries(range);
  const metrics = useMetricsSnapshot();
  const [{ data: systemInfo }] = useQuery<SystemInfoResponse>({ query: SYSTEM_INFO_QUERY });

  const labels = axisLabels(series.timestamps);
  const compute = systemInfo?.systemInfo?.compute ?? null;

  const throughput: ChartSeries[] = [
    {
      key: "downloaded",
      label: t("next.monitoring.series.downloaded"),
      color: WV.accent,
      fill: WV_FILL.accent,
      values: series.rate("weaver_pipeline_bytes_downloaded_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_downloaded_total"))),
    },
    {
      key: "committed",
      label: t("next.monitoring.series.committed"),
      color: WV.info,
      values: series.rate("weaver_pipeline_bytes_committed_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_committed_total"))),
    },
    {
      key: "decoded",
      label: t("next.monitoring.series.decoded"),
      color: WV.slate,
      values: series.rate("weaver_pipeline_bytes_decoded_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_decoded_total"))),
    },
  ];

  const segments: ChartSeries[] = [
    {
      key: "downloaded",
      label: t("next.monitoring.series.downloaded"),
      color: WV.info,
      fill: WV_FILL.info,
      values: series.rate("weaver_pipeline_segments_downloaded_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_downloaded_total"))),
    },
    {
      key: "decoded",
      label: t("next.monitoring.series.decoded"),
      color: WV.accent,
      values: series.rate("weaver_pipeline_segments_decoded_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_decoded_total"))),
    },
    {
      key: "retried",
      label: t("next.monitoring.series.retried"),
      color: WV.warn,
      values: series.rate("weaver_pipeline_segments_retried_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_retried_total"))),
    },
  ];

  const depths: ChartSeries[] = [
    {
      key: "download",
      label: t("next.monitoring.series.download"),
      color: WV.violet,
      fill: WV_FILL.violet,
      values: series.gauge("weaver_pipeline_download_queue_depth"),
      value: formatCount(last(series.gauge("weaver_pipeline_download_queue_depth"))),
    },
    {
      key: "decode",
      label: t("next.monitoring.series.decodePending"),
      color: WV.accent,
      values: series.gauge("weaver_pipeline_decode_pending"),
      value: formatCount(last(series.gauge("weaver_pipeline_decode_pending"))),
    },
    {
      key: "commit",
      label: t("next.monitoring.series.commitPending"),
      color: WV.info,
      values: series.gauge("weaver_pipeline_commit_pending"),
      value: formatCount(last(series.gauge("weaver_pipeline_commit_pending"))),
    },
  ];

  const failures = [
    {
      key: "articles-not-found",
      label: t("next.monitoring.failure.notFound"),
      color: WV.warn,
      rate: last(series.rate("weaver_pipeline_articles_not_found_total")),
      total: metrics?.articlesNotFound ?? 0,
    },
    {
      key: "decode-errors",
      label: t("next.monitoring.failure.decode"),
      color: WV.error,
      rate: last(series.rate("weaver_pipeline_decode_errors_total")),
      total: metrics?.decodeErrors ?? 0,
    },
    {
      key: "crc",
      label: t("next.monitoring.failure.crc"),
      color: WV.violet,
      rate: last(series.rate("weaver_pipeline_crc_errors_total")),
      total: metrics?.crcErrors ?? 0,
    },
    {
      key: "permanent",
      label: t("next.monitoring.failure.permanent"),
      color: WV.error,
      rate: last(series.rate("weaver_pipeline_segments_failed_permanent_total")),
      total: metrics?.segmentsFailedPermanent ?? 0,
    },
  ];

  const connectionsActive = providers.reduce(
    (total, provider) => total + provider.connectionsActive,
    0,
  );
  const connectionsMax = providers.reduce(
    (total, provider) => total + (provider.connectionsMax || provider.connectionsConfigured),
    0,
  );
  const activeProviders = providers.filter((provider) => provider.connectionsActive > 0).length;

  const throughputNow = splitSpeed(speed);
  const peak = splitSpeed(peakSpeed);
  const buffered = splitBytes(metrics?.writeBufferedBytes ?? 0);
  const repairs = metrics ? metrics.repairActive + metrics.verifyActive : 0;
  const pipelineBusy = queue.summary.activeItems > 0 || repairs > 0;

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      note={t("next.monitoring.note")}
      controls={
        <Segmented
          label={t("next.monitoring.range")}
          value={rangeId}
          onChange={setRangeId}
          options={RANGES}
          className="font-wv-mono text-[11.5px]"
        />
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={
        <div
          className="wv-cols grid flex-none border-b border-wv-hairline bg-wv-app"
          style={columnStyle({
            base: "repeat(2, minmax(0, 1fr))",
            sm: "repeat(3, minmax(0, 1fr))",
            lg: "repeat(5, minmax(0, 1fr))",
          })}
        >
          <MetricCell
            eyebrow={t("next.monitoring.pipeline")}
            value={pipelineBusy ? t("next.monitoring.active") : t("next.monitoring.idle")}
            valueClassName={pipelineBusy ? "text-wv-accent" : "text-wv-idle"}
            note={t("next.monitoring.pipelineNote", { downloads: queue.summary.activeItems, repairs })}
          />
          <MetricCell
            eyebrow={t("next.shell.throughput")}
            value={throughputNow.value}
            unit={throughputNow.unit}
            note={
              peakSpeed > 0
                ? t("next.monitoring.peak", { value: peak.value, unit: peak.unit })
                : t("next.monitoring.noTraffic")
            }
          />
          <MetricCell
            eyebrow={t("next.monitoring.connections")}
            value={formatCount(connectionsActive)}
            unit={`/ ${connectionsMax}`}
            note={countLabel(t, "next.monitoring.providersActive", activeProviders)}
          />
          <MetricCell
            eyebrow={t("next.monitoring.decode")}
            value={formatCount(metrics?.articlesPerSec ?? 0)}
            unit="seg/s"
            note={
              compute
                ? countLabel(t, "next.monitoring.workers", compute.logicalCores, {
                    tier: compute.decoderTier,
                  })
                : t("next.monitoring.tierUnknown")
            }
          />
          <MetricCell
            last
            eyebrow={t("next.monitoring.writeQueue")}
            value={buffered.value}
            unit={t("next.monitoring.buffered", { unit: buffered.unit })}
            valueClassName={(metrics?.writeBufferedBytes ?? 0) > 0 ? "text-wv-warn" : undefined}
            note={`p99 ${formatLatency((metrics?.diskWriteLatencyUs ?? 0) / 1000)}`}
          />
        </div>
      }
      statusRight={
        series.error
          ? series.error
          : t("next.monitoring.sampled", {
              window: RANGES.find((entry) => entry.value === rangeId)!.label,
            })
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <Chart
          title={t("next.shell.throughput")}
          note={t("next.monitoring.throughputNote")}
          series={throughput}
          xLabels={labels}
          loading={series.isLoading}
          formatValue={(value) => formatRate(value)}
        />
        <Chart
          title={t("next.monitoring.segments")}
          note={t("next.monitoring.segmentsNote")}
          series={segments}
          xLabels={labels}
          loading={series.isLoading}
          formatValue={(value) => formatPerSecond(value)}
        />
        <Chart
          title={t("next.monitoring.depths")}
          note={t("next.monitoring.depthsNote")}
          series={depths}
          xLabels={labels}
          loading={series.isLoading}
          formatValue={(value) => formatCompactCount(value)}
        />

        <DataCapSection block={downloadBlock} />

        <section className="flex flex-none flex-col">
          <SectionHeader
            label={t("next.rail.providers")}
            note={t("next.monitoring.providersNote")}
            sticky={false}
          />
          {providers.length === 0 ? (
            <div className="px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
              {t("next.monitoring.noProviders")}
            </div>
          ) : (
            providers.map((provider) => {
              const max = provider.connectionsMax || provider.connectionsConfigured;
              const share =
                connectionsActive > 0
                  ? Math.round((provider.connectionsActive / connectionsActive) * 100)
                  : 0;
              const degraded = provider.state !== "healthy";
              const sequential =
                provider.bodyPipeliningPinnedSequential || provider.bodyPipelineDepth <= 1;
              return (
                <div
                  key={`${provider.host}:${provider.port}`}
                  className="flex flex-wrap items-center gap-x-5 gap-y-3 border-b border-wv-hairline px-4 sm:px-6 py-3 hover:bg-wv-cell-hover"
                >
                  <div className="flex min-w-0 flex-[1_1_240px] items-center gap-[9px]">
                    <Square color={degraded ? WV.warn : WV.accent} />
                    <span className="truncate font-wv-mono text-[12.5px] text-wv-fg">
                      {provider.host}
                    </span>
                    <Eyebrow tone="rail" className="flex-none font-wv-mono tracking-[0.12em]">
                      {provider.tier}
                    </Eyebrow>
                  </div>
                  <div className="ml-auto flex flex-wrap items-center justify-end gap-x-5 gap-y-1">
                    <Bar
                      percent={providerLoadPercent(provider)}
                      color={degraded ? WV.warn : WV.accent}
                      className="w-[150px]"
                    />
                    <span className="w-[62px] text-right font-wv-mono text-[12.5px] text-wv-secondary">
                      {provider.connectionsActive} / {max}
                    </span>
                    <span
                      title={t("next.monitoring.roundTrip")}
                      className={`min-w-[68px] text-right font-wv-mono text-[12.5px] ${degraded ? "text-wv-warn" : "text-wv-secondary"}`}
                    >
                      {formatLatency(provider.latencyMs)}
                    </span>
                    <span
                      title={bodyDepthTitle(t, provider)}
                      className={`w-[44px] text-right font-wv-mono text-[12.5px] ${sequential ? "text-wv-muted" : "text-wv-secondary"}`}
                    >
                      {sequential
                        ? t("metrics.serverBodyDepthSequential")
                        : `x${provider.bodyPipelineDepth}`}
                    </span>
                    <span
                      title={t("next.monitoring.failuresSinceStart")}
                      className="w-[74px] text-right font-wv-mono text-[12.5px] text-wv-secondary"
                    >
                      {formatCount(provider.failureCount)}
                    </span>
                    <span
                      title={t("next.monitoring.connectionShare")}
                      className="w-[50px] text-right font-wv-mono text-[12.5px] text-wv-muted"
                    >
                      {share}%
                    </span>
                  </div>
                </div>
              );
            })
          )}
        </section>

        <section className="flex flex-none flex-col">
          <SectionHeader
            label={t("next.monitoring.failures")}
            note={t("next.monitoring.failuresNote")}
            sticky={false}
          />
          {failures.map((failure) => (
            <div
              key={failure.key}
              className="flex flex-wrap items-center gap-x-5 gap-y-2 border-b border-wv-hairline px-4 sm:px-6 py-3 hover:bg-wv-cell-hover"
            >
              <div className="flex min-w-0 flex-[1_1_240px] items-center gap-[9px]">
                <Square color={failure.color} />
                <span className="truncate text-[13px] text-wv-fg">{failure.label}</span>
              </div>
              <div className="ml-auto flex flex-wrap items-center justify-end gap-x-5 gap-y-1">
                <span className="w-[74px] text-right font-wv-mono text-[12.5px] text-wv-secondary">
                  {formatPerSecond(failure.rate)}
                </span>
                <span className="w-[62px] text-right font-wv-mono text-[12.5px] text-wv-secondary">
                  {formatCount(failure.total)}
                </span>
              </div>
            </div>
          ))}
        </section>
      </div>
    </NextShell>
  );
}

const PERIOD_NOTE: Record<"DAILY" | "WEEKLY" | "MONTHLY", string> = {
  DAILY: "next.monitoring.period.daily",
  WEEKLY: "next.monitoring.period.weekly",
  MONTHLY: "next.monitoring.period.monthly",
};

/**
 * How much of the data cap's current window is spent.
 *
 * The same counters Settings → Bandwidth shows under Current window, here
 * because this is the screen someone watching a download's pace is already on.
 */
function DataCapSection({ block }: { block: ReturnType<typeof useNextData>["downloadBlock"] }) {
  const t = useTranslate();
  const settingsLink = (
    <Link to="/settings/bandwidth" className="text-wv-accent hover:text-wv-accent-hover">
      {t("next.monitoring.bandwidthLink")}
    </Link>
  );

  if (!block.capEnabled) {
    const [noCapBefore, noCapAfter] = aroundSlot(t, "next.monitoring.noCap", "link");
    return (
      <section className="flex flex-none flex-col">
        <SectionHeader
          label={t("next.monitoring.dataCap")}
          note={t("next.monitoring.capNotEnforced")}
          sticky={false}
        />
        <div className="px-4 py-5 text-[13px] text-wv-muted sm:px-6">
          {noCapBefore}
          {settingsLink}
          {noCapAfter}
        </div>
      </section>
    );
  }

  // A provider quota block carries placeholder cap counters, so none are shown for it.
  if (block.kind === "SERVER_QUOTA") {
    return (
      <section className="flex flex-none flex-col">
        <SectionHeader
          label={t("next.monitoring.dataCap")}
          note={t("next.monitoring.capProviderQuota")}
          sticky={false}
        />
        <div className="px-4 py-5 text-[13px] text-wv-muted sm:px-6">
          {t("next.monitoring.capQuotaHeld")}
        </div>
      </section>
    );
  }

  const percent = block.limitBytes > 0 ? (block.usedBytes / block.limitBytes) * 100 : 0;
  const color = percent >= 90 ? WV.error : percent >= 70 ? WV.warn : WV.accent;
  const note = [block.period ? t(PERIOD_NOTE[block.period]) : null, block.timezoneName || null]
    .filter(Boolean)
    .join(" · ");

  return (
    <section className="flex flex-none flex-col">
      <SectionHeader label={t("next.monitoring.dataCap")} note={note || undefined} sticky={false} />
      <div className="flex flex-col gap-2 border-b border-wv-hairline px-4 py-[14px] sm:px-6">
        <Bar percent={percent} color={color} label={t("next.monitoring.capUsed")} />
        <div className="flex items-baseline justify-between font-wv-mono text-[11.5px] text-wv-muted">
          <span>
            {t("next.monitoring.usedPercent", {
              size: formatSize(block.usedBytes),
              percent: Math.round(percent),
            })}
          </span>
          <span>{t("next.monitoring.allowance", { size: formatSize(block.limitBytes) })}</span>
        </div>
      </div>
      <KeyValueRow label={t("next.monitoring.remaining")} value={formatSize(block.remainingBytes)} />
      <KeyValueRow label={t("next.monitoring.reserved")} value={formatSize(block.reservedBytes)} />
      <KeyValueRow
        label={t("next.monitoring.windowResets")}
        value={formatDayClock(block.windowEndsAtEpochMs)}
      />
      <KeyValueRow
        label={t("next.monitoring.heldByCap")}
        value={
          block.kind === "ISP_CAP" ? (
            <span className="text-wv-warn">{t("next.common.yes")}</span>
          ) : (
            t("next.common.no")
          )
        }
      />
    </section>
  );
}

/**
 * The hover text for a provider's BODY depth: what the number is, and the
 * latency and transfer halves the lanes derived it from when both are known.
 */
function bodyDepthTitle(t: Translate, provider: ProviderHealth): string {
  const parts = [t("metrics.serverBodyDepth")];
  if (provider.bodyLatencyBand) {
    parts.push(t(`metrics.serverLatencyBand.${provider.bodyLatencyBand}`));
  }
  if (provider.bodyLatencyMs != null && provider.bodyTransferMs != null) {
    parts.push(`${Math.round(provider.bodyLatencyMs)}/${Math.round(provider.bodyTransferMs)} ms`);
  }
  return parts.join(" · ");
}

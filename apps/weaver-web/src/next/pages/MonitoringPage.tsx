// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createContext, memo, useContext, useMemo, useState } from "react";
import { Link } from "react-router";
import { useQuery } from "urql";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import type { MetricsHistoryRange } from "@/lib/metrics";
import { Bar, Eyebrow, MetricCell, SectionHeader, Square } from "../components/chrome";
import { Segmented } from "../components/controls";
import { Chart, type ChartSeries } from "../components/Chart";
import { columnStyle } from "../components/columns";
import { useNextData, useNextDataSelector, type ProviderHealth } from "../data/next-data";
import { shallowEqual, useSettled } from "../data/settled";
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
import {
  MetricsSnapshotContext,
  useMetricsSeries,
  useMetricsSnapshot,
  type MetricsSeries,
} from "../data/use-metrics-series";
import { WV, WV_FILL } from "../data/palette";
import { aroundSlot, countLabel } from "../i18n/labels";
import { NextShell } from "../shell/NextShell";
import { AttentionBlock, UptimeBlock, providerLoadPercent } from "../shell/rail-blocks";
import {
  cappedDirections,
  directionCap,
  directionPercent,
  metered,
  quotaHolding,
  quotaStateLabel,
  usageColor,
  usageOf,
  useEgressQuotas,
} from "../features/networking/egress-quota";

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

/** The four failure counters the failures list shows beside each rate. */
interface FailureTotals {
  articlesNotFound: number;
  decodeErrors: number;
  crcErrors: number;
  segmentsFailedPermanent: number;
}

function sameTotals(left: FailureTotals | null, right: FailureTotals | null): boolean {
  return left === right || (left !== null && right !== null && shallowEqual(left, right));
}

/**
 * The failure counters alone, kept while they read the same: a live sample
 * almost never moves them, and the list need not redraw for one that did not.
 */
const FailureTotalsContext = createContext<FailureTotals | null>(null);

export function MonitoringPage() {
  const [rangeId, setRangeId] = useState<RangeId>("1h");
  const range = RANGES.find((entry) => entry.value === rangeId)!.range;

  const series = useMetricsSeries(range);
  const metrics = useMetricsSnapshot();
  const [{ data: systemInfo }] = useQuery<SystemInfoResponse>({ query: SYSTEM_INFO_QUERY });

  const compute = systemInfo?.systemInfo?.compute ?? null;
  const failureTotals = useSettled<FailureTotals | null>(
    metrics
      ? {
          articlesNotFound: metrics.articlesNotFound,
          decodeErrors: metrics.decodeErrors,
          crcErrors: metrics.crcErrors,
          segmentsFailedPermanent: metrics.segmentsFailedPermanent,
        }
      : null,
    sameTotals,
  );

  // A live sample reaches the figures that read it through context; the
  // screen around them takes none of its props from it and is left alone.
  return (
    <MetricsSnapshotContext value={metrics}>
      <FailureTotalsContext value={failureTotals}>
        <MonitoringScreen
          rangeId={rangeId}
          onRangeChange={setRangeId}
          series={series}
          compute={compute}
        />
      </FailureTotalsContext>
    </MetricsSnapshotContext>
  );
}

const MonitoringScreen = memo(function MonitoringScreen({
  rangeId,
  onRangeChange,
  series,
  compute,
}: {
  rangeId: RangeId;
  onRangeChange: (rangeId: RangeId) => void;
  series: MetricsSeries;
  compute: { decoderTier: string; logicalCores: number } | null;
}) {
  const t = useTranslate();
  const labels = useMemo(() => axisLabels(series.timestamps), [series.timestamps]);
  const { throughput, segments, depths } = useMemo(() => chartSeries(t, series), [t, series]);

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      note={t("next.monitoring.note")}
      controls={
        <Segmented
          label={t("next.monitoring.range")}
          value={rangeId}
          onChange={onRangeChange}
          options={RANGES}
          className="font-wv-mono text-[11.5px]"
        />
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={<MonitoringStrip compute={compute} />}
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
          formatValue={formatRate}
        />
        <Chart
          title={t("next.monitoring.segments")}
          note={t("next.monitoring.segmentsNote")}
          series={segments}
          xLabels={labels}
          loading={series.isLoading}
          formatValue={formatPerSecond}
        />
        <Chart
          title={t("next.monitoring.depths")}
          note={t("next.monitoring.depthsNote")}
          series={depths}
          xLabels={labels}
          loading={series.isLoading}
          formatValue={formatCompactCount}
        />

        <EgressQuotaSection />

        <ProvidersSection />

        <FailuresSection series={series} />
      </div>
    </NextShell>
  );
});

/** One chart line: the series and the newest sample, formatted, for its legend. */
function line(
  values: number[],
  format: (value: number) => string,
  entry: Omit<ChartSeries, "values" | "value">,
): ChartSeries {
  return { ...entry, values, value: format(last(values)) };
}

/** The three charts' lines, worked out once per history rather than per sample. */
function chartSeries(t: Translate, series: MetricsSeries) {
  const throughput: ChartSeries[] = [
    line(series.rate("weaver_pipeline_bytes_downloaded_total"), formatRate, {
      key: "downloaded",
      label: t("next.monitoring.series.downloaded"),
      color: WV.accent,
      fill: WV_FILL.accent,
    }),
    line(series.rate("weaver_pipeline_bytes_committed_total"), formatRate, {
      key: "committed",
      label: t("next.monitoring.series.committed"),
      color: WV.info,
    }),
    line(series.rate("weaver_pipeline_bytes_decoded_total"), formatRate, {
      key: "decoded",
      label: t("next.monitoring.series.decoded"),
      color: WV.slate,
    }),
  ];

  const segments: ChartSeries[] = [
    line(series.rate("weaver_pipeline_segments_downloaded_total"), formatPerSecond, {
      key: "downloaded",
      label: t("next.monitoring.series.downloaded"),
      color: WV.info,
      fill: WV_FILL.info,
    }),
    line(series.rate("weaver_pipeline_segments_decoded_total"), formatPerSecond, {
      key: "decoded",
      label: t("next.monitoring.series.decoded"),
      color: WV.accent,
    }),
    line(series.rate("weaver_pipeline_segments_retried_total"), formatPerSecond, {
      key: "retried",
      label: t("next.monitoring.series.retried"),
      color: WV.warn,
    }),
  ];

  const depths: ChartSeries[] = [
    line(series.gauge("weaver_pipeline_download_queue_depth"), formatCount, {
      key: "download",
      label: t("next.monitoring.series.download"),
      color: WV.violet,
      fill: WV_FILL.violet,
    }),
    line(series.gauge("weaver_pipeline_decode_pending"), formatCount, {
      key: "decode",
      label: t("next.monitoring.series.decodePending"),
      color: WV.accent,
    }),
    line(series.gauge("weaver_pipeline_commit_pending"), formatCount, {
      key: "commit",
      label: t("next.monitoring.series.commitPending"),
      color: WV.info,
    }),
  ];

  return { throughput, segments, depths };
}

/** Every provider's open connections, against what each may open. */
function connectionTotals(providers: readonly ProviderHealth[]) {
  return {
    active: providers.reduce((total, provider) => total + provider.connectionsActive, 0),
    max: providers.reduce(
      (total, provider) => total + (provider.connectionsMax || provider.connectionsConfigured),
      0,
    ),
  };
}

/**
 * The figures across the top. They follow the live speed and the metrics
 * sample, so they redraw on their own rather than the screen redrawing for them.
 */
const MonitoringStrip = memo(function MonitoringStrip({
  compute,
}: {
  compute: { decoderTier: string; logicalCores: number } | null;
}) {
  const t = useTranslate();
  const metrics = useContext(MetricsSnapshotContext);
  const { speed, peakSpeed, providers } = useNextData("speed", "peakSpeed", "providers");
  const activeItems = useNextDataSelector((data) => data.queue.summary.activeItems);

  const { active: connectionsActive, max: connectionsMax } = connectionTotals(providers);
  const activeProviders = providers.filter((provider) => provider.connectionsActive > 0).length;

  const throughputNow = splitSpeed(speed);
  const peak = splitSpeed(peakSpeed);
  const buffered = splitBytes(metrics?.writeBufferedBytes ?? 0);
  const repairs = metrics ? metrics.repairActive + metrics.verifyActive : 0;
  const pipelineBusy = activeItems > 0 || repairs > 0;

  return (
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
        note={t("next.monitoring.pipelineNote", { downloads: activeItems, repairs })}
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
  );
});

/** One row per provider: its load, round trip, depth, failures and share of the connections. */
const ProvidersSection = memo(function ProvidersSection() {
  const t = useTranslate();
  const { providers } = useNextData("providers");
  const { active: connectionsActive } = connectionTotals(providers);

  return (
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
                {provider.pinnedAddress ? (
                  <span
                    title={t("next.monitoring.pinnedAddress")}
                    className="truncate font-wv-mono text-[11.5px] text-wv-muted"
                  >
                    {provider.pinnedAddress}
                  </span>
                ) : null}
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
  );
});

/** Each kind of failure: its rate now and its total since the start. */
const FailuresSection = memo(function FailuresSection({ series }: { series: MetricsSeries }) {
  const t = useTranslate();
  const totals = useContext(FailureTotalsContext);

  const failures = [
    {
      key: "articles-not-found",
      label: t("next.monitoring.failure.notFound"),
      color: WV.warn,
      rate: last(series.rate("weaver_pipeline_articles_not_found_total")),
      total: totals?.articlesNotFound ?? 0,
    },
    {
      key: "decode-errors",
      label: t("next.monitoring.failure.decode"),
      color: WV.error,
      rate: last(series.rate("weaver_pipeline_decode_errors_total")),
      total: totals?.decodeErrors ?? 0,
    },
    {
      key: "crc",
      label: t("next.monitoring.failure.crc"),
      color: WV.violet,
      rate: last(series.rate("weaver_pipeline_crc_errors_total")),
      total: totals?.crcErrors ?? 0,
    },
    {
      key: "permanent",
      label: t("next.monitoring.failure.permanent"),
      color: WV.error,
      rate: last(series.rate("weaver_pipeline_segments_failed_permanent_total")),
      total: totals?.segmentsFailedPermanent ?? 0,
    },
  ];

  return (
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
  );
});

/**
 * How much of each egress's download quota the current window has spent.
 *
 * The same counters the egress table lists, here because this is the
 * screen someone watching a download's pace is already on.
 */
function EgressQuotaSection() {
  const t = useTranslate();
  const { rows } = useEgressQuotas(t);
  const meteredRows = rows.filter(metered);

  if (meteredRows.length === 0) {
    const [before, after] = aroundSlot(t, "next.monitoring.noEgressQuota", "link");
    return (
      <section className="flex flex-none flex-col">
        <SectionHeader label={t("next.monitoring.egressQuotas")} note={t("next.monitoring.quotaNotEnforced")} sticky={false} />
        <div className="px-4 py-5 text-[13px] text-wv-muted sm:px-6">
          {before}
          <Link to="/settings/networking/egress" className="text-wv-accent hover:text-wv-accent-hover">
            {t("next.monitoring.egressLink")}
          </Link>
          {after}
        </div>
      </section>
    );
  }

  return (
    <section className="flex flex-none flex-col">
      <SectionHeader label={t("next.monitoring.egressQuotas")} sticky={false} />
      {meteredRows.map((egress) => (
        <div key={egress.id} className="flex flex-col gap-2 border-b border-wv-hairline px-4 py-[14px] sm:px-6">
          <div className="flex items-baseline justify-between gap-3 text-[13px]">
            <span className="truncate text-wv-fg">{egress.name}</span>
            <span className="font-wv-mono text-[11.5px] text-wv-muted">
              {t("next.monitoring.resetsAt", { time: formatDayClock(egress.quotaUsage?.periodEndEpochMs ?? null) })}
            </span>
          </div>
          {cappedDirections(egress).map((direction) => {
            const state = usageOf(egress, direction)?.state ?? "NORMAL";
            const percent = directionPercent(egress, direction);
            const used = usageOf(egress, direction)?.usedBytes ?? 0;
            const label = t(direction === "down" ? "next.quota.download" : "next.quota.upload");
            return (
              <div key={direction} className="flex flex-col gap-[6px]">
                <div className="flex items-baseline justify-between gap-3 font-wv-mono text-[11.5px] text-wv-muted">
                  <span>{label}</span>
                  {quotaHolding(state) ? <span className="text-wv-warn">{quotaStateLabel(t, state)}</span> : null}
                </div>
                <Bar
                  percent={percent}
                  color={usageColor(state, percent)}
                  label={t("next.monitoring.quotaUsed", { name: `${egress.name} ${label}` })}
                />
                <div className="flex flex-wrap items-baseline justify-between gap-x-4 font-wv-mono text-[11.5px] text-wv-muted">
                  <span>{t("next.monitoring.usedPercent", { size: formatSize(used), percent: Math.round(percent) })}</span>
                  <span>
                    {t("next.monitoring.allowance", { size: formatSize(directionCap(egress.quota, direction) ?? 0) })}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      ))}
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

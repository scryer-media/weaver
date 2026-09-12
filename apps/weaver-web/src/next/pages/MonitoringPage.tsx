import { useState } from "react";
import { useQuery } from "urql";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import type { MetricsHistoryRange } from "@/lib/metrics";
import { Bar, Eyebrow, MetricCell, SectionHeader, Square } from "../components/chrome";
import { Segmented } from "../components/controls";
import { Chart, type ChartSeries } from "../components/Chart";
import { columnStyle } from "../components/columns";
import { useNextData } from "../data/next-data";
import {
  EM_DASH,
  formatClock,
  formatCompactCount,
  formatCount,
  formatLatency,
  formatPerSecond,
  formatRate,
  splitBytes,
  splitSpeed,
} from "../data/format";
import { useMetricsSeries, useMetricsSnapshot } from "../data/use-metrics-series";
import { WV, WV_FILL } from "../data/palette";
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
  const { speed, peakSpeed, providers, queue } = useNextData();
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
      label: "Downloaded",
      color: WV.accent,
      fill: WV_FILL.accent,
      values: series.rate("weaver_pipeline_bytes_downloaded_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_downloaded_total"))),
    },
    {
      key: "committed",
      label: "Committed",
      color: WV.info,
      values: series.rate("weaver_pipeline_bytes_committed_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_committed_total"))),
    },
    {
      key: "decoded",
      label: "Decoded",
      color: WV.slate,
      values: series.rate("weaver_pipeline_bytes_decoded_total"),
      value: formatRate(last(series.rate("weaver_pipeline_bytes_decoded_total"))),
    },
  ];

  const segments: ChartSeries[] = [
    {
      key: "downloaded",
      label: "Downloaded",
      color: WV.info,
      fill: WV_FILL.info,
      values: series.rate("weaver_pipeline_segments_downloaded_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_downloaded_total"))),
    },
    {
      key: "decoded",
      label: "Decoded",
      color: WV.accent,
      values: series.rate("weaver_pipeline_segments_decoded_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_decoded_total"))),
    },
    {
      key: "retried",
      label: "Retried",
      color: WV.warn,
      values: series.rate("weaver_pipeline_segments_retried_total"),
      value: formatPerSecond(last(series.rate("weaver_pipeline_segments_retried_total"))),
    },
  ];

  const depths: ChartSeries[] = [
    {
      key: "download",
      label: "Download",
      color: WV.violet,
      fill: WV_FILL.violet,
      values: series.gauge("weaver_pipeline_download_queue_depth"),
      value: formatCount(last(series.gauge("weaver_pipeline_download_queue_depth"))),
    },
    {
      key: "decode",
      label: "Decode pending",
      color: WV.accent,
      values: series.gauge("weaver_pipeline_decode_pending"),
      value: formatCount(last(series.gauge("weaver_pipeline_decode_pending"))),
    },
    {
      key: "commit",
      label: "Commit pending",
      color: WV.info,
      values: series.gauge("weaver_pipeline_commit_pending"),
      value: formatCount(last(series.gauge("weaver_pipeline_commit_pending"))),
    },
  ];

  const failures = [
    {
      key: "articles-not-found",
      label: "Articles not found",
      color: WV.warn,
      rate: last(series.rate("weaver_pipeline_articles_not_found_total")),
      total: metrics?.articlesNotFound ?? 0,
    },
    {
      key: "decode-errors",
      label: "Decode errors",
      color: WV.error,
      rate: last(series.rate("weaver_pipeline_decode_errors_total")),
      total: metrics?.decodeErrors ?? 0,
    },
    {
      key: "crc",
      label: "CRC mismatches",
      color: WV.violet,
      rate: last(series.rate("weaver_pipeline_crc_errors_total")),
      total: metrics?.crcErrors ?? 0,
    },
    {
      key: "permanent",
      label: "Permanent failures",
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
      title="Monitoring"
      note="pipeline pressure, providers, failures"
      controls={
        <Segmented
          label="Time range"
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
            eyebrow="Pipeline"
            value={pipelineBusy ? "Active" : "Idle"}
            valueClassName={pipelineBusy ? "text-wv-accent" : "text-wv-idle"}
            note={`${queue.summary.activeItems} transfers, ${repairs} repairs`}
          />
          <MetricCell
            eyebrow="Throughput"
            value={throughputNow.value}
            unit={throughputNow.unit}
            note={peakSpeed > 0 ? `peak ${peak.value} ${peak.unit} this session` : "no traffic yet"}
          />
          <MetricCell
            eyebrow="Threads"
            value={formatCount(connectionsActive)}
            unit={`/ ${connectionsMax}`}
            note={`${activeProviders} ${activeProviders === 1 ? "provider" : "providers"} active`}
          />
          <MetricCell
            eyebrow="Decode"
            value={formatCount(metrics?.articlesPerSec ?? 0)}
            unit="seg/s"
            note={
              compute
                ? `${compute.decoderTier} · ${compute.logicalCores} workers`
                : "decoder tier unknown"
            }
          />
          <MetricCell
            last
            eyebrow="Write queue"
            value={buffered.value}
            unit={`${buffered.unit} buffered`}
            valueClassName={(metrics?.writeBufferedBytes ?? 0) > 0 ? "text-wv-warn" : undefined}
            note={`p99 ${formatLatency((metrics?.diskWriteLatencyUs ?? 0) / 1000)}`}
          />
        </div>
      }
      statusRight={
        series.error
          ? series.error
          : `sampled every 5s · ${RANGES.find((entry) => entry.value === rangeId)!.label} window`
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <Chart
          title="Throughput"
          note="bytes per second, downloaded vs committed"
          series={throughput}
          xLabels={labels}
          formatValue={(value) => formatRate(value)}
        />
        <Chart
          title="Segments"
          note="downloaded, decoded and retried per second"
          series={segments}
          xLabels={labels}
          formatValue={(value) => formatPerSecond(value)}
        />
        <Chart
          title="Queue depths"
          note="backlog across download, decode and commit"
          series={depths}
          xLabels={labels}
          formatValue={(value) => formatCompactCount(value)}
        />

        <section className="flex flex-none flex-col">
          <SectionHeader
            label="Providers"
            note="connection load, round-trip and failures"
            sticky={false}
          />
          {providers.length === 0 ? (
            <div className="px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
              No providers configured — add one in Settings → Providers.
            </div>
          ) : (
            providers.map((provider) => {
              const max = provider.connectionsMax || provider.connectionsConfigured;
              const share =
                connectionsActive > 0
                  ? Math.round((provider.connectionsActive / connectionsActive) * 100)
                  : 0;
              const degraded = provider.state !== "healthy";
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
                      title="round-trip"
                      className={`min-w-[68px] text-right font-wv-mono text-[12.5px] ${degraded ? "text-wv-warn" : "text-wv-secondary"}`}
                    >
                      {formatLatency(provider.latencyMs)}
                    </span>
                    <span
                      title="failures since start"
                      className="w-[74px] text-right font-wv-mono text-[12.5px] text-wv-secondary"
                    >
                      {formatCount(provider.failureCount)}
                    </span>
                    <span
                      title="share of active connections"
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
          <SectionHeader label="Failures" note="counter-derived, since startup" sticky={false} />
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

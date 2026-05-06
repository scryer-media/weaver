import { useEffect, useRef, useState } from "react";
import type uPlot from "uplot";
import { useClient } from "urql";
import { METRICS_HISTORY_QUERY } from "@/graphql/queries";
import type { JobData } from "@/lib/job-types";
import {
  COUNTER_HISTORY_METRIC_NAMES,
  GAUGE_HISTORY_METRIC_NAMES,
  JOB_STATUS_COLORS,
  JOB_STATUS_HISTORY_METRIC,
  JOB_STATUS_LABEL_ORDER,
  METRIC_CHART_DEFINITIONS,
  PROM_METRIC_TO_SNAPSHOT_FIELD,
  humanizeJobStatusLabel,
  isRollupMetricsRange,
  jobStatusMetricLabelFromLiveStatus,
  metricsRangeWindowMinutes,
  type ChartColorToken,
  type ChartDisplayVariant,
  type HistoryMetricName,
  type MetricChartDefinition,
  type MetricScale,
  type MetricValueFormat,
  type MetricsHistoryRange,
  type MetricsHistorySeriesVariant,
  type MetricsJobStatusRow,
  type MetricsSnapshot,
  type SnapshotHistoryMetricName,
} from "@/lib/metrics";

type MetricsHistoryResponse = {
  metricsHistory: {
    timestamps: number[];
    resolutionSec: number;
    series: {
      metric: string;
      variant: MetricsHistorySeriesVariant;
      labels: {
        key: string;
        value: string;
      }[];
      values: number[];
    }[];
  };
};

type MetricsHistoryVariables = {
  range: MetricsHistoryRange;
};

type MetricsHistorySeries = MetricsHistoryResponse["metricsHistory"]["series"][number];
type MetricsHistoryLabel = MetricsHistorySeries["labels"][number];

type HistoryBuffer = {
  resolutionSec: number;
  timestampsMs: number[];
  series: MetricsHistorySeries[];
  persistedPointCount: number;
};

type LiveSample = {
  timestampMs: number;
  metrics: MetricsSnapshot;
};

const LIVE_SNAPSHOT_INTERVAL_MS = 10_000;

export interface MetricsChartDisplaySeries {
  labelKey: string;
  colorToken: ChartColorToken;
  format: MetricValueFormat;
  scale?: MetricScale;
  variant: ChartDisplayVariant;
}

export interface MetricsChartModel {
  definition: MetricChartDefinition;
  data: uPlot.AlignedData;
  series: MetricsChartDisplaySeries[];
}

interface UseMetricsHistoryOptions {
  range: MetricsHistoryRange;
  liveMetrics?: MetricsSnapshot;
  liveJobs?: JobData[];
}

interface UseMetricsHistoryResult {
  charts: MetricsChartModel[];
  jobStatusRows: MetricsJobStatusRow[];
  isLoading: boolean;
  error: string | null;
}

function createEmptyBuffer(): HistoryBuffer {
  return {
    resolutionSec: 10,
    timestampsMs: [],
    series: [],
    persistedPointCount: 0,
  };
}

function bufferFromResponse(data?: MetricsHistoryResponse["metricsHistory"]): HistoryBuffer {
  if (!data) {
    return createEmptyBuffer();
  }

  return {
    resolutionSec: data.resolutionSec ?? 10,
    timestampsMs: [...(data.timestamps ?? [])],
    series: (data.series ?? []).map((series) => ({
      metric: series.metric,
      variant: series.variant,
      labels: (series.labels ?? []).map((label) => ({ ...label })),
      values: [...(series.values ?? [])],
    })),
    persistedPointCount: data.timestamps?.length ?? 0,
  };
}

function trimBufferToRange(buffer: HistoryBuffer, cutoffMs: number) {
  const startIndex = buffer.timestampsMs.findIndex((timestamp) => timestamp >= cutoffMs);
  if (startIndex === -1) {
    buffer.timestampsMs = [];
    buffer.series = buffer.series.map((series) => ({ ...series, values: [] }));
    buffer.persistedPointCount = 0;
    return;
  }

  if (startIndex === 0) {
    return;
  }

  buffer.timestampsMs = buffer.timestampsMs.slice(startIndex);
  buffer.series = buffer.series.map((series) => ({
    ...series,
    values: series.values.slice(startIndex),
  }));
  buffer.persistedPointCount = Math.max(0, buffer.persistedPointCount - startIndex);
}

function sortLabels(labels: MetricsHistoryLabel[]): MetricsHistoryLabel[] {
  return [...labels].sort(
    (left, right) => left.key.localeCompare(right.key) || left.value.localeCompare(right.value),
  );
}

function labelsEqual(left: MetricsHistoryLabel[], right: MetricsHistoryLabel[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  return left.every(
    (label, index) => label.key === right[index]?.key && label.value === right[index]?.value,
  );
}

function ensureSeries(
  buffer: HistoryBuffer,
  metric: string,
  variant: MetricsHistorySeriesVariant,
  labels: MetricsHistoryLabel[] = [],
): MetricsHistorySeries {
  const normalizedLabels = sortLabels(labels);
  const existing = buffer.series.find(
    (series) =>
      series.metric === metric
      && series.variant === variant
      && labelsEqual(series.labels, normalizedLabels),
  );
  if (existing) {
    return existing;
  }

  const nextSeries: MetricsHistorySeries = {
    metric,
    variant,
    labels: normalizedLabels,
    values: new Array(buffer.timestampsMs.length).fill(0),
  };
  buffer.series.push(nextSeries);
  return nextSeries;
}

function appendTimestamp(buffer: HistoryBuffer, timestampMs: number): number {
  const lastTimestampMs = buffer.timestampsMs[buffer.timestampsMs.length - 1];
  const replaceLastPoint =
    typeof lastTimestampMs === "number"
    && Math.floor(lastTimestampMs / LIVE_SNAPSHOT_INTERVAL_MS)
      === Math.floor(timestampMs / LIVE_SNAPSHOT_INTERVAL_MS);

  if (!replaceLastPoint) {
    buffer.timestampsMs.push(timestampMs);
    for (const series of buffer.series) {
      series.values.push(0);
    }
    return buffer.timestampsMs.length - 1;
  }

  return buffer.timestampsMs.length - 1;
}

function appendRollupOverlayTimestamp(buffer: HistoryBuffer, timestampMs: number): number {
  if (buffer.timestampsMs.length > buffer.persistedPointCount) {
    const overlayIndex = buffer.timestampsMs.length - 1;
    buffer.timestampsMs[overlayIndex] = timestampMs;
    return overlayIndex;
  }

  buffer.timestampsMs.push(timestampMs);
  for (const series of buffer.series) {
    series.values.push(0);
  }
  return buffer.timestampsMs.length - 1;
}

function applyLiveJobStatuses(
  buffer: HistoryBuffer,
  variants: readonly MetricsHistorySeriesVariant[],
  liveJobs: JobData[],
  targetIndex: number,
) {
  for (const status of JOB_STATUS_LABEL_ORDER) {
    for (const variant of variants) {
      const series = ensureSeries(buffer, JOB_STATUS_HISTORY_METRIC, variant, [
        { key: "status", value: status },
      ]);
      series.values[targetIndex] = 0;
    }
  }

  const countsByStatus = new Map<string, number>();
  for (const job of liveJobs) {
    const status = jobStatusMetricLabelFromLiveStatus(job.status);
    if (!status) {
      continue;
    }
    countsByStatus.set(status, (countsByStatus.get(status) ?? 0) + 1);
  }

  for (const status of JOB_STATUS_LABEL_ORDER) {
    const count = countsByStatus.get(status) ?? 0;
    for (const variant of variants) {
      const series = ensureSeries(buffer, JOB_STATUS_HISTORY_METRIC, variant, [
        { key: "status", value: status },
      ]);
      series.values[targetIndex] = count;
    }
  }
}

function applyLiveRawSnapshot(
  buffer: HistoryBuffer,
  liveMetrics: MetricsSnapshot,
  liveJobs: JobData[] | undefined,
  timestampMs: number,
  range: MetricsHistoryRange,
): boolean {
  const roundedTimestampMs =
    Math.floor(timestampMs / LIVE_SNAPSHOT_INTERVAL_MS) * LIVE_SNAPSHOT_INTERVAL_MS;
  const targetIndex = appendTimestamp(buffer, roundedTimestampMs);

  for (const metric of [...COUNTER_HISTORY_METRIC_NAMES, ...GAUGE_HISTORY_METRIC_NAMES]) {
    const snapshotKey = PROM_METRIC_TO_SNAPSHOT_FIELD[metric];
    const series = ensureSeries(buffer, metric, "ACTUAL");
    series.values[targetIndex] = liveMetrics[snapshotKey];
  }

  if (liveJobs) {
    applyLiveJobStatuses(buffer, ["ACTUAL"], liveJobs, targetIndex);
  }

  trimBufferToRange(buffer, roundedTimestampMs - metricsRangeWindowMinutes(range) * 60 * 1000);
  return true;
}

function computeCounterLiveRate(
  metric: SnapshotHistoryMetricName,
  previous: LiveSample | null,
  current: LiveSample,
): number {
  if (!previous) {
    return 0;
  }

  const elapsedSec = (current.timestampMs - previous.timestampMs) / 1000;
  if (elapsedSec <= 0) {
    return 0;
  }

  const snapshotKey = PROM_METRIC_TO_SNAPSHOT_FIELD[metric];
  const delta = current.metrics[snapshotKey] - previous.metrics[snapshotKey];
  return delta < 0 ? 0 : delta / elapsedSec;
}

function applyLiveRollupSnapshot(
  buffer: HistoryBuffer,
  liveMetrics: MetricsSnapshot,
  liveJobs: JobData[] | undefined,
  currentSample: LiveSample,
  previousSample: LiveSample | null,
  range: MetricsHistoryRange,
): boolean {
  const roundedTimestampMs =
    Math.floor(currentSample.timestampMs / LIVE_SNAPSHOT_INTERVAL_MS) * LIVE_SNAPSHOT_INTERVAL_MS;
  const targetIndex = appendRollupOverlayTimestamp(buffer, roundedTimestampMs);

  for (const metric of COUNTER_HISTORY_METRIC_NAMES) {
    const liveRate = computeCounterLiveRate(metric, previousSample, currentSample);
    const avgSeries = ensureSeries(buffer, metric, "AVG");
    const peakSeries = ensureSeries(buffer, metric, "PEAK");
    avgSeries.values[targetIndex] = liveRate;
    peakSeries.values[targetIndex] = liveRate;
  }

  for (const metric of GAUGE_HISTORY_METRIC_NAMES) {
    const snapshotKey = PROM_METRIC_TO_SNAPSHOT_FIELD[metric];
    const nextValue = liveMetrics[snapshotKey];
    const avgSeries = ensureSeries(buffer, metric, "AVG");
    const peakSeries = ensureSeries(buffer, metric, "PEAK");
    avgSeries.values[targetIndex] = nextValue;
    peakSeries.values[targetIndex] = nextValue;
  }

  if (liveJobs) {
    applyLiveJobStatuses(buffer, ["AVG", "PEAK"], liveJobs, targetIndex);
  }

  trimBufferToRange(buffer, roundedTimestampMs - metricsRangeWindowMinutes(range) * 60 * 1000);
  return true;
}

function lookupSeriesValues(
  buffer: HistoryBuffer,
  metric: HistoryMetricName,
  variant: MetricsHistorySeriesVariant,
  labels: MetricsHistoryLabel[] = [],
): number[] {
  const normalizedLabels = sortLabels(labels);
  const values = buffer.series.find(
    (series) =>
      series.metric === metric
      && series.variant === variant
      && labelsEqual(series.labels, normalizedLabels),
  )?.values;

  if (!values) {
    return new Array(buffer.timestampsMs.length).fill(0);
  }

  if (values.length < buffer.timestampsMs.length) {
    return [...values, ...new Array(buffer.timestampsMs.length - values.length).fill(0)];
  }

  return values.slice(0, buffer.timestampsMs.length);
}

function buildRateSeries(timestampsMs: number[], values: number[]): number[] {
  if (values.length === 0) {
    return [];
  }

  const rates = new Array(values.length).fill(0);
  for (let index = 1; index < values.length; index += 1) {
    const elapsedSec = (timestampsMs[index] - timestampsMs[index - 1]) / 1000;
    if (elapsedSec <= 0) {
      rates[index] = 0;
      continue;
    }

    const delta = values[index] - values[index - 1];
    rates[index] = delta < 0 ? 0 : delta / elapsedSec;
  }

  return rates;
}

function buildChartModel(
  buffer: HistoryBuffer,
  definition: MetricChartDefinition,
  range: MetricsHistoryRange,
): MetricsChartModel {
  const timestampsSec = buffer.timestampsMs.map((timestamp) => timestamp / 1000);
  const rollupRange = isRollupMetricsRange(range);
  const displaySeries: MetricsChartDisplaySeries[] = [];
  const lineValues: number[][] = [];

  for (const line of definition.lines) {
    if (!rollupRange) {
      const actualValues = lookupSeriesValues(buffer, line.metric, "ACTUAL");
      lineValues.push(
        line.kind === "counter" ? buildRateSeries(buffer.timestampsMs, actualValues) : actualValues,
      );
      displaySeries.push({
        labelKey: line.labelKey,
        colorToken: line.colorToken,
        format: line.format,
        scale: line.scale,
        variant: "actual",
      });
      continue;
    }

    lineValues.push(lookupSeriesValues(buffer, line.metric, "AVG"));
    displaySeries.push({
      labelKey: line.labelKey,
      colorToken: line.colorToken,
      format: line.format,
      scale: line.scale,
      variant: "avg",
    });

    lineValues.push(lookupSeriesValues(buffer, line.metric, "PEAK"));
    displaySeries.push({
      labelKey: line.labelKey,
      colorToken: line.colorToken,
      format: line.format,
      scale: line.scale,
      variant: "peak",
    });
  }

  return {
    definition,
    series: displaySeries,
    data: [timestampsSec, ...lineValues],
  };
}

function buildChartModels(buffer: HistoryBuffer, range: MetricsHistoryRange): MetricsChartModel[] {
  return METRIC_CHART_DEFINITIONS.map((definition) => buildChartModel(buffer, definition, range));
}

function readLatestSeriesValue(series: MetricsHistorySeries, index: number): number {
  const value = series.values[index];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return value;
}

function buildJobStatusRows(
  buffer: HistoryBuffer,
  range: MetricsHistoryRange,
): MetricsJobStatusRow[] {
  if (buffer.timestampsMs.length === 0) {
    return [];
  }

  const latestIndex = buffer.timestampsMs.length - 1;
  const expectedVariant: MetricsHistorySeriesVariant = isRollupMetricsRange(range) ? "AVG" : "ACTUAL";
  const countsByStatus = new Map<string, number>();

  for (const series of buffer.series) {
    if (series.metric !== JOB_STATUS_HISTORY_METRIC || series.variant !== expectedVariant) {
      continue;
    }

    const status = series.labels.find((label) => label.key === "status")?.value;
    if (!status) {
      continue;
    }

    countsByStatus.set(status, Math.max(0, Math.round(readLatestSeriesValue(series, latestIndex))));
  }

  return JOB_STATUS_LABEL_ORDER
    .filter((status) => (countsByStatus.get(status) ?? 0) > 0)
    .map((status) => ({
      status,
      label: humanizeJobStatusLabel(status),
      count: countsByStatus.get(status) ?? 0,
      color: JOB_STATUS_COLORS[status] ?? "#64748b",
    }));
}

export function useMetricsHistory({
  range,
  liveMetrics,
  liveJobs,
}: UseMetricsHistoryOptions): UseMetricsHistoryResult {
  const client = useClient();
  const [charts, setCharts] = useState<MetricsChartModel[]>(() =>
    buildChartModels(createEmptyBuffer(), range)
  );
  const [jobStatusRows, setJobStatusRows] = useState<MetricsJobStatusRow[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const historyRef = useRef<HistoryBuffer>(createEmptyBuffer());
  const latestLiveMetricsRef = useRef<MetricsSnapshot | undefined>(liveMetrics);
  const latestLiveJobsRef = useRef<JobData[] | undefined>(liveJobs);
  const previousLiveSampleRef = useRef<LiveSample | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setIsLoading(true);
      setError(null);

      const result = await client
        .query<MetricsHistoryResponse, MetricsHistoryVariables>(
          METRICS_HISTORY_QUERY,
          { range },
          { requestPolicy: "network-only" },
        )
        .toPromise();

      if (cancelled) {
        return;
      }

      if (result.error) {
        const emptyBuffer = createEmptyBuffer();
        historyRef.current = emptyBuffer;
        setCharts(buildChartModels(emptyBuffer, range));
        setJobStatusRows(buildJobStatusRows(emptyBuffer, range));
        setError(result.error.message);
        setIsLoading(false);
        return;
      }

      const nextBuffer = bufferFromResponse(result.data?.metricsHistory);
      historyRef.current = nextBuffer;
      setCharts(buildChartModels(nextBuffer, range));
      setJobStatusRows(buildJobStatusRows(nextBuffer, range));
      previousLiveSampleRef.current = null;
      setIsLoading(false);
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [client, range]);

  useEffect(() => {
    latestLiveMetricsRef.current = liveMetrics;
    latestLiveJobsRef.current = liveJobs;
  }, [liveJobs, liveMetrics]);

  useEffect(() => {
    let cancelled = false;
    let timeoutId: number | null = null;

    const sampleLatestSnapshot = () => {
      if (cancelled || !latestLiveMetricsRef.current) {
        return;
      }

      const nextBuffer = historyRef.current;
      const currentSample: LiveSample = {
        metrics: latestLiveMetricsRef.current,
        timestampMs: Date.now(),
      };
      const changed = isRollupMetricsRange(range)
        ? applyLiveRollupSnapshot(
            nextBuffer,
            latestLiveMetricsRef.current,
            latestLiveJobsRef.current,
            currentSample,
            previousLiveSampleRef.current,
            range,
          )
        : applyLiveRawSnapshot(
            nextBuffer,
            latestLiveMetricsRef.current,
            latestLiveJobsRef.current,
            currentSample.timestampMs,
            range,
          );
      previousLiveSampleRef.current = currentSample;
      if (!changed) {
        return;
      }

      setCharts(buildChartModels(nextBuffer, range));
      setJobStatusRows(buildJobStatusRows(nextBuffer, range));
    };

    const scheduleNextTick = () => {
      if (cancelled) {
        return;
      }

      const now = Date.now();
      const delay = LIVE_SNAPSHOT_INTERVAL_MS - (now % LIVE_SNAPSHOT_INTERVAL_MS);
      timeoutId = window.setTimeout(() => {
        sampleLatestSnapshot();
        scheduleNextTick();
      }, delay);
    };

    scheduleNextTick();

    return () => {
      cancelled = true;
      previousLiveSampleRef.current = null;
      if (timeoutId != null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [range]);

  return {
    charts,
    jobStatusRows,
    isLoading,
    error,
  };
}

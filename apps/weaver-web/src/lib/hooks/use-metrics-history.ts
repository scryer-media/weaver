import { useEffect, useRef, useState } from "react";
import type uPlot from "uplot";
import { useClient } from "urql";
import { METRICS_HISTORY_QUERY } from "@/graphql/queries";
import type { JobData } from "@/lib/job-types";
import {
  HISTORY_METRIC_NAMES,
  JOB_STATUS_COLORS,
  JOB_STATUS_HISTORY_METRIC,
  JOB_STATUS_LABEL_ORDER,
  METRIC_CHART_DEFINITIONS,
  PROM_METRIC_TO_SNAPSHOT_FIELD,
  SNAPSHOT_HISTORY_METRIC_NAMES,
  humanizeJobStatusLabel,
  jobStatusMetricLabelFromLiveStatus,
  type HistoryMetricName,
  type MetricChartDefinition,
  type MetricsJobStatusRow,
  type MetricsRangeMinutes,
  type MetricsSnapshot,
} from "@/lib/metrics";

type MetricsHistoryResponse = {
  metricsHistory: {
    timestamps: number[];
    series: {
      metric: string;
      labels: {
        key: string;
        value: string;
      }[];
      values: number[];
    }[];
  };
};

type MetricsHistoryVariables = {
  minutes: number;
  metrics: string[];
};

type MetricsHistorySeries = MetricsHistoryResponse["metricsHistory"]["series"][number];
type MetricsHistoryLabel = MetricsHistorySeries["labels"][number];

type HistoryBuffer = {
  timestampsMs: number[];
  series: MetricsHistorySeries[];
};

const LIVE_SNAPSHOT_INTERVAL_MS = 10_000;

export interface MetricsChartModel {
  definition: MetricChartDefinition;
  data: uPlot.AlignedData;
}

interface UseMetricsHistoryOptions {
  minutes: MetricsRangeMinutes;
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
    timestampsMs: [],
    series: [],
  };
}

function cloneBuffer(buffer: HistoryBuffer): HistoryBuffer {
  return {
    timestampsMs: [...buffer.timestampsMs],
    series: (buffer.series ?? []).map((series) => ({
      metric: series.metric,
      labels: (series.labels ?? []).map((label) => ({ ...label })),
      values: [...(series.values ?? [])],
    })),
  };
}

function bufferFromResponse(data?: MetricsHistoryResponse["metricsHistory"]): HistoryBuffer {
  if (!data) {
    return createEmptyBuffer();
  }

  return {
    timestampsMs: [...(data.timestamps ?? [])],
    series: (data.series ?? []).map((series) => ({
      metric: series.metric,
      labels: (series.labels ?? []).map((label) => ({ ...label })),
      values: [...(series.values ?? [])],
    })),
  };
}

function trimBufferToRange(buffer: HistoryBuffer, cutoffMs: number) {
  const startIndex = buffer.timestampsMs.findIndex((timestamp) => timestamp >= cutoffMs);
  if (startIndex === -1) {
    buffer.timestampsMs = [];
    buffer.series = buffer.series.map((series) => ({
      ...series,
      values: [],
    }));
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
  labels: MetricsHistoryLabel[] = [],
): MetricsHistorySeries {
  const normalizedLabels = sortLabels(labels);
  const existing = buffer.series.find(
    (series) => series.metric === metric && labelsEqual(series.labels, normalizedLabels),
  );
  if (existing) {
    return existing;
  }

  const nextSeries: MetricsHistorySeries = {
    metric,
    labels: normalizedLabels,
    values: new Array(buffer.timestampsMs.length).fill(0),
  };
  buffer.series.push(nextSeries);
  return nextSeries;
}

function applyLiveJobStatuses(buffer: HistoryBuffer, liveJobs: JobData[], targetIndex: number) {
  for (const series of buffer.series) {
    if (series.metric === JOB_STATUS_HISTORY_METRIC) {
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
    const series = ensureSeries(buffer, JOB_STATUS_HISTORY_METRIC, [{ key: "status", value: status }]);
    series.values[targetIndex] = countsByStatus.get(status) ?? 0;
  }
}

function applyLiveSnapshot(
  buffer: HistoryBuffer,
  liveMetrics: MetricsSnapshot,
  liveJobs: JobData[] | undefined,
  timestampMs: number,
  minutes: MetricsRangeMinutes,
) {
  const roundedTimestampMs =
    Math.floor(timestampMs / LIVE_SNAPSHOT_INTERVAL_MS) * LIVE_SNAPSHOT_INTERVAL_MS;
  const lastTimestampMs = buffer.timestampsMs[buffer.timestampsMs.length - 1];
  const replaceLastPoint =
    typeof lastTimestampMs === "number"
    && Math.floor(lastTimestampMs / LIVE_SNAPSHOT_INTERVAL_MS)
      === Math.floor(roundedTimestampMs / LIVE_SNAPSHOT_INTERVAL_MS);

  let targetIndex = buffer.timestampsMs.length - 1;
  if (!replaceLastPoint) {
    buffer.timestampsMs.push(roundedTimestampMs);
    for (const series of buffer.series) {
      series.values.push(0);
    }
    targetIndex = buffer.timestampsMs.length - 1;
  }

  for (const metric of SNAPSHOT_HISTORY_METRIC_NAMES) {
    const snapshotKey = PROM_METRIC_TO_SNAPSHOT_FIELD[metric];
    const series = ensureSeries(buffer, metric);
    series.values[targetIndex] = liveMetrics[snapshotKey];
  }

  if (liveJobs) {
    applyLiveJobStatuses(buffer, liveJobs, targetIndex);
  }

  trimBufferToRange(buffer, roundedTimestampMs - minutes * 60 * 1000);
}

function lookupSeriesValues(buffer: HistoryBuffer, metric: HistoryMetricName): number[] {
  const values = buffer.series.find(
    (series) => series.metric === metric && series.labels.length === 0,
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

function buildChartModel(buffer: HistoryBuffer, definition: MetricChartDefinition): MetricsChartModel {
  const timestampsSec = buffer.timestampsMs.map((timestamp) => timestamp / 1000);
  const lineValues = definition.lines.map((line) => {
    const values = lookupSeriesValues(buffer, line.metric);
    return line.mode === "rate" ? buildRateSeries(buffer.timestampsMs, values) : values;
  });

  return {
    definition,
    data: [timestampsSec, ...lineValues],
  };
}

function buildChartModels(buffer: HistoryBuffer): MetricsChartModel[] {
  return METRIC_CHART_DEFINITIONS.map((definition) => buildChartModel(buffer, definition));
}

function readLatestSeriesValue(series: MetricsHistorySeries, index: number): number {
  const value = series.values[index];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return value;
}

function buildJobStatusRows(buffer: HistoryBuffer): MetricsJobStatusRow[] {
  if (buffer.timestampsMs.length === 0) {
    return [];
  }

  const latestIndex = buffer.timestampsMs.length - 1;
  const countsByStatus = new Map<string, number>();

  for (const series of buffer.series) {
    if (series.metric !== JOB_STATUS_HISTORY_METRIC) {
      continue;
    }

    const status = series.labels.find((label) => label.key === "status")?.value;
    if (!status) {
      continue;
    }

    countsByStatus.set(status, Math.max(0, Math.round(readLatestSeriesValue(series, latestIndex))));
  }

  const orderedStatuses = Array.from(countsByStatus.keys()).sort((left, right) => {
    const leftRank = JOB_STATUS_LABEL_ORDER.indexOf(left as (typeof JOB_STATUS_LABEL_ORDER)[number]);
    const rightRank = JOB_STATUS_LABEL_ORDER.indexOf(right as (typeof JOB_STATUS_LABEL_ORDER)[number]);
    const normalizedLeftRank = leftRank === -1 ? Number.MAX_SAFE_INTEGER : leftRank;
    const normalizedRightRank = rightRank === -1 ? Number.MAX_SAFE_INTEGER : rightRank;

    return normalizedLeftRank - normalizedRightRank || left.localeCompare(right);
  });

  return orderedStatuses
    .map((status) => ({
      status,
      label: humanizeJobStatusLabel(status),
      count: countsByStatus.get(status) ?? 0,
      color: JOB_STATUS_COLORS[status as keyof typeof JOB_STATUS_COLORS] ?? "#64748b",
    }))
    .filter((row) => row.count > 0);
}

export function useMetricsHistory({
  minutes,
  liveMetrics,
  liveJobs,
}: UseMetricsHistoryOptions): UseMetricsHistoryResult {
  const client = useClient();
  const [charts, setCharts] = useState<MetricsChartModel[]>(() => buildChartModels(createEmptyBuffer()));
  const [jobStatusRows, setJobStatusRows] = useState<MetricsJobStatusRow[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const historyRef = useRef<HistoryBuffer>(createEmptyBuffer());
  const latestLiveMetricsRef = useRef<MetricsSnapshot | undefined>(liveMetrics);
  const latestLiveJobsRef = useRef<JobData[] | undefined>(liveJobs);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setIsLoading(true);
      setError(null);

      const result = await client
        .query<MetricsHistoryResponse, MetricsHistoryVariables>(
          METRICS_HISTORY_QUERY,
          {
            minutes,
            metrics: [...HISTORY_METRIC_NAMES],
          },
          { requestPolicy: "network-only" },
        )
        .toPromise();

      if (cancelled) {
        return;
      }

      if (result.error) {
        const emptyBuffer = createEmptyBuffer();
        historyRef.current = emptyBuffer;
        setCharts(buildChartModels(emptyBuffer));
        setJobStatusRows(buildJobStatusRows(emptyBuffer));
        setError(result.error.message);
        setIsLoading(false);
        return;
      }

      const nextBuffer = bufferFromResponse(result.data?.metricsHistory);
      if (latestLiveMetricsRef.current) {
        applyLiveSnapshot(
          nextBuffer,
          latestLiveMetricsRef.current,
          latestLiveJobsRef.current,
          Date.now(),
          minutes,
        );
      }

      historyRef.current = nextBuffer;
      setCharts(buildChartModels(nextBuffer));
      setJobStatusRows(buildJobStatusRows(nextBuffer));
      setIsLoading(false);
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [client, minutes]);

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

      const nextBuffer = cloneBuffer(historyRef.current);
      applyLiveSnapshot(
        nextBuffer,
        latestLiveMetricsRef.current,
        latestLiveJobsRef.current,
        Date.now(),
        minutes,
      );
      historyRef.current = nextBuffer;
      setCharts(buildChartModels(nextBuffer));
      setJobStatusRows(buildJobStatusRows(nextBuffer));
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
      if (timeoutId != null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [minutes]);

  return {
    charts,
    jobStatusRows,
    isLoading,
    error,
  };
}

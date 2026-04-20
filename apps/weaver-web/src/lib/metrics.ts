import { formatBytes } from "@/components/SpeedDisplay";
import type { DownloadBlockState } from "@/lib/context/live-data-context";

export interface MetricsSnapshot {
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
}

export interface MetricsPageData {
  metrics: MetricsSnapshot;
  globalState: {
    isPaused: boolean;
    downloadBlock: DownloadBlockState;
  };
}

export const METRICS_RANGE_OPTIONS = [
  { minutes: 10, labelKey: "metrics.range10m" },
  { minutes: 60, labelKey: "metrics.range1h" },
  { minutes: 360, labelKey: "metrics.range6h" },
  { minutes: 1440, labelKey: "metrics.range24h" },
] as const;

export type MetricsRangeMinutes = (typeof METRICS_RANGE_OPTIONS)[number]["minutes"];

export const JOB_STATUS_HISTORY_METRIC = "weaver_pipeline_jobs" as const;

export const JOB_STATUS_LABEL_ORDER = [
  "queued",
  "downloading",
  "paused",
  "checking",
  "verifying",
  "queued_repair",
  "repairing",
  "queued_extract",
  "extracting",
  "moving",
  "failed",
  "complete",
] as const;

export type JobStatusMetricLabel = (typeof JOB_STATUS_LABEL_ORDER)[number];

export interface MetricsJobStatusRow {
  status: string;
  label: string;
  count: number;
  color: string;
}

export const JOB_STATUS_COLORS: Record<JobStatusMetricLabel, string> = {
  queued: "#14b8a6",
  downloading: "#0ea5e9",
  paused: "#94a3b8",
  checking: "#f59e0b",
  verifying: "#f59e0b",
  queued_repair: "#fb923c",
  repairing: "#f97316",
  queued_extract: "#7c3aed",
  extracting: "#8b5cf6",
  moving: "#4f46e5",
  failed: "#ef4444",
  complete: "#22c55e",
};

const LIVE_JOB_STATUS_TO_METRIC_LABEL: Record<string, JobStatusMetricLabel> = {
  QUEUED: "queued",
  DOWNLOADING: "downloading",
  PAUSED: "paused",
  CHECKING: "verifying",
  VERIFYING: "verifying",
  QUEUED_REPAIR: "queued_repair",
  REPAIRING: "repairing",
  QUEUED_EXTRACT: "queued_extract",
  EXTRACTING: "extracting",
  MOVING: "moving",
  FAILED: "failed",
  COMPLETE: "complete",
};

export function jobStatusMetricLabelFromLiveStatus(status: string): JobStatusMetricLabel | null {
  return LIVE_JOB_STATUS_TO_METRIC_LABEL[status] ?? null;
}

export function humanizeJobStatusLabel(status: string): string {
  return status
    .toLowerCase()
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export const SNAPSHOT_HISTORY_METRIC_NAMES = [
  "weaver_pipeline_bytes_downloaded_total",
  "weaver_pipeline_bytes_decoded_total",
  "weaver_pipeline_bytes_committed_total",
  "weaver_pipeline_segments_downloaded_total",
  "weaver_pipeline_segments_decoded_total",
  "weaver_pipeline_segments_committed_total",
  "weaver_pipeline_segments_retried_total",
  "weaver_pipeline_segments_failed_permanent_total",
  "weaver_pipeline_articles_not_found_total",
  "weaver_pipeline_decode_errors_total",
  "weaver_pipeline_crc_errors_total",
  "weaver_pipeline_current_download_speed_bytes_per_second",
  "weaver_pipeline_download_queue_depth",
  "weaver_pipeline_decode_pending",
  "weaver_pipeline_commit_pending",
  "weaver_pipeline_recovery_queue_depth",
  "weaver_pipeline_verify_active",
  "weaver_pipeline_repair_active",
  "weaver_pipeline_extract_active",
  "weaver_pipeline_write_buffered_bytes",
  "weaver_pipeline_write_buffered_segments",
  "weaver_pipeline_disk_write_latency_microseconds",
  "weaver_pipeline_articles_per_second",
  "weaver_pipeline_decode_rate_mebibytes_per_second",
] as const;

export const HISTORY_METRIC_NAMES = [
  ...SNAPSHOT_HISTORY_METRIC_NAMES,
  JOB_STATUS_HISTORY_METRIC,
] as const satisfies readonly string[];

export type SnapshotHistoryMetricName = (typeof SNAPSHOT_HISTORY_METRIC_NAMES)[number];
export type HistoryMetricName = SnapshotHistoryMetricName | typeof JOB_STATUS_HISTORY_METRIC;

export const PROM_METRIC_TO_SNAPSHOT_FIELD: Record<
  SnapshotHistoryMetricName,
  keyof MetricsSnapshot
> = {
  weaver_pipeline_bytes_downloaded_total: "bytesDownloaded",
  weaver_pipeline_bytes_decoded_total: "bytesDecoded",
  weaver_pipeline_bytes_committed_total: "bytesCommitted",
  weaver_pipeline_segments_downloaded_total: "segmentsDownloaded",
  weaver_pipeline_segments_decoded_total: "segmentsDecoded",
  weaver_pipeline_segments_committed_total: "segmentsCommitted",
  weaver_pipeline_segments_retried_total: "segmentsRetried",
  weaver_pipeline_segments_failed_permanent_total: "segmentsFailedPermanent",
  weaver_pipeline_articles_not_found_total: "articlesNotFound",
  weaver_pipeline_decode_errors_total: "decodeErrors",
  weaver_pipeline_crc_errors_total: "crcErrors",
  weaver_pipeline_current_download_speed_bytes_per_second: "currentDownloadSpeed",
  weaver_pipeline_download_queue_depth: "downloadQueueDepth",
  weaver_pipeline_decode_pending: "decodePending",
  weaver_pipeline_commit_pending: "commitPending",
  weaver_pipeline_recovery_queue_depth: "recoveryQueueDepth",
  weaver_pipeline_verify_active: "verifyActive",
  weaver_pipeline_repair_active: "repairActive",
  weaver_pipeline_extract_active: "extractActive",
  weaver_pipeline_write_buffered_bytes: "writeBufferedBytes",
  weaver_pipeline_write_buffered_segments: "writeBufferedSegments",
  weaver_pipeline_disk_write_latency_microseconds: "diskWriteLatencyUs",
  weaver_pipeline_articles_per_second: "articlesPerSec",
  weaver_pipeline_decode_rate_mebibytes_per_second: "decodeRateMbps",
};

export type MetricValueFormat =
  | "bytes"
  | "bytesPerSecond"
  | "count"
  | "countPerSecond"
  | "latencyUs"
  | "mibPerSecond";

export type MetricScale = "left" | "right";
export type ChartColorToken =
  | "sky"
  | "teal"
  | "orange"
  | "violet"
  | "magenta"
  | "red"
  | "amber"
  | "indigo"
  | "green";

export interface MetricChartLineDefinition {
  metric: HistoryMetricName;
  labelKey: string;
  colorToken: ChartColorToken;
  mode: "raw" | "rate";
  format: MetricValueFormat;
  scale?: MetricScale;
}

export interface MetricChartDefinition {
  id: string;
  titleKey: string;
  descriptionKey: string;
  leftAxisFormat: MetricValueFormat;
  rightAxisFormat?: MetricValueFormat;
  lines: readonly MetricChartLineDefinition[];
}

export const METRIC_CHART_DEFINITIONS: readonly MetricChartDefinition[] = [
  {
    id: "download-throughput",
    titleKey: "metrics.downloadThroughputChart",
    descriptionKey: "metrics.downloadThroughputDesc",
    leftAxisFormat: "bytesPerSecond",
    lines: [
      {
        metric: "weaver_pipeline_bytes_downloaded_total",
        labelKey: "metrics.downloaded",
        colorToken: "sky",
        mode: "rate",
        format: "bytesPerSecond",
      },
      {
        metric: "weaver_pipeline_bytes_decoded_total",
        labelKey: "metrics.decoded",
        colorToken: "teal",
        mode: "rate",
        format: "bytesPerSecond",
      },
      {
        metric: "weaver_pipeline_bytes_committed_total",
        labelKey: "metrics.committed",
        colorToken: "orange",
        mode: "rate",
        format: "bytesPerSecond",
      },
    ],
  },
  {
    id: "segments",
    titleKey: "metrics.segmentsChart",
    descriptionKey: "metrics.segmentsDesc",
    leftAxisFormat: "countPerSecond",
    lines: [
      {
        metric: "weaver_pipeline_segments_downloaded_total",
        labelKey: "metrics.segmentsDownloaded",
        colorToken: "sky",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_segments_decoded_total",
        labelKey: "metrics.segmentsDecoded",
        colorToken: "teal",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_segments_committed_total",
        labelKey: "metrics.segmentsCommitted",
        colorToken: "orange",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_segments_retried_total",
        labelKey: "metrics.segmentsRetried",
        colorToken: "violet",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_segments_failed_permanent_total",
        labelKey: "metrics.failedPermanent",
        colorToken: "red",
        mode: "rate",
        format: "countPerSecond",
      },
    ],
  },
  {
    id: "errors",
    titleKey: "metrics.errorsChart",
    descriptionKey: "metrics.errorsDesc",
    leftAxisFormat: "countPerSecond",
    lines: [
      {
        metric: "weaver_pipeline_articles_not_found_total",
        labelKey: "metrics.articlesNotFound",
        colorToken: "amber",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_decode_errors_total",
        labelKey: "metrics.decodeErrors",
        colorToken: "red",
        mode: "rate",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_crc_errors_total",
        labelKey: "metrics.crcErrors",
        colorToken: "magenta",
        mode: "rate",
        format: "countPerSecond",
      },
    ],
  },
  {
    id: "download-speed",
    titleKey: "metrics.downloadSpeedChart",
    descriptionKey: "metrics.downloadSpeedDesc",
    leftAxisFormat: "bytesPerSecond",
    lines: [
      {
        metric: "weaver_pipeline_current_download_speed_bytes_per_second",
        labelKey: "metrics.downloadSpeed",
        colorToken: "sky",
        mode: "raw",
        format: "bytesPerSecond",
      },
    ],
  },
  {
    id: "queue-depths",
    titleKey: "metrics.queueDepthsChart",
    descriptionKey: "metrics.queueDepthsDesc",
    leftAxisFormat: "count",
    lines: [
      {
        metric: "weaver_pipeline_download_queue_depth",
        labelKey: "metrics.downloadQueue",
        colorToken: "sky",
        mode: "raw",
        format: "count",
      },
      {
        metric: "weaver_pipeline_decode_pending",
        labelKey: "metrics.decodePending",
        colorToken: "teal",
        mode: "raw",
        format: "count",
      },
      {
        metric: "weaver_pipeline_commit_pending",
        labelKey: "metrics.commitPending",
        colorToken: "orange",
        mode: "raw",
        format: "count",
      },
      {
        metric: "weaver_pipeline_recovery_queue_depth",
        labelKey: "metrics.recoveryQueue",
        colorToken: "violet",
        mode: "raw",
        format: "count",
      },
    ],
  },
  {
    id: "active-workers",
    titleKey: "metrics.activeWorkersChart",
    descriptionKey: "metrics.activeWorkersDesc",
    leftAxisFormat: "count",
    lines: [
      {
        metric: "weaver_pipeline_verify_active",
        labelKey: "metrics.verifyActive",
        colorToken: "teal",
        mode: "raw",
        format: "count",
      },
      {
        metric: "weaver_pipeline_repair_active",
        labelKey: "metrics.repairActive",
        colorToken: "amber",
        mode: "raw",
        format: "count",
      },
      {
        metric: "weaver_pipeline_extract_active",
        labelKey: "metrics.extractActive",
        colorToken: "indigo",
        mode: "raw",
        format: "count",
      },
    ],
  },
  {
    id: "write-buffer",
    titleKey: "metrics.writeBufferChart",
    descriptionKey: "metrics.writeBufferDesc",
    leftAxisFormat: "bytes",
    rightAxisFormat: "count",
    lines: [
      {
        metric: "weaver_pipeline_write_buffered_bytes",
        labelKey: "metrics.writeBufferedBytes",
        colorToken: "orange",
        mode: "raw",
        format: "bytes",
      },
      {
        metric: "weaver_pipeline_write_buffered_segments",
        labelKey: "metrics.writeBufferedSegments",
        colorToken: "violet",
        mode: "raw",
        format: "count",
        scale: "right",
      },
    ],
  },
  {
    id: "disk-write-latency",
    titleKey: "metrics.diskWriteLatencyChart",
    descriptionKey: "metrics.diskWriteLatencyDesc",
    leftAxisFormat: "latencyUs",
    lines: [
      {
        metric: "weaver_pipeline_disk_write_latency_microseconds",
        labelKey: "metrics.diskWriteLatency",
        colorToken: "red",
        mode: "raw",
        format: "latencyUs",
      },
    ],
  },
  {
    id: "throughput-rates",
    titleKey: "metrics.throughputRatesChart",
    descriptionKey: "metrics.throughputRatesDesc",
    leftAxisFormat: "countPerSecond",
    rightAxisFormat: "mibPerSecond",
    lines: [
      {
        metric: "weaver_pipeline_articles_per_second",
        labelKey: "metrics.articlesPerSec",
        colorToken: "sky",
        mode: "raw",
        format: "countPerSecond",
      },
      {
        metric: "weaver_pipeline_decode_rate_mebibytes_per_second",
        labelKey: "metrics.decodeRate",
        colorToken: "green",
        mode: "raw",
        format: "mibPerSecond",
        scale: "right",
      },
    ],
  },
];

const WHOLE_NUMBER_FORMAT = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 0,
});

const ONE_DECIMAL_FORMAT = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 0,
  maximumFractionDigits: 1,
});

const TWO_DECIMAL_FORMAT = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

function clampMetricValue(value: number): number {
  return Number.isFinite(value) ? value : 0;
}

function formatRoundedNumber(value: number, kind: "whole" | "one" | "two"): string {
  const safeValue = clampMetricValue(value);
  if (kind === "whole") {
    return WHOLE_NUMBER_FORMAT.format(safeValue);
  }
  if (kind === "one") {
    return ONE_DECIMAL_FORMAT.format(safeValue);
  }
  return TWO_DECIMAL_FORMAT.format(safeValue);
}

export function formatLatency(microseconds: number): string {
  const safeValue = clampMetricValue(microseconds);
  if (safeValue >= 1000) {
    return `${formatRoundedNumber(safeValue / 1000, safeValue >= 10_000 ? "whole" : "one")} ms`;
  }
  return `${formatRoundedNumber(safeValue, safeValue >= 100 ? "whole" : "one")} us`;
}

export function formatMetricValue(format: MetricValueFormat, value: number): string {
  const safeValue = clampMetricValue(value);
  switch (format) {
    case "bytes":
      return formatBytes(safeValue);
    case "bytesPerSecond":
      return `${formatBytes(safeValue)}/s`;
    case "count":
      return formatRoundedNumber(safeValue, safeValue >= 100 ? "whole" : "one");
    case "countPerSecond": {
      const precision = safeValue >= 100 ? "whole" : safeValue >= 10 ? "one" : "two";
      return `${formatRoundedNumber(safeValue, precision)}/s`;
    }
    case "latencyUs":
      return formatLatency(safeValue);
    case "mibPerSecond":
      return `${formatRoundedNumber(safeValue, safeValue >= 100 ? "whole" : "one")} MiB/s`;
  }
}

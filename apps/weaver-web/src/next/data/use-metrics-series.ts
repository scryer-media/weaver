import { useEffect, useMemo, useState } from "react";
import { useQuery, useSubscription } from "urql";
import {
  METRICS_HISTORY_QUERY,
  METRICS_PAGE_QUERY,
  METRICS_PAGE_SUBSCRIPTION,
} from "@/graphql/queries";
import { useGraphqlConnectionState } from "@/graphql/client";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { isRollupMetricsRange, type MetricsHistoryRange, type MetricsSnapshot } from "@/lib/metrics";
import type { DownloadBlockState } from "@/lib/context/live-data-context";

/**
 * Time series and the live snapshot behind the Monitoring screen.
 *
 * `metricsHistory` returns cumulative counters and instantaneous gauges at a
 * fixed resolution; the charts want per-second rates for the former and the
 * raw value for the latter, so the conversion happens once here and the page
 * only picks metric names.
 */

interface MetricsHistoryResponse {
  metricsHistory: {
    timestamps: number[];
    resolutionSec: number;
    series: {
      metric: string;
      variant: "ACTUAL" | "AVG" | "PEAK";
      labels: { key: string; value: string }[];
      values: number[];
    }[];
  };
}

export interface MetricsSeries {
  timestamps: number[];
  /** Per-second rate for counters, keyed by Prometheus metric name. */
  rate: (metric: string) => number[];
  /** Raw gauge samples, keyed by Prometheus metric name. */
  gauge: (metric: string) => number[];
  isLoading: boolean;
  error: string | null;
}

const EMPTY: number[] = [];

function toRates(timestamps: number[], values: number[]): number[] {
  const rates = values.map(() => 0);
  for (let index = 1; index < values.length; index += 1) {
    const elapsedSec = (timestamps[index]! - timestamps[index - 1]!) / 1000;
    if (elapsedSec <= 0) {
      continue;
    }
    const delta = values[index]! - values[index - 1]!;
    // Counters reset when the daemon restarts; a negative delta is a restart,
    // not a negative rate.
    rates[index] = delta < 0 ? 0 : delta / elapsedSec;
  }
  return rates;
}

export function useMetricsSeries(range: MetricsHistoryRange): MetricsSeries {
  const [{ data, error, fetching }] = useQuery<MetricsHistoryResponse>({
    query: METRICS_HISTORY_QUERY,
    variables: { range },
  });

  return useMemo(() => {
    const history = data?.metricsHistory;
    const timestamps = history?.timestamps ?? EMPTY;
    // Long ranges are served pre-rolled-up; short ones carry raw samples.
    const preferred = isRollupMetricsRange(range) ? "AVG" : "ACTUAL";

    const lookup = (metric: string): number[] => {
      const series = history?.series ?? [];
      const match =
        series.find((entry) => entry.metric === metric && entry.variant === preferred)
        ?? series.find((entry) => entry.metric === metric);
      if (!match) {
        return EMPTY;
      }
      // A series can lag the timestamp axis by a sample; pad rather than draw
      // a line that ends early against the other series in the same plot.
      const values = match.values ?? EMPTY;
      if (values.length >= timestamps.length) {
        return values.slice(0, timestamps.length);
      }
      return [...values, ...Array.from({ length: timestamps.length - values.length }, () => 0)];
    };

    return {
      timestamps,
      rate: (metric: string) => toRates(timestamps, lookup(metric)),
      gauge: lookup,
      isLoading: fetching && !history,
      error: error?.message ?? null,
    };
  }, [data, error, fetching, range]);
}

interface MetricsPageResponse {
  metrics: MetricsSnapshot;
  globalState: { isPaused: boolean; downloadBlock: DownloadBlockState };
}

/** The full live metrics snapshot — the stat strip reads every field of it. */
export function useMetricsSnapshot(): MetricsSnapshot | null {
  const connection = useGraphqlConnectionState();
  const [{ data: queryData }, reexecute] = useQuery<MetricsPageResponse>({
    query: METRICS_PAGE_QUERY,
  });
  const [{ data: subscriptionData, error: subscriptionError }] = useSubscription<{
    systemMetricsUpdates: MetricsPageResponse;
  }>({ query: METRICS_PAGE_SUBSCRIPTION });

  const [polled, setPolled] = useState<MetricsPageResponse>();
  useReconnectPolling<MetricsPageResponse>({
    enabled: connection.status === "disconnected",
    query: METRICS_PAGE_QUERY,
    onData: setPolled,
  });

  useEffect(() => {
    if (connection.status === "connected" && subscriptionData?.systemMetricsUpdates) {
      setPolled(undefined);
    }
  }, [connection.status, subscriptionData]);

  useEffect(() => {
    if (subscriptionError) {
      void reexecute({ requestPolicy: "network-only" });
    }
  }, [reexecute, subscriptionError]);

  const snapshot =
    polled
    ?? (connection.status === "connected" && !subscriptionError
      ? subscriptionData?.systemMetricsUpdates
      : undefined)
    ?? queryData;

  return snapshot?.metrics ?? null;
}

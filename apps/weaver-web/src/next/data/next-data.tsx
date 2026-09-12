import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useQuery, useSubscription } from "urql";
import { useGraphqlConnectionState, type GraphqlConnectionStatus } from "@/graphql/client";
import {
  CATEGORIES_QUERY,
  HISTORY_JOBS_COUNT_QUERY,
  LIVE_METRICS_QUERY,
  LIVE_METRICS_SUBSCRIPTION,
  SERVER_HEALTH_QUERY,
  VERSION_QUERY,
} from "@/graphql/queries";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import type { DownloadBlockState } from "@/lib/context/live-data-context";
import { useLiveQueue, type LiveQueue } from "./use-live-queue";

/**
 * Everything the Next chrome needs, resolved once above the router.
 *
 * The rail carries live values on every screen — download and history counts,
 * throughput, provider load, attention items — so these queries belong to the
 * shell rather than to whichever page happens to be mounted. Pages read the
 * same context instead of opening a second subscription for the same data.
 */

export interface ProviderHealth {
  host: string;
  port: number;
  label: string;
  /** `PRIMARY` for the first configured server, `BACKUP` for the rest. */
  tier: string;
  state: string;
  connectionsActive: number;
  connectionsMax: number;
  connectionsConfigured: number;
  capacityPenaltyUntilEpochMs: number | null;
  latencyMs: number;
  bodyLatencyMs: number | null;
  bodyLatencyBand: string | null;
  successCount: number;
  failureCount: number;
  consecutiveFailures: number;
  prematureDeaths: number;
}

interface ProviderHoldoff {
  label: string;
  untilEpochMs: number;
}

interface LiveMetricsSnapshot {
  metrics: { currentDownloadSpeed: number };
  globalState: { isPaused: boolean; downloadBlock: DownloadBlockState };
  providerHoldoffs?: ProviderHoldoff[];
}

/** A category as it is configured, not as the queue happens to use it. */
export interface ConfiguredCategory {
  id: number;
  name: string;
  destDir: string | null;
}

export interface NextData {
  version: string;
  speed: number;
  /** Highest speed seen since the tab opened; the rail and stat strip both note it. */
  peakSpeed: number;
  isPaused: boolean;
  downloadBlock: DownloadBlockState;
  queue: LiveQueue;
  /**
   * The categories a person configured, in the order the daemon returns them.
   *
   * Deliberately not `queue.categories`, which is the set of categories the
   * jobs in the queue happen to carry: a configured category with nothing in
   * it right now is still a category you can filter by, and an empty rail is
   * not the same statement as "you have no categories".
   */
  categories: ConfiguredCategory[];
  historyCount: number;
  providers: ProviderHealth[];
  holdoffs: ProviderHoldoff[];
  connection: { status: GraphqlConnectionStatus; isDisconnected: boolean; isPolling: boolean };
}

const DEFAULT_DOWNLOAD_BLOCK: DownloadBlockState = {
  kind: "NONE",
  capEnabled: false,
  period: null,
  usedBytes: 0,
  limitBytes: 0,
  remainingBytes: 0,
  reservedBytes: 0,
  windowStartsAtEpochMs: null,
  windowEndsAtEpochMs: null,
  timezoneName: "",
  scheduledSpeedLimit: 0,
};

const EMPTY_CATEGORIES: ConfiguredCategory[] = [];
const EMPTY_PROVIDERS: ProviderHealth[] = [];
const EMPTY_HOLDOFFS: ProviderHoldoff[] = [];

const NextDataContext = createContext<NextData | null>(null);

export function useNextData(): NextData {
  const value = useContext(NextDataContext);
  if (!value) {
    throw new Error("useNextData must be used inside NextDataProvider");
  }
  return value;
}

export function NextDataProvider({ children }: { children: ReactNode }) {
  const connectionState = useGraphqlConnectionState();
  const queue = useLiveQueue();

  const [{ data: versionData }] = useQuery<{ version: string }>({ query: VERSION_QUERY });
  const [{ data: historyCountData }, reexecuteHistoryCount] = useQuery<{ all: number }>({
    query: HISTORY_JOBS_COUNT_QUERY,
  });
  const [{ data: categoryData }] = useQuery<{ categories: ConfiguredCategory[] }>({
    query: CATEGORIES_QUERY,
  });
  const [{ data: providerData }, reexecuteProviders] = useQuery<{
    serverHealth: ProviderHealth[];
  }>({ query: SERVER_HEALTH_QUERY });

  const [{ data: metricsQueryData }, reexecuteMetrics] = useQuery<LiveMetricsSnapshot>({
    query: LIVE_METRICS_QUERY,
  });
  const [{ data: metricsSubscriptionData, error: metricsSubscriptionError }] = useSubscription<{
    systemMetricsUpdates: LiveMetricsSnapshot;
  }>({ query: LIVE_METRICS_SUBSCRIPTION });

  const [polledMetrics, setPolledMetrics] = useState<LiveMetricsSnapshot | undefined>();
  const reconnectPolling = useReconnectPolling<LiveMetricsSnapshot>({
    enabled: connectionState.status === "disconnected",
    query: LIVE_METRICS_QUERY,
    onData: setPolledMetrics,
  });

  useEffect(() => {
    if (connectionState.status === "connected" && metricsSubscriptionData?.systemMetricsUpdates) {
      setPolledMetrics(undefined);
    }
  }, [connectionState.status, metricsSubscriptionData]);

  // Server health has no subscription; the queue's own refresh cadence is the
  // right beat for it, so re-read it whenever the queue's shape changes.
  const queueShape = `${queue.summary.totalItems}:${queue.summary.activeItems}`;
  useEffect(() => {
    void reexecuteProviders({ requestPolicy: "network-only" });
    void reexecuteHistoryCount({ requestPolicy: "network-only" });
  }, [queueShape, reexecuteHistoryCount, reexecuteProviders]);

  // Providers drift on their own (latency, holdoffs) even with a still queue.
  useEffect(() => {
    const timer = window.setInterval(() => {
      void reexecuteProviders({ requestPolicy: "network-only" });
    }, 10_000);
    return () => window.clearInterval(timer);
  }, [reexecuteProviders]);

  const snapshot = polledMetrics
    ?? (connectionState.status === "connected" && !metricsSubscriptionError
      ? metricsSubscriptionData?.systemMetricsUpdates
      : undefined)
    ?? metricsQueryData;

  useEffect(() => {
    if (!metricsSubscriptionError) {
      return;
    }
    void reexecuteMetrics({ requestPolicy: "network-only" });
  }, [metricsSubscriptionError, reexecuteMetrics]);

  const speed = snapshot?.metrics?.currentDownloadSpeed ?? 0;
  const peakSpeedRef = useRef(0);
  if (speed > peakSpeedRef.current) {
    peakSpeedRef.current = speed;
  }

  const globalState = snapshot?.globalState;
  const downloadBlock = globalState?.downloadBlock ?? DEFAULT_DOWNLOAD_BLOCK;
  const isPaused = globalState?.isPaused ?? false;
  const categories = categoryData?.categories ?? EMPTY_CATEGORIES;
  const providers = providerData?.serverHealth ?? EMPTY_PROVIDERS;
  const holdoffs = snapshot?.providerHoldoffs ?? EMPTY_HOLDOFFS;
  const version = versionData?.version ?? "";
  const historyCount = historyCountData?.all ?? 0;
  const isPolling = reconnectPolling.isPolling;

  const value = useMemo<NextData>(
    () => ({
      version,
      speed,
      peakSpeed: peakSpeedRef.current,
      isPaused,
      downloadBlock,
      queue,
      categories,
      historyCount,
      providers,
      holdoffs,
      connection: {
        status: connectionState.status,
        isDisconnected: connectionState.status === "disconnected",
        isPolling,
      },
    }),
    [
      categories,
      connectionState.status,
      downloadBlock,
      historyCount,
      holdoffs,
      isPaused,
      isPolling,
      providers,
      queue,
      speed,
      version,
    ],
  );

  return <NextDataContext.Provider value={value}>{children}</NextDataContext.Provider>;
}

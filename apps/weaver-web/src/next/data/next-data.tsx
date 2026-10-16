// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  use,
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
  UPDATE_STATUS_QUERY,
  UPDATE_STATUS_SUBSCRIPTION,
  VERSION_QUERY,
} from "@/graphql/queries";
import { releaseNotification, type UpdateStatus } from "@/next/features/updates/update-notification";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { useTranslate } from "@/lib/context/translate-context";
import {
  LiveJobDownloadRatesProvider,
  type DownloadBlockState,
} from "@/lib/context/live-data-context";
import type { JobDownloadRate } from "@/lib/live-job-download-rates";
import { formatRate } from "./format";
import { sameItems, shallowEqual, useSettled } from "./settled";
import { withLiveConnections, type ProviderConnections } from "./provider-connections";
import { useHistoryLiveRefresh } from "./use-history-live-refresh";
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
  /**
   * What the pool is doing with this server right now: `downloading`, `idle`,
   * `preparing`, `over_limit`, `cooling_down`, `degraded` or `disabled`. The
   * rail leads with this rather than with a connection fraction, which reads
   * as a fault whenever the daemon is deliberately holding back.
   */
  activity: string;
  /** When the current activity is expected to end, when the daemon knows. */
  activityUntilEpochMs: number | null;
  connectionsOpen: number;
  connectionsBusy: number;
  connectionsActive: number;
  connectionsMax: number;
  connectionsConfigured: number;
  capacityPenaltyUntilEpochMs: number | null;
  latencyMs: number;
  bodyLatencyMs: number | null;
  bodyTransferMs: number | null;
  bodyLatencyBand: string | null;
  /** How many BODYs the lanes keep in flight on this server; 1 is sequential. */
  bodyPipelineDepth: number;
  /** The lanes proved pipelining does not pay here, whatever the depth says. */
  bodyPipeliningPinnedSequential: boolean;
  successCount: number;
  failureCount: number;
  consecutiveFailures: number;
  prematureDeaths: number;
  /** The resolved address new connections dial, once a race has picked one. */
  pinnedAddress: string | null;
}

interface ProviderHoldoff {
  label: string;
  untilEpochMs: number;
}

interface LiveMetricsSnapshot {
  metrics: { currentDownloadSpeed: number };
  globalState: { isPaused: boolean; speedLimitBytesPerSec: number; downloadBlock: DownloadBlockState };
  providerHoldoffs?: ProviderHoldoff[];
  providerConnections?: ProviderConnections[];
  /** Per-job download rates on the same tick as the speed; only the push carries them. */
  jobDownloadRates?: JobDownloadRate[];
}

/** A category as it is configured, not as the queue happens to use it. */
export interface ConfiguredCategory {
  id: number;
  name: string;
  destDir: string | null;
}

export interface NextData {
  version: string;
  /** A newer release to advertise, once one has both a version and a page to open. */
  update: { version: string; url: string } | undefined;
  speed: number;
  /** Highest speed seen since the tab opened; the rail and stat strip both note it. */
  peakSpeed: number;
  isPaused: boolean;
  /** The configured download ceiling in bytes per second; 0 is unlimited. */
  speedLimit: number;
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
  /** Re-read the categories after a change made elsewhere in the interface. */
  refreshCategories: () => void;
  historyCount: number;
  /** Re-read the history count after a change made elsewhere in the interface. */
  refreshHistoryCount: () => void;
  providers: ProviderHealth[];
  /**
   * Whether `providers` is an answer. It is also empty before server health
   * first returns and while a read is failing, and neither of those says
   * that nothing is configured.
   */
  providersLoaded: boolean;
  holdoffs: ProviderHoldoff[];
  connection: { status: GraphqlConnectionStatus; isDisconnected: boolean; isPolling: boolean };
}

const DEFAULT_DOWNLOAD_BLOCK: DownloadBlockState = {
  kind: "NONE",
  egressId: null,
  egressName: null,
  usedBytes: 0,
  limitBytes: 0,
  remainingBytes: 0,
  windowStartsAtEpochMs: null,
  windowEndsAtEpochMs: null,
  timezoneName: "",
  scheduledSpeedLimit: 0,
};

const EMPTY_CATEGORIES: ConfiguredCategory[] = [];
const EMPTY_PROVIDERS: ProviderHealth[] = [];
const EMPTY_HOLDOFFS: ProviderHoldoff[] = [];

/**
 * The whole value, new on every render of the provider, so its reader renders
 * in the provider's own pass each time: the queue's time-left estimate samples
 * on every render it gets and settles with the time between them. Exported so
 * a fixture can hand a screen the data it reads without the live streams.
 */
export const NextDataContext = createContext<NextData | null>(null);

/**
 * The provider's value, read a slice at a time.
 *
 * The speed moves several times a second and almost nothing else does, but a
 * context renders every reader again whenever its value changes: the rail, the
 * page and every panel under it redrew on each speed tick for a figure most of
 * them never show. The provider publishes here instead, and a reader renders
 * again only when what it asked for changes.
 */
interface NextDataStore {
  get: () => NextData;
  set: (data: NextData) => void;
  subscribe: (listener: () => void) => () => void;
}

function createNextDataStore(initial: NextData): NextDataStore {
  let data = initial;
  const listeners = new Set<() => void>();
  return {
    get: () => data,
    set: (next) => {
      if (next === data) {
        return;
      }
      data = next;
      for (const listener of listeners) {
        listener();
      }
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const NextDataStoreContext = createContext<NextDataStore | null>(null);
const subscribeNowhere = () => () => {};

/**
 * The whole value, or only the named keys. Named, the caller renders again only
 * when one of them changes, not on every speed tick.
 */
export function useNextData(): NextData;
export function useNextData<K extends keyof NextData>(...keys: K[]): Pick<NextData, K>;
export function useNextData<K extends keyof NextData>(...keys: K[]): NextData | Pick<NextData, K> {
  const whole = keys.length === 0;
  const data = whole ? use(NextDataContext) : null;
  const picked = useNextDataSelector<Pick<NextData, K> | null>(
    (next) => (whole ? null : pick(next, keys)),
    samePick,
  );
  if (!whole) {
    return picked!;
  }
  if (!data) {
    throw new Error("useNextData must be used inside NextDataProvider");
  }
  return data;
}

function samePick<T extends object>(left: T | null, right: T | null): boolean {
  return left === right || (left !== null && right !== null && shallowEqual(left, right));
}

/**
 * One figure worked out from the data, such as a count; the caller renders
 * again only when `isEqual` says it changed.
 */
export function useNextDataSelector<T>(
  select: (data: NextData) => T,
  isEqual: (left: T, right: T) => boolean = Object.is,
): T {
  const store = useContext(NextDataStoreContext);
  // Read only without a provider: under one, this context changes on every
  // render of it, and a slice reader is not to render with it.
  const fixture = store === null ? use(NextDataContext) : null;
  // The last answer, handed back while a new one is equal to it, so a reader
  // whose slice did not change sees the same value and does not render.
  const last = useRef<{ value: T } | null>(null);
  const getSnapshot = () => {
    const data = store?.get() ?? fixture;
    if (!data) {
      throw new Error("useNextData must be used inside NextDataProvider");
    }
    const value = select(data);
    if (last.current !== null && isEqual(last.current.value, value)) {
      return last.current.value;
    }
    last.current = { value };
    return value;
  };
  return useSyncExternalStore(store?.subscribe ?? subscribeNowhere, getSnapshot);
}

function pick<K extends keyof NextData>(data: NextData, keys: readonly K[]): Pick<NextData, K> {
  const picked = {} as Pick<NextData, K>;
  for (const key of keys) {
    picked[key] = data[key];
  }
  return picked;
}

export function NextDataProvider({ children }: { children: ReactNode }) {
  const connectionState = useGraphqlConnectionState();
  const queue = useLiveQueue();

  const [{ data: versionData }] = useQuery<{ version: string }>({ query: VERSION_QUERY });
  const [{ data: updateStatusData }] = useQuery<{ updateStatus: UpdateStatus }>({
    query: UPDATE_STATUS_QUERY,
  });
  const [{ data: updateStatusLive }] = useSubscription<{ updateStatusUpdates: UpdateStatus }>({
    query: UPDATE_STATUS_SUBSCRIPTION,
    pause: connectionState.status === "disconnected",
  });
  const [{ data: historyCountData }, reexecuteHistoryCount] = useQuery<{ all: number }>({
    query: HISTORY_JOBS_COUNT_QUERY,
  });
  const [{ data: categoryData }, reexecuteCategories] = useQuery<{ categories: ConfiguredCategory[] }>({
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

  // The history count follows history itself: a job reaching an outcome or a
  // row leaving it is a reason to count again, whichever screen is open.
  // Removals are counted even while a delete runs, so the rail falls with it.
  const refreshHistoryCount = useCallback(
    () => reexecuteHistoryCount({ requestPolicy: "network-only" }),
    [reexecuteHistoryCount],
  );
  useHistoryLiveRefresh({ refresh: refreshHistoryCount, deletesActive: false });

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
  // Raised while rendering, so the published peak never trails the speed.
  const [peakSpeed, setPeakSpeed] = useState(0);
  if (speed > peakSpeed) {
    setPeakSpeed(speed);
  }

  const globalState = snapshot?.globalState;
  const downloadBlock = useSettled(globalState?.downloadBlock ?? DEFAULT_DOWNLOAD_BLOCK, shallowEqual);
  const isPaused = globalState?.isPaused ?? false;
  const speedLimit = globalState?.speedLimitBytesPerSec ?? 0;
  const categories = categoryData?.categories ?? EMPTY_CATEGORIES;
  const refreshCategories = useCallback(
    () => reexecuteCategories({ requestPolicy: "network-only" }),
    [reexecuteCategories],
  );
  // Health is re-read on a slow beat; the counts on the meters follow the
  // metrics stream instead, so they move as the pool opens connections.
  const liveConnections = snapshot?.providerConnections;
  const providers = useSettled(
    useMemo(
      () => withLiveConnections(providerData?.serverHealth ?? EMPTY_PROVIDERS, liveConnections),
      [providerData, liveConnections],
    ),
    sameItems,
  );
  const providersLoaded = providerData !== undefined;
  const holdoffs = useSettled(snapshot?.providerHoldoffs ?? EMPTY_HOLDOFFS, sameItems);
  const version = versionData?.version ?? "";
  const update = useMemo(
    () =>
      releaseNotification(
        updateStatusLive?.updateStatusUpdates ?? updateStatusData?.updateStatus,
      ),
    [updateStatusData?.updateStatus, updateStatusLive?.updateStatusUpdates],
  );

  useDocumentTitle(speed, isPaused);
  const historyCount = historyCountData?.all ?? 0;
  const isPolling = reconnectPolling.isPolling;
  const connection = useMemo(
    () => ({
      status: connectionState.status,
      isDisconnected: connectionState.status === "disconnected",
      isPolling,
    }),
    [connectionState.status, isPolling],
  );

  const value = useMemo<NextData>(
    () => ({
      version,
      update,
      speed,
      peakSpeed,
      isPaused,
      speedLimit,
      downloadBlock,
      queue,
      categories,
      refreshCategories,
      historyCount,
      refreshHistoryCount,
      providers,
      providersLoaded,
      holdoffs,
      connection,
    }),
    [
      categories,
      connection,
      downloadBlock,
      historyCount,
      holdoffs,
      isPaused,
      peakSpeed,
      providers,
      providersLoaded,
      queue,
      refreshCategories,
      refreshHistoryCount,
      speed,
      speedLimit,
      update,
      version,
    ],
  );

  const [store] = useState(() => createNextDataStore(value));
  useLayoutEffect(() => {
    store.set(value);
  }, [store, value]);
  const live = useFreshCopy(value);

  return (
    <NextDataStoreContext.Provider value={store}>
      <NextDataContext value={live}>
        <LiveJobDownloadRatesProvider rates={snapshot?.jobDownloadRates}>
          {children}
        </LiveJobDownloadRatesProvider>
      </NextDataContext>
    </NextDataStoreContext.Provider>
  );
}

/** A copy of `value` made on every render of its caller. */
function useFreshCopy<T extends object>(value: T): T {
  // A new one on every call is the point: compiled, it would be made once.
  "use no memo";
  return { ...value };
}

/** How long the tab title holds one speed before it takes the next. */
const TITLE_HOLD_MS = 2500;

/**
 * The browser tab says what weaver is doing: the download speed while it
 * fetches, "Paused" while everything is held. A speed that moves every second
 * would make the tab flicker, so a new figure waits out the last one; a change
 * of state — pausing, going idle — shows at once.
 */
function useDocumentTitle(speed: number, isPaused: boolean) {
  const t = useTranslate();
  const lastUpdate = useRef(0);
  useEffect(() => {
    const title = isPaused
      ? t("next.title.paused")
      : speed > 0
        ? t("next.title.speed", { speed: formatRate(speed) })
        : "Weaver";
    const apply = () => {
      lastUpdate.current = Date.now();
      document.title = title;
    };
    // A held figure is applied once the hold runs out rather than dropped, or a
    // speed that then stays put would never reach the tab.
    const wait = speed > 0 && !isPaused ? lastUpdate.current + TITLE_HOLD_MS - Date.now() : 0;
    if (wait <= 0) {
      apply();
      return;
    }
    const timer = window.setTimeout(apply, wait);
    return () => window.clearTimeout(timer);
  }, [isPaused, speed, t]);

  useEffect(
    () => () => {
      document.title = "Weaver";
    },
    [],
  );
}

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useClient, useQuery } from "urql";
import { QUEUE_EVENTS_SUBSCRIPTION, QUEUE_PAGE_QUERY } from "@/graphql/queries";
import { useGraphqlConnectionState } from "@/graphql/client";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";

/**
 * The live queue, as the Next UI needs it.
 *
 * The redesign shows one grouped, unpaginated list and composes category, tab
 * and search filters client-side, so this fetches the whole queue in a single
 * page and lets the screen slice it. `queuePage` computes `summary` and
 * `categories` from every job *before* applying the input filter, which is what
 * makes the rail counts and tab counts correct on screens that are showing a
 * filtered list — and correct on screens that never render the list at all.
 *
 * Live updates follow the same contract as the classic queue table: apply the
 * per-item payload from `queueEvents` immediately so a row's progress moves at
 * event rate, and refetch the page on a throttle so membership and ordering
 * settle. Both are needed — the event stream never says where a new row sorts.
 */

const QUEUE_PAGE_SIZE = 500;
const QUEUE_EVENT_REFRESH_INTERVAL_MS = 2_000;

const EMPTY_ITEMS: GraphqlJobData[] = [];
const EMPTY_CATEGORIES: string[] = [];

export interface QueueSummary {
  totalItems: number;
  queuedItems: number;
  activeItems: number;
  pausedItems: number;
}

const EMPTY_SUMMARY: QueueSummary = {
  totalItems: 0,
  queuedItems: 0,
  activeItems: 0,
  pausedItems: 0,
};

interface QueuePageResponse {
  queuePage: {
    items: GraphqlJobData[];
    totalCount: number;
    summary: QueueSummary;
    categories: string[];
    latestCursor: string;
  };
}

type QueuePageData = QueuePageResponse["queuePage"];

interface QueueEventPayload {
  cursor: string;
  kind:
    | "ITEM_CREATED"
    | "ITEM_STATE_CHANGED"
    | "ITEM_PROGRESS"
    | "ITEM_ATTENTION"
    | "ITEM_COMPLETED"
    | "ITEM_REMOVED"
    | "GLOBAL_STATE_CHANGED";
  itemId: number | null;
  item: GraphqlJobData | null;
}

export interface LiveQueue {
  jobs: JobData[];
  summary: QueueSummary;
  categories: string[];
  /** Server-side total; larger than `jobs.length` once the queue exceeds a page. */
  totalCount: number;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

/** `queueEvents` cursors are base64 `evt:<sequence>`; decoding one orders overlays. */
function decodeQueueEventCursor(cursor: string): bigint | null {
  try {
    const base64 = cursor.replace(/-/g, "+").replace(/_/g, "/");
    const padded = `${base64}${"=".repeat((4 - (base64.length % 4)) % 4)}`;
    const decoded = atob(padded);
    if (!decoded.startsWith("evt:")) {
      return null;
    }
    return BigInt(decoded.slice(4));
  } catch {
    return null;
  }
}

const QUEUE_PAGE_VARIABLES = {
  input: { pageIndex: 0, pageSize: QUEUE_PAGE_SIZE },
} as const;

export function useLiveQueue(): LiveQueue {
  const client = useClient();
  const connection = useGraphqlConnectionState();
  const [{ data, error, fetching }, reexecuteQuery] = useQuery<QueuePageResponse>({
    query: QUEUE_PAGE_QUERY,
    variables: QUEUE_PAGE_VARIABLES,
  });

  // While the socket is down the subscription cannot deliver anything, so the
  // page falls back to polling and renders whatever that last returned.
  const [polledPage, setPolledPage] = useState<QueuePageData>();
  useReconnectPolling<QueuePageResponse>({
    enabled: connection.status === "disconnected",
    query: QUEUE_PAGE_QUERY,
    variables: QUEUE_PAGE_VARIABLES,
    onData: (payload) => setPolledPage(payload.queuePage),
  });

  const page = connection.status === "disconnected" && polledPage
    ? polledPage
    : data?.queuePage;

  const [overlays, setOverlays] = useState<Record<number, { item: GraphqlJobData; cursor: bigint }>>(
    {},
  );
  const [removedIds, setRemovedIds] = useState<ReadonlySet<number>>(() => new Set());
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastRefreshAtRef = useRef(0);
  const lastEventSequenceRef = useRef<bigint | null>(null);
  const lastConnectedAtRef = useRef<number | null | undefined>(undefined);

  const refreshNow = useCallback(() => {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
    lastRefreshAtRef.current = Date.now();
    void reexecuteQuery({ requestPolicy: "network-only" });
  }, [reexecuteQuery]);

  const scheduleRefresh = useCallback(() => {
    if (refreshTimerRef.current) {
      return;
    }
    const elapsed = Date.now() - lastRefreshAtRef.current;
    refreshTimerRef.current = setTimeout(
      () => {
        refreshTimerRef.current = null;
        lastRefreshAtRef.current = Date.now();
        void reexecuteQuery({ requestPolicy: "network-only" });
      },
      Math.max(0, QUEUE_EVENT_REFRESH_INTERVAL_MS - elapsed),
    );
  }, [reexecuteQuery]);

  // A reconnect means the event sequence restarted somewhere unknowable; drop
  // every overlay and re-read the page rather than replaying against stale state.
  useEffect(() => {
    if (connection.status !== "connected" || connection.lastConnectedAt === null) {
      return;
    }
    if (lastConnectedAtRef.current === undefined) {
      lastConnectedAtRef.current = connection.lastConnectedAt;
      return;
    }
    if (lastConnectedAtRef.current === connection.lastConnectedAt) {
      return;
    }
    lastConnectedAtRef.current = connection.lastConnectedAt;
    lastEventSequenceRef.current = null;
    setOverlays({});
    setRemovedIds(new Set());
    setPolledPage(undefined);
    refreshNow();
  }, [connection.lastConnectedAt, connection.status, refreshNow]);

  const latestCursor = page?.latestCursor;
  const visibleIds = useMemo(
    () => new Set((page?.items ?? EMPTY_ITEMS).map((item) => item.id)),
    [page?.items],
  );

  useEffect(() => {
    if (!latestCursor) {
      return;
    }
    // Subscribe through the client rather than `useSubscription`: a burst of
    // events must all be consumed, and a hook that only exposes the latest
    // result drops every job but one out of each React batch.
    const subscription = client
      .subscription<{ queueEvents: QueueEventPayload }>(QUEUE_EVENTS_SUBSCRIPTION, {
        after: latestCursor,
      })
      .subscribe(({ data: payload }) => {
        const event = payload?.queueEvents;
        if (!event) {
          return;
        }
        const sequence = decodeQueueEventCursor(event.cursor);
        if (sequence === null) {
          scheduleRefresh();
          return;
        }
        if (lastEventSequenceRef.current !== null && sequence <= lastEventSequenceRef.current) {
          return;
        }
        lastEventSequenceRef.current = sequence;

        if (event.kind === "ITEM_REMOVED" && event.itemId != null) {
          const removedId = event.itemId;
          setRemovedIds((current) => {
            if (current.has(removedId)) {
              return current;
            }
            const next = new Set(current);
            next.add(removedId);
            return next;
          });
          scheduleRefresh();
          return;
        }

        if (event.item && visibleIds.has(event.item.id)) {
          const item = event.item;
          setOverlays((current) => {
            const existing = current[item.id];
            if (existing && existing.cursor >= sequence) {
              return current;
            }
            return { ...current, [item.id]: { item, cursor: sequence } };
          });
          // Progress on a row already on screen is fully described by the
          // payload; only membership and ordering need the page back.
          if (event.kind === "ITEM_PROGRESS") {
            return;
          }
        }

        scheduleRefresh();
      });
    return () => subscription.unsubscribe();
  }, [client, latestCursor, scheduleRefresh, visibleIds]);

  useEffect(
    () => () => {
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
      }
    },
    [],
  );

  // Once a refetch lands without the removed rows, stop carrying the tombstones.
  const pageItems = page?.items ?? EMPTY_ITEMS;
  useEffect(() => {
    setRemovedIds((current) => {
      if (current.size === 0) {
        return current;
      }
      const stillPresent = new Set<number>();
      for (const item of pageItems) {
        if (current.has(item.id)) {
          stillPresent.add(item.id);
        }
      }
      return stillPresent.size === current.size ? current : stillPresent;
    });
  }, [pageItems]);

  const jobs = useMemo(
    () =>
      pageItems
        .filter((item) => !removedIds.has(item.id))
        .map((item) => normalizeJobData(overlays[item.id]?.item ?? item)),
    [overlays, pageItems, removedIds],
  );

  return {
    jobs,
    summary: page?.summary ?? EMPTY_SUMMARY,
    categories: page?.categories ?? EMPTY_CATEGORIES,
    totalCount: page?.totalCount ?? 0,
    isLoading: fetching && page === undefined,
    error: error?.message ?? null,
    refresh: refreshNow,
  };
}

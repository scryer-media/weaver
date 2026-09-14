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
 * and search filters client-side, so this fetches the whole queue and lets the
 * screen slice it. The first page is the live query; a queue longer than one
 * page reads the remaining pages behind it after every refetch. `queuePage` computes `summary` and
 * `categories` from every job *before* applying the input filter, which is what
 * makes the rail counts and tab counts correct on screens that are showing a
 * filtered list — and correct on screens that never render the list at all.
 *
 * Live updates follow the same contract as the classic queue table: apply the
 * per-item payload from `queueEvents` immediately so a row's progress moves at
 * event rate, and refetch the page on a throttle so membership and ordering
 * settle. Both are needed — the event stream never says where a new row sorts.
 *
 * A job someone just added is the exception to the throttle. Its row is drawn
 * from the creation event the moment it arrives, at the end of the list, and
 * the page is read again straight away to put it in its place and count it.
 */

const QUEUE_PAGE_SIZE = 500;
const QUEUE_EVENT_REFRESH_INTERVAL_MS = 2_000;
/** Jobs added together (a multi-file drop) are one refetch, not one each. */
const ARRIVAL_REFRESH_DELAY_MS = 100;

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
  // Jobs created since the page was read, held until a page read after their
  // creation either carries them or shows they have already left.
  const [arrivals, setArrivals] = useState<Record<number, { item: GraphqlJobData; cursor: bigint }>>(
    {},
  );
  const [removedIds, setRemovedIds] = useState<ReadonlySet<number>>(() => new Set());
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const refreshDueAtRef = useRef<number | null>(null);
  const lastRefreshAtRef = useRef(0);
  const lastEventSequenceRef = useRef<bigint | null>(null);
  const lastConnectedAtRef = useRef<number | null | undefined>(undefined);

  const refreshNow = useCallback(() => {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
    refreshDueAtRef.current = null;
    lastRefreshAtRef.current = Date.now();
    void reexecuteQuery({ requestPolicy: "network-only" });
  }, [reexecuteQuery]);

  /** `soon` brings a pending throttled refetch forward rather than waiting it out. */
  const scheduleRefresh = useCallback((soon = false) => {
    const now = Date.now();
    const dueAt = soon
      ? now + ARRIVAL_REFRESH_DELAY_MS
      : Math.max(now, lastRefreshAtRef.current + QUEUE_EVENT_REFRESH_INTERVAL_MS);
    if (refreshTimerRef.current) {
      if (refreshDueAtRef.current !== null && refreshDueAtRef.current <= dueAt) {
        return;
      }
      clearTimeout(refreshTimerRef.current);
    }
    refreshDueAtRef.current = dueAt;
    refreshTimerRef.current = setTimeout(() => {
      refreshTimerRef.current = null;
      refreshDueAtRef.current = null;
      lastRefreshAtRef.current = Date.now();
      void reexecuteQuery({ requestPolicy: "network-only" });
    }, dueAt - now);
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
    setArrivals({});
    setRemovedIds(new Set());
    setPolledPage(undefined);
    refreshNow();
  }, [connection.lastConnectedAt, connection.status, refreshNow]);

  // `queuePage` serves at most QUEUE_PAGE_SIZE jobs. Each time the first page
  // lands with a larger total, the rest are read behind it; until they arrive
  // the previous tail stands, so a refetch never truncates the list.
  const [tail, setTail] = useState<GraphqlJobData[]>(EMPTY_ITEMS);
  useEffect(() => {
    if (!page || page.items.length >= page.totalCount) {
      return;
    }
    let cancelled = false;
    const pageCount = Math.ceil(page.totalCount / QUEUE_PAGE_SIZE);
    void Promise.all(
      Array.from({ length: pageCount - 1 }, (_, index) =>
        client
          .query<QueuePageResponse>(
            QUEUE_PAGE_QUERY,
            { input: { pageIndex: index + 1, pageSize: QUEUE_PAGE_SIZE } },
            { requestPolicy: "network-only" },
          )
          .toPromise(),
      ),
    ).then((results) => {
      if (cancelled || results.some((result) => !result.data)) {
        return;
      }
      setTail(results.flatMap((result) => result.data?.queuePage.items ?? []));
    });
    return () => {
      cancelled = true;
    };
  }, [client, page]);

  const firstPageItems = page?.items ?? EMPTY_ITEMS;
  const pageItems = useMemo(() => {
    if (!page || firstPageItems.length >= page.totalCount || tail.length === 0) {
      return firstPageItems;
    }
    // Jobs can shift across a page boundary between the two reads.
    const seen = new Set(firstPageItems.map((item) => item.id));
    return [...firstPageItems, ...tail.filter((item) => !seen.has(item.id))];
  }, [firstPageItems, page, tail]);

  const latestCursor = page?.latestCursor;
  const visibleIds = useMemo(() => new Set(pageItems.map((item) => item.id)), [pageItems]);

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
          setArrivals((current) => {
            if (!(removedId in current)) {
              return current;
            }
            const next = { ...current };
            delete next[removedId];
            return next;
          });
          scheduleRefresh();
          return;
        }

        if (event.item && !visibleIds.has(event.item.id)) {
          const item = event.item;
          const created = event.kind === "ITEM_CREATED";
          setArrivals((current) => {
            const existing = current[item.id];
            if ((!created && !existing) || (existing && existing.cursor >= sequence)) {
              return current;
            }
            return { ...current, [item.id]: { item, cursor: sequence } };
          });
          scheduleRefresh(created);
          return;
        }

        if (event.item) {
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

  // A page read after a job's creation is the authority on it: either the job
  // is on it, or it has already finished or gone.
  useEffect(() => {
    const pageSequence = latestCursor ? decodeQueueEventCursor(latestCursor) : null;
    setArrivals((current) => {
      const entries = Object.entries(current);
      const kept = entries.filter(
        ([id, arrival]) =>
          !visibleIds.has(Number(id)) && (pageSequence === null || arrival.cursor > pageSequence),
      );
      return kept.length === entries.length ? current : Object.fromEntries(kept);
    });
  }, [latestCursor, visibleIds]);

  const jobs = useMemo(() => {
    const arrived = Object.values(arrivals)
      .filter((arrival) => !visibleIds.has(arrival.item.id))
      .sort((left, right) => (left.cursor < right.cursor ? -1 : left.cursor > right.cursor ? 1 : 0))
      .map((arrival) => arrival.item);
    return [...pageItems, ...arrived]
      .filter((item) => !removedIds.has(item.id))
      .map((item) => normalizeJobData(overlays[item.id]?.item ?? item));
  }, [arrivals, overlays, pageItems, removedIds, visibleIds]);

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

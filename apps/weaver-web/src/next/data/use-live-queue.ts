// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  stringifyVariables,
  useClient,
  useSubscription,
  type AnyVariables,
  type DocumentInput,
} from "urql";
import {
  QUEUE_PAGE_QUERY,
  QUEUE_PAGE_UPDATES_SUBSCRIPTION,
  QUEUE_TOTALS_QUERY,
  QUEUE_TOTALS_SUBSCRIPTION,
} from "@/graphql/queries";
import { useGraphqlConnectionState } from "@/graphql/client";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";
import type { QueuePageInput, QueueStateCount } from "./queue-page";

/**
 * The live queue, as pages the daemon cuts.
 *
 * Nothing here holds the whole queue. Every screen reads the queue's totals
 * (one row's worth of page, pushed again whenever the queue moves); the
 * Downloads screen reads the one page it shows, pushed the same way. While the
 * socket is down both fall back to polling the same query.
 */

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
const EMPTY_STATE_COUNTS: QueueStateCount[] = [];
const EMPTY_CATEGORIES: string[] = [];
const EMPTY_JOBS: JobData[] = [];
const EMPTY_CATEGORY_COUNTS: { category: string; count: number }[] = [];
const EMPTY_ETA_THROUGH: { id: number; bytesThrough: number }[] = [];

interface QueueTotals {
  totalCount: number;
  summary: QueueSummary;
  stateCounts: QueueStateCount[];
  categories: string[];
}

export interface LiveQueue {
  summary: QueueSummary;
  /** Jobs per state across the whole queue. */
  stateCounts: QueueStateCount[];
  /** The categories the queued jobs carry. */
  categories: string[];
  totalCount: number;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

/**
 * A query kept live: the subscription's pushes, the query's answer when one is
 * asked for since, and the poll's while the socket is down.
 *
 * Each push is tagged with the variables it answers, by value: a new object
 * holding the same variables asks the same question, and urql keeps the same
 * subscription for it. When the variables change, the last answer stays on
 * screen until the new one comes, and is reported `stale` meanwhile, so
 * nothing is judged against rows the new variables did not ask for.
 */
function useLiveRead<Data, Variables extends AnyVariables>(
  query: DocumentInput<Data, Variables>,
  subscription: DocumentInput<Data, Variables>,
  variables: Variables,
) {
  const client = useClient();
  const connection = useGraphqlConnectionState();
  const disconnected = connection.status === "disconnected";
  const key = useMemo(() => stringifyVariables(variables), [variables]);
  const tag = useCallback((_previous: unknown, data: Data) => ({ data, key }), [key]);
  const [{ data: tagged, error: pushError }] = useSubscription<
    Data,
    { data: Data; key: string },
    Variables
  >({ query: subscription, variables, pause: disconnected }, tag);
  const pushed = tagged?.key === key ? tagged.data : undefined;
  const [polled, setPolled] = useState<{ data: Data; key: string }>();
  useReconnectPolling<Data, Variables>({
    enabled: disconnected,
    query,
    variables,
    onData: (data) => setPolled({ data, key }),
  });
  // An answer read on request stands until the next push replaces it.
  const [asked, setAsked] = useState<{ data: Data; after: Data | undefined; key: string }>();
  const refresh = useCallback(() => {
    void client
      .query<Data, Variables>(query, variables, { requestPolicy: "network-only" })
      .toPromise()
      .then((result) => {
        if (result.data) {
          setAsked({ data: result.data, after: pushed, key });
        }
      });
  }, [client, key, pushed, query, variables]);

  useEffect(() => {
    if (!disconnected) setPolled(undefined);
  }, [disconnected]);

  let data = pushed;
  if (asked && asked.after === pushed && asked.key === key) data = asked.data;
  if (disconnected && polled && polled.key === key) data = polled.data;
  const stale = data === undefined && tagged !== undefined;
  if (stale) data = tagged.data;
  return { data, stale, error: pushError?.message ?? null, refresh };
}

const NO_VARIABLES = {};

/** The queue's totals, which every screen shows. */
export function useLiveQueue(): LiveQueue {
  const { data, error, refresh } = useLiveRead<
    { queuePage?: QueueTotals; queuePageUpdates?: QueueTotals },
    AnyVariables
  >(QUEUE_TOTALS_QUERY, QUEUE_TOTALS_SUBSCRIPTION, NO_VARIABLES);
  const totals = data?.queuePageUpdates ?? data?.queuePage;
  return useMemo(
    () => ({
      summary: totals?.summary ?? EMPTY_SUMMARY,
      stateCounts: totals?.stateCounts ?? EMPTY_STATE_COUNTS,
      categories: totals?.categories ?? EMPTY_CATEGORIES,
      totalCount: totals?.totalCount ?? 0,
      isLoading: totals === undefined && error === null,
      error,
      refresh,
    }),
    [error, refresh, totals],
  );
}

interface QueuePageData {
  items: GraphqlJobData[];
  totalCount: number;
  categoryCounts: { category: string; count: number }[];
  stateCounts: QueueStateCount[];
  remainingBytes: number;
  etaTotalBytes: number;
  etaDownloadedBytes: number;
  etaThrough: { id: number; bytesThrough: number }[];
  latestCursor: string;
}

export interface LiveQueuePage {
  jobs: JobData[];
  /** Jobs the page's filters match, across every page. */
  totalCount: number;
  categoryCounts: { category: string; count: number }[];
  stateCounts: QueueStateCount[];
  remainingBytes: number;
  etaTotalBytes: number;
  etaDownloadedBytes: number;
  etaThrough: { id: number; bytesThrough: number }[];
  isLoading: boolean;
  /** The rows shown answer earlier variables; the asked page has not come yet. */
  stale: boolean;
  error: string | null;
  refresh: () => void;
}

/*
 * Each item normalized once. A push that carries a row unchanged carries a new
 * object, so rows are matched on content to keep the old one and let a row
 * drawn from it tell that nothing about it changed.
 */
const normalizedJobs = new WeakMap<GraphqlJobData, JobData>();

function normalizeOnce(item: GraphqlJobData): JobData {
  let job = normalizedJobs.get(item);
  if (job === undefined) {
    job = normalizeJobData(item);
    normalizedJobs.set(item, job);
  }
  return job;
}

/** One page of the queue, as `input` names it. */
export function useQueuePage(input: QueuePageInput): LiveQueuePage {
  const variables = useMemo(() => ({ input }), [input]);
  const { data, stale, error, refresh } = useLiveRead<
    { queuePage?: QueuePageData; queuePageUpdates?: QueuePageData },
    { input: QueuePageInput }
  >(QUEUE_PAGE_QUERY, QUEUE_PAGE_UPDATES_SUBSCRIPTION, variables);
  const page = data?.queuePageUpdates ?? data?.queuePage;

  // Keep each row's object while its content is the same.
  const [previous, setPrevious] = useState<{ page: QueuePageData; jobs: JobData[] }>();
  let jobs = EMPTY_JOBS;
  if (page) {
    if (previous?.page === page) {
      jobs = previous.jobs;
    } else {
      const byId = new Map((previous?.jobs ?? EMPTY_JOBS).map((job) => [job.id, job]));
      const keyed = new Map(
        (previous?.page.items ?? []).map((item) => [item.id, JSON.stringify(item)]),
      );
      jobs = page.items.map((item) => {
        const kept = byId.get(item.id);
        return kept && keyed.get(item.id) === JSON.stringify(item) ? kept : normalizeOnce(item);
      });
      setPrevious({ page, jobs });
    }
  }

  return {
    jobs,
    totalCount: page?.totalCount ?? 0,
    categoryCounts: page?.categoryCounts ?? EMPTY_CATEGORY_COUNTS,
    stateCounts: page?.stateCounts ?? EMPTY_STATE_COUNTS,
    remainingBytes: page?.remainingBytes ?? 0,
    etaTotalBytes: page?.etaTotalBytes ?? 0,
    etaDownloadedBytes: page?.etaDownloadedBytes ?? 0,
    etaThrough: page?.etaThrough ?? EMPTY_ETA_THROUGH,
    isLoading: page === undefined && error === null,
    stale,
    error,
    refresh,
  };
}

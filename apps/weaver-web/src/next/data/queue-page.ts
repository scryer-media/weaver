// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { JobProtocol } from "../../lib/job-types.ts";
import { isActiveStatus, statusToken } from "../../lib/status-tokens.ts";
import { UNCATEGORISED } from "./categories.ts";

/**
 * The queue as pages the daemon cuts.
 *
 * The screen never holds the whole queue: it names a page (tab, categories,
 * search, sort, index, size) and the daemon answers with that page's rows and
 * the counts the screen draws around it. These are the pure parts of that
 * exchange; the hooks and the screen are the wiring.
 */

export type DownloadGroup = "active" | "paused" | "queued" | "attention";

export function downloadGroup(status: string): DownloadGroup {
  const token = statusToken(status);
  if (token === "paused") return "paused";
  if (token === "failed") return "attention";
  if (isActiveStatus(status)) return "active";
  return "queued";
}

export type QueueTab = "all" | "active" | "queued" | "paused";
export type QueueSort = "priority" | "name" | "size" | "progress" | "eta";

/** Every state a queued job can be in, as the daemon names it. */
export const QUEUE_ITEM_STATES = [
  "AWAITING_QUEUE_SCRIPTS",
  "QUEUED",
  "DOWNLOADING",
  "FETCHING_REPAIR_DATA",
  "FINALIZING_DOWNLOAD",
  "CHECKING",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "FINALIZING",
  "POST_PROCESSING",
  "COMPLETED",
  "FAILED",
  "PAUSED",
  "FETCHING_METADATA",
  "CHECKING_FILES",
  "SEEDING",
  "STALLED",
] as const;

export type QueueItemState = (typeof QUEUE_ITEM_STATES)[number];

/** The states a tab shows; `undefined` is every state. */
export function tabStates(tab: QueueTab): QueueItemState[] | undefined {
  if (tab === "all") {
    return undefined;
  }
  return QUEUE_ITEM_STATES.filter((state) => downloadGroup(state) === tab);
}

/** Which jobs the screen is looking at, whatever page of them it shows. */
export interface QueueSelection {
  search?: string;
  protocol?: JobProtocol;
  states?: QueueItemState[];
  categories?: string[];
}

export function queueSelection({
  tab,
  facets,
  query,
  protocol = null,
}: {
  tab: QueueTab;
  facets: ReadonlySet<string>;
  query: string;
  protocol?: JobProtocol | null;
}): QueueSelection {
  const search = query.trim();
  const selection: QueueSelection = {};
  if (search !== "") selection.search = search;
  if (protocol !== null) selection.protocol = protocol;
  const states = tabStates(tab);
  if (states) selection.states = states;
  if (facets.size > 0) {
    selection.categories = [...facets].map((key) => (key === UNCATEGORISED ? "" : key));
  }
  return selection;
}

const SORT_FIELDS: Record<QueueSort, { sortField: string; sortDirection: string } | null> = {
  // Queue order is priority order, and the order every time left counts in.
  priority: null,
  eta: null,
  name: { sortField: "NAME", sortDirection: "ASC" },
  size: { sortField: "SIZE", sortDirection: "DESC" },
  progress: { sortField: "PROGRESS", sortDirection: "DESC" },
};

export interface QueuePageInput extends QueueSelection {
  pageIndex: number;
  pageSize: number;
  sortField?: string;
  sortDirection?: string;
}

export function queuePageInput(
  selection: QueueSelection,
  sort: QueueSort,
  pageIndex: number,
  pageSize: number,
): QueuePageInput {
  return { pageIndex, pageSize, ...selection, ...(SORT_FIELDS[sort] ?? {}) };
}

export interface QueueStateCount {
  state: string;
  count: number;
}

/**
 * Jobs per section across every page, from the daemon's counts taken before
 * the tab applies: a section's header counts the queue, not the page.
 */
export function groupCounts(stateCounts: readonly QueueStateCount[]): Record<DownloadGroup, number> {
  const counts: Record<DownloadGroup, number> = { active: 0, paused: 0, queued: 0, attention: 0 };
  for (const { state, count } of stateCounts) {
    counts[downloadGroup(state)] += count;
  }
  return counts;
}

/** Jobs per tab, from the daemon's counts taken before the tab applies. */
export function tabCounts(stateCounts: readonly QueueStateCount[]): Record<QueueTab, number> {
  const groups = groupCounts(stateCounts);
  return {
    all: groups.active + groups.paused + groups.queued + groups.attention,
    active: groups.active,
    queued: groups.queued,
    paused: groups.paused,
  };
}

/**
 * Time left per page row, in queue order: each row's own bytes and every
 * waiting or downloading job's ahead of it, at `speed`.
 */
export function etaById(
  etaThrough: readonly { id: number; bytesThrough: number }[],
  speed: number,
  format: (bytes: number, speed: number) => string,
): Map<number, string> {
  const estimates = new Map<number, string>();
  if (speed <= 0) {
    return estimates;
  }
  for (const { id, bytesThrough } of etaThrough) {
    estimates.set(id, format(bytesThrough, speed));
  }
  return estimates;
}

/** A queue event, as far as the ticks care. */
export interface QueueEventSummary {
  kind: string;
  itemId: number | null;
  state: string | null;
}

/**
 * The job a queue event says has left the queue (finished, failed or
 * removed), or null. Ticks hold across pages, so this is what drops one.
 */
export function departedId(event: QueueEventSummary): number | null {
  if (event.itemId == null) return null;
  const gone =
    event.kind === "ITEM_COMPLETED"
    || event.kind === "ITEM_REMOVED"
    || (event.kind === "ITEM_STATE_CHANGED" && event.state === "FAILED");
  return gone ? event.itemId : null;
}

/** The ticks less `ids`; the same set when none of them was ticked. */
export function withoutIds(
  picked: ReadonlySet<number>,
  ids: Iterable<number>,
): ReadonlySet<number> {
  let next: Set<number> | null = null;
  for (const id of ids) {
    if (picked.has(id)) {
      next ??= new Set(picked);
      next.delete(id);
    }
  }
  return next ?? picked;
}

/** The selection a batch sends: an unfiltered one names the whole queue outright. */
export interface QueueBatchSelection extends QueueSelection {
  all?: boolean;
}

/**
 * What a batch acts on: the ticked rows, or every job the filters match,
 * with the count the operator confirmed.
 */
export type QueueBatchTarget =
  | { ids: number[] }
  | { selection: QueueBatchSelection; expectedMatched: number };

export function batchTarget(
  allMatching: boolean,
  picked: ReadonlySet<number>,
  selection: QueueSelection,
  expectedMatched: number,
): QueueBatchTarget {
  if (!allMatching) return { ids: [...picked] };
  const unfiltered =
    selection.search === undefined
    && selection.states === undefined
    && selection.categories === undefined;
  return {
    selection: unfiltered ? { ...selection, all: true } : selection,
    expectedMatched,
  };
}

interface GraphqlErrorLike {
  graphQLErrors?: readonly { extensions?: Record<string, unknown> }[];
}

/**
 * The count the daemon matches now, when it refused a selection batch
 * because the queue moved since the operator confirmed; otherwise null.
 */
export function conflictCount(error: GraphqlErrorLike | undefined): number | null {
  const extensions = error?.graphQLErrors?.[0]?.extensions;
  if (extensions?.code !== "CONFLICT") return null;
  const matched = Number(extensions.matched);
  return Number.isFinite(matched) ? matched : null;
}

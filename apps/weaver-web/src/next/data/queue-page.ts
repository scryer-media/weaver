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

/**
 * The list's state tabs. Downloading is every job underway that is not
 * seeding, waiting or paused, so the four between them hold every job; the
 * All tab beside them is no state filter.
 */
export type QueueTab = "downloading" | "seeding" | "queued" | "paused";

/** The tab a job in `status` sits under. */
export function queueTab(status: string): QueueTab {
  if (status.toUpperCase() === "SEEDING") return "seeding";
  const token = statusToken(status);
  if (token === "paused") return "paused";
  if (token === "queued" && !isActiveStatus(status)) return "queued";
  return "downloading";
}
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

/** The states a tab shows; no tab is every state. */
export function tabStates(tab: QueueTab | null): QueueItemState[] | undefined {
  if (tab === null) {
    return undefined;
  }
  return QUEUE_ITEM_STATES.filter((state) => queueTab(state) === tab);
}

/** Which jobs the screen is looking at, whatever page of them it shows. */
export interface QueueSelection {
  search?: string;
  protocol?: JobProtocol;
  states?: QueueItemState[];
  categories?: string[];
  /** Jobs carrying this tag; the empty tag is jobs with none. */
  tag?: string;
}

export function queueSelection({
  tab = null,
  facets,
  query,
  protocol = null,
  tag = null,
}: {
  /** No tab is the whole queue. */
  tab?: QueueTab | null;
  facets: ReadonlySet<string>;
  query: string;
  protocol?: JobProtocol | null;
  tag?: string | null;
}): QueueSelection {
  const search = query.trim();
  const selection: QueueSelection = {};
  if (search !== "") selection.search = search;
  if (protocol !== null) selection.protocol = protocol;
  const states = tabStates(tab);
  if (states) selection.states = states;
  if (facets.size > 0) {
    selection.categories = [...facets].map((key) =>
      key === UNCATEGORISED ? "" : key,
    );
  }
  if (tag !== null) selection.tag = tag;
  return selection;
}

const SORT_FIELDS: Record<
  QueueSort,
  { sortField: string; sortDirection: string } | null
> = {
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
export function groupCounts(
  stateCounts: readonly QueueStateCount[],
): Record<DownloadGroup, number> {
  const counts: Record<DownloadGroup, number> = {
    active: 0,
    paused: 0,
    queued: 0,
    attention: 0,
  };
  for (const { state, count } of stateCounts) {
    counts[downloadGroup(state)] += count;
  }
  return counts;
}

/** Jobs per tab, All included, from the daemon's counts taken before the tab applies. */
export function tabCounts(
  stateCounts: readonly QueueStateCount[],
): Record<QueueTab | "all", number> {
  const counts: Record<QueueTab | "all", number> = {
    all: 0,
    downloading: 0,
    seeding: 0,
    queued: 0,
    paused: 0,
  };
  for (const { state, count } of stateCounts) {
    counts[queueTab(state)] += count;
    counts.all += count;
  }
  return counts;
}

/**
 * How fast the slow destination disks take bytes. A row held back by one
 * finishes when that disk has written what waits for it and what is still to
 * come, so its time left runs at the disk's rate, not the download speed.
 */
export interface DiskPace {
  /** Per disk-limited row: its volume's measured write rate and bytes waiting for it. */
  byId: ReadonlyMap<number, { rate: number; waiting: number }>;
  /** The slowest measured rate among the page's slow disks, 0 when none is measured. */
  slowest: number;
}

const NO_DISK_PACE: DiskPace = { byId: new Map(), slowest: 0 };

/**
 * Time left per page row, in queue order: each row's own bytes and every
 * waiting or downloading job's ahead of it, at `speed`.
 *
 * With `disk`, a row a slow disk holds back counts its volume's waiting bytes
 * too, at that volume's write rate or at `speed` when that is higher (a
 * measured rate sags while the disk is shared, and no row runs slower than
 * the queue's own time left); any other row falls back to the slowest
 * disk's rate when nothing is downloading, since the queue drains no faster.
 * Without a slow disk the estimates are the plain ones.
 */
export function etaById(
  etaThrough: readonly { id: number; bytesThrough: number }[],
  speed: number,
  format: (bytes: number, speed: number) => string,
  disk: DiskPace = NO_DISK_PACE,
): Map<number, string> {
  const estimates = new Map<number, string>();
  const fallback = speed > 0 ? speed : disk.slowest;
  if (fallback <= 0 && disk.byId.size === 0) {
    return estimates;
  }
  for (const { id, bytesThrough } of etaThrough) {
    const held = disk.byId.get(id);
    if (held && held.rate > 0) {
      estimates.set(
        id,
        format(bytesThrough + held.waiting, Math.max(held.rate, speed)),
      );
    } else if (fallback > 0) {
      estimates.set(id, format(bytesThrough, fallback));
    }
  }
  return estimates;
}

/**
 * The disk pace for the rows on a page: each row held by a slow disk, with
 * that disk's figures from the live disk list.
 */
export function diskPace(
  rows: readonly {
    id: number;
    status: string;
    diskWrite?: { volume: string; slow: boolean } | null;
  }[],
  volumes: readonly {
    label: string;
    writeRateBytesPerSec: number;
    waitingBytes: number;
  }[],
): DiskPace {
  if (volumes.length === 0) return NO_DISK_PACE;
  const byId = new Map<number, { rate: number; waiting: number }>();
  let slowest = 0;
  for (const row of rows) {
    const write = row.diskWrite;
    if (!write?.slow) continue;
    const figures = volumes.find((volume) => volume.label === write.volume);
    if (!figures) continue;
    const rate = figures.writeRateBytesPerSec;
    if (rate > 0 && (slowest === 0 || rate < slowest)) slowest = rate;
    if (row.status !== "PAUSED") {
      byId.set(row.id, { rate, waiting: figures.waitingBytes });
    }
  }
  return byId.size === 0 && slowest === 0 ? NO_DISK_PACE : { byId, slowest };
}

const FETCHING_STATES = new Set(["DOWNLOADING", "FETCHING_REPAIR_DATA"]);

/** Whether no job in the queue is fetching bytes, from the daemon's counts. */
export function nothingDownloading(
  stateCounts: readonly QueueStateCount[],
): boolean {
  return !stateCounts.some(
    ({ state, count }) => FETCHING_STATES.has(state) && count > 0,
  );
}

/**
 * The write rate the queue drains at while a slow disk holds bytes waiting:
 * the slowest measured rate among such disks, 0 when there is none.
 */
export function slowDiskRate(
  volumes: readonly {
    slow: boolean;
    writeRateBytesPerSec: number;
    waitingBytes: number;
  }[],
): number {
  let slowest = 0;
  for (const { slow, writeRateBytesPerSec: rate, waitingBytes } of volumes) {
    if (
      slow &&
      waitingBytes > 0 &&
      rate > 0 &&
      (slowest === 0 || rate < slowest)
    )
      slowest = rate;
  }
  return slowest;
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
    event.kind === "ITEM_COMPLETED" ||
    event.kind === "ITEM_REMOVED" ||
    (event.kind === "ITEM_STATE_CHANGED" && event.state === "FAILED");
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
    selection.search === undefined &&
    selection.states === undefined &&
    selection.categories === undefined &&
    selection.tag === undefined;
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
export function conflictCount(
  error: GraphqlErrorLike | undefined,
): number | null {
  const extensions = error?.graphQLErrors?.[0]?.extensions;
  if (extensions?.code !== "CONFLICT") return null;
  const matched = Number(extensions.matched);
  return Number.isFinite(matched) ? matched : null;
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import type { DownloadBlockState } from "@/lib/context/live-data-context";
import type { ProviderHealth } from "./next-data";
import { providerActivityLabel } from "./provider-activity.ts";

/** What one consumer used over a span, as the server sums it. */
export interface UsageTotals {
  bytes: number;
  articlesTried: number;
  articlesSucceeded: number;
  downSeconds: number;
  /** Sent to peers, for a torrent or tracker. */
  bytesUploaded: number;
  /** Jobs for a job row, timed runs for a stage. */
  events: number;
  /** Stage time, or seeding time. */
  durationMs: number;
}

export interface UsageDay extends UsageTotals {
  /** `YYYY-MM-DD`, the server's local date. */
  day: string;
}

export interface UsageConsumer {
  kind: string;
  id: string;
  today: UsageTotals;
  week: UsageTotals;
  month: UsageTotals;
  year: UsageTotals;
  allTime: UsageTotals;
  range: UsageTotals;
  days: UsageDay[];
  /** A removed server's name, while its usage is kept until its quota resets. */
  removedHost?: string | null;
}

export interface FinishedJobs {
  complete: number;
  partial: number;
  failed: number;
}

export interface UsageHistory {
  today: string;
  rangeStart: string;
  rangeEnd: string;
  todayStartEpochSec: number;
  total: UsageConsumer;
  consumers: UsageConsumer[];
  finishedToday: FinishedJobs;
  finishedInRange: FinishedJobs;
}

export type UsagePeriod = "TODAY" | "WEEK" | "MONTH" | "YEAR" | "ALL" | "CUSTOM";

export const NO_USAGE: UsageTotals = {
  bytes: 0,
  articlesTried: 0,
  articlesSucceeded: 0,
  downSeconds: 0,
  bytesUploaded: 0,
  events: 0,
  durationMs: 0,
};

/** A local date as the server spells it. */
export function dayKey(epochMs: number): string {
  const day = new Date(epochMs);
  const month = String(day.getMonth() + 1).padStart(2, "0");
  const date = String(day.getDate()).padStart(2, "0");
  return `${day.getFullYear()}-${month}-${date}`;
}

/** The `count` local dates ending today, as a custom range. */
export function lastDays(nowMs: number, count: number): { fromDay: string; toDay: string } {
  const today = new Date(nowMs);
  const first = new Date(today.getFullYear(), today.getMonth(), today.getDate() - (count - 1));
  return { fromDay: dayKey(first.getTime()), toDay: dayKey(nowMs) };
}

/** Days added up, for a span the server did not sum. */
export function sumDays(days: readonly UsageTotals[]): UsageTotals {
  return days.reduce<UsageTotals>(
    (total, day) => ({
      bytes: total.bytes + day.bytes,
      articlesTried: total.articlesTried + day.articlesTried,
      articlesSucceeded: total.articlesSucceeded + day.articlesSucceeded,
      downSeconds: total.downSeconds + day.downSeconds,
      bytesUploaded: total.bytesUploaded + day.bytesUploaded,
      events: total.events + day.events,
      durationMs: total.durationMs + day.durationMs,
    }),
    NO_USAGE,
  );
}

/** Articles found out of those asked for, 0 to 100; `null` when none were asked for. */
export function foundPercent(totals: UsageTotals): number | null {
  if (totals.articlesTried <= 0) return null;
  return (Math.min(totals.articlesSucceeded, totals.articlesTried) / totals.articlesTried) * 100;
}

export interface RecoveryTotals {
  availableBytes: number;
  fetchedBytes: number;
}

export interface FailureCount {
  /** The server's failure kind; empty when the job kept none. */
  cause: string;
  count: number;
}

export interface IntegrityHistory {
  today: string;
  rangeStart: string;
  rangeEnd: string;
  finished: UsageConsumer;
  repaired: UsageConsumer;
  stages: UsageConsumer[];
  recovery: RecoveryTotals;
  failures: FailureCount[];
}

export interface TorrentPeers {
  torrents: number;
  connected: number;
  seeds: number;
}

export interface TorrentHistory {
  today: string;
  rangeStart: string;
  rangeEnd: string;
  total: UsageConsumer;
  /** A torrent counts under each tracker it lists, so these never add up. */
  trackers: UsageConsumer[];
  peers: TorrentPeers;
}

/** Part out of whole, 0 to 100; `null` when the whole is nothing. */
export function percentOf(part: number, whole: number): number | null {
  if (whole <= 0) return null;
  return (Math.min(Math.max(part, 0), whole) / whole) * 100;
}

/** Each day's share of finished jobs that needed a repair, 0 to 100. */
export function dailyRepairShare(finished: readonly UsageDay[], repaired: readonly UsageDay[]): number[] {
  const byDay = new Map(repaired.map((day) => [day.day, day.events]));
  return finished.map((day) => percentOf(byDay.get(day.day) ?? 0, day.events) ?? 0);
}

/** A stage's average run in seconds; `null` when it never ran. */
export function averageSeconds(totals: UsageTotals): number | null {
  return totals.events > 0 ? totals.durationMs / totals.events / 1000 : null;
}

/** Sent over received; `null` when nothing was received. */
export function shareRatio(totals: UsageTotals): number | null {
  return totals.bytes > 0 ? totals.bytesUploaded / totals.bytes : null;
}

/** Job stages in the order a job meets them; the server names them. */
export const STAGE_ORDER = ["download", "verify", "repair", "extract", "move", "post_process"] as const;

/** Stages in job order; a stage the web does not know goes last. */
export function stagesInOrder(stages: readonly UsageConsumer[]): UsageConsumer[] {
  const rank = (id: string) => {
    const at = (STAGE_ORDER as readonly string[]).indexOf(id);
    return at < 0 ? STAGE_ORDER.length : at;
  };
  return [...stages].sort((left, right) => rank(left.id) - rank(right.id) || left.id.localeCompare(right.id));
}

/** Whether the server has never held or seeded a torrent. */
export function noTorrents(history: TorrentHistory): boolean {
  const all = history.total.allTime;
  return history.peers.torrents === 0 && all.bytes === 0 && all.bytesUploaded === 0 && all.durationMs === 0;
}

/** Trackers with the most sent in the range first; the no-tracker row last. */
export function trackersInOrder(trackers: readonly UsageConsumer[]): UsageConsumer[] {
  return [...trackers].sort(
    (left, right) =>
      Number(left.id === "") - Number(right.id === "") ||
      right.range.bytesUploaded - left.range.bytesUploaded ||
      left.id.localeCompare(right.id),
  );
}

/** How many daily rows the server keeps; older days fold into all time. */
export const USAGE_KEPT_DAYS = 400;

/** The seconds a local date really has: an hour either way when the clocks change. */
export function secondsInDay(day: string): number {
  const [year, month, date] = day.split("-").map(Number);
  if (!year || !month || !date) return 86_400;
  const seconds = (new Date(year, month - 1, date + 1).getTime() - new Date(year, month - 1, date).getTime()) / 1000;
  return seconds > 0 ? seconds : 86_400;
}

/**
 * Each day's average speed: its bytes over the seconds it has had. A past
 * day had all of its seconds; today has had only those since midnight.
 */
export function averageDailySpeed(
  days: readonly UsageDay[],
  today: string,
  secondsIntoToday: number,
): number[] {
  return days.map((day) => {
    const seconds = day.day === today ? Math.max(1, secondsIntoToday) : secondsInDay(day.day);
    return day.day > today ? 0 : day.bytes / seconds;
  });
}

/** Each bar's height out of the tallest, 0 to 100. */
export function barHeights(values: readonly number[]): number[] {
  const tallest = values.reduce((max, value) => Math.max(max, value), 0);
  return values.map((value) => (tallest > 0 ? (Math.max(0, value) / tallest) * 100 : 0));
}

/**
 * Whether a server is up. The server decides what down means, and the
 * data-used ledger counts time down by the same rule: switched off by a fault,
 * or in a transport cooldown still running. A capacity hold is up.
 */
export function providerUp(provider: ProviderHealth): boolean {
  return provider.down !== true;
}

export interface ProvidersUp {
  up: number;
  total: number;
  /** One line per server that is not up: its name and why. */
  down: { key: string; host: string; reason: string }[];
}

export function providersUp(
  providers: readonly ProviderHealth[],
  t: Translate,
  now: number,
): ProvidersUp {
  const down = providers
    .filter((provider) => !providerUp(provider))
    .map((provider) => {
      const activity = providerActivityLabel(provider, t, now);
      return {
        key: `${provider.host}:${provider.port}`,
        host: provider.host,
        reason: activity.cause ?? activity.word,
      };
    });
  return { up: providers.length - down.length, total: providers.length, down };
}

export type DownloadsState = "running" | "paused" | "egressQuota" | "serverQuota" | "schedule";

/** What is holding downloads, if anything, and when it lifts. */
export function downloadsState(
  isPaused: boolean,
  block: DownloadBlockState,
): { state: DownloadsState; liftsAtEpochMs: number | null } {
  const lifts = block.windowEndsAtEpochMs ?? null;
  if (isPaused || block.kind === "MANUAL_PAUSE" || block.kind === "SPEED_TEST") {
    // A timed pause lifts by itself; any other pause waits for a resume.
    return { state: "paused", liftsAtEpochMs: block.resumeAtEpochMs ?? (isPaused ? null : lifts) };
  }
  switch (block.kind) {
    case "EGRESS_QUOTA":
      return { state: "egressQuota", liftsAtEpochMs: lifts };
    case "SERVER_QUOTA":
      return { state: "serverQuota", liftsAtEpochMs: lifts };
    case "SCHEDULED":
      return { state: "schedule", liftsAtEpochMs: lifts };
    default:
      return { state: "running", liftsAtEpochMs: null };
  }
}

export interface ProviderRow {
  key: string;
  /** The saved server id the ledger counts it under, when known. */
  serverId: string | null;
  provider: ProviderHealth | null;
  usage: UsageConsumer | null;
}

/**
 * A row's name: the configured server's host, or, for a server no longer
 * configured, `removed` of the name it had, or of its id when the ledger
 * keeps no name.
 */
export function providerRowName(row: ProviderRow, removed: (name: string) => string): string {
  return row.provider?.host ?? removed(row.usage?.removedHost ?? row.serverId ?? "");
}

/**
 * One row per server: each configured server with its usage, then any server
 * the ledger still remembers but the pool no longer has. Most data in the
 * range first.
 */
export function providerRows(
  providers: readonly ProviderHealth[],
  consumers: readonly UsageConsumer[],
): ProviderRow[] {
  const servers = consumers.filter((consumer) => consumer.kind === "server");
  const byId = new Map(servers.map((consumer) => [consumer.id, consumer]));
  const seen = new Set<string>();
  const rows: ProviderRow[] = providers.map((provider) => {
    const serverId = provider.serverId == null ? null : String(provider.serverId);
    if (serverId !== null) seen.add(serverId);
    return {
      key: `${provider.host}:${provider.port}:${serverId ?? ""}`,
      serverId,
      provider,
      usage: serverId === null ? null : (byId.get(serverId) ?? null),
    };
  });
  for (const usage of servers) {
    if (!seen.has(usage.id)) {
      rows.push({ key: `ledger:${usage.id}`, serverId: usage.id, provider: null, usage });
    }
  }
  return rows
    .map((row, index) => ({ row, index }))
    .sort(
      (left, right) =>
        (right.row.usage?.range.bytes ?? 0) - (left.row.usage?.range.bytes ?? 0) ||
        left.index - right.index,
    )
    .map(({ row }) => row);
}

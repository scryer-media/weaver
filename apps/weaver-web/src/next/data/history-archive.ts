// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import { statusToken } from "../../lib/status-tokens.ts";
import { countLabel } from "../i18n/labels.ts";
import { formatCount } from "./format.ts";

/**
 * Archiving history entries (removing their files and keeping the entries)
 * is queued work, as a delete is.
 *
 * `archiveHistory` answers at once with what it accepted; the files go in
 * the background. While they wait or go, the entry's `deleteOperation` holds
 * an ARCHIVE operation; on success it clears and `archivedAt` is set,
 * and on failure it stays, FAILED and unlocked, with how the archive ended.
 */

/** What the request answered for one id, before any disk work. */
export type ArchiveHistoryAcceptOutcome =
  | "ACCEPTED"
  | "NOT_FOUND"
  | "BUSY"
  | "NOT_FINISHED"
  | "ALREADY_ARCHIVED";

export interface ArchiveHistoryAcceptResult {
  id: number;
  outcome: ArchiveHistoryAcceptOutcome;
}

/** How an archive ended, on a failed ARCHIVE operation. */
export type ArchiveHistoryOutcome =
  | "ARCHIVED"
  | "ALREADY_GONE"
  | "NOT_MANAGED"
  | "BUSY"
  | "NOT_FOUND"
  | "FAILED";

/** The history operation an entry waits on, or the last one that failed for it. */
export interface HistoryRowOperation {
  state: "QUEUED" | "RUNNING" | "FAILED";
  locked: boolean;
  action: "DELETE" | "ARCHIVE";
  errorMessage?: string | null;
  archiveOutcome?: ArchiveHistoryOutcome | null;
}

/** The most ids one `archiveHistory` call takes. */
export const MAX_ARCHIVE_HISTORY_IDS = 100;

/** The most entries the status line names one by one. */
export const NAMED_FAILURES = 3;

/** What a history row's own actions are decided from. */
export interface HistoryRowState {
  status: string;
  archivedAt: string | number | null;
  deleteOperation: HistoryRowOperation | null;
}

/**
 * What a row's operation means for how it reads: being deleted (any delete,
 * as before), being archived (files waiting or going), or an archive that
 * failed. Null when
 * there is none.
 */
export function rowOperationKind(
  operation: Pick<HistoryRowOperation, "state"> & { action?: HistoryRowOperation["action"] } | null | undefined,
): "deleting" | "archiving" | "archiveFailed" | null {
  if (!operation) {
    return null;
  }
  if (operation.action !== "ARCHIVE") {
    return "deleting";
  }
  return operation.state === "FAILED" ? "archiveFailed" : "archiving";
}

/**
 * Whether a row offers to archive it: a finished entry not yet archived, that
 * no operation holds, or whose last archive failed.
 */
export function rowArchives(row: HistoryRowState): boolean {
  const token = statusToken(row.status);
  const operation = rowOperationKind(row.deleteOperation);
  return (
    (token === "completed" || token === "failed") &&
    row.archivedAt === null &&
    (operation === null || operation === "archiveFailed")
  );
}

/** Whether a row offers to delete it: as for picking it, any row no operation locks. */
export function rowDeletes(row: HistoryRowState): boolean {
  return !row.deleteOperation?.locked;
}

/** `Not archived · Failed: disk full`, for an archive that failed. */
export function describeFailedArchive(t: Translate, operation: HistoryRowOperation): string {
  const outcome = t(`next.historyArchive.outcome.${operation.archiveOutcome ?? "FAILED"}`);
  const message = operation.errorMessage?.trim();
  return `${t("next.historyArchive.failed")} · ${message ? `${outcome}: ${message}` : outcome}`;
}

/** The ids in calls of at most `MAX_ARCHIVE_HISTORY_IDS`, in order. */
export function archiveHistoryChunks(ids: readonly number[]): number[][] {
  const chunks: number[][] = [];
  for (let start = 0; start < ids.length; start += MAX_ARCHIVE_HISTORY_IDS) {
    chunks.push(ids.slice(start, start + MAX_ARCHIVE_HISTORY_IDS));
  }
  return chunks;
}

/** One call's answer: its results, or why it was refused whole. */
export type ArchiveHistoryCall =
  | { results: readonly ArchiveHistoryAcceptResult[] }
  | { error: string };

export interface ArchiveHistoryRequest {
  results: ArchiveHistoryAcceptResult[];
  /** Why a call was refused whole; the ids it and later calls held were not sent. */
  refused: string | null;
}

/**
 * Send `ids` in calls of at most `MAX_ARCHIVE_HISTORY_IDS`, stopping at the
 * first call refused whole. Every place that archives goes through this.
 */
export async function requestArchiveHistory(
  ids: readonly number[],
  send: (chunk: number[]) => Promise<ArchiveHistoryCall>,
): Promise<ArchiveHistoryRequest> {
  const results: ArchiveHistoryAcceptResult[] = [];
  for (const chunk of archiveHistoryChunks(ids)) {
    const answer = await send(chunk);
    if ("error" in answer) {
      return { results, refused: answer.error };
    }
    results.push(...answer.results);
  }
  return { results, refused: null };
}

/** The requested ids no result answered for, in order. */
export function unansweredIds(
  requested: readonly number[],
  results: readonly ArchiveHistoryAcceptResult[],
): number[] {
  const answered = new Set(results.map((result) => result.id));
  return requested.filter((id) => !answered.has(id));
}

/**
 * The status line for an archive request of `requested` entries. Accepted
 * entries are queued, not yet archived; entries already archived are counted
 * quietly. Everything accepted: how many were queued. Otherwise how many of
 * all were queued, the first few refused with why, how many more, and how
 * many were never sent.
 */
export function describeArchiveHistory(
  t: Translate,
  results: readonly ArchiveHistoryAcceptResult[],
  requested: number = results.length,
): string {
  const queued = results.filter((result) => result.outcome === "ACCEPTED").length;
  const gone = results.filter((result) => result.outcome === "ALREADY_ARCHIVED").length;
  const refused = results.filter(
    (result) => result.outcome !== "ACCEPTED" && result.outcome !== "ALREADY_ARCHIVED",
  );
  const unanswered = Math.max(0, requested - results.length);
  const alreadyGone =
    gone > 0 ? countLabel(t, "next.historyArchive.alreadyArchived", gone, { count: formatCount(gone) }) : null;
  if (refused.length === 0 && unanswered === 0) {
    const done =
      queued > 0 || gone === 0
        ? countLabel(t, "next.historyArchive.queued", queued, { count: formatCount(queued) })
        : null;
    return [done, alreadyGone].filter((part) => part !== null).join(" · ");
  }
  const named = refused
    .slice(0, NAMED_FAILURES)
    .map((result) => `#${result.id} ${t(`next.historyArchive.outcome.${result.outcome}`)}`);
  const more = refused.length - named.length;
  return [
    t("next.historyArchive.queuedPartial", {
      done: formatCount(queued),
      total: formatCount(requested),
    }),
    alreadyGone,
    ...named,
    more > 0 ? t("next.historyArchive.more", { count: formatCount(more) }) : null,
    unanswered > 0
      ? countLabel(t, "next.historyArchive.unanswered", unanswered, { count: formatCount(unanswered) })
      : null,
  ]
    .filter((part) => part !== null)
    .join(" · ");
}

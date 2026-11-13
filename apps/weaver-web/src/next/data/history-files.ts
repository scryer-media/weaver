// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import { statusToken } from "../../lib/status-tokens.ts";
import { countLabel } from "../i18n/labels.ts";
import { formatCount } from "./format.ts";

/**
 * Removing history entries' files is queued work, as a delete is.
 *
 * `removeHistoryFiles` answers at once with what it accepted; the files go in
 * the background. While they wait or go, the entry's `deleteOperation` holds
 * a REMOVE_FILES operation; on success it clears and `filesRemovedAt` is set,
 * and on failure it stays, FAILED and unlocked, with how the removal ended.
 */

/** What the request answered for one id, before any disk work. */
export type FilesRemovalAcceptOutcome =
  | "ACCEPTED"
  | "NOT_FOUND"
  | "BUSY"
  | "NOT_FINISHED"
  | "ALREADY_REMOVED";

export interface FilesRemovalAcceptResult {
  id: number;
  outcome: FilesRemovalAcceptOutcome;
}

/** How a files removal ended, on a failed REMOVE_FILES operation. */
export type HistoryFilesRemovalOutcome =
  | "REMOVED"
  | "ALREADY_GONE"
  | "NOT_MANAGED"
  | "BUSY"
  | "NOT_FOUND"
  | "FAILED";

/** The history operation an entry waits on, or the last one that failed for it. */
export interface HistoryRowOperation {
  state: "QUEUED" | "RUNNING" | "FAILED";
  locked: boolean;
  action: "DELETE" | "REMOVE_FILES";
  errorMessage?: string | null;
  filesRemovalOutcome?: HistoryFilesRemovalOutcome | null;
}

/** The most ids one `removeHistoryFiles` call takes. */
export const MAX_FILES_REMOVAL_IDS = 100;

/** The most entries the status line names one by one. */
export const NAMED_FAILURES = 3;

/** What a history row's own actions are decided from. */
export interface HistoryRowState {
  status: string;
  filesRemovedAt: string | number | null;
  deleteOperation: HistoryRowOperation | null;
}

/**
 * What a row's operation means for how it reads: being deleted (any delete,
 * as before), files waiting or going, or a removal that failed. Null when
 * there is none.
 */
export function rowOperationKind(
  operation: Pick<HistoryRowOperation, "state"> & { action?: HistoryRowOperation["action"] } | null | undefined,
): "deleting" | "removingFiles" | "removalFailed" | null {
  if (!operation) {
    return null;
  }
  if (operation.action !== "REMOVE_FILES") {
    return "deleting";
  }
  return operation.state === "FAILED" ? "removalFailed" : "removingFiles";
}

/**
 * Whether a row offers to remove its files: a finished entry whose files are
 * still kept, that no operation holds, or whose last removal failed.
 */
export function rowRemovesFiles(row: HistoryRowState): boolean {
  const token = statusToken(row.status);
  const operation = rowOperationKind(row.deleteOperation);
  return (
    (token === "completed" || token === "failed") &&
    row.filesRemovedAt === null &&
    (operation === null || operation === "removalFailed")
  );
}

/** Whether a row offers to delete it: as for picking it, any row no operation locks. */
export function rowDeletes(row: HistoryRowState): boolean {
  return !row.deleteOperation?.locked;
}

/** `Files not removed · Failed: disk full`, for a removal that failed. */
export function describeFailedRemoval(t: Translate, operation: HistoryRowOperation): string {
  const outcome = t(`next.filesRemoval.outcome.${operation.filesRemovalOutcome ?? "FAILED"}`);
  const message = operation.errorMessage?.trim();
  return `${t("next.filesRemoval.failed")} · ${message ? `${outcome}: ${message}` : outcome}`;
}

/** The ids in calls of at most `MAX_FILES_REMOVAL_IDS`, in order. */
export function filesRemovalChunks(ids: readonly number[]): number[][] {
  const chunks: number[][] = [];
  for (let start = 0; start < ids.length; start += MAX_FILES_REMOVAL_IDS) {
    chunks.push(ids.slice(start, start + MAX_FILES_REMOVAL_IDS));
  }
  return chunks;
}

/** One call's answer: its results, or why it was refused whole. */
export type FilesRemovalCall =
  | { results: readonly FilesRemovalAcceptResult[] }
  | { error: string };

export interface FilesRemovalRequest {
  results: FilesRemovalAcceptResult[];
  /** Why a call was refused whole; the ids it and later calls held were not sent. */
  refused: string | null;
}

/**
 * Send `ids` in calls of at most `MAX_FILES_REMOVAL_IDS`, stopping at the
 * first call refused whole. Every place that removes files goes through this.
 */
export async function requestFilesRemoval(
  ids: readonly number[],
  send: (chunk: number[]) => Promise<FilesRemovalCall>,
): Promise<FilesRemovalRequest> {
  const results: FilesRemovalAcceptResult[] = [];
  for (const chunk of filesRemovalChunks(ids)) {
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
  results: readonly FilesRemovalAcceptResult[],
): number[] {
  const answered = new Set(results.map((result) => result.id));
  return requested.filter((id) => !answered.has(id));
}

/**
 * The status line for a removal request of `requested` entries. Accepted
 * entries are queued, not yet removed; files already removed are counted
 * quietly. Everything accepted: how many were queued. Otherwise how many of
 * all were queued, the first few refused with why, how many more, and how
 * many were never sent.
 */
export function describeFilesRemoval(
  t: Translate,
  results: readonly FilesRemovalAcceptResult[],
  requested: number = results.length,
): string {
  const queued = results.filter((result) => result.outcome === "ACCEPTED").length;
  const gone = results.filter((result) => result.outcome === "ALREADY_REMOVED").length;
  const refused = results.filter(
    (result) => result.outcome !== "ACCEPTED" && result.outcome !== "ALREADY_REMOVED",
  );
  const unanswered = Math.max(0, requested - results.length);
  const alreadyGone =
    gone > 0 ? countLabel(t, "next.filesRemoval.alreadyRemoved", gone, { count: formatCount(gone) }) : null;
  if (refused.length === 0 && unanswered === 0) {
    const done =
      queued > 0 || gone === 0
        ? countLabel(t, "next.filesRemoval.queued", queued, { count: formatCount(queued) })
        : null;
    return [done, alreadyGone].filter((part) => part !== null).join(" · ");
  }
  const named = refused
    .slice(0, NAMED_FAILURES)
    .map((result) => `#${result.id} ${t(`next.filesRemoval.outcome.${result.outcome}`)}`);
  const more = refused.length - named.length;
  return [
    t("next.filesRemoval.queuedPartial", {
      done: formatCount(queued),
      total: formatCount(requested),
    }),
    alreadyGone,
    ...named,
    more > 0 ? t("next.filesRemoval.more", { count: formatCount(more) }) : null,
    unanswered > 0
      ? countLabel(t, "next.filesRemoval.unanswered", unanswered, { count: formatCount(unanswered) })
      : null,
  ]
    .filter((part) => part !== null)
    .join(" · ");
}

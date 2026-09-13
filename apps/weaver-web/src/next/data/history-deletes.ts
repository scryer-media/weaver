/**
 * History deletes are jobs, not calls.
 *
 * `acceptHistoryDelete` answers with an operation id and a target count; the
 * engine removes the entries (and, if asked, their files) in the background.
 * A screen therefore has to hold three things together until the work drains:
 * the rows it just handed over, which the server may not report as locked yet;
 * the operations' own progress; and a final refetch once nothing is active.
 * These are the pure parts of that; `useHistoryDeletes` is the wiring.
 */

import type { Translate } from "@/lib/context/translate-context";

export interface HistoryDeleteOperation {
  id: number;
  state: "QUEUED" | "RUNNING" | "COMPLETED" | "COMPLETED_WITH_ERRORS";
  deleteFiles: boolean;
  totalTargets: number;
  queuedTargets: number;
  runningTargets: number;
  completedTargets: number;
  failedTargets: number;
  requestedAt: string;
}

/** A row this screen handed to an accepted operation. */
export interface DeleteLock {
  operationId: number;
  deleteFiles: boolean;
}

export interface RowDeleteOperation {
  state: string;
  locked: boolean;
}

export interface DeleteProgress {
  total: number;
  queued: number;
  running: number;
  completed: number;
  failed: number;
}

/**
 * Rows as they should be drawn: one this screen has handed over reads as
 * queued for deletion until the server's own page says what it is.
 */
export function withDeleteLocks<T extends { id: number; deleteOperation: RowDeleteOperation | null }>(
  rows: readonly T[],
  locks: ReadonlyMap<number, DeleteLock>,
): T[] {
  if (locks.size === 0) {
    return rows as T[];
  }
  return rows.map((row) =>
    row.deleteOperation === null && locks.has(row.id)
      ? { ...row, deleteOperation: { state: "QUEUED", locked: true } }
      : row,
  );
}

/**
 * The locks still worth holding: a row that has left the page, or that the
 * server now reports on itself, needs no local stand-in.
 */
export function settleDeleteLocks(
  locks: ReadonlyMap<number, DeleteLock>,
  rows: readonly { id: number; deleteOperation: RowDeleteOperation | null }[],
): ReadonlyMap<number, DeleteLock> {
  if (locks.size === 0) {
    return locks;
  }
  const byId = new Map(rows.map((row) => [row.id, row]));
  const next = new Map<number, DeleteLock>();
  for (const [id, lock] of locks) {
    const row = byId.get(id);
    if (row && row.deleteOperation === null) {
      next.set(id, lock);
    }
  }
  return next.size === locks.size ? locks : next;
}

/**
 * Progress across every active operation. Straight after an acceptance the
 * operation list may not include the new one yet, so its target count stands
 * in as queued.
 */
export function deleteProgress(
  operations: readonly HistoryDeleteOperation[],
  pendingTargets: number,
): DeleteProgress {
  const summary = operations.reduce<DeleteProgress>(
    (total, operation) => ({
      total: total.total + operation.totalTargets,
      queued: total.queued + operation.queuedTargets,
      running: total.running + operation.runningTargets,
      completed: total.completed + operation.completedTargets,
      failed: total.failed + operation.failedTargets,
    }),
    { total: 0, queued: 0, running: 0, completed: 0, failed: 0 },
  );
  if (summary.total > 0) {
    return summary;
  }
  return { total: pendingTargets, queued: pendingTargets, running: 0, completed: 0, failed: 0 };
}

/** `Deleting history items · 12 tracked · 3 running · 8 queued · 1 failed`. */
export function describeDeleteProgress(t: Translate, progress: DeleteProgress): string {
  return [
    t("next.deletes.title"),
    t("next.deletes.tracked", { count: progress.total }),
    progress.running > 0 && t("next.deletes.running", { count: progress.running }),
    progress.queued > 0 && t("next.deletes.queued", { count: progress.queued }),
    progress.failed > 0 && t("next.deletes.failed", { count: progress.failed }),
  ]
    .filter(Boolean)
    .join(" · ");
}

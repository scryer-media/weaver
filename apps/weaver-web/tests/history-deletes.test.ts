// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  deleteProgress,
  describeDeleteProgress,
  settleDeleteLocks,
  withDeleteLocks,
  type DeleteLock,
  type HistoryDeleteOperation,
} from "../src/next/data/history-deletes.ts";
import { englishTranslate as t } from "./english-translate.ts";

type Operation = { state: string; locked: boolean; action: "DELETE" | "REMOVE_FILES" };

const row = (id: number, deleteOperation: Operation | null = null) => ({
  id,
  name: `Invented Release ${id}`,
  deleteOperation,
});

const lock: DeleteLock = { operationId: 7, deleteFiles: false };

function operation(overrides: Partial<HistoryDeleteOperation>): HistoryDeleteOperation {
  return {
    id: 7,
    state: "RUNNING",
    deleteFiles: false,
    totalTargets: 0,
    queuedTargets: 0,
    runningTargets: 0,
    completedTargets: 0,
    failedTargets: 0,
    requestedAt: "2026-09-13T00:00:00Z",
    ...overrides,
  };
}

test("a row handed to a delete reads as locked before the server says so", () => {
  const rows = [row(1), row(2)];
  const drawn = withDeleteLocks(rows, new Map([[2, lock]]));

  assert.equal(drawn[0], rows[0]);
  assert.deepEqual(drawn[1].deleteOperation, { state: "QUEUED", locked: true, action: "DELETE" });
  assert.equal(withDeleteLocks(rows, new Map()), rows);
});

test("the server's own delete state wins over the local stand-in", () => {
  const running: Operation = { state: "RUNNING", locked: true, action: "DELETE" };
  const drawn = withDeleteLocks([row(1, running)], new Map([[1, lock]]));
  assert.equal(drawn[0].deleteOperation, running);
});

test("a lock goes once its row is reported on or has left the page", () => {
  const locks = new Map([
    [1, lock],
    [2, lock],
    [3, lock],
  ]);
  const settled = settleDeleteLocks(locks, [row(1), row(2, { state: "RUNNING", locked: true, action: "DELETE" })]);
  assert.deepEqual([...settled.keys()], [1]);

  const unchanged = new Map([[1, lock]]);
  assert.equal(settleDeleteLocks(unchanged, [row(1)]), unchanged);
});

test("a failed files removal on a row handed to a delete reads as the delete", () => {
  const failedRemoval: Operation = { state: "FAILED", locked: false, action: "REMOVE_FILES" };
  const locks = new Map([[1, lock]]);
  const drawn = withDeleteLocks([row(1, failedRemoval)], locks);
  assert.deepEqual(drawn[0].deleteOperation, { state: "QUEUED", locked: true, action: "DELETE" });
  // The lock holds until the server reports the delete itself.
  assert.equal(settleDeleteLocks(locks, [row(1, failedRemoval)]), locks);
  assert.equal(settleDeleteLocks(locks, [row(1, { state: "QUEUED", locked: true, action: "DELETE" })]).size, 0);
});

test("progress sums the active operations", () => {
  const progress = deleteProgress(
    [
      operation({ totalTargets: 10, runningTargets: 2, queuedTargets: 6, completedTargets: 2 }),
      operation({ id: 8, totalTargets: 3, queuedTargets: 2, failedTargets: 1 }),
    ],
    13,
  );
  assert.deepEqual(progress, { total: 13, queued: 8, running: 2, completed: 2, failed: 1 });
  assert.equal(
    describeDeleteProgress(t, progress),
    "Deleting history items · 13 tracked · 2 running · 8 queued · 1 failed",
  );
});

test("an accepted delete the operation list has not caught up with counts as queued", () => {
  const progress = deleteProgress([], 4);
  assert.deepEqual(progress, { total: 4, queued: 4, running: 0, completed: 0, failed: 0 });
  assert.equal(describeDeleteProgress(t, progress), "Deleting history items · 4 tracked · 4 queued");
});

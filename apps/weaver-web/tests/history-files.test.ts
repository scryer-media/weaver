// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  describeFailedRemoval,
  describeFilesRemoval,
  filesRemovalChunks,
  requestFilesRemoval,
  rowDeletes,
  rowOperationKind,
  rowRemovesFiles,
  unansweredIds,
  type FilesRemovalAcceptOutcome,
  type FilesRemovalAcceptResult,
  type HistoryRowOperation,
} from "../src/next/data/history-files.ts";
import { englishTranslate as t } from "./english-translate.ts";

const result = (id: number, outcome: FilesRemovalAcceptOutcome): FilesRemovalAcceptResult => ({ id, outcome });

const op = (
  action: HistoryRowOperation["action"],
  state: HistoryRowOperation["state"],
  extra: Partial<HistoryRowOperation> = {},
): HistoryRowOperation => ({ action, state, locked: state !== "FAILED", ...extra });

const row = (
  status: string,
  filesRemovedAt: number | null = null,
  deleteOperation: HistoryRowOperation | null = null,
) => ({ status, filesRemovedAt, deleteOperation });

test("a row's operation reads as a delete, a pending removal or a failed one", () => {
  assert.equal(rowOperationKind(null), null);
  for (const state of ["QUEUED", "RUNNING", "FAILED"] as const) {
    assert.equal(rowOperationKind(op("DELETE", state)), "deleting");
  }
  assert.equal(rowOperationKind(op("REMOVE_FILES", "QUEUED")), "removingFiles");
  assert.equal(rowOperationKind(op("REMOVE_FILES", "RUNNING")), "removingFiles");
  assert.equal(rowOperationKind(op("REMOVE_FILES", "FAILED")), "removalFailed");
});

test("a row offers to remove its files when finished, kept, and free or last failed", () => {
  assert.equal(rowRemovesFiles(row("COMPLETE")), true);
  assert.equal(rowRemovesFiles(row("FAILED")), true);
  // Not finished.
  assert.equal(rowRemovesFiles(row("DOWNLOADING")), false);
  assert.equal(rowRemovesFiles(row("QUEUED")), false);
  // Files already removed.
  assert.equal(rowRemovesFiles(row("COMPLETE", 1_767_225_600_000)), false);
  // A removal waiting or running.
  assert.equal(rowRemovesFiles(row("COMPLETE", null, op("REMOVE_FILES", "QUEUED"))), false);
  assert.equal(rowRemovesFiles(row("COMPLETE", null, op("REMOVE_FILES", "RUNNING"))), false);
  // A removal that failed is offered again.
  assert.equal(rowRemovesFiles(row("COMPLETE", null, op("REMOVE_FILES", "FAILED"))), true);
  // Any delete, in any state.
  for (const state of ["QUEUED", "RUNNING", "FAILED"] as const) {
    assert.equal(rowRemovesFiles(row("COMPLETE", null, op("DELETE", state))), false);
  }
});

test("a row offers to delete it unless an operation locks it, as picking does", () => {
  assert.equal(rowDeletes(row("COMPLETE")), true);
  assert.equal(rowDeletes(row("FAILED", 1_767_225_600_000)), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("DELETE", "FAILED"))), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("REMOVE_FILES", "FAILED"))), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("DELETE", "RUNNING"))), false);
  assert.equal(rowDeletes(row("COMPLETE", null, op("REMOVE_FILES", "QUEUED"))), false);
  assert.equal(rowDeletes(row("COMPLETE", null, op("REMOVE_FILES", "RUNNING"))), false);
});

test("a failed removal reads as its outcome, with the server's message when there is one", () => {
  assert.equal(
    describeFailedRemoval(t, op("REMOVE_FILES", "FAILED", { filesRemovalOutcome: "NOT_MANAGED" })),
    "Files not removed · Not managed by Weaver",
  );
  assert.equal(
    describeFailedRemoval(t, op("REMOVE_FILES", "FAILED", { filesRemovalOutcome: "BUSY", errorMessage: "  " })),
    "Files not removed · Busy",
  );
  assert.equal(
    describeFailedRemoval(
      t,
      op("REMOVE_FILES", "FAILED", { filesRemovalOutcome: "FAILED", errorMessage: "disk error" }),
    ),
    "Files not removed · Failed: disk error",
  );
  // No outcome given reads as failed.
  assert.equal(describeFailedRemoval(t, op("REMOVE_FILES", "FAILED")), "Files not removed · Failed");
});

test("ids go out in calls of at most 100", () => {
  const ids = Array.from({ length: 250 }, (_, index) => index + 1);
  assert.deepEqual(
    filesRemovalChunks(ids).map((chunk) => chunk.length),
    [100, 100, 50],
  );
});

test("a request sends every chunk in order and stops at the first refused", async () => {
  const ids = Array.from({ length: 250 }, (_, index) => index + 1);
  const sent: number[][] = [];
  const all = await requestFilesRemoval(ids, async (chunk) => {
    sent.push(chunk);
    return { results: chunk.map((id) => result(id, "ACCEPTED")) };
  });
  assert.deepEqual(sent.map((chunk) => chunk[0]), [1, 101, 201]);
  assert.equal(all.results.length, 250);
  assert.equal(all.refused, null);

  const calls: number[][] = [];
  const stopped = await requestFilesRemoval(ids, async (chunk) => {
    calls.push(chunk);
    return calls.length === 2 ? { error: "refused" } : { results: chunk.map((id) => result(id, "ACCEPTED")) };
  });
  assert.equal(calls.length, 2);
  assert.equal(stopped.results.length, 100);
  assert.equal(stopped.refused, "refused");
  assert.deepEqual(unansweredIds(ids, stopped.results), ids.slice(100));
});

test("every entry accepted reads as queued, not removed", () => {
  assert.equal(
    describeFilesRemoval(t, [result(1, "ACCEPTED"), result(2, "ACCEPTED")]),
    "Queued the files of 2 entries for removal",
  );
  assert.equal(describeFilesRemoval(t, [result(1, "ACCEPTED")]), "Queued the files of 1 entry for removal");
});

test("files already removed are counted quietly, not as refusals", () => {
  assert.equal(
    describeFilesRemoval(t, [result(1, "ACCEPTED"), result(2, "ALREADY_REMOVED")]),
    "Queued the files of 1 entry for removal · 1 already removed",
  );
  assert.equal(describeFilesRemoval(t, [result(2, "ALREADY_REMOVED")]), "1 already removed");
});

test("refusals name a few entries, then how many more", () => {
  const results = [
    result(1, "ACCEPTED"),
    result(2, "ALREADY_REMOVED"),
    result(3, "BUSY"),
    result(4, "NOT_FINISHED"),
    result(5, "NOT_FOUND"),
    result(6, "BUSY"),
    result(7, "NOT_FOUND"),
  ];
  assert.equal(
    describeFilesRemoval(t, results),
    "Queued 1 of 7 for removal · 1 already removed · #3 Busy · #4 Not finished · " +
      "#5 Not in history · and 2 more",
  );
});

test("entries never sent count against the whole request and stay picked", () => {
  const requested = Array.from({ length: 250 }, (_, index) => index + 1);
  // The first call of 100 answered; the second was refused.
  const answered = requested.slice(0, 100).map((id) => result(id, "ACCEPTED"));
  assert.equal(
    describeFilesRemoval(t, answered, requested.length),
    "Queued 100 of 250 for removal · 150 not sent, still selected",
  );
  assert.deepEqual(unansweredIds(requested, answered), requested.slice(100));
});

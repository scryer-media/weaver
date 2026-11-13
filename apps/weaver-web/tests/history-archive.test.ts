// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  describeFailedArchive,
  describeArchiveHistory,
  archiveHistoryChunks,
  requestArchiveHistory,
  rowDeletes,
  rowOperationKind,
  rowArchives,
  unansweredIds,
  type ArchiveHistoryAcceptOutcome,
  type ArchiveHistoryAcceptResult,
  type HistoryRowOperation,
} from "../src/next/data/history-archive.ts";
import { englishTranslate as t } from "./english-translate.ts";

const result = (id: number, outcome: ArchiveHistoryAcceptOutcome): ArchiveHistoryAcceptResult => ({ id, outcome });

const op = (
  action: HistoryRowOperation["action"],
  state: HistoryRowOperation["state"],
  extra: Partial<HistoryRowOperation> = {},
): HistoryRowOperation => ({ action, state, locked: state !== "FAILED", ...extra });

const row = (
  status: string,
  archivedAt: number | null = null,
  deleteOperation: HistoryRowOperation | null = null,
) => ({ status, archivedAt, deleteOperation });

test("a row's operation reads as a delete, a pending archive or a failed one", () => {
  assert.equal(rowOperationKind(null), null);
  for (const state of ["QUEUED", "RUNNING", "FAILED"] as const) {
    assert.equal(rowOperationKind(op("DELETE", state)), "deleting");
  }
  assert.equal(rowOperationKind(op("ARCHIVE", "QUEUED")), "archiving");
  assert.equal(rowOperationKind(op("ARCHIVE", "RUNNING")), "archiving");
  assert.equal(rowOperationKind(op("ARCHIVE", "FAILED")), "archiveFailed");
});

test("a row offers to archive it when finished, not archived, and free or last failed", () => {
  assert.equal(rowArchives(row("COMPLETE")), true);
  assert.equal(rowArchives(row("FAILED")), true);
  // Not finished.
  assert.equal(rowArchives(row("DOWNLOADING")), false);
  assert.equal(rowArchives(row("QUEUED")), false);
  // Already archived.
  assert.equal(rowArchives(row("COMPLETE", 1_767_225_600_000)), false);
  // An archive waiting or running.
  assert.equal(rowArchives(row("COMPLETE", null, op("ARCHIVE", "QUEUED"))), false);
  assert.equal(rowArchives(row("COMPLETE", null, op("ARCHIVE", "RUNNING"))), false);
  // An archive that failed is offered again.
  assert.equal(rowArchives(row("COMPLETE", null, op("ARCHIVE", "FAILED"))), true);
  // Any delete, in any state.
  for (const state of ["QUEUED", "RUNNING", "FAILED"] as const) {
    assert.equal(rowArchives(row("COMPLETE", null, op("DELETE", state))), false);
  }
});

test("a row offers to delete it unless an operation locks it, as picking does", () => {
  assert.equal(rowDeletes(row("COMPLETE")), true);
  assert.equal(rowDeletes(row("FAILED", 1_767_225_600_000)), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("DELETE", "FAILED"))), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("ARCHIVE", "FAILED"))), true);
  assert.equal(rowDeletes(row("COMPLETE", null, op("DELETE", "RUNNING"))), false);
  assert.equal(rowDeletes(row("COMPLETE", null, op("ARCHIVE", "QUEUED"))), false);
  assert.equal(rowDeletes(row("COMPLETE", null, op("ARCHIVE", "RUNNING"))), false);
});

test("a failed archive reads as its outcome, with the server's message when there is one", () => {
  assert.equal(
    describeFailedArchive(t, op("ARCHIVE", "FAILED", { archiveOutcome: "NOT_MANAGED" })),
    "Not archived · Not managed by Weaver",
  );
  assert.equal(
    describeFailedArchive(t, op("ARCHIVE", "FAILED", { archiveOutcome: "BUSY", errorMessage: "  " })),
    "Not archived · Busy",
  );
  assert.equal(
    describeFailedArchive(
      t,
      op("ARCHIVE", "FAILED", { archiveOutcome: "FAILED", errorMessage: "disk error" }),
    ),
    "Not archived · Failed: disk error",
  );
  // No outcome given reads as failed.
  assert.equal(describeFailedArchive(t, op("ARCHIVE", "FAILED")), "Not archived · Failed");
});

test("ids go out in calls of at most 100", () => {
  const ids = Array.from({ length: 250 }, (_, index) => index + 1);
  assert.deepEqual(
    archiveHistoryChunks(ids).map((chunk) => chunk.length),
    [100, 100, 50],
  );
});

test("a request sends every chunk in order and stops at the first refused", async () => {
  const ids = Array.from({ length: 250 }, (_, index) => index + 1);
  const sent: number[][] = [];
  const all = await requestArchiveHistory(ids, async (chunk) => {
    sent.push(chunk);
    return { results: chunk.map((id) => result(id, "ACCEPTED")) };
  });
  assert.deepEqual(sent.map((chunk) => chunk[0]), [1, 101, 201]);
  assert.equal(all.results.length, 250);
  assert.equal(all.refused, null);

  const calls: number[][] = [];
  const stopped = await requestArchiveHistory(ids, async (chunk) => {
    calls.push(chunk);
    return calls.length === 2 ? { error: "refused" } : { results: chunk.map((id) => result(id, "ACCEPTED")) };
  });
  assert.equal(calls.length, 2);
  assert.equal(stopped.results.length, 100);
  assert.equal(stopped.refused, "refused");
  assert.deepEqual(unansweredIds(ids, stopped.results), ids.slice(100));
});

test("every entry accepted reads as queued, not archived", () => {
  assert.equal(
    describeArchiveHistory(t, [result(1, "ACCEPTED"), result(2, "ACCEPTED")]),
    "Queued 2 entries to archive",
  );
  assert.equal(describeArchiveHistory(t, [result(1, "ACCEPTED")]), "Queued 1 entry to archive");
});

test("entries already archived are counted quietly, not as refusals", () => {
  assert.equal(
    describeArchiveHistory(t, [result(1, "ACCEPTED"), result(2, "ALREADY_ARCHIVED")]),
    "Queued 1 entry to archive · 1 already archived",
  );
  assert.equal(describeArchiveHistory(t, [result(2, "ALREADY_ARCHIVED")]), "1 already archived");
});

test("refusals name a few entries, then how many more", () => {
  const results = [
    result(1, "ACCEPTED"),
    result(2, "ALREADY_ARCHIVED"),
    result(3, "BUSY"),
    result(4, "NOT_FINISHED"),
    result(5, "NOT_FOUND"),
    result(6, "BUSY"),
    result(7, "NOT_FOUND"),
  ];
  assert.equal(
    describeArchiveHistory(t, results),
    "Queued 1 of 7 to archive · 1 already archived · #3 Busy · #4 Not finished · " +
      "#5 Not in history · and 2 more",
  );
});

test("entries never sent count against the whole request and stay picked", () => {
  const requested = Array.from({ length: 250 }, (_, index) => index + 1);
  // The first call of 100 answered; the second was refused.
  const answered = requested.slice(0, 100).map((id) => result(id, "ACCEPTED"));
  assert.equal(
    describeArchiveHistory(t, answered, requested.length),
    "Queued 100 of 250 to archive · 150 not sent, still selected",
  );
  assert.deepEqual(unansweredIds(requested, answered), requested.slice(100));
});

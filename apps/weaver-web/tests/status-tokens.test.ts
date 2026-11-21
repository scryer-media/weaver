// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { isActiveStatus, progressDisplayKind, statusI18nKey, statusToken } from "../src/lib/status-tokens.ts";

test("every queue API state has an explicit first-party presentation", () => {
  const cases = [
    ["QUEUED", "queued", "status.queued"],
    ["PROPAGATING", "queued", "status.propagating"],
    ["DOWNLOADING", "downloading", "status.downloading"],
    ["FETCHING_REPAIR_DATA", "repairing", "status.fetchingRepairData"],
    ["FINALIZING_DOWNLOAD", "downloading", "timeline.finalizingDownload"],
    ["CHECKING", "verifying", "status.verifying"],
    ["VERIFYING", "verifying", "status.verifying"],
    ["REPAIRING", "repairing", "status.repairing"],
    ["EXTRACTING", "extracting", "status.extracting"],
    ["FINALIZING", "copying", "status.finalizing"],
    ["POST_PROCESSING", "copying", "status.postProcessing"],
    ["COMPLETED", "completed", "status.complete"],
    ["FAILED", "failed", "status.failed"],
    ["PAUSED", "paused", "status.paused"],
    ["FETCHING_METADATA", "downloading", "status.fetchingMetadata"],
    ["CHECKING_FILES", "verifying", "status.checkingFiles"],
    ["SEEDING", "completed", "status.seeding"],
    ["STALLED", "downloading", "status.stalled"],
  ] as const;

  for (const [status, token, key] of cases) {
    assert.equal(statusToken(status), token);
    assert.equal(statusI18nKey(status), key);
  }
});

test("synthetic download stages remain part of the active queue filter", () => {
  assert.equal(isActiveStatus("FETCHING_REPAIR_DATA"), true);
  assert.equal(isActiveStatus("FINALIZING_DOWNLOAD"), true);
});

test("a job waiting on queue scripts counts as active, as the queue summary does", () => {
  assert.equal(isActiveStatus("AWAITING_QUEUE_SCRIPTS"), true);
});

test("a seeding torrent is complete and still active; a stalled one is downloading but idle", () => {
  assert.equal(isActiveStatus("SEEDING"), true);
  assert.equal(progressDisplayKind("SEEDING", 1), "complete");
  assert.equal(isActiveStatus("STALLED"), false);
  assert.equal(progressDisplayKind("STALLED", 0.4), "determinate");
});

test("fetching a torrent's metadata and checking its files are active and have no percentage yet", () => {
  for (const status of ["FETCHING_METADATA", "CHECKING_FILES"]) {
    assert.equal(isActiveStatus(status), true);
    assert.equal(progressDisplayKind(status, 0), "indeterminate");
  }
});

import assert from "node:assert/strict";
import test from "node:test";
import { normalizeJobData, type GraphqlJobData } from "../src/lib/job-types.ts";
import { isActiveStatus, progressDisplayKind, statusI18nKey } from "../src/lib/status-tokens.ts";

function job(status: string, reason: string | null) {
  return normalizeJobData({
    id: 1,
    status,
    downloadWaitReason: reason,
    downloadRetryAtEpochMs: reason ? Date.now() + 300_000 : null,
    progressPercent: 0,
  } as GraphqlJobData);
}

test("a propagation hold is visible without looking like active downloading", () => {
  for (const status of ["QUEUED", "DOWNLOADING"]) {
    const held = job(status, "propagation_delay");
    assert.equal(held.status, "PROPAGATING");
    assert.equal(statusI18nKey(held.status), "status.propagating");
    assert.equal(isActiveStatus(held.status), false);
    assert.equal(progressDisplayKind(held.status, held.progress), "empty");
    assert.ok(held.downloadRetryAtEpochMs! > Date.now());
  }
});

test("the next API snapshot clears the propagation label when dispatch resumes", () => {
  assert.equal(job("DOWNLOADING", null).status, "DOWNLOADING");
  assert.equal(job("QUEUED", null).status, "QUEUED");
});

test("pause, terminal states, and unrelated retry reasons are not propagation holds", () => {
  assert.equal(job("PAUSED", "propagation_delay").status, "PAUSED");
  assert.equal(job("COMPLETED", "propagation_delay").status, "COMPLETE");
  assert.equal(job("DOWNLOADING", "network_error").status, "DOWNLOADING");
});

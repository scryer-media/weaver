import assert from "node:assert/strict";
import test from "node:test";
import { statusI18nKey, statusToken } from "../src/lib/status-tokens.ts";

test("every queue API state has an explicit first-party presentation", () => {
  const cases = [
    ["QUEUED", "queued", "status.queued"],
    ["DOWNLOADING", "downloading", "status.downloading"],
    ["CHECKING", "verifying", "status.verifying"],
    ["VERIFYING", "verifying", "status.verifying"],
    ["REPAIRING", "repairing", "status.repairing"],
    ["EXTRACTING", "extracting", "status.extracting"],
    ["FINALIZING", "copying", "status.finalizing"],
    ["POST_PROCESSING", "copying", "status.postProcessing"],
    ["COMPLETED", "completed", "status.complete"],
    ["FAILED", "failed", "status.failed"],
    ["PAUSED", "paused", "status.paused"],
  ] as const;

  for (const [status, token, key] of cases) {
    assert.equal(statusToken(status), token);
    assert.equal(statusI18nKey(status), key);
  }
});

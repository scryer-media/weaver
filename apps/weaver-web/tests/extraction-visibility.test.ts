import assert from "node:assert/strict";
import test from "node:test";
import {
  extractionPresentationStatus,
  extractionVisibilityIdentity,
} from "../src/lib/hooks/use-extraction-visibility.ts";
import type { JobPhaseProgressData } from "../src/lib/job-types.ts";

function phase(
  kind: JobPhaseProgressData["phase"],
  completedBytes: number,
  totalBytes: number,
  startedAtEpochMs: number,
): JobPhaseProgressData {
  return {
    phase: kind,
    completedBytes,
    totalBytes,
    progressPercent: totalBytes === 0 ? 0 : (completedBytes / totalBytes) * 100,
    startedAtEpochMs,
    updatedAtEpochMs: startedAtEpochMs,
  };
}

test("in-progress extraction becomes eligible for delayed display", () => {
  const phases = [phase("DOWNLOADING", 20, 100, 1), phase("EXTRACTING", 30, 100, 2)];

  assert.equal(extractionVisibilityIdentity(phases), 2);
  assert.equal(extractionPresentationStatus("EXTRACTING", phases, false), "DOWNLOADING");
  assert.equal(extractionPresentationStatus("EXTRACTING", phases, true), "EXTRACTING");
});

test("streaming extraction waiting at 100 percent stays behind its active download", () => {
  const phases = [phase("DOWNLOADING", 20, 100, 1), phase("EXTRACTING", 100, 100, 2)];

  assert.equal(extractionVisibilityIdentity(phases), null);
  assert.equal(extractionPresentationStatus("EXTRACTING", phases, false), "DOWNLOADING");
});

test("standalone extraction remains visible through completion", () => {
  const phases = [phase("EXTRACTING", 100, 100, 2)];

  assert.equal(extractionVisibilityIdentity(phases), 2);
  assert.equal(extractionPresentationStatus("EXTRACTING", phases, false), "EXTRACTING");
});

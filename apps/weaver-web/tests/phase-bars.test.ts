import assert from "node:assert/strict";
import test from "node:test";
import {
  NO_PROGRESS_STATE,
  PHASE_SETTLE_MS,
  advanceJobProgress,
  latestPhase,
  ratePhase,
  runningPhases,
  settlePhases,
  settleStatus,
  type PhaseClocks,
} from "../src/next/data/phase-bars.ts";
import type { JobPhaseProgressData } from "../src/lib/job-types.ts";

function phase(
  kind: JobPhaseProgressData["phase"],
  completedBytes: number,
  totalBytes = 100,
): JobPhaseProgressData {
  return {
    phase: kind,
    completedBytes,
    totalBytes,
    progressPercent: totalBytes === 0 ? 0 : (completedBytes / totalBytes) * 100,
    startedAtEpochMs: 1,
    updatedAtEpochMs: 1,
  };
}

const NONE: PhaseClocks = new Map();

/** Feed a sequence of (time, running phases) samples and return the bars after each. */
function run(samples: [number, JobPhaseProgressData[]][]) {
  let clocks = NONE;
  return samples.map(([now, running]) => {
    const settled = settlePhases(clocks, running, now);
    clocks = settled.clocks;
    return settled;
  });
}

const kinds = (bars: JobPhaseProgressData[]) => bars.map((bar) => bar.phase);

test("a phase gets its bar only after it has run for the settling time", () => {
  const [start, early, settled] = run([
    [0, [phase("DOWNLOADING", 10)]],
    [PHASE_SETTLE_MS - 1, [phase("DOWNLOADING", 20)]],
    [PHASE_SETTLE_MS, [phase("DOWNLOADING", 30)]],
  ]);

  assert.deepEqual(kinds(start.bars), []);
  assert.equal(start.nextChangeAt, PHASE_SETTLE_MS);
  assert.deepEqual(kinds(early.bars), []);
  assert.deepEqual(kinds(settled.bars), ["DOWNLOADING"]);
  assert.equal(settled.bars[0].completedBytes, 30);
  assert.equal(settled.nextChangeAt, null);
});

test("an extraction shorter than the settling time never reaches the queue", () => {
  const results = run([
    [0, [phase("DOWNLOADING", 10)]],
    [3_000, [phase("DOWNLOADING", 40), phase("EXTRACTING", 5)]],
    [4_500, [phase("DOWNLOADING", 60)]],
    [4_600, [phase("DOWNLOADING", 61)]],
    [9_000, [phase("DOWNLOADING", 90)]],
  ]);

  for (const result of results.slice(1)) {
    assert.deepEqual(kinds(result.bars), ["DOWNLOADING"]);
  }
});

test("a second and third bar stack in pipeline order once each has settled", () => {
  const results = run([
    [0, [phase("DOWNLOADING", 10)]],
    [2_000, [phase("EXTRACTING", 1), phase("DOWNLOADING", 20)]],
    [4_000, [phase("MOVING", 1), phase("EXTRACTING", 50), phase("DOWNLOADING", 40)]],
    [6_000, [phase("MOVING", 5), phase("EXTRACTING", 70), phase("DOWNLOADING", 60)]],
  ]);

  assert.deepEqual(kinds(results[1].bars), ["DOWNLOADING"]);
  assert.deepEqual(kinds(results[2].bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.deepEqual(kinds(results[3].bars), ["DOWNLOADING", "EXTRACTING", "MOVING"]);
});

test("a finished phase holds its last progress for the settling time, then goes", () => {
  const [, , ended, held, gone] = run([
    [0, [phase("DOWNLOADING", 50), phase("EXTRACTING", 10)]],
    [2_000, [phase("DOWNLOADING", 60), phase("EXTRACTING", 98)]],
    [2_500, [phase("DOWNLOADING", 65)]],
    [4_499, [phase("DOWNLOADING", 80)]],
    [4_500, [phase("DOWNLOADING", 81)]],
  ]);

  assert.deepEqual(kinds(ended.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.equal(ended.bars[1].completedBytes, 98);
  assert.equal(ended.nextChangeAt, 4_500);
  assert.deepEqual(kinds(held.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.deepEqual(kinds(gone.bars), ["DOWNLOADING"]);
});

test("a held bar whose phase resumes keeps its place instead of settling again", () => {
  const [, shown, paused, resumed] = run([
    [0, [phase("EXTRACTING", 10)]],
    [2_000, [phase("EXTRACTING", 20)]],
    [2_100, []],
    [3_000, [phase("EXTRACTING", 30)]],
  ]);

  assert.deepEqual(kinds(shown.bars), ["EXTRACTING"]);
  assert.deepEqual(kinds(paused.bars), ["EXTRACTING"]);
  assert.deepEqual(kinds(resumed.bars), ["EXTRACTING"]);
  assert.equal(resumed.bars[0].completedBytes, 30);
});

test("an extraction waiting at 100% for the next volume is not running", () => {
  const phases = [phase("DOWNLOADING", 40), phase("EXTRACTING", 100)];
  assert.deepEqual(kinds(runningPhases(phases)), ["DOWNLOADING"]);

  const afterDownload = [phase("DOWNLOADING", 100), phase("EXTRACTING", 100)];
  assert.deepEqual(kinds(runningPhases(afterDownload)), ["DOWNLOADING", "EXTRACTING"]);

  assert.deepEqual(kinds(runningPhases([phase("REPAIRING", 0, 0)])), []);
});

test("a status label follows the pipeline only once the new status has held", () => {
  let clock = settleStatus(null, "DOWNLOADING", 0).clock;
  assert.equal(clock.shown, "DOWNLOADING");

  const moved = settleStatus(clock, "EXTRACTING", 1_000);
  assert.equal(moved.clock.shown, "DOWNLOADING");
  assert.equal(moved.nextChangeAt, 3_000);

  // Back before it settled: the brief extraction never reached the label.
  clock = settleStatus(moved.clock, "DOWNLOADING", 2_000).clock;
  assert.equal(clock.shown, "DOWNLOADING");
  assert.equal(clock.pending, null);

  clock = settleStatus(clock, "MOVING", 5_000).clock;
  clock = settleStatus(clock, "MOVING", 7_000).clock;
  assert.equal(clock.shown, "MOVING");
});

test("a pause, a queue slot or an outcome changes the label at once", () => {
  const downloading = settleStatus(null, "DOWNLOADING", 0).clock;
  assert.equal(settleStatus(downloading, "PAUSED", 10).clock.shown, "PAUSED");

  const paused = settleStatus(null, "PAUSED", 0).clock;
  assert.equal(settleStatus(paused, "DOWNLOADING", 10).clock.shown, "DOWNLOADING");

  const extracting = settleStatus(null, "EXTRACTING", 0).clock;
  assert.equal(settleStatus(extracting, "COMPLETED", 10).clock.shown, "COMPLETED");
  assert.equal(settleStatus(extracting, "FAILED", 10).clock.shown, "FAILED");
});

test("a page opened mid-phase shows the phases weaver says have already run", () => {
  const long: JobPhaseProgressData = { ...phase("DOWNLOADING", 50), startedAtEpochMs: 1_000, updatedAtEpochMs: 60_000 };
  const fresh: JobPhaseProgressData = { ...phase("EXTRACTING", 5), startedAtEpochMs: 59_500, updatedAtEpochMs: 60_000 };
  const state = advanceJobProgress(NO_PROGRESS_STATE, "EXTRACTING", [long, fresh], 5);

  assert.deepEqual(kinds(state.bars), ["DOWNLOADING"]);
  // The download is what the job is visibly doing until the extraction settles.
  assert.equal(state.status?.shown, "DOWNLOADING");
  assert.equal(state.nextChangeAt, 5 + 1_500);

  const later = advanceJobProgress(state, "EXTRACTING", [long, fresh], 5 + 1_500);
  // The extraction has waited once for its bar; the label does not make it wait again.
  assert.deepEqual(kinds(later.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.equal(later.status?.shown, "EXTRACTING");
  assert.equal(later.nextChangeAt, null);
});

test("rate and time left come from a running download, not an extraction that reported with it", () => {
  const download = { ...phase("DOWNLOADING", 40), rateBps: 12_000_000, updatedAtEpochMs: 5 };
  const extraction = { ...phase("EXTRACTING", 10), rateBps: null, updatedAtEpochMs: 5 };

  assert.equal(latestPhase([download, extraction]), extraction);
  assert.equal(ratePhase([download, extraction]), download);
});

test("once the download is done, the rate is quoted from the phase still working", () => {
  const download = { ...phase("DOWNLOADING", 100), updatedAtEpochMs: 4 };
  const extraction = { ...phase("EXTRACTING", 60), rateBps: 80_000_000, updatedAtEpochMs: 9 };

  assert.equal(ratePhase([download, extraction]), extraction);
  assert.equal(ratePhase([]), null);
});

test("an extraction still running after the last article stacks under the download bar", () => {
  const download = { completedBytes: 1_000, totalBytes: 1_000, progressPercent: 100 };
  const extraction = { ...phase("EXTRACTING", 30), startedAtEpochMs: 1_000, updatedAtEpochMs: 9_000 };
  // Weaver keeps the job DOWNLOADING while download-side work lingers, and
  // no longer reports the download phase at all.
  const state = advanceJobProgress(NO_PROGRESS_STATE, "DOWNLOADING", [extraction], 5, download);

  assert.deepEqual(kinds(state.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.equal(state.bars[0].progressPercent, 100);
  assert.equal(state.status?.shown, "EXTRACTING");
});

test("a download that has just gone quiet keeps the label until its bar has gone", () => {
  const download = { completedBytes: 600, totalBytes: 1_000, progressPercent: 60 };
  let state = advanceJobProgress(NO_PROGRESS_STATE, "DOWNLOADING", [phase("DOWNLOADING", 50)], 0, download);
  state = advanceJobProgress(state, "DOWNLOADING", [phase("DOWNLOADING", 55), phase("EXTRACTING", 10)], 2_000, download);
  state = advanceJobProgress(state, "DOWNLOADING", [phase("DOWNLOADING", 60), phase("EXTRACTING", 20)], 4_000, download);
  assert.deepEqual(kinds(state.bars), ["DOWNLOADING", "EXTRACTING"]);

  // The download phase drops out for a moment; its bar is held, so the label waits.
  const quiet = advanceJobProgress(state, "DOWNLOADING", [phase("EXTRACTING", 30)], 4_500, download);
  assert.deepEqual(kinds(quiet.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.equal(quiet.status?.shown, "DOWNLOADING");

  const gone = advanceJobProgress(quiet, "DOWNLOADING", [phase("EXTRACTING", 40)], 6_500, download);
  assert.deepEqual(kinds(gone.bars), ["DOWNLOADING", "EXTRACTING"]);
  assert.equal(gone.bars[0].progressPercent, 60);
  assert.equal(gone.status?.shown, "EXTRACTING");
});

test("with no later phase running there is no stack to hold the download bar up", () => {
  const download = { completedBytes: 1_000, totalBytes: 1_000, progressPercent: 100 };
  const state = advanceJobProgress(NO_PROGRESS_STATE, "VERIFYING", [], 5, download);
  assert.deepEqual(kinds(state.bars), []);
});

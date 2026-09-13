import type { JobPhase, JobPhaseProgressData } from "@/lib/job-types";

/**
 * Which of a job's pipeline phases get a progress bar, and when.
 *
 * A job can be in more than one phase at once: weaver extracts a RAR set while
 * the rest of the release is still downloading, so a queue row carries a bar per
 * running phase rather than one for the whole job. Phases also come and go fast.
 * A small archive can extract in under a second, and a bar for every one of
 * those would make the list jump. So a phase earns its bar only after it has
 * run for `PHASE_SETTLE_MS`, and keeps it for as long again after it ends. A
 * phase shorter than that never reaches the queue at all; the job's timeline is
 * where it is recorded.
 *
 * This file is the clock and nothing else, so it can be tested without React.
 */

export const PHASE_SETTLE_MS = 2_000;

/** Pipeline order, which is also the order the bars stack in. */
export const PHASE_ORDER: readonly JobPhase[] = ["DOWNLOADING", "REPAIRING", "EXTRACTING", "MOVING"];

/**
 * The phases doing work right now.
 *
 * An extraction that has caught up with the volumes downloaded so far sits at
 * 100% while it waits for the next one. That wait is the download's time, not
 * the extraction's, so it does not count as a running extraction.
 */
export function runningPhases(phases: readonly JobPhaseProgressData[]): JobPhaseProgressData[] {
  const downloading = phases.some(
    (phase) =>
      phase.phase === "DOWNLOADING" && phase.totalBytes > 0 && phase.completedBytes < phase.totalBytes,
  );
  return phases.filter(
    (phase) =>
      phase.totalBytes > 0
      && !(phase.phase === "EXTRACTING" && downloading && phase.completedBytes >= phase.totalBytes),
  );
}

export interface PhaseClock {
  /** When the phase was first seen running in its current run. */
  since: number;
  /** When it stopped being seen, while its bar is still held; null while it runs. */
  endedAt: number | null;
  /** Whether it has been running long enough to have a bar. */
  shown: boolean;
  /** Its progress the last time it was seen, which a held bar keeps drawing. */
  last: JobPhaseProgressData;
}

export type PhaseClocks = ReadonlyMap<JobPhase, PhaseClock>;

export interface SettledPhases {
  clocks: PhaseClocks;
  /** The phases to draw, in pipeline order, each with its latest progress. */
  bars: JobPhaseProgressData[];
  /** When the answer next changes with no new data, or null if it will not. */
  nextChangeAt: number | null;
}

/** Advance the clocks to `now`, given the phases running at that moment. */
export function settlePhases(
  previous: PhaseClocks,
  running: readonly JobPhaseProgressData[],
  now: number,
): SettledPhases {
  const clocks = new Map<JobPhase, PhaseClock>();
  const bars: JobPhaseProgressData[] = [];
  let nextChangeAt: number | null = null;
  const due = (at: number) => {
    nextChangeAt = nextChangeAt === null ? at : Math.min(nextChangeAt, at);
  };

  for (const phase of PHASE_ORDER) {
    const current = running.find((entry) => entry.phase === phase);
    const clock = previous.get(phase);

    if (current) {
      // A held bar whose phase comes back keeps its bar; anything else that was
      // not already running starts its settling time now.
      const since = clock && (clock.endedAt === null || clock.shown) ? clock.since : now;
      const shown = (clock?.shown ?? false) || now - since >= PHASE_SETTLE_MS;
      clocks.set(phase, { since, endedAt: null, shown, last: current });
      if (shown) {
        bars.push(current);
      } else {
        due(since + PHASE_SETTLE_MS);
      }
      continue;
    }

    // Not running. A phase that never earned a bar is simply forgotten; one that
    // did keeps it until it has been gone for the settling time.
    if (!clock?.shown) {
      continue;
    }
    const endedAt = clock.endedAt ?? now;
    if (now - endedAt >= PHASE_SETTLE_MS) {
      continue;
    }
    clocks.set(phase, { ...clock, endedAt });
    bars.push(clock.last);
    due(endedAt + PHASE_SETTLE_MS);
  }

  return { clocks, bars, nextChangeAt };
}

/**
 * Statuses the pipeline moves between on its own.
 *
 * A change between two of these is the engine at work, and it waits out the
 * settling time before the label follows, for the same reason the bars do. A
 * change to or from anything else (a pause, a queue slot, an outcome) is the
 * answer to something the user did or is waiting on, so it shows at once.
 */
const WORKING_STATUSES = new Set([
  "DOWNLOADING",
  "FETCHING_REPAIR_DATA",
  "FINALIZING_DOWNLOAD",
  "CHECKING",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "POST_PROCESSING",
  "MOVING",
  "FINALIZING",
]);

export interface StatusClock {
  /** The status the label shows. */
  shown: string;
  /** A working status the job has moved to, while it settles. */
  pending: string | null;
  since: number;
}

/**
 * Advance a status label to `now`.
 *
 * `settled` says the new status has already waited out its time somewhere else
 * (its phase has a bar), so the label does not make it wait a second time.
 */
export function settleStatus(
  previous: StatusClock | null,
  status: string,
  now: number,
  settled = false,
): { clock: StatusClock; nextChangeAt: number | null } {
  if (
    previous === null
    || settled
    || status === previous.shown
    || !WORKING_STATUSES.has(status)
    || !WORKING_STATUSES.has(previous.shown)
  ) {
    return { clock: { shown: status, pending: null, since: now }, nextChangeAt: null };
  }
  const since = status === previous.pending ? previous.since : now;
  if (now - since >= PHASE_SETTLE_MS) {
    return { clock: { shown: status, pending: null, since: now }, nextChangeAt: null };
  }
  return {
    clock: { shown: previous.shown, pending: status, since },
    nextChangeAt: since + PHASE_SETTLE_MS,
  };
}

/** Everything a job's progress display holds between one update and the next. */
export interface JobProgressState {
  phases: PhaseClocks;
  status: StatusClock | null;
  /** The bars to draw, in pipeline order. */
  bars: JobPhaseProgressData[];
  nextChangeAt: number | null;
}

export const NO_PROGRESS_STATE: JobProgressState = {
  phases: new Map(),
  status: null,
  bars: [],
  nextChangeAt: null,
};

/**
 * Advance a job's progress display to `now`.
 *
 * On the first update a display sees (a page opened on a job that is already
 * well into a phase), a phase is credited with the time weaver says it has
 * already run. Both ends of that measure are the server's, so it does not
 * depend on the browser's clock agreeing with it.
 */
export function advanceJobProgress(
  previous: JobProgressState,
  status: string,
  phases: readonly JobPhaseProgressData[],
  now: number,
): JobProgressState {
  const first = previous.status === null;
  let clocks = previous.phases;
  const running = runningPhases(phases);
  if (first) {
    const seeded = new Map<JobPhase, PhaseClock>();
    for (const phase of running) {
      const ran = Math.max(0, phase.updatedAtEpochMs - phase.startedAtEpochMs);
      seeded.set(phase.phase, {
        since: now - ran,
        endedAt: null,
        shown: ran >= PHASE_SETTLE_MS,
        last: phase,
      });
    }
    clocks = seeded;
  }
  const settled = settlePhases(clocks, running, now);

  // Weaver reports an archive that is extracting while the rest of the release
  // downloads as EXTRACTING. Until that extraction has a bar of its own, the
  // download is what the job is visibly doing, so that is what the label says.
  const extractionShown = settled.bars.some((bar) => bar.phase === "EXTRACTING");
  const downloading = phases.some(
    (phase) =>
      phase.phase === "DOWNLOADING" && phase.totalBytes > 0 && phase.completedBytes < phase.totalBytes,
  );
  const presented = status === "EXTRACTING" && !extractionShown && downloading ? "DOWNLOADING" : status;
  const label = settleStatus(
    first ? null : previous.status,
    presented,
    now,
    settled.bars.some((bar) => bar.phase === presented),
  );

  const due = [settled.nextChangeAt, label.nextChangeAt].filter((at): at is number => at !== null);
  return {
    phases: settled.clocks,
    status: label.clock,
    bars: settled.bars,
    nextChangeAt: due.length === 0 ? null : Math.min(...due),
  };
}

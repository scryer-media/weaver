import { useEffect, useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import type { JobData, JobPhase, JobPhaseProgressData } from "@/lib/job-types";
import { cn } from "@/lib/utils";
import { Bar } from "./chrome";
import { statusColor } from "../data/palette";
import { useStatusLabel } from "../data/status";
import {
  advanceJobProgress,
  NO_PROGRESS_STATE,
  type JobProgressState,
} from "../data/phase-bars";

/**
 * A job's progress as the list, the inspector and the job screen draw it: one
 * bar per pipeline phase that is running, with a status label that keeps pace.
 *
 * The timing rules live in `phase-bars.ts`; this is the part that holds them
 * in React state and wakes up when the answer is due to change by itself.
 */

export interface JobProgressView {
  /** The status to label the job with, which trails a brief change. */
  status: string;
  /** The phases that have a bar, in pipeline order. None falls back to one bar for the job. */
  bars: JobPhaseProgressData[];
}

interface HeldState extends JobProgressState {
  jobId: number;
}

function start(job: Pick<JobData, "id" | "status" | "phaseProgress">): HeldState {
  return {
    ...advanceJobProgress(NO_PROGRESS_STATE, job.status, job.phaseProgress, Date.now()),
    jobId: job.id,
  };
}

export function useJobProgress(job: Pick<JobData, "id" | "status" | "phaseProgress">): JobProgressView {
  const { id, status, phaseProgress } = job;
  const [state, setState] = useState<HeldState>(() => start(job));

  useEffect(() => {
    setState((previous) =>
      previous.jobId === id
        ? { ...advanceJobProgress(previous, status, phaseProgress, Date.now()), jobId: id }
        : start({ id, status, phaseProgress }),
    );
  }, [id, status, phaseProgress]);

  const { nextChangeAt } = state;
  useEffect(() => {
    if (nextChangeAt === null) {
      return;
    }
    const timer = window.setTimeout(() => {
      setState((previous) =>
        previous.jobId === id
          ? { ...advanceJobProgress(previous, status, phaseProgress, Date.now()), jobId: id }
          : previous,
      );
    }, Math.max(0, nextChangeAt - Date.now()));
    return () => window.clearTimeout(timer);
  }, [id, nextChangeAt, status, phaseProgress]);

  // The render that switches to another job has not been through the effect
  // yet; it shows that job as reported rather than the last one's bars.
  if (state.jobId !== id) {
    return { status, bars: [] };
  }
  return { status: state.status?.shown ?? status, bars: state.bars };
}

const PHASE_LABEL_KEY: Record<JobPhase, string> = {
  DOWNLOADING: "phase.downloading",
  REPAIRING: "phase.repairing",
  EXTRACTING: "phase.extracting",
  MOVING: "phase.moving",
};

/**
 * The bars themselves.
 *
 * A lone bar needs no name, since the status beside it already says what it
 * is; a second or third one names every line, because two bars side by side
 * with no labels are a puzzle. Names sit after the percentage, so a bar does
 * not move sideways when a second phase joins it, and every name in a stack
 * takes the width of the longest, so the bars above and below end together.
 */
export function PhaseBars({
  job,
  view,
  height,
  barClassName,
  percentClassName,
  decimals = 0,
  className,
}: {
  job: Pick<JobData, "progress">;
  view: JobProgressView;
  height: number;
  barClassName?: string;
  percentClassName?: string;
  decimals?: number;
  className?: string;
}) {
  const t = useTranslate();
  const statusLabel = useStatusLabel();
  const lines =
    view.bars.length === 0
      ? [
          {
            key: "job",
            percent: job.progress * 100,
            color: statusColor(view.status),
            name: statusLabel(view.status),
            labelled: false,
          },
        ]
      : view.bars.map((bar) => ({
          key: bar.phase,
          percent: bar.progressPercent,
          color: statusColor(bar.phase),
          name: t(PHASE_LABEL_KEY[bar.phase]),
          labelled: view.bars.length > 1,
        }));

  // The names are set in a monospace face, where a character is one `ch`.
  const nameWidth = Math.max(0, ...lines.map((line) => (line.labelled ? [...line.name].length : 0)));

  return (
    <div className={cn("flex min-w-0 flex-col gap-[7px]", className)}>
      {lines.map((line) => {
        const percent = Number.isFinite(line.percent) ? Math.max(0, Math.min(100, line.percent)) : 0;
        return (
          <div key={line.key} className="flex min-w-0 items-center gap-3">
            <Bar
              percent={percent}
              color={line.color}
              height={height}
              className={barClassName}
              label={line.name}
            />
            <span aria-hidden="true" className={cn("flex-none font-wv-mono", percentClassName)}>
              {percent.toFixed(decimals)}%
            </span>
            {line.labelled ? (
              <span
                aria-hidden="true"
                className="min-w-0 truncate font-wv-mono text-[11px] text-wv-faint"
                style={{ flexBasis: `${nameWidth}ch` }}
              >
                {line.name}
              </span>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

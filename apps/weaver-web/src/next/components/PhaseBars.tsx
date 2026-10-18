// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, useEffect, useMemo, useRef, useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import type { JobData, JobPhase, JobPhaseProgressData } from "@/lib/job-types";
import { cn } from "@/lib/utils";
import { Bar } from "./chrome";
import { statusColor } from "../data/palette";
import { useStatusLabel } from "../data/status";
import {
  advanceJobProgress,
  NO_PROGRESS_STATE,
  type JobDownloadTotals,
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

const NO_BARS: JobPhaseProgressData[] = [];

type ProgressJob = Pick<JobData, "id" | "status" | "phaseProgress"> &
  Partial<Pick<JobData, "downloadedBytes" | "totalBytes" | "progress">>;

function downloadTotals(job: ProgressJob): JobDownloadTotals | null {
  return job.totalBytes === undefined
    ? null
    : {
        completedBytes: job.downloadedBytes ?? 0,
        totalBytes: job.totalBytes,
        progressPercent: (job.progress ?? 0) * 100,
      };
}

function start(job: ProgressJob): HeldState {
  return {
    ...advanceJobProgress(NO_PROGRESS_STATE, job.status, job.phaseProgress, Date.now(), downloadTotals(job)),
    jobId: job.id,
  };
}

export function useJobProgress(job: ProgressJob): JobProgressView {
  const { id, status, phaseProgress, downloadedBytes, totalBytes, progress } = job;
  const [state, setState] = useState<HeldState>(() => start(job));
  // The first render's state already holds what it was given, and the same
  // answer computed again would draw the row twice; time is the timer's job.
  const started = useRef(true);

  useEffect(() => {
    if (started.current) {
      started.current = false;
      return;
    }
    const current = { id, status, phaseProgress, downloadedBytes, totalBytes, progress };
    setState((previous) =>
      previous.jobId === id
        ? {
            ...advanceJobProgress(previous, status, phaseProgress, Date.now(), downloadTotals(current)),
            jobId: id,
          }
        : start(current),
    );
  }, [id, status, phaseProgress, downloadedBytes, totalBytes, progress]);

  const { nextChangeAt } = state;
  useEffect(() => {
    if (nextChangeAt === null) {
      return;
    }
    const current = { id, status, phaseProgress, downloadedBytes, totalBytes, progress };
    const timer = window.setTimeout(() => {
      setState((previous) =>
        previous.jobId === id
          ? {
              ...advanceJobProgress(previous, status, phaseProgress, Date.now(), downloadTotals(current)),
              jobId: id,
            }
          : previous,
      );
    }, Math.max(0, nextChangeAt - Date.now()));
    return () => window.clearTimeout(timer);
  }, [id, nextChangeAt, status, phaseProgress, downloadedBytes, totalBytes, progress]);

  // The render that switches to another job has not been through the effect
  // yet; it shows that job as reported rather than the last one's bars.
  const switching = state.jobId !== id;
  const shown = switching ? status : (state.status?.shown ?? status);
  const bars = switching ? NO_BARS : state.bars;
  // One view for as long as it says the same thing, so a row drawn again for
  // its estimate alone leaves its bars be.
  return useMemo(() => ({ status: shown, bars }), [shown, bars]);
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
export const PhaseBars = memo(function PhaseBars({
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
            // A finished job is whole even when it never needed every posted
            // byte (repair blocks it had no use for), so it reads full rather
            // than as a download that stopped short.
            percent: view.status === "COMPLETE" ? 100 : job.progress * 100,
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
});

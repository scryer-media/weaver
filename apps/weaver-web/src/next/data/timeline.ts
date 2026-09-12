import type { WaterfallStage } from "../components/Waterfall";
import { formatClockSeconds, formatSpan } from "./format";
import { WV } from "./palette";

/**
 * A job's pipeline, turned into the waterfall's own terms.
 *
 * The server reports lanes with absolute timestamps; the waterfall works in
 * percentages of the job's window, because the whole point of the view is that
 * a 34s download and a 0.4s move share one axis. That conversion lives here
 * rather than in the screen, so a second screen that wants the same picture —
 * an inspector, say — gets it from the same place.
 *
 * Only lanes the server actually reports are drawn. The prototype shows the
 * stages a job has not reached yet as pending rows, but weaver cannot know
 * whether a given job will ever verify, repair or extract, and a permanently
 * pending "Repairing" row on a healthy job would be a claim, not a placeholder.
 */

export interface TimelineSpan {
  startedAt: number;
  endedAt: number | null;
  state: "RUNNING" | "COMPLETE" | "FAILED";
  label: string | null;
}

export interface TimelineLane {
  stage: string;
  spans: TimelineSpan[];
}

export interface JobTimelineData {
  startedAt: number;
  endedAt: number | null;
  outcome: string;
  lanes: TimelineLane[];
}

/** Canonical pipeline order, which is not the order the lanes arrive in. */
const STAGE_ORDER = [
  "PENDING_DOWNLOAD",
  "DOWNLOADING",
  "PAUSED",
  "FINALIZING_DOWNLOAD",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "FINAL_MOVE",
  "INTERRUPTED",
] as const;

const STAGE_LABEL: Record<string, string> = {
  PENDING_DOWNLOAD: "Waiting to start",
  DOWNLOADING: "Downloading",
  PAUSED: "Paused",
  FINALIZING_DOWNLOAD: "Finalising",
  VERIFYING: "Verifying",
  REPAIRING: "Repairing",
  EXTRACTING: "Extracting",
  FINAL_MOVE: "Final move",
  INTERRUPTED: "Interrupted",
};

const STAGE_COLOR: Record<string, string> = {
  PENDING_DOWNLOAD: WV.idle,
  DOWNLOADING: WV.info,
  PAUSED: WV.warn,
  FINALIZING_DOWNLOAD: WV.info,
  VERIFYING: WV.accent,
  REPAIRING: WV.info,
  EXTRACTING: WV.violet,
  FINAL_MOVE: WV.accent,
  INTERRUPTED: WV.error,
};

export interface TimelineView {
  stages: WaterfallStage[];
  ticks: string[];
  window: string;
  total: string;
  /** `4 stages · 36s total`, or where a running job has got to. */
  note: string;
  running: boolean;
}

export function buildTimelineView(
  timeline: JobTimelineData | null | undefined,
  now: number,
): TimelineView | null {
  if (!timeline || !Number.isFinite(timeline.startedAt)) {
    return null;
  }
  const running = timeline.endedAt === null;
  const from = timeline.startedAt;
  const to = Math.max(from + 1, timeline.endedAt ?? now);
  const span = to - from;

  const lanes = [...timeline.lanes].sort(
    (left, right) => orderOf(left.stage) - orderOf(right.stage),
  );

  const stages = lanes.map((lane): WaterfallStage => {
    const started = lane.spans.reduce(
      (earliest, entry) => Math.min(earliest, entry.startedAt),
      Number.POSITIVE_INFINITY,
    );
    const ended = lane.spans.reduce(
      (latest, entry) => Math.max(latest, entry.endedAt ?? to),
      Number.NEGATIVE_INFINITY,
    );
    const pending = lane.spans.length === 0;
    const failed = lane.spans.some((entry) => entry.state === "FAILED");
    return {
      id: lane.stage,
      label: STAGE_LABEL[lane.stage] ?? lane.stage.toLowerCase().replace(/_/g, " "),
      start: pending ? 0 : ((started - from) / span) * 100,
      end: pending ? 0 : ((ended - from) / span) * 100,
      color: failed ? WV.error : (STAGE_COLOR[lane.stage] ?? WV.slate),
      duration: pending ? "pending" : formatSpan(ended - started),
      pending,
    };
  });

  const active = stages.filter((stage) => !stage.pending).length;
  return {
    stages,
    ticks: [0, 1, 2, 3, 4].map((quarter) =>
      quarter === 0 ? "0s" : formatSpan((span * quarter) / 4),
    ),
    window: running
      ? `started ${formatClockSeconds(from)} · still running`
      : `${formatClockSeconds(from)} → ${formatClockSeconds(to)}`,
    total: running ? `${formatSpan(span)} so far` : formatSpan(span),
    note: running
      ? `stage ${active} of ${stages.length} · ${formatSpan(span)} elapsed`
      : `${active} ${active === 1 ? "stage" : "stages"} · ${formatSpan(span)} total`,
    running,
  };
}

function orderOf(stage: string): number {
  const index = (STAGE_ORDER as readonly string[]).indexOf(stage);
  return index === -1 ? STAGE_ORDER.length : index;
}

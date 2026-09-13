import type { WaterfallSegment, WaterfallStage } from "../components/Waterfall";
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
 *
 * A lane is drawn as the runs it is made of, not as one span from its first
 * start to its last end: a download paused twice is three runs with two gaps,
 * and the gaps are the part worth seeing.
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

export interface ExtractionMemberSpan {
  kind: "EXTRACTING" | "WAITING_FOR_VOLUME" | "APPENDING";
  startedAt: number;
  endedAt: number | null;
  state: "RUNNING" | "COMPLETE" | "FAILED";
  label: string | null;
}

export interface ExtractionMember {
  member: string;
  state: "RUNNING" | "INTERRUPTED" | "COMPLETE" | "AWAITING_REPAIR" | "FAILED";
  error: string | null;
  spans: ExtractionMemberSpan[];
}

export interface ExtractionGroup {
  setName: string;
  members: ExtractionMember[];
}

export interface JobTimelineData {
  startedAt: number;
  endedAt: number | null;
  outcome: string;
  lanes: TimelineLane[];
  extractionGroups?: ExtractionGroup[] | null;
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

const MEMBER_SPAN_LABEL: Record<ExtractionMemberSpan["kind"], string> = {
  EXTRACTING: "Extracting",
  WAITING_FOR_VOLUME: "Waiting for volume",
  APPENDING: "Appending",
};

const MEMBER_SPAN_COLOR: Record<ExtractionMemberSpan["kind"], string> = {
  EXTRACTING: WV.violet,
  WAITING_FOR_VOLUME: WV.idle,
  APPENDING: WV.info,
};

const MEMBER_STATE_LABEL: Record<ExtractionMember["state"], string> = {
  RUNNING: "running",
  INTERRUPTED: "interrupted",
  COMPLETE: "complete",
  AWAITING_REPAIR: "awaiting repair",
  FAILED: "failed",
};

const MEMBER_STATE_COLOR: Record<ExtractionMember["state"], string> = {
  RUNNING: WV.violet,
  INTERRUPTED: WV.idle,
  COMPLETE: WV.green,
  AWAITING_REPAIR: WV.warn,
  FAILED: WV.error,
};

export interface TimelineView {
  stages: WaterfallStage[];
  /** One row per file extracted from the job's archives, in the order they started. */
  members: WaterfallStage[];
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
  /** The job is waiting out its propagation delay, which is what its pending lane is. */
  propagating = false,
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

  const percentOf = (at: number) => ((at - from) / span) * 100;
  const segmentOf = (
    entry: { startedAt: number; endedAt: number | null },
    color: string,
    title: string,
    dashed = false,
  ): WaterfallSegment => {
    const ended = entry.endedAt ?? to;
    return {
      start: percentOf(entry.startedAt),
      end: percentOf(ended),
      color,
      dashed,
      title: `${title} · ${formatClockSeconds(entry.startedAt)} → ${
        entry.endedAt === null ? "now" : formatClockSeconds(entry.endedAt)
      } · ${formatSpan(ended - entry.startedAt)}`,
    };
  };

  const stages = lanes.map((lane): WaterfallStage => {
    const label =
      propagating && lane.stage === "PENDING_DOWNLOAD"
        ? "Propagating"
        : (STAGE_LABEL[lane.stage] ?? lane.stage.toLowerCase().replace(/_/g, " "));
    const color = STAGE_COLOR[lane.stage] ?? WV.slate;
    const failed = lane.spans.some((entry) => entry.state === "FAILED");
    const pending = lane.spans.length === 0;
    const extent = extentOf(lane.spans, to);
    return {
      id: lane.stage,
      label,
      start: pending ? 0 : percentOf(extent.started),
      end: pending ? 0 : percentOf(extent.ended),
      color: failed ? WV.error : color,
      duration: pending ? "pending" : formatSpan(runTime(lane.spans, to)),
      pending,
      segments: lane.spans.map((entry) =>
        segmentOf(
          entry,
          entry.state === "FAILED" ? WV.error : color,
          entry.label ?? label,
          // A restart's downtime is a gap the job sat through, not work it did.
          lane.stage === "INTERRUPTED",
        ),
      ),
    };
  });

  const members = (timeline.extractionGroups ?? [])
    .flatMap((group) =>
      group.members.map((member) => {
        const name = member.member.split("/").pop() || member.member;
        const extent = extentOf(member.spans, to);
        const state = MEMBER_STATE_LABEL[member.state] ?? member.state.toLowerCase();
        const stage: WaterfallStage = {
          id: `${group.setName}:${member.member}`,
          label: name,
          start: member.spans.length === 0 ? 0 : percentOf(extent.started),
          end: member.spans.length === 0 ? 0 : percentOf(extent.ended),
          color: MEMBER_STATE_COLOR[member.state] ?? WV.slate,
          duration:
            member.state === "COMPLETE"
              ? formatSpan(runTime(member.spans, to))
              : `${formatSpan(runTime(member.spans, to))} · ${state}`,
          pending: member.spans.length === 0,
          title: [member.member, `set ${group.setName}`, state, member.error]
            .filter(Boolean)
            .join(" · "),
          segments: member.spans.map((entry) =>
            segmentOf(
              entry,
              entry.state === "FAILED" ? WV.error : MEMBER_SPAN_COLOR[entry.kind],
              entry.label ? `${MEMBER_SPAN_LABEL[entry.kind]} · ${entry.label}` : MEMBER_SPAN_LABEL[entry.kind],
            ),
          ),
        };
        return { stage, startedAt: extent.started };
      }),
    )
    .sort(
      (left, right) =>
        left.startedAt - right.startedAt || left.stage.label.localeCompare(right.stage.label),
    )
    .map((entry) => entry.stage);

  const active = stages.filter((stage) => !stage.pending).length;
  return {
    stages,
    members,
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

/** The first start and the last end of a set of runs; a run still going ends now. */
function extentOf(spans: readonly { startedAt: number; endedAt: number | null }[], now: number) {
  let started = Number.POSITIVE_INFINITY;
  let ended = Number.NEGATIVE_INFINITY;
  for (const entry of spans) {
    started = Math.min(started, entry.startedAt);
    ended = Math.max(ended, entry.endedAt ?? now);
  }
  return { started, ended };
}

/** Time actually spent in the runs, leaving out the gaps between them. */
function runTime(spans: readonly { startedAt: number; endedAt: number | null }[], now: number) {
  return spans.reduce((total, entry) => total + Math.max(0, (entry.endedAt ?? now) - entry.startedAt), 0);
}

function orderOf(stage: string): number {
  const index = (STAGE_ORDER as readonly string[]).indexOf(stage);
  return index === -1 ? STAGE_ORDER.length : index;
}

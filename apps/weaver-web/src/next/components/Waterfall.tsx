import { useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { blockFill, blockPeriod } from "./chrome";
import { Icon } from "./icons";
import { COLS_CLASS, columnStyle } from "./columns";
import { WV } from "../data/palette";

/**
 * A devtools-style waterfall: one row per stage, laid out against the job's
 * own window rather than a fixed scale.
 *
 * The whole point is that a 34s download and a 0.4s move share an axis, so the
 * duration label is placed by rule rather than always sitting in the same
 * place: after a span that ends early, before one that starts late, and inside
 * anything that spans the middle — where it takes a solid chip of the span's
 * own colour so it stays readable against the cells.
 *
 * Under the stages, on the same axis, sit the files extracted from the job's
 * archives. There can be hundreds of them, so they stay folded until asked for.
 */

/** One run inside a stage's span. */
export interface WaterfallSegment {
  start: number;
  end: number;
  color: string;
  /** Time the job sat through rather than spent working: drawn as an outline. */
  dashed?: boolean;
  /** What the run was and when, shown on hover. */
  title?: string;
}

export interface WaterfallStage {
  id: string;
  label: string;
  /** Where the span starts and ends, as a percentage of the job's window. */
  start: number;
  end: number;
  color: string;
  duration: string;
  /** A stage that has not run: hollow square, dimmed label, no span. */
  pending?: boolean;
  /** The runs the span is made of; without them it is drawn as one run. */
  segments?: WaterfallSegment[];
  /** The row's full story on hover, where the label has to be cut short. */
  title?: string;
}

const TICK_POSITIONS = [0, 25, 50, 75, 100] as const;
const TICK_SHIFTS = ["0%", "-50%", "-50%", "-50%", "-100%"] as const;
// The stage label truncates rather than wrapping, so a narrow screen keeps a
// readable track for the spans themselves.
const COLUMNS = {
  base: "64px minmax(0, 1fr)",
  sm: "minmax(84px, 140px) minmax(0, 1fr)",
};
/** The gridlines are the track: one hairline every quarter, under every row. */
const GRIDLINES = `repeating-linear-gradient(90deg, ${WV.gridline} 0 1px, transparent 1px 25%)`;
/**
 * A span's height, which has to be stated twice: once here, for the cell period
 * it earns, and once as a literal `h-[13px]` below, because Tailwind reads the
 * class out of the source and cannot be handed a number.
 */
const SPAN_HEIGHT = 13;

export function Waterfall({
  stages,
  members = [],
  ticks,
  window,
  total,
}: {
  stages: readonly WaterfallStage[];
  /** Files extracted from the job's archives, drawn as quieter rows under the stages. */
  members?: readonly WaterfallStage[];
  /** Five labels, one per quarter of the axis. */
  ticks: readonly string[];
  /** The job's start and end, in words: `18:40:57 → 18:41:33`. */
  window: string;
  total: string;
}) {
  const t = useTranslate();
  const [membersOpen, setMembersOpen] = useState(false);

  return (
    <div className="flex min-w-0 flex-col">
      <div
        className={cn(COLS_CLASS, "grid items-end gap-[14px] pb-[5px]")}
        style={columnStyle(COLUMNS)}
      >
        <span />
        <div className="relative h-[13px] min-w-0">
          {ticks.map((tick, index) => (
            <span
              key={TICK_POSITIONS[index]}
              className="absolute top-0 font-wv-mono text-[10px] whitespace-nowrap text-wv-disabled"
              style={{
                left: `${TICK_POSITIONS[index]}%`,
                transform: `translateX(${TICK_SHIFTS[index]})`,
              }}
            >
              {tick}
            </span>
          ))}
        </div>
      </div>

      {stages.map((stage) => (
        <StageRow key={stage.id} stage={stage} />
      ))}

      {members.length === 0 ? null : (
        <>
          <button
            type="button"
            aria-expanded={membersOpen}
            onClick={() => setMembersOpen((open) => !open)}
            className="flex h-8 items-center gap-2 border-t border-wv-hairline text-left font-wv-mono text-[11px] text-wv-muted hover:text-wv-fg"
          >
            <Icon
              name="expand"
              size={12}
              className={cn("flex-none transition-transform", membersOpen && "rotate-90")}
            />
            {t("next.waterfall.extractedFiles", { count: members.length })}
          </button>
          {membersOpen
            ? members.map((member) => <StageRow key={member.id} stage={member} tone="member" />)
            : null}
        </>
      )}

      <div
        className={cn(COLS_CLASS, "grid h-[30px] items-center gap-[14px] border-t border-wv-axis")}
        style={columnStyle(COLUMNS)}
      >
        <span className="font-wv-mono text-[10.5px] tracking-[0.08em] text-wv-disabled uppercase">
          {t("timeline.totalDuration")}
        </span>
        <div className="flex min-w-0 items-baseline gap-[10px] font-wv-mono text-[10.5px] text-wv-faint">
          <span className="truncate">{window}</span>
          <span className="ml-auto flex-none whitespace-nowrap text-wv-muted">{total}</span>
        </div>
      </div>
    </div>
  );
}

function StageRow({ stage, tone = "stage" }: { stage: WaterfallStage; tone?: "stage" | "member" }) {
  const start = Math.max(0, Math.min(100, stage.start));
  const end = Math.max(start, Math.min(100, stage.end));
  // Where the label goes: after a span that finishes in the first two thirds,
  // before one that starts in the last two thirds, inside anything else.
  const after = end <= 62;
  const before = !after && start >= 38;
  const inside = !after && !before && !stage.pending;
  const member = tone === "member";
  const segments = stage.segments ?? [
    { start: stage.start, end: stage.end, color: stage.color },
  ];
  // A label inside the span sits on its longest run: anchored to the first one,
  // a download paused after a moment would paint its chip across the gap.
  const anchor = inside && segments.length > 0
    ? segments.reduce((widest, segment) =>
        segment.end - segment.start > widest.end - widest.start ? segment : widest,
      )
    : null;
  const anchorStart = anchor ? Math.max(0, Math.min(100, anchor.start)) : start;

  return (
    <div
      className={cn(
        COLS_CLASS,
        "grid items-stretch gap-[14px] border-t border-wv-hairline",
        member ? "h-7" : "h-8",
      )}
      style={columnStyle(COLUMNS)}
      title={stage.title}
    >
      <div className={cn("flex min-w-0 items-center gap-2", member && "pl-[14px]")}>
        <span
          aria-hidden="true"
          className="size-1.5 flex-none"
          style={{ background: stage.pending ? WV.trackDash : stage.color }}
        />
        <span
          className={cn(
            "truncate",
            member ? "font-wv-mono text-[11px]" : "text-[12.5px]",
            stage.pending ? "text-wv-disabled" : member ? "text-wv-secondary" : "text-wv-fg",
          )}
        >
          {member ? (
            // Files from one release share their leading name; the end of the
            // path is what tells them apart, so a long one is cut at the front.
            <span dir="rtl" className="block truncate text-left">
              <bdi>{stage.label}</bdi>
            </span>
          ) : (
            stage.label
          )}
        </span>
      </div>
      <div className="relative min-w-0" style={{ backgroundImage: GRIDLINES }}>
        {stage.pending
          ? null
          : segments.map((segment, index) => {
              const left = Math.max(0, Math.min(100, segment.start));
              const right = Math.max(left, Math.min(100, segment.end));
              return (
                <div
                  // Runs never reorder within a row, so their position is their identity.
                  key={index}
                  title={segment.title}
                  className={cn(
                    "absolute h-[13px] min-w-[3px] overflow-hidden",
                    member ? "top-[7px]" : "top-[10px]",
                  )}
                  style={{
                    left: `${left}%`,
                    width: `${right - left}%`,
                    ...(segment.dashed
                      ? { border: `1px dashed ${segment.color}` }
                      : { backgroundImage: blockFill(segment.color, blockPeriod(SPAN_HEIGHT)) }),
                  }}
                />
              );
            })}
        <span
          className={cn(
            "pointer-events-none absolute h-[13px] px-1.5 font-wv-mono text-[10px] leading-[13px] whitespace-nowrap",
            member ? "top-[7px]" : "top-[10px]",
          )}
          style={{
            left: after ? `${end}%` : before ? "auto" : `${anchorStart}%`,
            right: after || !before ? "auto" : `${100 - start}%`,
            background: inside ? stage.color : "transparent",
            color: inside ? WV.onSpan : stage.pending ? WV.disabled : WV.muted,
          }}
        >
          {stage.duration}
        </span>
      </div>
    </div>
  );
}

import { cn } from "@/lib/utils";
import { blockFill, blockPeriod } from "./chrome";
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
 */

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
  ticks,
  window,
  total,
}: {
  stages: readonly WaterfallStage[];
  /** Five labels, one per quarter of the axis. */
  ticks: readonly string[];
  /** The job's start and end, in words: `18:40:57 → 18:41:33`. */
  window: string;
  total: string;
}) {
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

      <div
        className={cn(COLS_CLASS, "grid h-[30px] items-center gap-[14px] border-t border-wv-axis")}
        style={columnStyle(COLUMNS)}
      >
        <span className="font-wv-mono text-[10.5px] tracking-[0.08em] text-wv-disabled uppercase">
          Total
        </span>
        <div className="flex min-w-0 items-baseline gap-[10px] font-wv-mono text-[10.5px] text-wv-faint">
          <span className="truncate">{window}</span>
          <span className="ml-auto flex-none whitespace-nowrap text-wv-muted">{total}</span>
        </div>
      </div>
    </div>
  );
}

function StageRow({ stage }: { stage: WaterfallStage }) {
  const start = Math.max(0, Math.min(100, stage.start));
  const end = Math.max(start, Math.min(100, stage.end));
  // Where the label goes: after a span that finishes in the first two thirds,
  // before one that starts in the last two thirds, inside anything else.
  const after = end <= 62;
  const before = !after && start >= 38;
  const inside = !after && !before && !stage.pending;

  return (
    <div
      className={cn(COLS_CLASS, "grid h-8 items-stretch gap-[14px] border-t border-wv-hairline")}
      style={columnStyle(COLUMNS)}
    >
      <div className="flex min-w-0 items-center gap-2">
        <span
          aria-hidden="true"
          className="size-1.5 flex-none"
          style={{ background: stage.pending ? WV.trackDash : stage.color }}
        />
        <span
          className={`truncate text-[12.5px] ${stage.pending ? "text-wv-disabled" : "text-wv-fg"}`}
        >
          {stage.label}
        </span>
      </div>
      <div className="relative min-w-0" style={{ backgroundImage: GRIDLINES }}>
        {stage.pending ? null : (
          <div
            className="absolute top-[10px] h-[13px] min-w-[3px] overflow-hidden"
            style={{
              left: `${start}%`,
              width: `${end - start}%`,
              backgroundImage: blockFill(stage.color, blockPeriod(SPAN_HEIGHT)),
            }}
          />
        )}
        <span
          className="absolute top-[10px] h-[13px] px-1.5 font-wv-mono text-[10px] leading-[13px] whitespace-nowrap"
          style={{
            left: after ? `${end}%` : before ? "auto" : `${start}%`,
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

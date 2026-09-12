import type { CSSProperties, ReactNode } from "react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { WV } from "../data/palette";

/** Uppercase 10.5/600/0.14em label. Rail blocks use `tone="rail"`, headers "header". */
export function Eyebrow({
  children,
  tone = "header",
  className,
}: {
  children: ReactNode;
  tone?: "header" | "rail";
  className?: string;
}) {
  return (
    <div
      className={cn(
        "font-wv-title text-[10.5px] font-semibold uppercase tracking-[0.14em]",
        tone === "header" ? "text-wv-eyebrow" : "text-wv-faint",
        className,
      )}
    >
      {children}
    </div>
  );
}

/**
 * The 30px band that separates every group of rows.
 *
 * It is `sticky` by default because the only scrolling region on every screen
 * is the row list beneath it; the shell keeps everything above the list fixed.
 */
export function SectionHeader({
  label,
  count,
  note,
  sticky = true,
}: {
  label: ReactNode;
  count?: ReactNode;
  note?: ReactNode;
  sticky?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex h-[30px] flex-none items-center gap-[10px] border-b border-wv-line-strong bg-wv-section px-4 sm:px-6",
        sticky && "sticky top-0 z-[5]",
      )}
    >
      <Eyebrow>{label}</Eyebrow>
      {count === undefined ? null : (
        <span className="font-wv-mono text-[11px] text-wv-note">{count}</span>
      )}
      {note === undefined ? null : (
        <span className="ml-auto truncate font-wv-mono text-[11px] text-wv-note">{note}</span>
      )}
    </div>
  );
}

/**
 * A wrapping strip of metric cells.
 *
 * The cells are an auto-fit grid rather than a flex row, so four of them hold
 * one line on a wide window and fold two-by-two on a narrow one instead of
 * crushing. `min` is the width a cell refuses to go below: 146px on a list
 * screen's metric strip, 168px on a job's stat strip.
 */
export function MetricStrip({ min = 146, children }: { min?: number; children: ReactNode }) {
  return (
    <div
      className="grid flex-none border-b border-wv-line-strong bg-wv-chrome"
      style={{ gridTemplateColumns: `repeat(auto-fit, minmax(${min}px, 1fr))` }}
    >
      {children}
    </div>
  );
}

/**
 * One cell of a metric or stat strip: eyebrow, big number with a unit, mono note.
 *
 * `eyebrow` is a node rather than a string because the Storage cell turns its
 * eyebrow row into a menu trigger.
 *
 * The variants are the three sizes the system uses: `wide` is the flex row two
 * or three big numbers sit in, `strip` and `stat` are cells of a `MetricStrip`
 * — `stat` being the smaller one a single job's numbers take.
 */
export function MetricCell({
  eyebrow,
  value,
  unit,
  note,
  valueClassName,
  variant = "wide",
  children,
  last,
}: {
  eyebrow: ReactNode;
  value: ReactNode;
  unit?: ReactNode;
  note?: ReactNode;
  valueClassName?: string;
  variant?: "wide" | "strip" | "stat";
  /** Anything absolutely positioned against the cell, e.g. the storage menu. */
  children?: ReactNode;
  last?: boolean;
}) {
  const wide = variant === "wide";
  return (
    <div
      className={cn(
        "relative flex min-w-0 flex-col",
        wide && "flex-1 gap-2 border-t border-wv-hairline px-4 py-4 sm:px-6 lg:border-t-0",
        variant === "strip" && "gap-2 border-t border-wv-hairline px-4 sm:px-[22px] py-[15px]",
        variant === "stat" && "gap-[7px] border-t border-wv-hairline px-5 py-[14px]",
        (!wide || !last) && "border-r border-wv-hairline",
      )}
    >
      {typeof eyebrow === "string" ? <Eyebrow tone="rail">{eyebrow}</Eyebrow> : eyebrow}
      <div className="flex items-baseline gap-1.5 whitespace-nowrap">
        <span
          className={cn(
            "truncate font-semibold leading-none tracking-[-0.02em]",
            wide ? "text-[25px]" : variant === "strip" ? "text-[24px]" : "text-[21px]",
            valueClassName ?? "text-wv-fg",
          )}
        >
          {value}
        </span>
        {unit === undefined ? null : (
          <span className="min-w-0 truncate text-[12px] text-wv-muted">{unit}</span>
        )}
      </div>
      <div
        className={cn(
          "truncate font-wv-mono",
          variant === "stat" ? "text-[10.5px] text-wv-faint" : "text-[11px] text-wv-muted",
        )}
      >
        {note ?? " "}
      </div>
      {children}
    </div>
  );
}

/**
 * The 21px uppercase chip that states a job's outcome beside its title.
 *
 * Two grounds only, because history speaks in two outcomes: the accent-tinted
 * one for anything that is fine, the danger one for a failure.
 */
export function StateChip({ label, tone }: { label: string; tone: "ok" | "bad" }) {
  return (
    <span
      className={cn(
        "flex h-[21px] flex-none items-center border px-[9px] font-wv-mono text-[10.5px] tracking-[0.08em] whitespace-nowrap uppercase",
        tone === "bad"
          ? "border-wv-danger-border bg-wv-danger-bg text-wv-error-text"
          : "border-wv-bulk-line bg-wv-ok-bg text-wv-accent",
      )}
    >
      {label}
    </span>
  );
}

/**
 * One block of a long scrolling screen: an eyebrow, a mono note beside it, an
 * optional right-hand affordance, and the block's own content below.
 *
 * Job detail is built entirely from these — pipeline, files, log and every
 * reference panel — so the rhythm between them is stated once, here.
 */
export function DetailBlock({
  title,
  note,
  right,
  tone = "block",
  className,
  bodyClassName,
  children,
  id,
}: {
  title: string;
  note?: ReactNode;
  right?: ReactNode;
  /**
   * `block` is a full-width band of the screen; `panel` is one cell of the
   * reference grid at the bottom of it, which sits on hairlines and takes the
   * quieter rail eyebrow so the bands above still lead.
   */
  tone?: "block" | "panel";
  className?: string;
  bodyClassName?: string;
  children: ReactNode;
  id?: string;
}) {
  return (
    <section
      id={id}
      className={cn(
        "flex min-w-0 flex-col gap-3 px-4 sm:px-[22px] pt-[18px]",
        tone === "panel"
          ? "border-r border-b border-wv-hairline pb-5"
          : "border-b border-wv-line-strong pb-[22px]",
        className,
      )}
    >
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <Eyebrow tone={tone === "panel" ? "rail" : "header"}>{title}</Eyebrow>
        {note === undefined ? null : (
          <span className="font-wv-mono text-[11px] text-wv-note">{note}</span>
        )}
        {right === undefined ? null : (
          <span className="ml-auto min-w-0 truncate font-wv-mono text-[11px] text-wv-idle">
            {right}
          </span>
        )}
      </div>
      <div className={cn("flex min-w-0 flex-col", bodyClassName)}>{children}</div>
    </section>
  );
}

/**
 * The reference grid a detail screen ends on: as many `DetailBlock tone="panel"`
 * cells as fit, folding rather than crushing. 272px is the width the narrowest
 * of them — a two-column key/value list — stops being readable below.
 */
export function PanelGrid({ min = 272, children }: { min?: number; children: ReactNode }) {
  return (
    <div
      className="grid min-w-0"
      style={{ gridTemplateColumns: `repeat(auto-fit, minmax(${min}px, 1fr))` }}
    >
      {children}
    </div>
  );
}

/**
 * A labelled value, in the three shapes the reference panels need.
 *
 * `micro` is the dense parse grid — a tiny uppercase key over its value.
 * `inline` puts the key in a fixed column beside a mono value, for lists whose
 * keys are short and whose values want the room. `stacked` is for values that
 * have to wrap: ids, paths, original titles.
 */
export function Field({
  label,
  value,
  variant = "micro",
  title,
}: {
  label: ReactNode;
  value: ReactNode;
  variant?: "micro" | "inline" | "stacked";
  title?: string;
}) {
  if (variant === "inline") {
    return (
      <div className="flex min-w-0 items-baseline gap-3">
        <span className="w-[92px] flex-none text-[12.5px] text-wv-muted">{label}</span>
        <span
          title={title}
          className="min-w-0 truncate font-wv-mono text-[11.5px] text-wv-fg"
        >
          {value}
        </span>
      </div>
    );
  }
  if (variant === "stacked") {
    return (
      <div className="flex min-w-0 flex-col gap-[3px]">
        <span className="text-[12px] text-wv-muted">{label}</span>
        <span className="font-wv-mono text-[11px] leading-[1.5] break-all text-wv-tertiary">
          {value}
        </span>
      </div>
    );
  }
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <span className="truncate text-[10px] tracking-[0.1em] text-wv-faint uppercase">
        {label}
      </span>
      <span title={title} className="truncate text-[13px] font-medium text-wv-fg">
        {value}
      </span>
    </div>
  );
}

/** An outlined mono chip: a parsed release's flags, and nothing else so far. */
export function Tag({ children }: { children: ReactNode }) {
  return (
    <span className="border border-wv-control px-2 py-[3px] font-wv-mono text-[10.5px] tracking-[0.06em] text-wv-tertiary uppercase">
      {children}
    </span>
  );
}

/** 7px status/category square — squares, never circles, in this design. */
export function Square({
  color,
  size = 7,
  className,
}: {
  color: string;
  size?: number;
  className?: string;
}) {
  return (
    <span
      aria-hidden="true"
      className={cn("flex-none", className)}
      style={{ width: size, height: size, background: color }}
    />
  );
}

/** Quarter-cell levels a building cell climbs before it reads as filled. */
const BAR_LEVELS = 4;

/** How long a reported jump takes to walk up to its new value. */
const BAR_STEP_MS = 900;

/*
 * One size observer for every meter on the page.
 *
 * Each bar needs its own width to know where its cell boundaries fall, and a
 * queue view can hold dozens of them -- one observer apiece is a lot of
 * machinery for one number each.
 */
let barSizes: ResizeObserver | null = null;
const barSinks = new WeakMap<Element, (width: number) => void>();

function barSizeObserver(): ResizeObserver {
  barSizes ??= new ResizeObserver((entries) => {
    for (const entry of entries) {
      barSinks.get(entry.target)?.(entry.contentRect.width);
    }
  });
  return barSizes;
}

function useTrackWidth() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const track = ref.current;
    if (track === null) {
      return;
    }
    // Measured before the first paint, so a meter never shows a frame drawn
    // against a width of zero.
    setWidth(track.getBoundingClientRect().width);
    barSinks.set(track, setWidth);
    const observer = barSizeObserver();
    observer.observe(track);
    return () => {
      observer.unobserve(track);
      barSinks.delete(track);
    };
  }, []);
  return [ref, width] as const;
}

/**
 * Walk a value up to its target one drawn level at a time.
 *
 * Progress lands every couple of seconds, often several cells at once, and
 * painting it straight makes the meter lurch. Walking it means each cell climbs
 * its levels the way a terminal meter redraws. `quantum` is the value change
 * worth one level, so this re-renders only when the drawing would differ --
 * four times per cell crossed rather than once per frame. Reduced motion skips
 * the walk and takes the new value as it is.
 */
function useSteppedValue(target: number, quantum: number): number {
  const [shown, setShown] = useState(target);
  const shownRef = useRef(target);

  useEffect(() => {
    const settle = () => {
      shownRef.current = target;
      setShown(target);
    };
    const reduced =
      window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
    if (!(quantum > 0) || reduced || Math.abs(target - shownRef.current) < quantum) {
      settle();
      return;
    }
    const from = shownRef.current;
    const start = performance.now();
    let frame = 0;
    const tick = (now: number) => {
      const fraction = Math.min(1, (now - start) / BAR_STEP_MS);
      if (fraction >= 1) {
        settle();
        return;
      }
      const next = from + (target - from) * fraction;
      if (Math.abs(next - shownRef.current) >= quantum) {
        shownRef.current = next;
        setShown(next);
      }
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    // A backgrounded tab is handed no animation frames, so the walk above would
    // neither advance nor end there: the meter would sit on whatever value it
    // held when the tab went away, even after the job behind it finished.
    // Timers still fire, throttled, so this is what guarantees the value
    // arrives. Where frames do flow the walk has already settled before it
    // runs, and settling twice on the same number is a no-op.
    const guard = window.setTimeout(settle, BAR_STEP_MS + 50);
    return () => {
      window.clearTimeout(guard);
      cancelAnimationFrame(frame);
    };
  }, [target, quantum]);

  return shown;
}

/**
 * A terminal-style block meter.
 *
 * The track is 2px dashes on one cell period and the fill is the same period in
 * near-solid cells, so the bar reads as a row of character cells rather than a
 * continuous strip.
 *
 * It fills the way a TUI meter does. Whole cells behind the value are solid;
 * the cell at the value is a stub that climbs in quarters, so it gains height
 * first and only once it is full does the next one start. Nothing eases -- the
 * value itself is walked up in those same quarter steps, so every level gets
 * drawn instead of being skipped over by a smooth slide.
 *
 * Sizes in the system: 7-8px in rows and gauges, 10px for a pipeline span,
 * 12px for a job's hero bar. The two larger ones take the wider 7px period the
 * design gives them.
 */
export function Bar({
  percent,
  color,
  height = 8,
  period,
  className,
  style,
}: {
  percent: number;
  color: string;
  height?: number;
  /** Cell width in px. Defaults to 6, or 7 once the bar is 10px or taller. */
  period?: number;
  className?: string;
  style?: CSSProperties;
}) {
  const clamped = Number.isFinite(percent) ? Math.max(0, Math.min(100, percent)) : 0;
  const cell = period ?? blockPeriod(height);
  const [ref, width] = useTrackWidth();
  const cells = Math.max(1, Math.floor(width / cell));
  const shown = useSteppedValue(clamped, 100 / (cells * BAR_LEVELS));

  // Whole cells first, then the level the next one has reached. The epsilon
  // keeps a value sitting exactly on a boundary from also drawing an empty stub
  // past it.
  const exact = (shown / 100) * cells;
  const filled = Math.min(cells, Math.floor(exact + 1e-6));
  const level = filled >= cells ? 0 : Math.floor((exact - filled) * BAR_LEVELS);

  return (
    <div
      ref={ref}
      className={cn("relative", className)}
      style={{
        height,
        backgroundImage: blockTrack(cell),
        // The cells tile from the left and a track is rarely an exact multiple
        // of one, so the ground is cut to the last whole cell. Otherwise a
        // finished meter ends in a few pixels of leftover dashes and reads as
        // still having somewhere to go.
        backgroundRepeat: "no-repeat",
        backgroundSize: `${cells * cell}px 100%`,
        ...style,
      }}
    >
      {filled === 0 ? null : (
        <div
          className="absolute inset-y-0 left-0"
          style={{ width: filled * cell, backgroundImage: blockFill(color, cell) }}
        />
      )}
      {level === 0 ? null : (
        <div
          className="absolute bottom-0"
          style={{
            left: filled * cell,
            width: cell - 1,
            height: `${(level / BAR_LEVELS) * 100}%`,
            background: color,
          }}
        />
      )}
    </div>
  );
}

/**
 * A filled fraction, drawn round.
 *
 * The bars measure progress along a length; this measures how full something
 * already is, which is a different question and reads better as a wedge than as
 * one more horizontal strip. A pie also survives being small: at 30px the
 * quarter marks are still legible, where a 30px bar is a smudge.
 */
export function Pie({
  percent,
  color,
  size = 46,
  className,
}: {
  percent: number;
  color: string;
  size?: number;
  className?: string;
}) {
  const filled = Number.isFinite(percent) ? Math.max(0, Math.min(100, percent)) : 0;
  return (
    <div
      role="img"
      aria-label={`${Math.round(filled)}% full`}
      className={cn("flex-none rounded-full", className)}
      style={{
        width: size,
        height: size,
        background: `conic-gradient(${color} 0 ${filled}%, ${WV.pending} ${filled}% 100%)`,
        boxShadow: `inset 0 0 0 1px ${WV.track}`,
      }}
    />
  );
}

/** The cell period a bar of this height takes. */
export function blockPeriod(height: number): number {
  return height >= 10 ? 7 : 6;
}

/** The dashed ground a block bar is measured against. */
export function blockTrack(cell = 6): string {
  return `repeating-linear-gradient(90deg, ${WV.trackDash} 0 2px, transparent 2px ${cell}px)`;
}

/**
 * The cells themselves.
 *
 * Exported because a waterfall span is a block fill with no track of its own —
 * the timeline's gridlines already say where it sits — and it has to share
 * this exact period to read as the same kind of object.
 */
export function blockFill(color: string, cell = 6): string {
  return `repeating-linear-gradient(90deg, ${color} 0 ${cell - 1}px, transparent ${cell - 1}px ${cell}px)`;
}

export function EmptyState({ title, body }: { title: string; body: string }) {
  return (
    <div className="flex flex-col gap-1.5 px-4 sm:px-6 py-10">
      <div className="font-wv-title text-[14px] font-semibold text-wv-fg">{title}</div>
      <div className="text-[13px] text-wv-muted">{body}</div>
    </div>
  );
}

/** A dense key/value row — System info's basic unit, 11px vertical padding. */
export function KeyValueRow({ label, value }: { label: ReactNode; value: ReactNode }) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-6 gap-y-1.5 border-b border-wv-hairline px-4 sm:px-6 py-[11px] hover:bg-wv-cell-hover">
      <div className="min-w-0 flex-[1_1_260px] text-[13px] text-wv-muted">{label}</div>
      <div className="ml-auto flex-none text-right font-wv-mono text-[12.5px] text-wv-fg">
        {value}
      </div>
    </div>
  );
}

import type { CSSProperties, ReactNode } from "react";
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
        "text-[10.5px] font-semibold uppercase tracking-[0.14em]",
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

/**
 * A terminal-style block meter.
 *
 * Two layers: a track of 2px dashes and a fill of near-solid cells, clipped to
 * the value. Both share one cell period and one origin, so the cells line up
 * and a fill cut mid-cell reads as a partial block rather than a smooth edge.
 *
 * Sizes in the system: 7-8px in rows and gauges, 10px for a pipeline span,
 * 12px for a job's hero bar. The two larger ones take the wider 7px period the
 * design gives them. The width never transitions — the bar steps, it does not
 * ease.
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
  return (
    <div
      className={cn("relative", className)}
      style={{ height, backgroundImage: blockTrack(cell), ...style }}
    >
      <div
        className="absolute inset-y-0 left-0 overflow-hidden"
        style={{ width: `${clamped}%`, backgroundImage: blockFill(color, cell) }}
      />
    </div>
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
      <div className="text-[14px] font-semibold text-wv-fg">{title}</div>
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

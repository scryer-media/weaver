import { statusToken, type StatusToken } from "@/lib/status-tokens";

/**
 * The design's colour vocabulary, resolved in JS.
 *
 * Tailwind classes cover everything declared in markup, but squares, bars and
 * SVG series take their colour from data, so those reach for these constants.
 * Every value is a handoff token; the accent reads from the CSS variable so a
 * rebrand still only changes one place.
 */

export const WV = {
  accent: "var(--wv-accent)",
  info: "#6aa9cf",
  warn: "#d3a43e",
  error: "#e0745a",
  violet: "#a98ad6",
  green: "#5fbf94",
  gold: "#c2ad5e",
  idle: "#8d8a82",
  slate: "#7f8692",
  inert: "#4e4f56",
  track: "#33343a",
  /** The block bar's dashed track. */
  trackDash: "#3d3e45",
  /** Gridlines behind a waterfall track. */
  gridline: "#26272c",
  /** Text on a solid span — the waterfall's duration chip. */
  onSpan: "#11201d",
  /** A stage that has not run yet. */
  pending: "#34353b",
  muted: "#a09d96",
  disabled: "#75726b",
  faint: "#84817a",
} as const;

/**
 * How full is too full.
 *
 * Shared so a capacity bar and a capacity pie never disagree about when a
 * volume has become a problem.
 */
export function usageColor(percent: number): string {
  if (percent >= 85) return WV.error;
  if (percent >= 65) return WV.warn;
  return WV.accent;
}

/** Area fills under the lead series of a chart. */
export const WV_FILL = {
  accent: "rgba(63, 179, 156, 0.13)",
  info: "rgba(106, 169, 207, 0.12)",
  violet: "rgba(169, 138, 214, 0.12)",
} as const;

const STATUS_COLORS: Record<StatusToken, string> = {
  downloading: WV.accent,
  queued: WV.idle,
  paused: WV.warn,
  verifying: WV.info,
  repairing: WV.info,
  extracting: WV.violet,
  copying: WV.info,
  completed: WV.green,
  failed: WV.error,
};

/** Colour for a backend job state, via the shared status-token mapping. */
export function statusColor(status: string): string {
  return STATUS_COLORS[statusToken(status)];
}

/**
 * Category colours.
 *
 * The prototype hard-codes four categories; weaver's are user-defined, so a
 * stable hash picks from the same four hues. Deterministic on the name, which
 * is what keeps a category the same colour in the rail, in the list and in the
 * settings table.
 */
const CATEGORY_COLORS = [WV.info, WV.violet, WV.green, WV.gold, WV.accent] as const;

export const UNCATEGORISED_COLOR = WV.inert;

export function categoryColor(name: string | null | undefined): string {
  if (!name) {
    return UNCATEGORISED_COLOR;
  }
  let hash = 0;
  for (let index = 0; index < name.length; index += 1) {
    hash = (hash * 31 + name.charCodeAt(index)) >>> 0;
  }
  return CATEGORY_COLORS[hash % CATEGORY_COLORS.length];
}

/**
 * The bar colour a job's output file takes.
 *
 * Anything that is not the payload — the nfo, the recovery set, the sfv — gets
 * the inert bar, so a file list reads as "one big thing plus its paperwork"
 * rather than as a dozen equal items.
 */
const SUPPORTING_FILE = /\.(nfo|par2|sfv|srr|txt|jpg|png|nzb)$/i;

export function fileColor(name: string, payload: string): string {
  return SUPPORTING_FILE.test(name) ? WV.inert : payload;
}

/** Log level colours, matching the level chips and the level column. */
export const LOG_LEVEL_COLORS: Record<string, string> = {
  error: WV.error,
  warn: WV.warn,
  info: WV.info,
  debug: WV.green,
  trace: WV.idle,
};

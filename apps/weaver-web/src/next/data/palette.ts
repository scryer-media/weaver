// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { statusToken, type StatusToken } from "@/lib/status-tokens";

/**
 * The design's colour vocabulary, resolved in JS.
 *
 * Tailwind classes cover everything declared in markup, but squares, bars and
 * SVG series take their colour from data, so those reach for these CSS tokens.
 */

export const WV = {
  accent: "var(--wv-accent)",
  progress: "var(--color-wv-progress)",
  ok: "var(--color-wv-ok)",
  info: "var(--color-wv-info)",
  warn: "var(--color-wv-warn)",
  error: "var(--color-wv-error)",
  violet: "var(--color-wv-violet)",
  green: "var(--color-wv-green)",
  gold: "var(--color-wv-gold)",
  idle: "var(--color-wv-idle)",
  slate: "var(--color-wv-slate)",
  inert: "var(--color-wv-inert)",
  track: "var(--color-wv-track)",
  /** The block bar's dashed track. */
  trackDash: "var(--color-wv-track-dash)",
  /** Gridlines behind a waterfall track. */
  gridline: "var(--color-wv-hairline)",
  /** Text on a solid span — the waterfall's duration chip. */
  onSpan: "var(--color-wv-on-span)",
  /** A stage that has not run yet. */
  pending: "var(--color-wv-pending)",
  muted: "var(--color-wv-muted)",
  disabled: "var(--color-wv-disabled)",
  faint: "var(--color-wv-faint)",
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
  return WV.ok;
}

/** Area fills under the lead series of a chart. */
export const WV_FILL = {
  accent: "color-mix(in srgb, var(--color-wv-accent) 13%, transparent)",
  info: "color-mix(in srgb, var(--color-wv-info) 12%, transparent)",
  violet: "color-mix(in srgb, var(--color-wv-violet) 12%, transparent)",
} as const;

const STATUS_COLORS: Record<StatusToken, string> = {
  downloading: WV.progress,
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
 * A stable hash of the name keeps each category the same colour in the rail,
 * list and settings table across accent choices.
 */
const CATEGORY_COLORS = [
  WV.info,
  WV.violet,
  WV.green,
  WV.gold,
  "var(--color-wv-teal)",
] as const;

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

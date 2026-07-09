/**
 * Single source of truth mapping backend job/pipeline statuses to the design's
 * semantic status tokens. Every status chip, progress bar, dot, sparkline, and
 * timeline lane resolves its color through here so the palette stays consistent
 * and lives in `globals.css` (see `--status-*` / `--priority-*`).
 *
 * Utility class strings below are written as literals so the Tailwind scanner
 * keeps them in the build. For inline SVG (sparklines, timeline segments) set the
 * status text class on a parent and stroke/fill with `currentColor`.
 */

export type StatusToken =
  | "downloading"
  | "queued"
  | "paused"
  | "verifying"
  | "repairing"
  | "extracting"
  | "copying"
  | "completed"
  | "failed";

export type PriorityToken = "high" | "normal" | "low";

const STATUS_TO_TOKEN: Record<string, StatusToken> = {
  QUEUED: "queued",
  QUEUED_REPAIR: "queued",
  QUEUED_EXTRACT: "queued",
  DOWNLOADING: "downloading",
  PAUSED: "paused",
  CHECKING: "verifying",
  VERIFYING: "verifying",
  REPAIRING: "repairing",
  EXTRACTING: "extracting",
  MOVING: "copying",
  FINALIZING: "copying",
  COMPLETE: "completed",
  COMPLETED: "completed",
  FAILED: "failed",
  CANCELLED: "failed",
  INTERRUPTED: "failed",
};

/** i18n keys for status labels (see `lib/i18n/locales`). */
const STATUS_TO_I18N_KEY: Record<string, string> = {
  QUEUED: "status.queued",
  QUEUED_REPAIR: "status.queued",
  QUEUED_EXTRACT: "status.queued",
  DOWNLOADING: "status.downloading",
  PAUSED: "status.paused",
  CHECKING: "status.verifying",
  VERIFYING: "status.verifying",
  REPAIRING: "status.repairing",
  EXTRACTING: "status.extracting",
  MOVING: "status.moving",
  FINALIZING: "status.finalizing",
  COMPLETE: "status.complete",
  COMPLETED: "status.complete",
  FAILED: "status.failed",
  CANCELLED: "status.cancelled",
  INTERRUPTED: "status.failed",
};

const ACTIVE_STATUSES = new Set([
  "DOWNLOADING",
  "CHECKING",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "MOVING",
  "FINALIZING",
]);

const INDETERMINATE_STATUSES = new Set([
  "CHECKING",
  "VERIFYING",
  "REPAIRING",
  "QUEUED_REPAIR",
  "QUEUED_EXTRACT",
]);

export const STATUS_TEXT_CLASS: Record<StatusToken, string> = {
  downloading: "text-status-downloading",
  queued: "text-status-queued",
  paused: "text-status-paused",
  verifying: "text-status-verifying",
  repairing: "text-status-repairing",
  extracting: "text-status-extracting",
  copying: "text-status-copying",
  completed: "text-status-completed",
  failed: "text-status-failed",
};

export const STATUS_BG_CLASS: Record<StatusToken, string> = {
  downloading: "bg-status-downloading",
  queued: "bg-status-queued",
  paused: "bg-status-paused",
  verifying: "bg-status-verifying",
  repairing: "bg-status-repairing",
  extracting: "bg-status-extracting",
  copying: "bg-status-copying",
  completed: "bg-status-completed",
  failed: "bg-status-failed",
};

/** Soft 15% tint backgrounds for chips/badges. Literals kept for the Tailwind scanner. */
export const STATUS_SOFT_CLASS: Record<StatusToken, string> = {
  downloading: "bg-status-downloading/15",
  queued: "bg-status-queued/15",
  paused: "bg-status-paused/15",
  verifying: "bg-status-verifying/15",
  repairing: "bg-status-repairing/15",
  extracting: "bg-status-extracting/15",
  copying: "bg-status-copying/15",
  completed: "bg-status-completed/15",
  failed: "bg-status-failed/15",
};

export const PRIORITY_TEXT_CLASS: Record<PriorityToken, string> = {
  high: "text-priority-high",
  normal: "text-priority-normal",
  low: "text-priority-low",
};

export const PRIORITY_BG_CLASS: Record<PriorityToken, string> = {
  high: "bg-priority-high",
  normal: "bg-priority-normal",
  low: "bg-priority-low",
};

function normalizeStatus(status: string | null | undefined): string {
  return (status ?? "").toUpperCase();
}

export function statusToken(status: string | null | undefined): StatusToken {
  return STATUS_TO_TOKEN[normalizeStatus(status)] ?? "queued";
}

export function statusI18nKey(status: string | null | undefined): string {
  return STATUS_TO_I18N_KEY[normalizeStatus(status)] ?? "status.queued";
}

export function statusTextClass(status: string | null | undefined): string {
  return STATUS_TEXT_CLASS[statusToken(status)];
}

export function statusBgClass(status: string | null | undefined): string {
  return STATUS_BG_CLASS[statusToken(status)];
}

export function priorityToken(priority: string | null | undefined): PriorityToken {
  const value = normalizeStatus(priority);
  if (value === "HIGH") return "high";
  if (value === "LOW") return "low";
  return "normal";
}

export function isActiveStatus(status: string | null | undefined): boolean {
  return ACTIVE_STATUSES.has(normalizeStatus(status));
}

/**
 * How a status should drive the shared progress bar (handoff behavior):
 * - `determinate`: show the percentage fill (downloading, extracting/copying with a %).
 * - `indeterminate`: animated stripe fill, no % (verify/repair/checking).
 * - `empty`: queued — empty track, no fill.
 * - `complete`: full fill.
 */
export type ProgressKind = "determinate" | "indeterminate" | "empty" | "complete";

export function progressDisplayKind(
  status: string | null | undefined,
  progressFraction: number,
): ProgressKind {
  const value = normalizeStatus(status);
  if (value === "COMPLETE" || value === "COMPLETED") return "complete";
  if (value === "QUEUED") return "empty";
  if (INDETERMINATE_STATUSES.has(value) && progressFraction <= 0) return "indeterminate";
  return "determinate";
}

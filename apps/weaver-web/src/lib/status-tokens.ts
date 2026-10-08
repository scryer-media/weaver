/**
 * Single source of truth mapping backend job/pipeline statuses to semantic
 * status tokens. Screens resolve a token's colour through `next/data/palette`.
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

const STATUS_TO_TOKEN: Record<string, StatusToken> = {
  QUEUED: "queued",
  PROPAGATING: "queued",
  QUEUED_REPAIR: "queued",
  QUEUED_EXTRACT: "queued",
  DOWNLOADING: "downloading",
  FETCHING_REPAIR_DATA: "repairing",
  FINALIZING_DOWNLOAD: "downloading",
  PAUSED: "paused",
  CHECKING: "verifying",
  VERIFYING: "verifying",
  REPAIRING: "repairing",
  EXTRACTING: "extracting",
  POST_PROCESSING: "copying",
  AWAITING_QUEUE_SCRIPTS: "queued",
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
  PROPAGATING: "status.propagating",
  QUEUED_REPAIR: "status.queued",
  QUEUED_EXTRACT: "status.queued",
  DOWNLOADING: "status.downloading",
  FETCHING_REPAIR_DATA: "status.fetchingRepairData",
  FINALIZING_DOWNLOAD: "timeline.finalizingDownload",
  PAUSED: "status.paused",
  CHECKING: "status.verifying",
  VERIFYING: "status.verifying",
  REPAIRING: "status.repairing",
  EXTRACTING: "status.extracting",
  POST_PROCESSING: "status.postProcessing",
  AWAITING_QUEUE_SCRIPTS: "status.awaitingQueueScripts",
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
  "FETCHING_REPAIR_DATA",
  "FINALIZING_DOWNLOAD",
  "CHECKING",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "POST_PROCESSING",
  "MOVING",
  "FINALIZING",
]);

const INDETERMINATE_STATUSES = new Set([
  "AWAITING_QUEUE_SCRIPTS",
  "CHECKING",
  "FETCHING_REPAIR_DATA",
  "VERIFYING",
  "REPAIRING",
  "QUEUED_REPAIR",
  "QUEUED_EXTRACT",
]);

function normalizeStatus(status: string | null | undefined): string {
  return (status ?? "").toUpperCase();
}

export function statusToken(status: string | null | undefined): StatusToken {
  return STATUS_TO_TOKEN[normalizeStatus(status)] ?? "queued";
}

export function statusI18nKey(status: string | null | undefined): string {
  return STATUS_TO_I18N_KEY[normalizeStatus(status)] ?? "status.queued";
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
  if (value === "QUEUED" || value === "PROPAGATING") return "empty";
  if (INDETERMINATE_STATUSES.has(value) && progressFraction <= 0) return "indeterminate";
  return "determinate";
}

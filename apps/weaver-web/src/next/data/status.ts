import { useCallback } from "react";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { isActiveStatus, statusI18nKey, statusToken } from "@/lib/status-tokens";
import type { JobData } from "@/lib/job-types";
import { latestPhase } from "./phase-bars";

/**
 * Status vocabulary shared by every Next screen.
 *
 * Labels come from the same i18n keys the classic UI uses — the redesign is a
 * new interface, not a new set of words for weaver's pipeline states.
 */

export type DownloadGroup = "active" | "paused" | "queued" | "attention";

export const DOWNLOAD_GROUP_ORDER: readonly DownloadGroup[] = [
  "active",
  "paused",
  "queued",
  "attention",
];

/** Translation keys for each group's section label. */
export const DOWNLOAD_GROUP_LABEL: Record<DownloadGroup, string> = {
  active: "next.group.active",
  paused: "status.paused",
  queued: "status.queued",
  attention: "next.group.attention",
};

/** Translation keys for each group's section note; the in-progress group has none. */
export const DOWNLOAD_GROUP_NOTE: Record<DownloadGroup, string | null> = {
  active: null,
  paused: "next.group.note.paused",
  queued: "next.group.note.queued",
  attention: "next.group.note.attention",
};

export function downloadGroup(status: string): DownloadGroup {
  const token = statusToken(status);
  if (token === "paused") return "paused";
  if (token === "failed") return "attention";
  if (isActiveStatus(status)) return "active";
  return "queued";
}

export function useStatusLabel(): (status: string) => string {
  const t = useTranslate();
  return useCallback((status: string) => t(statusI18nKey(status)), [t]);
}

/**
 * The second line of the inspector's Status field: what the pipeline is doing
 * with this download right now, in weaver's own terms.
 */
export function statusDetail(
  t: Translate,
  job: JobData,
  /** The phase the screen is showing; by default, the one that last reported. */
  phase = latestPhase(job.phaseProgress),
  /** The status the screen labels the job with, which the detail does not repeat. */
  shownStatus = job.status,
): string | null {
  if (job.error) {
    return job.error;
  }
  if (job.downloadWaitReason) {
    const key = `next.status.wait.${job.downloadWaitReason.toLowerCase()}`;
    const label = t(key);
    return label === key ? job.downloadWaitReason.toLowerCase().replace(/_/g, " ") : label;
  }
  if (phase && phase.totalBytes > 0) {
    const percent = `${Math.round(phase.progressPercent)}%`;
    return phase.phase === shownStatus ? percent : t(`next.status.percentOf.${phase.phase}`, { percent });
  }
  return null;
}

/**
 * The colour an event kind takes in a job's log.
 *
 * Event kinds are open-ended — the engine adds them as the pipeline grows — so
 * this reads the name rather than enumerating it. Three tones only: something
 * went wrong, something finished well, something the engine decided (a retry,
 * a skipped recovery set). Everything else stays quiet.
 */
export function eventTone(kind: string): "bad" | "good" | "note" | "plain" {
  if (/FAIL|ERROR|MISSING|CANCEL|INTERRUPT|CORRUPT|DAMAGE/.test(kind)) {
    return "bad";
  }
  if (/COMPLETE|SUCCEED|PASSED|REPAIRED/.test(kind)) {
    return "good";
  }
  if (/RETRY|SKIP|REPAIR|PAUSE|RESUME|WAIT|PROPAGAT/.test(kind)) {
    return "note";
  }
  return "plain";
}

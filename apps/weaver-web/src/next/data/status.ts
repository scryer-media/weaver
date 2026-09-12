import { useCallback } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { isActiveStatus, statusI18nKey, statusToken } from "@/lib/status-tokens";
import type { JobData } from "@/lib/job-types";

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

export const DOWNLOAD_GROUP_LABEL: Record<DownloadGroup, string> = {
  active: "In progress",
  paused: "Paused",
  queued: "Queued",
  attention: "Needs attention",
};

export const DOWNLOAD_GROUP_NOTE: Record<DownloadGroup, string> = {
  active: "",
  paused: "resume to continue",
  queued: "starts when a slot frees",
  attention: "cancelled or failed",
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
export function statusDetail(job: JobData): string | null {
  if (job.error) {
    return job.error;
  }
  if (job.downloadWaitReason) {
    return job.downloadWaitReason.toLowerCase().replace(/_/g, " ");
  }
  const phase = currentPhase(job);
  if (phase && phase.totalBytes > 0) {
    return `${Math.round(phase.progressPercent)}% of ${phase.phase.toLowerCase()}`;
  }
  return null;
}

/** The phase that last reported progress; `phaseProgress` is ordered by start. */
export function currentPhase(job: JobData) {
  let latest = null as JobData["phaseProgress"][number] | null;
  for (const phase of job.phaseProgress) {
    if (!latest || phase.updatedAtEpochMs >= latest.updatedAtEpochMs) {
      latest = phase;
    }
  }
  return latest;
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

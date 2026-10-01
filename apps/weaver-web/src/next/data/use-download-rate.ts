import { useLiveJobDownloadRate } from "@/lib/context/live-data-context";
import type { JobData } from "@/lib/job-types";

/**
 * A job's download rate in bytes per second, 0 when it is not transferring.
 *
 * Read from the metrics push when one is in hand, so a row agrees with the
 * speed counter on the same tick; otherwise from the queue item's download
 * phase, which lags it by up to a second.
 */
export function useDownloadRate(job: Pick<JobData, "id" | "phaseProgress">): number {
  const live = useLiveJobDownloadRate(job.id);
  if (live !== undefined) {
    return live ?? 0;
  }
  const phase = job.phaseProgress.find((candidate) => candidate.phase === "DOWNLOADING");
  return phase?.rateBps ?? 0;
}

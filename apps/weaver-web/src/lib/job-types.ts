// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export interface DeleteOperationData {
  operationId: number;
  state: "QUEUED" | "RUNNING" | "FAILED";
  locked: boolean;
  deleteFiles: boolean;
  errorMessage?: string | null;
  /** Selected where it is read: the job page. */
  action?: "DELETE" | "ARCHIVE";
  archiveOutcome?:
    | "ARCHIVED"
    | "ALREADY_GONE"
    | "NOT_MANAGED"
    | "BUSY"
    | "NOT_FOUND"
    | "FAILED"
    | null;
}

export type JobPhase = "DOWNLOADING" | "REPAIRING" | "EXTRACTING" | "MOVING";

export interface JobPhaseProgressData {
  phase: JobPhase;
  completedBytes: number;
  totalBytes: number;
  progressPercent: number;
  rateBps?: number | null;
  estimatedRemainingMs?: number | null;
  startedAtEpochMs: number;
  updatedAtEpochMs: number;
}

/** How a finished job ended. `status` shows a partial delivery as failed. */
export type JobOutcome = "COMPLETE" | "PARTIAL" | "FAILED";

/** How a job's content is fetched. */
export type JobProtocol = "NZB" | "TORRENT";

export interface JobData {
  id: number;
  name: string;
  /** Absent from an older daemon, which only fetches NZBs. */
  protocol?: JobProtocol;
  displayTitle: string;
  originalTitle: string;
  status: string;
  downloadWaitReason?: string | null;
  downloadRetryAtEpochMs?: number | null;
  /** While the job waits to start for its disk: the bytes it still has to fetch; queue rows only. */
  diskNeedBytes?: number | null;
  /** The room its disk leaves for them, or for `disk_too_small` the most the disk holds; queue rows only. */
  diskFreeBytes?: number | null;
  /** The slow volume holding the download back, while one does; queue rows only. */
  /**
   * The job's write to its disk while the disk is slow, or while it has
   * nothing left to fetch and bytes still waiting; null otherwise. The
   * figures are the disk's, from every job that writes there.
   */
  diskWrite?: DiskWrite | null;
  progress: number;
  progressPercent?: number | null;
  totalBytes: number;
  downloadedBytes: number;
  optionalRecoveryBytes: number;
  optionalRecoveryDownloadedBytes: number;
  /** Declared bytes still to come, recovery held back until a loss asks left out; queue rows only. */
  remainingBytes?: number;
  /** Declared bytes the download is out to bring; queue rows only. */
  wantedBytes?: number;
  phaseProgress: JobPhaseProgressData[];
  failedBytes: number;
  health: number;
  hasPassword: boolean;
  category: string | null;
  /** The job's tags, in tag order. */
  tags?: string[];
  createdAt?: number | null;
  completedAt?: number | null;
  error?: string | null;
  /** How the job ended, a partial delivery as its own value; selected where it is read: the job page. */
  outcome?: JobOutcome | null;
  /** Failed for a decode window over the memory profile's ceiling; history rows only. */
  failedForMemory?: boolean;
  outputDir?: string | null;
  /** When the entry was archived, or null while it is not; history rows only. */
  archivedAt?: string | number | null;
  metadata: { key: string; value: string }[];
  deleteOperation?: DeleteOperationData | null;
}

export interface GraphqlJobData extends Omit<
  JobData,
  "progress" | "phaseProgress" | "createdAt" | "completedAt" | "metadata"
> {
  progress?: number | null;
  progressPercent?: number | null;
  createdAt?: string | number | null;
  completedAt?: string | number | null;
  metadata?: { key: string; value: string }[];
  attributes?: { key: string; value: string }[];
  phaseProgress?: JobPhaseProgressData[] | null;
}

export function normalizeFacadeJobStatus(status: string): string {
  switch (status) {
    case "COMPLETED":
      return "COMPLETE";
    case "FINALIZING":
      return "MOVING";
    case "CHECKING":
      return "VERIFYING";
    default:
      return status;
  }
}

export function normalizeFacadeJobProgress(
  progressPercent?: number | null,
  progress?: number | null,
): number {
  if (typeof progressPercent === "number" && Number.isFinite(progressPercent)) {
    return progressPercent / 100;
  }
  if (typeof progress === "number" && Number.isFinite(progress)) {
    return progress;
  }
  return 0;
}

export function normalizeGraphqlTimestamp(
  value?: string | number | null,
): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const epochMs = Date.parse(value);
    return Number.isFinite(epochMs) ? epochMs : null;
  }
  return null;
}

export function normalizeJobData<T extends GraphqlJobData>(
  job: T,
): T & JobData {
  return {
    ...job,
    status:
      job.downloadWaitReason === "propagation_delay" &&
      (job.status === "QUEUED" || job.status === "DOWNLOADING")
        ? "PROPAGATING"
        : normalizeFacadeJobStatus(job.status),
    progress: normalizeFacadeJobProgress(job.progressPercent, job.progress),
    phaseProgress: job.phaseProgress ?? [],
    createdAt: normalizeGraphqlTimestamp(job.createdAt),
    completedAt: normalizeGraphqlTimestamp(job.completedAt),
    metadata: job.metadata ?? job.attributes ?? [],
  };
}

export function formatJobReleaseName(
  job: Pick<JobData, "name" | "originalTitle" | "displayTitle">,
): string {
  const releaseName = job.originalTitle.trim() || job.name.trim();
  if (!releaseName) {
    return job.displayTitle;
  }

  return releaseName.replaceAll(".", " ").replace(/\s+/g, " ").trim();
}

/**
 * A job's write to its destination disk, as the queue rows carry it. The
 * disk's figures come with the live metrics, under the same label.
 */
export interface DiskWrite {
  /** The disk, by the label the live disk list gives it. */
  volume: string;
  /** Downloads keep waiting on the disk. */
  slow: boolean;
  /** Nothing is left to fetch and bytes still wait in memory for the disk. */
  draining: boolean;
}

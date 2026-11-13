// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export interface DeleteOperationData {
  operationId: number;
  state: "QUEUED" | "RUNNING" | "FAILED";
  locked: boolean;
  deleteFiles: boolean;
  errorMessage?: string | null;
  /** Selected where it is read: the job page. */
  action?: "DELETE" | "REMOVE_FILES";
  filesRemovalOutcome?: "REMOVED" | "ALREADY_GONE" | "NOT_MANAGED" | "BUSY" | "NOT_FOUND" | "FAILED" | null;
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

export interface JobData {
  id: number;
  name: string;
  displayTitle: string;
  originalTitle: string;
  status: string;
  downloadWaitReason?: string | null;
  downloadRetryAtEpochMs?: number | null;
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
  createdAt?: number | null;
  completedAt?: number | null;
  error?: string | null;
  /** Failed for a decode window over the memory profile's ceiling; history rows only. */
  failedForMemory?: boolean;
  outputDir?: string | null;
  /** When the entry's files were removed, or null while they are kept; history rows only. */
  filesRemovedAt?: string | number | null;
  metadata: { key: string; value: string }[];
  deleteOperation?: DeleteOperationData | null;
}

export interface GraphqlJobData extends Omit<JobData, "progress" | "phaseProgress" | "createdAt" | "completedAt" | "metadata"> {
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

export function normalizeGraphqlTimestamp(value?: string | number | null): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const epochMs = Date.parse(value);
    return Number.isFinite(epochMs) ? epochMs : null;
  }
  return null;
}

export function normalizeJobData<T extends GraphqlJobData>(job: T): T & JobData {
  return {
    ...job,
    status: job.downloadWaitReason === "propagation_delay"
      && (job.status === "QUEUED" || job.status === "DOWNLOADING")
      ? "PROPAGATING"
      : normalizeFacadeJobStatus(job.status),
    progress: normalizeFacadeJobProgress(job.progressPercent, job.progress),
    phaseProgress: job.phaseProgress ?? [],
    createdAt: normalizeGraphqlTimestamp(job.createdAt),
    completedAt: normalizeGraphqlTimestamp(job.completedAt),
    metadata: job.metadata ?? job.attributes ?? [],
  };
}

export function formatJobReleaseName(job: Pick<JobData, "name" | "originalTitle" | "displayTitle">): string {
  const releaseName = job.originalTitle.trim() || job.name.trim();
  if (!releaseName) {
    return job.displayTitle;
  }

  return releaseName.replaceAll(".", " ").replace(/\s+/g, " ").trim();
}

type QueueActivity = {
  status?: string;
  phaseProgress: ReadonlyArray<{
    rateBps?: number | null;
  }>;
};

function queueDownloadRank(job: QueueActivity): number {
  return job.status === "DOWNLOADING" || job.status === "FETCHING_REPAIR_DATA" ? 1 : 0;
}

export function queueLiveRate(job: QueueActivity): number {
  return job.phaseProgress.reduce((maximum, phase) => {
    const rate = phase.rateBps;
    return typeof rate === "number" && Number.isFinite(rate) && rate > maximum ? rate : maximum;
  }, 0);
}

/** Keeps the scheduler's order for jobs with the same observed activity. */
export function orderQueueByLiveActivity<T extends QueueActivity>(jobs: readonly T[]): T[] {
  return jobs
    .map((job, index) => ({ job, index, rank: queueDownloadRank(job), rate: queueLiveRate(job) }))
    .sort((left, right) => right.rank - left.rank || right.rate - left.rate || left.index - right.index)
    .map(({ job }) => job);
}

/** Retains the server's column order within each transfer tier. */
export function prioritizeDownloadingJobs<T extends QueueActivity>(jobs: readonly T[]): T[] {
  return jobs
    .map((job, index) => ({ job, index, rank: queueDownloadRank(job) }))
    .sort((left, right) => right.rank - left.rank || left.index - right.index)
    .map(({ job }) => job);
}

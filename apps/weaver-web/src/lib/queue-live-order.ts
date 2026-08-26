type QueueActivity = {
  phaseProgress: ReadonlyArray<{
    rateBps?: number | null;
  }>;
};

export function queueLiveRate(job: QueueActivity): number {
  return job.phaseProgress.reduce((maximum, phase) => {
    const rate = phase.rateBps;
    return typeof rate === "number" && Number.isFinite(rate) && rate > maximum ? rate : maximum;
  }, 0);
}

/** Keeps the scheduler's order for jobs with the same observed activity. */
export function orderQueueByLiveActivity<T extends QueueActivity>(jobs: readonly T[]): T[] {
  return jobs
    .map((job, index) => ({ job, index, rate: queueLiveRate(job) }))
    .sort((left, right) => right.rate - left.rate || left.index - right.index)
    .map(({ job }) => job);
}

/**
 * Per-job download rates carried by the live metrics push.
 *
 * The nav speed counter and a queue row's "Downloading · x MB/s" label used to
 * arrive over two channels with two cadences: the counter on the 250 ms
 * `systemMetricsUpdates` push, the row on queue events published at most once
 * a second. Even with both fed by the same estimator server-side, the row was
 * up to a second stale against the counter and the two could read far apart.
 *
 * This store holds the rates that ride the same push as the counter, so a row
 * reading from it shows the figure sampled on the same tick. Rows subscribe
 * per job: a snapshot only re-renders the rows whose rate actually changed.
 */

export interface JobDownloadRate {
  jobId: number;
  rateBps: number;
}

/**
 * `undefined` means no live snapshot is in hand (before the first push, or
 * while the UI is on the polled fallback), so the caller should fall back to
 * the rate carried by the queue item. `null` means the live snapshot is in
 * hand and this job is not transferring.
 */
export type LiveJobDownloadRate = number | null | undefined;

export interface LiveJobDownloadRatesStore {
  getRate: (jobId: number | null | undefined) => LiveJobDownloadRate;
  setRates: (rates: readonly JobDownloadRate[] | undefined) => void;
  subscribe: (listener: () => void) => () => void;
}

export function createLiveJobDownloadRatesStore(): LiveJobDownloadRatesStore {
  let current: readonly JobDownloadRate[] | undefined;
  let ratesById = new Map<number, number>();
  const listeners = new Set<() => void>();

  return {
    getRate: (jobId) => {
      if (current === undefined) {
        return undefined;
      }
      if (typeof jobId !== "number" || !Number.isFinite(jobId)) {
        return null;
      }
      return ratesById.get(jobId) ?? null;
    },
    setRates: (rates) => {
      if (rates === current) {
        return;
      }
      current = rates;
      ratesById = new Map((rates ?? []).map((rate) => [rate.jobId, rate.rateBps] as const));
      for (const listener of listeners) {
        listener();
      }
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

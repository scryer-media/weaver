import { useRef } from "react";

export interface EtaJobInput {
  id: number;
  status: string;
  totalBytes: number;
  downloadedBytes: number;
}

interface EtaSample {
  at: number;
  downloadedBytes: number;
}

interface EtaEstimatorState {
  signature: string;
  samples: EtaSample[];
  smoothedSpeed: number;
  displayedSpeed: number;
  displayedAt: number;
}

const ACTIVE_ETA_STATUSES = new Set(["DOWNLOADING", "QUEUED"]);
const SAMPLE_INTERVAL_MS = 400;
const DISPLAY_INTERVAL_MS = 1_000;
const MIN_WINDOW_MS = 8_000;
const MAX_WINDOW_MS = 120_000;
const MAX_SAMPLE_AGE_MS = 180_000;
const WARMUP_MS = 30_000;
const SETTLED_DEVIATION = 0.2;

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function activeEtaJobs(jobs: EtaJobInput[]): EtaJobInput[] {
  return jobs.filter((job) => ACTIVE_ETA_STATUSES.has(job.status));
}

function buildQueueSignature(jobs: EtaJobInput[]): string {
  return jobs.map((job) => `${job.id}:${job.status}:${job.totalBytes}`).join("|");
}

function sumDownloadedBytes(jobs: EtaJobInput[]): number {
  return jobs.reduce((sum, job) => sum + job.downloadedBytes, 0);
}

function trimSamples(samples: EtaSample[], now: number): EtaSample[] {
  const cutoff = now - MAX_SAMPLE_AGE_MS;
  const trimmed = samples.filter((sample, index) => index === samples.length - 1 || sample.at >= cutoff);
  return trimmed.length > 0 ? trimmed : samples.slice(-1);
}

function comparisonSample(samples: EtaSample[], now: number): EtaSample {
  const first = samples[0];
  const elapsedMs = Math.max(now - first.at, 0);
  const windowMs = clamp(elapsedMs * 0.92, MIN_WINDOW_MS, MAX_WINDOW_MS);
  const cutoff = now - windowMs;
  return samples.find((sample) => sample.at >= cutoff) ?? first;
}

function observedSpeed(samples: EtaSample[], now: number): number {
  if (samples.length < 2) {
    return 0;
  }

  const baseline = comparisonSample(samples, now);
  const latest = samples[samples.length - 1];
  const deltaBytes = Math.max(latest.downloadedBytes - baseline.downloadedBytes, 0);
  const deltaSeconds = Math.max((latest.at - baseline.at) / 1_000, 0);
  if (deltaBytes <= 0 || deltaSeconds <= 0) {
    return 0;
  }

  return deltaBytes / deltaSeconds;
}

function nextEtaSpeed(
  state: EtaEstimatorState,
  jobs: EtaJobInput[],
  instantaneousSpeed: number,
  now: number,
): number {
  const activeJobs = activeEtaJobs(jobs);
  if (activeJobs.length === 0) {
    state.signature = "";
    state.samples = [];
    state.smoothedSpeed = 0;
    state.displayedSpeed = 0;
    state.displayedAt = now;
    return 0;
  }

  const signature = buildQueueSignature(activeJobs);
  const downloadedBytes = sumDownloadedBytes(activeJobs);
  const lastSample = state.samples[state.samples.length - 1];
  const queueChanged = signature !== state.signature;
  const bytesWentBackwards = !!lastSample && downloadedBytes < lastSample.downloadedBytes;

  if (queueChanged || bytesWentBackwards || state.samples.length === 0) {
    state.signature = signature;
    state.samples = [{ at: now, downloadedBytes }];
    state.smoothedSpeed = Math.max(instantaneousSpeed, 0);
    state.displayedSpeed = state.smoothedSpeed;
    state.displayedAt = now;
    return state.displayedSpeed;
  }

  if (
    !lastSample
    || downloadedBytes !== lastSample.downloadedBytes
    || now - lastSample.at >= SAMPLE_INTERVAL_MS
  ) {
    state.samples = trimSamples([...state.samples, { at: now, downloadedBytes }], now);
  }

  const first = state.samples[0];
  const elapsedMs = Math.max(now - first.at, 0);
  const warmupWeight = 1 - clamp(elapsedMs / WARMUP_MS, 0, 1);
  const longWindowSpeed = observedSpeed(state.samples, now);

  let targetSpeed = Math.max(longWindowSpeed, instantaneousSpeed, 0);
  if (longWindowSpeed > 0 && instantaneousSpeed > 0) {
    const shortWindowWeight = 0.15 + warmupWeight * 0.6;
    const longWindowWeight = 1 - shortWindowWeight;
    const blended = instantaneousSpeed * shortWindowWeight + longWindowSpeed * longWindowWeight;
    if (warmupWeight < 0.45) {
      const floor = longWindowSpeed * (1 - SETTLED_DEVIATION);
      const ceiling = longWindowSpeed * (1 + SETTLED_DEVIATION);
      targetSpeed = clamp(blended, floor, ceiling);
    } else {
      targetSpeed = blended;
    }
  } else if (longWindowSpeed > 0) {
    targetSpeed = longWindowSpeed;
  }

  if (targetSpeed <= 0) {
    state.smoothedSpeed *= elapsedMs < WARMUP_MS ? 0.78 : 0.92;
    if (state.smoothedSpeed < 1) {
      state.smoothedSpeed = 0;
    }
    if (now - state.displayedAt >= DISPLAY_INTERVAL_MS) {
      state.displayedSpeed = state.smoothedSpeed;
      state.displayedAt = now;
    }
    return state.displayedSpeed;
  }

  const settled = clamp(elapsedMs / WARMUP_MS, 0, 1);
  const alpha = 0.22 - settled * 0.14;
  state.smoothedSpeed = state.smoothedSpeed > 0
    ? state.smoothedSpeed + alpha * (targetSpeed - state.smoothedSpeed)
    : targetSpeed;
  if (now - state.displayedAt >= DISPLAY_INTERVAL_MS) {
    state.displayedSpeed = state.smoothedSpeed;
    state.displayedAt = now;
  }
  return state.displayedSpeed;
}

export function formatEtaFromRemainingBytes(remainingBytes: number, speed: number): string {
  if (speed <= 0 || remainingBytes <= 0) return "\u2014";
  const secs = Math.ceil(remainingBytes / speed);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

function buildQueueEtaById(jobs: EtaJobInput[], speed: number): Map<number, string> {
  const etaById = new Map<number, string>();
  if (speed <= 0) {
    return etaById;
  }

  let bytesAhead = 0;
  for (const job of activeEtaJobs(jobs)) {
    const remaining = Math.max(job.totalBytes - job.downloadedBytes, 0);
    bytesAhead += remaining;
    etaById.set(job.id, formatEtaFromRemainingBytes(bytesAhead, speed));
  }

  return etaById;
}

export function useStableEtaSpeed(jobs: EtaJobInput[], instantaneousSpeed: number): number {
  const stateRef = useRef<EtaEstimatorState>({
    signature: "",
    samples: [],
    smoothedSpeed: 0,
    displayedSpeed: 0,
    displayedAt: 0,
  });

  // eslint-disable-next-line react-hooks/purity -- sampling is intentionally driven by wall-clock time
  return nextEtaSpeed(stateRef.current, jobs, instantaneousSpeed, Date.now());
}

export function useStableQueueEta(jobs: EtaJobInput[], instantaneousSpeed: number): Map<number, string> {
  const etaSpeed = useStableEtaSpeed(jobs, instantaneousSpeed);
  return buildQueueEtaById(jobs, etaSpeed);
}
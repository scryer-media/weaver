// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState } from "react";

export interface EtaJobInput {
  id: number;
  status: string;
  totalBytes: number;
  downloadedBytes: number;
  /** What the daemon says is still to come, recovery it holds back left out. */
  remainingBytes: number;
}

/** What the estimate cannot see in the bytes: the queue held, or a new limit. */
export interface EtaConditions {
  /** Everything is held: nothing moves until it resumes. */
  paused: boolean;
  /** The speed limit in force in bytes per second, 0 for none. */
  speedLimit: number;
  /** No job is downloading: the last one finished, or what is left waits. Left out, one is. */
  idle?: boolean;
  /**
   * The write rate of a slow disk that holds bytes waiting, in bytes per
   * second, 0 or left out for none. The queue drains no slower than that
   * disk writes, so the speed is not eased below it.
   */
  diskRate?: number;
}

const FREE_RUNNING: EtaConditions = { paused: false, speedLimit: 0 };

interface EtaSample {
  at: number;
  downloadedBytes: number;
}

export interface EtaEstimatorState {
  signature: string;
  speedLimit: number;
  samples: EtaSample[];
  smoothedSpeed: number;
  displayedSpeed: number;
  displayedAt: number;
  /** The highest smoothed speed since the estimate last started again. */
  peakSpeed: number;
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

// Which jobs are counted, not how big they are: a total that moves, as a
// job's recovery is parked or asked for, leaves the speed as it was.
function buildQueueSignature(jobs: EtaJobInput[]): string {
  return jobs.map((job) => `${job.id}:${job.status}`).join("|");
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

export function createEtaEstimator(): EtaEstimatorState {
  return {
    signature: "",
    speedLimit: 0,
    samples: [],
    smoothedSpeed: 0,
    displayedSpeed: 0,
    displayedAt: 0,
    peakSpeed: 0,
  };
}

// No speed is shown above the limit, which the daemon holds the wire to.
function capped(speed: number, speedLimit: number): number {
  return speedLimit > 0 ? Math.min(speed, speedLimit) : speed;
}

// While a slow disk holds bytes waiting, downloads stand behind its writer
// and the bytes come in bursts. The queue still drains at that disk's rate,
// so the speed shown stays at or above it, and above no speed this run of
// samples has reached: a disk faster than the wire lifts nothing.
export function nextEtaSpeed(
  state: EtaEstimatorState,
  jobs: EtaJobInput[],
  instantaneousSpeed: number,
  now: number,
  conditions: EtaConditions = FREE_RUNNING,
): number {
  // A new limit, a pause or an emptied queue starts the peak again. A queue
  // whose jobs only wait (behind a full disk, say) keeps it: the wire is as
  // fast when they go on, and the samples start again from nothing.
  if (
    conditions.speedLimit !== state.speedLimit
    || conditions.paused
    || activeEtaJobs(jobs).length === 0
  ) {
    state.peakSpeed = 0;
  }
  const eased = easedEtaSpeed(state, jobs, instantaneousSpeed, now, conditions);
  if (state.samples.length === 0) {
    return eased;
  }
  state.peakSpeed = Math.max(state.peakSpeed, state.smoothedSpeed);
  const diskRate = conditions.diskRate ?? 0;
  const floor = diskRate > 0 ? capped(Math.min(diskRate, state.peakSpeed), conditions.speedLimit) : 0;
  return Math.max(eased, floor);
}

function easedEtaSpeed(
  state: EtaEstimatorState,
  jobs: EtaJobInput[],
  instantaneousSpeed: number,
  now: number,
  conditions: EtaConditions,
): number {
  // A new limit makes the bytes before it no guide to the speed after it,
  // so the estimate starts again from the next samples.
  const limitChanged = conditions.speedLimit !== state.speedLimit;
  state.speedLimit = conditions.speedLimit;
  const activeJobs = activeEtaJobs(jobs);
  // Held, or nothing downloading: the speed is 0 now, not once the easing
  // decays, and a resume starts from fresh samples.
  if (conditions.paused || conditions.idle || activeJobs.length === 0) {
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

  if (queueChanged || bytesWentBackwards || limitChanged || state.samples.length === 0) {
    state.signature = signature;
    state.samples = [{ at: now, downloadedBytes }];
    state.smoothedSpeed = capped(Math.max(instantaneousSpeed, 0), conditions.speedLimit);
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
  targetSpeed = capped(targetSpeed, conditions.speedLimit);
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

export function buildQueueEtaById(jobs: EtaJobInput[], speed: number): Map<number, string> {
  const etaById = new Map<number, string>();
  if (speed <= 0) {
    return etaById;
  }

  let bytesAhead = 0;
  for (const job of activeEtaJobs(jobs)) {
    bytesAhead += Math.max(job.remainingBytes, 0);
    etaById.set(job.id, formatEtaFromRemainingBytes(bytesAhead, speed));
  }

  return etaById;
}

export function useStableEtaSpeed(
  jobs: EtaJobInput[],
  instantaneousSpeed: number,
  conditions: EtaConditions = FREE_RUNNING,
): number {
  // Every render is a sample, the same jobs and speed included: the estimate
  // settles with the time between them, so it must not be cached by its inputs.
  "use no memo";
  const [state] = useState<EtaEstimatorState>(createEtaEstimator);

  // eslint-disable-next-line react-hooks/purity -- sampling is intentionally driven by wall-clock time
  return nextEtaSpeed(state, jobs, instantaneousSpeed, Date.now(), conditions);
}

export function useStableQueueEta(
  jobs: EtaJobInput[],
  instantaneousSpeed: number,
  conditions: EtaConditions = FREE_RUNNING,
): Map<number, string> {
  const etaSpeed = useStableEtaSpeed(jobs, instantaneousSpeed, conditions);
  return useMemo(() => buildQueueEtaById(jobs, etaSpeed), [jobs, etaSpeed]);
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";

import {
  buildQueueEtaById,
  createEtaEstimator,
  nextEtaSpeed,
  type EtaConditions,
  type EtaEstimatorState,
} from "../src/lib/hooks/use-stable-queue-eta.ts";

const MB = 1_000_000;
const TICK_MS = 1_000;
const TOTAL = 100_000 * MB;

// The queue as the page samples it: one job holding the daemon's totals.
function sample(downloaded: number) {
  return [
    { id: 0, status: "DOWNLOADING", totalBytes: TOTAL, downloadedBytes: downloaded, remainingBytes: TOTAL - downloaded },
  ];
}

// Drives the estimator one display tick at a time on a clock the test owns.
class Clock {
  now = 0;
  downloaded = 0;
  readonly state: EtaEstimatorState = createEtaEstimator();

  tick(rate: number, conditions: EtaConditions, shown = rate): number {
    this.now += TICK_MS;
    this.downloaded += (rate * TICK_MS) / 1_000;
    return nextEtaSpeed(this.state, sample(this.downloaded), shown, this.now, conditions);
  }

  run(seconds: number, rate: number, conditions: EtaConditions): number {
    let speed = 0;
    for (let at = 0; at < seconds; at += 1) {
      speed = this.tick(rate, conditions);
    }
    return speed;
  }
}

const FREE: EtaConditions = { paused: false, speedLimit: 0 };

test("each job's time left counts what the daemon says is still to come", () => {
  const etaById = buildQueueEtaById(
    [
      // Every wanted article is in; only held-back recovery is undone.
      { id: 1, status: "DOWNLOADING", totalBytes: 1_000, downloadedBytes: 870, remainingBytes: 0 },
      { id: 2, status: "QUEUED", totalBytes: 500, downloadedBytes: 0, remainingBytes: 150 },
    ],
    50,
  );

  assert.equal(etaById.get(1), "—");
  assert.equal(etaById.get(2), "3s");
});

test("a paused queue reads 0 on the next tick and starts afresh on resume", () => {
  const clock = new Clock();
  assert.ok(clock.run(60, 10 * MB, FREE) > 9 * MB);

  // The daemon's own rate still trails the pause.
  assert.equal(clock.tick(0, { paused: true, speedLimit: 0 }, 8 * MB), 0);
  assert.equal(clock.tick(0, { paused: true, speedLimit: 0 }, 3 * MB), 0);

  // Resumed at a different rate: the first tick reads it, not the old one.
  assert.equal(clock.tick(4 * MB, FREE), 4 * MB);
  assert.ok(Math.abs(clock.run(30, 4 * MB, FREE) - 4 * MB) < 0.05 * 4 * MB);
});

test("a stall with no known cause keeps easing down", () => {
  const clock = new Clock();
  clock.run(60, 10 * MB, FREE);

  const next = clock.tick(0, FREE);
  assert.ok(next > 0);
  assert.ok(next < 10 * MB);
});

test("a lowered limit reads at or below the new limit on the next tick", () => {
  const clock = new Clock();
  clock.run(60, 10 * MB, FREE);

  // The daemon's rate still shows the old speed for this tick.
  const next = clock.tick(2 * MB, { paused: false, speedLimit: 2 * MB }, 10 * MB);
  assert.ok(next <= 2 * MB, `${next}`);
  assert.ok(clock.run(30, 2 * MB, { paused: false, speedLimit: 2 * MB }) <= 2 * MB);
});

test("a raised limit reaches the new rate within the warm-up", () => {
  const clock = new Clock();
  clock.run(120, 2 * MB, { paused: false, speedLimit: 2 * MB });

  const raised: EtaConditions = { paused: false, speedLimit: 10 * MB };
  // The daemon's rate trails by a tick.
  clock.tick(10 * MB, raised, 2 * MB);
  const settled = clock.run(30, 10 * MB, raised);
  assert.ok(settled >= 9.5 * MB, `${settled}`);
});

test("a removed limit starts the window again", () => {
  const clock = new Clock();
  clock.run(120, 2 * MB, { paused: false, speedLimit: 2 * MB });

  clock.tick(10 * MB, FREE, 2 * MB);
  assert.ok(clock.run(30, 10 * MB, FREE) >= 9.5 * MB);
});

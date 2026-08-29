import assert from "node:assert/strict";
import test from "node:test";

import {
  orderQueueByLiveActivity,
  prioritizeDownloadingJobs,
} from "../src/lib/queue-live-order.ts";

test("queue live order puts every transferring job ahead of cold jobs", () => {
  const jobs = [
    { id: "cold-first", phaseProgress: [] },
    { id: "slow", phaseProgress: [{ rateBps: 10_000 }] },
    { id: "fast", phaseProgress: [{ rateBps: 31_700_000 }] },
    { id: "cold-second", phaseProgress: [] },
  ];

  assert.deepEqual(
    orderQueueByLiveActivity(jobs).map(({ id }) => id),
    ["fast", "slow", "cold-first", "cold-second"],
  );
});

test("queue live order preserves scheduler order for equal rates", () => {
  const jobs = [
    { id: "first", phaseProgress: [{ rateBps: 0 }] },
    { id: "second", phaseProgress: [] },
    { id: "third", phaseProgress: [{ rateBps: 0 }] },
  ];

  assert.deepEqual(
    orderQueueByLiveActivity(jobs).map(({ id }) => id),
    ["first", "second", "third"],
  );
});

test("queue column sorts retain downloading jobs above queued jobs", () => {
  const jobs = [
    { id: "queued-first", status: "QUEUED", phaseProgress: [] },
    { id: "downloading", status: "DOWNLOADING", phaseProgress: [{ rateBps: 0 }] },
    { id: "queued-second", status: "QUEUED", phaseProgress: [] },
  ];

  assert.deepEqual(
    prioritizeDownloadingJobs(jobs).map(({ id }) => id),
    ["downloading", "queued-first", "queued-second"],
  );
});

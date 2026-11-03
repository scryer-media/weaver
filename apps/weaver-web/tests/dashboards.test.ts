// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import type { DownloadBlockState } from "../src/lib/context/live-data-context.tsx";
import type { ProviderHealth } from "../src/next/data/next-data.tsx";
import {
  averageDailySpeed,
  barHeights,
  dayKey,
  downloadsState,
  foundPercent,
  lastDays,
  providerRows,
  providersUp,
  secondsInDay,
  sumDays,
  type UsageConsumer,
  type UsageDay,
} from "../src/next/data/dashboards.ts";
import { englishTranslate as t } from "./english-translate.ts";

const NOW = new Date(2026, 10, 3, 9, 30).getTime();

function day(date: string, bytes: number, tried = 0, succeeded = 0, down = 0): UsageDay {
  return { day: date, bytes, articlesTried: tried, articlesSucceeded: succeeded, downSeconds: down };
}

function provider(overrides: Partial<ProviderHealth> = {}): ProviderHealth {
  return {
    host: "news.example.test",
    port: 563,
    label: "news.example.test:563",
    tier: "PRIMARY",
    state: "healthy",
    activity: "downloading",
    activityUntilEpochMs: null,
    connectionsOpen: 40,
    connectionsBusy: 38,
    connectionsActive: 39,
    connectionsMax: 100,
    connectionsConfigured: 100,
    capacityPenaltyUntilEpochMs: null,
    latencyMs: 0,
    bodyLatencyMs: null,
    bodyLatencyBand: null,
    successCount: 0,
    failureCount: 0,
    consecutiveFailures: 0,
    prematureDeaths: 0,
    ...overrides,
  };
}

function consumer(id: string, rangeBytes: number): UsageConsumer {
  const totals = day("", rangeBytes);
  return {
    kind: "server",
    id,
    today: totals,
    week: totals,
    month: totals,
    year: totals,
    allTime: totals,
    range: totals,
    days: [],
  };
}

function block(overrides: Partial<DownloadBlockState> = {}): DownloadBlockState {
  return {
    kind: "NONE",
    usedBytes: 0,
    limitBytes: 0,
    remainingBytes: 0,
    timezoneName: "UTC",
    scheduledSpeedLimit: null,
    ...overrides,
  };
}

test("days add up field by field", () => {
  const total = sumDays([day("2026-11-01", 100, 10, 9, 60), day("2026-11-02", 50, 5, 5, 0)]);
  assert.deepEqual(
    { ...total },
    { bytes: 150, articlesTried: 15, articlesSucceeded: 14, downSeconds: 60 },
  );
  assert.equal(sumDays([]).bytes, 0);
});

test("found is succeeded over tried, and nothing tried is no figure", () => {
  assert.equal(foundPercent(day("", 0, 200, 199)), 99.5);
  assert.equal(foundPercent(day("", 0, 0, 0)), null);
});

test("the last thirty days end today and start twenty-nine days back", () => {
  assert.deepEqual(lastDays(NOW, 30), { fromDay: "2026-10-05", toDay: "2026-11-03" });
  assert.equal(dayKey(NOW), "2026-11-03");
});

test("a day has its real seconds when the clocks change", () => {
  assert.equal(secondsInDay("2026-11-03"), 86_400);
  const springForward = new Date(2026, 2, 8).getTimezoneOffset() !== new Date(2026, 2, 9).getTimezoneOffset();
  const lengths = ["2026-03-08", "2026-03-29"].map(secondsInDay);
  // Whatever zone the test runs in, every day is 23, 24 or 25 hours.
  for (const length of lengths) assert.ok([82_800, 86_400, 90_000].includes(length));
  if (springForward) assert.equal(secondsInDay("2026-03-08"), 82_800);
});

test("bars are scaled to the tallest day", () => {
  assert.deepEqual(barHeights([0, 50, 100]), [0, 50, 100]);
  assert.deepEqual(barHeights([0, 0]), [0, 0]);
});

test("a past day averages over all its seconds, today over the seconds so far", () => {
  const speeds = averageDailySpeed(
    [day("2026-11-02", 86_400 * 10), day("2026-11-03", 3_600 * 20)],
    "2026-11-03",
    3_600,
  );
  assert.deepEqual(speeds, [10, 20]);
});

test("the providers tile counts servers up and names each one down with its reason", () => {
  const tile = providersUp(
    [
      provider(),
      provider({
        host: "backup.example.test",
        state: "disabled",
        activity: "disabled",
        activityUntilEpochMs: NOW + 5 * 60_000,
        down: true,
      }),
      // Held back for capacity: still up, as the server says.
      provider({ host: "held.example.test", state: "cooling_down", activity: "cooling_down", down: false }),
    ],
    t,
    NOW,
  );
  assert.equal(tile.up, 2);
  assert.equal(tile.total, 3);
  assert.equal(tile.down.length, 1);
  assert.equal(tile.down[0]!.host, "backup.example.test");
  assert.match(tile.down[0]!.reason, /5m/);
});

test("the downloads tile names the block and when it lifts", () => {
  assert.deepEqual(downloadsState(false, block()), { state: "running", liftsAtEpochMs: null });
  assert.deepEqual(downloadsState(true, block()), { state: "paused", liftsAtEpochMs: null });
  assert.deepEqual(
    downloadsState(false, block({ kind: "EGRESS_QUOTA", windowEndsAtEpochMs: NOW + 1 })),
    { state: "egressQuota", liftsAtEpochMs: NOW + 1 },
  );
  assert.deepEqual(
    downloadsState(false, block({ kind: "SCHEDULED", windowEndsAtEpochMs: NOW + 2 })),
    { state: "schedule", liftsAtEpochMs: NOW + 2 },
  );
  assert.equal(downloadsState(false, block({ kind: "SERVER_QUOTA" })).state, "serverQuota");
  // A timed pause says when it lifts; a pause by hand waits to be resumed.
  assert.deepEqual(downloadsState(true, block({ kind: "MANUAL_PAUSE", resumeAtEpochMs: NOW + 3 })), {
    state: "paused",
    liftsAtEpochMs: NOW + 3,
  });
});

test("provider rows sort by data in the range and keep removed servers the ledger remembers", () => {
  const rows = providerRows(
    [
      provider({ host: "a.example.test", serverId: 1 }),
      provider({ host: "b.example.test", serverId: 2 }),
      provider({ host: "new.example.test", serverId: null }),
    ],
    [consumer("1", 10), consumer("2", 30), consumer("9", 20)],
  );
  assert.deepEqual(
    rows.map((row) => row.provider?.host ?? `removed ${row.serverId}`),
    ["b.example.test", "removed 9", "a.example.test", "new.example.test"],
  );
  assert.equal(rows[3]!.usage, null);
});

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import type { DownloadBlockState } from "../src/lib/context/live-data-context.tsx";
import type { ProviderHealth } from "../src/next/data/next-data.tsx";
import {
  averageDailySpeed,
  averageSeconds,
  barHeights,
  dailyRepairShare,
  dayKey,
  downloadsState,
  foundPercent,
  lastDays,
  noTorrents,
  percentOf,
  providerRows,
  providersUp,
  secondsInDay,
  shareRatio,
  stagesInOrder,
  sumDays,
  trackersInOrder,
  type TorrentHistory,
  type UsageConsumer,
  type UsageDay,
  type UsageTotals,
} from "../src/next/data/dashboards.ts";
import { englishTranslate as t } from "./english-translate.ts";

const NOW = new Date(2026, 10, 3, 9, 30).getTime();

function day(date: string, bytes: number, tried = 0, succeeded = 0, down = 0): UsageDay {
  return {
    day: date,
    bytes,
    articlesTried: tried,
    articlesSucceeded: succeeded,
    downSeconds: down,
    bytesUploaded: 0,
    events: 0,
    durationMs: 0,
  };
}

function counted(date: string, fields: Partial<UsageTotals>): UsageDay {
  return { ...day(date, 0), ...fields };
}

function ledger(kind: string, id: string, range: Partial<UsageTotals>, days: UsageDay[] = []): UsageConsumer {
  const totals = counted("", range);
  return { kind, id, today: totals, week: totals, month: totals, year: totals, allTime: totals, range: totals, days };
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
    {
      bytes: 150,
      articlesTried: 15,
      articlesSucceeded: 14,
      downSeconds: 60,
      bytesUploaded: 0,
      events: 0,
      durationMs: 0,
    },
  );
  const counts = sumDays([
    counted("2026-11-01", { bytesUploaded: 7, events: 2, durationMs: 1_500 }),
    counted("2026-11-02", { bytesUploaded: 3, events: 1, durationMs: 500 }),
  ]);
  assert.deepEqual([counts.bytesUploaded, counts.events, counts.durationMs], [10, 3, 2_000]);
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

test("repair share is repaired over finished, and no finished jobs is no figure", () => {
  assert.equal(percentOf(1, 4), 25);
  assert.equal(percentOf(0, 0), null);
  // A share never runs past the whole.
  assert.equal(percentOf(5, 4), 100);
  const finished = [counted("2026-11-01", { events: 4 }), counted("2026-11-02", { events: 0 }), counted("2026-11-03", { events: 2 })];
  const repaired = [counted("2026-11-01", { events: 1 }), counted("2026-11-03", { events: 2 })];
  assert.deepEqual(dailyRepairShare(finished, repaired), [25, 0, 100]);
});

test("a stage's average is its time over its runs", () => {
  assert.equal(averageSeconds(counted("", { events: 4, durationMs: 10_000 })), 2.5);
  assert.equal(averageSeconds(counted("", { events: 0, durationMs: 0 })), null);
});

test("stages read in the order a job meets them", () => {
  const stages = ["move", "later", "download", "repair", "verify"].map((id) => ledger("stage", id, {}));
  assert.deepEqual(
    stagesInOrder(stages).map((stage) => stage.id),
    ["download", "verify", "repair", "move", "later"],
  );
});

test("ratio is uploaded over downloaded, and nothing downloaded is no figure", () => {
  assert.equal(shareRatio(counted("", { bytes: 200, bytesUploaded: 300 })), 1.5);
  assert.equal(shareRatio(counted("", { bytes: 0, bytesUploaded: 300 })), null);
});

test("trackers sort by uploaded in the range, the no-tracker row last", () => {
  const trackers = [
    ledger("tracker", "", { bytesUploaded: 900 }),
    ledger("tracker", "b.example.test", { bytesUploaded: 10 }),
    ledger("tracker", "a.example.test", { bytesUploaded: 50 }),
  ];
  assert.deepEqual(
    trackersInOrder(trackers).map((tracker) => tracker.id),
    ["a.example.test", "b.example.test", ""],
  );
});

test("a server with no torrent ever reads as no torrents", () => {
  const history = (all: Partial<UsageTotals>, torrents = 0): TorrentHistory => ({
    today: "2026-11-03",
    rangeStart: "2026-11-01",
    rangeEnd: "2026-11-03",
    total: { ...ledger("torrent", "", {}), allTime: counted("", all) },
    trackers: [],
    peers: { torrents, connected: 0, seeds: 0 },
  });
  assert.equal(noTorrents(history({})), true);
  assert.equal(noTorrents(history({}, 1)), false);
  assert.equal(noTorrents(history({ durationMs: 1 })), false);
  assert.equal(noTorrents(history({ bytesUploaded: 1 })), false);
});

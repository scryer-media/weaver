import assert from "node:assert/strict";
import test from "node:test";
import type { ProviderHealth } from "../src/next/data/next-data.tsx";
import { providerActivityLabel } from "../src/next/data/provider-activity.ts";
import { englishTranslate as t } from "./english-translate.ts";

const NOW = 1_700_000_000_000;

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

test("a running pool leads with the state word and keeps the counts", () => {
  const label = providerActivityLabel(provider(), t, NOW);
  assert.equal(label.word, "downloading");
  assert.equal(label.cause, null);
  assert.equal(label.fraction, "38 active · 40 open · 100 max");
  assert.equal(label.tone, "accent");
});

test("an idle pool says so rather than showing a zero", () => {
  const label = providerActivityLabel(
    provider({ activity: "idle", connectionsOpen: 0, connectionsBusy: 0, connectionsActive: 0 }),
    t,
    NOW,
  );
  assert.equal(label.word, "idle");
  assert.equal(label.cause, null);
  assert.equal(label.fraction, "0 active · 0 open · 100 max");
  assert.equal(label.tone, "inert");
});

test("a provider refusing connections says so, and counts down to the retry", () => {
  const label = providerActivityLabel(
    provider({
      activity: "over_limit",
      activityUntilEpochMs: NOW + 4 * 60_000,
      connectionsOpen: 1,
      connectionsBusy: 1,
      connectionsActive: 1,
    }),
    t,
    NOW,
  );
  assert.equal(label.word, "cooling");
  assert.equal(label.cause, "Too Many Connections, retrying in 4m");
  assert.equal(label.fraction, "1 active · 1 open · 100 max");
  assert.equal(label.tone, "warn");
});

test("a holdoff whose deadline has passed drops the countdown, not the cause", () => {
  const label = providerActivityLabel(
    provider({ activity: "over_limit", activityUntilEpochMs: NOW - 1 }),
    t,
    NOW,
  );
  assert.equal(label.cause, "Too Many Connections");
});

test("a cooldown names the errors behind it and when it lifts", () => {
  const withDeadline = providerActivityLabel(
    provider({ activity: "cooling_down", activityUntilEpochMs: NOW + 45_000 }),
    t,
    NOW,
  );
  assert.equal(withDeadline.word, "cooling");
  assert.equal(withDeadline.cause, "paused after repeated errors, resuming in 45s");
  assert.equal(withDeadline.tone, "warn");

  const withoutDeadline = providerActivityLabel(provider({ activity: "cooling_down" }), t, NOW);
  assert.equal(withoutDeadline.cause, "paused after repeated errors");
});

test("degraded and disabled each carry their own cause", () => {
  assert.deepEqual(
    (({ word, cause, tone }) => ({ word, cause, tone }))(
      providerActivityLabel(provider({ activity: "degraded" }), t, NOW),
    ),
    { word: "degraded", cause: "errors on recent requests", tone: "error" },
  );
  assert.deepEqual(
    (({ word, cause, tone }) => ({ word, cause, tone }))(
      providerActivityLabel(provider({ activity: "disabled" }), t, NOW),
    ),
    { word: "disabled", cause: "paused after login or repeated failures", tone: "error" },
  );
});

test("a quarantined server counts down to its retry like any other holdoff", () => {
  const quarantined = providerActivityLabel(
    provider({ activity: "disabled", activityUntilEpochMs: NOW + 45_000 }),
    t,
    NOW,
  );
  assert.equal(quarantined.word, "disabled");
  assert.equal(quarantined.cause, "paused after login or repeated failures, retrying in 45s");
  assert.equal(quarantined.tone, "error");
});

test("held-open connections read as preparing, with the repair fetch as the cause", () => {
  const many = providerActivityLabel(
    provider({ activity: "preparing", connectionsOpen: 8, connectionsBusy: 0 }),
    t,
    NOW,
  );
  assert.equal(many.word, "preparing");
  assert.equal(many.cause, "fetching repair data");
  assert.equal(many.fraction, "0 active · 8 open · 100 max");
  assert.equal(many.tone, "inert");
});

test("an activity the daemon has not taught the rail falls back to idle", () => {
  assert.equal(providerActivityLabel(provider({ activity: "" }), t, NOW).word, "idle");
});

test("a live maximum of zero falls back to the configured one", () => {
  const label = providerActivityLabel(
    provider({ activity: "idle", connectionsMax: 0, connectionsConfigured: 20 }),
    t,
    NOW,
  );
  assert.equal(label.fraction, "38 active · 40 open · 20 max");
});

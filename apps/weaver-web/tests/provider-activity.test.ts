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

test("a running pool leads with the state word and keeps the leased fraction", () => {
  const label = providerActivityLabel(provider(), t, NOW);
  assert.equal(label.word, "downloading");
  assert.equal(label.cause, null);
  assert.equal(label.fraction, "39 / 100");
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
  assert.equal(label.fraction, "0 / 100");
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
  assert.equal(label.word, "held back");
  assert.equal(label.cause, "provider refused more connections, retrying in 4m");
  assert.equal(label.fraction, "1 / 100");
  assert.equal(label.tone, "warn");
});

test("a holdoff whose deadline has passed drops the countdown, not the cause", () => {
  const label = providerActivityLabel(
    provider({ activity: "over_limit", activityUntilEpochMs: NOW - 1 }),
    t,
    NOW,
  );
  assert.equal(label.cause, "provider refused more connections");
});

test("a cooldown names the errors behind it and when it lifts", () => {
  const withDeadline = providerActivityLabel(
    provider({ activity: "cooling_down", activityUntilEpochMs: NOW + 45_000 }),
    t,
    NOW,
  );
  assert.equal(withDeadline.word, "cooling down");
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
    { word: "degraded", cause: "errors on recent requests", tone: "warn" },
  );
  assert.deepEqual(
    (({ word, cause, tone }) => ({ word, cause, tone }))(
      providerActivityLabel(provider({ activity: "disabled" }), t, NOW),
    ),
    { word: "off", cause: "switched off by the daemon", tone: "inert" },
  );
});

test("held-open connections read as preparing, and the cause counts them", () => {
  const many = providerActivityLabel(
    provider({ activity: "preparing", connectionsOpen: 8, connectionsBusy: 0 }),
    t,
    NOW,
  );
  assert.equal(many.word, "preparing");
  assert.equal(many.cause, "8 connections held while the index is read");
  assert.equal(many.fraction, "0 / 100");
  assert.equal(many.tone, "inert");

  const one = providerActivityLabel(
    provider({ activity: "preparing", connectionsOpen: 1, connectionsBusy: 0 }),
    t,
    NOW,
  );
  assert.equal(one.cause, "1 connection held while the index is read");
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
  assert.equal(label.fraction, "39 / 20");
});

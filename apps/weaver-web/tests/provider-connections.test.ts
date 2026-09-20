import assert from "node:assert/strict";
import test from "node:test";
import type { ProviderHealth } from "../src/next/data/next-data.tsx";
import { withLiveConnections } from "../src/next/data/provider-connections.ts";

function provider(label: string, active: number, max = 20): ProviderHealth {
  const [host, port] = label.split(":");
  return {
    host,
    port: Number(port),
    label,
    tier: "PRIMARY",
    state: "healthy",
    activity: active > 0 ? "downloading" : "idle",
    activityUntilEpochMs: null,
    connectionsOpen: active,
    connectionsBusy: active,
    connectionsActive: active,
    connectionsMax: max,
    connectionsConfigured: max,
    capacityPenaltyUntilEpochMs: null,
    latencyMs: 0,
    bodyLatencyMs: null,
    bodyLatencyBand: null,
    successCount: 0,
    failureCount: 0,
    consecutiveFailures: 0,
    prematureDeaths: 0,
  };
}

test("live counts replace the ones server health last read", () => {
  const providers = [provider("news.example.test:563", 2), provider("backup.example.test:563", 0, 10)];
  const merged = withLiveConnections(providers, [
    { label: "news.example.test:563", active: 17, open: 19, busy: 17, max: 20 },
    { label: "backup.example.test:563", active: 0, open: 0, busy: 0, max: 10 },
  ]);
  assert.equal(merged[0].connectionsActive, 17);
  assert.equal(merged[0].latencyMs, 0);
  assert.equal(merged[1], providers[1], "an unchanged server keeps its object");
});

test("the open and busy readings the meters draw come from the stream too", () => {
  const providers = [provider("news.example.test:563", 2)];
  // A pool holding sockets open while almost none of them carry a request is
  // exactly the gap the two bars exist to show, so it has to survive the merge.
  const merged = withLiveConnections(providers, [
    { label: "news.example.test:563", active: 1, open: 14, busy: 1, max: 20 },
  ]);
  assert.notEqual(merged[0], providers[0]);
  assert.equal(merged[0].connectionsOpen, 14);
  assert.equal(merged[0].connectionsBusy, 1);
  assert.equal(merged[0].connectionsActive, 1);
  assert.equal(merged[0].connectionsMax, 20);
});

test("a stream entry that only moves open or busy still refreshes the row", () => {
  const providers = [provider("news.example.test:563", 5)];
  const merged = withLiveConnections(providers, [
    { label: "news.example.test:563", active: 5, open: 12, busy: 5, max: 20 },
  ]);
  assert.notEqual(merged[0], providers[0], "a changed open count is a change");
  assert.equal(merged[0].connectionsOpen, 12);
});

test("nothing live, or nothing different, hands back the same list", () => {
  const providers = [provider("news.example.test:563", 5)];
  assert.equal(withLiveConnections(providers, undefined), providers);
  assert.equal(withLiveConnections(providers, []), providers);
  assert.equal(
    withLiveConnections(providers, [
      { label: "news.example.test:563", active: 5, open: 5, busy: 5, max: 20 },
    ]),
    providers,
  );
  assert.equal(
    withLiveConnections(providers, [
      { label: "other.example.test:119", active: 3, open: 3, busy: 3, max: 8 },
    ]),
    providers,
  );
});

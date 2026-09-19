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
    { label: "news.example.test:563", active: 17, max: 20 },
    { label: "backup.example.test:563", active: 0, max: 10 },
  ]);
  assert.equal(merged[0].connectionsActive, 17);
  assert.equal(merged[0].latencyMs, 0);
  assert.equal(merged[1], providers[1], "an unchanged server keeps its object");
});

test("nothing live, or nothing different, hands back the same list", () => {
  const providers = [provider("news.example.test:563", 5)];
  assert.equal(withLiveConnections(providers, undefined), providers);
  assert.equal(withLiveConnections(providers, []), providers);
  assert.equal(
    withLiveConnections(providers, [{ label: "news.example.test:563", active: 5, max: 20 }]),
    providers,
  );
  assert.equal(
    withLiveConnections(providers, [{ label: "other.example.test:119", active: 3, max: 8 }]),
    providers,
  );
});

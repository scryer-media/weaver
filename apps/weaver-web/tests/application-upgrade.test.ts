import assert from "node:assert/strict";
import test from "node:test";
import {
  downloadPercent,
  installableUpgrade,
  upgradesInApp,
  type ApplicationUpgradeRun,
  type ApplicationUpgradeStatus,
} from "../src/features/updates/application-upgrade.ts";

function run(overrides: Partial<ApplicationUpgradeRun> = {}): ApplicationUpgradeRun {
  return {
    runId: "run-1",
    status: "RUNNING",
    phase: "downloading",
    downloadedBytes: 0,
    totalBytes: 0,
    targetVersion: "9.8.7",
    targetTag: "weaver-v9.8.7",
    fromVersion: "9.8.6",
    error: null,
    startedAtEpochMs: 0,
    completedAtEpochMs: null,
    ...overrides,
  };
}

function status(overrides: Partial<ApplicationUpgradeStatus> = {}): ApplicationUpgradeStatus {
  return {
    currentVersion: "9.8.6",
    updateVersion: "9.8.7",
    updateTag: "weaver-v9.8.7",
    updateAvailable: true,
    installationKind: "PORTABLE",
    managementOwner: "IN_APP",
    eligible: true,
    eligibilityReason: "eligible",
    activeRun: null,
    latestRun: null,
    ...overrides,
  };
}

test("offers the release the server advertised", () => {
  assert.deepEqual(installableUpgrade(status()), {
    version: "9.8.7",
    tag: "weaver-v9.8.7",
  });
});

test("offers nothing before the first status arrives", () => {
  assert.equal(installableUpgrade(undefined), undefined);
});

test("offers nothing when there is no update, or the install is managed elsewhere", () => {
  assert.equal(installableUpgrade(status({ updateAvailable: false })), undefined);
  assert.equal(
    installableUpgrade(status({ eligible: false, eligibilityReason: "managed_by_docker" })),
    undefined,
  );
});

test("offers nothing while a run is already in flight", () => {
  assert.equal(installableUpgrade(status({ activeRun: run() })), undefined);
});

test("offers nothing on a partial status the server would refuse", () => {
  assert.equal(installableUpgrade(status({ updateTag: null })), undefined);
  assert.equal(installableUpgrade(status({ updateVersion: null })), undefined);
});

test("download progress waits for a known size", () => {
  assert.equal(downloadPercent(run()), undefined);
  assert.equal(downloadPercent(run({ downloadedBytes: 50, totalBytes: 200 })), 25);
  // A server that over-reports cannot push the bar past the end.
  assert.equal(downloadPercent(run({ downloadedBytes: 300, totalBytes: 200 })), 100);
});

test("a release notice goes to the installer only on an installation that upgrades itself", () => {
  assert.equal(upgradesInApp(status()), true);
  assert.equal(upgradesInApp(status({ updateAvailable: false, activeRun: run() })), true);
  assert.equal(
    upgradesInApp(status({ eligible: false, managementOwner: "HOMEBREW" })),
    false,
    "a managed install upgrades through its manager, so the notice keeps the release page",
  );
  assert.equal(upgradesInApp(status({ updateAvailable: false })), false);
  assert.equal(upgradesInApp(undefined), false, "unknown state keeps the release page");
});

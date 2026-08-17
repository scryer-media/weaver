import assert from "node:assert/strict";
import test from "node:test";
import {
  releaseNotification,
  type UpdateStatus,
} from "../src/features/updates/update-notification.ts";

function status(overrides: Partial<UpdateStatus> = {}): UpdateStatus {
  return {
    currentVersion: "1.2.3",
    latestVersion: null,
    updateAvailable: false,
    releaseUrl: null,
    publishedAtEpochMs: null,
    checking: false,
    lastCheckedAtEpochMs: null,
    lastSuccessfulCheckAtEpochMs: null,
    lastError: null,
    ...overrides,
  };
}

test("returns the version and URL when an update is available", () => {
  const notification = releaseNotification(
    status({
      updateAvailable: true,
      latestVersion: "1.3.0",
      releaseUrl: "https://github.com/scryer-media/weaver/releases/tag/weaver-v1.3.0",
    }),
  );
  assert.deepEqual(notification, {
    version: "1.3.0",
    url: "https://github.com/scryer-media/weaver/releases/tag/weaver-v1.3.0",
  });
});

test("returns undefined when no update is available", () => {
  assert.equal(
    releaseNotification(
      status({
        updateAvailable: false,
        latestVersion: "1.2.3",
        releaseUrl: "https://github.com/scryer-media/weaver/releases/tag/weaver-v1.2.3",
      }),
    ),
    undefined,
  );
});

test("returns undefined before the first status arrives", () => {
  assert.equal(releaseNotification(undefined), undefined);
});

test("returns undefined on a partial status that would render a dead link", () => {
  // Available but unnamed.
  assert.equal(
    releaseNotification(
      status({ updateAvailable: true, latestVersion: null, releaseUrl: "https://example.invalid" }),
    ),
    undefined,
  );
  // Named but nowhere to send the user.
  assert.equal(
    releaseNotification(status({ updateAvailable: true, latestVersion: "1.3.0", releaseUrl: null })),
    undefined,
  );
  // Empty strings are as useless as nulls.
  assert.equal(
    releaseNotification(status({ updateAvailable: true, latestVersion: "", releaseUrl: "" })),
    undefined,
  );
});

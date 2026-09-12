import assert from "node:assert/strict";
import test from "node:test";
import { storageMounts, type StorageVolume } from "../src/next/data/storage-mounts.ts";

function volume(
  path: string,
  capacity: { totalBytes: number; usedBytes: number } | null,
  error: string | null = null,
): StorageVolume {
  return {
    labels: [path],
    path,
    error,
    capacity: capacity
      ? { ...capacity, freeBytes: capacity.totalBytes - capacity.usedBytes }
      : null,
  };
}

const PROBE_FAILED = "Filesystem capacity is unavailable for this path: No such file or directory";

test("paths with the same capacity are one disk", () => {
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/srv/weaver/complete", { totalBytes: 4_000, usedBytes: 1_000 }),
  ]);

  assert.equal(mounts.length, 1);
  assert.equal(mounts[0].label, "/srv/weaver");
  assert.deepEqual(mounts[0].paths, ["/srv/weaver", "/srv/weaver/complete"]);
});

test("a category directory that does not exist yet folds into its library", () => {
  // The daemon reports one entry per configured path, and a category's
  // directory is only created when something lands in it — so the probe fails
  // on a perfectly healthy install.
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/srv/weaver/complete", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/srv/weaver/complete/movies", null, PROBE_FAILED),
  ]);

  assert.equal(mounts.length, 1, "one filesystem, one pie");
  assert.equal(mounts[0].error, null, "the disk is fine; only the probe failed");
  assert.deepEqual(mounts[0].paths, [
    "/srv/weaver",
    "/srv/weaver/complete",
    "/srv/weaver/complete/movies",
  ]);
});

test("the fold picks the nearest configured ancestor", () => {
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/library", { totalBytes: 9_000, usedBytes: 2_000 }),
    volume("/library/tv", null, PROBE_FAILED),
  ]);

  const host = mounts.find((mount) => mount.paths.includes("/library/tv"));
  assert.ok(host);
  assert.equal(host.capacity?.totalBytes, 9_000);
  assert.equal(mounts.length, 2);
});

test("a nested mount of its own stays its own disk", () => {
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/srv/weaver/complete", { totalBytes: 80_000, usedBytes: 40_000 }),
  ]);

  assert.equal(mounts.length, 2, "different capacity means a different filesystem");
});

test("an unreadable path with no configured ancestor says why", () => {
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/mnt/archive", null, "Permission denied"),
  ]);

  assert.equal(mounts.length, 2);
  const orphan = mounts.find((mount) => mount.label === "/mnt/archive");
  assert.equal(orphan?.error, "Permission denied");
});

test("a sibling is not a child", () => {
  const mounts = storageMounts([
    volume("/srv/weaver", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("/srv/weaver-archive", null, PROBE_FAILED),
  ]);

  assert.equal(mounts.length, 2, "a shared prefix is not containment");
});

test("windows paths fold on their own separator", () => {
  const mounts = storageMounts([
    volume("D:\\weaver\\complete", { totalBytes: 4_000, usedBytes: 1_000 }),
    volume("D:\\weaver\\complete\\movies", null, PROBE_FAILED),
  ]);

  assert.equal(mounts.length, 1);
});

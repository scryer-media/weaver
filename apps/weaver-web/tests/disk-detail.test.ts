// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { diskDetail, diskShort } from "../src/next/data/disk-detail.ts";
import { englishTranslate as t } from "./english-translate.ts";

const MB = 1024 ** 2;
const slow = { volume: "/mnt/nas", slow: true, draining: false };
const nas = {
  label: "/mnt/nas",
  slow: true,
  writeRateBytesPerSec: 28 * MB,
  waitingBytes: 240 * MB,
};
const disks = [nas];
const fetching = {
  status: "DOWNLOADING",
  totalBytes: 4000 * MB,
  downloadedBytes: 1000 * MB,
  remainingBytes: 3000 * MB,
};
const done = {
  status: "DOWNLOADING",
  totalBytes: 4000 * MB,
  downloadedBytes: 4000 * MB,
  remainingBytes: 0,
};

test("a download a slow disk holds back says what waits on the disk and how fast it is written", () => {
  assert.equal(
    diskDetail(t, { ...fetching, diskWrite: slow }, disks),
    "Writing to disk · 240 MB waiting on /mnt/nas · 28.0 MB/s",
  );
  // No rate measured yet: the waiting bytes alone, never a rate of nothing.
  assert.equal(
    diskDetail(t, { ...fetching, diskWrite: slow }, [
      { ...nas, writeRateBytesPerSec: 0 },
    ]),
    "Writing to disk · 240 MB waiting on /mnt/nas",
  );
  // The row's status line has the short form.
  assert.equal(
    diskShort(t, { ...fetching, diskWrite: slow }),
    "Writing to disk",
  );
  // Before the live disk list carries the disk, the short form too.
  assert.equal(
    diskDetail(t, { ...fetching, diskWrite: slow }, []),
    "Writing to disk",
  );
});

test("a download done fetching says what is left to write while bytes are still on their way", () => {
  const draining = { ...slow, slow: false, draining: true };
  const usb = [{ ...nas, slow: false, waitingBytes: 1.2 * 1024 * MB }];
  // Whatever the vote says: the disk need not be slow.
  assert.equal(
    diskDetail(t, { ...done, diskWrite: draining }, usb),
    "Writing to disk: 1.2 GB left to write on /mnt/nas",
  );
  assert.equal(
    diskShort(t, { ...done, diskWrite: draining }),
    "Writing to disk",
  );
  // A row without the queue's remaining bytes falls back to its own counts.
  assert.equal(
    diskDetail(
      t,
      {
        status: "DOWNLOADING",
        totalBytes: 10 * MB,
        downloadedBytes: 10 * MB,
        diskWrite: draining,
      },
      usb,
    ),
    "Writing to disk: 1.2 GB left to write on /mnt/nas",
  );
  // Nothing waits for the disk: nothing to say, slow disk or not.
  assert.equal(
    diskDetail(t, { ...done, diskWrite: { ...slow, draining: false } }, disks),
    null,
  );
});

test("only a row the daemon names, and not a paused one still fetching, shows its disk", () => {
  assert.equal(diskDetail(t, { ...fetching, diskWrite: null }, disks), null);
  assert.equal(diskDetail(t, fetching, disks), null);
  assert.equal(
    diskDetail(t, { ...fetching, status: "PAUSED", diskWrite: slow }, disks),
    null,
  );
  assert.equal(
    diskShort(t, { ...fetching, status: "PAUSED", diskWrite: slow }),
    null,
  );
  // A job waiting its turn has written nothing, though its disk is slow.
  assert.equal(
    diskDetail(t, { ...fetching, status: "QUEUED", diskWrite: slow }, disks),
    null,
  );
  assert.equal(
    diskShort(t, { ...fetching, status: "QUEUED", diskWrite: slow }),
    null,
  );
  // Fetching repair data is fetching too.
  assert.equal(
    diskShort(t, {
      ...fetching,
      status: "FETCHING_REPAIR_DATA",
      diskWrite: slow,
    }),
    "Writing to disk",
  );
  // A disk that is not slow says nothing while the row still fetches.
  assert.equal(
    diskDetail(t, { ...fetching, diskWrite: { ...slow, slow: false } }, disks),
    null,
  );
});

test("a job verifying, repairing or unpacking shows its own status unless its writes wait on a slow disk", () => {
  const draining = { ...slow, slow: false, draining: true };
  for (const status of ["EXTRACTING", "VERIFYING", "REPAIRING"]) {
    assert.equal(
      diskShort(t, { ...done, status, diskWrite: draining }),
      null,
      status,
    );
    assert.equal(
      diskDetail(t, { ...done, status, diskWrite: draining }, disks),
      null,
      status,
    );
    assert.equal(
      diskShort(t, { ...done, status, diskWrite: slow }),
      "Writing to disk",
      status,
    );
    assert.equal(
      diskDetail(t, { ...done, status, diskWrite: slow }, disks),
      diskDetail(t, { ...fetching, diskWrite: slow }, disks),
      status,
    );
  }
});

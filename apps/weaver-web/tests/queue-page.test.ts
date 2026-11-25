// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { UNCATEGORISED } from "../src/next/data/categories.ts";
import { UNTAGGED } from "../src/next/data/tags.ts";
import {
  batchTarget,
  conflictCount,
  departedId,
  diskPace,
  etaById,
  groupCounts,
  QUEUE_ITEM_STATES,
  queuePageInput,
  queueSelection,
  tabCounts,
  tabStates,
  withoutIds,
} from "../src/next/data/queue-page.ts";

test("a tab names the states under it and no tab names none", () => {
  assert.equal(tabStates(null), undefined);
  assert.deepEqual(tabStates("paused"), ["PAUSED"]);
  assert.deepEqual(tabStates("seeding"), ["SEEDING"]);
  assert.deepEqual(tabStates("queued"), ["QUEUED"]);
  const downloading = tabStates("downloading")!;
  for (const state of [
    "DOWNLOADING",
    "VERIFYING",
    "REPAIRING",
    "EXTRACTING",
    "POST_PROCESSING",
    "FETCHING_METADATA",
    "CHECKING_FILES",
    "STALLED",
  ] as const) {
    assert.ok(downloading.includes(state), state);
  }
  assert.ok(
    !downloading.includes("QUEUED") &&
      !downloading.includes("PAUSED") &&
      !downloading.includes("SEEDING"),
  );
});

test("the four tabs between them hold every state, each once", () => {
  const tabs = (
    ["downloading", "seeding", "queued", "paused"] as const
  ).flatMap((tab) => tabStates(tab)!);
  assert.deepEqual([...tabs].sort(), [...QUEUE_ITEM_STATES].sort());
});

test("the selection carries only the filters set, uncategorised as the empty string", () => {
  assert.deepEqual(queueSelection({ facets: new Set(), query: "  " }), {});
  assert.deepEqual(
    queueSelection({
      tab: "paused",
      facets: new Set(["tv", UNCATEGORISED]),
      query: " show ",
    }),
    { search: "show", states: ["PAUSED"], categories: ["tv", ""] },
  );
});

test("a tag filter rides in the selection, no tag as the empty string, and is not the whole queue", () => {
  const tagged = queueSelection({ facets: new Set(), query: "", tag: "hd" });
  assert.deepEqual(tagged, { tag: "hd" });
  const untagged = queueSelection({
    facets: new Set(),
    query: "",
    tag: UNTAGGED,
  });
  assert.deepEqual(untagged, { tag: "" });
  assert.deepEqual(batchTarget(true, new Set(), untagged, 3), {
    selection: { tag: "" },
    expectedMatched: 3,
  });
});

test("priority and time left keep queue order; other sorts ask the daemon", () => {
  assert.deepEqual(queuePageInput({}, "priority", 2, 50), {
    pageIndex: 2,
    pageSize: 50,
  });
  assert.deepEqual(queuePageInput({}, "eta", 0, 25), {
    pageIndex: 0,
    pageSize: 25,
  });
  assert.deepEqual(queuePageInput({ search: "x" }, "size", 0, 25), {
    pageIndex: 0,
    pageSize: 25,
    search: "x",
    sortField: "SIZE",
    sortDirection: "DESC",
  });
});

test("tab counts come from the daemon's state counts, not the page", () => {
  assert.deepEqual(
    tabCounts([
      { state: "DOWNLOADING", count: 2 },
      { state: "EXTRACTING", count: 1 },
      { state: "QUEUED", count: 9000 },
      { state: "PAUSED", count: 40 },
      { state: "FAILED", count: 3 },
      { state: "SEEDING", count: 5 },
      { state: "STALLED", count: 1 },
    ]),
    { all: 9052, downloading: 7, seeding: 5, queued: 9000, paused: 40 },
  );
});

test("time left follows each row's bytes through it, and nothing without speed", () => {
  const through = [
    { id: 7, bytesThrough: 100 },
    { id: 3, bytesThrough: 300 },
  ];
  const format = (bytes: number, speed: number) => `${bytes / speed}s`;
  assert.deepEqual(
    [...etaById(through, 100, format)],
    [
      [7, "1s"],
      [3, "3s"],
    ],
  );
  assert.equal(etaById(through, 0, format).size, 0);
});

test("a row a slow disk holds back runs at the disk's rate, with what waits for it", () => {
  const through = [
    { id: 7, bytesThrough: 100 },
    { id: 3, bytesThrough: 300 },
    { id: 9, bytesThrough: 500 },
  ];
  const format = (bytes: number, speed: number) => `${bytes / speed}s`;
  const nas = { volume: "/mnt/nas", slow: true };
  const volumes = [
    { label: "/mnt/nas", writeRateBytesPerSec: 50, waitingBytes: 400 },
    { label: "/mnt/usb", writeRateBytesPerSec: 25, waitingBytes: 0 },
    { label: "/", writeRateBytesPerSec: 5, waitingBytes: 900 },
  ];
  const rows = [
    { id: 7, diskWrite: nas, status: "DOWNLOADING" },
    { id: 3, diskWrite: null, status: "QUEUED" },
    { id: 9, diskWrite: nas, status: "PAUSED" },
    // A slow disk the page's paused row alone writes to still sets the slowest rate.
    { id: 4, diskWrite: { volume: "/mnt/usb", slow: true }, status: "PAUSED" },
    // Done fetching on a disk that is not slow: no pace of its own.
    { id: 5, diskWrite: { volume: "/", slow: false }, status: "DOWNLOADING" },
    // A disk the live list does not carry yet: no pace.
    {
      id: 6,
      diskWrite: { volume: "/mnt/new", slow: true },
      status: "DOWNLOADING",
    },
  ];
  const pace = diskPace(rows, volumes);
  assert.deepEqual([...pace.byId], [[7, { rate: 50, waiting: 400 }]]);
  assert.equal(pace.slowest, 25);
  // While something downloads below the disk's rate, the held row runs at
  // the disk's rate and the other rows keep the download speed.
  assert.deepEqual(
    [...etaById(through, 20, format, pace)],
    [
      [7, "10s"],
      [3, "15s"],
      [9, "25s"],
    ],
  );
  // A speed above the disk's measured rate: the held row runs no slower
  // than the queue does, and still counts what waits for the disk.
  assert.deepEqual(
    [...etaById(through, 100, format, pace)],
    [
      [7, "5s"],
      [3, "3s"],
      [9, "5s"],
    ],
  );
  // With the speed at nothing, the estimate is never blank: the slowest disk sets it.
  assert.deepEqual(
    [...etaById(through, 0, format, pace)],
    [
      [7, "10s"],
      [3, "12s"],
      [9, "20s"],
    ],
  );
  // No slow volume: the plain estimates, the very same as without a pace.
  const none = diskPace(
    rows.map((row) => ({ ...row, diskWrite: null })),
    volumes,
  );
  assert.deepEqual(
    [...etaById(through, 100, format, none)],
    [...etaById(through, 100, format)],
  );
  assert.equal(diskPace(rows, []), none);
  assert.equal(etaById(through, 0, format, none).size, 0);
});

test("section headers count the whole queue by section", () => {
  assert.deepEqual(
    groupCounts([
      { state: "DOWNLOADING", count: 2 },
      { state: "EXTRACTING", count: 1 },
      { state: "QUEUED", count: 1999 },
      { state: "PAUSED", count: 40 },
      { state: "FAILED", count: 3 },
    ]),
    { active: 3, paused: 40, queued: 1999, attention: 3 },
  );
});

test("a tick leaves only when its job leaves the queue", () => {
  const picked = new Set([1, 2, 3]);
  assert.equal(withoutIds(picked, [7, 8]), picked);
  assert.deepEqual([...withoutIds(picked, [2])], [1, 3]);
  assert.equal(
    departedId({ kind: "ITEM_COMPLETED", itemId: 4, state: "COMPLETED" }),
    4,
  );
  assert.equal(departedId({ kind: "ITEM_REMOVED", itemId: 5, state: null }), 5);
  assert.equal(
    departedId({ kind: "ITEM_STATE_CHANGED", itemId: 6, state: "FAILED" }),
    6,
  );
  assert.equal(
    departedId({ kind: "ITEM_STATE_CHANGED", itemId: 6, state: "PAUSED" }),
    null,
  );
  assert.equal(
    departedId({ kind: "ITEM_PROGRESS", itemId: 6, state: null }),
    null,
  );
});

test("a batch names the ticked ids, or every match by the filters with the count confirmed", () => {
  const selection = { states: ["PAUSED" as const] };
  assert.deepEqual(batchTarget(false, new Set([4, 5]), selection, 9), {
    ids: [4, 5],
  });
  assert.deepEqual(batchTarget(true, new Set([4, 5]), selection, 9), {
    selection,
    expectedMatched: 9,
  });
  // No filter names the whole queue only outright.
  assert.deepEqual(batchTarget(true, new Set(), {}, 12), {
    selection: { all: true },
    expectedMatched: 12,
  });
});

test("a moved queue answers with the count to confirm again", () => {
  assert.equal(
    conflictCount({
      graphQLErrors: [{ extensions: { code: "CONFLICT", matched: 14 } }],
    }),
    14,
  );
  assert.equal(
    conflictCount({
      graphQLErrors: [{ extensions: { code: "INVALID_INPUT" } }],
    }),
    null,
  );
  assert.equal(conflictCount(undefined), null);
});

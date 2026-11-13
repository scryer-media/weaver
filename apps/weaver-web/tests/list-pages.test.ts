// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { extendKept, LIST_PAGE_SIZE, prependOlder, showsMore } from "../src/next/data/list-pages.ts";

test("a job page list loads 100 entries at a time", () => {
  assert.equal(LIST_PAGE_SIZE, 100);
});

// The output files: the server's next cursor says whether another page exists.
test("a file list of exactly 100 shows no control", () => {
  assert.equal(showsMore(100, 100, false), false);
  // Even were a cursor offered, one page is not paged.
  assert.equal(showsMore(100, 100, true), false);
});

test("a file list of 101 shows the control until the last file is loaded", () => {
  assert.equal(showsMore(101, 100, true), true);
  assert.equal(showsMore(101, 101, false), false);
});

test("a removed record shorter than its totals stops when the record ends", () => {
  assert.equal(showsMore(6000, 5000, false), false);
});

// The event log: no older page has come back empty yet.
test("a log of exactly 100 events shows no control", () => {
  assert.equal(showsMore(100, 100, true), false);
});

test("a log of 101 events shows the control until every event is loaded", () => {
  assert.equal(showsMore(101, 100, true), true);
  assert.equal(showsMore(101, 101, true), false);
  // An older page that came back empty ends it too.
  assert.equal(showsMore(101, 100, false), false);
});

const event = (id: number) => ({ cursor: `c${id}`, id });
const ids = (list: { id: number }[]) => list.map((entry) => entry.id);

test("older pages and newer snapshots keep each event once, oldest first", () => {
  // The newest page, then the page before it.
  let kept = extendKept([], [3, 4, 5].map(event)) ?? [];
  kept = prependOlder(kept, [1, 2, 3].map(event));
  assert.deepEqual(ids(kept), [1, 2, 3, 4, 5]);

  // Events arrive: the snapshot window moves on, and nothing is lost or doubled.
  kept = extendKept(kept, [5, 6, 7].map(event)) ?? [];
  kept = extendKept(kept, [7, 8, 9].map(event)) ?? [];
  assert.deepEqual(ids(kept), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
});

test("a snapshot that adds nothing keeps the same list", () => {
  const kept = extendKept([], [1, 2].map(event));
  assert.ok(kept);
  assert.equal(extendKept(kept, [2].map(event)), kept);
});

test("a snapshot that moved past the kept events leaves no hole", () => {
  // Pages 1-200 are kept; 150 events arrive while the tab is in the
  // background, so the next snapshot is 251-350 and 201-250 were never seen.
  const kept = Array.from({ length: 200 }, (_, index) => event(index + 1));
  const moved = Array.from({ length: LIST_PAGE_SIZE }, (_, index) => event(index + 251));
  assert.equal(extendKept(kept, moved), null);
  // A window that still shares one event joins on.
  const joined = extendKept(kept, Array.from({ length: LIST_PAGE_SIZE }, (_, index) => event(index + 200)));
  assert.ok(joined);
  assert.deepEqual(ids(joined), Array.from({ length: 299 }, (_, index) => index + 1));
});

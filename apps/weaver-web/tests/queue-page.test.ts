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
  etaById,
  groupCounts,
  queuePageInput,
  queueSelection,
  tabCounts,
  tabStates,
  withoutIds,
} from "../src/next/data/queue-page.ts";

test("a tab names the states of its section and all names none", () => {
  assert.equal(tabStates("all"), undefined);
  assert.deepEqual(tabStates("paused"), ["PAUSED"]);
  const active = tabStates("active")!;
  assert.ok(active.includes("DOWNLOADING") && active.includes("EXTRACTING"));
  assert.ok(!active.includes("QUEUED") && !active.includes("PAUSED"));
  assert.ok(tabStates("queued")!.includes("QUEUED"));
});

test("the selection carries only the filters set, uncategorised as the empty string", () => {
  assert.deepEqual(queueSelection({ tab: "all", facets: new Set(), query: "  " }), {});
  assert.deepEqual(
    queueSelection({ tab: "paused", facets: new Set(["tv", UNCATEGORISED]), query: " show " }),
    { search: "show", states: ["PAUSED"], categories: ["tv", ""] },
  );
});

test("a tag filter rides in the selection, no tag as the empty string, and is not the whole queue", () => {
  const tagged = queueSelection({ tab: "all", facets: new Set(), query: "", tag: "hd" });
  assert.deepEqual(tagged, { tag: "hd" });
  const untagged = queueSelection({ tab: "all", facets: new Set(), query: "", tag: UNTAGGED });
  assert.deepEqual(untagged, { tag: "" });
  assert.deepEqual(batchTarget(true, new Set(), untagged, 3), { selection: { tag: "" }, expectedMatched: 3 });
});

test("priority and time left keep queue order; other sorts ask the daemon", () => {
  assert.deepEqual(queuePageInput({}, "priority", 2, 50), { pageIndex: 2, pageSize: 50 });
  assert.deepEqual(queuePageInput({}, "eta", 0, 25), { pageIndex: 0, pageSize: 25 });
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
    ]),
    { all: 9046, active: 3, queued: 9000, paused: 40 },
  );
});

test("time left follows each row's bytes through it, and nothing without speed", () => {
  const through = [
    { id: 7, bytesThrough: 100 },
    { id: 3, bytesThrough: 300 },
  ];
  const format = (bytes: number, speed: number) => `${bytes / speed}s`;
  assert.deepEqual([...etaById(through, 100, format)], [
    [7, "1s"],
    [3, "3s"],
  ]);
  assert.equal(etaById(through, 0, format).size, 0);
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
  assert.equal(departedId({ kind: "ITEM_COMPLETED", itemId: 4, state: "COMPLETED" }), 4);
  assert.equal(departedId({ kind: "ITEM_REMOVED", itemId: 5, state: null }), 5);
  assert.equal(departedId({ kind: "ITEM_STATE_CHANGED", itemId: 6, state: "FAILED" }), 6);
  assert.equal(departedId({ kind: "ITEM_STATE_CHANGED", itemId: 6, state: "PAUSED" }), null);
  assert.equal(departedId({ kind: "ITEM_PROGRESS", itemId: 6, state: null }), null);
});

test("a batch names the ticked ids, or every match by the filters with the count confirmed", () => {
  const selection = { states: ["PAUSED" as const] };
  assert.deepEqual(batchTarget(false, new Set([4, 5]), selection, 9), { ids: [4, 5] });
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
    conflictCount({ graphQLErrors: [{ extensions: { code: "CONFLICT", matched: 14 } }] }),
    14,
  );
  assert.equal(conflictCount({ graphQLErrors: [{ extensions: { code: "INVALID_INPUT" } }] }), null);
  assert.equal(conflictCount(undefined), null);
});

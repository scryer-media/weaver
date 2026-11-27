// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { rereadOnChange } from "../src/next/features/torrents/reread.ts";

// The schedule's seeding note and the unpacked files each follow one value
// with a query that reads by itself when it starts: one request on the
// first value, and one more for each change while it runs.
test("a query is read again only for a change while it runs", () => {
  assert.equal(rereadOnChange(null, null), false);
  // Starting, or starting again after a pause, reads by itself.
  assert.equal(rereadOnChange(null, "WHOLE"), false);
  // Pausing reads nothing.
  assert.equal(rereadOnChange("WHOLE", null), false);
  // The same value, as on a second render or effect run, is not a change.
  assert.equal(rereadOnChange("WHOLE", "WHOLE"), false);
  assert.equal(rereadOnChange("WHOLE", "PART"), true);

  const torrents = new Map([[1, "seeding"]]);
  assert.equal(rereadOnChange(torrents, torrents), false);
  assert.equal(rereadOnChange(torrents, new Map(torrents)), true);
});

// What the effect of each sends over a run of values: the start's own read
// plus one per change while running.
test("a run of values sends one request per start and per change", () => {
  const run = <T>(values: (T | null)[]) => {
    let before: T | null = values[0] ?? null;
    let requests = before === null ? 0 : 1;
    for (const now of values.slice(1)) {
      if (before === null && now !== null) requests += 1;
      if (rereadOnChange(before, now)) requests += 1;
      before = now;
    }
    return requests;
  };
  assert.equal(run(["WHOLE"]), 1);
  assert.equal(run([null, "WHOLE"]), 1);
  assert.equal(run(["WHOLE", "WHOLE", "PART"]), 2);
  assert.equal(run(["WHOLE", null, "WHOLE"]), 2);
  assert.equal(run<string>([null, null]), 0);
});

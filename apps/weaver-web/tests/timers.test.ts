// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { createDebouncer, type Timers } from "../src/next/data/timers.ts";

/** Timers that run only when the test moves the clock. */
function fakeTimers() {
  let now = 0;
  let nextId = 1;
  const pending = new Map<number, { at: number; run: () => void }>();
  const timers: Timers = {
    setTimeout(run, delayMs) {
      const id = nextId++;
      pending.set(id, { at: now + delayMs, run });
      return id;
    },
    clearTimeout(handle) {
      pending.delete(handle as number);
    },
  };
  const advance = (ms: number) => {
    now += ms;
    for (const [id, entry] of [...pending].sort((a, b) => a[1].at - b[1].at)) {
      if (entry.at <= now && pending.has(id)) {
        pending.delete(id);
        entry.run();
      }
    }
  };
  return { timers, advance, pendingCount: () => pending.size };
}

test("a debouncer settles once, on the last value, after pushes stop", () => {
  const clock = fakeTimers();
  const settled: string[] = [];
  const debouncer = createDebouncer<string>(300, (value) => settled.push(value), clock.timers);

  debouncer.push("u");
  clock.advance(100);
  debouncer.push("ub");
  clock.advance(100);
  debouncer.push("ubu");
  clock.advance(299);
  assert.deepEqual(settled, []);

  clock.advance(1);
  assert.deepEqual(settled, ["ubu"]);
  assert.equal(clock.pendingCount(), 0);
});

test("a cancelled debouncer settles nothing", () => {
  const clock = fakeTimers();
  const settled: string[] = [];
  const debouncer = createDebouncer<string>(300, (value) => settled.push(value), clock.timers);

  debouncer.push("ubuntu");
  debouncer.cancel();
  clock.advance(1_000);
  assert.deepEqual(settled, []);
});

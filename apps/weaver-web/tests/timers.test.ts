// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  createDebouncer,
  createVisibleInterval,
  type PageVisibility,
  type Timers,
} from "../src/next/data/timers.ts";

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

/** A page whose visibility the test flips. */
function fakeVisibility(initiallyVisible: boolean) {
  let visible = initiallyVisible;
  const listeners = new Set<() => void>();
  const visibility: PageVisibility = {
    visible: () => visible,
    subscribe(onChange) {
      listeners.add(onChange);
      return () => listeners.delete(onChange);
    },
  };
  const show = (next: boolean) => {
    visible = next;
    for (const listener of [...listeners]) listener();
  };
  return { visibility, show, listenerCount: () => listeners.size };
}

test("a visible interval runs each period while the page shows", () => {
  const clock = fakeTimers();
  const page = fakeVisibility(true);
  let runs = 0;
  createVisibleInterval(() => runs++, 1_000, page.visibility, clock.timers);

  clock.advance(999);
  assert.equal(runs, 0);
  clock.advance(1);
  assert.equal(runs, 1);
  clock.advance(1_000);
  assert.equal(runs, 2);
});

test("a hidden page is not polled, and is refreshed once when it shows", () => {
  const clock = fakeTimers();
  const page = fakeVisibility(true);
  let runs = 0;
  createVisibleInterval(() => runs++, 1_000, page.visibility, clock.timers);

  clock.advance(500);
  page.show(false);
  assert.equal(clock.pendingCount(), 0);
  clock.advance(60_000);
  assert.equal(runs, 0);

  page.show(true);
  assert.equal(runs, 1);
  clock.advance(999);
  assert.equal(runs, 1);
  clock.advance(1);
  assert.equal(runs, 2);
});

test("an interval started in a hidden page waits for it to show", () => {
  const clock = fakeTimers();
  const page = fakeVisibility(false);
  let runs = 0;
  createVisibleInterval(() => runs++, 1_000, page.visibility, clock.timers);

  clock.advance(10_000);
  assert.equal(runs, 0);
  page.show(true);
  assert.equal(runs, 1);
});

test("a stopped interval runs no more and stops listening", () => {
  const clock = fakeTimers();
  const page = fakeVisibility(true);
  let runs = 0;
  const stop = createVisibleInterval(() => runs++, 1_000, page.visibility, clock.timers);

  stop();
  clock.advance(5_000);
  page.show(false);
  page.show(true);
  assert.equal(runs, 0);
  assert.equal(page.listenerCount(), 0);
  assert.equal(clock.pendingCount(), 0);
});

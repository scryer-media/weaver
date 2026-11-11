// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useState } from "react";

/** The timer functions the helpers below schedule with; tests pass their own. */
export interface Timers {
  setTimeout(run: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

const browserTimers: Timers = {
  setTimeout: (run, delayMs) => globalThis.setTimeout(run, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface Debouncer<T> {
  /** Offer a value; it settles once no newer value arrives for the delay. */
  push(value: T): void;
  /** Drop a value that has not settled yet. */
  cancel(): void;
}

/** Calls `settle` with the last value pushed, once pushes stop for `delayMs`. */
export function createDebouncer<T>(
  delayMs: number,
  settle: (value: T) => void,
  timers: Timers = browserTimers,
): Debouncer<T> {
  let pending: unknown = null;
  const cancel = () => {
    if (pending !== null) {
      timers.clearTimeout(pending);
      pending = null;
    }
  };
  return {
    push(value) {
      cancel();
      pending = timers.setTimeout(() => {
        pending = null;
        settle(value);
      }, delayMs);
    },
    cancel,
  };
}

/** `value`, held back until it has stopped changing for `delayMs`. */
export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    if (Object.is(value, settled)) {
      return;
    }
    const debouncer = createDebouncer<T>(delayMs, setSettled);
    debouncer.push(value);
    return debouncer.cancel;
  }, [delayMs, settled, value]);
  return settled;
}

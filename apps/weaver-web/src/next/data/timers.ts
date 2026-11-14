// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useRef, useState, useSyncExternalStore } from "react";

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

/** Whether the page is showing, and a way to hear when that changes. */
export interface PageVisibility {
  visible(): boolean;
  subscribe(onChange: () => void): () => void;
}

export const documentVisibility: PageVisibility = {
  visible: () => typeof document === "undefined" || document.visibilityState !== "hidden",
  subscribe(onChange) {
    if (typeof document === "undefined") {
      return () => {};
    }
    document.addEventListener("visibilitychange", onChange);
    return () => document.removeEventListener("visibilitychange", onChange);
  },
};

/** True while the page is showing; false while its tab is in the background. */
export function usePageVisible(): boolean {
  return useSyncExternalStore(documentVisibility.subscribe, documentVisibility.visible, () => true);
}

/**
 * Calls `run` every `periodMs` while the page is showing. A hidden page is not
 * polled; when it shows again `run` is called once and the period starts over.
 * Returns a function that stops it.
 */
export function createVisibleInterval(
  run: () => void,
  periodMs: number,
  visibility: PageVisibility = documentVisibility,
  timers: Timers = browserTimers,
): () => void {
  let pending: unknown = null;
  let wasVisible = visibility.visible();
  const clear = () => {
    if (pending !== null) {
      timers.clearTimeout(pending);
      pending = null;
    }
  };
  const schedule = () => {
    clear();
    pending = timers.setTimeout(() => {
      pending = null;
      run();
      schedule();
    }, periodMs);
  };
  const unsubscribe = visibility.subscribe(() => {
    const visible = visibility.visible();
    if (visible === wasVisible) {
      return;
    }
    wasVisible = visible;
    if (visible) {
      run();
      schedule();
    } else {
      clear();
    }
  });
  if (wasVisible) {
    schedule();
  }
  return () => {
    unsubscribe();
    clear();
  };
}

/** `createVisibleInterval` for a component; `periodMs` of null stops it. */
export function useVisibleInterval(run: () => void, periodMs: number | null): void {
  const latest = useRef(run);
  latest.current = run;
  useEffect(() => {
    if (periodMs === null) {
      return;
    }
    return createVisibleInterval(() => latest.current(), periodMs);
  }, [periodMs]);
}

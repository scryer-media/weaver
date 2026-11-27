// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * Whether a paused query is read again when what it follows goes from
 * `before` to `now`, null standing for paused. A query that starts, or
 * starts again after a pause, reads by itself; only a change while it runs
 * needs a second read.
 */
export function rereadOnChange<T>(before: T | null, now: T | null): boolean {
  return before !== null && now !== null && before !== now;
}

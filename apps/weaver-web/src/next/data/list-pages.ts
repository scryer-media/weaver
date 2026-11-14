// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/** How many entries a job page list loads at a time. */
export const LIST_PAGE_SIZE = 100;

/**
 * Whether a list offers to load more. A list of one page or less never does,
 * so it looks as it always has; a longer one does while entries remain.
 * `more` is the server's word that another page exists.
 */
export function showsMore(total: number, loaded: number, more: boolean): boolean {
  return more && total > LIST_PAGE_SIZE && loaded < total;
}

/** An entry that names its place in the list. */
export interface Keyed {
  cursor?: string | null;
}

/**
 * The entries kept so far, oldest first, extended by the newest ones the live
 * snapshot carries: each entry once, in order. The snapshot is the newest end
 * of the same list, so what it adds goes after what is kept. An entry without
 * a cursor cannot be told apart from one kept already and is not added.
 *
 * Null when the snapshot shares no entry with what is kept: the window moved
 * past the kept list, so entries between the two are unknown and the kept
 * list can no longer be extended without a hole.
 */
export function extendKept<T extends Keyed>(kept: readonly T[], snapshot: readonly T[]): T[] | null {
  const seen = new Set(kept.map((entry) => entry.cursor));
  const added = snapshot.filter((entry) => entry.cursor && !seen.has(entry.cursor));
  if (added.length === 0) return kept as T[];
  const overlaps = snapshot.some((entry) => entry.cursor && seen.has(entry.cursor));
  if (kept.length > 0 && !overlaps) return null;
  return [...kept, ...added];
}

/**
 * Older entries in front of the kept ones, oldest first, dropping any the
 * list already holds.
 */
export function prependOlder<T extends Keyed>(kept: readonly T[], older: readonly T[]): T[] {
  const seen = new Set(kept.map((entry) => entry.cursor));
  return [...older.filter((entry) => !entry.cursor || !seen.has(entry.cursor)), ...kept];
}

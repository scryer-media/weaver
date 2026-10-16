// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useRef } from "react";

/**
 * Equality for live values, and a hook that keeps the last one while a new
 * one is equal to it. Every push carries fresh objects for figures that rarely
 * move; handing a reader the object it already has lets it see that nothing
 * changed.
 */

export function shallowEqual<T extends object>(left: T, right: T): boolean {
  const keys = Object.keys(left) as (keyof T)[];
  return (
    keys.length === Object.keys(right).length && keys.every((key) => Object.is(left[key], right[key]))
  );
}

/** The same length, and each item shallowly equal to the one at its index. */
export function sameItems<T extends object>(left: readonly T[], right: readonly T[]): boolean {
  return left.length === right.length && left.every((item, index) => shallowEqual(item, right[index]));
}

/** The same length, and the very same value at every index. */
export function sameElements<T>(left: readonly T[], right: readonly T[]): boolean {
  return left.length === right.length && left.every((item, index) => Object.is(item, right[index]));
}

/** The same keys in the same order, each with the very same value. */
export function sameEntries<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  const others = right.entries();
  for (const [key, value] of left) {
    const [otherKey, otherValue] = others.next().value!;
    if (!Object.is(key, otherKey) || !Object.is(value, otherValue)) {
      return false;
    }
  }
  return true;
}

/** `value`, or the one last returned while the two are still equal. */
export function useSettled<T>(value: T, isEqual: (left: T, right: T) => boolean): T {
  const settled = useRef(value);
  if (!isEqual(settled.current, value)) {
    settled.current = value;
  }
  return settled.current;
}

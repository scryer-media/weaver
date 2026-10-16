// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useRef } from "react";

/** A plain record, which is what a GraphQL result is built from. */
function isRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * `next`, with every branch that equals the same branch of `previous` swapped
 * for that branch. A live push rebuilds the whole tree, figures that never
 * moved included; handing the old branches back lets everything drawn from
 * them see that nothing changed.
 */
export function shareStructure<T>(previous: unknown, next: T): T {
  if (Object.is(previous, next)) {
    return next;
  }
  if (Array.isArray(next)) {
    if (!Array.isArray(previous)) {
      return next;
    }
    let same = previous.length === next.length;
    const merged = next.map((item: unknown, index) => {
      const shared = shareStructure(previous[index], item);
      if (shared !== previous[index]) {
        same = false;
      }
      return shared;
    });
    return (same ? previous : merged) as T;
  }
  if (!isRecord(next) || !isRecord(previous)) {
    return next;
  }
  const keys = Object.keys(next);
  let same = keys.length === Object.keys(previous).length;
  const merged: Record<string, unknown> = {};
  for (const key of keys) {
    const shared = shareStructure(previous[key], next[key]);
    if (shared !== previous[key] || !(key in previous)) {
      same = false;
    }
    merged[key] = shared;
  }
  return (same ? previous : merged) as T;
}

/** `value`, sharing every branch that is unchanged since the last one returned. */
export function useSharedStructure<T>(value: T): T {
  const held = useRef({ input: value, output: value });
  if (held.current.input !== value) {
    held.current = { input: value, output: shareStructure(held.current.output, value) };
  }
  return held.current.output;
}

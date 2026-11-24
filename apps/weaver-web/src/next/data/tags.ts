// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * Tags: free labels on a job. Nothing in Weaver reads one to decide anything.
 *
 * A tag is trimmed, and one that is empty or holds a comma is left out, as the
 * daemon (and qBittorrent) take them.
 */

/** The filter value that keeps jobs with no tag. */
export const UNTAGGED = "";

/** A tag as the daemon keeps it, or null for one it leaves out. */
export function cleanTag(text: string): string | null {
  const tag = text.trim();
  return tag === "" || tag.includes(",") ? null : tag;
}

/** The tags a comma-separated list names, each once, in the order given. */
export function splitTags(text: string): string[] {
  const tags: string[] = [];
  for (const part of text.split(",")) {
    const tag = cleanTag(part);
    if (tag !== null && !tags.includes(tag)) {
      tags.push(tag);
    }
  }
  return tags;
}

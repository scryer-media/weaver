// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/** What the analyzer was handed: an NZB, a .torrent file or a magnet link. */
export type AnalyzerKind = "nzb" | "torrent" | "magnet";

/** What the picker offers. The kind is read from the bytes, not the name. */
export const ANALYZER_ACCEPT = ".nzb,.gz,.zst,.xz,.torrent";

/** The daemon's default inline NZB limit; a larger NZB is refused before it is read. */
export const NZB_MAX_BYTES = 268_435_456;

/** The daemon's limit on a .torrent file. */
export const TORRENT_MAX_BYTES = 67_108_864;

/** Whether pasted or dropped text is a magnet link. */
export function isMagnet(text: string): boolean {
  return /^magnet:\?/i.test(text.trim());
}

const DIGIT_0 = 0x30;
const DIGIT_9 = 0x39;
const DICT = 0x64; // "d"

/**
 * The kind of a file from its first bytes: a .torrent is a bencoded
 * dictionary, `d` then a key's length. Anything else, XML or compressed, is
 * sent as an NZB, whose reader names what is wrong with it.
 */
export function fileKind(head: Uint8Array): "nzb" | "torrent" {
  const second = head[1];
  return head[0] === DICT && second !== undefined && second >= DIGIT_0 && second <= DIGIT_9
    ? "torrent"
    : "nzb";
}

/** The largest file of a kind the daemon reads. */
export function maxBytes(kind: "nzb" | "torrent"): number {
  return kind === "torrent" ? TORRENT_MAX_BYTES : NZB_MAX_BYTES;
}

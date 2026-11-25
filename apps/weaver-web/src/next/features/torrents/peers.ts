// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { TorrentPeerData, TorrentState } from "@/lib/torrents";

/** How many peers a page of the list holds. */
export const PEER_PAGE_SIZE = 25;

/** A column the peers sort by. */
export type PeerSortKey =
  | "address"
  | "client"
  | "connection"
  | "progress"
  | "downloadBps"
  | "uploadBps";

export interface PeerSort {
  key: PeerSortKey;
  descending: boolean;
}

/** Addresses in order. */
export const DEFAULT_PEER_SORT: PeerSort = {
  key: "address",
  descending: false,
};

/**
 * The order the peers take until a heading is clicked: the fastest sender
 * first while the torrent downloads, the fastest receiver first once it has
 * all it wants and uploads.
 */
export function peerSortFor(torrent: {
  state: TorrentState;
  progress: number;
}): PeerSort {
  const uploading = torrent.state === "SEEDING" || torrent.progress >= 1;
  return { key: uploading ? "uploadBps" : "downloadBps", descending: true };
}

/**
 * The sort a click on `key`'s heading gives: the other way round on the
 * column in force; on another, words from A and figures from the largest.
 */
export function nextPeerSort(sort: PeerSort, key: PeerSortKey): PeerSort {
  if (sort.key === key) return { key, descending: !sort.descending };
  return {
    key,
    descending: key !== "address" && key !== "client" && key !== "connection",
  };
}

// Numbers inside the text count as numbers, so 5.79.x comes before 5.180.x.
const words = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: "base",
});

/** `peers` in `sort`'s order, peers alike in it by address; `peers` itself is left as it is. */
export function sortPeers(
  peers: readonly TorrentPeerData[],
  sort: PeerSort,
): TorrentPeerData[] {
  const way = sort.descending ? -1 : 1;
  const by = (a: TorrentPeerData, b: TorrentPeerData): number => {
    switch (sort.key) {
      case "address":
        return words.compare(a.address, b.address);
      case "client":
        return words.compare(a.client, b.client);
      case "connection":
        return words.compare(a.connection, b.connection);
      default:
        return a[sort.key] - b[sort.key];
    }
  };
  return [...peers].sort(
    (a, b) => by(a, b) * way || words.compare(a.address, b.address),
  );
}

/** How many pages `count` peers take: one at least, so an empty list has a page to be on. */
export function peerPageCount(count: number): number {
  return Math.max(1, Math.ceil(count / PEER_PAGE_SIZE));
}

/** The page shown when `page` is asked of `count` peers: the last one once peers leaving have emptied it. */
export function peerPage(page: number, count: number): number {
  return Math.min(Math.max(0, page), peerPageCount(count) - 1);
}

/** The peers of `page`, of `sorted`. */
export function peersOfPage(
  sorted: readonly TorrentPeerData[],
  page: number,
): TorrentPeerData[] {
  const at = peerPage(page, sorted.length) * PEER_PAGE_SIZE;
  return sorted.slice(at, at + PEER_PAGE_SIZE);
}

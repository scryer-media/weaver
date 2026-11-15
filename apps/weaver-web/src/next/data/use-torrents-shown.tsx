// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ReactNode } from "react";
import { useNextDataSelector } from "./next-data";
import { torrentAddsOffered, torrentsShown, torrentsView } from "./torrents-shown";

/**
 * Whether torrent things show: while torrents are on or any torrent job is
 * left, and only once the server has said, so nothing torrent-only flashes in
 * while it is read.
 *
 * The one way any screen asks. It reads the shell's live metrics, so a caller
 * adds no request of its own.
 */
export function useTorrentsShown(): boolean {
  return useNextDataSelector((data) => torrentsShown(data.torrents));
}

/**
 * Whether a screen offers to start a torrent: a magnet link, a .torrent file,
 * the words that ask for one. Only while torrents are on; jobs left while they
 * are off still show through `useTorrentsShown`, but no new one is offered.
 *
 * The one way an add surface asks.
 */
export function useTorrentAddsOffered(): boolean {
  return useNextDataSelector((data) => torrentAddsOffered(data.torrentPresence));
}

/**
 * Its children while torrent things show, `fallback` once they are known not
 * to, and `pending` until that is known. A torrent-only screen passes a
 * redirect as `fallback` and the route's loading state as `pending`, so a
 * direct visit waits for the answer instead of leaving early.
 */
export function TorrentsOnly({
  children,
  fallback = null,
  pending = null,
}: {
  children: ReactNode;
  fallback?: ReactNode;
  pending?: ReactNode;
}) {
  return torrentsView(useNextDataSelector((data) => data.torrents), children, fallback, pending);
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ReactNode } from "react";
import { useNextDataSelector } from "./next-data";
import {
  torrentAddsOffered,
  torrentsAvailable,
  torrentsShown,
  torrentsView,
  type TorrentsState,
} from "./torrents-shown";

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
 * Whether the server has torrents, turned on or not, and only once it has
 * said so. What is there even while torrents are off asks this: the Torrents
 * settings and the torrent events a script can run on.
 */
export function useTorrentsAvailable(): boolean {
  return useNextDataSelector((data) => torrentsAvailable(data.torrents));
}

/** The state itself, for a screen that draws each of its answers apart. */
export function useTorrentsState(): TorrentsState {
  return useNextDataSelector((data) => data.torrents);
}

/**
 * Whether a screen offers to start a torrent: a magnet link, a .torrent file,
 * the words that ask for one. Only while torrents are on; jobs left while they
 * are off still show through `useTorrentsShown`, but no new one is offered.
 *
 * The one way an add surface asks.
 */
export function useTorrentAddsOffered(): boolean {
  return useNextDataSelector((data) =>
    torrentAddsOffered(data.torrentPresence),
  );
}

/**
 * Whether the torrent switch is on, as the server says: not merely torrent
 * jobs left over while it is off. What only means something while torrents
 * run asks this, such as the rail's torrent rates.
 */
export function useTorrentsEnabled(): boolean {
  return useNextDataSelector((data) =>
    torrentAddsOffered(data.torrentPresence),
  );
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
  return torrentsView(
    useNextDataSelector((data) => data.torrents),
    children,
    fallback,
    pending,
  );
}

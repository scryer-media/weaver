// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/** Whether torrents are on, and the torrents there are, as the server says. */
export interface TorrentPresence {
  /** The settings turn torrents on. */
  enabled: boolean;
  /** There is a torrent job, in any state. */
  jobs: boolean;
  /** A torrent job is not paused or stopped. */
  running: boolean;
  /** Torrents are off and the torrents still running are let finish. */
  windingDown: boolean;
  /** While winding down, how the torrents still running end; null otherwise. */
  windDown: TorrentDisableMode | null;
}

/**
 * Whether torrent things show, as the interface knows it: "unknown" until the
 * server has said.
 *
 * Torrent things (the seeding column, the protocol filter, the torrent
 * dashboard, a category's torrent fields, upload limits) show while torrents
 * are on, and also while any torrent job is left, so turning torrents off
 * never hides the torrents still there. Until the answer comes, or while the
 * read fails, they stay hidden and torrent-only routes wait: nothing shows
 * and then goes, and a failed read is never taken for off.
 */
export type TorrentsState = "on" | "off" | "unknown";

/** The state from the server's answer: none yet, or a failed read, is unknown. */
export function torrentsStateFrom(presence: TorrentPresence | null | undefined): TorrentsState {
  if (!presence) return "unknown";
  return presence.enabled || presence.jobs ? "on" : "off";
}

/** Whether torrent things show: only once known to. */
export function torrentsShown(state: TorrentsState): boolean {
  return state === "on";
}

/**
 * Whether a screen offers to start a torrent (a magnet link, a .torrent file):
 * only while the switch is on, as the server says. Torrent jobs left while
 * torrents are off, or winding down, still show, but no new one is offered,
 * since the server refuses it. Unknown offers none, as torrent things wait.
 */
export function torrentAddsOffered(presence: TorrentPresence | null | undefined): boolean {
  return presence?.enabled === true;
}

/** What a torrent-only screen draws: `on` while torrent things show, `off` once known not to, `pending` until then. */
export function torrentsView<T>(state: TorrentsState, on: T, off: T, pending: T): T {
  return state === "on" ? on : state === "off" ? off : pending;
}

/** `items` without the torrent-only ones while torrent things do not show. */
export function withoutTorrentOnly<T>(
  items: readonly T[],
  shown: boolean,
  torrentOnly: (item: T) => boolean,
): T[] {
  return shown ? [...items] : items.filter((item) => !torrentOnly(item));
}

/**
 * Whether turning torrents off asks first how the torrents still running end:
 * when the save turns them off and some torrent is running, or when that is
 * not known, so a save never stops them at once unasked. With none running,
 * there is nothing to choose.
 */
export function asksHowTorrentsEnd(
  wasEnabled: boolean,
  enabled: boolean,
  presence: TorrentPresence | null | undefined,
): boolean {
  return wasEnabled && !enabled && (presence == null || presence.running);
}

/**
 * Whether the Torrents switch waits: while it is on and the server has not
 * said which torrents run, turning it off could not ask how they end.
 */
export function torrentSwitchWaits(enabled: boolean, presence: TorrentPresence | null | undefined): boolean {
  return enabled && presence == null;
}

/** Whether a wind-down under way can be cut short: torrents off, some still let finish. */
export function windDownStoppable(enabled: boolean, presence: TorrentPresence | null | undefined): boolean {
  return !enabled && presence?.windingDown === true;
}

/** The settings change that stops a wind-down under way: every torrent stops now. */
export function stopNowInput(): { torrent: { enabled: false; whenDisabled: TorrentDisableMode } } {
  return { torrent: { enabled: false, whenDisabled: "STOP_NOW" } };
}

/**
 * The Torrents settings panel's blocks: the switch alone while the panel's
 * switch is off, whatever torrent jobs are left; the switch and the rest
 * while it is on.
 */
export function torrentSettingsDisclosed<T>(enabled: boolean, toggle: T, rest: readonly T[]): T[] {
  return enabled ? [toggle, ...rest] : [toggle];
}

/** How the torrents still running end when torrents are turned off. */
export type TorrentDisableMode = "STOP_NOW" | "FINISH_DOWNLOADS" | "FINISH_ALL";

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * Whether archives are extracted, where a level may inherit it: null
 * inherits. Option labels are translation keys.
 */
export const UNPACK_CHOICES = [
  { value: "inherit", label: "next.torrents.unpackInherit" },
  { value: "on", label: "next.common.on" },
  { value: "off", label: "next.common.off" },
] as const;

export const unpackChoice = (unpack: boolean | null): string =>
  unpack === null ? "inherit" : unpack ? "on" : "off";

export const unpackOf = (choice: string): boolean | null =>
  choice === "on" ? true : choice === "off" ? false : null;

/** A torrent's state as the daemon names it. */
export type TorrentState =
  | "QUEUED"
  | "FETCHING_METADATA"
  | "CHECKING_FILES"
  | "DOWNLOADING"
  | "STALLED"
  | "SEEDING"
  | "PAUSED"
  | "STOPPED";

/** Where a torrent's seeding policy comes from. */
export type SeedingSource = "CATEGORY" | "GLOBAL" | "TORRENT" | "TRACKER_RULE";

/** A seeding policy as the daemon sends it. */
export interface SeedingPolicyData {
  ratioGoal: number | null;
  seedTimeGoalMinutes: number | null;
  inactiveGoalMinutes: number | null;
  mode: "ANY" | "ALL";
  action: "STOP" | "REMOVE" | "REMOVE_WITH_DATA";
  maxUploadBps: number | null;
}

export type TrackerStatus = "NOT_CONTACTED" | "UPDATING" | "WORKING" | "NOT_WORKING" | "DISABLED";

export interface TorrentFileData {
  index: number;
  path: string;
  length: number;
  selected: boolean;
  /** The share of the file in hand, from 0 to 1. */
  progress: number;
}

export interface TorrentTrackerData {
  tier: number;
  url: string;
  status: TrackerStatus;
  message: string | null;
  seeders: number | null;
  leechers: number | null;
}

/** What the queue reads of a torrent beside its job: enough for the seeding column and the row actions. */
export interface TorrentSummaryData {
  id: number;
  state: TorrentState;
  forced: boolean;
  ratio: number;
  uploadBps: number;
}

/** A torrent as its detail reads it. */
export interface TorrentData extends TorrentSummaryData {
  name: string;
  infohash: string;
  detail: string | null;
  category: string | null;
  private: boolean;
  progress: number;
  totalBytes: number;
  haveBytes: number;
  uploadedBytes: number;
  downloadedBytes: number;
  downloadBps: number;
  peers: number;
  seeds: number;
  leechers: number;
  seedingLeft: string | null;
  /** The torrent's own policy; null when it inherits one. */
  seedingOverride: SeedingPolicyData | null;
  /** The policy in effect. */
  seedingPolicy: SeedingPolicyData;
  seedingSource: SeedingSource;
  /** The host pattern of the tracker rule the policy comes from, when it does. */
  seedingRule: string | null;
  /** Whether its archives are extracted. */
  unpack: boolean;
  route: string | null;
  files: TorrentFileData[];
  trackers: TorrentTrackerData[];
}

/** A torrent seeding, or stopped by its goal, shows its ratio and upload in the queue. */
export function showsSeeding(state: TorrentState): boolean {
  return state === "SEEDING" || state === "STOPPED";
}

/** As much of a failed GraphQL request as a torrent action reads. */
export interface TorrentActionError {
  message: string;
  graphQLErrors: readonly { message: string; extensions?: Record<string, unknown> }[];
}

/**
 * How a remove went: done, refused because the torrent has a seeding goal
 * still to meet (asked again only once the operator confirms), or failed.
 * The message is the daemon's own.
 */
export type RemoveOutcome =
  | { kind: "removed" }
  | { kind: "unmetGoal"; message: string }
  | { kind: "failed"; message: string };

export function removeOutcome(error: TorrentActionError | undefined): RemoveOutcome {
  if (!error) return { kind: "removed" };
  const refusal = error.graphQLErrors[0];
  const message = refusal?.message ?? error.message;
  return refusal?.extensions?.code === "SEEDING_GOAL_UNMET"
    ? { kind: "unmetGoal", message }
    : { kind: "failed", message };
}

/** The daemon's own text for a failed torrent action. */
export function torrentErrorText(error: TorrentActionError): string {
  return error.graphQLErrors[0]?.message ?? error.message;
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import type { ResolvedBy } from "./test-via.ts";

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
  | "STOPPED"
  | "MOVING"
  | "NEEDS_CERTIFICATE"
  | "CHECKING_RESUME_DATA";

/** A level a torrent takes a setting from. */
export type SettingLevel = "TORRENT" | "TRACKER_RULE" | "CATEGORY" | "GLOBAL";

/** One setting at one level: inherited, off, or set to `value`. */
export interface LeafData<T> {
  state: "INHERIT" | "OFF" | "SET";
  value: T | null;
}

export type SeedingModeData = "ANY" | "ALL";
export type SeedingActionData = "STOP" | "REMOVE" | "REMOVE_WITH_DATA";
export type SeedTimeCountingData = "CUMULATIVE" | "CONSECUTIVE";

/** Seeding terms at one level, as the daemon sends them. */
export interface SeedingPolicyData {
  ratioGoal: LeafData<number>;
  seedTimeGoalMinutes: LeafData<number>;
  inactiveGoalMinutes: LeafData<number>;
  mode: LeafData<SeedingModeData>;
  action: LeafData<SeedingActionData>;
  seedTimeCounting: LeafData<SeedTimeCountingData>;
}

/** The seeding terms a torrent is held to, and the level each comes from. */
export interface SeedingTermsData {
  ratioGoal: number | null;
  seedTimeGoalMinutes: number | null;
  inactiveGoalMinutes: number | null;
  mode: SeedingModeData;
  action: SeedingActionData;
  seedTimeCounting: SeedTimeCountingData;
  /** Bytes per second; null is none. */
  uploadCap: number | null;
  levels: {
    ratioGoal: SettingLevel;
    seedTimeGoalMinutes: SettingLevel;
    inactiveGoalMinutes: SettingLevel;
    mode: SettingLevel;
    action: SettingLevel;
    seedTimeCounting: SettingLevel;
    uploadCap: SettingLevel;
  };
}

export type TrackerStatus =
  | "NOT_CONTACTED"
  | "WORKING"
  | "NOT_WORKING"
  | "TRACKER_ERROR"
  | "UNREACHABLE"
  | "DISABLED";

/** How eagerly a torrent file is fetched; an ignored file is not fetched. */
export type TorrentFilePriority = "IGNORED" | "NORMAL" | "HIGH" | "MAXIMUM";

export interface TorrentFileData {
  index: number;
  path: string;
  length: number;
  /** Whether the file is fetched at all: its level is above ignored. */
  selected: boolean;
  priority: TorrentFilePriority;
  /** The share of the file in hand, from 0 to 1. */
  progress: number;
}

/** Who named a tracker: the torrent itself, or the operator who added it. */
export type TrackerSource = "TORRENT" | "OPERATOR";

/** Where the label for who named a tracker lives in the dictionary. */
export const trackerSourceKey = (source: TrackerSource) => `next.torrent.trackerSource.${source}`;

/** Where the label for who resolves a name lives in the dictionary. */
export const resolvedByKey = (by: ResolvedBy) => `next.resolvedBy.${by}`;

export interface TorrentTrackerData {
  tier: number;
  url: string;
  source: TrackerSource;
  /** Who resolves its host; none for an address. */
  resolvedBy: ResolvedBy | null;
  status: TrackerStatus;
  /** An announce to it is in flight. */
  updating: boolean;
  message: string | null;
  seeders: number | null;
  leechers: number | null;
  /** The peers its last answer named. */
  peers: number | null;
}

/** One connection to a peer of a torrent. */
export interface TorrentPeerData {
  address: string;
  client: string;
  /** The share of the torrent's pieces the peer has, from 0 to 1. */
  progress: number;
  downloadBps: number;
  uploadBps: number;
}

/** What the queue reads of a torrent beside its job: enough for the seeding column and the row actions. */
export interface TorrentSummaryData {
  id: number;
  state: TorrentState;
  forced: boolean;
  ratio: number;
  uploadBps: number;
}

/**
 * A `torrentUpdates` push: every followed torrent with `fullUpdate`, else
 * the torrents that changed or came and the ids of the ones gone.
 */
export interface TorrentChangesData<T> {
  fullUpdate: boolean;
  torrents: T[];
  removedIds: number[];
}

/** The torrents by id once `changes` lands on `held`. */
export function applyTorrentChanges<T extends { id: number }>(
  held: ReadonlyMap<number, T> | undefined,
  changes: TorrentChangesData<T>,
): Map<number, T> {
  const next = new Map(changes.fullUpdate || !held ? [] : held);
  for (const id of changes.removedIds) next.delete(id);
  for (const torrent of changes.torrents) next.set(torrent.id, torrent);
  return next;
}

/** A torrent as its detail reads it. */
export interface TorrentData extends TorrentSummaryData {
  name: string;
  infohash: string;
  infohashV2: string | null;
  detail: string | null;
  /** Why its route holds it, while its state comes from that hold. */
  routeReason: TorrentRouteReason | null;
  category: string | null;
  private: boolean;
  progress: number;
  totalBytes: number;
  haveBytes: number;
  uploadedBytes: number;
  downloadedBytes: number;
  downloadBps: number;
  /** The torrent's own limits in bytes per second; null is unlimited and 0 closed. */
  uploadLimit: number | null;
  downloadLimit: number | null;
  etaSeconds: number | null;
  peers: number;
  seeds: number;
  leechers: number;
  /** The most peers a torrent connects to. */
  maxPeers: number;
  /** The swarm as its trackers last counted it. */
  swarmSeeds: number | null;
  swarmLeechers: number | null;
  reannounceSeconds: number | null;
  /** Seconds spent running in a session. */
  activeSeconds: number;
  /** Seconds spent complete and seeding, in all. */
  seedingSeconds: number;
  /** Seconds of the current unbroken seeding run. */
  consecutiveSeedingSeconds: number;
  downloadAvgBps: number;
  uploadAvgBps: number;
  wastedBytes: number;
  lastSeenCompleteMs: number | null;
  pieceCount: number | null;
  pieceSize: number | null;
  piecesHave: number | null;
  createdBy: string | null;
  createdAtMs: number | null;
  comment: string | null;
  addedAtMs: number;
  completedAtMs: number | null;
  savePath: string | null;
  seedingLeft: string | null;
  /** The torrent's own terms; null when it inherits every one. */
  seedingOverride: SeedingPolicyData | null;
  /** The terms in effect. */
  seedingPolicy: SeedingTermsData;
  /** The host pattern of the tracker rule the torrent is under, when it is. */
  trackerRule: string | null;
  /** Whether its archives are extracted. */
  unpack: boolean;
  /** The torrent's own unpack; null inherits. */
  unpackOverride: boolean | null;
  route: string | null;
  files: TorrentFileData[];
  /** Whether its pieces are fetched in order. */
  sequential: boolean;
  /** Whether each file's first and last pieces are fetched first. */
  firstLastPieceFirst: boolean;
  trackers: TorrentTrackerData[];
}

/**
 * The label key of a torrent state its job's status does not name: a torrent
 * waiting for a certificate, or one starting from what was saved (qBittorrent's
 * "Checking resume data"). Null for every other state.
 */
export function torrentStateLabelKey(state: TorrentState | undefined): string | null {
  switch (state) {
    case "NEEDS_CERTIFICATE":
      return "next.torrent.needsCertificate";
    case "CHECKING_RESUME_DATA":
      return "next.torrent.checkingResumeData";
    default:
      return null;
  }
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

/** What a torrent session shows the trackers and peers it reaches. */
export interface TorrentExposure {
  resolvedBy: ResolvedBy | null;
  /** The address a tracker sees; none until one is known. */
  address: string | null;
  /** The port announced; 0 when none is. */
  announcedPort: number;
  inbound: boolean;
  udpTrackers: boolean;
  dht: boolean;
  utp: boolean;
}

/** An exposure as label and value pairs, in the order the views show them. */
export function exposureValues(t: Translate, exposure: TorrentExposure): { label: string; value: string }[] {
  const yesNo = (on: boolean) => t(on ? "next.common.yes" : "next.common.no");
  return [
    {
      label: t("next.proxies.resolvedBy"),
      value: exposure.resolvedBy === null ? "\u2014" : t(resolvedByKey(exposure.resolvedBy)),
    },
    { label: t("next.exposure.address"), value: exposure.address ?? t("next.exposure.unknown") },
    { label: t("next.exposure.announcedPort"), value: String(exposure.announcedPort) },
    { label: t("next.exposure.inbound"), value: yesNo(exposure.inbound) },
    { label: t("next.exposure.udpTrackers"), value: yesNo(exposure.udpTrackers) },
    { label: t("next.exposure.dht"), value: yesNo(exposure.dht) },
    { label: t("next.exposure.utp"), value: yesNo(exposure.utp) },
  ];
}

/** An exposure on one line. */
export const exposureText = (t: Translate, exposure: TorrentExposure): string =>
  exposureValues(t, exposure)
    .map(({ label, value }) => `${label} ${value}`)
    .join(" \u00b7 ");

export const TORRENT_ROUTE_REASONS = [
  "NOT_ASKED_TO_OPEN",
  "LEG_DOWN",
  "LEG_MISSING",
  "LEG_REVOKED",
  "QUOTA_SILENCED",
  "TORRENTS_DISABLED",
  "TORRENT_ROUTE_LEGS",
  "TORRENT_ROUTE_DIRECT_FALLBACK",
  "TORRENT_ROUTE_RUNGS",
  "LEG_OPEN_FAILED",
  "STATE_DIRECTORY_FAILED",
  "LISTEN_PORT_UNAVAILABLE",
  "LISTEN_PORT_BIND_FAILED",
  "SESSION_STATE_UNREADABLE",
  "SESSION_BUILD_FAILED",
  "BLOCKLIST_FAILED",
  "SESSION_LIMIT",
] as const;

export type TorrentRouteReason = (typeof TORRENT_ROUTE_REASONS)[number];

export function routeReasonText(t: Translate, reason: TorrentRouteReason): string {
  return t(reason.startsWith("TORRENT_ROUTE_") ? `next.refusal.${reason}` : `next.torrents.routeReason.${reason}`);
}

/**
 * A torrent route's exposure cell: the open session's exposure, or, while it
 * is closed, who resolves its names.
 */
export function routeExposureText(
  t: Translate,
  route: { resolvedBy: ResolvedBy | null; exposure: TorrentExposure | null },
): string {
  if (route.exposure !== null) return exposureText(t, route.exposure);
  return route.resolvedBy === null ? "\u2014" : `${t("next.proxies.resolvedBy")} ${t(resolvedByKey(route.resolvedBy))}`;
}

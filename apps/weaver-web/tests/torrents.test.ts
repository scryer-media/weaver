// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  removeOutcome,
  routeExposureText,
  showsSeeding,
  trackerSourceKey,
  type TorrentData,
} from "../src/lib/torrents.ts";
import { EM_DASH, formatRate, formatSize } from "../src/next/data/format.ts";
import { informationFields, transferFields } from "../src/next/features/torrents/general.ts";
import { englishTranslate as t } from "./english-translate.ts";

test("only a refusal over an unmet seeding goal asks for a second confirm", () => {
  assert.deepEqual(removeOutcome(undefined), { kind: "removed" });
  const refusal = (code: string, message: string) => ({
    message: `[GraphQL] ${message}`,
    graphQLErrors: [{ message, extensions: { code } }],
  });
  assert.deepEqual(removeOutcome(refusal("SEEDING_GOAL_UNMET", "ratio 0.40 of 1.00")), {
    kind: "unmetGoal",
    message: "ratio 0.40 of 1.00",
  });
  assert.deepEqual(removeOutcome(refusal("NOT_FOUND", "no such torrent")), {
    kind: "failed",
    message: "no such torrent",
  });
  assert.deepEqual(removeOutcome({ message: "network down", graphQLErrors: [] }), {
    kind: "failed",
    message: "network down",
  });
});

test("the seeding column shows while a torrent seeds or has stopped", () => {
  assert.equal(showsSeeding("SEEDING"), true);
  assert.equal(showsSeeding("STOPPED"), true);
  for (const state of ["QUEUED", "FETCHING_METADATA", "CHECKING_FILES", "DOWNLOADING", "STALLED", "PAUSED"] as const) {
    assert.equal(showsSeeding(state), false);
  }
});

const torrent = (overrides: Partial<TorrentData> = {}): TorrentData => ({
  id: 7,
  state: "DOWNLOADING",
  forced: false,
  ratio: 0.5,
  uploadBps: 1024,
  name: "set",
  infohash: "ab".repeat(20),
  infohashV2: null,
  detail: null,
  category: null,
  private: true,
  progress: 0.5,
  totalBytes: 4 * 1024 * 1024,
  haveBytes: 2 * 1024 * 1024,
  uploadedBytes: 1024 * 1024,
  downloadedBytes: 2 * 1024 * 1024,
  downloadBps: 2048,
  etaSeconds: 90,
  peers: 5,
  seeds: 2,
  leechers: 3,
  maxPeers: 500,
  swarmSeeds: 40,
  swarmLeechers: null,
  reannounceSeconds: 600,
  activeSeconds: 3600,
  downloadAvgBps: 512,
  uploadAvgBps: 256,
  wastedBytes: 16 * 1024,
  lastSeenCompleteMs: null,
  pieceCount: 16,
  pieceSize: 256 * 1024,
  piecesHave: 8,
  createdBy: "maker 1.0",
  createdAtMs: null,
  comment: null,
  addedAtMs: 1_700_000_000_000,
  completedAtMs: null,
  savePath: "/complete/set",
  seedingLeft: null,
  seedingOverride: null,
  seedingPolicy: {
    ratioGoal: null,
    seedTimeGoalMinutes: null,
    inactiveGoalMinutes: null,
    mode: "ANY",
    action: "STOP",
    maxUploadBps: null,
  },
  seedingSource: "GLOBAL",
  seedingRule: null,
  unpack: true,
  route: null,
  files: [],
  trackers: [],
  ...overrides,
});

const byLabel = (fields: { label: string; value: string }[]) =>
  Object.fromEntries(fields.map((field) => [field.label, field.value]));

test("the transfer section reads as qBittorrent's, with the swarm's totals and averages", () => {
  const fields = transferFields(t, torrent());
  assert.deepEqual(
    fields.map((field) => field.label),
    [
      "Time active",
      "ETA",
      "Connections",
      "Downloaded",
      "Uploaded",
      "Seeds",
      "Download speed",
      "Upload speed",
      "Peers",
      "Wasted",
      "Share ratio",
      "Reannounce in",
      "Last seen complete",
    ],
  );
  const values = byLabel(fields);
  assert.equal(values["Time active"], "1h 0m");
  assert.equal(values["ETA"], "1m");
  assert.equal(values["Connections"], "5 (500 max)");
  assert.equal(values["Seeds"], "2 (40 total)");
  // No tracker counted the leechers.
  assert.equal(values["Peers"], `3 (${EM_DASH} total)`);
  assert.equal(values["Download speed"], `${formatRate(2048)} (${formatRate(512)} avg.)`);
  assert.equal(values["Wasted"], formatSize(16 * 1024));
  assert.equal(values["Share ratio"], "0.50");
  assert.equal(values["Reannounce in"], "10m");
  assert.equal(values["Last seen complete"], EM_DASH);
  // No session yet, no ETA, no announce due.
  const idle = byLabel(transferFields(t, torrent({ activeSeconds: 0, etaSeconds: null, reannounceSeconds: null })));
  assert.deepEqual([idle["Time active"], idle["ETA"], idle["Reannounce in"]], [EM_DASH, EM_DASH, EM_DASH]);
});

test("the information section names the pieces, the maker and where the torrent is", () => {
  const values = byLabel(informationFields(t, torrent()));
  assert.equal(values["Pieces"], `16 × ${formatSize(256 * 1024)} (have 8)`);
  assert.equal(values["Created by"], "maker 1.0");
  assert.equal(values["Created on"], EM_DASH);
  assert.equal(values["Comment"], EM_DASH);
  assert.equal(values["Private"], "Yes");
  assert.equal(values["Info hash v1"], "ab".repeat(20));
  assert.equal(values["Info hash v2"], EM_DASH);
  assert.equal(values["Save path"], "/complete/set");
  assert.equal(values["Route"], EM_DASH);
  assert.equal(values["Unpack"], "On");
  const hybrid = byLabel(informationFields(t, torrent({ infohashV2: "cd".repeat(32) })));
  assert.equal(hybrid["Info hash v2"], "cd".repeat(32));
  // A magnet without its metainfo has no pieces yet.
  const magnet = byLabel(informationFields(t, torrent({ pieceCount: null, pieceSize: null, piecesHave: null })));
  assert.equal(magnet["Pieces"], EM_DASH);
});

test("a tracker row says whether the torrent or the operator named it", () => {
  assert.equal(t(trackerSourceKey("TORRENT")), "Torrent");
  assert.equal(t(trackerSourceKey("OPERATOR")), "Added");
});

test("a torrent route shows who resolves its names, and whether peers can reach its session", () => {
  const exposure = {
    resolvedBy: "PROXY" as const,
    address: null,
    announcedPort: 0,
    inbound: false,
    udpTrackers: false,
    dht: false,
    utp: false,
  };
  assert.equal(
    routeExposureText(t, { resolvedBy: "PROXY", exposure }),
    "Resolved by Proxy · Tracker sees Unknown · Announced port 0 · Inbound No · UDP trackers No · DHT No · uTP No",
  );
  assert.equal(
    routeExposureText(t, {
      resolvedBy: "HOST",
      exposure: { ...exposure, resolvedBy: "HOST", address: "203.0.113.7", announcedPort: 40001, inbound: true },
    }),
    "Resolved by Host DNS · Tracker sees 203.0.113.7 · Announced port 40001 · Inbound Yes · UDP trackers No · DHT No · uTP No",
  );
  assert.equal(routeExposureText(t, { resolvedBy: "SERVERS", exposure: null }), "Resolved by DNS servers");
  assert.equal(routeExposureText(t, { resolvedBy: null, exposure: null }), EM_DASH);
});

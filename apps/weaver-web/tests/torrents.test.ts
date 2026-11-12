// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  applyTorrentChanges,
  removeOutcome,
  routeExposureText,
  routeReasonText,
  TORRENT_ROUTE_REASONS,
  showsSeeding,
  torrentStateLabelKey,
  trackerSourceKey,
  type TorrentData,
} from "../src/lib/torrents.ts";
import { EM_DASH, formatRate, formatSize } from "../src/next/data/format.ts";
import { informationFields, transferFields, transferNote } from "../src/next/features/torrents/general.ts";
import { nextDe } from "../src/next/i18n/de.ts";
import { nextEn } from "../src/next/i18n/en.ts";
import { nextEs } from "../src/next/i18n/es.ts";
import { nextFr } from "../src/next/i18n/fr.ts";
import { nextIt } from "../src/next/i18n/it.ts";
import { nextJa } from "../src/next/i18n/ja.ts";
import { nextKo } from "../src/next/i18n/ko.ts";
import { nextPt } from "../src/next/i18n/pt.ts";
import { nextZh } from "../src/next/i18n/zh.ts";
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
  routeReason: null,
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
    uploadCap: null,
    levels: {
      ratioGoal: "GLOBAL",
      seedTimeGoalMinutes: "GLOBAL",
      inactiveGoalMinutes: "GLOBAL",
      mode: "GLOBAL",
      action: "GLOBAL",
      uploadCap: "GLOBAL",
    },
  },
  trackerRule: null,
  unpack: true,
  unpackOverride: null,
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

test("every locale renders route reasons and reuses its route refusal wording", () => {
  const faults = ["TORRENT_ROUTE_LEGS", "TORRENT_ROUTE_DIRECT_FALLBACK", "TORRENT_ROUTE_RUNGS"] as const;
  for (const [locale, dictionary] of Object.entries({ en: nextEn, de: nextDe, es: nextEs, fr: nextFr, it: nextIt, ja: nextJa, ko: nextKo, pt: nextPt, zh: nextZh })) {
    const translate = (key: string) => {
      const value = dictionary[key];
      assert.ok(value?.trim(), `${locale}: missing ${key}`);
      return value;
    };
    for (const reason of TORRENT_ROUTE_REASONS) {
      assert.ok(routeReasonText(translate, reason).trim());
    }
    for (const reason of faults) {
      assert.equal(routeReasonText(translate, reason), dictionary[`next.refusal.${reason}`]);
      assert.equal(dictionary[`next.torrents.routeReason.${reason}`], undefined);
    }
  }
  assert.equal(routeReasonText(t, "LEG_DOWN"), "Leg down");
  assert.equal(routeReasonText(t, "LEG_OPEN_FAILED"), "Leg failed to open");
  assert.equal(routeReasonText(t, "STATE_DIRECTORY_FAILED"), "State directory unavailable");
  assert.equal(routeReasonText(t, "LISTEN_PORT_UNAVAILABLE"), "Listen port unavailable");
});

test("the transfer section notes its route's reason in each locale, else the server's words", () => {
  for (const [locale, dictionary] of Object.entries({ en: nextEn, de: nextDe, es: nextEs, fr: nextFr, it: nextIt, ja: nextJa, ko: nextKo, pt: nextPt, zh: nextZh })) {
    const translate = (key: string) => {
      const value = dictionary[key];
      assert.ok(value?.trim(), `${locale}: missing ${key}`);
      return value;
    };
    for (const reason of TORRENT_ROUTE_REASONS) {
      const key = reason.startsWith("TORRENT_ROUTE_") ? `next.refusal.${reason}` : `next.torrents.routeReason.${reason}`;
      assert.equal(transferNote(translate, torrent({ state: "STALLED", routeReason: reason })), dictionary[key], `${locale}: ${reason}`);
    }
  }
  assert.equal(transferNote(t, torrent({ state: "STALLED", routeReason: "SESSION_LIMIT" })), "Session limit reached");
  assert.equal(transferNote(t, torrent({ state: "STOPPED", detail: "disk gone" })), "disk gone");
  assert.equal(transferNote(t, torrent()), undefined);
});

test("a torrentUpdates push lands on the torrents held: whole when full, else what moved", () => {
  const row = (id: number, ratio: number) => ({ id, ratio });
  const full = applyTorrentChanges(undefined, { fullUpdate: true, torrents: [row(1, 0), row(2, 0)], removedIds: [] });
  assert.deepEqual([...full.values()], [row(1, 0), row(2, 0)]);
  const moved = applyTorrentChanges(full, { fullUpdate: false, torrents: [row(2, 1)], removedIds: [1] });
  assert.deepEqual([...moved.values()], [row(2, 1)]);
  assert.deepEqual([...full.values()], [row(1, 0), row(2, 0)]);
  const again = applyTorrentChanges(moved, { fullUpdate: true, torrents: [row(3, 0)], removedIds: [] });
  assert.deepEqual([...again.values()], [row(3, 0)]);
});

test("a torrent starting from what was saved, one needing a certificate and one missing its files name their own state", () => {
  assert.equal(torrentStateLabelKey("CHECKING_RESUME_DATA"), "next.torrent.checkingResumeData");
  assert.equal(torrentStateLabelKey("NEEDS_CERTIFICATE"), "next.torrent.needsCertificate");
  assert.equal(torrentStateLabelKey("FILES_MISSING"), "next.torrent.filesMissing");
  assert.equal(t("next.torrent.filesMissing"), "Files missing");
  assert.equal(torrentStateLabelKey("QUEUED"), null);
  assert.equal(torrentStateLabelKey(undefined), null);
  assert.equal(t("next.torrent.checkingResumeData"), "Checking resume data");
  for (const [locale, dictionary] of Object.entries({ en: nextEn, de: nextDe, es: nextEs, fr: nextFr, it: nextIt, ja: nextJa, ko: nextKo, pt: nextPt, zh: nextZh })) {
    assert.ok(dictionary["next.torrent.checkingResumeData"], locale);
    assert.ok(dictionary["next.torrent.filesMissing"], locale);
  }
});

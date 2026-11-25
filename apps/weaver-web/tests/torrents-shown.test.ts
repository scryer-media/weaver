// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { isTorrentConsumer, type ConsumerKind } from "../src/lib/networking.ts";
import {
  asksHowTorrentsEnd,
  stopNowInput,
  torrentAddsOffered,
  torrentSettingsDisclosed,
  torrentSwitchWaits,
  torrentsAvailable,
  torrentsAvailableView,
  torrentsShown,
  torrentsStateFrom,
  torrentsView,
  windDownStoppable,
  withoutTorrentOnly,
  type TorrentPresence,
  type TorrentsState,
} from "../src/next/data/torrents-shown.ts";

const STATES: readonly TorrentsState[] = [
  "on",
  "off",
  "unavailable",
  "unknown",
];

const presence = (
  enabled: boolean,
  jobs: boolean,
  running = jobs,
): TorrentPresence => ({
  available: true,
  enabled,
  jobs,
  running,
  windingDown: false,
  windDown: null,
});

test("torrent things show while torrents are on or any torrent job is left, and never before the answer", () => {
  assert.equal(torrentsStateFrom(presence(true, false)), "on");
  assert.equal(torrentsStateFrom(presence(true, true)), "on");
  // Turned off with jobs still there, seeding or stopped: as if on.
  assert.equal(torrentsStateFrom(presence(false, true)), "on");
  assert.equal(torrentsStateFrom(presence(false, true, false)), "on");
  assert.equal(torrentsStateFrom(presence(false, false)), "off");
  // Not read yet, or the read failed: unknown, never off.
  assert.equal(torrentsStateFrom(undefined), "unknown");
  assert.equal(torrentsStateFrom(null), "unknown");
  assert.deepEqual(
    [
      presence(true, false),
      presence(false, true),
      presence(false, false),
      undefined,
    ].map((answer) => torrentsShown(torrentsStateFrom(answer))),
    [true, true, false, false],
  );
});

test("a new torrent is offered only while torrents are on, even while torrent jobs left still show", () => {
  const winding = {
    ...presence(false, true),
    windingDown: true,
    windDown: "FINISH_ALL" as const,
  };
  assert.equal(torrentAddsOffered(presence(true, false)), true);
  assert.equal(torrentAddsOffered(presence(true, true)), true);
  for (const off of [
    presence(false, false),
    presence(false, true),
    presence(false, true, false),
    winding,
  ]) {
    assert.equal(torrentAddsOffered(off), false);
  }
  // Jobs left keep torrent things showing, but offer no new torrent.
  assert.equal(torrentsShown(torrentsStateFrom(winding)), true);
  assert.equal(torrentAddsOffered(undefined), false);
  assert.equal(torrentAddsOffered(null), false);
});

test("the Torrents panel shows the switch alone while it is off, torrent jobs left or not", () => {
  const rest = ["seeding", "trackers", "connection"];
  assert.deepEqual(torrentSettingsDisclosed(true, "enabled", rest), [
    "enabled",
    ...rest,
  ]);
  assert.deepEqual(torrentSettingsDisclosed(false, "enabled", rest), [
    "enabled",
  ]);
});

test("turning torrents off asks how they end while some torrent is running, or that is not known", () => {
  // On to off, a torrent running: ask.
  assert.equal(asksHowTorrentsEnd(true, false, presence(true, true)), true);
  // Torrent jobs left, none running (paused or stopped), or none at all: no prompt.
  assert.equal(
    asksHowTorrentsEnd(true, false, presence(true, true, false)),
    false,
  );
  assert.equal(asksHowTorrentsEnd(true, false, presence(true, false)), false);
  // Not known: ask, never stop them at once unasked.
  assert.equal(asksHowTorrentsEnd(true, false, undefined), true);
  assert.equal(asksHowTorrentsEnd(true, false, null), true);
  // A save that does not turn torrents off never asks.
  assert.equal(asksHowTorrentsEnd(true, true, presence(true, true)), false);
  assert.equal(asksHowTorrentsEnd(false, false, presence(false, true)), false);
  assert.equal(asksHowTorrentsEnd(false, true, presence(false, true)), false);
});

test("the Torrents switch waits to be turned off until the server says which torrents run", () => {
  assert.equal(torrentSwitchWaits(true, undefined), true);
  assert.equal(torrentSwitchWaits(true, null), true);
  assert.equal(torrentSwitchWaits(true, presence(true, true)), false);
  // Turning on asks nothing, so it never waits.
  assert.equal(torrentSwitchWaits(false, undefined), false);
});

test("a wind-down under way offers Stop now, which sends torrents off to stop at once", () => {
  const winding: TorrentPresence = {
    ...presence(false, true),
    windingDown: true,
    windDown: "FINISH_ALL" as const,
  };
  assert.equal(windDownStoppable(false, winding), true);
  // Not winding down, not known, or the switch turned on again in the form: nothing to stop.
  assert.equal(windDownStoppable(false, presence(false, true)), false);
  assert.equal(windDownStoppable(false, undefined), false);
  assert.equal(windDownStoppable(true, winding), false);
  assert.deepEqual(stopNowInput(), {
    torrent: { enabled: false, whenDisabled: "STOP_NOW" },
  });
});

test("torrent-only things show only once torrents are known to be on", () => {
  assert.deepEqual(
    STATES.map((state) => [state, torrentsShown(state)]),
    [
      ["on", true],
      ["off", false],
      ["unavailable", false],
      ["unknown", false],
    ],
  );
});

// A server started without WEAVER_TORRENTS: the third state, apart from off.
const unavailable = (enabled: boolean, jobs: boolean): TorrentPresence => ({
  ...presence(enabled, jobs),
  available: false,
});

test("a server without torrents is unavailable whatever else it says, and shows and offers nothing", () => {
  for (const answer of [
    unavailable(false, false),
    unavailable(true, false),
    unavailable(false, true),
    unavailable(true, true),
  ]) {
    assert.equal(torrentsStateFrom(answer), "unavailable");
    assert.equal(torrentsShown(torrentsStateFrom(answer)), false);
    assert.equal(torrentAddsOffered(answer), false);
  }
  // A torrent-only screen leaves as it does while torrents are off.
  assert.equal(
    torrentsView("unavailable", "page", "redirect", "loading"),
    "redirect",
  );
});

test("the server has torrents while they are on or off, and not while unavailable or unsaid", () => {
  assert.deepEqual(
    STATES.map((state) => [state, torrentsAvailable(state)]),
    [
      ["on", true],
      ["off", true],
      ["unavailable", false],
      ["unknown", false],
    ],
  );
  // What a direct visit to the Torrents settings draws: the panel, no such panel, or nothing yet.
  assert.deepEqual(
    STATES.map((state) =>
      torrentsAvailableView(state, "panel", "none", "wait"),
    ),
    ["panel", "panel", "none", "wait"],
  );
});

test("the settings rail and search hold the Torrents panel only while the server has torrents", () => {
  // The same shape as the settings panels list, in rail order.
  const panels = [
    { slug: "general" },
    { slug: "torrents", torrentOnly: true as const },
    { slug: "rss" },
  ];
  const slugs = (state: TorrentsState) =>
    withoutTorrentOnly(
      panels,
      torrentsAvailable(state),
      (panel) => panel.torrentOnly === true,
    ).map((panel) => panel.slug);
  assert.deepEqual(slugs("on"), ["general", "torrents", "rss"]);
  // Off in the settings: the panel stays, since its switch turns them on.
  assert.deepEqual(slugs("off"), ["general", "torrents", "rss"]);
  assert.deepEqual(slugs("unavailable"), ["general", "rss"]);
  assert.deepEqual(slugs("unknown"), ["general", "rss"]);
});

test("a filter list keeps every item while on and drops only the torrent-only ones otherwise", () => {
  // The routes the networking pages list, draw and offer to the speed test.
  const consumers: { key: string; kind: ConsumerKind }[] = [
    { key: "outbound", kind: "OUTBOUND" },
    { key: "server:1", kind: "SERVER" },
    { key: "rss:2", kind: "RSS" },
    { key: "torrent:default", kind: "TORRENT_DEFAULT" },
    { key: "torrent:category:3", kind: "TORRENT_CATEGORY" },
    { key: "torrent:tracker:4", kind: "TORRENT_TRACKER" },
  ];
  const keys = (state: TorrentsState) =>
    withoutTorrentOnly(consumers, torrentsShown(state), (consumer) =>
      isTorrentConsumer(consumer.kind),
    ).map((consumer) => consumer.key);
  assert.deepEqual(
    keys("on"),
    consumers.map((consumer) => consumer.key),
  );
  assert.deepEqual(keys("off"), ["outbound", "server:1", "rss:2"]);
  assert.deepEqual(keys("unavailable"), ["outbound", "server:1", "rss:2"]);
  assert.deepEqual(keys("unknown"), ["outbound", "server:1", "rss:2"]);
});

test("the list handed back is a copy, so a caller cannot change the shared list", () => {
  const items = [{ id: "a", torrentOnly: false }] as const;
  const kept = withoutTorrentOnly(items, true, (item) => item.torrentOnly);
  assert.notEqual(kept, items);
  assert.deepEqual(kept, items);
});

test("the monitoring nav drops the torrents dashboard unless on, and a direct visit waits, then leaves", () => {
  // The same shape as the Monitoring dashboards list, in nav order.
  const dashboards = [
    { id: "overview" },
    { id: "providers" },
    { id: "integrity" },
    { id: "torrents", torrentOnly: true },
    { id: "engine" },
  ];
  const tabs = (state: TorrentsState) =>
    withoutTorrentOnly(
      dashboards,
      torrentsShown(state),
      (dashboard) => dashboard.torrentOnly === true,
    ).map((dashboard) => dashboard.id);
  assert.deepEqual(tabs("on"), [
    "overview",
    "providers",
    "integrity",
    "torrents",
    "engine",
  ]);
  assert.deepEqual(tabs("off"), [
    "overview",
    "providers",
    "integrity",
    "engine",
  ]);
  assert.deepEqual(tabs("unknown"), [
    "overview",
    "providers",
    "integrity",
    "engine",
  ]);

  // What /monitoring/torrents draws: the page, the redirect, or the loading state.
  assert.equal(torrentsView("on", "page", "redirect", "loading"), "page");
  assert.equal(torrentsView("off", "page", "redirect", "loading"), "redirect");
  assert.equal(
    torrentsView("unknown", "page", "redirect", "loading"),
    "loading",
  );
});

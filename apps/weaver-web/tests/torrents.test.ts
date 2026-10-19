// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import { addFileKind, base64Of, isMagnetLink, removeOutcome, showsSeeding } from "../src/lib/torrents.ts";

test("a dropped or picked file is told apart by its extension", () => {
  assert.equal(addFileKind("Some.Release.torrent"), "torrent");
  assert.equal(addFileKind("SOME.TORRENT"), "torrent");
  assert.equal(addFileKind("release.nzb"), "nzb");
  assert.equal(addFileKind("release.nzb.xz"), "nzb");
  assert.equal(addFileKind("release.txt"), null);
  assert.equal(addFileKind("torrent.nzb"), "nzb");
});

test("only text starting with a magnet scheme is a magnet link", () => {
  assert.equal(isMagnetLink("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"), true);
  assert.equal(isMagnetLink("  MAGNET:?xt=urn:btih:abc  "), true);
  assert.equal(isMagnetLink("https://example.invalid/file.torrent"), false);
  assert.equal(isMagnetLink("magnet:xt"), false);
  assert.equal(isMagnetLink(""), false);
});

test("a metainfo file is sent as base64 of its exact bytes", () => {
  const bytes = new Uint8Array(70000).map((_, index) => index % 251);
  assert.equal(base64Of(bytes), Buffer.from(bytes).toString("base64"));
  assert.equal(base64Of(new Uint8Array()), "");
});

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

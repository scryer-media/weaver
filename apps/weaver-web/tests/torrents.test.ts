// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import { removeOutcome, showsSeeding } from "../src/lib/torrents.ts";

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

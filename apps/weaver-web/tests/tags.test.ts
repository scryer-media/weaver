// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { cleanTag, splitTags } from "../src/next/data/tags.ts";

test("a tag is trimmed, and an empty one is left out", () => {
  assert.equal(cleanTag("  tv "), "tv");
  assert.equal(cleanTag("   "), null);
  assert.equal(cleanTag("a,b"), null);
});

test("a list keeps each tag once, in order, case as typed", () => {
  assert.deepEqual(splitTags(" tv, hd ,,tv, TV , "), ["tv", "hd", "TV"]);
  assert.deepEqual(splitTags(""), []);
});

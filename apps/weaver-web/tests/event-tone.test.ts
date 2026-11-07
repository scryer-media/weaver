// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { eventTone } from "../src/next/data/event-tone.ts";

test("an archive given up on is something the engine decided", () => {
  assert.equal(eventTone("ARCHIVE_DEMOTED"), "note");
});

test("event kinds keep their tones", () => {
  assert.equal(eventTone("REPAIR_FAILED"), "bad");
  assert.equal(eventTone("EXTRACTION_COMPLETE"), "good");
  assert.equal(eventTone("SEGMENT_RETRY_SCHEDULED"), "note");
  assert.equal(eventTone("JOB_CREATED"), "plain");
});

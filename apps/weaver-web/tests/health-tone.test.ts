// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { criticalHealthMilli, healthTone } from "../src/lib/health-tone.ts";

test("critical health follows the server's formula", () => {
  assert.equal(criticalHealthMilli(1000, 0), 850);
  assert.equal(criticalHealthMilli(1000, 501), 0);
  assert.equal(criticalHealthMilli(1000, 500), 0);
  assert.equal(criticalHealthMilli(1000, 100), 888);
  // A 1.1 GB post with a 101 MB recovery set.
  assert.equal(criticalHealthMilli(1_095_490_803, 101_003_076), 898);
});

test("health bands: green, yellow, orange, and red below critical", () => {
  const none = 0;
  assert.equal(healthTone(1000, none), "good");
  assert.equal(healthTone(950, none), "good");
  assert.equal(healthTone(949, none), "warn");
  assert.equal(healthTone(850, none), "warn");
  assert.equal(healthTone(849, none), "low");
  assert.equal(healthTone(0, none), "low");
  assert.equal(healthTone(849, 850), "failing");
  assert.equal(healthTone(850, 850), "warn");
  assert.equal(healthTone(960, 970), "failing");
  assert.equal(healthTone(999, 898), "good");
});

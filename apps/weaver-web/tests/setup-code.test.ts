// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  formatSetupCode,
  SETUP_CODE_DISPLAY_LENGTH,
  SETUP_CODE_PLACEHOLDER,
} from "../src/lib/setup-code.ts";

test("a typed setup code is grouped around a hyphen once it passes four characters", () => {
  assert.equal(formatSetupCode("k"), "K");
  assert.equal(formatSetupCode("k7pm"), "K7PM");
  assert.equal(formatSetupCode("k7pm2"), "K7PM-2");
  assert.equal(formatSetupCode("k7pm2xqr"), "K7PM-2XQR");
});

test("a pasted or re-edited setup code keeps one hyphen and eight characters", () => {
  assert.equal(formatSetupCode(" k7pm-2xqr "), "K7PM-2XQR");
  assert.equal(formatSetupCode("K7PM 2XQR"), "K7PM-2XQR");
  assert.equal(formatSetupCode("K7PM-2XQRS"), "K7PM-2XQR");
  assert.equal(formatSetupCode("K7PM-"), "K7PM");
  assert.equal(formatSetupCode("K7-PM2XQR"), "K7PM-2XQR");
});

test("the field's limit and placeholder have the displayed code's shape", () => {
  assert.equal(SETUP_CODE_DISPLAY_LENGTH, "K7PM-2XQR".length);
  assert.equal(SETUP_CODE_PLACEHOLDER, "····-····");
  assert.equal(formatSetupCode("K7PM2XQR").length, SETUP_CODE_DISPLAY_LENGTH);
});

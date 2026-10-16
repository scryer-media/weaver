// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";

import { ignoredByPasswordManagers } from "../src/lib/password-manager.ts";

test("password fields are hidden from password managers unless they are the Weaver login", () => {
  assert.equal(ignoredByPasswordManagers({ type: "password" }), true);
  assert.equal(ignoredByPasswordManagers({ type: "password", autoComplete: "off" }), true);
  assert.equal(ignoredByPasswordManagers({ type: "password", autoComplete: "current-password" }), false);
  assert.equal(ignoredByPasswordManagers({ type: "password", autoComplete: "new-password" }), false);
  assert.equal(ignoredByPasswordManagers({ type: "text" }), false);
});

test("a field can opt in or out explicitly", () => {
  assert.equal(ignoredByPasswordManagers({ type: "text", secret: true }), true);
  assert.equal(ignoredByPasswordManagers({ type: "password", secret: false }), false);
});

test("every field of a screen kept from password managers is hidden, bar the Weaver login and a field that opts out", () => {
  for (const type of ["text", "url", "number", "time", "textarea", undefined]) {
    assert.equal(ignoredByPasswordManagers({ type, everyField: true }), true);
    assert.equal(ignoredByPasswordManagers({ type, everyField: false }), false);
  }
  assert.equal(ignoredByPasswordManagers({ type: "password", autoComplete: "current-password", everyField: true }), false);
  assert.equal(ignoredByPasswordManagers({ type: "password", autoComplete: "new-password", everyField: true }), false);
  assert.equal(ignoredByPasswordManagers({ type: "text", secret: false, everyField: true }), false);
});

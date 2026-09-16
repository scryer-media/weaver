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

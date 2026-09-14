import assert from "node:assert/strict";
import test from "node:test";
import { formatSetupCode } from "../src/lib/setup-code.ts";

test("a typed setup code is grouped around a hyphen once it passes three characters", () => {
  assert.equal(formatSetupCode("k"), "K");
  assert.equal(formatSetupCode("k7p"), "K7P");
  assert.equal(formatSetupCode("k7pm"), "K7P-M");
  assert.equal(formatSetupCode("k7pm2x"), "K7P-M2X");
});

test("a pasted or re-edited setup code keeps one hyphen and six characters", () => {
  assert.equal(formatSetupCode(" k7p-m2x "), "K7P-M2X");
  assert.equal(formatSetupCode("K7P M2X"), "K7P-M2X");
  assert.equal(formatSetupCode("K7P-M2XQ"), "K7P-M2X");
  assert.equal(formatSetupCode("K7P-"), "K7P");
  assert.equal(formatSetupCode("K7-PM2X"), "K7P-M2X");
});

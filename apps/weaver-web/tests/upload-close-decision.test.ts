import assert from "node:assert/strict";
import test from "node:test";
import { shouldCloseAfterSubmit } from "../src/features/upload/close-decision.ts";

test("closes when every entry submitted (incl. a duplicate accepted-with-warning)", () => {
  // The regression: a queue-duplicate that is accepted lands as "submitted" and
  // is retained in the list, yet the dialog must still dismiss.
  assert.equal(shouldCloseAfterSubmit(["submitted"]), true);
  assert.equal(shouldCloseAfterSubmit(["submitted", "submitted"]), true);
});

test("stays open when any entry failed or is retained for force-accept", () => {
  assert.equal(shouldCloseAfterSubmit(["submitted", "failed"]), false);
  // A policy-blocked duplicate is retained as "staged" awaiting force-accept.
  assert.equal(shouldCloseAfterSubmit(["submitted", "staged"]), false);
  assert.equal(shouldCloseAfterSubmit(["submitting"]), false);
});

test("stays open on an empty list (nothing was submitted)", () => {
  assert.equal(shouldCloseAfterSubmit([]), false);
});

import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_DUPLICATE_POLICY,
  normalizeDuplicatePolicy,
} from "../src/features/duplicates/duplicate-policy.ts";
import {
  duplicateActionI18nKey,
  duplicateFingerprintKindI18nKey,
  duplicateLifecycleI18nKey,
  submissionOutcomeI18nKey,
  submissionStatusCanForceRetry,
  submissionStatusIsDurable,
} from "../src/features/duplicates/duplicate-presentation.ts";
import { duplicateSubmissionInput } from "../src/features/upload/duplicate-submission.ts";

test("SCORE keeps a normalized candidate key and integer score", () => {
  assert.deepEqual(
    duplicateSubmissionInput({ force: false, mode: "SCORE", key: "  release-group  ", score: "42" }),
    { force: false, dupeMode: "SCORE", dupeKey: "release-group", dupeScore: 42 },
  );
});

test("ordinary and ALL submissions do not send semantic candidate data", () => {
  for (const mode of ["ENFORCE", "ALL"] as const) {
    assert.deepEqual(
      duplicateSubmissionInput({ force: false, mode, key: "release-group", score: "42" }),
      { force: false, dupeMode: mode, dupeKey: null, dupeScore: null },
    );
  }
});

test("FORCE wins over SCORE and never forwards semantic candidate data", () => {
  assert.deepEqual(
    duplicateSubmissionInput({ force: true, mode: "SCORE", key: "release-group", score: "42" }),
    { force: true, dupeMode: "FORCE", dupeKey: null, dupeScore: null },
  );
});

test("invalid SCORE input is represented as an absent score", () => {
  assert.deepEqual(
    duplicateSubmissionInput({ force: false, mode: "SCORE", key: "", score: "not-a-score" }),
    { force: false, dupeMode: "SCORE", dupeKey: null, dupeScore: null },
  );
});

test("duplicate policy defaults preserve the accepted status matrix", () => {
  assert.deepEqual(normalizeDuplicatePolicy(), DEFAULT_DUPLICATE_POLICY);
  assert.deepEqual(normalizeDuplicatePolicy({ strictActiveOrSuccess: "WARN" }), {
    ...DEFAULT_DUPLICATE_POLICY,
    strictActiveOrSuccess: "WARN",
  });
});

test("every structured submission status has a localized outcome mapping", () => {
  const cases = [
    ["ACCEPTED", true],
    ["WARNED", true],
    ["PAUSED", true],
    ["PARKED", true],
    ["BLOCKED", false],
    ["IDEMPOTENT_REPLAY", true],
    ["IDEMPOTENCY_CONFLICT", false],
    ["FORCE_ACCEPTED", true],
  ] as const;

  for (const [status, durable] of cases) {
    assert.match(submissionOutcomeI18nKey(status), /^upload\.outcome/);
    assert.equal(submissionStatusIsDurable(status), durable);
  }
});

test("only policy-blocked submissions offer a force retry", () => {
  assert.equal(submissionStatusCanForceRetry("BLOCKED"), true);
  assert.equal(submissionStatusCanForceRetry("IDEMPOTENCY_CONFLICT"), false);
  assert.equal(submissionStatusCanForceRetry("ACCEPTED"), false);
  assert.equal(submissionStatusCanForceRetry(undefined), false);
});

test("compact list duplicate evidence maps to localized action and reason keys", () => {
  assert.equal(duplicateActionI18nKey("BLOCK"), "duplicate.action.block");
  assert.equal(
    duplicateFingerprintKindI18nKey("STRICT_ARTICLE_LAYOUT"),
    "duplicate.match.strict-article-layout",
  );
  assert.equal(duplicateLifecycleI18nKey("PARKED"), "duplicate.lifecycle.parked");
});

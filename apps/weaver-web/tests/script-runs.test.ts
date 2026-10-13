import assert from "node:assert/strict";
import test from "node:test";
import { formatRunDuration, triggerLabel } from "../src/next/data/script-runs.ts";
import { englishTranslate as t } from "./english-translate.ts";

test("a trigger reads in words, whatever it was for", () => {
  assert.equal(triggerLabel(t, "post_processing"), "Post-processing");
  assert.equal(triggerLabel(t, "scan"), "Scan");
  assert.equal(triggerLabel(t, "feed:12"), "Feed");
  assert.equal(triggerLabel(t, "scheduler:4"), "Schedule");
});

test("a queue trigger keeps the event that fired it", () => {
  assert.equal(triggerLabel(t, "queue:NZB_ADDED"), "Queue · NZB_ADDED");
  assert.equal(triggerLabel(t, "queue:FILE_DOWNLOADED"), "Queue · FILE_DOWNLOADED");
  assert.equal(triggerLabel(t, "queue"), "Queue");
  assert.equal(triggerLabel(t, "queue:"), "Queue");
});

test("a trigger the screen does not know is shown as the daemon named it", () => {
  assert.equal(triggerLabel(t, "import:3"), "import:3");
  assert.equal(triggerLabel(t, ""), "");
});

test("a run's duration reads in milliseconds, then seconds, then minutes", () => {
  assert.equal(formatRunDuration(0), "0 ms");
  assert.equal(formatRunDuration(240), "240 ms");
  assert.equal(formatRunDuration(999), "999 ms");
  assert.equal(formatRunDuration(1000), "1.0s");
  assert.equal(formatRunDuration(1500), "1.5s");
  assert.equal(formatRunDuration(59_900), "59.9s");
  assert.equal(formatRunDuration(95_000), "1m 35s");
  assert.equal(formatRunDuration(3_725_000), "1h 2m");
});

import assert from "node:assert/strict";
import test from "node:test";
import { mergeSnapshot, parseLogLine, type LogLine } from "../src/next/data/service-log-buffer.ts";

const stamp = (n: number) => `2026-09-17T16:21:${String(n).padStart(2, "0")}.000Z`;
const raw = (n: number) => `${stamp(n)}  INFO weaver::jobs: line ${n} job_id=${n}`;

function buffer(from: number, to: number, firstId = 0): LogLine[] {
  const lines: LogLine[] = [];
  for (let n = from; n <= to; n += 1) {
    lines.push(parseLogLine(firstId + n - from, raw(n)));
  }
  return lines;
}

test("a tracing line splits into its parts", () => {
  const line = parseLogLine(7, raw(3));
  assert.equal(line.level, "info");
  assert.equal(line.time, "16:21:03.000");
  assert.equal(line.target, "weaver::jobs");
  assert.deepEqual(
    line.kvPairs.map((pair) => [pair.key, pair.value]),
    [["job_id", "3"]],
  );
});

test("the first snapshot fills an empty buffer in order", () => {
  const merged = mergeSnapshot([], [raw(1), raw(2), raw(3)], 0, 100);
  assert.equal(merged.changed, true);
  assert.deepEqual(merged.lines.map((line) => line.id), [0, 1, 2]);
  assert.equal(merged.nextId, 3);
});

test("a snapshot that repeats the buffer changes nothing", () => {
  const held = buffer(1, 5);
  const merged = mergeSnapshot(held, [raw(1), raw(2), raw(3), raw(4), raw(5)], 5, 100);
  assert.equal(merged.changed, false);
  assert.equal(merged.lines, held);
  assert.equal(merged.nextId, 5);
});

test("a re-seed keeps the rows it repeats and numbers only the new lines", () => {
  const held = buffer(1, 3);
  const merged = mergeSnapshot(held, [raw(1), raw(2), raw(3), raw(4), raw(5)], 3, 100);
  assert.equal(merged.changed, true);
  assert.equal(merged.lines[0], held[0]);
  assert.equal(merged.lines[2], held[2]);
  assert.deepEqual(merged.lines.map((line) => line.id), [0, 1, 2, 3, 4]);
});

test("subscription lines that outran the snapshot stay after it", () => {
  const held = buffer(1, 5);
  const merged = mergeSnapshot(held, [raw(1), raw(2), raw(3)], 5, 100);
  assert.equal(merged.changed, false);
  assert.deepEqual(merged.lines.map((line) => line.raw), [1, 2, 3, 4, 5].map(raw));
});

test("older lines arriving under buffered ones are numbered again, in order", () => {
  // The first load: two subscription lines are in before the query answers
  // with the history that precedes them.
  const held = buffer(4, 5);
  const merged = mergeSnapshot(held, [raw(1), raw(2), raw(3), raw(4)], 2, 100);
  assert.deepEqual(merged.lines.map((line) => line.raw), [1, 2, 3, 4, 5].map(raw));
  const ids = merged.lines.map((line) => line.id);
  assert.deepEqual(ids, [...ids].sort((a, b) => a - b));
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(merged.nextId, Math.max(...ids) + 1);
});

test("identical lines are matched one for one", () => {
  const same = "plain line without a stamp";
  const held = [parseLogLine(0, same), parseLogLine(1, same)];
  const merged = mergeSnapshot(held, [same, same, same], 2, 100);
  assert.equal(merged.lines[0], held[0]);
  assert.equal(merged.lines[1], held[1]);
  assert.equal(merged.lines[2]!.id, 2);
});

test("the merged buffer keeps only the newest lines", () => {
  const merged = mergeSnapshot(buffer(1, 3), [1, 2, 3, 4, 5, 6].map(raw), 3, 4);
  assert.deepEqual(merged.lines.map((line) => line.raw), [3, 4, 5, 6].map(raw));
});

test("a buffer older than the whole snapshot stays ahead of it", () => {
  const held = buffer(1, 2);
  const merged = mergeSnapshot(held, [raw(7), raw(8)], 2, 100);
  assert.deepEqual(merged.lines.map((line) => line.raw), [1, 2, 7, 8].map(raw));
  assert.equal(merged.lines[0], held[0]);
});

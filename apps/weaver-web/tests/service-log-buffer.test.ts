// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  mergeSnapshot,
  parseLogLine,
  readLogLine,
  recordJson,
  type LogLine,
} from "../src/next/data/service-log-buffer.ts";

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

const RECORD =
  '{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs",' +
  '"fields":{"message":"job added","job_id":7,"name":"My Show","path":"/data/a","size":18446744073709551615}}';

test("a record reads as the line the text log writes for it", () => {
  const line = readLogLine(4, RECORD);
  assert.equal(
    line.raw,
    '2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: job added job_id=7 name="My Show" path=/data/a size=18446744073709551615',
  );
  assert.equal(line.source, RECORD);
  assert.equal(line.level, "info");
  assert.equal(line.time, "08:10:00.123");
  assert.equal(line.target, "weaver::jobs");
  assert.equal(line.message, 'job added job_id=7 name="My Show" path=/data/a size=18446744073709551615');
  assert.deepEqual(
    line.kvPairs.map((pair) => [pair.key, pair.value, line.message.slice(pair.start, pair.end)]),
    [
      ["job_id", "7", "job_id=7"],
      ["name", '"My Show"', 'name="My Show"'],
      ["path", "/data/a", "path=/data/a"],
      ["size", "18446744073709551615", "size=18446744073709551615"],
    ],
  );
});

test("a record reads the same as its text line did", () => {
  const text = parseLogLine(4, readLogLine(4, RECORD).raw);
  const record = readLogLine(4, RECORD);
  assert.equal(record.time, text.time);
  assert.equal(record.level, text.level);
  assert.equal(record.target, text.target);
  assert.equal(record.message, text.message);
  assert.deepEqual(record.kvPairs, text.kvPairs);
});

test("only a string quoted as one token is written bare", () => {
  const record =
    '{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs",' +
    '"fields":{"message":"two","plain":"\\"plain\\"","name":"\\"a\\" b \\"c\\"","tail":"\\"a\\\\\\""}}';
  assert.equal(
    readLogLine(4, record).raw,
    '2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: two plain="plain" name="\\"a\\" b \\"c\\"" tail="\\"a\\\\\\""',
  );
});

test("a record's spans come before its target", () => {
  const line = readLogLine(
    0,
    '{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"WARN","target":"weaver::http",' +
      '"fields":{"message":"slow","name":"\\"plain\\""},"spans":[{"name":"request","fields":{"id":3}},{"name":"bare"}]}',
  );
  assert.equal(
    line.raw,
    '2026-11-03T08:10:00.123456-07:00  WARN request{id=3}:bare: weaver::http: slow name="plain"',
  );
  assert.equal(line.level, "warn");
  assert.equal(line.target, "weaver::http");
  assert.equal(line.message, 'slow name="plain"');
});

test("a line that is not a record reads as text", () => {
  const line = readLogLine(1, raw(2));
  assert.deepEqual(line, parseLogLine(1, raw(2)));
  assert.equal(readLogLine(2, "{not json").message, "{not json");
});

test("a selected line shows its record as indented JSON, numbers as sent", () => {
  assert.equal(
    recordJson(readLogLine(4, RECORD)),
    [
      "{",
      '  "timestamp": "2026-11-03T08:10:00.123456-07:00",',
      '  "level": "INFO",',
      '  "target": "weaver::jobs",',
      '  "fields": {',
      '    "message": "job added",',
      '    "job_id": 7,',
      '    "name": "My Show",',
      '    "path": "/data/a",',
      '    "size": 18446744073709551615',
      "  }",
      "}",
    ].join("\n"),
  );
  assert.equal(recordJson(readLogLine(1, raw(2))), raw(2));
});

test("a re-seed of records keeps the rows it repeats", () => {
  const record = (n: number) =>
    `{"timestamp":"${stamp(n)}","level":"INFO","target":"weaver::jobs","fields":{"message":"line ${n}"}}`;
  const first = mergeSnapshot([], [record(1), record(2)], 0, 100);
  const again = mergeSnapshot(first.lines, [record(1), record(2), record(3)], first.nextId, 100);
  assert.equal(again.lines[0], first.lines[0]);
  assert.equal(again.lines[1], first.lines[1]);
  assert.deepEqual(again.lines.map((line) => line.message), ["line 1", "line 2", "line 3"]);
});

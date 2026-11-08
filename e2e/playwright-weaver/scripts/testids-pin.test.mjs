// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// The specs locate the web app by its test IDs. This package does not import
// across its boundary, so it keeps a copy of the app's ID modules; this check
// fails the moment the two drift, in either direction.
const mirrorDir = path.resolve(import.meta.dirname, "..", "tests", "support", "testids");
const appDir = path.resolve(
  import.meta.dirname,
  "..", "..", "..", "apps", "weaver-web", "src", "next", "testids",
);

const BEGIN = "// testids:begin\n";
const END = "// testids:end\n";

function pinnedBody(file) {
  const source = fs.readFileSync(file, "utf8");
  const start = source.indexOf(BEGIN);
  const end = source.indexOf(END);
  assert.ok(start >= 0 && end > start, `${file} has no testids:begin/testids:end markers`);
  assert.equal(source.indexOf(BEGIN, start + 1), -1, `${file} has two testids:begin markers`);
  return source.slice(start, end + END.length);
}

function areaFiles(directory) {
  return fs
    .readdirSync(directory)
    .filter((name) => name.endsWith(".ts") && name !== "index.ts")
    .sort();
}

function reExports(directory) {
  return fs
    .readFileSync(path.join(directory, "index.ts"), "utf8")
    .split("\n")
    .filter((line) => line.startsWith("export "))
    .sort();
}

test("the mirror holds the same area files as the web app", () => {
  assert.deepEqual(areaFiles(mirrorDir), areaFiles(appDir));
  assert.ok(areaFiles(appDir).length > 0, "the web app has no test ID modules");
});

test("each area's IDs are identical in the web app and the mirror", () => {
  for (const name of areaFiles(appDir)) {
    assert.equal(
      pinnedBody(path.join(mirrorDir, name)),
      pinnedBody(path.join(appDir, name)),
      `tests/support/testids/${name} drifted from the web app's copy`,
    );
  }
});

test("both index modules re-export the same areas", () => {
  assert.deepEqual(reExports(mirrorDir), reExports(appDir));
});

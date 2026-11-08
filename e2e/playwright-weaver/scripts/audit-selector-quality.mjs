// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import fs from "node:fs";
import path from "node:path";

import { isBrowserOwnedSpec } from "./browser-boundary.mjs";
import {
  auditCopyLocators,
  auditSelectorQuality,
  countCopyLifts,
} from "./selector-quality.mjs";

const testsDir = path.resolve(import.meta.dirname, "..", "tests");

/** Every TypeScript file under tests/, as a path relative to it. */
export function testSources(directory = testsDir, prefix = "") {
  return fs
    .readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) => {
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) return testSources(path.join(directory, entry.name), relative);
      return entry.isFile() && entry.name.endsWith(".ts") ? [relative] : [];
    })
    .sort();
}

/** The copy-is-claim lifts each file keeps, for the files that keep any. */
export function copyLiftCounts() {
  return Object.fromEntries(
    testSources()
      .map((file) => [file, countCopyLifts(fs.readFileSync(path.join(testsDir, file), "utf8"))])
      .filter(([, count]) => count > 0),
  );
}

function main() {
  const browserSpecs = fs
    .readdirSync(testsDir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && isBrowserOwnedSpec(entry.name))
    .map((entry) => entry.name)
    .sort();

  if (browserSpecs.length === 0) {
    process.stderr.write("Weaver selector-quality audit found no ui-*.spec.ts files.\n");
    process.exit(1);
  }

  const sources = testSources();
  const violations = [
    ...browserSpecs.flatMap((fileName) =>
      auditSelectorQuality(fileName, fs.readFileSync(path.join(testsDir, fileName), "utf8")),
    ),
    ...sources.flatMap((fileName) =>
      auditCopyLocators(fileName, fs.readFileSync(path.join(testsDir, fileName), "utf8")),
    ),
  ];

  if (violations.length > 0) {
    process.stderr.write(
      [
        "Weaver selector-quality audit failed.",
        "Browser-owned flows must use accessible selectors or explicit product-owned IDs/test IDs.",
        "XPath, DOM ancestry, positional locators, and unowned CSS structure are prohibited.",
        "No file under tests/ may locate an element by its copy unless the line says copy-is-claim.",
        ...violations,
        "",
      ].join("\n"),
    );
    process.exit(1);
  }

  const lifts = Object.values(copyLiftCounts()).reduce((sum, count) => sum + count, 0);
  process.stdout.write(
    `Weaver selector-quality audit passed (${browserSpecs.length} browser-owned specs, ${sources.length} test sources, ${lifts} copy-is-claim lifts).\n`,
  );
}

if (process.argv[1] === import.meta.filename) main();

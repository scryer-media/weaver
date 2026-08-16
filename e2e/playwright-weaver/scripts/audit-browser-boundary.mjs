import fs from "node:fs";
import path from "node:path";

import {
  auditBrowserSpec,
  isBrowserOwnedSpec,
} from "./browser-boundary.mjs";

const testsDir = path.resolve(import.meta.dirname, "..", "tests");
const browserSpecs = fs
  .readdirSync(testsDir, { withFileTypes: true })
  .filter((entry) => entry.isFile() && isBrowserOwnedSpec(entry.name))
  .map((entry) => entry.name)
  .sort();

if (browserSpecs.length === 0) {
  process.stderr.write(
    "Weaver browser-boundary audit found no ui-*.spec.ts files.\n",
  );
  process.exit(1);
}

const violations = browserSpecs.flatMap((fileName) => {
  const source = fs.readFileSync(path.join(testsDir, fileName), "utf8");
  return auditBrowserSpec(fileName, source);
});

if (violations.length > 0) {
  process.stderr.write(
    [
      "Weaver browser-boundary audit failed.",
      "Browser-owned specs must drive primary user actions and assertions through visible controls.",
      "Move sanctioned setup, external-system control, or runtime introspection behind a narrow helper under tests/support/.",
      ...violations,
      "",
    ].join("\n"),
  );
  process.exit(1);
}

process.stdout.write(
  `Weaver browser-boundary audit passed (${browserSpecs.length} browser-owned specs).\n`,
);

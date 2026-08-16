import fs from "node:fs";
import path from "node:path";

import { isBrowserOwnedSpec } from "./browser-boundary.mjs";
import { auditSelectorQuality } from "./selector-quality.mjs";

const testsDir = path.resolve(import.meta.dirname, "..", "tests");
const browserSpecs = fs
  .readdirSync(testsDir, { withFileTypes: true })
  .filter((entry) => entry.isFile() && isBrowserOwnedSpec(entry.name))
  .map((entry) => entry.name)
  .sort();

if (browserSpecs.length === 0) {
  process.stderr.write("Weaver selector-quality audit found no ui-*.spec.ts files.\n");
  process.exit(1);
}

const violations = browserSpecs.flatMap((fileName) => {
  const source = fs.readFileSync(path.join(testsDir, fileName), "utf8");
  return auditSelectorQuality(fileName, source);
});

if (violations.length > 0) {
  process.stderr.write(
    [
      "Weaver selector-quality audit failed.",
      "Browser-owned flows must use accessible selectors or explicit product-owned IDs/test IDs.",
      "XPath, DOM ancestry, positional locators, and unowned CSS structure are prohibited.",
      ...violations,
      "",
    ].join("\n"),
  );
  process.exit(1);
}

process.stdout.write(
  `Weaver selector-quality audit passed (${browserSpecs.length} browser-owned specs).\n`,
);

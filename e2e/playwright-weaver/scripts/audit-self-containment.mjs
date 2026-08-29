import fs from "node:fs";
import path from "node:path";

// This Playwright project is self-contained: specs, helpers, reporters and
// scripts live under playwright-weaver/ and the Docker build context is this
// directory. Reaching sideways into the Go harness would make the image build
// and the spec sources disagree about what a run actually needs, so any path
// that climbs out into a sibling harness directory fails the audit.
const root = path.resolve(import.meta.dirname, "..");
const forbidden = [
  "../internal/",
  "../cmd/",
  "../services/",
  "../scripts/",
  "../testdata/",
  "../test-corpus/",
];
const ignoredDirectories = new Set(["node_modules", "artifacts", "test-results"]);

function filesUnder(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      return ignoredDirectories.has(entry.name) ? [] : filesUnder(target);
    }
    return [target];
  });
}

const offenders = [];
for (const file of filesUnder(root)) {
  if (file.endsWith("package-lock.json") || file.endsWith("audit-self-containment.mjs")) continue;
  const body = fs.readFileSync(file, "utf8");
  for (const token of forbidden) {
    if (body.includes(token)) offenders.push(`${path.relative(root, file)}: ${token}`);
  }
}

if (offenders.length > 0) {
  process.stderr.write(
    `Weaver Playwright self-containment audit failed:\n${offenders.join("\n")}\n`,
  );
  process.exit(1);
}

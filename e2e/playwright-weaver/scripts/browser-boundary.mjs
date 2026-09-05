const helperImportPattern =
  /import\s*\{([^}]*)\}\s*from\s*["']\.\/helpers["']\s*;?/g;
const importSourcePattern = /from\s*["']([^"']+)["']/g;

// The only names a browser-owned spec may take from ./helpers: the test
// fixtures, the route builder, and openNavigation, which drives the visible
// navigation control (a role lookup and a click) rather than any transport.
const browserHelperExports = new Set([
  "expect",
  "test",
  "weaverRoute",
  "openNavigation",
]);

const sanctionedHelperPrefixes = [
  "./support/setup/",
  "./support/external-control/",
  "./support/runtime-introspection",
];

const directTransportPatterns = [
  {
    label: "direct GraphQL helper call",
    pattern: /\bgraphql\s*(?:<|\()/,
  },
  {
    label: "direct metrics helper call",
    pattern: /\bmetrics\s*\(/,
  },
  {
    label: "direct fetch call",
    pattern: /\bfetch\s*\(/,
  },
  {
    label: "direct Playwright request call",
    pattern:
      /\b(?:request|page\.request|context\.request)\s*\.\s*(?:get|post|put|patch|delete|fetch|head)\s*\(/,
  },
  {
    label: "direct APIRequestContext use",
    pattern: /\bAPIRequestContext\b/,
  },
];

function namedImports(body) {
  return body
    .split(",")
    .map((entry) => entry.trim().replace(/^type\s+/, "").split(/\s+as\s+/)[0])
    .filter(Boolean);
}

function hasSanctionedHelperImport(source) {
  for (const match of source.matchAll(importSourcePattern)) {
    if (sanctionedHelperPrefixes.some((prefix) => match[1].startsWith(prefix))) {
      return true;
    }
  }
  return false;
}

export function auditBrowserSpec(relativePath, source) {
  const violations = [];

  for (const match of source.matchAll(helperImportPattern)) {
    const forbiddenImports = namedImports(match[1]).filter(
      (name) => !browserHelperExports.has(name),
    );
    if (forbiddenImports.length > 0) {
      violations.push(
        `imports non-browser helpers from ./helpers: ${forbiddenImports.join(", ")}`,
      );
    }
  }

  for (const { label, pattern } of directTransportPatterns) {
    if (pattern.test(source)) violations.push(label);
  }

  const requestsApiFixture =
    /async\s*\(\s*\{[^}]*\brequest\b[^}]*\}/s.test(source);
  if (requestsApiFixture && !hasSanctionedHelperImport(source)) {
    violations.push(
      "uses the request fixture without a sanctioned setup, external-control, or runtime-introspection helper",
    );
  }

  return [...new Set(violations)].map(
    (violation) => `${relativePath}: ${violation}`,
  );
}

export function isBrowserOwnedSpec(fileName) {
  return /^ui-.+\.spec\.ts$/.test(fileName);
}

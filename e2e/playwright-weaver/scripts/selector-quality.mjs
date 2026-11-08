// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

const POSITIONAL_LOCATORS = [
  { pattern: /\.first\(\)/g, description: "positional .first() locator" },
  { pattern: /\.last\(\)/g, description: "positional .last() locator" },
  { pattern: /\.nth\(\s*[^)]*\)/g, description: "positional .nth() locator" },
  {
    pattern: /\.(?:all|allTextContents|allInnerTexts)\(\)\s*\)?\s*\[\s*\d+\s*\]/g,
    description: "indexed locator collection",
  },
];

const LOCATOR_CALL = /\.locator\(/g;
const LITERAL_LOCATOR = /^\.locator\(\s*(["'`])(.*?)\1\s*\)/s;
const PRODUCT_ID = /^#[A-Za-z][A-Za-z0-9_-]*$/;

function lineNumberAt(source, index) {
  return source.slice(0, index).split("\n").length;
}

// A locator that finds an element by what it says breaks the moment the
// wording changes. Specs locate by test ID; a copy locator stays only where the
// text itself is what the test certifies, and says so on its line with
// `// copy-is-claim: <reason>`.
const LIFT = /\/\/\s*copy-is-claim:\s*\S/;
const COPY_CALL = /\.(getByRole|getByText|getByLabel|getByTitle|getByPlaceholder|getByAltText)\(/g;

/** The argument text of the call whose "(" is at `open`, and where it ends. */
function callArguments(source, open) {
  let depth = 0;
  let quote = null;
  for (let index = open; index < source.length; index += 1) {
    const char = source[index];
    if (quote !== null) {
      if (char === "\\") index += 1;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") quote = char;
    else if (char === "(") depth += 1;
    else if (char === ")") {
      depth -= 1;
      if (depth === 0) return { args: source.slice(open + 1, index), end: index };
    }
  }
  return { args: source.slice(open + 1), end: source.length };
}

function copyLocator(method, args) {
  switch (method) {
    case "getByRole":
      return /\bname\s*:/.test(args) ? "getByRole located by accessible name" : null;
    case "getByText":
      return /^\s*["'`/]/.test(args) ? "getByText located by literal copy" : null;
    default:
      return `${method} locates by copy`;
  }
}

function copyCalls(source) {
  const calls = [];
  for (const match of source.matchAll(COPY_CALL)) {
    const open = match.index + match[0].length - 1;
    const { args, end } = callArguments(source, open);
    const description = copyLocator(match[1], args);
    if (description === null) continue;
    const lineStart = source.lastIndexOf("\n", match.index) + 1;
    const lineEnd = source.indexOf("\n", end);
    const lines = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    calls.push({ index: match.index, description, lifted: LIFT.test(lines) });
  }
  return calls;
}

/** Copy locators in a file of tests/**: each one unlifted is a violation. */
export function auditCopyLocators(fileName, source) {
  return copyCalls(source)
    .filter((call) => !call.lifted)
    .map(
      (call) =>
        `${fileName}:${lineNumberAt(source, call.index)}: ${call.description}; locate by getByTestId, or mark the line // copy-is-claim: <reason> where the text is the claim`,
    );
}

/** How many copy locators a file keeps under a copy-is-claim lift. */
export function countCopyLifts(source) {
  return copyCalls(source).filter((call) => call.lifted).length;
}

export function auditSelectorQuality(fileName, source) {
  const violations = [];

  for (const { pattern, description } of POSITIONAL_LOCATORS) {
    for (const match of source.matchAll(pattern)) {
      violations.push(
        `${fileName}:${lineNumberAt(source, match.index)}: ${description}; use an accessible name or product-owned ID/test ID`,
      );
    }
  }

  for (const match of source.matchAll(LOCATOR_CALL)) {
    const literal = source.slice(match.index).match(LITERAL_LOCATOR);
    if (!literal) {
      violations.push(
        `${fileName}:${lineNumberAt(source, match.index)}: nonliteral locator; use getByRole/getByLabel/getByTestId or a simple product-owned #id`,
      );
      continue;
    }

    const selector = literal[2].trim();
    if (PRODUCT_ID.test(selector)) continue;

    const description =
      selector === ".." || selector.startsWith("xpath=")
        ? "DOM traversal locator"
        : "CSS locator without a product-owned ID";
    violations.push(
      `${fileName}:${lineNumberAt(source, match.index)}: ${description} ${JSON.stringify(selector)}; use getByRole/getByLabel/getByTestId or a simple product-owned #id`,
    );
  }

  return violations;
}

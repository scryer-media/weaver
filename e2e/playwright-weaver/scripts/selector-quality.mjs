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

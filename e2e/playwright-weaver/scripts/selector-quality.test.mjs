import assert from "node:assert/strict";
import test from "node:test";

import { auditSelectorQuality } from "./selector-quality.mjs";

test("allows accessible selectors, test IDs, and simple product-owned IDs", () => {
  const source = `
    page.getByRole("button", { name: "Save" });
    page.getByLabel("Server name");
    page.getByTestId("server-row-primary");
    page.locator("#pp-concurrency");
  `;

  assert.deepEqual(auditSelectorQuality("ui-good.spec.ts", source), []);
});

test("rejects DOM traversal and structural CSS locators", () => {
  const source = `
    page.getByText("Server").locator("..");
    page.locator("xpath=ancestor::div[1]");
    page.locator('input[type="file"]');
    page.locator("nav a[href]:visible");
    page.locator(\`section > button\`);
    page.locator(selectorFromRuntime);
    page.locator("#server-row.active");
    page.locator("#submit:disabled");
  `;

  const violations = auditSelectorQuality("ui-bad.spec.ts", source);
  assert.equal(violations.length, 8);
  assert.match(violations[0], /DOM traversal locator/);
  assert.match(violations[2], /CSS locator without a product-owned ID/);
  assert.match(violations[5], /nonliteral locator/);
  assert.match(violations[6], /CSS locator without a product-owned ID/);
});

test("rejects positional locator ownership", () => {
  const source = `
    rows.first();
    rows.last();
    rows.nth(2);
    (await rows.allTextContents())[0];
  `;

  const violations = auditSelectorQuality("ui-positional.spec.ts", source);
  assert.equal(violations.length, 4);
  assert.match(violations[0], /\.first/);
  assert.match(violations[1], /\.last/);
  assert.match(violations[2], /\.nth/);
  assert.match(violations[3], /indexed locator collection/);
});

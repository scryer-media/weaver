// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";

import { copyLiftCounts } from "./audit-selector-quality.mjs";
import { auditCopyLocators, auditSelectorQuality, countCopyLifts } from "./selector-quality.mjs";

test("the structural audit allows roles, labels, test IDs, and simple product-owned IDs", () => {
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

test("rejects locating an element by its copy", () => {
  const source = `
    page.getByRole("button", { name: "Save" });
    page.getByRole("switch", {
      name: "SSL/TLS",
    });
    page.getByText("Connected");
    page.getByText(/^Reconnecting/);
    page.getByLabel("Server name");
    page.getByTitle(info.filename);
    page.getByPlaceholder("Search");
    page.getByAltText("Logo");
  `;

  const violations = auditCopyLocators("tests/any.spec.ts", source);
  assert.equal(violations.length, 8);
  assert.match(violations[0], /^tests\/any\.spec\.ts:2: getByRole located by accessible name/);
  assert.match(violations[1], /:3: getByRole located by accessible name/);
  assert.match(violations[2], /getByText located by literal copy/);
  assert.match(violations[3], /getByText located by literal copy/);
  assert.match(violations[4], /getByLabel locates by copy/);
  assert.match(violations[5], /getByTitle locates by copy/);
  assert.match(violations[6], /getByPlaceholder locates by copy/);
  assert.match(violations[7], /getByAltText locates by copy/);
});

test("allows test IDs, nameless roles and record data", () => {
  const source = `
    page.getByTestId("provider-tls-switch");
    page.getByRole("main");
    navigation.getByRole("link");
    rows.filter({ has: page.getByText(feedUrl, { exact: true }) });
  `;

  assert.deepEqual(auditCopyLocators("tests/good.spec.ts", source), []);
  assert.equal(countCopyLifts(source), 0);
});

test("a copy-is-claim lift on the call's lines keeps a copy locator, and is counted", () => {
  const source = `
    status.getByText("3 queued"); // copy-is-claim: the count is the behavior
    page.getByRole("alert", {
      name: "Invalid credentials", // copy-is-claim: the error text is the claim
    });
    page.getByText("Ready"); // copy-is-claim:
  `;

  const violations = auditCopyLocators("tests/lifted.spec.ts", source);
  assert.equal(violations.length, 1, "a lift with no reason lifts nothing");
  assert.match(violations[0], /:6: getByText located by literal copy/);
  assert.equal(countCopyLifts(source), 2);
});

// Widening the copy locators a file keeps is a reviewed change: this map is
// every lift under tests/, per file, and it moves only with a spec change.
test("the copy-is-claim lifts under tests/ are the reviewed set", () => {
  assert.deepEqual(copyLiftCounts(), {});
});

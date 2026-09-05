import assert from "node:assert/strict";
import test from "node:test";

import {
  auditBrowserSpec,
  isBrowserOwnedSpec,
} from "./browser-boundary.mjs";

test("identifies browser-owned specs by their ui prefix", () => {
  assert.equal(isBrowserOwnedSpec("ui-settings-crud.spec.ts"), true);
  assert.equal(isBrowserOwnedSpec("rate-limits.spec.ts"), false);
});

test("rejects direct GraphQL helpers and API request calls", () => {
  const source = `
    import { expect, graphql, test } from "./helpers";
    test("shortcut", async ({ request }) => {
      await graphql(request, "mutation { updateSettings }");
      await request.post("/graphql");
    });
  `;
  const violations = auditBrowserSpec("ui-shortcut.spec.ts", source);
  assert.ok(violations.some((entry) => entry.includes("non-browser helpers")));
  assert.ok(violations.some((entry) => entry.includes("GraphQL helper")));
  assert.ok(violations.some((entry) => entry.includes("request call")));
});

test("rejects direct REST and fetch shortcuts", () => {
  const source = `
    import { expect, test } from "./helpers";
    test("shortcut", async () => {
      await fetch("/api/settings");
    });
  `;
  const violations = auditBrowserSpec("ui-shortcut.spec.ts", source);
  assert.ok(violations.some((entry) => entry.includes("fetch call")));
});

test("allows browser actions and visible assertions", () => {
  const source = `
    import { expect, test, weaverRoute } from "./helpers";
    test("browser behavior", async ({ cleanPage: page }) => {
      await page.goto(weaverRoute("/settings/general"));
      await page.getByRole("button", { name: "Save" }).click();
      await expect(page.getByText("Saved")).toBeVisible();
    });
  `;
  assert.deepEqual(auditBrowserSpec("ui-settings.spec.ts", source), []);
});

test("allows the visible-navigation helper beside the fixtures", () => {
  const source = `
    import { expect, openNavigation, test, weaverRoute } from "./helpers";
    test("browser behavior", async ({ cleanPage: page }) => {
      await page.goto(weaverRoute("/"));
      const navigation = await openNavigation(page);
      await navigation.getByRole("link", { name: "Settings" }).click();
      await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();
    });
  `;
  assert.deepEqual(auditBrowserSpec("ui-settings.spec.ts", source), []);
});

test("allows request only through a sanctioned narrow helper", () => {
  const source = `
    import { expect, test } from "./helpers";
    import { introspectPublicMutationNames } from "./support/runtime-introspection";
    test("coverage contract", async ({ request }) => {
      const names = await introspectPublicMutationNames(request);
      expect(names.length).toBeGreaterThan(0);
    });
  `;
  assert.deepEqual(
    auditBrowserSpec("ui-runtime-observability.spec.ts", source),
    [],
  );
});

test("a sanctioned import does not permit direct request calls", () => {
  const source = `
    import { expect, test } from "./helpers";
    import { prepareFixture } from "./support/setup/fixture";
    test("shortcut", async ({ request }) => {
      await prepareFixture(request);
      await request.post("/graphql");
    });
  `;
  const violations = auditBrowserSpec("ui-settings.spec.ts", source);
  assert.ok(violations.some((entry) => entry.includes("request call")));
});

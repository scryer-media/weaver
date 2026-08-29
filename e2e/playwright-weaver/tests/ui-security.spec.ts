import type { Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectHttpErrors } from "./support/http-errors";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const username = "e2e-admin";
const initialPassword = "release-password";
const changedPassword = "release-password-changed";

async function signIn(page: Page, password: string): Promise<void> {
  await page.locator("#username").fill(username);
  await page.locator("#password").fill(password);
  const loginResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/login"
    && response.request().method() === "POST"
  );
  await page.getByRole("button", { name: "Sign In" }).click();
  expect((await loginResponse).status()).toBe(200);
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign In" })).toHaveCount(0);
}

async function expectRejectedSignIn(page: Page, password: string): Promise<void> {
  await page.locator("#username").fill(username);
  await page.locator("#password").fill(password);
  expectHttpErrors(page, {
    method: "POST",
    pathname: "/api/login",
    status: 401,
  });
  const loginResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/login"
    && response.request().method() === "POST"
  );
  await page.getByRole("button", { name: "Sign In" }).click();
  expect((await loginResponse).status()).toBe(401);
  await expect(page.locator("#error")).toContainText("invalid credentials");
}

test.describe.serial("security product behavior", () => {
  test("API keys are created once, shown once, persisted, and revoked through controls", async ({ cleanPage: page }) => {
    if (afterRestart) {
      await page.goto("/");
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      await expect(page.getByText("e2e-release-key", { exact: true })).toBeVisible();
      const persistedKeyRow = page.getByTestId("api-key-row").filter({
        has: page.getByText("e2e-release-key", { exact: true }),
      });
      await persistedKeyRow.getByRole("button", { name: "Delete" }).click();
      await page.getByRole("dialog").getByRole("button", { name: "Delete Key" }).click();
      await expect(page.getByText("e2e-release-key", { exact: true })).toHaveCount(0);
      return;
    }

    await page.goto("/settings/security");
    await page.getByTestId("api-key-name").fill("e2e-release-key");
    await page.getByLabel("Scope", { exact: true }).click();
    await page.getByRole("option", { name: "Read" }).click();
    await page.getByRole("button", { name: "Create API Key" }).click();

    const createdDialog = page.getByRole("dialog", { name: "API Key Created" });
    await expect(createdDialog).toContainText("e2e-release-key");
    await expect(createdDialog).toContainText("Read");
    const rawKey = await createdDialog.getByTestId("raw-api-key").inputValue();
    expect(rawKey.length).toBeGreaterThan(20);
    await createdDialog.press("Escape");
    await page.reload();
    await expect(page.getByText("e2e-release-key", { exact: true })).toBeVisible();
    await expect(page.getByRole("main")).not.toContainText(rawKey);
  });

  test("login enable, invalid login, logout, password change, and disable are browser-owned", async ({ cleanPage: page }) => {
    if (afterRestart) {
      await page.goto("/");
      await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
      await expectRejectedSignIn(page, initialPassword);
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      await expect(page.getByText(`Enabled — signed in as ${username}`)).toBeVisible();
      await page.reload();
      await expect(page.getByText(`Enabled — signed in as ${username}`)).toBeVisible();
      await page.getByRole("button", { name: "Sign Out" }).click();
      await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      expectHttpErrors(page, {
        method: "POST",
        pathname: "/graphql",
        status: 401,
        count: 1,
      });
      await page.getByRole("button", { name: "Disable Login" }).click();
      await expect(page.getByText("Login protection disabled")).toBeVisible();
      await page.goto("/");
      await expect(page.getByRole("button", { name: "Sign In" })).toHaveCount(0);
      await expect(page.getByRole("main")).toBeVisible();
      return;
    }

    await page.goto("/settings/security");
    await page.locator("#login-username").fill(username);
    await page.locator("#login-password").fill(initialPassword);
    await page.locator("#login-confirm").fill("different-password");
    await page.getByRole("button", { name: "Enable Login" }).click();
    await expect(page.getByText("Passwords do not match")).toBeVisible();
    await page.locator("#login-confirm").fill(initialPassword);
    // Enabling login rejects any in-flight browser queries. The number is
    // scheduling-dependent; the explicit rejected-login checks below cover
    // the credential contract.
    expectHttpErrors(page, {
      method: "POST",
      pathname: "/graphql",
      status: 401,
      count: 1,
      maxCount: 8,
    });
    await page.getByRole("button", { name: "Enable Login" }).click();
    await expect(page.getByText("Login protection enabled")).toBeVisible();

    await page.goto("/");
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
    await expectRejectedSignIn(page, "not-the-password");
    await signIn(page, initialPassword);

    await page.goto("/settings/security");
    await expect(page.getByText(`Enabled — signed in as ${username}`)).toBeVisible();
    await page.reload();
    await expect(page.getByText(`Enabled — signed in as ${username}`)).toBeVisible();
    await page.getByRole("button", { name: "Sign Out" }).click();
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();

    await signIn(page, initialPassword);
    await page.goto("/settings/security");
    await page.locator("#current-password").fill(initialPassword);
    await page.locator("#new-password").fill(changedPassword);
    await page.locator("#confirm-new-password").fill(changedPassword);
    await page.getByRole("button", { name: "Change Password" }).click();
    await expect(page.getByText(/password changed/i)).toBeVisible();

    await page.goto("/");
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
    await expectRejectedSignIn(page, initialPassword);
    await signIn(page, changedPassword);
    await page.goto("/settings/security");
    await expect(page.getByText(`Enabled — signed in as ${username}`)).toBeVisible();
  });
});

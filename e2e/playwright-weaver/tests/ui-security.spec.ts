import type { Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectHttpErrors } from "./support/http-errors";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const username = "e2e-admin";
const initialPassword = "release-password";
const changedPassword = "release-password-changed";

const signInButton = (page: Page) => page.getByRole("button", { name: "Sign in", exact: true });

async function signIn(page: Page, password: string): Promise<void> {
  await page.locator("#username").fill(username);
  await page.locator("#password").fill(password);
  const loginResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/login"
    && response.request().method() === "POST"
  );
  await signInButton(page).click();
  expect((await loginResponse).status()).toBe(200);
  // A successful sign-in reloads into the interface.
  await expect(page.getByRole("main")).toBeVisible();
  await expect(signInButton(page)).toHaveCount(0);
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
  await signInButton(page).click();
  expect((await loginResponse).status()).toBe(401);
  await expect(page.locator("#error")).toContainText(/invalid credentials/i);
}

async function expectSignedInAs(page: Page): Promise<void> {
  await expect(
    page.getByRole("region", { name: "Sign-in", exact: true }).getByText(`signed in as ${username}`, { exact: true }),
  ).toBeVisible();
}

async function signOut(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(signInButton(page)).toBeVisible();
}

test.describe.serial("security product behavior", () => {
  test("API keys are created once, shown once, persisted, and revoked through controls", async ({ cleanPage: page }) => {
    const keys = page.getByRole("region", { name: "API keys", exact: true });
    if (afterRestart) {
      await page.goto("/");
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      await expect(keys.getByText("e2e-release-key", { exact: true })).toBeVisible();
      const revoke = keys.getByRole("button", { name: "Revoke", exact: true });
      await expect(revoke).toHaveCount(1);
      await revoke.click();
      const confirm = page.getByRole("dialog", { name: "Revoke API key", exact: true });
      await expect(confirm).toContainText("e2e-release-key");
      await confirm.getByRole("button", { name: "Revoke key", exact: true }).click();
      await expect(confirm).toBeHidden();
      await expect(keys.getByText("e2e-release-key", { exact: true })).toHaveCount(0);
      return;
    }

    await page.goto("/settings/security");
    await page.getByRole("banner").getByRole("button", { name: "Add API key", exact: true }).click();
    const editor = page.getByRole("dialog", { name: "New API key", exact: true });
    await editor.getByRole("textbox", { name: "Name", exact: true }).fill("e2e-release-key");
    await editor.getByRole("button", { name: "Scope", exact: true }).click();
    await page
      .getByRole("menu", { name: "Scope", exact: true })
      .getByRole("menuitemradio", { name: "Read only", exact: true })
      .click();
    await expect(editor.getByRole("button", { name: "Scope", exact: true })).toHaveText("Read only");
    await editor.getByRole("button", { name: "Create key", exact: true }).click();

    const createdDialog = page.getByRole("dialog", { name: "API key created", exact: true });
    await expect(createdDialog).toContainText("e2e-release-key");
    const rawKey = await createdDialog.getByRole("textbox", { name: "API key", exact: true }).inputValue();
    expect(rawKey.length).toBeGreaterThan(20);
    await createdDialog.getByRole("button", { name: "Done", exact: true }).click();
    await expect(createdDialog).toBeHidden();
    await page.reload();
    await expect(keys.getByText("e2e-release-key", { exact: true })).toBeVisible();
    await expect(keys.getByText("Read only", { exact: true })).toBeVisible();
    await expect(page.getByRole("main")).not.toContainText(rawKey);
  });

  test("login enable, invalid login, logout, password change, and disable are browser-owned", async ({ cleanPage: page }) => {
    if (afterRestart) {
      await page.goto("/");
      await expect(signInButton(page)).toBeVisible();
      await expectRejectedSignIn(page, initialPassword);
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      await expectSignedInAs(page);
      await page.reload();
      await expectSignedInAs(page);
      await signOut(page);
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      expectHttpErrors(page, {
        method: "POST",
        pathname: "/graphql",
        status: 401,
        count: 0,
        maxCount: 8,
      });
      await page.getByRole("button", { name: "Turn off", exact: true }).click();
      const confirm = page.getByRole("dialog", { name: "Turn off the login", exact: true });
      await confirm.getByRole("button", { name: "Turn off login", exact: true }).click();
      await expect(confirm).toBeHidden();
      await expect(
        page.getByRole("region", { name: "Sign-in", exact: true }).getByText("not configured", { exact: true }),
      ).toBeVisible();
      await expect(page.getByRole("button", { name: "Set up a login", exact: true })).toBeVisible();
      await page.goto("/");
      await expect(page.getByRole("main")).toBeVisible();
      await expect(signInButton(page)).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Sign out", exact: true })).toHaveCount(0);
      return;
    }

    await page.goto("/settings/security");
    await page.getByRole("button", { name: "Set up a login", exact: true }).click();
    const setup = page.getByRole("dialog", { name: "Set up a login", exact: true });
    await setup.getByLabel("Username", { exact: true }).fill(username);
    await setup.getByLabel("Password", { exact: true }).fill(initialPassword);
    await setup.getByLabel("Repeat the password", { exact: true }).fill("different-password");
    await setup.getByRole("button", { name: "Turn on login", exact: true }).click();
    await expect(setup.getByText("The two passwords do not match.", { exact: true })).toBeVisible();
    await setup.getByLabel("Repeat the password", { exact: true }).fill(initialPassword);
    // Turning the login on refuses any in-flight browser queries. The number
    // is scheduling-dependent; the explicit rejected sign-in checks below
    // cover the credential contract.
    expectHttpErrors(page, {
      method: "POST",
      pathname: "/graphql",
      status: 401,
      count: 1,
      maxCount: 8,
    });
    await setup.getByRole("button", { name: "Turn on login", exact: true }).click();
    // The first query refused after the switch takes this very tab to the
    // sign-in page.
    await expect(signInButton(page)).toBeVisible();

    await page.goto("/");
    await expect(signInButton(page)).toBeVisible();
    await expectRejectedSignIn(page, "not-the-password");
    await signIn(page, initialPassword);

    await page.goto("/settings/security");
    await expectSignedInAs(page);
    await page.reload();
    await expectSignedInAs(page);
    await signOut(page);

    await signIn(page, initialPassword);
    await page.goto("/settings/security");
    await page.getByRole("button", { name: "Change password", exact: true }).click();
    const change = page.getByRole("dialog", { name: "Change password", exact: true });
    await change.getByLabel("Current password", { exact: true }).fill(initialPassword);
    await change.getByLabel("New password", { exact: true }).fill(changedPassword);
    await change.getByLabel("Repeat the new password", { exact: true }).fill(changedPassword);
    // A new password ends every session, this tab's included, so its polling
    // queries still in flight are refused too; how many is scheduling noise.
    expectHttpErrors(page, {
      method: "POST",
      pathname: "/graphql",
      status: 401,
      count: 0,
      maxCount: 8,
    });
    await change.getByRole("button", { name: "Change password", exact: true }).click();
    await expect(change).toBeHidden();

    await page.goto("/");
    await expect(signInButton(page)).toBeVisible();
    await expectRejectedSignIn(page, initialPassword);
    await signIn(page, changedPassword);
    await page.goto("/settings/security");
    await expectSignedInAs(page);
  });
});

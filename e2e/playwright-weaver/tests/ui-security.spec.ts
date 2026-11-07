// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { expectHttpErrors } from "./support/http-errors";
import { tableRow } from "./support/settings-locators";
import { settingsIds, testIds } from "./support/testids";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const username = "e2e-admin";
const initialPassword = "release-password";
const changedPassword = "release-password-changed";

const keyName = "e2e-release-key";

const signInButton = (page: Page) => page.getByTestId(settingsIds.loginSubmit);

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
  // The 401 above is the refusal; the form shows it as an error.
  await expect(page.locator("#error")).toBeVisible();
}

async function expectSignedInAs(page: Page): Promise<void> {
  await expect(page.getByTestId(testIds.sectionNote("sign-in"))).toContainText(username);
}

async function signOut(page: Page): Promise<void> {
  await page.getByTestId(testIds.shellSignOut).click();
  await expect(signInButton(page)).toBeVisible();
}

test.describe.serial("security product behavior", () => {
  test("API keys are created once, shown once, persisted, and revoked through controls", async ({ cleanPage: page }) => {
    const keys = page.getByTestId(testIds.section("keys"));
    const keyRow = tableRow(page, "keys", keyName, keys);
    if (afterRestart) {
      await page.goto("/");
      await signIn(page, changedPassword);
      await page.goto("/settings/security");
      await expect(keyRow).toBeVisible();
      const revoke = keys.getByTestId(settingsIds.keyRevoke);
      await expect(revoke).toHaveCount(1);
      await revoke.click();
      const confirm = page.getByTestId(testIds.dialog("key-revoke"));
      await expect(confirm).toContainText(keyName);
      await confirm.getByTestId(testIds.confirmAccept).click();
      await expect(confirm).toBeHidden();
      await expect(keyRow).toHaveCount(0);
      return;
    }

    await page.goto("/settings/security");
    await page.getByTestId(testIds.tableAdd("keys")).click();
    const editor = page.getByTestId(testIds.dialog("key-new"));
    await editor.getByTestId(testIds.field("name")).fill(keyName);
    await editor.getByTestId(testIds.field("scope")).click();
    await page
      .getByTestId(testIds.menu(testIds.field("scope")))
      .getByTestId(testIds.option(testIds.field("scope"), "READ"))
      .click();
    await editor.getByTestId(testIds.editorSave).click();

    const createdDialog = page.getByTestId(testIds.dialog("key-created"));
    await expect(createdDialog).toContainText(keyName);
    const rawKey = await createdDialog.getByTestId(settingsIds.keyCreatedValue).inputValue();
    expect(rawKey.length).toBeGreaterThan(20);
    await createdDialog.getByTestId(settingsIds.keyCreatedDone).click();
    await expect(createdDialog).toBeHidden();
    await page.reload();
    await expect(keyRow).toBeVisible();
    // The scope the key was created with is the read-only one.
    await expect(keyRow.getByTestId(settingsIds.keyScope("READ"))).toBeVisible();
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
      await page.getByTestId(settingsIds.loginOff).click();
      const confirm = page.getByTestId(testIds.dialog("login-off"));
      await confirm.getByTestId(testIds.confirmAccept).click();
      await expect(confirm).toBeHidden();
      // With the login off, Sign-in no longer names an account and offers to set one up.
      await expect(page.getByTestId(testIds.sectionNote("sign-in"))).not.toContainText(username);
      await expect(page.getByTestId(settingsIds.loginSetup)).toBeVisible();
      await page.goto("/");
      await expect(page.getByRole("main")).toBeVisible();
      await expect(signInButton(page)).toHaveCount(0);
      await expect(page.getByTestId(testIds.shellSignOut)).toHaveCount(0);
      return;
    }

    await page.goto("/settings/security");
    await page.getByTestId(settingsIds.loginSetup).click();
    const setup = page.getByTestId(testIds.dialog("login-setup"));
    await setup.getByTestId(testIds.field("username")).fill(username);
    await setup.getByTestId(testIds.field("password")).fill(initialPassword);
    await setup.getByTestId(testIds.field("confirm")).fill("different-password");
    await setup.getByTestId(testIds.editorSave).click();
    // Two passwords that differ are refused in the dialog, which stays open.
    await expect(setup.getByTestId(testIds.editorError)).toBeVisible();
    await setup.getByTestId(testIds.field("confirm")).fill(initialPassword);
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
    await setup.getByTestId(testIds.editorSave).click();
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
    await page.getByTestId(settingsIds.passwordChange).click();
    const change = page.getByTestId(testIds.dialog("password-change"));
    await change.getByTestId(testIds.field("current")).fill(initialPassword);
    await change.getByTestId(testIds.field("next")).fill(changedPassword);
    await change.getByTestId(testIds.field("confirm")).fill(changedPassword);
    // A new password ends every session, this tab's included, so its polling
    // queries still in flight are refused too; how many is scheduling noise.
    expectHttpErrors(page, {
      method: "POST",
      pathname: "/graphql",
      status: 401,
      count: 0,
      maxCount: 8,
    });
    await change.getByTestId(testIds.editorSave).click();
    await expect(change).toBeHidden();

    await page.goto("/");
    await expect(signInButton(page)).toBeVisible();
    await expectRejectedSignIn(page, initialPassword);
    await signIn(page, changedPassword);
    await page.goto("/settings/security");
    await expectSignedInAs(page);
  });
});

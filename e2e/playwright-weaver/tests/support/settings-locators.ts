// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { expect, type Locator, type Page } from "@playwright/test";
import { settingsIds, testIds } from "./testids";

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Every element whose test ID starts with `prefix`. */
export function byTestIdPrefix(scope: Page | Locator, prefix: string): Locator {
  return scope.getByTestId(new RegExp(`^${escapeRegExp(prefix)}`));
}

/** The rows of one settings table, by the table's block id. */
export function tableRows(scope: Page | Locator, blockId: string): Locator {
  return byTestIdPrefix(scope, testIds.rowPrefix(blockId));
}

/**
 * A settings table row, found by the exact text of one of its cells. The text
 * is the record's own data (a name, a host, a pattern), never the app's copy.
 */
export function tableRow(page: Page, blockId: string, cellText: string, scope: Page | Locator = page): Locator {
  return tableRows(scope, blockId).filter({ has: page.getByText(cellText, { exact: true }) });
}

/** Commit a panel's draft through the settings top bar and wait for it to land. */
export async function saveSettings(page: Page): Promise<void> {
  await page.getByTestId(settingsIds.save).click();
  await expect(page.getByTestId(settingsIds.saved)).toBeDisabled();
}

/** Pick `value` from the Select whose test ID is `controlId`, inside `scope`. */
export async function choose(page: Page, scope: Page | Locator, controlId: string, value: string): Promise<void> {
  await scope.getByTestId(controlId).click();
  await page.getByTestId(testIds.menu(controlId)).getByTestId(testIds.option(controlId, value)).click();
}

/** Accept the confirmation dialog `dialogId` and wait for it to close. */
export async function confirmDialog(page: Page, dialogId: string): Promise<void> {
  const confirm = page.getByTestId(testIds.dialog(dialogId));
  await confirm.getByTestId(testIds.confirmAccept).click();
  await expect(confirm).toBeHidden();
}

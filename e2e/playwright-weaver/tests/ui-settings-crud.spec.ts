// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { operationResponse } from "./support/graphql-operations";
import { introspectHardwareProfile } from "./support/runtime-introspection";
import { byTestIdPrefix, choose, saveSettings, tableRow } from "./support/settings-locators";
import { settingsIds, testIds } from "./support/testids";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const persistedCategory = "e2e-product-category-persisted";
const persistedSchedule = "e2e-off-peak-persisted";

const field = testIds.field;
const systemEgress = "System";
const fixtureServer = "nntp";

/** A schedule row in the group of its track. */
function scheduleRow(page: Page, track: string, label: string): Locator {
  return tableRow(page, "schedules", label, page.getByTestId(testIds.rowGroup("schedules", track)));
}

/** A General field saves itself: do `action` and wait for its own mutation to answer without errors. */
async function savedBy(page: Page, mutation: string, action: () => Promise<void>) {
  const saved = operationResponse(page, mutation);
  await action();
  const response = await saved;
  expect(response.ok()).toBeTruthy();
  const payload = await response.json();
  expect(payload.errors ?? [], JSON.stringify(payload.errors ?? [])).toEqual([]);
}

async function confirmRemoval(page: Page, dialogId: string) {
  const confirm = page.getByTestId(testIds.dialog(dialogId));
  await confirm.getByTestId(testIds.confirmAccept).click();
  await expect(confirm).toBeHidden();
}

/** Open the System egress's editor from the egress table. */
async function openSystemEgress(page: Page): Promise<Locator> {
  await page.goto("/settings/networking/egress");
  await tableRow(page, "egress", systemEgress).click();
  const editor = page.getByTestId(testIds.dialog("egress"));
  await expect(editor).toContainText(systemEgress);
  await expect(editor).toBeVisible();
  return editor;
}

test("general SRRDB lookup, bandwidth ceiling and an egress download quota persist", async ({ cleanPage: page }) => {
  await page.goto("/settings/general");
  const srrdbLookup = page.getByTestId(field("srrdb"));
  await expect(srrdbLookup).toBeVisible();
  if (afterRestart) {
    await expect(srrdbLookup).toBeChecked();
    await page.goto("/settings/networking/egress");
    await expect(page.getByTestId(field("maxDownloadSpeed"))).toHaveValue("8");
    const editor = await openSystemEgress(page);
    await expect(editor.getByTestId(field("quotaEnabled"))).toBeChecked();
    await expect(editor.getByTestId(field("quotaMonthDay"))).toHaveValue("17");
    await editor.getByTestId(testIds.editorCancel).click();
    await expect(editor).toBeHidden();
    return;
  }

  await expect(srrdbLookup).not.toBeChecked();
  // The switch saves as it changes; the top bar has nothing to save.
  await savedBy(page, "UpdateSettings", () => srrdbLookup.click());
  await expect(page.getByTestId(settingsIds.saved)).toBeDisabled();
  await page.reload();
  await expect(srrdbLookup).toBeChecked();

  await page.goto("/settings/networking/egress");
  const ceiling = page.getByTestId(field("maxDownloadSpeed"));
  await ceiling.fill("8");
  await ceiling.press("Tab");
  await saveSettings(page);
  await page.reload();
  await expect(ceiling).toHaveValue("8");

  // The data cap is a download quota on an egress.
  let editor = await openSystemEgress(page);
  await editor.getByTestId(field("quotaEnabled")).click();
  await choose(page, editor, field("quotaPeriod"), "MONTHLY");
  await editor.getByTestId(field("quotaLimit")).fill("500");
  const monthlyDay = editor.getByTestId(field("quotaMonthDay"));
  // The day of month clamps to the calendar instead of refusing the draft.
  await monthlyDay.fill("32");
  await monthlyDay.press("Tab");
  await expect(monthlyDay).toHaveValue("31");
  await monthlyDay.fill("17");
  await monthlyDay.press("Tab");
  await editor.getByTestId(testIds.editorSave).click();
  await expect(editor).toBeHidden();
  await page.reload();
  editor = await openSystemEgress(page);
  await expect(editor.getByTestId(field("quotaEnabled"))).toBeChecked();
  await expect(editor.getByTestId(field("quotaMonthDay"))).toHaveValue("17");
  await editor.getByTestId(testIds.editorCancel).click();
  await expect(editor).toBeHidden();
});

test("the unacceptable extension list saves under General downloads, normalized", async ({ cleanPage: page }) => {
  test.skip(afterRestart, "the list is put back before restart");
  await page.goto("/settings/general");
  const extensions = page
    .getByTestId(testIds.section("downloads"))
    .getByTestId(field("unacceptableExtensions"));
  await expect(extensions).toBeVisible();
  const original = await extensions.inputValue();
  try {
    // The list saves when Enter commits it.
    await extensions.fill("EXE, r??");
    await savedBy(page, "SetUnacceptableExtensions", () => extensions.press("Enter"));
    await page.reload();
    await expect(extensions).toHaveValue("exe, r??");
  } finally {
    // A commit that changes nothing sends nothing, so only a changed list is put back.
    await page.reload();
    if ((await extensions.inputValue()) !== original) {
      await extensions.fill(original);
      await savedBy(page, "SetUnacceptableExtensions", () => extensions.press("Enter"));
      await page.reload();
    }
    await expect(extensions).toHaveValue(original);
  }
});

test("archive passwords and the password file save, clear and persist without echoing the list", async ({ cleanPage: page }) => {
  await page.goto("/settings/general");
  const section = page.getByTestId(testIds.section("archivePasswords"));
  const list = section.getByTestId(settingsIds.archivePasswordList);
  const file = section.getByTestId(field("archivePasswordFile"));
  const clear = section.getByTestId(settingsIds.archivePasswordClear);
  // The list stays behind Edit passwords until asked for, so a shared screen never shows it.
  const edit = section.getByTestId(settingsIds.archivePasswordEdit);
  const passwordFile = "/data/e2e-archive-passwords.txt";
  if (afterRestart) {
    // The daemon reports only that a list is stored; the list never comes back.
    await expect(clear).toBeVisible();
    await expect(list).toHaveCount(0);
    await edit.click();
    await expect(list).toHaveValue("");
    await expect(file).toHaveValue(passwordFile);
    return;
  }

  await expect(clear).toHaveCount(0);
  await expect(list).toHaveCount(0);
  await edit.click();
  writeFileSync("/weaver-data/e2e-archive-passwords.txt", "fixture-password\n", { mode: 0o644 });
  await list.fill("first-secret\nsecond-secret");
  // Leaving the box stores the list, and the box shuts.
  await savedBy(page, "UpdateArchivePasswordSettings", () => list.blur());
  await expect(list).toHaveCount(0);
  await expect(clear).toBeVisible();
  // A file picked through Browse is stored as it is picked.
  await file.click();
  const picker = page.getByTestId(testIds.dialog("picker"));
  await expect(picker).toBeVisible();
  await picker.getByTestId(testIds.pickerEntry("e2e-archive-passwords.txt")).click();
  await savedBy(page, "UpdateArchivePasswordSettings", () =>
    picker.getByTestId(testIds.pickerUse).click(),
  );
  await expect(picker).toBeHidden();
  await expect(file).toHaveValue(passwordFile);
  await page.reload();
  await expect(clear).toBeVisible();
  await expect(list).toHaveCount(0);
  await edit.click();
  await expect(list).toHaveValue("");
  await expect(file).toHaveValue(passwordFile);

  // Clearing asks first, then removes the stored list and keeps the file.
  await clear.click();
  const confirm = page.getByTestId(testIds.dialog("archive-passwords-clear"));
  await savedBy(page, "UpdateArchivePasswordSettings", () =>
    confirm.getByTestId(testIds.confirmAccept).click(),
  );
  await expect(confirm).toHaveCount(0);
  await expect(clear).toHaveCount(0);
  await page.reload();
  await expect(clear).toHaveCount(0);
  await expect(file).toHaveValue(passwordFile);

  // Leave a list stored for the restart stage.
  await edit.click();
  await list.fill("third-secret");
  await savedBy(page, "UpdateArchivePasswordSettings", () => list.blur());
  await page.reload();
  await expect(clear).toBeVisible();
});

test("provider edits keep the stored password masked and a disabled provider can be added and removed", async ({ cleanPage: page }) => {
  await page.goto("/settings/servers");
  const serverRow = tableRow(page, "servers", fixtureServer);
  const editor = page.getByTestId(testIds.dialog("provider"));
  const expectMaskedPassword = async () => {
    await expect(editor).toContainText(fixtureServer);
    // The stored password never comes back: the field is empty and shows the mask.
    const password = editor.getByTestId(field("password"));
    await expect(password).toHaveValue("");
    await expect(password).toHaveAttribute("placeholder", "••••••••");
  };
  const connectionsCell = serverRow.getByTestId(settingsIds.providerConnections);
  if (afterRestart) {
    await expect(connectionsCell).toHaveText("5");
    await serverRow.click();
    await expectMaskedPassword();
    await expect(editor.getByTestId(field("connections"))).toHaveValue("5");
    await editor.getByTestId(testIds.editorCancel).click();
    await expect(editor).toBeHidden();
    return;
  }

  await serverRow.click();
  await expectMaskedPassword();
  const connections = editor.getByTestId(field("connections"));
  await connections.fill("5");
  await connections.press("Tab");
  await editor.getByTestId(testIds.editorSave).click();
  await expect(editor).toBeHidden();
  await expect(connectionsCell).toHaveText("5");
  await page.reload();
  await expect(connectionsCell).toHaveText("5");

  await page.getByTestId(testIds.tableAdd("servers")).click();
  const addForm = page.getByTestId(testIds.dialog("provider"));
  await addForm.getByTestId(field("host")).fill("e2e-ui.invalid");
  await addForm.getByTestId(field("tls")).click();
  await expect(addForm.getByTestId(field("port"))).toHaveValue("119");
  const addConnections = addForm.getByTestId(field("connections"));
  await addConnections.fill("1");
  await addConnections.press("Tab");
  const enabled = addForm.getByTestId(field("active"));
  await expect(enabled).toBeChecked();
  await enabled.click();
  await addForm.getByTestId(testIds.editorSave).click();
  await expect(addForm).toBeHidden();

  const temporaryRow = tableRow(page, "servers", "e2e-ui.invalid");
  await expect(temporaryRow.getByTestId(settingsIds.providerEnabled)).not.toBeChecked();
  await expect(temporaryRow.getByTestId(settingsIds.providerTransport("plain"))).toBeVisible();
  await temporaryRow.click();
  await page.getByTestId(testIds.dialog("provider")).getByTestId(testIds.editorDelete).click();
  await confirmRemoval(page, "provider-remove");
  await expect(temporaryRow).toHaveCount(0);
});

test("category create, edit, persistence, and delete are browser-owned", async ({ cleanPage: page }) => {
  await page.goto("/settings/categories");
  const removeCategory = async (name: string) => {
    await tableRow(page, "categories", name).click();
    const editor = page.getByTestId(testIds.dialog("category"));
    await expect(editor).toContainText(name);
    await editor.getByTestId(testIds.editorDelete).click();
    await confirmRemoval(page, "category-remove");
    await expect(tableRow(page, "categories", name)).toHaveCount(0);
  };
  const persistedAliases = "persisted-*";
  if (afterRestart) {
    await expect(tableRow(page, "categories", persistedCategory).getByText(persistedAliases, { exact: true })).toBeVisible();
    await removeCategory(persistedCategory);
    return;
  }

  const addCategory = async (name: string, aliases: string) => {
    await page.getByTestId(testIds.tableAdd("categories")).click();
    const form = page.getByTestId(testIds.dialog("category"));
    await form.getByTestId(field("name")).fill(name);
    await form.getByTestId(field("aliases")).fill(aliases);
    await form.getByTestId(testIds.editorSave).click();
    await expect(form).toBeHidden();
    await expect(tableRow(page, "categories", name).getByText(aliases, { exact: true })).toBeVisible();
  };

  const category = "e2e-product-category";
  const edited = "e2e-product-category-edited";
  await addCategory(category, "e2e-product-*");
  await page.reload();
  await expect(tableRow(page, "categories", category)).toBeVisible();
  await tableRow(page, "categories", category).click();
  const form = page.getByTestId(testIds.dialog("category"));
  await expect(form).toContainText(category);
  await form.getByTestId(field("name")).fill(edited);
  await form.getByTestId(testIds.editorSave).click();
  await expect(form).toBeHidden();
  await expect(tableRow(page, "categories", edited)).toBeVisible();
  await expect(tableRow(page, "categories", category)).toHaveCount(0);

  await removeCategory(edited);
  await addCategory(persistedCategory, persistedAliases);
});

test("schedule rules support create, toggle, edit, and delete", async ({ cleanPage: page }) => {
  await page.goto("/settings/schedules");
  // The list heads only the groups that hold a rule, so the list itself is what loads.
  await expect(page.getByTestId(testIds.section("schedules"))).toBeVisible();
  const removeSchedule = async (label: string) => {
    await scheduleRow(page, "DOWNLOADS", label).click();
    const editor = page.getByTestId(testIds.dialog("schedule"));
    await expect(editor).toContainText(label);
    await editor.getByTestId(testIds.editorDelete).click();
    await confirmRemoval(page, "schedule-remove");
    await expect(scheduleRow(page, "DOWNLOADS", label)).toHaveCount(0);
  };
  if (afterRestart) {
    const persistedRule = scheduleRow(page, "DOWNLOADS", persistedSchedule);
    await expect(persistedRule).toContainText("04:30");
    await expect(persistedRule.getByTestId(settingsIds.scheduleEnabled)).not.toBeChecked();
    await removeSchedule(persistedSchedule);
    return;
  }

  const addSchedule = async (time: string, label: string) => {
    await page.getByTestId(testIds.tableAdd("schedules")).click();
    const form = page.getByTestId(testIds.dialog("schedule"));
    await form.getByTestId(field("time")).fill(time);
    await form.getByTestId(field("label")).fill(label);
    await form.getByTestId(field("enabled")).click();
    await form.getByTestId(testIds.editorSave).click();
    await expect(form).toBeHidden();
    const row = scheduleRow(page, "DOWNLOADS", label);
    await expect(row).toContainText(time);
    return row;
  };

  let rule = await addSchedule("03:15", "e2e-off-peak");
  let enabled = rule.getByTestId(settingsIds.scheduleEnabled);
  await expect(enabled).not.toBeChecked();
  await page.reload();
  await expect(enabled).not.toBeChecked();

  await rule.click();
  const form = page.getByTestId(testIds.dialog("schedule"));
  await expect(form).toContainText("e2e-off-peak");
  await form.getByTestId(field("label")).fill("e2e-off-peak-edited");
  // Exercise the editor toggle without publishing an enabled hold to this
  // shared instance, where either pause or resume affects other scenarios.
  const draftEnabled = form.getByTestId(field("enabled"));
  await draftEnabled.click();
  await expect(draftEnabled).toBeChecked();
  await draftEnabled.click();
  await expect(draftEnabled).not.toBeChecked();
  await form.getByTestId(testIds.editorSave).click();
  await expect(form).toBeHidden();
  rule = scheduleRow(page, "DOWNLOADS", "e2e-off-peak-edited");
  await expect(rule).toBeVisible();
  await expect(rule).toContainText("03:15");
  enabled = rule.getByTestId(settingsIds.scheduleEnabled);
  await expect(enabled).not.toBeChecked();
  await removeSchedule("e2e-off-peak-edited");

  rule = await addSchedule("04:30", persistedSchedule);
  const persistedEnabled = rule.getByTestId(settingsIds.scheduleEnabled);
  await expect(persistedEnabled).not.toBeChecked();
});

test("new schedule actions stay disabled and appear on their own tracks", async ({ cleanPage: page }) => {
  test.skip(afterRestart, "temporary rules are removed before restart");
  await page.goto("/settings/schedules");
  // Each action, by its value, and the track its rules land on.
  const cases = [
    ["pause_all", "DOWNLOADS"],
    ["pause_post_processing", "POST_PROCESSING"],
    ["resume_post_processing", "POST_PROCESSING"],
    ["set_server_active", "SERVER"],
    ["set_quota_metering", "QUOTA"],
    ["pause_rss", "RSS"],
    ["resume_rss", "RSS"],
    ["prune_history", "ONE_SHOT"],
  ];
  for (const [action, track] of cases) {
    const label = `e2e-disabled-${action}`;
    await page.getByTestId(testIds.tableAdd("schedules")).click();
    const form = page.getByTestId(testIds.dialog("schedule"));
    await form.getByTestId(field("label")).fill(label);
    await form.getByTestId(field("enabled")).click();
    await choose(page, form, field("actionType"), action);
    if (action === "set_server_active") {
      await form.getByTestId(field("serverId")).click();
      await byTestIdPrefix(page.getByTestId(testIds.menu(field("serverId"))), testIds.option(field("serverId"), ""))
        .filter({ has: page.getByText(fixtureServer, { exact: true }) })
        .click();
    }
    if (action === "prune_history") await form.getByTestId(field("pruneCompleted")).check();
    await form.getByTestId(testIds.editorSave).click();
    await expect(form).toBeHidden();
    let row = scheduleRow(page, track, label);
    await expect(row).toBeVisible();
    await expect(row.getByTestId(settingsIds.scheduleEnabled)).not.toBeChecked();
    await row.click();
    const edit = page.getByTestId(testIds.dialog("schedule"));
    await expect(edit).toContainText(label);
    await edit.getByTestId(field("label")).fill(`${label}-edited`);
    // Toggle only the draft, and save disabled so no hold reaches the shared instance.
    await edit.getByTestId(field("enabled")).click();
    await edit.getByTestId(field("enabled")).click();
    await edit.getByTestId(testIds.editorSave).click();
    await expect(edit).toBeHidden();
    await page.reload();
    row = scheduleRow(page, track, `${label}-edited`);
    await expect(row.getByTestId(settingsIds.scheduleEnabled)).not.toBeChecked();
    await row.click();
    const editedForm = page.getByTestId(testIds.dialog("schedule"));
    await expect(editedForm).toContainText(`${label}-edited`);
    await editedForm.getByTestId(testIds.editorDelete).click();
    await confirmRemoval(page, "schedule-remove");
    await expect(row).toHaveCount(0);
  }
});

test("the performance profile saves the moment one is picked and keeps it", async ({ cleanPage: page, request }) => {
  const offered = await introspectHardwareProfile(request);
  test.skip(
    offered.available.length < 2,
    "this machine can run only one hardware profile, so Settings offers no choice",
  );

  await page.goto("/settings/general");
  const recommended = offered.recommended;
  const profileId = field("hardwareProfile");
  const profile = page.getByTestId(testIds.section("performance")).getByTestId(profileId);
  const showsRecommended = profile.getByTestId(testIds.selected(profileId, recommended));
  const unconfirmed = page.getByTestId(settingsIds.profileUnconfirmed);
  if (afterRestart) {
    await expect(showsRecommended).toBeVisible();
    await expect(unconfirmed).toBeHidden();
    return;
  }

  const saved = operationResponse(page, "SetHardwareProfile");
  await profile.click();
  await page.getByTestId(testIds.menu(profileId)).getByTestId(testIds.option(profileId, recommended)).click();
  const response = await saved;
  expect(response.ok()).toBeTruthy();
  const payload = await response.json();
  expect(payload.errors ?? [], JSON.stringify(payload.errors ?? [])).toEqual([]);
  await expect(showsRecommended).toBeVisible();
  await expect(unconfirmed).toBeHidden();
  await expect(page.getByRole("alert")).toBeHidden();

  await page.reload();
  await expect(showsRecommended).toBeVisible();
  await expect(unconfirmed).toBeHidden();
});

test("settings navigation owns every coverage-ledger route", async ({ cleanPage: page }) => {
  await page.goto("/settings");
  await expect(page).toHaveURL(/\/settings\/general$/);
  const navigation = page.getByTestId(settingsIds.nav);
  const ledger = JSON.parse(
    readFileSync(resolve(process.cwd(), "coverage-ledger.v1.json"), "utf8"),
  ) as { routes: Array<{ path: string }> };
  const expected = ledger.routes
    .map(({ path }) => path)
    .filter((path) => path.startsWith("/settings/"))
    .map((path) => path.slice("/settings/".length))
    .sort();
  const settingsRoutes = async () => {
    const links = await navigation.getByRole("link").all();
    const hrefs = await Promise.all(links.map((link) => link.getAttribute("href")));
    return Array.from(
      new Set(
        hrefs
          .filter((href): href is string => href?.includes("/settings/") === true)
          .map((href) => new URL(href, page.url()).pathname)
          .map((pathname) => pathname.split("/settings/").at(-1)!.replace(/\/+$/, "")),
      ),
    ).sort();
  };
  // The rail is client-rendered, so keep polling; the deep equality keeps
  // missing and extra routes exact.
  await expect.poll(settingsRoutes).toEqual(expected);
});

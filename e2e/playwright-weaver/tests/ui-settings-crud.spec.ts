import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const persistedCategory = "e2e-product-category-persisted";
const persistedSchedule = "e2e-off-peak-persisted";

/** A settings table row, found by the exact text of one of its cells. */
function tableRow(page: Page, table: string, cellText: string): Locator {
  return page
    .getByRole("region", { name: table, exact: true })
    .getByRole("button")
    .filter({ has: page.getByText(cellText, { exact: true }) });
}

/** Commit a panel's draft through the settings top bar and wait for it to land. */
async function saveSettings(page: Page) {
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("button", { name: "Saved", exact: true })).toBeDisabled();
}

async function confirmRemoval(page: Page, label: string) {
  const confirm = page.getByRole("dialog", { name: label, exact: true });
  await confirm.getByRole("button", { name: label, exact: true }).click();
  await expect(confirm).toBeHidden();
}

test("general SRRDB lookup and bandwidth ceiling and cap persist through the settings top bar", async ({ cleanPage: page }) => {
  await page.goto("/settings/general");
  const srrdbLookup = page.getByRole("switch", { name: "SRRDB release lookup", exact: true });
  await expect(srrdbLookup).toBeVisible();
  if (afterRestart) {
    await expect(srrdbLookup).toBeChecked();
    await page.goto("/settings/bandwidth");
    await expect(page.getByRole("spinbutton", { name: "Download ceiling", exact: true })).toHaveValue("8");
    await expect(page.getByRole("switch", { name: "Enforce a data cap", exact: true })).toBeChecked();
    await expect(page.getByRole("spinbutton", { name: "Reset day of month", exact: true })).toHaveValue("17");
    return;
  }

  await expect(srrdbLookup).not.toBeChecked();
  await srrdbLookup.click();
  await expect(page.getByText("Unsaved changes", { exact: true })).toBeVisible();
  await saveSettings(page);
  await page.reload();
  await expect(srrdbLookup).toBeChecked();

  await page.goto("/settings/bandwidth");
  const ceiling = page.getByRole("spinbutton", { name: "Download ceiling", exact: true });
  await ceiling.fill("8");
  await ceiling.press("Tab");
  await page.getByRole("switch", { name: "Enforce a data cap", exact: true }).click();
  await page
    .getByRole("radiogroup", { name: "Cap window", exact: true })
    .getByRole("radio", { name: "Monthly", exact: true })
    .click();
  await page.getByRole("textbox", { name: "Allowance", exact: true }).fill("500");
  const monthlyDay = page.getByRole("spinbutton", { name: "Reset day of month", exact: true });
  // The day of month clamps to the calendar instead of refusing the draft.
  await monthlyDay.fill("32");
  await monthlyDay.press("Tab");
  await expect(monthlyDay).toHaveValue("31");
  await monthlyDay.fill("17");
  await monthlyDay.press("Tab");
  await saveSettings(page);
  await page.reload();
  await expect(ceiling).toHaveValue("8");
  await expect(monthlyDay).toHaveValue("17");
});

test("provider edits keep the stored password masked and a disabled provider can be added and removed", async ({ cleanPage: page }) => {
  await page.goto("/settings/servers");
  const serverRow = tableRow(page, "Servers", "nntp");
  const editor = page.getByRole("dialog", { name: "nntp", exact: true });
  const expectMaskedPassword = async () => {
    const password = editor.getByLabel("Password", { exact: true });
    await expect(password).toHaveValue("");
    await expect(password).toHaveAttribute("placeholder", "••••••••");
    await expect(editor.getByText("Leave blank to keep the stored password.", { exact: true })).toBeVisible();
  };
  if (afterRestart) {
    await expect(serverRow.getByText("5", { exact: true })).toBeVisible();
    await serverRow.click();
    await expectMaskedPassword();
    await expect(editor.getByRole("spinbutton", { name: "Connections", exact: true })).toHaveValue("5");
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(editor).toBeHidden();
    return;
  }

  await serverRow.click();
  await expectMaskedPassword();
  const connections = editor.getByRole("spinbutton", { name: "Connections", exact: true });
  await connections.fill("5");
  await connections.press("Tab");
  await editor.getByRole("button", { name: "Save", exact: true }).click();
  await expect(editor).toBeHidden();
  await expect(serverRow.getByText("5", { exact: true })).toBeVisible();
  await page.reload();
  await expect(serverRow.getByText("5", { exact: true })).toBeVisible();

  await page.getByRole("banner").getByRole("button", { name: "Add provider", exact: true }).click();
  const addForm = page.getByRole("dialog", { name: "Add provider", exact: true });
  await addForm.getByRole("textbox", { name: "Host", exact: true }).fill("e2e-ui.invalid");
  await addForm.getByRole("switch", { name: "TLS", exact: true }).click();
  await expect(addForm.getByRole("spinbutton", { name: "Port", exact: true })).toHaveValue("119");
  const addConnections = addForm.getByRole("spinbutton", { name: "Connections", exact: true });
  await addConnections.fill("1");
  await addConnections.press("Tab");
  const enabled = addForm.getByRole("switch", { name: "Enabled", exact: true });
  await expect(enabled).toBeChecked();
  await enabled.click();
  await addForm.getByRole("button", { name: "Save", exact: true }).click();
  await expect(addForm).toBeHidden();

  const temporaryRow = tableRow(page, "Servers", "e2e-ui.invalid");
  await expect(temporaryRow.getByRole("switch", { name: "e2e-ui.invalid enabled", exact: true })).not.toBeChecked();
  await expect(temporaryRow.getByText("Plain", { exact: true })).toBeVisible();
  await temporaryRow.click();
  await page
    .getByRole("dialog", { name: "e2e-ui.invalid", exact: true })
    .getByRole("button", { name: "Remove provider", exact: true })
    .click();
  await confirmRemoval(page, "Remove provider");
  await expect(temporaryRow).toHaveCount(0);
});

test("category create, edit, persistence, and delete are browser-owned", async ({ cleanPage: page }) => {
  await page.goto("/settings/categories");
  const removeCategory = async (name: string) => {
    await tableRow(page, "Categories", name).click();
    await page
      .getByRole("dialog", { name, exact: true })
      .getByRole("button", { name: "Remove category", exact: true })
      .click();
    await confirmRemoval(page, "Remove category");
    await expect(tableRow(page, "Categories", name)).toHaveCount(0);
  };
  if (afterRestart) {
    await expect(tableRow(page, "Categories", persistedCategory).getByText("persisted-*", { exact: true })).toBeVisible();
    await removeCategory(persistedCategory);
    return;
  }

  const addCategory = async (name: string, aliases: string) => {
    await page.getByRole("banner").getByRole("button", { name: "Add category", exact: true }).click();
    const form = page.getByRole("dialog", { name: "Add category", exact: true });
    await form.getByRole("textbox", { name: "Name", exact: true }).fill(name);
    await form.getByRole("textbox", { name: "Also known as", exact: true }).fill(aliases);
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await expect(form).toBeHidden();
    await expect(tableRow(page, "Categories", name).getByText(aliases, { exact: true })).toBeVisible();
  };

  await addCategory("e2e-product-category", "e2e-product-*");
  await page.reload();
  await expect(tableRow(page, "Categories", "e2e-product-category")).toBeVisible();
  await tableRow(page, "Categories", "e2e-product-category").click();
  const form = page.getByRole("dialog", { name: "e2e-product-category", exact: true });
  await form.getByRole("textbox", { name: "Name", exact: true }).fill("e2e-product-category-edited");
  await form.getByRole("button", { name: "Save", exact: true }).click();
  await expect(form).toBeHidden();
  await expect(tableRow(page, "Categories", "e2e-product-category-edited")).toBeVisible();
  await expect(tableRow(page, "Categories", "e2e-product-category")).toHaveCount(0);

  await removeCategory("e2e-product-category-edited");
  await addCategory(persistedCategory, "persisted-*");
});

test("schedule rules support create, toggle, edit, and delete", async ({ cleanPage: page }) => {
  await page.goto("/settings/schedules");
  const removeSchedule = async (label: string) => {
    await tableRow(page, "Schedules", label).click();
    await page
      .getByRole("dialog", { name: label, exact: true })
      .getByRole("button", { name: "Remove schedule", exact: true })
      .click();
    await confirmRemoval(page, "Remove schedule");
    await expect(tableRow(page, "Schedules", label)).toHaveCount(0);
  };
  if (afterRestart) {
    const persistedRule = tableRow(page, "Schedules", persistedSchedule);
    await expect(persistedRule.getByRole("switch", { name: "04:30 schedule enabled", exact: true })).not.toBeChecked();
    await removeSchedule(persistedSchedule);
    return;
  }

  const addSchedule = async (time: string, label: string) => {
    await page.getByRole("banner").getByRole("button", { name: "Add schedule", exact: true }).click();
    const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
    await form.getByLabel("Time", { exact: true }).fill(time);
    await form.getByRole("textbox", { name: "Label", exact: true }).fill(label);
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await expect(form).toBeHidden();
    return tableRow(page, "Schedules", label);
  };

  let rule = await addSchedule("03:15", "e2e-off-peak");
  const enabled = rule.getByRole("switch", { name: "03:15 schedule enabled", exact: true });
  await expect(enabled).toBeChecked();
  await enabled.click();
  await expect(enabled).not.toBeChecked();
  await page.reload();
  await expect(enabled).not.toBeChecked();

  await rule.click();
  const form = page.getByRole("dialog", { name: "e2e-off-peak", exact: true });
  await form.getByRole("textbox", { name: "Label", exact: true }).fill("e2e-off-peak-edited");
  await form.getByRole("button", { name: "Save", exact: true }).click();
  await expect(form).toBeHidden();
  rule = tableRow(page, "Schedules", "e2e-off-peak-edited");
  await expect(rule).toBeVisible();
  await removeSchedule("e2e-off-peak-edited");

  rule = await addSchedule("04:30", persistedSchedule);
  const persistedEnabled = rule.getByRole("switch", { name: "04:30 schedule enabled", exact: true });
  await persistedEnabled.click();
  await expect(persistedEnabled).not.toBeChecked();
});

test("settings navigation owns every coverage-ledger route", async ({ cleanPage: page }) => {
  await page.goto("/settings");
  await expect(page).toHaveURL(/\/settings\/general$/);
  const navigation = page.getByRole("navigation", { name: "Settings", exact: true });
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

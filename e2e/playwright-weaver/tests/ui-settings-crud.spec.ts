import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Locator, Page } from "@playwright/test";
import { expect, test } from "./helpers";
import { introspectHardwareProfile } from "./support/runtime-introspection";

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
  // The list heads only the groups that hold a rule, so the list itself is what loads.
  await expect(page.getByRole("region", { name: "Schedules", exact: true })).toBeVisible();
  const removeSchedule = async (label: string) => {
    await tableRow(page, "Downloads", label).click();
    await page
      .getByRole("dialog", { name: label, exact: true })
      .getByRole("button", { name: "Remove schedule", exact: true })
      .click();
    await confirmRemoval(page, "Remove schedule");
    await expect(tableRow(page, "Downloads", label)).toHaveCount(0);
  };
  if (afterRestart) {
    const persistedRule = tableRow(page, "Downloads", persistedSchedule);
    await expect(persistedRule.getByRole("switch", { name: "04:30 schedule enabled", exact: true })).not.toBeChecked();
    await removeSchedule(persistedSchedule);
    return;
  }

  const addSchedule = async (time: string, label: string) => {
    await page.getByRole("banner").getByRole("button", { name: "Add schedule", exact: true }).click();
    const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
    await form.getByLabel("Time", { exact: true }).fill(time);
    await form.getByRole("textbox", { name: "Label", exact: true }).fill(label);
    await form.getByRole("switch", { name: "Enabled", exact: true }).click();
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await expect(form).toBeHidden();
    return tableRow(page, "Downloads", label);
  };

  let rule = await addSchedule("03:15", "e2e-off-peak");
  let enabled = rule.getByRole("switch", { name: "03:15 schedule enabled", exact: true });
  await expect(enabled).not.toBeChecked();
  await page.reload();
  await expect(enabled).not.toBeChecked();

  await rule.click();
  const form = page.getByRole("dialog", { name: "e2e-off-peak", exact: true });
  await form.getByRole("textbox", { name: "Label", exact: true }).fill("e2e-off-peak-edited");
  // Exercise the editor toggle without publishing an enabled hold to this
  // shared instance, where either pause or resume affects other scenarios.
  const draftEnabled = form.getByRole("switch", { name: "Enabled", exact: true });
  await draftEnabled.click();
  await expect(draftEnabled).toBeChecked();
  await draftEnabled.click();
  await expect(draftEnabled).not.toBeChecked();
  await form.getByRole("button", { name: "Save", exact: true }).click();
  await expect(form).toBeHidden();
  rule = tableRow(page, "Downloads", "e2e-off-peak-edited");
  await expect(rule).toBeVisible();
  enabled = rule.getByRole("switch", { name: "03:15 schedule enabled", exact: true });
  await expect(enabled).not.toBeChecked();
  await removeSchedule("e2e-off-peak-edited");

  rule = await addSchedule("04:30", persistedSchedule);
  const persistedEnabled = rule.getByRole("switch", { name: "04:30 schedule enabled", exact: true });
  await expect(persistedEnabled).not.toBeChecked();
});

test("new schedule actions stay disabled and appear on their own tracks", async ({ cleanPage: page }) => {
  test.skip(afterRestart, "temporary rules are removed before restart");
  await page.goto("/settings/schedules");
  const cases = [
    ["Pause all intake", "Downloads"],
    ["Pause post-processing", "Post-processing"],
    ["Resume post-processing", "Post-processing"],
    ["Set server availability", "Servers"],
    ["Set quota metering", "Quota metering"],
    ["Scan watch folder", "One-shot actions"],
    ["Fetch RSS", "One-shot actions"],
    ["Prune history", "One-shot actions"],
  ];
  for (const [action, track] of cases) {
    const label = `e2e-disabled-${action}`;
    await page.getByRole("banner").getByRole("button", { name: "Add schedule", exact: true }).click();
    const form = page.getByRole("dialog", { name: "Add schedule", exact: true });
    await form.getByRole("textbox", { name: "Label", exact: true }).fill(label);
    await form.getByRole("switch", { name: "Enabled", exact: true }).click();
    await form.getByRole("button", { name: "Action", exact: true }).click();
    await page.getByRole("menuitemradio", { name: action, exact: true }).click();
    if (action === "Set server availability") {
      await form.getByRole("button", { name: "Server", exact: true }).click();
      await page.getByRole("menuitemradio", { name: "nntp", exact: true }).click();
    }
    if (action === "Prune history") await form.getByRole("switch", { name: "Completed", exact: true }).check();
    await form.getByRole("button", { name: "Save", exact: true }).click();
    await expect(form).toBeHidden();
    let row = tableRow(page, track, label);
    await expect(row).toBeVisible();
    await expect(row.getByRole("switch")).not.toBeChecked();
    await row.click();
    const edit = page.getByRole("dialog", { name: label, exact: true });
    await edit.getByRole("textbox", { name: "Label", exact: true }).fill(`${label}-edited`);
    // Toggle only the draft, and save disabled so no hold reaches the shared instance.
    await edit.getByRole("switch", { name: "Enabled", exact: true }).click();
    await edit.getByRole("switch", { name: "Enabled", exact: true }).click();
    await edit.getByRole("button", { name: "Save", exact: true }).click();
    await expect(edit).toBeHidden();
    await page.reload();
    row = tableRow(page, track, `${label}-edited`);
    await expect(row.getByRole("switch")).not.toBeChecked();
    await row.click();
    await page.getByRole("dialog", { name: `${label}-edited`, exact: true }).getByRole("button", { name: "Remove schedule", exact: true }).click();
    await confirmRemoval(page, "Remove schedule");
    await expect(row).toHaveCount(0);
  }
});

const profileNames: Record<string, string> = {
  EFFICIENT: "Efficient",
  BALANCED: "Balanced",
  PERFORMANCE: "Performance",
};

test("the performance profile saves the moment one is picked and keeps it", async ({ cleanPage: page, request }) => {
  const offered = await introspectHardwareProfile(request);
  test.skip(
    offered.available.length < 2,
    "this machine can run only one hardware profile, so Settings offers no choice",
  );

  await page.goto("/settings/general");
  const recommendedName = profileNames[offered.recommended];
  expect(recommendedName, `unknown profile ${offered.recommended}`).toBeTruthy();
  const profile = page
    .getByRole("region", { name: "Performance", exact: true })
    .getByRole("button", { name: "Profile", exact: true });
  const unconfirmed = page.getByText("Running the recommended profile, not yet confirmed.", { exact: true });
  if (afterRestart) {
    await expect(profile).toHaveText(recommendedName);
    await expect(unconfirmed).toBeHidden();
    return;
  }

  const saved = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname.endsWith("/graphql")
      && response.request().method() === "POST"
      && response.request().postData()?.includes("mutation SetHardwareProfile") === true,
  );
  await profile.click();
  await page
    .getByRole("menu", { name: "Profile", exact: true })
    .getByRole("menuitemradio", { name: recommendedName, exact: true })
    .click();
  const response = await saved;
  expect(response.ok()).toBeTruthy();
  const payload = await response.json();
  expect(payload.errors ?? [], JSON.stringify(payload.errors ?? [])).toEqual([]);
  await expect(profile).toHaveText(recommendedName);
  await expect(unconfirmed).toBeHidden();
  await expect(page.getByRole("alert")).toBeHidden();

  await page.reload();
  await expect(profile).toHaveText(recommendedName);
  await expect(unconfirmed).toBeHidden();
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

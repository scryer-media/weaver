import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, openNavigation, test } from "./helpers";

const afterRestart = process.env.E2E_WEAVER_UI_STAGE === "after-restart";
const persistedCategory = "e2e-product-category-persisted";
const persistedSchedule = "e2e-off-peak-persisted";

test("general speed, SRRDB lookup, and bandwidth-cap settings persist through browser controls", async ({ cleanPage: page }) => {
  await page.goto("/settings/general");
  const speed = page.getByRole("slider", { name: "Speed Limit" });
  const srrdbLookup = page.getByRole("switch", { name: "Use SRRDB release lookup" });
  await expect(speed).toBeVisible();
  await expect(srrdbLookup).toBeVisible();
  if (afterRestart) {
    await expect(speed).toHaveValue(String(8 * 1024 * 1024));
    await expect(srrdbLookup).toBeChecked();
    await page.goto("/settings/bandwidth");
    await expect(page.getByRole("spinbutton", { name: "Billing Day" })).toHaveValue("17");
    return;
  }
  await speed.press("Home");
  for (let step = 0; step < 8; step += 1) {
    await speed.press("ArrowRight");
  }
  await expect(speed).toHaveValue(String(8 * 1024 * 1024));
  await page.getByRole("button", { name: "Apply Now" }).click();
  await expect(page.getByText("Saved", { exact: true })).toBeVisible();
  await page.reload();
  await expect(speed).toHaveValue(String(8 * 1024 * 1024));
  await expect(srrdbLookup).not.toBeChecked();
  await srrdbLookup.click();
  await expect(page.getByText("Saved", { exact: true })).toBeVisible();
  await page.reload();
  await expect(srrdbLookup).toBeChecked();

  await page.goto("/settings/bandwidth");
  const monthlyDay = page.getByRole("spinbutton", { name: "Billing Day" });
  await monthlyDay.fill("32");
  await expect(page.getByRole("button", { name: "Save" })).toBeDisabled();
  await monthlyDay.fill("17");
  await page.getByRole("button", { name: "Save" }).click();
  await expect(page.getByText("Saved", { exact: true })).toBeVisible();
  await page.reload();
  await expect(monthlyDay).toHaveValue("17");
});

test("server edits preserve masked secrets and survive refresh", async ({ cleanPage: page }) => {
  await page.goto("/settings/servers");
  const serverRow = page.getByRole("row").filter({ hasText: "nntp:119" });
  const connectionsCell = serverRow.getByTestId("server-connections");
  if (afterRestart) {
    await expect(connectionsCell).toHaveText("5");
    await serverRow.getByRole("button", { name: "Edit" }).click();
    const persistedForm = page.getByRole("region", { name: "Edit Server" });
    await expect(persistedForm.getByLabel("Password")).toHaveValue("");
    await expect(persistedForm.getByLabel("Password")).toHaveAttribute(
      "placeholder",
      "Leave blank to keep",
    );
    return;
  }
  await serverRow.getByRole("button", { name: "Edit" }).click();
  const form = page.getByRole("region", { name: "Edit Server" });
  const password = form.getByLabel("Password");
  await expect(password).toHaveAttribute("placeholder", "Leave blank to keep");
  await expect(password).toHaveValue("");
  await form.getByLabel("Connections").fill("5");
  await form.getByRole("button", { name: "Save" }).click();
  await expect(connectionsCell).toHaveText("5");
  await page.reload();
  await expect(connectionsCell).toHaveText("5");

  await page.getByTestId("add-server-button").click();
  const addForm = page.getByRole("region", { name: "Add Server" });
  await addForm.getByLabel("Host").fill("e2e-ui.invalid");
  await addForm.getByLabel("Port").fill("119");
  await addForm.getByLabel("Connections").fill("1");
  const active = addForm.getByRole("checkbox", { name: "Active" });
  if (await active.isChecked()) await active.click();
  await addForm.getByRole("button", { name: "Add Server" }).click();
  const temporaryRow = page.getByRole("row").filter({ hasText: "e2e-ui.invalid:119" });
  await expect(temporaryRow).toContainText("Disabled");
  await temporaryRow.getByRole("button", { name: "Delete" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete Server" }).click();
  await expect(page.getByText("e2e-ui.invalid:119", { exact: true })).toHaveCount(0);
});

test("category create, edit, persistence, and delete are browser-owned", async ({ cleanPage: page }) => {
  await page.goto("/settings/categories");
  if (afterRestart) {
    const persistedRow = page.getByRole("row").filter({ hasText: persistedCategory });
    await expect(persistedRow).toContainText("persisted-*");
    await persistedRow.getByRole("button", { name: "Delete" }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete Category" }).click();
    await expect(page.getByText(persistedCategory, { exact: true })).toHaveCount(0);
    return;
  }
  await page.getByTestId("add-category-button").click();
  let form = page.getByRole("region", { name: "Add Category" });
  await form.getByLabel("Name").fill("e2e-product-category");
  await form.getByLabel("Aliases").fill("e2e-product-*");
  await form.getByRole("button", { name: "Add Category" }).click();

  let row = page.getByRole("row").filter({ hasText: "e2e-product-category" });
  await expect(row).toContainText("e2e-product-*");
  await page.reload();
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Edit" }).click();
  form = page.getByRole("region", { name: "Edit Category" });
  await form.getByLabel("Name").fill("e2e-product-category-edited");
  await form.getByRole("button", { name: "Save" }).click();
  row = page.getByRole("row").filter({ hasText: "e2e-product-category-edited" });
  await expect(row).toBeVisible();

  await row.getByRole("button", { name: "Delete" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete Category" }).click();
  await expect(page.getByText("e2e-product-category-edited", { exact: true })).toHaveCount(0);

  await page.getByTestId("add-category-button").click();
  form = page.getByRole("region", { name: "Add Category" });
  await form.getByLabel("Name").fill(persistedCategory);
  await form.getByLabel("Aliases").fill("persisted-*");
  await form.getByRole("button", { name: "Add Category" }).click();
  await expect(page.getByRole("row").filter({ hasText: persistedCategory })).toBeVisible();
});

test("schedule rules support create, toggle, edit, and delete", async ({ cleanPage: page }) => {
  await page.goto("/settings/schedules");
  if (afterRestart) {
    const persistedRule = page.getByRole("group", { name: persistedSchedule, exact: true });
    await expect(persistedRule).toContainText("Disabled");
    await persistedRule.getByRole("button", { name: "Delete" }).click();
    await expect(page.getByRole("group", { name: persistedSchedule, exact: true })).toHaveCount(0);
    return;
  }
  await page.getByRole("button", { name: "Add Rule" }).click();
  await page.getByLabel("Time").fill("03:15");
  await page.getByLabel("Label").fill("e2e-off-peak");
  await page.getByRole("button", { name: "Create" }).click();

  let rule = page.getByRole("group", { name: "e2e-off-peak", exact: true });
  await expect(rule).toContainText("Enabled");
  await rule.getByRole("switch").click();
  await expect(rule).toContainText("Disabled");
  await page.reload();
  await expect(rule).toContainText("Disabled");

  await rule.getByRole("button", { name: "Edit" }).click();
  await page.getByLabel("Label").fill("e2e-off-peak-edited");
  await page.getByRole("button", { name: "Save" }).click();
  rule = page.getByRole("group", { name: "e2e-off-peak-edited", exact: true });
  await expect(rule).toBeVisible();
  await rule.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByRole("group", { name: "e2e-off-peak-edited", exact: true })).toHaveCount(0);

  await page.getByRole("button", { name: "Add Rule" }).click();
  await page.getByLabel("Time").fill("04:30");
  await page.getByLabel("Label").fill(persistedSchedule);
  await page.getByRole("button", { name: "Create" }).click();
  rule = page.getByRole("group", { name: persistedSchedule, exact: true });
  await rule.getByRole("switch").click();
  await expect(rule).toContainText("Disabled");
});

test("settings navigation owns every coverage-ledger route", async ({ cleanPage: page }) => {
  await page.goto("/settings/general");
  const navigation = await openNavigation(page);
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
  // The whole shell is client-rendered, so keep polling after opening whichever
  // responsive navigation variant the current viewport exposes. The same deep
  // equality keeps missing and extra routes exact.
  await expect.poll(settingsRoutes).toEqual(expected);
});

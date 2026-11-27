// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(
  process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright"
);
let server, browser, baseUrl;
before(async () => {
  server = await createServer({
    cacheDir: "node_modules/.vite/browser-general-panel",
    server: { host: "127.0.0.1", port: 0 },
  });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => {
  await browser?.close();
  await server?.close();
});

async function open(options = {}) {
  const page = await browser.newPage({
    viewport: { width: 1400, height: 1000 },
    ...options,
  });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) =>
    new URL(route.request().url()).origin === baseUrl
      ? route.continue()
      : route.abort(),
  );
  await page.goto(`${baseUrl}/tests/browser/general-panel.html`);
  return { page, errors };
}
/** With `GENERAL_PANEL_SCREENSHOT_DIR` set to a directory, `part` of the page is saved there as `name`.png. */
async function shot(part, name) {
  if (process.env.GENERAL_PANEL_SCREENSHOT_DIR) {
    await part.screenshot({
      path: join(process.env.GENERAL_PANEL_SCREENSHOT_DIR, `${name}.png`),
    });
  }
}
const sent = (page, name) =>
  page.evaluate(
    (name) =>
      window.generalPanelFixture.sent
        .filter((entry) => entry.name === name)
        .map((entry) => entry.variables),
    name,
  );
/** Wait until the panel has sent `count` mutations named `name`. */
const sentCount = (page, name, count) =>
  page.waitForFunction(
    ([name, count]) =>
      window.generalPanelFixture.sent.filter((entry) => entry.name === name)
        .length >= count,
    [name, count],
  );
/** The panel never offered the top bar anything to save or revert. */
async function assertNeverDirty(page) {
  const { dirty, actions } = await page.evaluate(() => ({
    dirty: window.generalPanelFixture.flags.filter((flags) => flags.dirty)
      .length,
    actions: window.generalPanelFixture.actions.current,
  }));
  assert.equal(dirty, 0);
  assert.equal(actions, null);
}

const appearanceMutation = "UpdateWebAppearance";
const appearancePicker = (page) =>
  page.getByRole("group", { name: "Appearance", exact: true });
const appearanceChoice = (page, kind, value) =>
  page.getByTestId(`appearance-${kind}-${value}`);
const pressed = (page, choice) =>
  choice.and(page.locator('[aria-pressed="true"]'));
const idleAppearance = (page) =>
  appearancePicker(page).and(page.locator('[aria-busy="false"]')).waitFor();
const bodyBackground = (page) =>
  page.evaluate(() => getComputedStyle(document.body).backgroundColor);

async function chooseAppearance(page, kind, value) {
  const choice = appearanceChoice(page, kind, value);
  await choice.click();
  await pressed(page, choice).waitFor();
  await idleAppearance(page);
}

test("the appearance picker previews the full page, rolls back a refusal, and restores the saved Weaver mode", async () => {
  const { page, errors } = await open();
  try {
    const picker = appearancePicker(page);
    const phosphor = appearanceChoice(page, "palette", "PHOSPHOR");
    const light = appearanceChoice(page, "mode", "LIGHT");
    const blue = appearanceChoice(page, "accent", "BLUE");
    await chooseAppearance(page, "mode", "LIGHT");
    const lightBackground = await bodyBackground(page);
    const phosphorBackground = await phosphor
      .locator('[data-palette="PHOSPHOR"]')
      .evaluate((preview) => getComputedStyle(preview).backgroundColor);
    assert.notEqual(phosphorBackground, lightBackground);

    await page.evaluate(() => {
      const fixture = window.generalPanelFixture;
      fixture.appearanceSave.pause();
      fixture.refuse.UpdateWebAppearance = "appearance was refused";
    });
    await phosphor.click();
    await sentCount(page, appearanceMutation, 2);
    await picker.and(page.locator('[aria-busy="true"]')).waitFor();
    await pressed(page, phosphor).waitFor();
    assert.equal(await bodyBackground(page), phosphorBackground);
    assert.equal(await light.count(), 0);
    assert.equal(await blue.isDisabled(), true);
    await page.evaluate(() =>
      window.generalPanelFixture.appearanceSave.resume(),
    );

    await picker.getByRole("alert").waitFor();
    assert.equal(
      await picker.getByRole("alert").textContent(),
      "appearance was refused",
    );
    assert.equal(await bodyBackground(page), lightBackground);
    await pressed(page, appearanceChoice(page, "palette", "WEAVER")).waitFor();
    await pressed(page, light).waitFor();

    await page.evaluate(() => {
      delete window.generalPanelFixture.refuse.UpdateWebAppearance;
    });
    await chooseAppearance(page, "palette", "PHOSPHOR");
    await chooseAppearance(page, "accent", "BLUE");
    assert.equal(await picker.getByRole("alert").count(), 0);
    assert.deepEqual(await sent(page, appearanceMutation), [
      { input: { mode: "LIGHT" } },
      { input: { palette: "PHOSPHOR" } },
      { input: { palette: "PHOSPHOR" } },
      { input: { accent: "BLUE" } },
    ]);
    assert.deepEqual(await sent(page, "UpdateSettings"), []);
    await assertNeverDirty(page);

    await page.reload();
    await pressed(page, phosphor).waitFor();
    await pressed(page, blue).waitFor();
    assert.equal(await bodyBackground(page), phosphorBackground);
    assert.equal(await light.count(), 0);
    await chooseAppearance(page, "palette", "WEAVER");
    await pressed(page, light).waitFor();
    await pressed(page, blue).waitFor();
    assert.equal(await bodyBackground(page), lightBackground);
    assert.deepEqual(await sent(page, appearanceMutation), [
      { input: { palette: "WEAVER" } },
    ]);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("Weaver follows the OS by default and returns to it after explicit Light and Dark modes", async () => {
  const { page, errors } = await open({ colorScheme: "light" });
  try {
    const light = appearanceChoice(page, "mode", "LIGHT");
    const system = appearanceChoice(page, "mode", "SYSTEM");
    await system.waitFor();
    assert.equal(await system.getAttribute("aria-pressed"), "true");
    const lightBackground = await bodyBackground(page);
    await page.emulateMedia({ colorScheme: "dark" });
    await page.waitForFunction(
      (light) => getComputedStyle(document.body).backgroundColor !== light,
      lightBackground,
    );
    const darkBackground = await bodyBackground(page);
    await page.reload();
    await system.waitFor();
    assert.equal(await system.getAttribute("aria-pressed"), "true");
    assert.equal(await bodyBackground(page), darkBackground);
    assert.deepEqual(await sent(page, appearanceMutation), []);

    await chooseAppearance(page, "mode", "LIGHT");
    assert.equal(await bodyBackground(page), lightBackground);
    await page.emulateMedia({ colorScheme: "light" });
    await page.emulateMedia({ colorScheme: "dark" });
    assert.equal(await bodyBackground(page), lightBackground);

    await chooseAppearance(page, "mode", "DARK");
    await page.emulateMedia({ colorScheme: "light" });
    assert.equal(await bodyBackground(page), darkBackground);

    await light.focus();
    await page.keyboard.press("Shift+Tab");
    await system.and(page.locator(":focus-visible")).waitFor();
    assert.equal(
      await system.evaluate((button) => getComputedStyle(button).outlineWidth),
      "2px",
    );
    await page.keyboard.press("Space");
    await pressed(page, system).waitFor();
    await idleAppearance(page);
    assert.equal(await bodyBackground(page), lightBackground);

    await page.emulateMedia({ colorScheme: "dark" });
    await page.waitForFunction(
      (expected) =>
        getComputedStyle(document.body).backgroundColor === expected,
      darkBackground,
    );
    await page.emulateMedia({ colorScheme: "light" });
    await page.waitForFunction(
      (expected) =>
        getComputedStyle(document.body).backgroundColor === expected,
      lightBackground,
    );
    await pressed(page, system).waitFor();
    assert.deepEqual(await sent(page, appearanceMutation), [
      { input: { mode: "LIGHT" } },
      { input: { mode: "DARK" } },
      { input: { mode: "SYSTEM" } },
    ]);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

const palettes = ["WEAVER", "PHOSPHOR", "AMBER", "TERMINAL"];
const accents = ["TEAL", "BLUE", "GREEN", "AMBER", "VIOLET", "ROSE"];

test("palette previews and accent swatches stay distinct and fit the picker on a narrow screen", async () => {
  const { page, errors } = await open({
    viewport: { width: 360, height: 1000 },
  });
  try {
    const picker = appearancePicker(page);
    await picker.waitFor();
    await page.evaluate(() => document.fonts.ready);
    assert.equal(await picker.getByTestId(/^appearance-palette-/).count(), 4);
    const previewColors = await Promise.all(
      palettes.map((palette) =>
        appearanceChoice(page, "palette", palette)
          .locator(`[data-palette="${palette}"]`)
          .evaluate((preview) => getComputedStyle(preview).backgroundColor),
      ),
    );
    assert.equal(new Set(previewColors).size, palettes.length);
    const swatchColors = await Promise.all(
      accents.map((accent) =>
        appearanceChoice(page, "accent", accent)
          .locator(`[data-accent="${accent}"]`)
          .evaluate((swatch) => getComputedStyle(swatch).backgroundColor),
      ),
    );
    assert.equal(new Set(swatchColors).size, accents.length);
    assert.ok(swatchColors.every((color) => color !== "rgba(0, 0, 0, 0)"));

    const clipped = await picker.getByRole("button").evaluateAll((buttons) =>
      buttons
        .filter((button) => {
          const { left, right } = button.getBoundingClientRect();
          return (
            left < 0 ||
            right > document.documentElement.clientWidth ||
            button.scrollWidth > button.clientWidth
          );
        })
        .map((button) => button.dataset.testid),
    );
    assert.deepEqual(clipped, []);
    await shot(picker, "appearance-narrow");
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("Terminal renders black canvases, white text and a bright green download bar", async () => {
  const { page, errors } = await open();
  try {
    await chooseAppearance(page, "palette", "TERMINAL");
    const list = page.getByTestId("appearance-list-sample");
    const { background, foreground } = await list.evaluate((element) => {
      const { backgroundColor, color } = getComputedStyle(element);
      return { background: backgroundColor, foreground: color };
    });
    assert.equal(await bodyBackground(page), "rgb(0, 0, 0)");
    assert.equal(background, "rgb(0, 0, 0)");
    const text = foreground.match(/^rgb\((\d+), (\d+), (\d+)\)$/);
    assert.ok(
      text?.slice(1).every((channel) => Number(channel) >= 235),
      foreground,
    );

    const fill = await list
      .getByRole("progressbar")
      .locator("div")
      .evaluate((element) => getComputedStyle(element).backgroundImage);
    const color = fill.match(/rgb\((\d+), (\d+), (\d+)\)/);
    assert.ok(color, fill);
    const [red, green, blue] = color.slice(1).map(Number);
    assert.ok(green >= 210 && red < 110 && blue < 110, fill);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the page text, accent and text on accent stay readable for each palette and accent", async () => {
  const { page, errors } = await open();
  try {
    for (const [palette, mode] of [
      ["WEAVER", "DARK"],
      ["WEAVER", "LIGHT"],
      ["PHOSPHOR", null],
      ["AMBER", null],
      ["TERMINAL", null],
    ]) {
      await chooseAppearance(page, "palette", palette);
      if (mode) await chooseAppearance(page, "mode", mode);
      for (const accent of ["default", ...accents]) {
        await chooseAppearance(page, "accent", accent);
        const ratios = await page.evaluate(() => {
          const sample = document.createElement("span");
          document.body.append(sample);
          const canvas = document.createElement("canvas");
          canvas.width = canvas.height = 1;
          const ctx = canvas.getContext("2d", { willReadFrequently: true });
          function luminance(color) {
            ctx.clearRect(0, 0, 1, 1);
            ctx.fillStyle = color;
            ctx.fillRect(0, 0, 1, 1);
            const channels = [...ctx.getImageData(0, 0, 1, 1).data].slice(0, 3);
            return channels.reduce((sum, channel, index) => {
              const srgb = channel / 255;
              const linear =
                srgb <= 0.04045
                  ? srgb / 12.92
                  : ((srgb + 0.055) / 1.055) ** 2.4;
              return sum + linear * [0.2126, 0.7152, 0.0722][index];
            }, 0);
          }
          const contrast = (a, b) =>
            (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
          const token = (name) => {
            sample.style.color = `var(${name})`;
            return luminance(getComputedStyle(sample).color);
          };
          const bodyStyle = getComputedStyle(document.body);
          const body = luminance(bodyStyle.backgroundColor);
          const accentColor = token("--color-wv-accent");
          const ratios = {
            text: contrast(luminance(bodyStyle.color), body),
            accent: contrast(accentColor, body),
            onAccent: contrast(token("--color-wv-on-accent"), accentColor),
          };
          sample.remove();
          return ratios;
        });
        const combination = [palette, mode, accent].filter(Boolean).join(" / ");
        for (const [part, ratio] of Object.entries(ratios)) {
          assert.ok(
            ratio >= 4.5,
            `${combination} ${part} contrast: ${ratio.toFixed(2)}`,
          );
        }
      }
    }
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the NZB limit defaults to ten and saves only its positive integer field", async () => {
  const { page, errors } = await open();
  try {
    const limit = page.getByRole("spinbutton", {
      name: "Maximum concurrent NZBs",
      exact: true,
    });
    await limit.waitFor();
    assert.equal(await limit.inputValue(), "10");
    assert.equal(await limit.getAttribute("min"), "1");
    assert.equal(await limit.getAttribute("step"), "1");
    await limit.fill("3");
    await limit.press("Enter");
    await sentCount(page, "UpdateSettings", 1);
    assert.deepEqual(await sent(page, "UpdateSettings"), [
      { input: { maxConcurrentNzbs: 3 } },
    ]);
    await page
      .locator("#status")
      .filter({ hasText: /^clean$/ })
      .waitFor();
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("a toggle, a select and a committed number each save on their own, and the top bar never turns dirty", async () => {
  const { page, errors } = await open();
  try {
    const downloads = page.getByRole("region", {
      name: "Downloads",
      exact: true,
    });
    const srrdb = downloads.getByRole("switch", {
      name: "SRRDB release lookup",
      exact: true,
    });
    const retries = downloads.getByRole("spinbutton", {
      name: "Article retries",
      exact: true,
    });
    const delay = downloads.getByRole("spinbutton", {
      name: "Propagation delay",
      exact: true,
    });
    await srrdb.waitFor();

    // A toggle saves as it changes, and only its own field.
    await srrdb.click();
    await sentCount(page, "UpdateSettings", 1);
    assert.deepEqual(await sent(page, "UpdateSettings"), [
      { input: { enableSrrdbLookup: true } },
    ]);
    await downloads.locator('[role="switch"][aria-checked="true"]').waitFor();

    // A number saves when Enter commits it, not while it is typed.
    await retries.fill("5");
    assert.equal((await sent(page, "UpdateSettings")).length, 1);
    await retries.press("Enter");
    await sentCount(page, "UpdateSettings", 2);
    assert.deepEqual((await sent(page, "UpdateSettings"))[1], {
      input: { maxRetries: 5 },
    });

    // Committing what is already stored sends nothing: the next save is the delay's.
    await retries.press("Enter");
    await retries.blur();
    await delay.fill("120");
    await delay.blur();
    await sentCount(page, "UpdateSettings", 3);
    assert.deepEqual((await sent(page, "UpdateSettings")).slice(2), [
      { input: { propagationDelaySecs: 120 } },
    ]);

    // A duplicate rule saves alone, as its one field of the policy.
    const duplicates = page.getByRole("region", {
      name: "Duplicates",
      exact: true,
    });
    await duplicates
      .getByRole("button", { name: "Overlapping article set", exact: true })
      .click();
    await page
      .getByRole("menu", { name: "Overlapping article set", exact: true })
      .getByRole("menuitemradio", { name: "Block", exact: true })
      .click();
    await sentCount(page, "UpdateSettings", 4);
    assert.deepEqual((await sent(page, "UpdateSettings"))[3], {
      input: { duplicatePolicy: { articleSet: "BLOCK" } },
    });
    await duplicates
      .getByRole("button", { name: "Overlapping article set", exact: true })
      .filter({ hasText: "Block" })
      .waitFor();

    // Each save went alone through its own mutation.
    assert.equal((await sent(page, "UpdateArchivePasswordSettings")).length, 0);
    assert.equal((await sent(page, "SetUnacceptableExtensions")).length, 0);
    await page
      .locator("#status")
      .filter({ hasText: /^clean$/ })
      .waitFor();
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("a refused save keeps what was typed or chosen and shows the daemon's reason", async () => {
  const { page, errors } = await open();
  try {
    const downloads = page.getByRole("region", {
      name: "Downloads",
      exact: true,
    });
    const extensions = downloads.getByRole("textbox", {
      name: "Unacceptable extensions",
      exact: true,
    });
    const status = page.locator("#status");
    await extensions.waitFor();

    await page.evaluate(() => {
      window.generalPanelFixture.refuse.SetUnacceptableExtensions =
        "the extension list was refused";
    });
    await extensions.fill("bat, iso");
    await extensions.press("Enter");
    await status
      .filter({ hasText: /^clean the extension list was refused$/ })
      .waitFor();
    assert.equal(await extensions.inputValue(), "bat, iso");
    assert.equal(
      await page.evaluate(() => window.generalPanelFixture.flags.at(-1).failed),
      true,
    );

    // Committed again once the daemon takes it, it is stored and the reason goes.
    await page.evaluate(() => {
      delete window.generalPanelFixture.refuse.SetUnacceptableExtensions;
    });
    await extensions.press("Enter");
    await sentCount(page, "SetUnacceptableExtensions", 2);
    await status.filter({ hasText: /^clean$/ }).waitFor();
    assert.deepEqual(await sent(page, "SetUnacceptableExtensions"), [
      { extensions: ["bat", "iso"] },
      { extensions: ["bat", "iso"] },
    ]);

    // A refused toggle keeps the choice in view beside the reason.
    const storage = page.getByRole("region", { name: "Storage", exact: true });
    const cleanup = storage.getByRole("switch", {
      name: "Clean up after unpacking",
      exact: true,
    });
    await page.evaluate(() => {
      window.generalPanelFixture.refuse.UpdateSettings =
        "settings were refused";
    });
    await cleanup.click();
    await status.filter({ hasText: /^clean settings were refused$/ }).waitFor();
    assert.equal(await cleanup.getAttribute("aria-checked"), "false");
    assert.deepEqual(await sent(page, "UpdateSettings"), [
      { input: { cleanupAfterExtract: false } },
    ]);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the password list stays shut until Edit passwords opens it, saves when left, and Clear removes the stored one once confirmed", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", {
      name: "Archive passwords",
      exact: true,
    });
    const edit = section.getByRole("button", {
      name: "Edit passwords",
      exact: true,
    });
    const list = section.getByRole("textbox", {
      name: "Passwords (one per line)",
      exact: true,
    });
    const clear = section.getByRole("button", { name: "Clear", exact: true });
    const file = section.getByRole("textbox", {
      name: "Password file",
      exact: true,
    });
    await edit.waitFor();
    assert.equal(await file.inputValue(), "/data/archive-passwords.txt");
    assert.equal(await list.count(), 0);
    await clear.waitFor();
    await shot(section, "archive-passwords");

    // Opened and left blank, it shuts again and keeps what is stored.
    await edit.click();
    await list.focus();
    await list.blur();
    await edit.waitFor();
    assert.equal(await list.count(), 0);

    // A typed list saves when the box is left, and the box shuts.
    await edit.click();
    await list.fill("first-secret\nsecond-secret");
    await list.blur();
    await edit.waitFor();
    assert.equal(await list.count(), 0);
    assert.deepEqual(await sent(page, "UpdateArchivePasswordSettings"), [
      { passwords: ["first-secret", "second-secret"] },
    ]);

    // Clear asks first; Cancel keeps the stored list and sends nothing.
    const confirm = page.getByRole("dialog", {
      name: "Clear stored passwords",
      exact: true,
    });
    await clear.click();
    await confirm.waitFor();
    await shot(confirm, "archive-passwords-clear");
    await confirm.getByRole("button", { name: "Cancel", exact: true }).click();
    await confirm.waitFor({ state: "detached" });
    assert.equal(await clear.isVisible(), true);
    assert.deepEqual(await sent(page, "UpdateArchivePasswordSettings"), [
      { passwords: ["first-secret", "second-secret"] },
    ]);

    // Confirmed, it removes the stored list.
    await clear.click();
    await confirm.getByRole("button", { name: "Clear", exact: true }).click();
    await clear.waitFor({ state: "detached" });
    assert.deepEqual(await sent(page, "UpdateArchivePasswordSettings"), [
      { passwords: ["first-secret", "second-secret"] },
      { passwords: [] },
    ]);
    assert.equal(await file.inputValue(), "/data/archive-passwords.txt");
    assert.equal(await list.count(), 0);
    // Nothing else was touched, so nothing else was sent.
    assert.equal((await sent(page, "SetUnacceptableExtensions")).length, 0);
    assert.equal((await sent(page, "UpdateSettings")).length, 0);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the unacceptable extension list is a Downloads row, and saves on its own as the daemon normalizes it", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", {
      name: "Downloads",
      exact: true,
    });
    const extensions = section.getByRole("textbox", {
      name: "Unacceptable extensions",
      exact: true,
    });
    await extensions.waitFor();
    assert.equal(
      await extensions.inputValue(),
      "bat, cmd, com, exe, js, lnk, msi, ps1, scr, vbs",
    );
    // The whole list shows: the box is wide enough for the default ten without scrolling.
    assert.equal(
      await extensions.evaluate(
        (input) => input.scrollWidth <= input.clientWidth,
      ),
      true,
    );
    await shot(section, "downloads");

    await extensions.fill("bat, cmd, EXE, iso");
    await extensions.blur();
    await section
      .getByRole("textbox", { name: "Unacceptable extensions", exact: true })
      .and(page.locator('[value="bat, cmd, exe, iso"]'))
      .waitFor();
    assert.deepEqual(await sent(page, "SetUnacceptableExtensions"), [
      { extensions: ["bat", "cmd", "EXE", "iso"] },
    ]);
    assert.equal((await sent(page, "UpdateSettings")).length, 0);
    await assertNeverDirty(page);
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

test("the hardware profile is a row like the others: its timing under the label, and no note in the band", async () => {
  const { page, errors } = await open();
  try {
    const section = page.getByRole("region", {
      name: "Performance",
      exact: true,
    });
    await section
      .getByRole("button", { name: "Hardware profile", exact: true })
      .waitFor();
    await section.getByText("Hardware profile", { exact: true }).waitFor();
    await section
      .getByText(
        "Changes apply to the next download or extraction that starts; work already running finishes with the settings it started with.",
        { exact: true },
      )
      .waitFor();
    assert.equal(
      await page
        .getByText("Choose your performance profile.", { exact: true })
        .count(),
      0,
    );
    await shot(section, "hardware-profile");
    assert.deepEqual(errors, []);
  } finally {
    await page.close();
  }
});

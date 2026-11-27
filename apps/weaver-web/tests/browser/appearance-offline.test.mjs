// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { extname, join } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { build } from "vite";

const { chromium } = await import(
  process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright"
);

const base = "/install/weaver/";
const mime = {
  ".css": "text/css",
  ".js": "text/javascript",
  ".svg": "image/svg+xml",
  ".webmanifest": "application/manifest+json",
  ".woff2": "font/woff2",
};

function appearanceAttributes({ palette, mode, accent }) {
  return `data-palette="${palette}" data-mode="${mode}" data-accent="${accent ?? ""}"`;
}

async function choose(page, testId) {
  const choice = page.getByTestId(testId);
  await choice.click();
  await choice.and(page.locator('[aria-pressed="true"]')).waitFor();
  await page
    .getByRole("group", { name: "Appearance", exact: true })
    .and(page.locator('[aria-busy="false"]'))
    .waitFor();
}

function cachedNavigation(page, root, marker) {
  return page.waitForFunction(
    async ({ root, marker }) => {
      const response = await caches.match(root);
      return (await response?.text())?.includes(marker);
    },
    { root, marker },
  );
}

let outDir, browser, server, root, appearance;
const mutations = [];

beforeEach(() => {
  appearance = { palette: "WEAVER", mode: "SYSTEM", accent: null };
  mutations.length = 0;
});

before(async () => {
  outDir = await mkdtemp(join(tmpdir(), "weaver-browser-build-"));
  await build({
    build: { outDir, reportCompressedSize: false },
    logLevel: "silent",
  });
  const index = await readFile(join(outDir, "index.html"), "utf8");
  server = createServer(async (request, response) => {
    const { pathname } = new URL(request.url, "http://fixture.invalid");
    if (!pathname.startsWith(base)) {
      response.writeHead(404).end();
      return;
    }
    const file = pathname.slice(base.length);
    const json = (value) =>
      response
        .writeHead(200, { "content-type": "application/json" })
        .end(JSON.stringify(value));
    if (file === "api/auth/status") {
      json({ enabled: false, setupRequired: false, appearance });
      return;
    }
    if (file === "graphql") {
      let body = "";
      for await (const chunk of request) body += chunk;
      const { query, variables } = JSON.parse(body);
      const [, operation] = query.match(/(?:query|mutation)\s+(\w+)/) ?? [];
      if (operation === "UpdateWebAppearance") {
        mutations.push(variables.input);
        appearance = { ...appearance, ...variables.input };
        json({ data: { updateWebAppearance: appearance } });
      } else if (operation === "Settings") {
        json({ data: { settings: { dataDir: "/data" } } });
      } else if (operation === "RailAppearanceAccess") {
        json({ data: { callerScope: "ADMIN" } });
      } else {
        json({
          errors: [{ message: "Not provided by the appearance fixture" }],
        });
      }
      return;
    }
    if (file === "server-page") {
      response
        .writeHead(403, { "content-type": "text/html" })
        .end("<!doctype html><html><title>Server page</title></html>");
      return;
    }
    if (file && file !== "index.html" && extname(file)) {
      try {
        const asset = await readFile(join(outDir, file));
        response
          .writeHead(200, {
            "content-type": mime[extname(file)] ?? "application/octet-stream",
          })
          .end(asset);
      } catch {
        response.writeHead(404).end();
      }
      return;
    }
    response
      .writeHead(200, {
        "content-type": "text/html",
        "cache-control": "no-store",
      })
      .end(
        index
          .replace("data-appearance-root", appearanceAttributes(appearance))
          .replace('<base href="/" />', `<base href="${base}" />`)
          .replace(
            "</head>",
            `<script>window.__WEAVER_BASE__ = "${base.slice(0, -1)}"</script></head>`,
          ),
      );
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  root = `http://127.0.0.1:${server.address().port}${base}`;
  browser = await chromium.launch({ headless: true });
});

after(async () => {
  await browser?.close();
  if (server) await new Promise((resolve) => server.close(resolve));
  if (outDir) await rm(outDir, { recursive: true, force: true });
});

test("the installed shell uses its last confirmed appearance offline and the daemon appearance online", async () => {
  const settings = new URL("settings/general", root).href;
  const context = await browser.newContext({ colorScheme: "dark" });
  try {
    const page = await context.newPage();
    page.setDefaultTimeout(0);
    const initial = await page.goto(settings);
    assert.ok(
      (await initial.text()).includes(appearanceAttributes(appearance)),
    );
    await page.waitForFunction(() => navigator.serviceWorker.controller);
    assert.equal(
      await page.evaluate(
        async () => (await navigator.serviceWorker.ready).scope,
      ),
      root,
    );

    const system = page.getByTestId("appearance-mode-SYSTEM");
    await system.waitFor();
    assert.equal(await system.getAttribute("aria-pressed"), "true");
    const darkBackground = await page.evaluate(
      () => getComputedStyle(document.body).backgroundColor,
    );
    await choose(page, "appearance-accent-ROSE");
    await choose(page, "appearance-mode-LIGHT");
    assert.deepEqual(mutations, [{ accent: "ROSE" }, { mode: "LIGHT" }]);
    const lightBackground = await page.evaluate(
      () => getComputedStyle(document.body).backgroundColor,
    );
    assert.notEqual(lightBackground, darkBackground);

    await context.setOffline(true);
    const offline = await page.reload();
    assert.equal(offline.fromServiceWorker(), true);
    assert.ok(
      (await offline.text()).includes(appearanceAttributes(appearance)),
    );
    await page.waitForFunction((background) => {
      return (
        getComputedStyle(document.body).backgroundColor === background &&
        document.querySelector('meta[name="theme-color"]').content ===
          background
      );
    }, lightBackground);

    appearance = { palette: "AMBER", mode: "SYSTEM", accent: "BLUE" };
    await context.setOffline(false);
    const fresh = await page.reload();
    assert.ok((await fresh.text()).includes(appearanceAttributes(appearance)));
    assert.equal(
      await page.locator("html").getAttribute("data-palette"),
      "AMBER",
    );

    await cachedNavigation(page, root, appearanceAttributes(appearance));
    await context.setOffline(true);
    const cachedFresh = await page.goto(settings);
    assert.equal(cachedFresh.fromServiceWorker(), true);
    assert.ok(
      (await cachedFresh.text()).includes(appearanceAttributes(appearance)),
    );
    assert.equal(
      await page.locator("html").getAttribute("data-palette"),
      "AMBER",
    );

    await context.setOffline(false);
    const notice = await page.goto(new URL("server-page", root).href);
    assert.equal(notice.status(), 403);
    assert.equal(await page.title(), "Server page");
    await cachedNavigation(page, root, "<title>Server page</title>");
    await context.setOffline(true);
    const cachedNotice = await page.reload();
    assert.equal(cachedNotice.fromServiceWorker(), true);
    assert.equal(cachedNotice.status(), 403);
    assert.equal(await page.title(), "Server page");
    assert.equal(await page.locator("html").getAttribute("data-palette"), null);
  } finally {
    await context.close();
  }
});

async function assertExternalLink(link, href) {
  await link.waitFor();
  assert.equal(await link.getAttribute("href"), href);
  assert.equal(await link.getAttribute("target"), "_blank");
  const rel = (await link.getAttribute("rel")).split(" ");
  assert.ok(rel.includes("noopener") && rel.includes("noreferrer"));
}

test("the rail shortcuts change appearance in place, share it with Settings, and work in the drawer", async () => {
  const context = await browser.newContext({
    viewport: { width: 1400, height: 900 },
  });
  try {
    const page = await context.newPage();
    page.setDefaultTimeout(0);
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto(root);
    const downloads = page.getByRole("heading", {
      name: "Downloads",
      exact: true,
    });
    const shortcuts = page
      .getByTestId("shell-rail-shortcuts")
      .filter({ visible: true });
    const theme = shortcuts.getByTestId("shell-rail-theme");
    const social = shortcuts.getByTestId("shell-rail-social");
    const sponsor = shortcuts.getByTestId("shell-rail-sponsor");
    const docs = shortcuts.getByTestId("shell-rail-docs");
    const themePanel = page
      .getByTestId("shell-rail-theme-popover")
      .filter({ visible: true });
    const socialMenu = page
      .getByTestId("shell-rail-social-menu")
      .filter({ visible: true });
    await downloads.waitFor();
    await shortcuts.waitFor();

    const [sponsorBox, themeBox, docsBox, socialBox] = await Promise.all(
      [sponsor, theme, docs, social].map((control) => control.boundingBox()),
    );
    assert.equal(sponsorBox.y, themeBox.y);
    assert.equal(docsBox.y, socialBox.y);
    assert.ok(sponsorBox.y < docsBox.y);
    assert.equal(sponsorBox.x, docsBox.x);
    assert.equal(themeBox.x, socialBox.x);

    await assertExternalLink(
      sponsor,
      "https://www.scryer.media/weaver/donate/",
    );
    await assertExternalLink(docs, "https://www.scryer.media/weaver/docs/");
    await social.hover();
    const reddit = socialMenu.getByTestId("shell-rail-reddit");
    const discord = socialMenu.getByTestId("shell-rail-discord");
    await assertExternalLink(reddit, "https://www.reddit.com/r/scryer_media/");
    await assertExternalLink(discord, "https://discord.gg/SQmtZTanqm");
    await social.click();
    await reddit.and(page.locator(":focus")).waitFor();
    await page.keyboard.press("ArrowDown");
    await discord.and(page.locator(":focus")).waitFor();
    await page.keyboard.press("Escape");
    await socialMenu.waitFor({ state: "hidden" });

    const downloadsUrl = page.url();
    await theme.click();
    await themePanel.waitFor();
    await choose(page, "rail-appearance-palette-TERMINAL");
    assert.equal(page.url(), downloadsUrl);
    assert.equal(await downloads.isVisible(), true);
    assert.equal(
      await page.locator("html").getAttribute("data-palette"),
      "TERMINAL",
    );
    await page.keyboard.press("Escape");
    await themePanel.waitFor({ state: "hidden" });

    await page.getByTestId("nav-settings").filter({ visible: true }).click();
    const settingsTerminal = page.getByTestId("appearance-palette-TERMINAL");
    await settingsTerminal.waitFor();
    assert.equal(await settingsTerminal.getAttribute("aria-pressed"), "true");
    await choose(page, "appearance-palette-AMBER");
    await page.getByTestId("nav-downloads").filter({ visible: true }).click();
    await downloads.waitFor();
    const drawerDownloadsUrl = page.url();
    await theme.click();
    const railAmber = themePanel.getByTestId("rail-appearance-palette-AMBER");
    await railAmber.waitFor();
    assert.equal(await railAmber.getAttribute("aria-pressed"), "true");
    await page.keyboard.press("Escape");
    await themePanel.waitFor({ state: "hidden" });

    await page.setViewportSize({ width: 390, height: 844 });
    await shortcuts.waitFor({ state: "hidden" });
    const openNav = page.getByTestId("shell-open-navigation");
    await openNav.click();
    await shortcuts.waitFor();
    assert.equal(await openNav.getAttribute("aria-expanded"), "true");
    await theme.click();
    await themePanel.waitFor();
    await choose(page, "rail-appearance-palette-WEAVER");
    const system = themePanel.getByTestId("rail-appearance-mode-SYSTEM");
    await system.waitFor();
    assert.equal(await system.getAttribute("aria-pressed"), "true");
    assert.equal(page.url(), drawerDownloadsUrl);
    await page.keyboard.press("Escape");
    await themePanel.waitFor({ state: "hidden" });
    assert.equal(await openNav.getAttribute("aria-expanded"), "true");
    assert.equal(
      await theme.evaluate((button) => button === document.activeElement),
      true,
    );
    await page.keyboard.press("Escape");
    await shortcuts.waitFor({ state: "hidden" });
    assert.equal(await openNav.getAttribute("aria-expanded"), "false");
    assert.deepEqual(mutations, [
      { palette: "TERMINAL" },
      { palette: "AMBER" },
      { palette: "WEAVER" },
    ]);
    assert.deepEqual(errors, []);
  } finally {
    await context.close();
  }
});

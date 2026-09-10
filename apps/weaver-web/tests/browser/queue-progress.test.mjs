import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
import { readFileSync } from "node:fs";

// Use an existing Playwright installation; this suite changes no application dependencies.
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server;
let browser;
let baseUrl;

before(async () => {
  server = await createServer({
    server: { host: "127.0.0.1", port: 0 },
    plugins: process.env.QUEUE_PROGRESS_BASELINE ? [{
      name: "queue-progress-baseline",
      enforce: "pre",
      load(id) {
        if (id.endsWith("/src/pages/JobList.tsx")) {
          return readFileSync(process.env.QUEUE_PROGRESS_BASELINE, "utf8");
        }
      },
    }] : [],
  });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});

after(async () => {
  await browser?.close();
  await server?.close();
});

async function openQueue(width = 1700) {
  const page = await browser.newPage({ viewport: { width, height: 1000 } });
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => {
    const url = new URL(route.request().url());
    return url.origin === baseUrl && !url.pathname.startsWith("/graphql")
      ? route.continue() : route.abort();
  });
  await page.goto(`${baseUrl}/tests/browser/queue-progress.html`);
  await page.waitForFunction(() => window.queueFixture?.ready());
  await page.getByRole("link", { name: "Moving fixture", exact: true }).waitFor();
  await page.evaluate(() => document.fonts.ready);
  return page;
}

function row(page, name) {
  return page.getByRole("row").filter({ has: page.getByRole("link", { name, exact: true }) });
}

test("moving and downloading progress both survive a burst before a render", async () => {
  const page = await openQueue();
  try {
    for (const percent of [15, 35, 65]) {
      await page.evaluate((value) => window.queueFixture.burst(value, value + 1, 75_300_000), percent);
      await page.waitForFunction((value) => {
        const link = [...document.querySelectorAll("a")].find((node) => node.textContent === "Moving fixture");
        return link?.closest("tr")?.querySelector('[role="progressbar"]')?.getAttribute("aria-valuenow") === String(value);
      }, percent, { timeout: 2000 });
      await page.waitForFunction((value) => {
        const link = [...document.querySelectorAll("a")].find((node) => node.textContent === "Download fixture");
        return link?.closest("tr")?.querySelector('[role="progressbar"]')?.getAttribute("aria-valuenow") === String(value + 1);
      }, percent, { timeout: 2000 });
    }
  } finally {
    await page.close();
  }
});

test("progress tracks and columns stay fixed across percentage and rate ticks", async () => {
  for (const width of [1280, 1360, 1440, 1500, 1600, 1620, 1700, 2000]) {
    const page = await openQueue(width);
    try {
      assert.equal(await page.getByRole("table").evaluate((table) => {
        const wrapper = table.parentElement;
        return wrapper.scrollWidth <= wrapper.clientWidth;
      }), true, `queue must fit without horizontal scrolling at viewport ${width}`);
      const initial = await row(page, "Download fixture").getByRole("progressbar").boundingBox();
      for (const [percent, rate] of [[9, 999_000], [10, 10_000_000], [99, 999_000_000], [100, 0]]) {
        await page.evaluate(([value, speed]) => window.queueFixture.download(value, speed), [percent, rate]);
        await page.waitForFunction((value) => {
          const link = [...document.querySelectorAll("a")].find((node) => node.textContent === "Download fixture");
          return link?.closest("tr")?.querySelector('[role="progressbar"]')?.getAttribute("aria-valuenow") === String(value);
        }, percent, { timeout: 2000 });
        const actual = await row(page, "Download fixture").getByRole("progressbar").boundingBox();
        assert.equal(actual.x, initial.x, `track moved at viewport ${width}, progress ${percent}`);
        assert.equal(actual.width, initial.width, `track resized at viewport ${width}, progress ${percent}`);
      }
    } finally {
      await page.close();
    }
  }
});

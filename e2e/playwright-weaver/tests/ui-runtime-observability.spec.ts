import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { WebSocketRoute } from "@playwright/test";
import { expect, openNavigation, test, weaverRoute } from "./helpers";
import { introspectPublicMutationNames } from "./support/runtime-introspection";
import { seedRuntimeHistory } from "./support/setup/runtime-history";

type CoverageEntry = {
  name?: string;
  path?: string;
  owner: string;
  oracle: "browser" | "api-metrics" | "existing-pipeline" | "unit-only";
  rationale?: string;
};

type CoverageLedger = {
  version: number;
  routes: CoverageEntry[];
  mutations: CoverageEntry[];
  behaviors: CoverageEntry[];
};

const coverageLedger = JSON.parse(
  readFileSync(resolve(process.cwd(), "coverage-ledger.v1.json"), "utf8"),
) as CoverageLedger;

const staticBrowserRoutes = coverageLedger.routes
  .filter(({ oracle, path }) => oracle === "browser" && path && !path.includes(":"))
  .map(({ path }) => path as string);
const configuredBasePath = new URL(
  process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090/",
).pathname.replace(/\/+$/, "");

test("all user-facing routes render, refresh, and retain live connectivity", async ({ cleanPage: page }) => {
  for (const route of staticBrowserRoutes) {
    await test.step(route, async () => {
      const response = await page.goto(weaverRoute(route));
      expect(response?.ok(), `${route} returned ${response?.status()}`).toBeTruthy();
      const main = page.getByRole("main");
      await expect(main).not.toContainText(/application error|failed to fetch/i);
      await expect(main).toBeVisible();
      await page.reload();
      await expect(main).toBeVisible();
    });
  }
});

test("visible navigation routes are assigned to the Weaver coverage ledger", async ({ cleanPage: page }) => {
  await page.goto(weaverRoute("/"));
  await expect(page.getByRole("main")).toBeVisible();

  const navigation = await openNavigation(page);
  const navigationLinks = await navigation.getByRole("link").all();
  const navigationHrefs = await Promise.all(
    navigationLinks.map((link) => link.getAttribute("href")),
  );
  const currentUrl = new URL(page.url());
  const visibleNavigationPaths = Array.from(
    new Set(
      navigationHrefs
        .filter((href): href is string => href != null)
        .map((href) => new URL(href, currentUrl))
        .filter((url) => url.origin === currentUrl.origin)
        .map((url) => {
          const applicationPath = url.pathname.startsWith(configuredBasePath)
            ? url.pathname.slice(configuredBasePath.length)
            : url.pathname;
          return applicationPath.replace(/\/+$/, "") || "/";
        }),
    ),
  ).sort();

  expect(visibleNavigationPaths, "Weaver rendered no visible navigation links").not.toEqual([]);
  const ownedPaths = new Set(coverageLedger.routes.map(({ path }) => path));
  const missingOwners = visibleNavigationPaths.filter((path) => !ownedPaths.has(path));
  expect(
    missingOwners,
    `visible Weaver routes missing from coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
});

test("monitoring metrics and live logs expose real product state and controls", async ({ cleanPage: page }) => {
  await page.goto(weaverRoute("/monitoring"));
  await expect(page.getByRole("heading", { name: "Monitoring", exact: true })).toBeVisible();
  await expect(page.getByText("Pipeline", { exact: true })).toBeVisible();
  await expect(page.getByText("Connections", { exact: true })).toBeVisible();
  const main = page.getByRole("main");
  await expect(main.getByText("Queue depths", { exact: true })).toBeVisible();
  // The provider list reads live server health, so the configured provider
  // shows up by host.
  await expect(main.getByText("nntp", { exact: true })).toBeVisible();

  await page.goto(weaverRoute("/logs"));
  await expect(page.getByRole("heading", { name: "Logs", exact: true })).toBeVisible();
  const following = page.getByText("following new lines", { exact: true });
  await expect(following).toBeVisible();
  await page.getByRole("banner").getByRole("button", { name: "Pause tail", exact: true }).click();
  await expect(page.getByText("paused — scroll freely", { exact: true })).toBeVisible();
  await page.getByRole("banner").getByRole("button", { name: "Resume tail", exact: true }).click();
  await expect(following).toBeVisible();
});

test("history pagination renders metadata-only seeded records", async ({ cleanPage: page, request }) => {
  const suffix = configuredBasePath.replaceAll("/", "-") || "root";
  const markers = await seedRuntimeHistory(request, `e2e-runtime-history-${suffix}`);
  await page.goto(weaverRoute("/history"));
  const main = page.getByRole("main");
  const historyRow = (name: string) =>
    main.getByRole("button").filter({ has: page.getByText(name, { exact: true }) });
  await expect(historyRow(markers.newest)).toBeVisible();
  await page
    .getByRole("radiogroup", { name: "Rows per page", exact: true })
    .getByRole("radio", { name: "25", exact: true })
    .click();
  await expect(historyRow(markers.oldest)).toHaveCount(0);
  const next = page.getByRole("button", { name: "Next", exact: true });
  await expect(next).toBeEnabled();
  await next.click();
  await expect(historyRow(markers.oldest)).toBeVisible();
});

test("subscription loss polls and reconnects without losing the visible application", async ({ cleanPage: page }) => {
  let blockReconnects = false;
  let disconnected = false;
  let pollingRequests = 0;
  let socketConnections = 0;
  let liveBrowserSocket: WebSocketRoute | undefined;
  let liveServerSocket: WebSocketRoute | undefined;
  let blockedReconnectSocket: WebSocketRoute | undefined;

  page.on("request", (request) => {
    if (
      disconnected
      && request.method() === "POST"
      && request.url().includes("/graphql")
      && /LiveMetrics/.test(request.postData() ?? "")
    ) {
      pollingRequests += 1;
    }
  });
  await page.routeWebSocket(/\/graphql\/ws(?:\?|$)/, (socket) => {
    if (blockReconnects) {
      // Return so Playwright can still observe the HTTP polling fallback.
      blockedReconnectSocket = socket;
      return;
    }
    liveBrowserSocket = socket;
    liveServerSocket = socket.connectToServer();
    socketConnections += 1;
  });

  await page.goto(weaverRoute("/"));
  await expect(page.getByRole("main")).toBeVisible();
  const status = page.getByRole("contentinfo");
  await expect(status.getByText("Connected", { exact: true })).toBeVisible();
  await expect.poll(() => liveBrowserSocket).toBeTruthy();
  await expect.poll(() => liveServerSocket).toBeTruthy();
  expect(socketConnections).toBe(1);

  blockReconnects = true;
  disconnected = true;
  try {
    await liveServerSocket!.close({ code: 1012, reason: "Weaver e2e subscription interruption" });
    await liveBrowserSocket!.close({ code: 1012, reason: "Weaver e2e subscription interruption" });
    await expect(status.getByText(/^Reconnecting/)).toBeVisible();
    await expect.poll(() => pollingRequests, { timeout: 20_000 }).toBeGreaterThan(0);
    await expect(status.getByText("Reconnecting — polling", { exact: true })).toBeVisible();
    await expect(page.getByRole("main")).toBeVisible();
  } finally {
    blockReconnects = false;
    await blockedReconnectSocket?.close({
      code: 1012,
      reason: "Weaver e2e reconnect interruption released",
    });
  }
  await expect.poll(() => socketConnections, { timeout: 20_000 }).toBeGreaterThan(1);
  await expect(status.getByText("Connected", { exact: true })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("main")).toBeVisible();
});

test("every public GraphQL mutation has a release-gate owner", async ({ request }) => {
  const schemaMutations = await introspectPublicMutationNames(request);
  const ledgerMutations = coverageLedger.mutations
    .map(({ name }) => name)
    .filter((name): name is string => Boolean(name))
    .sort();

  expect(schemaMutations, "Weaver exposes no GraphQL mutation fields").not.toEqual([]);
  expect(
    schemaMutations.filter((name) => !ledgerMutations.includes(name)),
    `public mutations missing from coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
  expect(
    ledgerMutations.filter((name) => !schemaMutations.includes(name)),
    `stale mutation entries in coverage-ledger.v${coverageLedger.version}.json`,
  ).toEqual([]);
});

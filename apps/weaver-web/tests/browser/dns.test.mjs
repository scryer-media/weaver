// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-dns", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });

async function open(query = "", viewport = { width: 1400, height: 1200 }) {
  const page = await browser.newPage({ viewport });
  page.setDefaultTimeout(0);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/dns.html${query}`);
  return { page, errors };
}
/** With `DNS_SCREENSHOT_DIR` set to a directory, the page as it stands is saved there as `name`.png. */
async function shot(page, name) {
  if (process.env.DNS_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.DNS_SCREENSHOT_DIR, `${name}.png`) });
}
const daemon = (page) => page.evaluate(() => structuredClone({ requests: window.dnsFixture.requests, routeSaves: window.dnsFixture.routeSaves }));
const sent = async (page, name) => (await daemon(page)).requests.filter((request) => request.name === name);
const action = (scope, name) => scope.getByRole("button", { name, exact: true });
const option = (scope, name) => scope.getByRole("radio", { name, exact: true });
const dialog = (page, name) => page.getByRole("dialog", { name, exact: true });
async function choose(page, scope, testId, name) {
  await scope.getByTestId(testId).click();
  await page.getByRole("menuitemradio", { name, exact: true }).click();
}
/** Starts a capability test through the drafted route the picker opens on. */
async function startTest(page, editor) {
  await editor.getByTestId("action-dnsTestCapabilities").click();
  const picker = dialog(page, "Test DNS servers");
  await picker.getByTestId("dns-test-run").click();
  await picker.waitFor({ state: "detached" });
}
async function editingList(page, id, name) {
  await page.getByTestId(`row-dnsLists-${id}`).click();
  const editor = dialog(page, name);
  await editor.waitFor();
  return editor;
}
const VALIDATE_NOTE = "Weaver checks the DNSSEC signatures itself. Unsigned names still resolve; names that fail the check do not.";
const PROXY_VALIDATE_NOTE = "Weaver checks the DNSSEC signatures itself. Only a WireGuard proxy's tunnel servers can be checked.";

test("the DNS page lists profiles, System built in and first, and the server lists they use", async () => {
  const { page, errors } = await open();
  const system = page.getByTestId("row-dnsProfiles-0");
  await system.waitFor();
  // copy-is-claim: the built-in profile is named and marked as such.
  await system.getByText("Built in", { exact: true }).waitFor();
  // copy-is-claim: each profile reads in one line, a list by its name and wires.
  await page.getByTestId("row-dnsProfiles-3").getByText("Weaver · Mixed Plain TLS · Via the proxy · Validate here", { exact: true }).waitFor();
  await page.getByTestId("row-dnsProfiles-2").getByText("The proxy · As sent", { exact: true }).waitFor();
  await page.getByTestId("row-dnsProfiles-4").getByText("The proxy · As sent · System where no proxy is used", { exact: true }).waitFor();
  await system.getByText("3 routes", { exact: true }).waitFor();
  await page.getByTestId("row-dnsLists-1").getByText("2 servers", { exact: true }).waitFor();
  await page.getByTestId("row-dnsLists-1").getByText("Checked", { exact: true }).waitFor();
  // The built-in profile does not open: there is nothing in it to change.
  await system.click();
  await page.getByTestId("row-dnsProfiles-3").click();
  await dialog(page, "Checked").waitFor();
  assert.equal(await dialog(page, "System").count(), 0);
  await shot(page, "dns-page");
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a profile's editor shows who answers first and only what that who allows", async () => {
  const { page, errors } = await open();
  await page.getByTestId("row-dnsProfiles-3").click();
  const editor = dialog(page, "Checked");
  assert.equal(await option(editor, "Weaver").getAttribute("aria-checked"), "true");
  // Asking the system on a leg with no proxy is a choice for the proxy alone.
  const fallback = editor.getByTestId("field-dnsSystemFallback");
  assert.equal(await fallback.count(), 0);
  // Weaver's answers are taken as sent or validated here, and nothing else.
  const trust = editor.getByTestId("field-dnsTrust");
  assert.deepEqual(await trust.getByRole("radio").allTextContents(), ["As sent", "Validate here"]);
  assert.equal(await option(editor, "Validate here").getAttribute("aria-checked"), "true");
  // copy-is-claim: validating here says unsigned names still resolve and failing ones do not.
  assert.equal(await editor.getByTestId("help-dnsTrust").textContent(), VALIDATE_NOTE);
  await option(editor, "As sent").click();
  await editor.getByTestId("help-dnsTrust").waitFor({ state: "detached" });
  await choose(page, editor, "field-dnsList", "Encrypted");
  // The proxy answering takes no list and no transit, and the same two trusts.
  await option(editor, "The proxy").click();
  assert.equal(await editor.getByTestId("field-dnsList").count(), 0);
  assert.equal(await editor.getByTestId("field-dnsTransit").count(), 0);
  assert.deepEqual(await trust.getByRole("radio").allTextContents(), ["As sent", "Validate here"]);
  assert.equal(await option(editor, "As sent").getAttribute("aria-checked"), "true");
  await fallback.waitFor();
  assert.equal(await fallback.getAttribute("aria-checked"), "false");
  // copy-is-claim: off, a route with a direct leg or fallback cannot take the profile.
  assert.equal(await editor.getByTestId("help-dnsSystemFallback").textContent(), "Off: a route with a direct leg or a direct fallback cannot use this profile.");
  await fallback.click();
  assert.equal(await fallback.getAttribute("aria-checked"), "true");
  await option(editor, "Validate here").click();
  // copy-is-claim: validating a proxy's answers needs a WireGuard tunnel's own servers.
  assert.equal(await editor.getByTestId("help-dnsTrust").textContent(), PROXY_VALIDATE_NOTE);
  // The system takes answers as sent: there is no trust to choose.
  await option(editor, "The system").click();
  assert.equal(await editor.getByTestId("field-dnsTrust").count(), 0);
  assert.equal(await fallback.count(), 0);
  await option(editor, "The proxy").click();
  await option(editor, "Validate here").click();
  await action(editor, "Save profile").click();
  await editor.waitFor({ state: "detached" });
  const [save] = await sent(page, "SaveDnsProfile");
  assert.deepEqual(save.variables.input, { id: 3, kind: "ROUTE", name: "Checked", who: "PROXY", transit: null, trust: "VALIDATE", serverListId: null, systemFallback: true });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a new profile makes a new list without losing its draft", async () => {
  const { page, errors } = await open();
  await page.getByTestId("add-dnsProfiles").click();
  const editor = dialog(page, "Add route profile");
  await editor.getByTestId("field-dnsProfileName").fill("Quad9 checked");
  await option(editor, "Via the egress").click();
  await editor.getByTestId("field-dnsListNew").click();
  const list = dialog(page, "Add server list");
  await list.getByTestId("field-dnsListName").fill("Quad9");
  await list.getByTestId("field-dnsServer-0-wire").click();
  await page.getByRole("menuitemradio", { name: "DNS over TLS", exact: true }).click();
  await list.getByTestId("field-dnsServer-0-address").fill("192.0.2.9");
  await action(list, "Save list").click();
  await list.waitFor({ state: "detached" });
  // Back in the profile, the draft stands and the new list is chosen.
  assert.equal(await editor.getByTestId("field-dnsProfileName").inputValue(), "Quad9 checked");
  assert.equal(await option(editor, "Via the egress").getAttribute("aria-checked"), "true");
  await editor.getByTestId("field-dnsList").getByText("Quad9", { exact: true }).waitFor();
  await action(editor, "Save profile").click();
  await editor.waitFor({ state: "detached" });
  const [listSave] = await sent(page, "SaveDnsServerList");
  assert.deepEqual(listSave.variables.input, { name: "Quad9", servers: [{ wire: "TLS", address: "192.0.2.9", tlsName: null, url: null }] });
  const [profileSave] = await sent(page, "SaveDnsProfile");
  assert.deepEqual(profileSave.variables.input, { kind: "ROUTE", name: "Quad9 checked", who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: 3, systemFallback: false });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the DNS page has server lists, then proxy name profiles, then route profiles, System first in both", async () => {
  const { page, errors } = await open();
  await page.getByTestId("row-dnsProfiles-0").waitFor();
  const order = await page.evaluate(() =>
    [...document.querySelectorAll("[data-testid^='section-dns']")].map((section) => section.getAttribute("data-testid")),
  );
  assert.deepEqual(order, ["section-dnsLists", "section-dnsNameProfiles", "section-dnsProfiles"]);
  const names = page.getByTestId("section-dnsNameProfiles");
  const routes = page.getByTestId("section-dnsProfiles");
  // Each section holds System and its own kind alone.
  assert.deepEqual(await names.locator("[data-testid^='row-dnsNameProfiles-']").evaluateAll((rows) => rows.map((row) => row.getAttribute("data-testid"))), ["row-dnsNameProfiles-0", "row-dnsNameProfiles-6"]);
  assert.equal(await routes.getByTestId("row-dnsProfiles-6").count(), 0);
  assert.equal(await routes.locator("[data-testid^='row-dnsProfiles-']").first().getAttribute("data-testid"), "row-dnsProfiles-0");
  // copy-is-claim: a proxy name profile counts the proxies it finds, and its line names no transit.
  await names.getByTestId("row-dnsNameProfiles-6").getByText("1 proxy", { exact: true }).waitFor();
  await names.getByTestId("row-dnsNameProfiles-6").getByText("Weaver · Mixed Plain TLS · As sent", { exact: true }).waitFor();
  // copy-is-claim: the built-in profile is the host where a proxy is found, and the system for a route.
  // The name cell also holds the Built in mark, so the name is read from its title.
  await names.getByTestId("row-dnsNameProfiles-0").getByTitle("Host", { exact: true }).waitFor();
  await names.getByTestId("row-dnsNameProfiles-0").getByText("The host", { exact: true }).first().waitFor();
  await routes.getByTestId("row-dnsProfiles-0").getByTitle("System", { exact: true }).waitFor();
  await routes.getByTestId("row-dnsProfiles-0").getByText("The system", { exact: true }).first().waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a proxy name profile has no proxy answer, no transit and no fallback, and a new one keeps its draft through a new list", async () => {
  const { page, errors } = await open();
  await page.getByTestId("row-dnsNameProfiles-6").click();
  const stored = dialog(page, "Proxy names");
  // copy-is-claim: finding a proxy, the system is the host, and its help says who that is.
  assert.deepEqual(await stored.getByTestId("field-dnsWho").getByRole("radio").allTextContents(), ["The host", "Weaver"]);
  await option(stored, "The host").click();
  assert.equal(await stored.getByTestId("help-dnsWho").textContent(), "The host looks this proxy up. Inside a chain, the proxy that carries it does.");
  assert.equal(await stored.getByTestId("field-dnsTransit").count(), 0);
  assert.equal(await stored.getByTestId("field-dnsSystemFallback").count(), 0);
  await stored.getByTestId("editor-cancel").click();
  await stored.waitFor({ state: "detached" });
  await page.getByTestId("add-dnsNameProfiles").click();
  const editor = dialog(page, "Add proxy name profile");
  await editor.getByTestId("field-dnsProfileName").fill("Names checked");
  await option(editor, "Validate here").click();
  await editor.getByTestId("field-dnsListNew").click();
  const list = dialog(page, "Add server list");
  await list.getByTestId("field-dnsListName").fill("Names");
  await list.getByTestId("field-dnsServer-0-address").fill("192.0.2.10");
  await action(list, "Save list").click();
  await list.waitFor({ state: "detached" });
  assert.equal(await editor.getByTestId("field-dnsProfileName").inputValue(), "Names checked");
  assert.equal(await option(editor, "Validate here").getAttribute("aria-checked"), "true");
  await editor.getByTestId("field-dnsList").getByText("Names", { exact: true }).waitFor();
  assert.equal(await editor.getByTestId("field-dnsTransit").count(), 0);
  await action(editor, "Save profile").click();
  await editor.waitFor({ state: "detached" });
  const [profileSave] = await sent(page, "SaveDnsProfile");
  assert.deepEqual(profileSave.variables.input, { kind: "PROXY_NAME", name: "Names checked", who: "WEAVER", transit: "EGRESS", trust: "VALIDATE", serverListId: 3, systemFallback: false });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a profile proxies still use is refused deletion, and the refusal names those proxies", async () => {
  const { page, errors } = await open();
  await page.getByTestId("row-dnsNameProfiles-6").click();
  const editor = dialog(page, "Proxy names");
  await action(editor, "Delete profile").click();
  const confirm = dialog(page, "Delete profile");
  await action(confirm, "Delete profile").click();
  await confirm.waitFor({ state: "detached" });
  // copy-is-claim: each proxy is named; one with no name found stands as its number.
  await editor.getByText("These proxies still use this profile to find their address: Relay, Proxy #99. Choose another profile for them first.", { exact: true }).waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a list a profile still uses is refused deletion, and says which profiles", async () => {
  const { page, errors } = await open();
  const editor = await editingList(page, 1, "Mixed");
  await action(editor, "Delete list").click();
  const confirm = dialog(page, "Delete list");
  await action(confirm, "Delete list").click();
  await confirm.waitFor({ state: "detached" });
  // copy-is-claim: the refusal names the profiles, joined.
  await editor.getByText("These DNS profiles still use this list: Checked.", { exact: true }).waitFor();
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a profile routes still follow is refused deletion, and the refusal names those routes", async () => {
  const { page, errors } = await open();
  await page.getByTestId("row-dnsProfiles-3").click();
  const editor = dialog(page, "Checked");
  await action(editor, "Delete profile").click();
  const confirm = dialog(page, "Delete profile");
  await action(confirm, "Delete profile").click();
  await confirm.waitFor({ state: "detached" });
  // copy-is-claim: each route is named as the routes page names it; one with no name found is a deleted route, never its key.
  await editor.getByText("These routes still follow this profile: news.fixture.invalid, Fixture feed, a deleted route. Choose another profile for them first.", { exact: true }).waitFor();
  assert.equal((await sent(page, "RouteNames")).length, 1);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a test the daemon refuses says why under the server it names, and shows no table", async () => {
  const { page, errors } = await open();
  const editor = await editingList(page, 1, "Mixed");
  // The blank first row is not sent, so the refused server is the first sent and the form's second row.
  await editor.getByTestId("field-dnsServer-0-address").fill("");
  await editor.getByTestId("field-dnsServer-1-address").fill("192.0.2.99");
  await startTest(page, editor);
  await page.waitForFunction(() => window.dnsFixture.testHeld());
  await page.evaluate(() => window.dnsFixture.release());
  await editor.getByTestId("help-dnsServer-1").waitFor();
  // copy-is-claim: the refusal reads in our words for its code.
  assert.equal(await editor.getByTestId("help-dnsServer-1").textContent(), "DNS servers must be IP addresses.");
  assert.equal(await editor.getByTestId("help-dnsServer-0").count(), 0);
  assert.equal(await editor.getByTestId("table-dnsCapabilities").count(), 0);
  assert.equal(await editor.getByTestId("action-dnsTestCapabilities").isDisabled(), false);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("an earlier run's answer never lands on a later run, refused or not", async () => {
  const { page, errors } = await open();
  const editor = await editingList(page, 1, "Mixed");
  const table = editor.getByTestId("table-dnsCapabilities");
  await editor.getByTestId("field-dnsServer-0-address").fill("192.0.2.99");
  await startTest(page, editor);
  await table.waitFor();
  // Editing a row while the test is in flight drops its table and frees the button.
  await editor.getByTestId("field-dnsServer-0-address").fill("192.0.2.53");
  await table.waitFor({ state: "detached" });
  assert.equal(await editor.getByTestId("action-dnsTestCapabilities").isDisabled(), false);
  await startTest(page, editor);
  await page.waitForFunction(() => window.dnsFixture.testHeld(2));
  // The first run is refused, and its answer comes first: the second run keeps its table and shows its results.
  await page.evaluate(() => window.dnsFixture.release(0));
  await page.evaluate(() => window.dnsFixture.release(0));
  await editor.locator("[data-testid='result-dnsServer-0-plain'][data-supported='true']").waitFor();
  assert.equal(await table.getAttribute("aria-busy"), "false");
  assert.equal(await editor.getByText("DNS servers must be IP addresses.", { exact: true }).count(), 0);
  const runs = await sent(page, "TestDnsServers");
  assert.deepEqual(runs.map((run) => run.variables.servers[0].address), ["192.0.2.99", "192.0.2.53"]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a blank row before a tested one is not sent, and each result lands on its own form row", async () => {
  const { page, errors } = await open();
  const editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("field-dnsServer-0-address").fill("");
  await startTest(page, editor);
  await page.waitForFunction(() => window.dnsFixture.testHeld());
  await page.evaluate(() => window.dnsFixture.release());
  // The daemon answers for its first server; that is the form's second row, set to TLS.
  const tls = editor.locator("[data-testid='result-dnsServer-1-plain'][data-supported='true']");
  await tls.waitFor();
  assert.equal(await editor.getByTestId("result-dnsServer-1-tls").getAttribute("data-own"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-0-plain").count(), 0);
  const [run] = await sent(page, "TestDnsServers");
  assert.deepEqual(run.variables.servers.map((server) => server.address), ["192.0.2.54"]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("closing the editor while a test is in flight drops it", async () => {
  const { page, errors } = await open();
  let editor = await editingList(page, 1, "Mixed");
  await startTest(page, editor);
  await editor.getByTestId("table-dnsCapabilities").waitFor();
  await action(editor, "Cancel").click();
  await editor.waitFor({ state: "detached" });
  await page.evaluate(() => window.dnsFixture.release());
  editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("field-dnsServer-0-address").waitFor();
  assert.equal(await editor.getByTestId("table-dnsCapabilities").count(), 0);
  assert.equal(await editor.getByTestId("action-dnsTestCapabilities").isDisabled(), false);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("testing a list's servers asks through a drafted route and shows a table that clears on any edit", async () => {
  const { page, errors } = await open();
  const editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("action-dnsTestCapabilities").click();
  const picker = dialog(page, "Test DNS servers");
  await picker.getByTestId("dns-test-run").click();
  await picker.waitFor({ state: "detached" });
  // In flight: the table is there, its cells pending, and the button waits.
  const table = editor.getByTestId("table-dnsCapabilities");
  await table.waitFor();
  assert.equal(await table.getAttribute("aria-busy"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-0-plain").getAttribute("data-supported"), null);
  assert.equal(await editor.getByTestId("action-dnsTestCapabilities").isDisabled(), true);
  await page.waitForFunction(() => window.dnsFixture.testHeld());
  await page.evaluate(() => window.dnsFixture.release());
  const tls = editor.getByTestId("result-dnsServer-1-tls");
  await editor.locator("[data-testid='result-dnsServer-1-tls'][data-supported='false']").waitFor();
  assert.equal(await table.getAttribute("aria-busy"), "false");
  // The second row is set to TLS: that cell is its own wire, and it failed.
  assert.equal(await tls.getAttribute("data-own"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-0-plain").getAttribute("data-own"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-0-plain").getAttribute("data-supported"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-1-https").getAttribute("data-supported"), "true");
  // DNSSEC is green where the signatures validated.
  const dnssec = editor.getByTestId("result-dnsServer-0-dnssec");
  assert.equal(await dnssec.getAttribute("data-supported"), "true");
  assert.equal(await editor.getByTestId("result-dnsServer-1-dnssec").getAttribute("data-supported"), "false");
  // copy-is-claim: a failure explains itself in the lookup-error wording, and DNSSEC says what it found.
  assert.equal(await tls.getAttribute("title"), "secure connection to the DNS server failed");
  assert.equal(await dnssec.getAttribute("title"), "14 ms · Signatures validated");
  assert.equal(await editor.getByTestId("result-dnsServer-1-dnssec").getAttribute("title"), "no DNSSEC records to check");
  // copy-is-claim: the caption names the route the test went through.
  await editor.getByText("Tested through System → Direct", { exact: true }).waitFor();
  const [run] = await sent(page, "TestDnsServers");
  assert.deepEqual(run.variables, {
    route: { failover: "REDISTRIBUTE", legs: [{ egressId: 0, weight: 100, path: { direct: true } }] },
    servers: [
      { wire: "PLAIN", address: "192.0.2.53", tlsName: null, url: null },
      { wire: "TLS", address: "192.0.2.54", tlsName: "dns.fixture.invalid", url: null },
    ],
  });
  await shot(page, "dns-capabilities");
  // Any edit to the rows drops the results.
  await editor.getByTestId("field-dnsServer-1-address").fill("192.0.2.56");
  await table.waitFor({ state: "detached" });
  // A save carries the list alone, never the results.
  await action(editor, "Save list").click();
  await editor.waitFor({ state: "detached" });
  const [save] = await sent(page, "SaveDnsServerList");
  assert.deepEqual(Object.keys(save.variables.input).sort(), ["id", "name", "servers"]);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("closing the editor drops a test's results", async () => {
  const { page, errors } = await open();
  let editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("action-dnsTestCapabilities").click();
  await dialog(page, "Test DNS servers").getByTestId("dns-test-run").click();
  await page.waitForFunction(() => window.dnsFixture.testHeld());
  await page.evaluate(() => window.dnsFixture.release());
  await editor.locator("[data-testid='result-dnsServer-0-plain'][data-supported='true']").waitFor();
  await action(editor, "Cancel").click();
  await editor.waitFor({ state: "detached" });
  editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("field-dnsServer-0-address").waitFor();
  assert.equal(await editor.getByTestId("table-dnsCapabilities").count(), 0);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("at phone width the results table scrolls in its own box and the page does not", async () => {
  const { page, errors } = await open("", { width: 375, height: 812 });
  const editor = await editingList(page, 1, "Mixed");
  await editor.getByTestId("action-dnsTestCapabilities").click();
  await dialog(page, "Test DNS servers").getByTestId("dns-test-run").click();
  await page.waitForFunction(() => window.dnsFixture.testHeld());
  await page.evaluate(() => window.dnsFixture.release());
  await editor.locator("[data-testid='result-dnsServer-1-tls'][data-supported='false']").waitFor();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  assert.equal(overflow, 0);
  // copy-is-claim: with no hover, why a probe failed shows under its row.
  await editor.getByText("DNS over TLS: secure connection to the DNS server failed", { exact: true }).first().waitFor();
  const width = await page.evaluate(() => document.documentElement.clientWidth);
  for (const id of ["action-dnsTestCapabilities", "field-dnsServer-1-address", "field-dnsServer-1-remove"]) {
    const box = await editor.getByTestId(id).boundingBox();
    assert.ok(box && box.x >= 0 && box.x + box.width <= width, id);
  }
  await shot(page, "dns-capabilities-phone");
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("a route follows one DNS profile, and one the route cannot take is said in the form", async () => {
  const { page, errors } = await open("?routes");
  await page.getByText("news.fixture.invalid", { exact: true }).click();
  const editor = dialog(page, "Route for news.fixture.invalid");
  await editor.getByTestId("field-dnsProfile").getByText("System", { exact: true }).waitFor();
  // The proxy answering needs a proxy on every leg; this one goes direct.
  await choose(page, editor, "field-dnsProfile", "Tunnel");
  const problem = editor.getByTestId("help-dnsProfile");
  await problem.waitFor();
  assert.equal(await problem.textContent(), "This route has a direct leg or a direct fallback, where the proxy cannot answer DNS. Give it a proxy, or turn on Ask the system on a leg with no proxy in the profile.");
  assert.equal(await action(editor, "Save route").isDisabled(), true);
  // The same proxy profile asking the system on a leg with no proxy fits this route.
  await choose(page, editor, "field-dnsProfile", "Tunnel or system");
  await problem.waitFor({ state: "detached" });
  await choose(page, editor, "field-dnsProfile", "Tunnel");
  await problem.waitFor();
  await choose(page, editor, "field-dnsProfile", "Checked");
  await problem.waitFor({ state: "detached" });
  // A route never follows a proxy name profile.
  await editor.getByTestId("field-dnsProfile").click();
  await page.getByRole("menuitemradio", { name: "Checked", exact: true }).waitFor();
  assert.equal(await page.getByRole("menuitemradio", { name: "Proxy names", exact: true }).count(), 0);
  await page.keyboard.press("Escape");
  await page.getByRole("menuitemradio", { name: "Checked", exact: true }).waitFor({ state: "detached" });
  await action(editor, "Save route").click();
  await editor.waitFor({ state: "detached" });
  const { routeSaves } = await daemon(page);
  assert.equal(routeSaves.at(-1).input.dnsProfileId, 3);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("validating the proxy's answers needs every proxy the route reaches to be WireGuard", async () => {
  const { page, errors } = await open("?routes");
  await page.getByText("Fixture feed", { exact: true }).click();
  const editor = dialog(page, "Route for Fixture feed");
  // This route's one rung is a SOCKS5 proxy: the proxy answers as sent, but cannot be validated here.
  await choose(page, editor, "field-dnsProfile", "Tunnel");
  await editor.getByTestId("field-dnsProfile").getByText("Tunnel", { exact: true }).waitFor();
  const problem = editor.getByTestId("help-dnsProfile");
  assert.equal(await problem.count(), 0);
  await choose(page, editor, "field-dnsProfile", "Tunnel checked");
  await problem.waitFor();
  assert.equal(await problem.textContent(), "Only a WireGuard proxy's answers can be validated here, so every leg of this route must end at a WireGuard proxy, with no direct leg or direct fallback.");
  assert.equal(await action(editor, "Save route").isDisabled(), true);
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

test("the proxy answering at a WireGuard proxy with no tunnel DNS servers is said in the form, naming the proxy", async () => {
  const { page, errors } = await open("?routes");
  await page.getByText("Tunnel feed", { exact: true }).click();
  const editor = dialog(page, "Route for Tunnel feed");
  await editor.getByTestId("field-dnsProfile").getByText("System", { exact: true }).waitFor();
  const problem = editor.getByTestId("help-dnsProfile");
  await choose(page, editor, "field-dnsProfile", "Tunnel");
  await problem.waitFor();
  assert.equal(await problem.textContent(), "The proxy answers DNS here, but these WireGuard proxies list no DNS servers for their tunnel: Mullvad. Add the tunnel's DNS servers to each.");
  assert.equal(await action(editor, "Save route").isDisabled(), true);
  // Weaver asking its own listed servers does not need the tunnel's.
  await choose(page, editor, "field-dnsProfile", "Checked");
  await problem.waitFor({ state: "detached" });
  assert.equal(errors.length, 0, errors.join("\n"));
  await page.close();
});

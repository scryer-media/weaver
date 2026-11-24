// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { expect, expectGraphqlErrors, test as base } from "./helpers";
import { operationResponse } from "./support/graphql-operations";
import {
  type DnsProfile, type DnsServerList, type RouteInput,
  dnsServerLists, directLeg, graphqlErrors, hostAddress, ladderLeg, networkRoutes, plainDnsServers, routeInputOf, rung,
  saveDnsProfile, saveDnsServerList, serverKey, testViaLookup,
} from "./support/network-flow";
import { CONNECT_PORTS, NetworkWorld, PROXIED_HOST } from "./support/network-scenario";
import { fixtureEvents, fixtureMark, fixtureState, waitFixtureEvents } from "./support/proxy-fixture";
import {
  PROXIES_PAGE, chooseRouteProfile, countOperations, expectAddresses, fillProfile, fillServers, newListEditor,
  openDns, openRoute, resultCell, saveEditor, savedId, turnOnDnssec,
} from "./support/dns-page";
import { choose } from "./support/settings-locators";
import { networkIds, settingsIds, testIds } from "./support/testids";

const field = testIds.field;
// A zone the fixture answers that is not on the validator's private list
// (`.test` is), so validating a name in it asks the root for proof, and the
// fixture signs nothing.
const SIGNABLE_ZONE = "proxy.example";

const SAVE_ROUTE = `mutation($kind: NetworkConsumerKind!, $id: Int!, $input: RouteInput!) {
  saveNetworkRoute(kind: $kind, id: $id, input: $input) { consumer }
}`;

/**
 * The networking world of one test, put back after it. Every DNS list and
 * profile a test makes, in the browser or not, joins it so it is deleted
 * after the servers, proxies and routes that use it.
 */
const test = base.extend<{ world: NetworkWorld; unique: (tag: string) => string }>({
  world: async ({ request }, use, info) => {
    const world = await NetworkWorld.create(request);
    await use(world);
    await world.cleanup(info);
  },
  // A name no other test or run uses, fit to be a DNS label.
  unique: async ({}, use, info) => {
    const own = `${Date.now().toString(36)}-${info.testId.toLowerCase().replace(/[^a-z0-9]/g, "").slice(-12)}`;
    await use((tag) => `${tag}-${own}`);
  },
});

/** The proxy fixture's address on network a, and the ports it answers on. */
async function fixture(request: Parameters<typeof fixtureState>[0]) {
  const { ports } = await fixtureState(request);
  return { address: hostAddress("proxy-fixture", "a"), dns: ports.dns!, dnsAlt: ports.dnsAlt!, http: ports.http!, nntp: ports.nntp! };
}

async function listOf(world: NetworkWorld, name: string, addresses: string[]): Promise<DnsServerList> {
  const list = await saveDnsServerList(world.request, { name, servers: plainDnsServers(addresses) });
  world.dnsServerLists.push(list.id);
  return list;
}

async function profileOf(world: NetworkWorld, input: Parameters<typeof saveDnsProfile>[1]): Promise<DnsProfile> {
  const profile = await saveDnsProfile(world.request, input);
  world.dnsProfiles.push(profile.id);
  return profile;
}

test.describe("Networking -> DNS", () => {
  test("D01 a server list is created, reordered, saved and read back in its order", async ({ cleanPage: page, request, world, unique }) => {
    const { address, dnsAlt } = await fixture(request);
    const first = address;
    const second = `${address}:${dnsAlt}`;
    const name = unique("d01");
    await openDns(page);
    const editor = await newListEditor(page);
    await editor.getByTestId(field("dnsListName")).fill(name);
    await fillServers(page, editor, [{ address: first }, { address: second }]);
    await editor.getByTestId(field("dnsServer-1-moveUp")).click();
    await expectAddresses(editor, [second, first], "after moving row 1 up");
    const id = await saveEditor(page, editor, "SaveDnsServerList", "saveDnsServerList");
    world.dnsServerLists.push(id);

    await page.reload();
    await page.getByTestId(testIds.row("dnsLists", String(id))).click();
    const reopened = page.getByTestId(testIds.dialog("dnsList"));
    await expect(reopened.getByTestId(field("dnsListName")), `list ${id} name`).toHaveValue(name);
    await expectAddresses(reopened, [second, first], `list ${id} read back`);

    // Secondary evidence: the daemon holds the servers in the order saved.
    const saved = (await dnsServerLists(request)).find((list) => list.id === id);
    expect(saved?.servers.map((server) => [server.wire, server.address]), `list ${id} servers`).toEqual([["PLAIN", second], ["PLAIN", first]]);
  });

  test("D02 a list takes ten servers and the editor offers no eleventh", async ({ cleanPage: page, request }) => {
    const { address } = await fixture(request);
    await openDns(page);
    const editor = await newListEditor(page);
    await fillServers(page, editor, Array.from({ length: 10 }, (_, index) => ({ address: `${address}:${5300 + index}` })));
    await expect(editor.getByTestId(field("dnsServerAdd")), "Add server at ten servers").toBeDisabled();
    await expect(editor.getByTestId(field("dnsServer-10-address")), "no eleventh row").toHaveCount(0);

    // Secondary evidence: the daemon refuses an eleventh server on its own.
    const servers = plainDnsServers(Array.from({ length: 11 }, (_, index) => `${address}:${5300 + index}`));
    expect((await graphqlErrors(request, "mutation($input: DnsServerListInput!) { saveDnsServerList(input: $input) { id } }",
      { input: { name: "d02-eleven", servers } })).join("\n"), "eleven servers refused").toContain("holds at most 10 servers");
  });

  test("D03 a plain server after an encrypted one is refused in the editor", async ({ cleanPage: page, request, unique }) => {
    const { address, dnsAlt } = await fixture(request);
    const saves = countOperations(page, "SaveDnsServerList");
    await openDns(page);
    const editor = await newListEditor(page);
    await editor.getByTestId(field("dnsListName")).fill(unique("d03"));
    await fillServers(page, editor, [{ wire: "TLS", address }, { address: `${address}:${dnsAlt}` }]);
    await editor.getByTestId(testIds.editorSave).click();
    await expect(editor.getByTestId(testIds.editorError), "plain after TLS").toHaveAttribute("data-problem", "PROXY_DNS_PLAIN_AFTER_ENCRYPTED");
    await expect(editor, "the refused list stays open").toBeVisible();
    expect(saves(), "nothing was sent to save").toBe(0);
  });

  test("D04 a server listed twice is refused in the editor", async ({ cleanPage: page, request, unique }) => {
    const { address } = await fixture(request);
    const saves = countOperations(page, "SaveDnsServerList");
    await openDns(page);
    const editor = await newListEditor(page);
    await editor.getByTestId(field("dnsListName")).fill(unique("d04"));
    // Port 53 is the plain wire's own, so the two rows name one server.
    await fillServers(page, editor, [{ address }, { address: `${address}:53` }]);
    await editor.getByTestId(testIds.editorSave).click();
    await expect(editor.getByTestId(testIds.editorError), "the same server twice").toHaveAttribute("data-problem", "PROXY_DNS_SERVER_DUPLICATE");
    expect(saves(), "nothing was sent to save").toBe(0);
  });

  test("D05 a route profile on listed servers, validated here, is selected on a route and its lookups record who answered and the trust", async ({ cleanPage: page, request, world, unique }) => {
    const { address, dns, http } = await fixture(request);
    const a = await world.egress("a");
    const connect1 = await world.connect("connect1", { direct: true });
    const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 100)] } });
    const list = await listOf(world, unique("d05-list"), [address]);

    await openDns(page);
    await page.getByTestId(testIds.tableAdd("dnsProfiles")).click();
    const editor = page.getByTestId(testIds.dialog("dnsProfile"));
    await fillProfile(page, editor, { name: unique("d05"), who: "WEAVER", listId: list.id, transit: "EGRESS", trust: "VALIDATE" });
    const profileId = await saveEditor(page, editor, "SaveDnsProfile", "saveDnsProfile");
    world.dnsProfiles.push(profileId);

    const dialog = await openRoute(page, serverKey(server));
    await chooseRouteProfile(page, dialog, profileId);
    await expect(dialog.getByTestId(testIds.fieldHelp("dnsProfile")), "listed servers suit any route").toHaveCount(0);
    const saved = operationResponse(page, "SaveNetworkRoute");
    await dialog.getByTestId(networkIds.routeSave).click();
    expect((await (await saved).json()).errors ?? [], "route saved").toEqual([]);
    await expect(dialog, "the saved route closes its editor").toBeHidden();

    // Secondary evidence: the saved route follows the profile.
    const route = (await networkRoutes(request)).find((entry) => entry.consumer === serverKey(server));
    expect(route?.dnsProfileId, `route ${serverKey(server)} profile`).toBe(profileId);

    // Secondary evidence: no page shows a server route's lookups, so the lookup record is read from a test over the saved route.
    const target = `${unique("d05-target")}.${SIGNABLE_ZONE}`;
    const mark = await fixtureMark(request);
    const [path] = await testViaLookup(request, routeInputOf(route!), { host: target, port: http });
    expect(path?.resolution, "the target's lookup record").toMatchObject({
      host: target, resolvedBy: "SERVERS", trust: "BOGUS", addresses: [], error: { code: "DNSSEC_UNAVAILABLE" },
    });
    expect(path!.resolution!.dnsServers.some((listed) => listed.startsWith(address)), `${address} was listed`).toBe(true);
    expect(path!.resolution!.error!.detail, "the failure names the server that answered").toContain(address);
    // The fixture's own word that the lookup reached the listed server from the egress, not the host's resolver.
    const asked = await waitFixtureEvents(request, mark,
      (events) => events.some((event) => event.kind === "direct-dns" && event.name === target), `${target} asked of the fixture`);
    expect(asked.filter((event) => event.name === target).every((event) => event.port === dns), `${target} asked on port ${dns}`).toBe(true);
  });

  test("D06 an edited profile changes the route's next lookup without a route save and without ending its live connection", async ({ cleanPage: page, request, world, unique }) => {
    const { address, http, nntp } = await fixture(request);
    const a = await world.egress("a");
    const connect1 = await world.connect("connect1", { direct: true });
    const list = await listOf(world, unique("d06-list"), [address]);
    const profile = await profileOf(world, { kind: "ROUTE", name: unique("d06"), who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: list.id, systemFallback: false });
    const route: RouteInput = { legs: [ladderLeg(a.id, [rung.proxy(connect1.id)], 100)], dnsProfileId: profile.id };
    const server = await world.server({ host: PROXIED_HOST, connections: 1, route });
    const routeSaves = countOperations(page, "SaveNetworkRoute");

    // A paced download keeps the server's one connection busy, and streaming, until it is released.
    const mark = await fixtureMark(request);
    const download = await world.pacedDownload(unique("d06-job"));
    const live = await waitFixtureEvents(request, mark,
      (events) => events.some((event) => event.kind === "connected" && event.route === "connect1" && event.port === nntp)
        && events.some((event) => event.kind === "nntp-bytes" && event.route === "connect1"),
      `server ${server} streaming through connect1`);
    const connection = live.find((event) => event.kind === "connected" && event.route === "connect1" && event.port === nntp)!.connection!;

    // Secondary evidence: the lookup record before the edit.
    const before = await testViaLookup(request, route, { host: `${unique("d06-before")}.${SIGNABLE_ZONE}`, port: http });
    expect(before[0]?.resolution, "taken as sent").toMatchObject({ resolvedBy: "SERVERS", trust: "AS_SENT", error: null });
    expect(before[0]!.resolution!.answeredBy ?? "", "answered by the listed server").toContain(address);

    await openDns(page);
    await page.getByTestId(testIds.row("dnsProfiles", String(profile.id))).click();
    const editor = page.getByTestId(testIds.dialog("dnsProfile"));
    await turnOnDnssec(editor);
    expect(await saveEditor(page, editor, "SaveDnsProfile", "saveDnsProfile"), "the edited profile").toBe(profile.id);

    // Secondary evidence: the next lookup on the same route validates, and the fixture signs nothing.
    const after = await testViaLookup(request, route, { host: `${unique("d06-after")}.${SIGNABLE_ZONE}`, port: http });
    expect(after[0]?.resolution, "validated here after the edit").toMatchObject({ resolvedBy: "SERVERS", trust: "BOGUS", error: { code: "DNSSEC_UNAVAILABLE" } });
    expect(routeSaves(), "the route was not saved again").toBe(0);
    // Secondary evidence: the route still names the profile it named.
    expect((await networkRoutes(request)).find((entry) => entry.consumer === serverKey(server))?.dnsProfileId, "route profile").toBe(profile.id);

    // The fixture's word that the connection is the same one: still open, and no other came after it.
    expect((await fixtureState(request)).open, `connection ${connection} still open`).toContain(connection);
    expect(await download.release(), `job ${download.jobId} finished on the live connection`).toBe("COMPLETED");
    const dialed = (await fixtureEvents(request, mark)).filter((event) => event.kind === "connected" && event.route === "connect1" && event.port === nntp);
    expect(dialed.map((event) => event.connection), "the server dialed connect1 once").toEqual([connection]);
  });

  test("D07 a proxy addressed by name is found through its name profile's servers, not the host resolver", async ({ cleanPage: page, request, world, unique }) => {
    const { address, dns, dnsAlt, http } = await fixture(request);
    const a = await world.egress("a");
    // The second fixture listener tells the listed server apart from the host resolver, which the fixture is too.
    const list = await listOf(world, unique("d07-list"), [`${address}:${dnsAlt}`]);

    await openDns(page);
    await page.getByTestId(testIds.tableAdd("dnsNameProfiles")).click();
    const editor = page.getByTestId(testIds.dialog("dnsProfile"));
    await fillProfile(page, editor, { name: unique("d07"), who: "WEAVER", listId: list.id });
    await expect(editor.getByTestId(field("dnsTransit")), "a name profile always asks from the egress").toHaveCount(0);
    const nameProfileId = await saveEditor(page, editor, "SaveDnsProfile", "saveDnsProfile");
    world.dnsProfiles.push(nameProfileId);

    const proxyHost = `${unique("d07-proxy")}.proxy.test`;
    const proxy = await world.profile({
      name: unique("d07-proxy"), kind: "HTTP_CONNECT", enabled: true, host: proxyHost, port: CONNECT_PORTS.connect1!,
      username: "fixture", password: "fixture", timeoutSeconds: 5,
    }, "fixture");
    await page.goto(PROXIES_PAGE);
    await page.getByTestId(testIds.row("proxies", String(proxy.id))).click();
    const proxyEditor = page.getByTestId(testIds.dialog("proxy"));
    await choose(page, proxyEditor, field("proxyDnsProfile"), String(nameProfileId));
    const saved = operationResponse(page, "SaveProxyProfile");
    await proxyEditor.getByTestId(testIds.editorSave).click();
    expect(await savedId(await saved, "saveProxyProfile"), "the proxy saved").toBe(proxy.id);
    await expect(proxyEditor).toBeHidden();
    await expect(page.getByTestId(settingsIds.proxyDnsProfile(proxy.id)), `proxy ${proxy.id} names its profile`).toBeVisible();

    const mark = await fixtureMark(request);
    const route = await world.withDns({ legs: [ladderLeg(a.id, [rung.proxy(proxy.id)], 100)] });
    // Secondary evidence: a test through the proxy connects.
    const [path] = await testViaLookup(request, route, { host: `${unique("d07-target")}.proxy.test`, port: http });
    // Tested alone, the route names its proxy among the carriers, not as the proxy under test.
    expect(path, `a path through proxy ${proxy.id}`).toMatchObject({ success: true, carrierIds: [proxy.id] });
    const events = await waitFixtureEvents(request, mark,
      (seen) => seen.some((event) => event.kind === "direct-dns" && event.name === proxyHost),
      `${proxyHost} looked up`);
    const lookups = events.filter((event) => event.name === proxyHost && (event.kind === "direct-dns" || event.kind === "routed-dns"));
    expect(lookups.map((event) => event.port), `${proxyHost} asked only of the listed server`).not.toContain(dns);
    expect(lookups.every((event) => event.kind === "direct-dns" && event.port === dnsAlt), `${proxyHost} asked from the egress on ${dnsAlt}`).toBe(true);
    expect(events.some((event) => event.kind === "connected" && event.route === "connect1"), "the proxy connected").toBe(true);
  });

  test("D08 a profile that validates the proxy's answers cannot be saved on a route with a SOCKS rung", async ({ cleanPage: page, request, world, unique }) => {
    const a = await world.egress("a");
    const socks1 = await world.socks("socks1", { direct: true });
    const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.proxy(socks1.id)], 100)] } });
    const profile = await profileOf(world, { kind: "ROUTE", name: unique("d08"), who: "PROXY", transit: null, trust: "VALIDATE", serverListId: null, systemFallback: false });
    const original = (await networkRoutes(request)).find((entry) => entry.consumer === serverKey(server))!;

    const dialog = await openRoute(page, serverKey(server));
    await chooseRouteProfile(page, dialog, profile.id);
    await expect(dialog.getByTestId(testIds.fieldHelp("dnsProfile")), "a SOCKS rung is not WireGuard").toHaveAttribute("data-problem", "needsWireGuard");
    await expect(dialog.getByTestId(networkIds.routeSave), "Save is held").toBeDisabled();

    // Secondary evidence: the daemon refuses the same save, and the route keeps its profile.
    expect((await graphqlErrors(request, SAVE_ROUTE, { kind: "SERVER", id: server, input: { ...routeInputOf(original), dnsProfileId: profile.id } })).join("\n"),
      "refused by the daemon").toContain("only a WireGuard proxy's answers can be validated here");
    expect((await networkRoutes(request)).find((entry) => entry.consumer === serverKey(server))?.dnsProfileId).toBe(original.dnsProfileId);
  });

  test("D09 a profile that hands names to the proxy, without asking the system, cannot be saved on a route with a direct leg", async ({ cleanPage: page, request, world, unique }) => {
    const a = await world.egress("a");
    const server = await world.server({ route: { legs: [directLeg(a.id, 100)] } });
    const profile = await profileOf(world, { kind: "ROUTE", name: unique("d09"), who: "PROXY", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false });
    const original = (await networkRoutes(request)).find((entry) => entry.consumer === serverKey(server))!;

    const dialog = await openRoute(page, serverKey(server));
    await chooseRouteProfile(page, dialog, profile.id);
    await expect(dialog.getByTestId(testIds.fieldHelp("dnsProfile")), "a direct leg has no proxy to answer").toHaveAttribute("data-problem", "needsProxy");
    await expect(dialog.getByTestId(networkIds.routeSave), "Save is held").toBeDisabled();

    // Secondary evidence: the daemon refuses the same save.
    expect((await graphqlErrors(request, SAVE_ROUTE, { kind: "SERVER", id: server, input: { ...routeInputOf(original), dnsProfileId: profile.id } })).join("\n"),
      "refused by the daemon").toContain("the proxy cannot answer for");
  });

  test("D10 deleting a list a profile uses names the profile", async ({ cleanPage: page, request, world, unique }) => {
    const { address } = await fixture(request);
    const list = await listOf(world, unique("d10-list"), [address]);
    const profile = await profileOf(world, { kind: "ROUTE", name: unique("d10"), who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: list.id, systemFallback: false });

    await openDns(page);
    await page.getByTestId(testIds.row("dnsLists", String(list.id))).click();
    const editor = page.getByTestId(testIds.dialog("dnsList"));
    await editor.getByTestId(testIds.editorDelete).click();
    expectGraphqlErrors(page, /DNS server list is used by/);
    const refused = operationResponse(page, "DeleteDnsServerList");
    await page.getByTestId(testIds.dialog("dnsListDelete")).getByTestId(testIds.confirmAccept).click();
    await refused;
    await expect(editor.getByTestId(testIds.editorError), `list ${list.id} refusal`).toContainText(profile.name); // copy-is-claim: the refusal names the profile that holds the list

    // Secondary evidence: the list is still there.
    expect((await dnsServerLists(request)).some((entry) => entry.id === list.id), `list ${list.id} kept`).toBe(true);
  });

  test("D11 deleting a route profile a route follows names the route", async ({ cleanPage: page, request, world, unique }) => {
    const { address } = await fixture(request);
    const a = await world.egress("a");
    const list = await listOf(world, unique("d11-list"), [address]);
    const profile = await profileOf(world, { kind: "ROUTE", name: unique("d11"), who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: list.id, systemFallback: false });
    const host = `${unique("d11-server")}.proxy.test`;
    await world.server({ host, route: { legs: [directLeg(a.id, 100)], dnsProfileId: profile.id } });

    await openDns(page);
    await page.getByTestId(testIds.row("dnsProfiles", String(profile.id))).click();
    const editor = page.getByTestId(testIds.dialog("dnsProfile"));
    await editor.getByTestId(testIds.editorDelete).click();
    expectGraphqlErrors(page, /DNS profile is used by/);
    const refused = operationResponse(page, "DeleteDnsProfile");
    await page.getByTestId(testIds.dialog("dnsProfileDelete")).getByTestId(testIds.confirmAccept).click();
    await refused;
    await expect(editor.getByTestId(testIds.editorError), `profile ${profile.id} refusal`).toContainText(host); // copy-is-claim: the refusal names the server whose route follows the profile
  });

  test("D12 deleting a name profile a proxy uses names the proxy", async ({ cleanPage: page, request, world, unique }) => {
    const { address, dnsAlt } = await fixture(request);
    const list = await listOf(world, unique("d12-list"), [`${address}:${dnsAlt}`]);
    const profile = await profileOf(world, { kind: "PROXY_NAME", name: unique("d12"), who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: list.id, systemFallback: false });
    const proxy = await world.profile({
      name: unique("d12-proxy"), kind: "HTTP_CONNECT", enabled: true, host: `${unique("d12-proxy")}.proxy.test`, port: CONNECT_PORTS.connect1!,
      username: "fixture", password: "fixture", timeoutSeconds: 5, dnsProfileId: profile.id,
    }, "fixture");

    await openDns(page);
    await page.getByTestId(testIds.row("dnsNameProfiles", String(profile.id))).click();
    const editor = page.getByTestId(testIds.dialog("dnsProfile"));
    await editor.getByTestId(testIds.editorDelete).click();
    expectGraphqlErrors(page, /DNS profile is used by/);
    const refused = operationResponse(page, "DeleteDnsProfile");
    await page.getByTestId(testIds.dialog("dnsProfileDelete")).getByTestId(testIds.confirmAccept).click();
    await refused;
    await expect(editor.getByTestId(testIds.editorError), `profile ${profile.id} refusal`).toContainText(proxy.name); // copy-is-claim: the refusal names the proxy found through the profile
  });

  test("D13 a capability test through a chosen route shows one row per server with its four probes, and a row edit clears it", async ({ cleanPage: page, request, world }) => {
    const { address, dnsAlt } = await fixture(request);
    const a = await world.egress("a");
    await openDns(page);
    const editor = await newListEditor(page);
    await fillServers(page, editor, [{ address }, { address: `${address}:${dnsAlt}` }]);
    await editor.getByTestId(networkIds.dnsTestCapabilities).click();
    const picker = page.getByTestId(testIds.dialog("dnsTest"));
    await choose(page, picker, networkIds.routeLegEgress(0), String(a.id));
    const tested = operationResponse(page, "TestDnsServers");
    await picker.getByTestId(networkIds.dnsTestRun).click();
    await tested;
    await expect(picker, "the route picker closes for the run").toBeHidden();

    const table = editor.getByTestId(networkIds.dnsCapabilities);
    await expect(table, "the results are in").toHaveAttribute("aria-busy", "false");
    // The fixture answers plain DNS on both listeners. It has no DNS over TLS
    // listener, no DNS over HTTPS endpoint for an address, and signs nothing.
    for (const row of [0, 1]) {
      await expect(resultCell(editor, row, "plain"), `row ${row} plain`).toHaveAttribute("data-supported", "true");
      await expect(resultCell(editor, row, "plain"), `row ${row} is listed plain`).toHaveAttribute("data-own", "true");
      for (const probe of ["tls", "https", "dnssec"] as const) {
        await expect(resultCell(editor, row, probe), `row ${row} ${probe}`).toHaveAttribute("data-supported", "false");
      }
    }
    await expect(resultCell(editor, 2, "plain"), "one row per server").toHaveCount(0);

    await editor.getByTestId(field("dnsServer-1-address")).fill(`${address}:${dnsAlt + 1}`);
    await expect(table, "a row edit clears the results").toHaveCount(0);
  });
});

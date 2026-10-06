import os from "node:os";
import { expect, graphql, test } from "./helpers";
import { ifaceFor, setLink } from "./support/capture";
import { waitTerminal } from "./support/downloads";
import {
  FlowSubscription, createEgress, directLeg, discoverInterfaces, egressAddress, egressInterfaces,
  flowAfter, flowMark, graphqlErrors, hostAddress, interfaceForAddress, legOn, networkHosts,
  platformNetworking, serverKey, testEgress, updateEgress,
} from "./support/network-flow";
import { DIRECT_HOST, NetworkWorld, PROXIED_HOST, saveEvidence } from "./support/network-scenario";
import { controlRoute, fixtureState } from "./support/proxy-fixture";

/**
 * Egress interfaces. The advanced-networking flow runs this file twice: on the
 * two-egress layout with NET_RAW retained, and on a single network with
 * NET_RAW dropped (only the @no-net-raw scenarios run there).
 */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

const SAVE_ROUTE = `mutation($id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: SERVER, id: $id, input: $input) { consumer } }`;

test("E01 System egress is built in, Up, and carries every usable address", async ({ request }) => {
  // The product reserves the System binding for the built-in egress 0, so a
  // second SYSTEM egress is refused; egress 0 is the one under test.
  expect((await graphqlErrors(request,
    "mutation { createEgressInterface(input: { name: \"another system\", bindingKind: SYSTEM }) { id } }")).join("\n"))
    .toContain("only egress 0 may use the System binding");
  const system = (await egressInterfaces(request)).find(egress => egress.id === 0);
  expect(system).toMatchObject({ name: "System", bindingKind: "SYSTEM", health: "UP", reason: null, enabled: true });
  const discovered = (await discoverInterfaces(request)).filter(entry => entry.up).flatMap(entry => entry.addresses);
  expect([...system!.addresses].sort()).toEqual([...discovered].sort());
  expect(system!.addresses).toEqual(expect.arrayContaining([egressAddress("a"), egressAddress("b")]));
});

test("E02 an interface egress is Up and reports only that interface's addresses", async ({ request }) => {
  const iface = await interfaceForAddress(request, egressAddress("a"));
  const egress = await createEgress(request, { name: "e02", bindingKind: "INTERFACE", interfaceName: iface.name });
  world.track("egress", egress.id);
  expect(egress).toMatchObject({ health: "UP", reason: null, interfaceName: iface.name });
  expect([...egress.addresses].sort()).toEqual([...iface.addresses].sort());
  expect(egress.addresses).not.toContain(egressAddress("b"));
});

test("E03 a missing interface is Down and Redistribute moves its share to the other leg", async ({ request }) => {
  const missing = await createEgress(request, { name: "e03-missing", bindingKind: "INTERFACE", interfaceName: "eth9" });
  world.track("egress", missing.id);
  expect(missing).toMatchObject({ health: "DOWN", reason: "Interface is missing" });
  const good = await world.egress("a");
  const server = await world.server({ connections: 4, route: { legs: [directLeg(missing.id, 50), directLeg(good.id, 50)], failover: "REDISTRIBUTE" } });
  const mark = await flowMark(request);
  const { jobId } = await world.download("e03-probe");
  const flow = await flowAfter(request, mark, sample => {
    const down = legOn(sample, serverKey(server), missing.id);
    const up = legOn(sample, serverKey(server), good.id);
    return down?.state === "DOWN" && up?.target === 4;
  }, "missing-interface leg Down and the other leg holding the full cap");
  expect(legOn(flow, serverKey(server), missing.id)).toMatchObject({ state: "DOWN", reason: "Interface is missing", target: 0 });
  expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
});

test("E04 a source-address egress is Up only for an address Weaver holds", async ({ request }) => {
  const held = await createEgress(request, { name: "e04-held", bindingKind: "SOURCE_ADDRESS", sourceAddress: egressAddress("a") });
  world.track("egress", held.id);
  const foreign = await createEgress(request, { name: "e04-foreign", bindingKind: "SOURCE_ADDRESS", sourceAddress: "10.255.255.1" });
  world.track("egress", foreign.id);
  expect(held).toMatchObject({ health: "UP", reason: null, addresses: [egressAddress("a")] });
  expect(foreign).toMatchObject({ health: "DOWN", reason: "Source address is unavailable", addresses: [] });
});

test("E05 a route cannot use a disabled egress, which reports Disabled", async ({ request }) => {
  const iface = await interfaceForAddress(request, egressAddress("b"));
  const disabled = await createEgress(request, { name: "e05", bindingKind: "INTERFACE", interfaceName: iface.name, enabled: false });
  world.track("egress", disabled.id);
  const server = await world.server();
  const errors = await graphqlErrors(request, SAVE_ROUTE, { id: server, input: { legs: [directLeg(disabled.id, 100)] } });
  expect(errors.join("\n")).toContain("route references a disabled egress");
  expect((await egressInterfaces(request)).find(egress => egress.id === disabled.id)).toMatchObject({ health: "DOWN", reason: "Disabled" });
});

test("E06 a referenced egress cannot be deleted; moving the route frees it and the new leg is Up", async ({ request }) => {
  // Deleting an egress a route still references is refused; the
  // restored-egress warning on a Down leg is reached through a restart
  // instead (R05).
  const a = await world.egress("a");
  const b = await world.egress("b");
  const server = await world.server({ route: { legs: [directLeg(b.id, 100)] } });
  const refused = await graphqlErrors(request, "mutation($id: Int!) { deleteEgressInterface(id: $id) }", { id: b.id });
  expect(refused.join("\n")).toContain("egress is referenced by a server or RSS feed");
  await world.route("SERVER", server, { legs: [directLeg(a.id, 100)] });
  expect((await graphql<{ deleteEgressInterface: boolean }>(request, "mutation($id: Int!) { deleteEgressInterface(id: $id) }", { id: b.id })).deleteEgressInterface).toBe(true);
  world.egresses.splice(world.egresses.indexOf(b.id), 1);
  const mark = await flowMark(request);
  const { jobId } = await world.download("e06-probe");
  await flowAfter(request, mark, sample => legOn(sample, serverKey(server), a.id)?.state === "UP", "leg on the remaining egress Up");
  expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
});

test("E07 taking an interface down moves its share to the other leg and the egress recovers", async ({ request }, info) => {
  // Stands in for `docker network disconnect`: the capture sidecar owns
  // Weaver's namespace and sets the link down, so the interface keeps its
  // name and the recovery half is deterministic.
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const linkB = await ifaceFor(request, egressAddress("b"));
  const server = await world.server({ connections: 4, route: { legs: [directLeg(a.id, 50), directLeg(b.id, 50)] } });
  const download = await world.pacedDownload("e07-paced");
  await flowAfter(request, await flowMark(request), sample => {
    const legs = [legOn(sample, serverKey(server), a.id), legOn(sample, serverKey(server), b.id)];
    return legs.every(leg => leg && leg.open === leg.target && leg.target === 2);
  }, "both legs carrying their two connections");
  const down = await flowMark(request);
  await setLink(request, linkB, false);
  try {
    const flow = await flowAfter(request, down, sample => {
      const egress = sample.egresses.find(entry => entry.id === b.id);
      return egress?.health === "DOWN" && legOn(sample, serverKey(server), b.id)?.state === "DOWN"
        && legOn(sample, serverKey(server), a.id)?.target === 4;
    }, "egress-b Down, its leg Down, egress-a holding the cap");
    expect(flow.egresses.find(entry => entry.id === b.id)?.reason).toBe("Interface is down");
    await saveEvidence(info, "E07-down", flow);
  } finally {
    await setLink(request, linkB, true);
  }
  await flowAfter(request, await flowMark(request), sample => sample.egresses.find(entry => entry.id === b.id)?.health === "UP",
    "egress-b Up again once the interface returns under the same name");
  expect(await download.release()).toBe("COMPLETED");
});

test("E08 an egress speed limit caps every leg sample", async ({ request }, info) => {
  test.setTimeout(10 * 60_000);
  test.info().annotations.push({ type: "gap", description: "Weaver exposes no egress-throttling metrics counter, so only the 1 Hz leg samples are asserted." });
  const limit = 1024 * 1024;
  const iface = await interfaceForAddress(request, egressAddress("a"));
  const egress = await createEgress(request, { name: "e08", bindingKind: "INTERFACE", interfaceName: iface.name, maxDownloadSpeed: limit });
  world.track("egress", egress.id);
  const server = await world.server({ route: { legs: [directLeg(egress.id, 100)] } });
  const subscription = await FlowSubscription.open();
  try {
    const start = (await subscription.next(0, () => true, "first flow sample")).sampledAt;
    const { jobId } = await world.download("e08-limited", { parts: 320 });
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const samples = subscription.since(start).flatMap(sample => sample.legs.filter(leg => leg.consumer === serverKey(server)).map(leg => leg.bytesPerSecond));
    await saveEvidence(info, "E08-samples", samples);
    expect(samples.some(rate => rate > 0), "the limited leg carried traffic").toBe(true);
    expect(samples.filter(rate => rate > limit * 1.1), "samples above the limit + 10%").toEqual([]);
  } finally {
    subscription.close();
  }
});

test("E09 egress tests report the source address, go through a proxy, and fail when it is down", async ({ request }) => {
  const egress = await world.egress("a");
  const fixture = await fixtureState(request);
  const primary = await world.profile({
    name: "e09-primary", kind: "HTTP_CONNECT", enabled: true, host: hostAddress("proxy-fixture", "a"), port: fixture.ports.primary!,
    username: "fixture", password: "fixture", dnsServers: [hostAddress("proxy-fixture", "a")], timeoutSeconds: 5,
  });
  const direct = await testEgress(request, egress.id, DIRECT_HOST, 119);
  expect(direct).toMatchObject({ success: true, proxyId: null });
  expect(direct.sourceAddress?.replace(/:\d+$/, "")).toBe(egressAddress("a"));
  expect(direct.connectMillis).toBeGreaterThan(0);
  const proxied = await testEgress(request, egress.id, PROXIED_HOST, fixture.ports.nntp!, primary.id);
  expect(proxied).toMatchObject({ success: true, proxyId: primary.id });
  await controlRoute(request, { route: "primary", up: false });
  const failed = await testEgress(request, egress.id, PROXIED_HOST, fixture.ports.nntp!, primary.id);
  expect(failed.success).toBe(false);
  expect(failed.message.trim()).not.toBe("");
});

test("E10 platform networking on a two-network container", async ({ request }) => {
  const platform = await platformNetworking(request);
  expect(platform).toMatchObject({ platform: "linux", container: true, bridgeNetworkSuspected: false });
  expect(platform.notes.some(note => note.includes("CAP_NET_RAW"))).toBe(true);
  // The budget is a quarter of the memory Weaver can see, in 516 MiB
  // instances, clamped to 1..8. The overlay's 5 GiB limit gives 2 where the
  // runtime honours it; a runtime that ignores the limit, or a loaded host,
  // gives another value, so the exact figure is evidence rather than a rule.
  expect(Number.isInteger(platform.maxWireguardInstances)).toBe(true);
  expect(platform.maxWireguardInstances).toBeGreaterThanOrEqual(1);
  expect(platform.maxWireguardInstances).toBeLessThanOrEqual(8);
  test.info().annotations.push({ type: "observed", description: `maxWireguardInstances ${platform.maxWireguardInstances}` });
});

test("E11 a single bridge network is flagged with the multiple-networks note @no-net-raw", async ({ request }) => {
  const platform = await platformNetworking(request);
  expect(platform).toMatchObject({ container: true, bridgeNetworkSuspected: true });
  expect(platform.notes.some(note => note.includes("attach multiple container networks"))).toBe(true);
});

test("E12 discovery lists both egress interfaces Up and never loopback", async ({ request }) => {
  const discovered = await discoverInterfaces(request);
  for (const network of ["a", "b"] as const) {
    const entry = discovered.find(candidate => candidate.addresses.includes(egressAddress(network)));
    expect(entry, `interface carrying egress-${network}`).toMatchObject({ up: true });
  }
  const system = (await egressInterfaces(request)).find(egress => egress.id === 0)!;
  expect(system.addresses).toEqual(expect.arrayContaining([egressAddress("a"), egressAddress("b")]));
  expect(system.addresses.filter(address => address.startsWith("127.") || address === "::1")).toEqual([]);
});

test("E13 interface binding without NET_RAW follows the kernel rule and System carries the job @no-net-raw", async ({ request }) => {
  expect(process.env.E2E_WEAVER_NET_RAW).toBe("dropped");
  const weaverAddress = networkHosts().weaver![0]!;
  const iface = await interfaceForAddress(request, weaverAddress);
  const bound = await createEgress(request, { name: "e13", bindingKind: "INTERFACE", interfaceName: iface.name });
  world.track("egress", bound.id);
  const server = await world.server({ connections: 4, route: { legs: [directLeg(0, 50), directLeg(bound.id, 50)] } });
  const mark = await flowMark(request);
  const { jobId } = await world.download("e13-probe");
  // Playwright shares Weaver's kernel. From Linux 5.7 an unprivileged socket
  // may bind to a device, so the leg binds; before it, binding needs NET_RAW.
  const [major, minor] = os.release().split(".").map(Number);
  const unprivilegedBind = major! > 5 || (major === 5 && minor! >= 7);
  test.info().annotations.push({ type: "observed", description: `kernel ${os.release()}: ${unprivilegedBind ? "the unprivileged-bind branch ran; the pre-5.7 refusal is not exercised on this host" : "the pre-5.7 refusal branch ran"}` });
  if (unprivilegedBind) {
    await flowAfter(request, mark, sample => legOn(sample, serverKey(server), bound.id)?.state === "UP", "bound leg Up on a 5.7+ kernel");
  } else {
    const flow = await flowAfter(request, mark, sample => legOn(sample, serverKey(server), bound.id)?.state === "DOWN", "bound leg Down after two bind failures");
    expect(legOn(flow, serverKey(server), bound.id)?.reason).toContain("interface binding needs Linux 5.7 or CAP_NET_RAW");
    expect(legOn(flow, serverKey(server), 0)?.target).toBe(4);
  }
  expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
});

test("E14 renaming and disabling an egress shows in the flow, and a disabled leg is Down", async ({ request }) => {
  const egress = await world.egress("b");
  const other = await world.egress("a");
  const server = await world.server({ route: { legs: [directLeg(other.id, 50), directLeg(egress.id, 50)] } });
  const renamed = await updateEgress(request, egress.id, { name: "e14-renamed", bindingKind: "INTERFACE", interfaceName: egress.interfaceName, enabled: true });
  expect(renamed.name).toBe("e14-renamed");
  const mark = await flowMark(request);
  await flowAfter(request, mark, sample => sample.egresses.some(entry => entry.id === egress.id && entry.name === "e14-renamed"), "flow shows the new name");
  const { jobId } = await world.download("e14-probe");
  await updateEgress(request, egress.id, { name: "e14-renamed", bindingKind: "INTERFACE", interfaceName: egress.interfaceName, enabled: false });
  const disabled = await flowMark(request);
  const flow = await flowAfter(request, disabled, sample =>
    sample.egresses.some(entry => entry.id === egress.id && !entry.enabled) && legOn(sample, serverKey(server), egress.id)?.state === "DOWN",
  "disabled egress and its leg Down");
  expect(legOn(flow, serverKey(server), egress.id)?.reason).toBe("Disabled");
  expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
  await world.route("SERVER", server, { legs: [directLeg(other.id, 100)] });
});

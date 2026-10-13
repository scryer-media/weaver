import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, setNntpChaos, submitProbeNzb, test } from "./helpers";
import { packetCount, setLink, startCapture, stopCapture, tcpTo } from "./support/capture";
import { postProbeFile, waitTerminal } from "./support/downloads";
import {
  type Egress, type NetworkFlow, type RouteInput, FlowSubscription, createEgress, deleteEgress, deleteProxyProfile,
  directLeg, discoverInterfaces, flowAfter, flowMark, ladderLeg, legOn, platformNetworking, rung, saveProxyProfile,
  saveRoute, serverKey, stage,
} from "./support/network-flow";
import { saveEvidence } from "./support/network-scenario";
import { controlRoute, resetRoutes } from "./support/proxy-fixture";

/**
 * Two-NIC lanes, run only by the `two-nic` command against
 * real machines. Lane M: native macOS Weaver with Wi-Fi and LAN egresses.
 * Lane L: Weaver's Linux image with host networking. Lane L scenario titles
 * carry a "Lane L" prefix because L01-L05 are also the network-legs IDs.
 *
 * Everything host-specific comes from the runner's environment: the stack
 * address, the egresses (name -> interface or source address), the expected
 * source address per egress and the interfaces captured on Weaver's host.
 */
const lane = process.env.E2E_TWO_NIC_LANE ?? "";
test.skip(!lane, "two-NIC lanes run only from the two-nic command");

type LaneEgress = { name: string; kind: "interface" | "source"; value: string };
const stack = () => process.env.E2E_TWO_NIC_STACK_HOST ?? "";
const laneEgresses = (): LaneEgress[] => JSON.parse(process.env.E2E_TWO_NIC_EGRESSES || "[]") as LaneEgress[];
const expected = (): Record<string, string> => JSON.parse(process.env.E2E_TWO_NIC_EXPECT || "{}") as Record<string, string>;
const wifiDevice = () => process.env.E2E_TWO_NIC_WIFI_DEVICE || "en0";

function laneEgress(name: string): LaneEgress {
  const found = laneEgresses().find(entry => entry.name === name);
  expect(found, `E2E_TWO_NIC_EGRESSES must name a "${name}" egress`).toBeTruthy();
  return found!;
}
const expectedSource = (name: string) => {
  const address = expected()[name];
  expect(address, `E2E_TWO_NIC_EXPECT must give "${name}" an address`).toBeTruthy();
  return address!;
};
const bareAddress = (socket: string | null | undefined) => (socket ?? "").replace(/^\[?(.*?)\]?:\d+$/, "$1");

const created = { servers: [] as number[], egresses: [] as number[], profiles: [] as number[], wifiOff: false };

test.afterEach(async ({ request }, info) => {
  const failures: string[] = [];
  const attempt = async (label: string, action: () => Promise<unknown>) => { try { await action(); } catch (error) { failures.push(`${label}: ${String(error)}`); } };
  if (created.wifiOff) await attempt("Wi-Fi on", () => setLink(request, wifiDevice(), true));
  created.wifiOff = false;
  await attempt("chaos off", () => setNntpChaos("off", stack()));
  await attempt("stop capture", () => stopCapture(request));
  await attempt("reset proxy fixture", () => resetRoutes(request));
  for (const id of created.servers.splice(0)) await attempt(`server ${id}`, () => graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id }));
  for (const id of created.profiles.splice(0)) await attempt(`profile ${id}`, () => deleteProxyProfile(request, id));
  for (const id of created.egresses.splice(0)) await attempt(`egress ${id}`, () => deleteEgress(request, id));
  if (failures.length) await info.attach("two-nic-cleanup-failures", { body: failures.join("\n"), contentType: "text/plain" });
});

async function egress(request: APIRequestContext, name: string, extra: { maxDownloadSpeed?: number } = {}): Promise<Egress> {
  const spec = laneEgress(name);
  const made = await createEgress(request, {
    name: `${name}-${Date.now()}`,
    ...(spec.kind === "interface" ? { bindingKind: "INTERFACE", interfaceName: spec.value } : { bindingKind: "SOURCE_ADDRESS", sourceAddress: spec.value }),
    ...extra,
  });
  created.egresses.push(made.id);
  return made;
}

async function server(request: APIRequestContext, route: RouteInput, connections = 4): Promise<number> {
  const id = (await graphql<{ addServer: { id: number } }>(request, "mutation($input: ServerInput!) { addServer(input: $input) { id } }", {
    input: { host: stack(), port: 119, tls: false, username: "e2e-user", password: "e2e-pass", connections, active: true, priority: 0, backfill: false, retentionDays: 0 },
  })).addServer.id;
  created.servers.push(id);
  await saveRoute(request, "SERVER", id, route);
  return id;
}

/** A download e2e-nntp paces with slow_body until `release`. */
async function pacedDownload(request: APIRequestContext, name: string, parts = 1600, slowMs = 400) {
  const articles = await postProbeFile(name, { count: parts, partBytes: 64 * 1024, nntpHost: stack() });
  await setNntpChaos(`slow_body=${slowMs}`, stack());
  const result = await submitProbeNzb(request, name, articles, {}, "single-multipart-file");
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  const jobId = result.jobId!;
  return { jobId, release: async () => { await setNntpChaos("off", stack()); return waitTerminal(request, jobId); } };
}

const carrying = (flow: NetworkFlow, id: number, egresses: number[], target = 2) =>
  egresses.every(egressId => { const leg = legOn(flow, serverKey(id), egressId); return leg?.target === target && leg.open === target; });

// ------------------------------------------------------------------ Lane M

test.describe("lane M", () => {
  test.skip(lane !== "M", "Lane M only");

  async function wifiAndLan(request: APIRequestContext, failover: "REDISTRIBUTE" | "HOLD" = "REDISTRIBUTE") {
    const wifi = await egress(request, "wifi");
    const lan = await egress(request, "lan");
    const id = await server(request, { legs: [directLeg(wifi.id, 50), directLeg(lan.id, 50)], failover });
    return { wifi, lan, id };
  }

  test("M01 a 50/50 route sends each leg from its own NIC and address", async ({ request }, info) => {
    test.setTimeout(0);
    const { wifi, lan, id } = await wifiAndLan(request);
    await startCapture(request, "m01");
    const download = await pacedDownload(request, "m01-two-nic");
    const flow = await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [wifi.id, lan.id])
      && [wifi.id, lan.id].every(egressId => (legOn(sample, serverKey(id), egressId)?.bytesPerSecond ?? 0) > 0), "both legs carrying bytes");
    expect(bareAddress(legOn(flow, serverKey(id), wifi.id)?.sourceAddress)).toBe(expectedSource("wifi"));
    expect(bareAddress(legOn(flow, serverKey(id), lan.id)?.sourceAddress)).toBe(expectedSource("lan"));
    expect(await download.release()).toBe("COMPLETED");
    await stopCapture(request);
    const counts = {
      wifi: await packetCount(request, "m01", laneEgress("wifi").value, tcpTo(stack(), 119)),
      lan: await packetCount(request, "m01", laneEgress("lan").value, tcpTo(stack(), 119)),
    };
    await saveEvidence(info, "M01", { flow, counts });
    expect(counts.wifi).toBeGreaterThan(0);
    expect(counts.lan).toBeGreaterThan(0);
  });

  test("M02 turning Wi-Fi off takes its egress Down and the LAN leg takes the whole cap", async ({ request }, info) => {
    test.setTimeout(0);
    const { wifi, lan, id } = await wifiAndLan(request);
    await startCapture(request, "m02");
    const download = await pacedDownload(request, "m02-wifi-off");
    await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [wifi.id, lan.id]), "legs 2/2 open");
    const mark = await flowMark(request);
    created.wifiOff = true;
    await setLink(request, wifiDevice(), false);
    const down = await flowAfter(request, mark, sample => sample.egresses.find(entry => entry.id === wifi.id)?.health === "DOWN"
      && legOn(sample, serverKey(id), wifi.id)?.state === "DOWN" && legOn(sample, serverKey(id), lan.id)?.target === 4, "Wi-Fi egress and leg Down, LAN target 4");
    expect(down.egresses.find(entry => entry.id === wifi.id)?.reason?.trim()).toBeTruthy();
    const atDown = await packetCount(request, "m02", wifiDevice(), tcpTo(stack(), 119));
    expect(await download.release()).toBe("COMPLETED");
    const atEnd = await packetCount(request, "m02", wifiDevice(), tcpTo(stack(), 119));
    await saveEvidence(info, "M02", { down, atDown, atEnd });
    expect(atEnd).toBe(atDown);
  });

  test("M03 turning Wi-Fi back on brings its leg through probing to Up at 2/2", async ({ request }) => {
    test.setTimeout(0);
    const { wifi, lan, id } = await wifiAndLan(request);
    await startCapture(request, "m03");
    const download = await pacedDownload(request, "m03-wifi-on", 3200);
    await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [wifi.id, lan.id]), "legs 2/2 open");
    const subscription = await FlowSubscription.open();
    try {
      const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
      created.wifiOff = true;
      await setLink(request, wifiDevice(), false);
      const down = await subscription.next(start, sample => legOn(sample, serverKey(id), wifi.id)?.state === "DOWN", "Wi-Fi leg Down");
      const before = await packetCount(request, "m03", wifiDevice(), tcpTo(stack(), 119));
      await setLink(request, wifiDevice(), true);
      created.wifiOff = false;
      await subscription.next(down.sampledAt, sample => sample.egresses.find(entry => entry.id === wifi.id)?.health === "UP", "Wi-Fi egress Up");
      const probing = await subscription.next(down.sampledAt, sample => legOn(sample, serverKey(id), wifi.id)?.state === "PROBING", "Wi-Fi leg probing");
      await subscription.next(probing.sampledAt, sample => legOn(sample, serverKey(id), wifi.id)?.state === "UP" && carrying(sample, id, [wifi.id, lan.id]), "legs 2/2 again");
      await expect.poll(async () => packetCount(request, "m03", wifiDevice(), tcpTo(stack(), 119)), { timeout: 0, message: "Wi-Fi capture grows again" }).toBeGreaterThan(before);
    } finally {
      subscription.close();
    }
    expect(await download.release()).toBe("COMPLETED");
  });

  test("M04 with Hold, losing Wi-Fi leaves the LAN leg at its own share", async ({ request }) => {
    test.setTimeout(0);
    const { wifi, lan, id } = await wifiAndLan(request, "HOLD");
    const download = await pacedDownload(request, "m04-hold");
    await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [wifi.id, lan.id]), "legs 2/2 open");
    const mark = await flowMark(request);
    created.wifiOff = true;
    await setLink(request, wifiDevice(), false);
    const down = await flowAfter(request, mark, sample => legOn(sample, serverKey(id), wifi.id)?.state === "DOWN", "Wi-Fi leg Down");
    expect(legOn(down, serverKey(id), lan.id)?.target).toBe(2);
    await flowAfter(request, down.sampledAt, sample => (legOn(sample, serverKey(id), lan.id)?.bytesPerSecond ?? 0) > 0, "LAN still carrying bytes");
    expect(await download.release()).toBe("COMPLETED");
  });

  test("M05 a source-address egress uses that address on whichever NIC macOS routes it", async ({ request }, info) => {
    test.setTimeout(0);
    const address = expectedSource("wifi");
    const made = await createEgress(request, { name: `m05-source-${Date.now()}`, bindingKind: "SOURCE_ADDRESS", sourceAddress: address });
    created.egresses.push(made.id);
    const id = await server(request, { legs: [directLeg(made.id, 100)] });
    await startCapture(request, "m05");
    const download = await pacedDownload(request, "m05-source", 320);
    const flow = await flowAfter(request, await flowMark(request), sample => (legOn(sample, serverKey(id), made.id)?.open ?? 0) > 0, "source leg open");
    expect(bareAddress(legOn(flow, serverKey(id), made.id)?.sourceAddress)).toBe(address);
    expect(await download.release()).toBe("COMPLETED");
    await stopCapture(request);
    const perInterface: Record<string, number> = {};
    for (const iface of (process.env.E2E_TWO_NIC_CAPTURE_IFACES ?? "").split(",").filter(Boolean)) {
      perInterface[iface] = await packetCount(request, "m05", iface, `src host ${address} and dst host ${stack()}`);
    }
    await saveEvidence(info, "M05", { platformHint: (await platformNetworking(request)).sourceAddressHint, perInterface });
    expect(Object.values(perInterface).reduce((sum, count) => sum + count, 0)).toBeGreaterThan(0);
  });

  test("M06 a Wi-Fi speed cap holds while the LAN leg runs past it", async ({ request }, info) => {
    test.setTimeout(0);
    const cap = 2 * 1024 * 1024;
    const wifi = await egress(request, "wifi", { maxDownloadSpeed: cap });
    const lan = await egress(request, "lan");
    const id = await server(request, { legs: [directLeg(wifi.id, 50), directLeg(lan.id, 50)] });
    const articles = await postProbeFile("m06-cap", { count: 3200, partBytes: 64 * 1024, nntpHost: stack() });
    const subscription = await FlowSubscription.open();
    try {
      const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
      const result = await submitProbeNzb(request, "m06-cap", articles, {}, "single-multipart-file");
      expect(await waitTerminal(request, result.jobId!)).toBe("COMPLETED");
      const window = subscription.since(start);
      const wifiRates = window.map(sample => legOn(sample, serverKey(id), wifi.id)?.bytesPerSecond ?? 0);
      const lanRates = window.map(sample => legOn(sample, serverKey(id), lan.id)?.bytesPerSecond ?? 0);
      await saveEvidence(info, "M06", { wifiRates, lanRates });
      expect(Math.max(...wifiRates)).toBeLessThanOrEqual(cap * 1.1);
      expect(Math.max(...lanRates)).toBeGreaterThan(cap);
    } finally {
      subscription.close();
    }
  });

  test("M07 a proxy rung on Wi-Fi fails and recovers on a real NIC", async ({ request }, info) => {
    test.setTimeout(0);
    const wifi = await egress(request, "wifi");
    const lan = await egress(request, "lan");
    const connect1 = await saveProxyProfile(request, { name: `m07-connect1-${Date.now()}`, kind: "HTTP_CONNECT", enabled: true, host: stack(), port: 8101, username: "fixture", password: "fixture", dnsServers: [], timeoutSeconds: 5 });
    created.profiles.push(connect1.id);
    const id = await server(request, { legs: [ladderLeg(wifi.id, [rung.proxy(connect1.id)], 50), directLeg(lan.id, 50)] });
    await startCapture(request, "m07");
    const download = await pacedDownload(request, "m07-proxy", 3200);
    await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [wifi.id, lan.id]), "legs 2/2 open");
    const subscription = await FlowSubscription.open();
    try {
      const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
      await controlRoute(request, { route: "connect1", up: false, cut: true });
      const down = await subscription.next(start, sample => legOn(sample, serverKey(id), wifi.id)?.state === "DOWN", "Wi-Fi leg Down");
      await controlRoute(request, { route: "connect1", up: true });
      const probing = await subscription.next(down.sampledAt, sample => legOn(sample, serverKey(id), wifi.id)?.state === "PROBING", "Wi-Fi leg probing");
      await subscription.next(probing.sampledAt, sample => legOn(sample, serverKey(id), wifi.id)?.state === "UP", "Wi-Fi leg Up");
    } finally {
      subscription.close();
    }
    expect(await download.release()).toBe("COMPLETED");
    await stopCapture(request);
    const counts = {
      wifiProxy: await packetCount(request, "m07", laneEgress("wifi").value, tcpTo(stack(), 8101)),
      lanProxy: await packetCount(request, "m07", laneEgress("lan").value, tcpTo(stack(), 8101)),
      lanNntp: await packetCount(request, "m07", laneEgress("lan").value, tcpTo(stack(), 119)),
    };
    await saveEvidence(info, "M07", counts);
    expect(counts.wifiProxy).toBeGreaterThan(0);
    expect(counts.lanProxy).toBe(0);
    expect(counts.lanNntp).toBeGreaterThan(0);
  });

  test("M08 macOS reports its platform and both NICs", async ({ request }) => {
    const platform = await platformNetworking(request);
    expect(platform).toMatchObject({ platform: "macos", container: false });
    expect([...platform.egressBindingKinds].sort()).toEqual(["INTERFACE", "SOURCE_ADDRESS", "SYSTEM"]);
    const interfaces = await discoverInterfaces(request);
    for (const name of ["wifi", "lan"]) {
      const listed = interfaces.find(entry => entry.name === laneEgress(name).value);
      expect(listed, laneEgress(name).value).toBeTruthy();
      expect(listed!.addresses).toContain(expectedSource(name));
    }
  });
});

// ------------------------------------------------------------------ Lane L

test.describe("lane L", () => {
  test.skip(lane !== "L", "Lane L only");

  const kernelAtLeast57 = () => {
    const [major = 0, minor = 0] = (process.env.E2E_TWO_NIC_KERNEL ?? "").split(/[.-]/).map(Number);
    return major > 5 || (major === 5 && minor >= 7);
  };

  test("Lane L L01 an interface and a source-address egress both leave from the LAN address", async ({ request }, info) => {
    test.skip(stage() !== "initial", "runs with CAP_NET_RAW retained");
    test.setTimeout(0);
    const lan = await egress(request, "lan");
    const src = await egress(request, "src");
    const id = await server(request, { legs: [directLeg(lan.id, 50), directLeg(src.id, 50)] });
    await startCapture(request, "lane-l-01");
    const download = await pacedDownload(request, "lane-l-01", 640);
    const flow = await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [lan.id, src.id]), "legs 2/2 open");
    expect(flow.egresses.find(entry => entry.id === lan.id)?.health).toBe("UP");
    for (const egressId of [lan.id, src.id]) expect(bareAddress(legOn(flow, serverKey(id), egressId)?.sourceAddress)).toBe(expectedSource("lan"));
    expect(await download.release()).toBe("COMPLETED");
    await stopCapture(request);
    const count = await packetCount(request, "lane-l-01", laneEgress("lan").value, tcpTo(stack(), 119));
    await saveEvidence(info, "Lane-L-L01", { flow, count });
    expect(count).toBeGreaterThan(0);
  });

  test("Lane L L02 without CAP_NET_RAW an interface bind depends on the kernel", async ({ request }) => {
    test.skip(stage() !== "no-net-raw", "runs in the stage without WEAVER_RETAIN_NET_RAW");
    test.setTimeout(0);
    const lan = await egress(request, "lan");
    const src = await egress(request, "src");
    const id = await server(request, { legs: [directLeg(lan.id, 50), directLeg(src.id, 50)] });
    const download = await pacedDownload(request, "lane-l-02", 320);
    test.info().annotations.push({ type: "observed", description: `kernel ${process.env.E2E_TWO_NIC_KERNEL ?? "unknown"}: ${kernelAtLeast57() ? "unprivileged bind branch" : "refusal branch"}` });
    if (kernelAtLeast57()) {
      await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(id), lan.id)?.state === "UP", "LAN leg Up (unprivileged bind on 5.7+)");
    } else {
      const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(id), lan.id)?.state === "DOWN", "LAN leg Down");
      expect(legOn(flow, serverKey(id), lan.id)?.reason).toContain("needs Linux 5.7 or CAP_NET_RAW");
      expect(legOn(flow, serverKey(id), src.id)?.target).toBe(4);
    }
    expect(await download.release()).toBe("COMPLETED");
  });

  test("Lane L L03 an egress with no route to the destination is Down and its share moves", async ({ request }) => {
    test.skip(stage() !== "initial", "initial stage only");
    const lan = await egress(request, "lan");
    const usb = await egress(request, "usb");
    const id = await server(request, { legs: [directLeg(lan.id, 50), directLeg(usb.id, 50)] });
    const download = await pacedDownload(request, "lane-l-03", 320);
    const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(id), usb.id)?.state === "DOWN", "USB leg Down");
    expect(legOn(flow, serverKey(id), usb.id)?.reason).toBe("No usable address for this destination");
    expect(legOn(flow, serverKey(id), lan.id)?.target).toBe(4);
    expect(await download.release()).toBe("COMPLETED");
  });

  test("Lane L L04 a LAN link cycle takes the egress Down and back Up", async ({ request }) => {
    test.skip(stage() !== "initial", "initial stage only");
    test.setTimeout(0);
    const lan = await egress(request, "lan");
    const src = await egress(request, "src");
    const id = await server(request, { legs: [directLeg(lan.id, 50), directLeg(src.id, 50)] });
    const download = await pacedDownload(request, "lane-l-04", 1600);
    await flowAfter(request, await flowMark(request), sample => carrying(sample, id, [lan.id, src.id]), "legs 2/2 open");
    const subscription = await FlowSubscription.open();
    try {
      const start = (await subscription.next(0, () => true, "first sample")).sampledAt;
      // The runner takes the link down and brings it back in one remote
      // command, once Weaver reports this egress Down.
      await setLink(request, laneEgress("lan").value, false);
      const down = await subscription.next(start, sample => sample.egresses.find(entry => entry.id === lan.id)?.health === "DOWN", "LAN egress Down");
      expect(down.egresses.find(entry => entry.id === lan.id)?.reason).toBe("Interface is down");
      await subscription.next(down.sampledAt, sample => sample.egresses.find(entry => entry.id === lan.id)?.health === "UP", "LAN egress Up");
    } finally {
      subscription.close();
    }
    expect(await download.release()).toBe("COMPLETED");
  });

  test("Lane L L05 a host-networked container reports no bridge and the CAP_NET_RAW note", async ({ request }) => {
    test.skip(stage() !== "initial", "initial stage only");
    const platform = await platformNetworking(request);
    expect(platform).toMatchObject({ platform: "linux", container: true, bridgeNetworkSuspected: false });
    expect(platform.notes.join("\n")).toContain("CAP_NET_RAW");
  });
});

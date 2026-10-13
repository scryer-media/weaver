import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { directLeg, type Egress, type NetworkRoute, type SpeedTestInput, type SpeedTestRun } from "@/lib/networking";
import type { ProxyKind, ProxyProfile } from "@/lib/proxies";
import type { Consumer, NetworkingData } from "@/next/features/networking/data";
import { RoutesPage } from "@/next/features/networking/RoutesPage";
import { SpeedPage } from "@/next/features/networking/SpeedPage";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

/* The fixture's daemon: two egresses, a pool of two proxies, a server and a feed, and the runs it holds. */

const egresses: Egress[] = [
  { id: 0, name: "System", bindingKind: "SYSTEM", interfaceName: null, sourceAddress: null, enabled: true, maxDownloadSpeed: 0, health: "UP", reason: null },
  { id: 1, name: "WAN Fiber", bindingKind: "SOURCE_ADDRESS", interfaceName: null, sourceAddress: "192.0.2.10", enabled: true, maxDownloadSpeed: 0, health: "UP", reason: null },
];
const profile = (id: number, name: string, kind: ProxyKind): ProxyProfile => ({
  id, name, kind, enabled: true, host: `${name.toLowerCase()}.fixture.invalid`, port: 51820, dnsServers: [],
  tunnelAddresses: [`10.0.0.${id}/32`], peerPublicKey: null, tunnelPublicKey: null, mtu: 1420, keepaliveSeconds: null,
  timeoutSeconds: 10, hostKeyFingerprint: null, hasUsername: false, hasPassword: false, hasPrivateKey: true,
  hasPassphrase: false, hasPresharedKey: false,
});
const profiles = [profile(1, "Amsterdam", "WIRE_GUARD"), profile(2, "Frankfurt", "WIRE_GUARD")];
const pools = [{ id: 1, name: "Europe", kind: "WIRE_GUARD" as const, memberIds: [1, 2], enabled: true }];
const route: NetworkRoute = {
  failover: "REDISTRIBUTE",
  legs: [
    { egressId: 1, weight: 60, path: { kind: "LADDER", directFallback: false, rungs: [{ kind: "POOL", poolId: 1, proxyId: null, chainIds: [] }] } },
    { ...directLeg(), egressId: 0, weight: 40 },
  ],
};
const consumers: Consumer[] = [
  { key: "server:1", id: 1, name: "news.fixture.invalid", kind: "SERVER", cap: 20, route },
  { key: "rss:1", id: 1, name: "Feed mirror", kind: "RSS", cap: 1, route: null },
];
const data: NetworkingData = {
  egressInterfaces: egresses, proxyProfiles: profiles, proxyPools: pools, discoverNetworkInterfaces: [],
  platformNetworking: { platform: "fixture", egressBindingKinds: ["SYSTEM", "SOURCE_ADDRESS"], sourceAddressHint: null, container: false, bridgeNetworkSuspected: false, maxWireguardInstances: 4, notes: [] },
  servers: [{ id: 1, host: "news.fixture.invalid", connections: 20, active: true, routing: route }],
  rssFeeds: [{ id: 1, name: "Feed mirror", enabled: true, routing: null }],
};
const providers = [
  { id: "cloudflare", name: "Cloudflare", fixedSizes: [], anySize: true, maxBytes: 99999999, needsServer: false },
  { id: "librespeed", name: "LibreSpeed", fixedSizes: [], anySize: true, maxBytes: 1073741824, needsServer: false },
  { id: "hetzner", name: "Hetzner", fixedSizes: [104857600, 1073741824], anySize: false, maxBytes: null, needsServer: false },
];

const noOverrides = () => ({ downLegs: [], downRungs: [], downProxies: [], downEgresses: [] });
const MIB = 1024 * 1024;
/** A feed's bandwidth run from before the page opened, shown only under "All routes". */
const runs: SpeedTestRun[] = [
  {
    id: 1, consumer: "rss:1", consumerName: "Feed mirror", kind: "BANDWIDTH",
    request: { bandwidth: { providerId: "cloudflare", server: null, customUrl: null, bytes: 99999999, streams: 4 }, usenet: null, maxDurationSeconds: 60 },
    status: "COMPLETED", phase: null, startedAt: 1790640000000, finishedAt: 1790640010000, error: null,
    tested: { legs: [{ position: 0, egressId: 0, weight: 100, down: false, downReason: null, path: { kind: "DIRECT" }, target: 4 }], failover: "REDISTRIBUTE", overrides: noOverrides(), names: [{ kind: "egress", id: 0, name: "System" }] },
    result: {
      bytes: 100 * MIB, elapsedMs: 10000, averageBps: 10 * MIB, peakBps: 12 * MIB,
      legs: [{ position: 0, bytes: 100 * MIB, averageBps: 10 * MIB, peakBps: 12 * MIB, connections: 4, sourceAddresses: ["192.0.2.10"] }],
      connections: [], usenet: null, bandwidth: { providerId: "cloudflare", server: null, url: "https://speed.fixture.invalid/__down", streams: 4 },
      samples: [{ atMs: 1000, leg: 0, bytes: 10 * MIB }],
    },
  },
];
const requests: { name: string; variables: Record<string, unknown> }[] = [];
const finished = new Set<number>();

/** What a run reports: still measuring, or done once the test has said so. */
function report(run: SpeedTestRun): SpeedTestRun {
  if (run.status !== "RUNNING" || !finished.has(run.id)) return run;
  const live = run.tested.legs.filter((leg) => !leg.down);
  const usenet = run.kind === "USENET";
  Object.assign(run, {
    status: "COMPLETED", phase: null, finishedAt: run.startedAt + 12000,
    result: {
      bytes: 240 * MIB, elapsedMs: 12000, averageBps: 20 * MIB, peakBps: 30 * MIB,
      legs: live.map((leg) => ({ position: leg.position, bytes: 240 * MIB / live.length, averageBps: 20 * MIB / live.length, peakBps: 30 * MIB / live.length, connections: leg.target, sourceAddresses: ["192.0.2.10"] })),
      connections: live.map((leg) => ({ leg: leg.position, connectMs: 18, path: { egressId: leg.egressId, rung: null, poolId: null, memberId: null, proxyIds: [] }, sourceAddress: "192.0.2.10:45123", bytes: 240 * MIB / live.length, error: null })),
      usenet: usenet ? { articlesRequested: 320, delivered: 318, missing: 2, failed: 0, declaredBytes: 100_000_000, decodedBytes: 230 * MIB, pipelining: true, tls: true, tlsCipherSuite: "TLS13_AES_256_GCM_SHA384" } : null,
      bandwidth: usenet ? null : { providerId: "cloudflare", server: null, url: "https://speed.fixture.invalid/__down", streams: 4 },
      samples: live.flatMap((leg) => [1000, 2000].map((atMs) => ({ atMs, leg: leg.position, bytes: 10 * MIB }))),
    },
  });
  return run;
}

function graphql(name: string, variables: Record<string, unknown>) {
  if (name !== "SpeedTest" && name !== "SpeedTestRun") requests.push({ name, variables: structuredClone(variables) });
  if (name === "StartSpeedTest") {
    const input = variables.input as SpeedTestInput;
    const owner = consumers.find((consumer) => consumer.kind === input.kind && consumer.id === input.id)!;
    const tested = input.route ?? owner.route ?? { failover: "REDISTRIBUTE", legs: [directLeg()] };
    const overrides = input.overrides ?? noOverrides();
    const run: SpeedTestRun = {
      id: runs.reduce((top, entry) => Math.max(top, entry.id), 0) + 1,
      consumer: owner.key, consumerName: owner.name, kind: input.usenet ? "USENET" : "BANDWIDTH",
      // Kept as the daemon keeps it: every field present, absent ones null.
      request: {
        bandwidth: input.bandwidth ? { providerId: input.bandwidth.providerId ?? null, server: input.bandwidth.server ?? null, customUrl: input.bandwidth.customUrl ?? null, bytes: input.bandwidth.bytes, streams: input.bandwidth.streams } : null,
        usenet: input.usenet ? { sabSize: input.usenet.sabSize ?? null, nzbUrl: input.usenet.nzbUrl ?? null, connections: input.usenet.connections ?? null } : null,
        maxDurationSeconds: input.maxDurationSeconds,
      }, status: "RUNNING", phase: "draining 3",
      startedAt: 1790640100000, finishedAt: null, error: null,
      tested: {
        legs: tested.legs.map((leg, position) => ({ position, egressId: leg.egressId, weight: leg.weight, down: overrides.downLegs.includes(position), downReason: overrides.downLegs.includes(position) ? "taken down for this test" : null, path: leg.path, target: overrides.downLegs.includes(position) ? 0 : 10 })),
        failover: tested.failover, overrides, names: [],
      },
      result: { bytes: 12 * MIB, elapsedMs: 1000, averageBps: 0, peakBps: 0, legs: [], connections: [], usenet: null, bandwidth: null, samples: [{ atMs: 1000, leg: 0, bytes: 12 * MIB }] },
    };
    runs.unshift(run);
    return { data: { startSpeedTest: run } };
  }
  if (name === "SpeedTestRun") {
    const run = runs.find((entry) => entry.id === variables.id);
    return { data: { speedTestRun: run ? report(run) : null } };
  }
  if (name === "CancelSpeedTest") {
    const run = runs.find((entry) => entry.id === variables.id && entry.status === "RUNNING");
    // Cancelled before it measured: no result.
    if (run) Object.assign(run, { status: "CANCELLED", phase: null, finishedAt: run.startedAt + 1000, result: null });
    return { data: { cancelSpeedTest: run !== undefined } };
  }
  if (name === "BandwidthProviderServers") return { data: { bandwidthProviderServers: [] } };
  return { data: { bandwidthProviders: providers, speedTestRuns: runs.map(report) } };
}
const fixture = { requests, finish: (id: number) => finished.add(id) };
Object.assign(window, { speedFixture: fixture });

const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  if (new URL(String(request), document.baseURI).pathname !== "/graphql") return originalFetch(request, init);
  const body = JSON.parse(String(init?.body));
  return Response.json(graphql(body.operationName, body.variables));
};
/* With `?routes` it is the routes table and its editor; otherwise the speed test page, opened on the server. */
const query = new URLSearchParams(window.location.search);
const screen = query.has("routes")
  ? <RoutesPage data={data} consumers={consumers} flow={{ legs: [], pools: [] }} action={async () => null} busy={false} />
  : <SpeedPage data={data} consumers={consumers} />;
const dictionary: Record<string, string> = { ...en, ...nextEn };
const shell = { search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: document.getElementById("controls") };
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter initialEntries={[query.has("routes") ? "/settings/networking/routes" : "/settings/networking/speed?consumer=server:1"]}><SettingsShellProvider {...shell}><main className="mx-auto flex max-w-[1400px] flex-col p-8">{screen}</main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);

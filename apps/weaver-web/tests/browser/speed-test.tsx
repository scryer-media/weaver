// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext, type Translate } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { directLeg, type Egress, type NetworkRoute, type SpeedTestInput, type SpeedTestRun } from "@/lib/networking";
import type { ProxyKind, ProxyProfile } from "@/lib/proxies";
import { NextDataContext, type NextData } from "@/next/data/next-data";
import { flowConsumers, type Consumer, type NetworkingAction, type NetworkingData } from "@/next/features/networking/data";
import { RoutesPage } from "@/next/features/networking/RoutesPage";
import { SpeedPage } from "@/next/features/networking/SpeedPage";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

/* The fixture's daemon: two egresses, a pool of two proxies, Default outbound, a server and a feed, and the runs it holds. */

const dictionary: Record<string, string> = { ...en, ...nextEn };
const t: Translate = (key, values) =>
  Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key);

const egresses: Egress[] = [
  { id: 0, name: "System", bindingKind: "SYSTEM", interfaceName: null, sourceAddress: null, enabled: true, maxDownloadSpeed: 0, health: "UP", reason: null },
  { id: 1, name: "WAN Fiber", bindingKind: "SOURCE_ADDRESS", interfaceName: null, sourceAddress: "192.0.2.10", enabled: true, maxDownloadSpeed: 0, health: "UP", reason: null },
];
const profile = (id: number, name: string, kind: ProxyKind): ProxyProfile => ({
  id, name, kind, enabled: true, host: `${name.toLowerCase()}.fixture.invalid`, port: 51820, resolver: "WEAVER", dnsServers: [],
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
  // Default outbound comes as the flow brings it, first, and is named in the reader's language.
  ...flowConsumers(
    { legs: [], pools: [], consumers: [{ key: "outbound", id: 0, name: "Default outbound", kind: "OUTBOUND", cap: 1, route: { failover: "REDISTRIBUTE", legs: [directLeg()] } }] },
    undefined,
    t,
  ),
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
  { id: "cloudflare", name: "Cloudflare", host: "speed.cloudflare.com", needsServer: false },
  { id: "librespeed", name: "LibreSpeed", host: null, needsServer: false },
  { id: "hetzner", name: "Hetzner", host: "nbg1-speed.hetzner.com", needsServer: false },
];

const noOverrides = () => ({ downLegs: [], downRungs: [], downProxies: [], downEgresses: [] });
const MIB = 1024 * 1024;
/** The server's bandwidth run from before the page opened, listed under its route alone. */
const runs: SpeedTestRun[] = [
  {
    id: 1, consumer: "server:1", consumerName: "news.fixture.invalid", kind: "BANDWIDTH",
    request: { bandwidth: { providerId: "cloudflare", server: null, customUrl: null }, usenet: null },
    status: "COMPLETED", phase: null, startedAt: 1790640000000, finishedAt: 1790640010000, failure: null,
    tested: { legs: [{ position: 0, egressId: 0, weight: 100, down: false, downReason: null, path: { kind: "DIRECT" }, target: 4 }], failover: "REDISTRIBUTE", overrides: noOverrides(), names: [{ kind: "egress", id: 0, name: "System" }] },
    result: {
      bytes: 100 * MIB, elapsedMs: 10000, averageBps: 10 * MIB, peakBps: 12 * MIB,
      legs: [{ position: 0, bytes: 100 * MIB, averageBps: 10 * MIB, peakBps: 12 * MIB, connections: 4, sourceAddresses: ["192.0.2.10"] }],
      connections: [], usenet: null, bandwidth: { providerId: "cloudflare", server: null, url: "https://speed.fixture.invalid/__down", connections: 4 },
      samples: [{ atMs: 1000, leg: 0, bytes: 10 * MIB, decodedBytes: 0 }],
    },
  },
];
const requests: { name: string; variables: Record<string, unknown> }[] = [];
const finished = new Set<number>();
/** Runs the test has failed, with why: each reports FAILED with nothing measured. */
const failures = new Map<number, NonNullable<SpeedTestRun["failure"]>>();

/** What a run reports: still measuring, or done once the test has said so. */
function report(run: SpeedTestRun): SpeedTestRun {
  const failure = run.status === "RUNNING" ? failures.get(run.id) : undefined;
  if (failure) {
    Object.assign(run, { status: "FAILED", phase: null, finishedAt: run.startedAt + 1000, failure, result: null });
    return run;
  }
  if (run.status !== "RUNNING" || !finished.has(run.id)) return run;
  const live = run.tested.legs.filter((leg) => !leg.down);
  const usenet = run.kind === "USENET";
  Object.assign(run, {
    status: "COMPLETED", phase: null, finishedAt: run.startedAt + 12000,
    result: {
      bytes: 240 * MIB, elapsedMs: 12000, averageBps: 20 * MIB, peakBps: 30 * MIB,
      legs: live.map((leg) => ({ position: leg.position, bytes: 240 * MIB / live.length, averageBps: 20 * MIB / live.length, peakBps: 30 * MIB / live.length, connections: leg.target, sourceAddresses: ["192.0.2.10"] })),
      connections: live.map((leg) => ({ leg: leg.position, connectMs: 18, path: { egressId: leg.egressId, rung: null, poolId: null, memberId: null, proxyIds: [] }, sourceAddress: "192.0.2.10:45123", bytes: 240 * MIB / live.length, bytesPerSecond: 0, error: null })),
      usenet: usenet ? { articlesRequested: 320, delivered: 318, missing: 2, failed: 0, declaredBytes: 100_000_000, decodedBytes: 230 * MIB, pipelining: true, tls: true, tlsCipherSuite: "TLS13_AES_256_GCM_SHA384" } : null,
      bandwidth: usenet ? null : { providerId: "cloudflare", server: null, url: "https://speed.fixture.invalid/__down", connections: 4 },
      samples: live.flatMap((leg) => [1000, 2000].map((atMs) => ({ atMs, leg: leg.position, bytes: 10 * MIB, decodedBytes: usenet ? 9 * MIB : 0 }))),
    },
  });
  return run;
}

/** A route as the daemon reports it, from the shape a request carries it in. */
function reported(route: NonNullable<SpeedTestInput["route"]>): NetworkRoute {
  return {
    failover: route.failover,
    legs: route.legs.map((leg) => ({
      egressId: leg.egressId,
      weight: leg.weight,
      path:
        "direct" in leg.path
          ? directLeg().path
          : {
              kind: "LADDER",
              directFallback: leg.path.ladder.directFallback,
              rungs: leg.path.ladder.rungs.map((rung) =>
                "proxy" in rung
                  ? { kind: "PROXY", proxyId: rung.proxy, poolId: null, chainIds: [] }
                  : "pool" in rung
                    ? { kind: "POOL", poolId: rung.pool, proxyId: null, chainIds: [] }
                    : { kind: "CHAIN", chainIds: rung.chain, proxyId: null, poolId: null },
              ),
            },
    })),
  };
}

/**
 * Moves a running test on to measuring, `seconds` in: one connection held on each leg still up, through its
 * first rung (a pool's by its second member), each second's bin a little different and the newest one 12 MiB
 * on each leg. A Usenet run's bins say what they decoded to as well.
 */
function measure(id: number, seconds = 1) {
  const run = runs.find((entry) => entry.id === id);
  if (!run) return;
  const live = run.tested.legs.filter((leg) => !leg.down);
  const binBytes = (second: number) => (second === seconds ? 12 : 9 + 3 * Math.sin(second / 1.7)) * MIB;
  const total = Array.from({ length: seconds }, (_, index) => binBytes(index + 1)).reduce((sum, bytes) => sum + bytes, 0);
  Object.assign(run, {
    phase: "measuring",
    result: {
      bytes: total * live.length, elapsedMs: seconds * 1000, averageBps: total / seconds, peakBps: 12 * MIB,
      legs: live.map((leg) => ({ position: leg.position, bytes: total, averageBps: total / seconds, peakBps: 12 * MIB, connections: 1, sourceAddresses: [] })),
      connections: live.map((leg) => {
        const pool = leg.path.kind === "LADDER" && leg.path.rungs[0]?.kind === "POOL" ? pools.find((entry) => entry.id === leg.path.rungs[0]!.poolId) : undefined;
        return { leg: leg.position, connectMs: 18, path: { egressId: leg.egressId, rung: leg.path.kind === "LADDER" ? 0 : null, poolId: pool?.id ?? null, memberId: pool?.memberIds[1] ?? null, proxyIds: [] }, sourceAddress: null, bytes: total, bytesPerSecond: binBytes(seconds), error: null };
      }),
      usenet: run.kind === "USENET" ? { articlesRequested: 320, delivered: 120, missing: 0, failed: 0, declaredBytes: 100_000_000, decodedBytes: Math.round(total * live.length * 0.97), pipelining: true, tls: true, tlsCipherSuite: "TLS13_AES_256_GCM_SHA384" } : null,
      bandwidth: null,
      samples: Array.from({ length: seconds }, (_, index) => index + 1).flatMap((second) => live.map((leg) => ({
        atMs: second * 1000, leg: leg.position, bytes: binBytes(second),
        decodedBytes: run.kind === "USENET" ? Math.round(binBytes(second) * (0.9 + 0.08 * Math.cos(second))) : 0,
      }))),
    },
  });
}

/** An uploaded file as the fixture keeps it: its name and what it holds, in place of the `File` the page sent. */
type Uploaded = { upload: string; text: string };
/** The multipart parts each upload came in, as the page laid them out. */
const uploads: { map: unknown; name: string; text: string }[] = [];

function invalid(message: string) {
  return { errors: [{ message, extensions: { code: "INVALID_INPUT" } }] };
}

function graphql(name: string, variables: Record<string, unknown>) {
  if (name !== "SpeedTest" && name !== "SpeedTestRun") requests.push({ name, variables: structuredClone(variables) });
  if (name === "StartSpeedTest") {
    const input = variables.input as SpeedTestInput;
    const usenet = input.usenet;
    const uploaded = usenet?.nzbUpload as unknown as Uploaded | null | undefined;
    // A held run's upload, as the daemon resolves `nzbRunId`; only a run that took one holds one.
    const held = usenet?.nzbRunId == null ? null : runs.find((entry) => entry.id === usenet.nzbRunId)?.request.usenet?.nzbFilename ?? null;
    if (usenet?.nzbRunId != null && held === null) return invalid(`run ${usenet.nzbRunId} holds no uploaded NZB`);
    const owner = consumers.find((consumer) => consumer.kind === input.kind && consumer.id === input.id)!;
    const tested = input.route ? reported(input.route) : (owner.route ?? { failover: "REDISTRIBUTE", legs: [directLeg()] });
    const overrides = input.overrides ?? noOverrides();
    const run: SpeedTestRun = {
      id: runs.reduce((top, entry) => Math.max(top, entry.id), 0) + 1,
      consumer: owner.key, consumerName: owner.name, kind: input.usenet ? "USENET" : "BANDWIDTH",
      // Kept as the daemon keeps it: every field present, absent ones null.
      request: {
        bandwidth: input.bandwidth ? { providerId: input.bandwidth.providerId ?? null, server: input.bandwidth.server ?? null, customUrl: input.bandwidth.customUrl ?? null } : null,
        usenet: usenet
          ? {
              sabSize: usenet.sabSize ?? null, nzbUrl: usenet.nzbUrl ?? null, nzbFilename: uploaded?.upload ?? held,
              nzbRunId: usenet.nzbRunId ?? null, connections: usenet.connections ?? null, maxDurationSeconds: usenet.maxDurationSeconds,
            }
          : null,
      }, status: "RUNNING", phase: "draining 3",
      startedAt: 1790640100000, finishedAt: null, failure: null,
      tested: {
        legs: tested.legs.map((leg, position) => ({ position, egressId: leg.egressId, weight: leg.weight, down: overrides.downLegs.includes(position), downReason: overrides.downLegs.includes(position) ? "taken down for this test" : null, path: leg.path, target: overrides.downLegs.includes(position) ? 0 : 10 })),
        failover: tested.failover, overrides, names: [],
      },
      // Nothing is sampled before the run measures.
      result: { bytes: 0, elapsedMs: 0, averageBps: 0, peakBps: 0, legs: [], connections: [], usenet: null, bandwidth: null, samples: [] },
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
/** A route saved from the routes table's editor, kept as the request it would have sent. */
const action: NetworkingAction = async (_document, variables) => {
  requests.push({ name: "SaveNetworkRoute", variables: structuredClone(variables) });
  return null;
};
const fixture = {
  requests, uploads, finish: (id: number) => finished.add(id), measure,
  fail: (id: number, failure: NonNullable<SpeedTestRun["failure"]>) => failures.set(id, failure),
};
Object.assign(window, { speedFixture: fixture });

const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  if (new URL(String(request), document.baseURI).pathname !== "/graphql") return originalFetch(request, init);
  if (!(init?.body instanceof FormData)) {
    const body = JSON.parse(String(init?.body));
    return Response.json(graphql(body.operationName, body.variables));
  }
  // A multipart request, as the GraphQL multipart request spec lays it out: each file part is put
  // back where `map` says it goes.
  const form = init.body;
  const operation = JSON.parse(String(form.get("operations")));
  const map = JSON.parse(String(form.get("map"))) as Record<string, string[]>;
  for (const [part, paths] of Object.entries(map)) {
    const file = form.get(part) as File;
    const uploaded: Uploaded = { upload: file.name, text: await file.text() };
    uploads.push({ map, name: file.name, text: uploaded.text });
    for (const path of paths) {
      const keys = path.split(".");
      const parent = keys.slice(0, -1).reduce<Record<string, unknown>>((node, key) => node[key] as Record<string, unknown>, operation);
      parent[keys.at(-1)!] = uploaded;
    }
  }
  return Response.json(graphql(operation.operationName, operation.variables));
};
/* With `?routes` it is the routes table and its editor; otherwise the speed test page, opened on the server. */
const query = new URLSearchParams(window.location.search);
const screen = query.has("routes")
  ? <RoutesPage data={data} consumers={consumers} flow={{ legs: [], pools: [] }} action={action} busy={false} />
  : <SpeedPage data={data} consumers={consumers} />;
/*
 * With `?downloading` a job is downloading, so a run asks before it pauses downloads; otherwise the queue is idle.
 * With `?gauge` the header gauge reads a rate, as it can just after a Usenet run.
 */
const live = {
  isPaused: false,
  speed: query.has("gauge") ? 12 * 1024 * 1024 : 0,
  queue: { jobs: query.has("downloading") ? [{ id: 7, status: "DOWNLOADING" }] : [] } as unknown as NextData["queue"],
} as NextData;
const shell = { search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: document.getElementById("controls") };
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t, uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter initialEntries={[query.has("routes") ? "/settings/networking/routes" : "/settings/networking/speed?consumer=server:1"]}><SettingsShellProvider {...shell}><NextDataContext.Provider value={live}><main className="mx-auto flex max-w-[1400px] flex-col p-8">{screen}</main></NextDataContext.Provider></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);

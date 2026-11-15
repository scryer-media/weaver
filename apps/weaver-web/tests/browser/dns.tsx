// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { StrictMode, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { DnsProfile, DnsServer, DnsServerCapability, DnsServerList } from "@/lib/dns";
import { NextDataContext, useDnsProfileReads, type NextData } from "@/next/data/next-data";
import type { NetworkingData } from "@/next/features/networking/data";
import { DnsPage } from "@/next/features/networking/DnsPage";
import { RoutesPage } from "@/next/features/networking/RoutesPage";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

/* The fixture's daemon: its server lists and profiles, and every request it was sent. */

const plain = (address: string): DnsServer => ({ wire: "PLAIN", address, tlsName: null, url: null });
const lists: DnsServerList[] = [
  { id: 1, name: "Mixed", servers: [plain("192.0.2.53"), { wire: "TLS", address: "192.0.2.54", tlsName: "dns.fixture.invalid", url: null }], usedByProfiles: ["Checked"] },
  { id: 2, name: "Encrypted", servers: [{ wire: "HTTPS", address: "", tlsName: null, url: "https://192.0.2.55/dns-query" }], usedByProfiles: [] },
];
const route = (profile: Omit<DnsProfile, "kind" | "usedByProxies">): DnsProfile => ({ ...profile, kind: "ROUTE", usedByProxies: 0 });
const profiles: DnsProfile[] = [
  { id: 0, name: "System", builtIn: true, kind: "ROUTE", who: "SYSTEM", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false, usedByRoutes: 3, usedByProxies: 2 },
  route({ id: 2, name: "Tunnel", builtIn: false, who: "PROXY", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: false, usedByRoutes: 0 }),
  route({ id: 3, name: "Checked", builtIn: false, who: "WEAVER", transit: "PROXY", trust: "VALIDATE", serverListId: 1, systemFallback: false, usedByRoutes: 1 }),
  route({ id: 4, name: "Tunnel or system", builtIn: false, who: "PROXY", transit: null, trust: "AS_SENT", serverListId: null, systemFallback: true, usedByRoutes: 0 }),
  route({ id: 5, name: "Tunnel checked", builtIn: false, who: "PROXY", transit: null, trust: "VALIDATE", serverListId: null, systemFallback: false, usedByRoutes: 0 }),
  /* A proxy name profile one proxy finds its address with. */
  { id: 6, name: "Proxy names", builtIn: false, kind: "PROXY_NAME", who: "WEAVER", transit: "EGRESS", trust: "AS_SENT", serverListId: 1, systemFallback: false, usedByRoutes: 0, usedByProxies: 1 },
];
const requests: { name: string; variables: Record<string, unknown> }[] = [];
const refusal = (code: string, params: Record<string, unknown> = {}) => ({ errors: [{ message: code, extensions: { code, params } }] });

const proxy = (id: number, name: string, kind: string) => ({
  id, name, kind, enabled: true, host: `${name.toLowerCase()}.fixture.invalid`, port: 1080, dnsServers: [], dnsProfileId: 0, tunnelAddresses: [],
  peerPublicKey: null, tunnelPublicKey: null, mtu: 1420, keepaliveSeconds: null, timeoutSeconds: 10, hostKeyFingerprint: null, natpmp: false,
  forwardedPort: null, hasUsername: false, hasPassword: false, hasPrivateKey: kind === "WIRE_GUARD", hasPassphrase: false, hasPresharedKey: false,
});
const proxies = [proxy(7, "Relay", "SOCKS5"), proxy(8, "Mullvad", "WIRE_GUARD")];
const egresses = [{ id: 0, name: "System", bindingKind: "SYSTEM", interfaceName: null, sourceAddress: null, enabled: true, downloadBps: null, uploadBps: null, health: "UP", reason: null }];
const networking = {
  egressInterfaces: egresses, proxyProfiles: proxies, proxyPools: [], discoverNetworkInterfaces: [], servers: [], rssFeeds: [],
  platformNetworking: { platform: "fixture", egressBindingKinds: ["SYSTEM"], sourceAddressHint: null, container: false, bridgeNetworkSuspected: false, maxWireguardInstances: 4, notes: [] },
} as unknown as NetworkingData;

const yes = (millis: number) => ({ supported: true, millis, error: null });
const no = (code: string) => ({ supported: false, millis: 0, error: { code, detail: "" } });
/* What each server answers over: the first row anything but TLS, the second every wire but its own TLS. */
const capabilities = (servers: DnsServer[]): DnsServerCapability[] =>
  servers.map((server, index) => index === 0
    ? { index, plain: yes(3.2), tls: no("TLS_FAILED"), https: no("SERVER_FAILURE"), dnssec: yes(14) }
    : { index, plain: yes(4), tls: no("TLS_FAILED"), https: yes(21), dnssec: no("DNSSEC_UNAVAILABLE") });

/* Each capability test is answered only once the test lets it go, so runs can be seen in flight and answered in any order. */
const pending: (() => void)[] = [];
const held = () => new Promise<void>((resolve) => { pending.push(resolve); });
/* A list naming this server is refused by the daemon. */
const REFUSED_SERVER = "192.0.2.99";

async function graphql(name: string, variables: Record<string, unknown>) {
  requests.push({ name, variables: structuredClone(variables) });
  const input = variables.input as Record<string, unknown> | undefined;
  switch (name) {
    case "Dns":
      return { data: { dnsServerLists: lists, dnsProfiles: profiles } };
    case "DnsProfileChoices":
      return { data: { dnsProfiles: profiles.map(({ id, name: label, kind, who, trust, systemFallback }) => ({ id, name: label, kind, who, trust, systemFallback })) } };
    case "DnsProfileNames":
      return { data: { dnsProfiles: profiles.map(({ id, name: label, kind }) => ({ id, name: label, kind })) } };
    case "Networking":
    case "DraftRoute":
      return { data: networking };
    case "TestDnsServers": {
      await held();
      const servers = variables.servers as DnsServer[];
      const refused = servers.findIndex((server) => server.address === REFUSED_SERVER);
      if (refused >= 0) return refusal("PROXY_DNS_SERVER_INVALID", { index: refused });
      return { data: { testDnsServers: capabilities(servers) } };
    }
    case "RouteNames":
      // The flow has no consumers yet; one key the page cannot name is a deleted route.
      return { data: { networkFlow: { consumers: null }, servers: [{ id: 1, host: "news.fixture.invalid" }], rssFeeds: [{ id: 4, name: "Fixture feed" }], proxyProfiles: proxies.map(({ id, name: label }) => ({ id, name: label })) } };
    case "SaveDnsServerList": {
      const id = (input!.id as number | undefined) ?? Math.max(...lists.map((list) => list.id)) + 1;
      const saved = { id, name: input!.name as string, servers: input!.servers as DnsServer[], usedByProfiles: [] };
      const at = lists.findIndex((list) => list.id === id);
      if (at < 0) lists.push(saved); else lists[at] = { ...saved, usedByProfiles: lists[at]!.usedByProfiles };
      return { data: { saveDnsServerList: { id } } };
    }
    case "DeleteDnsServerList": {
      const list = lists.find((candidate) => candidate.id === variables.id);
      if (list && list.usedByProfiles.length > 0) return refusal("DNS_LIST_IN_USE", { profiles: list.usedByProfiles });
      lists.splice(lists.findIndex((candidate) => candidate.id === variables.id), 1);
      return { data: { deleteDnsServerList: true } };
    }
    case "SaveDnsProfile": {
      const id = (input!.id as number | undefined) ?? Math.max(...profiles.map((profile) => profile.id)) + 1;
      const saved = { ...(input as Omit<DnsProfile, "id" | "builtIn" | "usedByRoutes" | "usedByProxies">), id, builtIn: false, usedByRoutes: 0, usedByProxies: 0 };
      const at = profiles.findIndex((profile) => profile.id === id);
      if (at < 0) profiles.push(saved); else profiles[at] = { ...saved, usedByRoutes: profiles[at]!.usedByRoutes, usedByProxies: profiles[at]!.usedByProxies };
      return { data: { saveDnsProfile: { id } } };
    }
    case "DeleteDnsProfile": {
      const profile = profiles.find((candidate) => candidate.id === variables.id);
      if (profile && profile.usedByRoutes > 0) return refusal("DNS_PROFILE_IN_USE", { routes: ["server:1", "rss:4", "server:9"], proxies: [] });
      if (profile && profile.usedByProxies > 0) return refusal("DNS_PROFILE_IN_USE", { routes: [], proxies: [7, 99] });
      profiles.splice(profiles.findIndex((candidate) => candidate.id === variables.id), 1);
      return { data: { deleteDnsProfile: true } };
    }
    default:
      return { errors: [{ message: `the fixture does not answer ${name}` }] };
  }
}

const routeSaves: Record<string, unknown>[] = [];
const fixture = {
  requests, routeSaves,
  /** Answers the held test at `at`, the oldest first. */
  release: (at = 0) => { pending.splice(at, 1)[0]?.(); },
  testHeld: (count = 1) => pending.length >= count,
};
Object.assign(window, { dnsFixture: fixture });

/* With `?routes` it is the routes table: a server that goes direct, a feed through Relay, and a feed through Mullvad, which lists no tunnel DNS. */
const consumers = [
  { key: "server:1", id: 1, name: "news.fixture.invalid", kind: "SERVER" as const, cap: 20, route: null },
  {
    key: "rss:4", id: 4, name: "Fixture feed", kind: "RSS" as const, cap: 1,
    route: { failover: "REDISTRIBUTE" as const, legs: [{ egressId: 0, weight: 100, path: { kind: "LADDER" as const, rungs: [{ kind: "PROXY" as const, proxyId: 7, poolId: null, chainIds: [] }], directFallback: false } }] },
  },
  {
    key: "rss:5", id: 5, name: "Tunnel feed", kind: "RSS" as const, cap: 1,
    route: { failover: "REDISTRIBUTE" as const, legs: [{ egressId: 0, weight: 100, path: { kind: "LADDER" as const, rungs: [{ kind: "PROXY" as const, proxyId: 8, poolId: null, chainIds: [] }], directFallback: false } }] },
  },
];
const query = new URLSearchParams(window.location.search);
const screen = query.has("routes")
  ? <RoutesPage data={networking} consumers={consumers} flow={{ legs: [], pools: [] }} action={async (_document, variables) => { routeSaves.push(structuredClone(variables)); return null; }} busy={false} />
  : <DnsPage />;

const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  if (new URL(String(request), document.baseURI).pathname !== "/graphql") return originalFetch(request, init);
  const body = JSON.parse(String(init?.body));
  return Response.json(await graphql(body.operationName, body.variables));
};
const dictionary: Record<string, string> = { ...en, ...nextEn };
const shell = {
  search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: document.getElementById("controls"),
};
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter><SettingsShellProvider {...shell}><DnsReads><main className="mx-auto flex max-w-[1400px] flex-col sm:p-8">{screen}</main></DnsReads></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);

/* Holds the DNS profile reads as NextDataProvider does, for the editors that offer them. */
function DnsReads({ children }: { children: ReactNode }) {
  return <NextDataContext.Provider value={useDnsProfileReads() as NextData}>{children}</NextDataContext.Provider>;
}

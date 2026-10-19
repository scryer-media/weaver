// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createRoot } from "react-dom/client";
import { MemoryRouter, useLocation, useRoutes } from "react-router";
import { Client, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { Egress, EgressQuota, QuotaState } from "@/lib/networking";
import { NextDataContext, type NextData } from "@/next/data/next-data";
import type { NetworkingAction, NetworkingData } from "@/next/features/networking/data";
import { EgressPage } from "@/next/features/networking/EgressPage";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { SETTINGS_REDIRECTS } from "@/next/pages/settings/redirects";
import { AttentionBlock } from "@/next/shell/rail-blocks";
import "@/next/fonts.css";
import "@/next/theme.css";

const MIB = 1024 ** 2;
const GIB = 1024 ** 3;
const TIB = 1024 ** 4;
// The period every capped egress is in: October, resetting at midnight UTC on the first.
const periodStart = Date.UTC(2026, 9, 1);
const periodEnd = Date.UTC(2026, 10, 1);
const windowStart = periodStart;
const windowEnd = periodEnd;
const caps = (rest: Partial<EgressQuota>): EgressQuota => ({
  downloadBytes: null, uploadSoftBytes: null, uploadTrickleBps: 2048 * 1024, uploadHardBytes: null,
  period: "MONTHLY", resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON", monthlyResetDay: 1, ...rest,
});
const spent = (usedBytes: number, capBytes: number | null, state: QuotaState = "NORMAL") => ({
  usedBytes, remainingBytes: capBytes === null ? null : Math.max(0, capBytes - usedBytes), state,
});
const egress = (id: number, name: string, rest: Partial<Egress>): Egress => ({
  id, name, bindingKind: "SOURCE_ADDRESS", interfaceName: null, sourceAddress: `192.0.2.${10 + id}`, addresses: [], enabled: true,
  downloadBps: null, uploadBps: null, health: "UP", reason: null, quota: caps({}),
  quotaUsage: { down: spent(0, null), up: spent(0, null), state: "NORMAL", periodStartEpochMs: null, periodEndEpochMs: null, timezoneName: "UTC" },
  ...rest,
});
// System last, as a daemon may list it; the table puts it first.
const egresses: Egress[] = [
  egress(1, "WAN Fiber", {
    uploadBps: 5 * MIB,
    quota: caps({ downloadBytes: TIB }),
    quotaUsage: { down: spent(400 * GIB, TIB), up: spent(30 * GIB, null), state: "NORMAL", periodStartEpochMs: periodStart, periodEndEpochMs: periodEnd, timezoneName: "UTC" },
  }),
  egress(2, "LTE standby", {
    downloadBps: 2 * MIB,
    quota: caps({ downloadBytes: 20 * GIB, uploadHardBytes: 2 * GIB }),
    quotaUsage: { down: spent(20 * GIB, 20 * GIB, "STOPPED"), up: spent(GIB, 2 * GIB), state: "STOPPED", periodStartEpochMs: periodStart, periodEndEpochMs: periodEnd, timezoneName: "UTC" },
    health: "DOWN", reason: "Quota reached",
  }),
  egress(0, "System", { bindingKind: "SYSTEM", sourceAddress: null }),
];
const data: NetworkingData = {
  egressInterfaces: egresses, proxyProfiles: [], proxyPools: [], discoverNetworkInterfaces: [],
  platformNetworking: { platform: "fixture", egressBindingKinds: ["SYSTEM", "INTERFACE", "SOURCE_ADDRESS"], sourceAddressHint: null, container: false, bridgeNetworkSuspected: false, maxWireguardInstances: 1, notes: [] },
  servers: [], rssFeeds: [],
};

const saved: { name: string; variables: Record<string, unknown> }[] = [];
const action: NetworkingAction = async (document, variables) => {
  const definition = document.definitions.find((entry) => entry.kind === "OperationDefinition");
  saved.push({ name: definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "", variables });
  return null;
};
// The global limits the daemon holds, one per direction, and every write the page sends through the client.
let bandwidth: { downloadBps: number | null; uploadBps: number | null } = { downloadBps: 25 * MIB, uploadBps: null };
const sent: { name: string; variables: Record<string, unknown> }[] = [];
const actionsRef: { current: { save: () => void; revert: () => void } | null } = { current: null };
Object.assign(window, { egressQuotaFixture: { saved, sent, actions: actionsRef } });

// The page's reads and the limits' save, answered at once from the fixture.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  if (operation.kind === "mutation") sent.push({ name, variables: operation.variables as Record<string, unknown> });
  if (name === "SetBandwidth") bandwidth = (operation.variables as { input: typeof bandwidth }).input;
  const answer = name === "EgressQuotas" ? { egressInterfaces: egresses }
    : name === "Bandwidth" ? { networking: { bandwidth } }
    : name === "SetBandwidth" ? { setBandwidth: bandwidth }
    : {};
  return fromValue({ operation, data: answer });
}))] });

// What the rail reads while the metered egress has spent its allowance.
const blockedBy = (windowEndsAtEpochMs: number | null) => ({
  version: "fixture", update: undefined, speed: 0, peakSpeed: 0, isPaused: false, speedLimit: 0,
  downloadBlock: {
    kind: "EGRESS_QUOTA", egressId: 2, egressName: "LTE standby", usedBytes: 20 * GIB, limitBytes: 20 * GIB, remainingBytes: 0,
    windowStartsAtEpochMs: windowStart, windowEndsAtEpochMs, timezoneName: "UTC", scheduledSpeedLimit: 0,
  },
  queue: {} as NextData["queue"], categories: [], refreshCategories: () => {}, historyCount: 0, refreshHistoryCount: () => {},
  providers: [], providersLoaded: true, holdoffs: [], connection: { status: "connected", isDisconnected: false, isPolling: false },
}) as NextData;

const query = new URLSearchParams(location.search);
const screen = query.get("screen") ?? "egress";
const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: query.get("search") ?? "", actionsRef, controlsHost: document.getElementById("controls"), registry: new Map(),
  setFlags: (flags: { dirty: boolean; status: string | null }) => { status.textContent = `${flags.dirty ? "dirty" : "clean"} ${flags.status ?? ""}`.trim(); },
};

/** Where a settings address lands once the route table has had its say. */
function Landed() {
  return <output aria-label="Landed at">{useLocation().pathname}</output>;
}

function Redirects() {
  return useRoutes([{ path: "settings", children: [...SETTINGS_REDIRECTS, { path: ":panel/*", element: <Landed /> }] }]);
}

function Fixture() {
  if (screen === "banner" || screen === "banner-open-ended") {
    const block = blockedBy(screen === "banner" ? windowEnd : null);
    return <NextDataContext.Provider value={block}><aside style={{ width: 300 }}><AttentionBlock /></aside></NextDataContext.Provider>;
  }
  if (screen === "redirect") return <Redirects />;
  return <EgressPage data={data} egresses={egresses} action={action} busy={false} />;
}

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter initialEntries={[query.get("from") ?? "/"]}><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><Fixture /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

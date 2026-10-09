import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { Egress } from "@/lib/networking";
import { NextDataContext, type NextData } from "@/next/data/next-data";
import type { NetworkingAction, NetworkingData } from "@/next/features/networking/data";
import { EgressPage } from "@/next/features/networking/EgressPage";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { BandwidthPanel } from "@/next/pages/settings/panels/BandwidthPanel";
import { AttentionBlock } from "@/next/shell/rail-blocks";
import "@/next/fonts.css";
import "@/next/theme.css";

const GIB = 1024 ** 3;
const TIB = 1024 ** 4;
// The window every metered egress is in: October, resetting at midnight UTC on the first.
const windowStart = Date.UTC(2026, 9, 1);
const windowEnd = Date.UTC(2026, 10, 1);
const monthly = (limitBytes: number) => ({
  enabled: true, period: "MONTHLY" as const, limitBytes, resetTimeMinutesLocal: 0, weeklyResetWeekday: "MON" as const, monthlyResetDay: 1,
});
const usage = (usedBytes: number, limitBytes: number | null, blocked = false) => ({
  usedBytes, reservedBytes: 0, remainingBytes: limitBytes === null ? null : Math.max(0, limitBytes - usedBytes), blocked,
  windowStartsAtEpochMs: limitBytes === null ? null : windowStart, windowEndsAtEpochMs: limitBytes === null ? null : windowEnd, timezoneName: "UTC",
});
const off = { ...monthly(0), enabled: false };
const egress = (id: number, name: string, rest: Partial<Egress>): Egress => ({
  id, name, bindingKind: "SOURCE_ADDRESS", interfaceName: null, sourceAddress: `192.0.2.${10 + id}`, addresses: [], enabled: true,
  maxDownloadSpeed: 0, health: "UP", reason: null, downloadQuota: off, downloadQuotaUsage: usage(0, null), ...rest,
});
const egresses: Egress[] = [
  egress(0, "System", { bindingKind: "SYSTEM", sourceAddress: null, downloadQuotaUsage: usage(3 * GIB, null) }),
  egress(1, "WAN Fiber", { downloadQuota: monthly(TIB), downloadQuotaUsage: usage(400 * GIB, TIB) }),
  egress(2, "LTE standby", {
    downloadQuota: monthly(20 * GIB), downloadQuotaUsage: usage(20 * GIB, 20 * GIB, true), health: "DOWN", reason: "Quota reached",
  }),
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
Object.assign(window, { egressQuotaFixture: { saved } });

// The Bandwidth panel's reads, answered at once from the fixture.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  const answer = name === "EgressQuotas" ? { egressInterfaces: egresses }
    : name === "Settings" ? { settings: { maxDownloadSpeed: 0 }, globalState: null }
    : {};
  return fromValue({ operation, data: answer });
}))] });

// What the rail reads while the metered egress has spent its allowance.
const blocked = {
  version: "fixture", update: undefined, speed: 0, peakSpeed: 0, isPaused: false, speedLimit: 0,
  downloadBlock: {
    kind: "EGRESS_QUOTA", egressId: 2, egressName: "LTE standby", usedBytes: 20 * GIB, limitBytes: 20 * GIB, remainingBytes: 0,
    windowStartsAtEpochMs: windowStart, windowEndsAtEpochMs: windowEnd, timezoneName: "UTC", scheduledSpeedLimit: 0,
  },
  queue: {} as NextData["queue"], categories: [], refreshCategories: () => {}, historyCount: 0, refreshHistoryCount: () => {},
  providers: [], providersLoaded: true, holdoffs: [], connection: { status: "connected", isDisconnected: false, isPolling: false },
} as NextData;

const screen = new URLSearchParams(location.search).get("screen") ?? "egress";
const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: "", actionsRef: { current: null }, controlsHost: document.getElementById("controls"),
  setFlags: (flags: { status: string | null }) => { status.textContent = flags.status ?? ""; },
};

function Fixture() {
  if (screen === "banner") {
    return <NextDataContext.Provider value={blocked}><aside style={{ width: 300 }}><AttentionBlock /></aside></NextDataContext.Provider>;
  }
  if (screen === "bandwidth") return <BandwidthPanel />;
  return <EgressPage data={data} egresses={egresses} action={action} busy={false} />;
}

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><Fixture /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

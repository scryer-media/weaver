// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, type OperationResult } from "urql";
import { fromValue, make, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { NextDataContext, type NextData } from "@/next/data/next-data";
import { torrentsStateFrom, type TorrentPresence } from "@/next/data/torrents-shown";
import { nextEn } from "@/next/i18n/en";
import { DownloadsPage } from "@/next/pages/DownloadsPage";
import { SettingsShellProvider, type PanelFlags } from "@/next/pages/settings/framework";
import { SearchRegistry } from "@/next/pages/settings/search";
import { TorrentsPanel } from "@/next/pages/settings/panels/TorrentsPanel";
import "@/next/fonts.css";
import "@/next/theme.css";

const query = new URLSearchParams(location.search);
// Downloads or the Torrents settings panel.
const screen = query.get("screen") ?? "downloads";
// What the server says of torrents: off with none left, off with some still seeding, or on with some running or none.
const PRESENCES: Record<string, TorrentPresence> = {
  off: { available: true, enabled: false, jobs: false, running: false, windingDown: false, windDown: null },
  offJobs: { available: true, enabled: false, jobs: true, running: true, windingDown: true, windDown: "FINISH_ALL" },
  offFinishDownloads: { available: true, enabled: false, jobs: true, running: true, windingDown: true, windDown: "FINISH_DOWNLOADS" },
  onRunning: { available: true, enabled: true, jobs: true, running: true, windingDown: false, windDown: null },
  // A server started without torrents, whatever its saved setting and rows say.
  unavailable: { available: false, enabled: true, jobs: true, running: false, windingDown: false, windDown: null },
  onIdle: { available: true, enabled: true, jobs: true, running: false, windingDown: false, windDown: null },
};
// "unknown" is the server not having said yet: torrents on, as the settings read.
const unknown = query.get("presence") === "unknown";
const presence: TorrentPresence | undefined = unknown ? undefined : PRESENCES[query.get("presence") ?? "off"] ?? PRESENCES.off;

const sent: { name: string; variables: Record<string, unknown> }[] = [];
const flags: PanelFlags[] = [];
const actionsRef: { current: { save: () => void; revert: () => void } | null } = { current: null };
Object.assign(window, { torrentsOffFixture: { sent, flags, actions: actionsRef } });

// The panel's settings, with torrents on or off as the presence says.
let settings = { seeding: null, uploadCap: null, torrent: { enabled: presence?.enabled ?? true } };
// An empty queue: Downloads shows its empty state.
const emptyPage = {
  items: [], totalCount: 0, categoryCounts: [], protocolCounts: [], stateCounts: [], remainingBytes: 0,
  etaTotalBytes: 0, etaDownloadedBytes: 0, etaThrough: [], latestCursor: "0",
};

// Reads are answered at once. Subscriptions stay open; the queue page's pushes the
// empty page once, as the server's first push is the page as it stands, and the
// others push nothing.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  if (operation.kind === "teardown") return make<OperationResult>((observer) => { observer.complete(); return () => {}; });
  if (operation.kind === "subscription") return make<OperationResult>((observer) => {
    if (name === "QueuePageUpdates") observer.next({ operation, data: { queuePageUpdates: emptyPage }, stale: false, hasNext: true });
    return () => {};
  });
  const variables: Record<string, unknown> = operation.variables ?? {};
  if (operation.kind === "mutation") {
    sent.push({ name, variables });
    if (name === "UpdateTorrentSettings") {
      const input = variables.input as { torrent?: { enabled?: boolean } };
      settings = { ...settings, torrent: { ...settings.torrent, ...(input.torrent?.enabled === undefined ? {} : { enabled: input.torrent.enabled }) } };
      return fromValue({ operation, data: { updateSettings: settings }, stale: false, hasNext: false });
    }
    return fromValue({ operation, data: {}, stale: false, hasNext: false });
  }
  const data = name === "TorrentSettings" ? { settings }
    : name === "TrackerRules" ? { trackerRules: [] }
    : name === "TrackerHosts" ? { trackerHosts: [] }
    : name === "TorrentRoutes" ? { torrentRoutes: [] }
    : name === "QueuePage" ? { queuePage: emptyPage }
    : name === "SystemInfo" ? { systemInfo: { configuredStorage: [] } }
    : {};
  return fromValue({ operation, data, stale: false, hasNext: false });
}))] });

// The shell's data, as it reads with this presence and an empty queue.
const nextData: NextData = {
  version: "0.0.0", update: undefined, speed: 0, peakSpeed: 0, isPaused: false, pausedClasses: [],
  torrents: torrentsStateFrom(presence), torrentPresence: presence, resumeAt: null, speedLimit: null,
  downloadBlock: {
    kind: "NONE", egressId: null, egressName: null, usedBytes: 0, limitBytes: 0, remainingBytes: 0,
    windowStartsAtEpochMs: null, windowEndsAtEpochMs: null, timezoneName: "", scheduledSpeedLimit: null,
    speedLimitInForce: null, speedLimitBySchedule: false,
  },
  queue: {
    summary: { totalItems: 0, queuedItems: 0, activeItems: 0, pausedItems: 0 }, stateCounts: [], categories: [],
    totalCount: 0, isLoading: false, error: null, refresh: () => {},
  },
  categories: [], refreshCategories: () => {}, historyCount: 0, refreshHistoryCount: () => {},
  providers: [], providersLoaded: true, holdoffs: [],
  connection: { status: "connected", isDisconnected: false, isPolling: false },
};

const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: "", actionsRef, controlsHost: document.getElementById("controls"), registry: new SearchRegistry(),
  setFlags: (next: PanelFlags) => {
    flags.push(next);
    status.textContent = next.dirty ? "dirty" : "clean";
  },
};

function Fixture() {
  if (screen === "panel") return <SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><TorrentsPanel /></main></SettingsShellProvider>;
  return <DownloadsPage />;
}

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><NextDataContext.Provider value={nextData}><Fixture /></NextDataContext.Provider></MemoryRouter></TranslateContext.Provider></Provider>,
);

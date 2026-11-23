// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createRoot } from "react-dom/client";
import { MemoryRouter, Route, Routes } from "react-router";
import { Client, CombinedError, Provider, type OperationResult } from "urql";
import { filter, fromValue, make, mergeMap, pipe, share, takeUntil } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { NextDataContext, type NextData } from "@/next/data/next-data";
import { torrentsStateFrom, type TorrentPresence } from "@/next/data/torrents-shown";
import { nextEn } from "@/next/i18n/en";
import { MonitoringRoute } from "@/next/pages/monitoring/dashboards";
import "@/next/fonts.css";
import "@/next/theme.css";

// The dashboard Monitoring opens on.
const start = new URLSearchParams(location.search).get("dashboard") ?? "integrity";
const presence: TorrentPresence = { available: true, enabled: true, jobs: false, running: false, windingDown: false, windDown: null };
const asked: string[] = [];
Object.assign(window, { monitoringFixture: { asked } });

// Every dashboard's read is answered at once, with nothing recorded; a
// subscription stays open and pushes nothing.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => {
  // A read torn down before its answer gets none, as with a fetch.
  const shared = pipe(operations, share);
  return pipe(shared, filter((operation) => operation.kind !== "teardown"), mergeMap((operation) => {
    const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
    const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
    if (operation.kind === "subscription") return make<OperationResult>(() => () => {});
    asked.push(name);
    const teardown = pipe(shared, filter((next) => next.kind === "teardown" && next.key === operation.key));
    if (name === "Networking" || name === "NetworkFlow") {
      return pipe(fromValue({ operation, error: new CombinedError({ graphQLErrors: [{ message: "forbidden" }] }) } as OperationResult), takeUntil(teardown));
    }
    const data = name === "SystemInfo" || name === "StorageVolumes" || name === "MonitoringCompute" ? { systemInfo: { configuredStorage: [], uptimeSeconds: 0, version: "0.0.0", compute: null } } : {};
    // Answered a moment later, as a fetch is.
    return pipe(
      make<OperationResult>((observer) => { const timer = setTimeout(() => { observer.next({ operation, data, stale: false, hasNext: false }); observer.complete(); }, 5); return () => clearTimeout(timer); }),
      takeUntil(teardown),
    );
  }));
}] });

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
  providers: [], providersLoaded: true, holdoffs: [], diskVolumes: [],
  connection: { status: "connected", isDisconnected: false, isPolling: false },
};

const dictionary = { ...en, ...nextEn } as Record<string, string>;

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter initialEntries={[`/monitoring/${start}`]}><NextDataContext.Provider value={nextData}>
    <Routes><Route path="/monitoring/:dashboard?" element={<MonitoringRoute />} /></Routes>
  </NextDataContext.Provider></MemoryRouter></TranslateContext.Provider></Provider>,
);

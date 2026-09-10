import { StrictMode, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, type OperationResult } from "urql";
import { fromValue, make, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import { LiveDataProvider, useLiveData } from "@/lib/context/live-data-context";
import en from "@/lib/i18n/locales/en";
import { JobList } from "@/pages/JobList";
import "@/fonts.css";
import "@/globals.css";

const cursor = (sequence: number) => btoa(`evt:${sequence}`);
let sequence = 0;
const listeners = new Set<(event: unknown) => void>();
const speedListeners = new Set<() => void>();
let speed = 9_000_000;
function subscribeSpeed(listener: () => void) {
  speedListeners.add(listener);
  return () => { speedListeners.delete(listener); };
}
function LiveFixture({ children }: { children: React.ReactNode }) {
  const defaults = useLiveData();
  const currentSpeed = useSyncExternalStore(subscribeSpeed, () => speed);
  return <LiveDataProvider {...defaults} speed={currentSpeed}>{children}</LiveDataProvider>;
}

function job(id: number, phase: "MOVING" | "DOWNLOADING", percent = 0, rate = 0) {
  const name = phase === "MOVING" ? "Moving fixture" : "Download fixture";
  return {
    id, name, displayTitle: name, originalTitle: name,
    status: phase === "MOVING" ? "FINALIZING" : phase,
    progressPercent: phase === "MOVING" ? 100 : percent,
    totalBytes: 1_000_000_000, downloadedBytes: phase === "MOVING" ? 1_000_000_000 : percent * 10_000_000,
    failedBytes: 0, health: 1000, hasPassword: false, category: "anime", metadata: [],
    phaseProgress: [{
      phase, completedBytes: percent * 10_000_000, totalBytes: 1_000_000_000,
      progressPercent: percent, rateBps: rate || null, estimatedRemainingMs: null,
      startedAtEpochMs: 0, updatedAtEpochMs: sequence,
    }],
  };
}

const items = [job(1, "DOWNLOADING", 5, 9_000_000), job(2, "MOVING")];
const client = new Client({
  url: "http://fixture.invalid/graphql",
  exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
    if (operation.kind === "subscription") {
      return make<OperationResult>((observer) => {
        const receive = (event: unknown) => observer.next({ operation, data: { queueEvents: event }, hasNext: true });
        listeners.add(receive);
        return () => { listeners.delete(receive); };
      });
    }
    return fromValue({ operation, data: {
      hasConfiguredServers: true, categories: [{ id: 1, name: "anime" }],
      queuePage: {
        items: [...items], totalCount: items.length, categories: ["anime"], latestCursor: cursor(sequence),
        summary: { totalItems: items.length, activeItems: items.length, queuedItems: 0, pausedItems: 0 },
      },
    } });
  }))],
});

function emit(id: number, phase: "MOVING" | "DOWNLOADING", percent: number, rate: number) {
  sequence += 1;
  const item = job(id, phase, percent, rate);
  items[id - 1] = item;
  const event = { cursor: cursor(sequence), kind: "ITEM_PROGRESS", itemId: id, item };
  for (const receive of listeners) receive(event);
  if (phase === "DOWNLOADING") {
    speed = rate;
    for (const notify of speedListeners) notify();
  }
}

Object.assign(window, {
  queueFixture: {
    ready: () => listeners.size > 0,
    // Both notifications arrive before React paints, as they do on a phase tick.
    burst: (moving: number, downloading: number, rate: number) => {
      emit(2, "MOVING", moving, 10_000_000);
      emit(1, "DOWNLOADING", downloading, rate);
    },
    download: (percent: number, rate: number) => emit(1, "DOWNLOADING", percent, rate),
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Provider value={client}>
      <TranslateContext.Provider value={{
        t: (key) => en[key] ?? key,
        uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" },
        setLanguagePreference: () => {},
      }}>
        <LiveFixture><MemoryRouter>
          <div className="flex h-screen overflow-hidden">
            <aside className="hidden w-[248px] shrink-0 min-[1600px]:block" />
            <main className="min-w-0 flex-1 overflow-y-auto">
              <div className="mx-auto w-full max-w-[2080px] p-8"><JobList /></div>
            </main>
          </div>
        </MemoryRouter></LiveFixture>
      </TranslateContext.Provider>
    </Provider>
  </StrictMode>,
);

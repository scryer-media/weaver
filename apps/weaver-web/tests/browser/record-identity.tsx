import { act, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter, Route, Routes, useNavigate } from "react-router";
import { Client, Provider, type OperationResult } from "urql";
import { fromValue, make, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { ProvidersPanel } from "@/next/pages/settings/panels/ProvidersPanel";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { JobDetailPage } from "@/next/pages/JobDetailPage";
import { DirectoryBrowserDialog } from "@/next/features/DirectoryBrowserDialog";
import "@/next/fonts.css";
import "@/next/theme.css";

const params = new URLSearchParams(location.search);
const screen = params.get("screen") ?? "providers";
const quota = { enabled: false, period: "MONTHLY", limitBytes: 1000, resetTimeMinutesLocal: 0,
  weeklyResetWeekday: "MON", monthlyResetDay: 1, usedBytes: 0, reservedBytes: 0,
  remainingBytes: 1000, blocked: false, timezoneName: "UTC" };
const servers = [1, 2].map((id) => ({ id, host: `provider-${id}.example`, username: `account-${id}`,
  port: 563, tls: true, connections: id * 10, active: true, priority: 0, backfill: false,
  retentionDays: 0, maxDownloadSpeed: 0, downloadQuota: quota,
  routing: { proxyIds: [], allowDirect: true },
  tlsNameMismatchCertificateDerBase64: null, tlsNameMismatchCertificateFingerprint: null }));
function snapshot(id: number) {
  return { queueItem: null, historyItem: { id, name: `Job-${id}`, displayTitle: `Job-${id}`,
    originalTitle: `Job-${id}`, status: "COMPLETED", progressPercent: 100, totalBytes: 100,
    downloadedBytes: 100, failedBytes: 0, health: 1000, category: null, metadata: [],
    phaseProgress: [], hasPassword: false, outputDir: `/job-${id}`,
    parsedRelease: { languagesAudio: [], languagesSubtitles: [] } },
    jobEvents: [], jobTimeline: null, serverAttribution: [] };
}
const listing = (path: string) => ({ currentPath: path, parentPath: "/", entries: [] });
const pending: { name: string; variables: Record<string, unknown>; deliver: () => void }[] = [];
const mutations: { name: string; variables: Record<string, unknown> }[] = [];
const delivered: string[] = [];
const held = new Set<string>();
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) =>
  pipe(operations, mergeMap((operation) => {
    const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
    const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
    if (operation.kind === "teardown") return make<OperationResult>((observer) => { observer.complete(); return () => {}; });
    const variables = operation.variables;
    if (operation.kind === "mutation") mutations.push({ name, variables });
    const id = Number(variables.id ?? variables.jobId ?? 1);
    const data = name === "Server" ? { server: servers.find((server) => server.id === id) }
      : name === "UpdateServer" ? { updateServer: { ...servers[id - 1], ...variables.input as object } }
      : name === "AddServer" ? { addServer: { ...servers[0], ...variables.input as object, id: 3 } }
      : name === "TestConnection" ? { testConnection: { success: false, message: "Result for provider 1", latencyMs: null,
          adoptableTlsNameMismatchCertificate: { derBase64: "old-certificate", sha256Fingerprint: "old-fingerprint", names: ["provider-1.example"] } } }
      : name === "BrowseDirectories" ? { browseDirectories: listing(String(variables.path)) }
      : name === "CreateDirectory" ? { createDirectory: listing(`${variables.path}/${variables.name}`) }
      : name === "JobDetailUpdates" ? { jobDetailUpdates: snapshot(id) }
      : name === "Job" ? { jobDetailSnapshot: snapshot(id) }
      : name === "JobOutputFiles" ? { jobOutputFiles: { outputDir: `/job-${id}`, files: [], totalBytes: 0 } }
      : name === "DuplicateSnapshot" ? { duplicateSnapshot: null }
      : name === "Networking" ? { egressInterfaces: [{ id: 0, name: "System", bindingKind: "SYSTEM", interfaceName: null, sourceAddress: null, addresses: [], enabled: true, maxDownloadSpeed: 0, health: "UP", reason: null }], proxyProfiles: [], proxyPools: [], servers: [], rssFeeds: [], discoverNetworkInterfaces: [], platformNetworking: { platform: "fixture", egressBindingKinds: ["SYSTEM"], sourceAddressHint: null, container: false, bridgeNetworkSuspected: false, maxWireguardInstances: 1, notes: [] } }
      : name === "NetworkFlow" || name === "NetworkFlowUpdates" ? { networkFlow: { legs: [], sampledAt: 1767225600, consumers: [], proxies: [], proxyPools: [], egresses: [], pools: [] } }
      : { servers, proxies: [], categories: [], schedules: [], generalSettings: {} };
    // Real urql hooks receive no new result until the test explicitly delivers it.
    if (held.has(name) || (id === 2 && ["Server", "Job", "JobDetailUpdates", "JobOutputFiles", "DuplicateSnapshot"].includes(name))) {
      return make<OperationResult>((observer) => {
        pending.push({ name, variables, deliver: () => {
          delivered.push(name);
          observer.next({ operation, data, hasNext: operation.kind === "subscription" });
          if (operation.kind !== "subscription") observer.complete();
        } });
        return () => {};
      });
    }
    return fromValue({ operation, data });
  }))],
});

const fixture = {
  ready: false, pending, mutations, delivered, chosen: "", navigate: (_path: string) => {},
  openFolder: (_path: string) => {},
  hold: (name: string) => held.add(name),
  async release(name: string) {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
    await act(async () => {
      held.delete(name);
      for (const request of pending.splice(0)) {
        if (request.name === name) request.deliver(); else pending.push(request);
      }
    });
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: false });
  },
};
Object.assign(window, { recordFixture: fixture });
function Fixture() {
  const navigate = useNavigate();
  const [folder, setFolder] = useState<string | null>(null);
  useEffect(() => {
    fixture.navigate = navigate;
    fixture.openFolder = setFolder;
    fixture.ready = true;
  }, [navigate]);
  if (screen === "folders") {
    const choose = (path: string) => { fixture.chosen = path; setFolder(null); };
    return <DirectoryBrowserDialog open={folder !== null} initialPath={folder} onClose={() => setFolder(null)} onChoose={choose} />;
  }
  if (screen === "jobs") return <Routes><Route path="/jobs/:id" element={<JobDetailPage />} /></Routes>;
  return <SettingsShellProvider search="" actionsRef={{ current: null }} setFlags={() => {}} controlsHost={document.getElementById("controls")}><ProvidersPanel /></SettingsShellProvider>;
}
createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{${name}}`, String(value)), en[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter initialEntries={["/jobs/1"]}><Fixture /></MemoryRouter></TranslateContext.Provider></Provider>,
);

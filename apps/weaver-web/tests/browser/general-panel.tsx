import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { GeneralPanel } from "@/next/pages/settings/panels/GeneralPanel";
import "@/next/fonts.css";
import "@/next/theme.css";

const settings = {
  dataDir: "/data", intermediateDir: "", completeDir: "", cleanupAfterExtract: true, maxRetries: 3,
  propagationDelaySecs: 0, enableSrrdbLookup: false, duplicatePolicy: {},
};
// A list is stored: the daemon says so, and never sends the list itself.
const archive = { hasPasswords: true, passwordFile: null };
// The unwanted extension list, saved apart from the rest and normalized as the daemon does.
let extensions = ["bat", "cmd", "com", "exe", "js", "lnk", "msi", "ps1", "scr", "vbs"];
// A machine that can run more than one hardware profile, so the panel offers the choice.
const option = (profile: string, threads: number) => ({
  profile, sevenzDecodeMemoryBytes: threads * 256 * 1024 ** 2, decodeThreads: threads, extractThreads: threads, maxConcurrentDownloads: null,
});
const hardwareProfile = {
  selected: "PERFORMANCE", active: "PERFORMANCE", scheduled: null, recommended: "BALANCED",
  available: ["EFFICIENT", "BALANCED", "PERFORMANCE"], options: [option("EFFICIENT", 2), option("BALANCED", 4), option("PERFORMANCE", 8)],
  detected: { memoryBytes: 32 * 1024 ** 3, cores: 10 },
};

const sent: { name: string; variables: Record<string, unknown> }[] = [];
const actionsRef: { current: { save: () => void; revert: () => void } | null } = { current: null };
Object.assign(window, { generalPanelFixture: { sent, actions: actionsRef } });

// The General panel's reads and saves, answered at once from the fixture.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  if (operation.kind === "mutation") sent.push({ name, variables: operation.variables as Record<string, unknown> });
  const answer = name === "Settings" ? { settings, globalState: null }
    : name === "ArchivePasswordSettings" ? { archivePasswordSettings: archive }
    : name === "HardwareProfile" ? { hardwareProfile }
    : name === "UpdateSettings" ? { updateSettings: settings }
    : name === "UpdateArchivePasswordSettings" ? { updateArchivePasswordSettings: archive }
    : name === "UnacceptableExtensions" ? { postProcessingSettings: { unacceptableExtensions: extensions } }
    : name === "SetUnacceptableExtensions" ? { setUnacceptableExtensions: (extensions = [...new Set((operation.variables as { extensions: string[] }).extensions.map((entry) => entry.toLowerCase()))].sort()) }
    : {};
  return fromValue({ operation, data: answer });
}))] });

const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: "", actionsRef, controlsHost: document.getElementById("controls"), registry: new Map(),
  setFlags: (flags: { dirty: boolean; status: string | null }) => { status.textContent = `${flags.dirty ? "dirty" : "clean"} ${flags.status ?? ""}`.trim(); },
};

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><GeneralPanel /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, CombinedError, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { nextEn } from "@/next/i18n/en";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { GeneralPanel } from "@/next/pages/settings/panels/GeneralPanel";
import "@/next/fonts.css";
import "@/next/theme.css";

let settings = {
  dataDir: "/data", intermediateDir: "", completeDir: "", cleanupAfterExtract: true, maxRetries: 3,
  propagationDelaySecs: 0, enableSrrdbLookup: false,
  duplicatePolicy: {
    strictActiveOrSuccess: "BLOCK", strictFailedOrCancelled: "WARN", articleLayoutActiveOrSuccess: "PAUSE",
    articleLayoutFailedOrCancelled: "WARN", articleSet: "WARN", normalizedName: "WARN",
  },
};
// A list is stored: the daemon says so, and never sends the list itself.
let archive: { hasPasswords: boolean; passwordFile: string | null } = { hasPasswords: true, passwordFile: "/data/archive-passwords.txt" };
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
// Every state the panel gave the top bar, in order.
const flags: { dirty: boolean; status: string | null; failed: boolean }[] = [];
// A mutation named here is refused with that message instead of stored.
const refuse: Record<string, string> = {};
const actionsRef: { current: { save: () => void; revert: () => void } | null } = { current: null };
Object.assign(window, { generalPanelFixture: { sent, flags, refuse, actions: actionsRef } });

// What each save stores, applied as the daemon applies it, and what it answers with.
function store(name: string, variables: Record<string, unknown>): unknown {
  if (name === "UpdateSettings") {
    const input = variables.input as Record<string, unknown>;
    const { duplicatePolicy, ...rest } = input;
    settings = { ...settings, ...rest, duplicatePolicy: { ...settings.duplicatePolicy, ...(duplicatePolicy as object | undefined) } };
    return { updateSettings: settings };
  }
  if (name === "UpdateArchivePasswordSettings") {
    const { passwords, passwordFile } = variables as { passwords?: string[]; passwordFile?: string | null };
    archive = {
      hasPasswords: passwords === undefined ? archive.hasPasswords : passwords.some((entry) => entry.trim() !== ""),
      passwordFile: passwordFile === undefined ? archive.passwordFile : passwordFile,
    };
    return { updateArchivePasswordSettings: archive };
  }
  if (name === "SetUnacceptableExtensions") {
    extensions = [...new Set((variables as { extensions: string[] }).extensions.map((entry) => entry.toLowerCase()))].sort();
    return { setUnacceptableExtensions: extensions };
  }
  return {};
}

// The General panel's reads and saves, answered at once from the fixture.
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  if (operation.kind === "mutation") {
    const variables = operation.variables as Record<string, unknown>;
    sent.push({ name, variables });
    if (refuse[name] !== undefined) {
      return fromValue({ operation, data: undefined, error: new CombinedError({ graphQLErrors: [{ message: refuse[name] }] }) });
    }
    const argumentsForStore = { ...variables };
    if (name === "UpdateArchivePasswordSettings" && definition?.kind === "OperationDefinition") {
      const field = definition.selectionSet.selections.find(entry => entry.kind === "Field" && entry.name.value === "updateArchivePasswordSettings");
      // Match the daemon: a referenced, absent file variable resolves to
      // null; only omitting the argument itself leaves the stored file alone.
      if (field?.kind === "Field" && field.arguments?.some(argument => argument.name.value === "passwordFile") && !("passwordFile" in variables)) {
        argumentsForStore.passwordFile = null;
      }
    }
    return fromValue({ operation, data: store(name, argumentsForStore) });
  }
  const answer = name === "Settings" ? { settings, globalState: null }
    : name === "ArchivePasswordSettings" ? { archivePasswordSettings: archive }
    : name === "HardwareProfile" ? { hardwareProfile }
    : name === "UnacceptableExtensions" ? { postProcessingSettings: { unacceptableExtensions: extensions } }
    : {};
  return fromValue({ operation, data: answer });
}))] });

const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: "", actionsRef, controlsHost: document.getElementById("controls"), registry: new Map(),
  setFlags: (next: { dirty: boolean; status: string | null; failed: boolean }) => {
    flags.push(next);
    status.textContent = `${next.dirty ? "dirty" : "clean"} ${next.status ?? ""}`.trim();
  },
};

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><GeneralPanel /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

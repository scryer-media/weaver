// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, type OperationResult } from "urql";
import { fromValue, make, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { SecurityPanel } from "@/next/pages/settings/panels/SecurityPanel";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

// The Security panel with the API key the deep link in the address bar mints.
// `lan=1` takes the asynchronous clipboard away first, which is what a browser
// does on its own for a Weaver reached over plain HTTP at a LAN address.
const params = new URLSearchParams(location.search);
if (params.get("lan") === "1") {
  Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
}

export const RAW_KEY = "wv_fixture_0123456789abcdefghijklmnopqrstuv";

// Every selection command is recorded with the text the focused control had selected.
const commands: { command: string; text: string }[] = [];
const execCommand = document.execCommand.bind(document);
document.execCommand = (command: string, ...rest: unknown[]) => {
  const control = document.activeElement as HTMLTextAreaElement | HTMLInputElement | null;
  const text =
    control && typeof control.selectionStart === "number" && typeof control.selectionEnd === "number"
      ? control.value.slice(control.selectionStart, control.selectionEnd)
      : "";
  commands.push({ command, text });
  return (execCommand as (command: string, ...args: unknown[]) => boolean)(command, ...rest);
};
Object.assign(window, { copyFixture: { commands, rawKey: RAW_KEY } });

const bindAddress = { address: "0.0.0.0:9090", storedAddress: null, source: "default", editable: true,
  exposedWithoutLogin: false, restartRequired: false, bindFallback: null };
const networkAccess = { authenticatedAccess: true, legacyCompatibility: false, trustedNetworks: [], trustedProxies: [],
  trustedNetworksSource: "default", proxiesSource: "default", editable: true, envPinned: false, proxiesEditable: true,
  proxiesEnvPinned: false, rememberedPolicyValid: true,
  currentClient: { available: true, peer: "127.0.0.1", resolvedClient: "127.0.0.1", forwardingHeadersIgnored: false },
  bindAddress };
const key = { id: 1, name: "Sonarr", scope: "CONTROL", createdAt: 1767225600, lastUsedAt: null };
const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) =>
  pipe(operations, mergeMap((operation) => {
    const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
    const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
    if (operation.kind === "teardown") return make<OperationResult>((observer) => { observer.complete(); return () => {}; });
    const data = name === "NetworkAccess" ? { networkAccess }
      : name === "LoginStatus" ? { adminLoginStatus: { enabled: true, username: "admin" } }
      : name === "ApiKeys" ? { apiKeys: [] }
      : name === "CreateApiKey" ? { createApiKey: { key: { ...key, name: operation.variables.name, scope: operation.variables.scope }, rawKey: RAW_KEY } }
      : {};
    return fromValue({ operation, data });
  }))],
});

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{${name}}`, String(value)), en[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><SettingsShellProvider search="" actionsRef={{ current: null }} setFlags={() => {}} controlsHost={document.getElementById("controls")}><SecurityPanel /></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

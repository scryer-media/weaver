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
import { ProvidersPanel } from "@/next/pages/settings/panels/ProvidersPanel";
import "@/next/fonts.css";
import "@/next/theme.css";

// `?servers=N` is how many servers the daemon holds. `?refuse` refuses reads
// of the list the way a caller without admin scope is refused, until the test
// sets `providersFixture.refusing` to false; `?partial` answers every read with
// the list and a failure beside it.
const params = new URLSearchParams(location.search);
const count = Number(params.get("servers") ?? "0");
const partial = params.has("partial");

const servers = Array.from({ length: count }, (_, index) => ({
  id: index + 1, host: `news${index + 1}.example.test`, port: 563, tls: true, connections: 10, active: true,
  supportsPipelining: false, priority: 0, backfill: false, retentionDays: 0, maxDownloadSpeed: 0,
  routing: null, routingStatus: null, tlsCipherSuite: null, tlsNameMismatchCertificateFingerprint: null,
  downloadQuota: { enabled: false, limitBytes: 0, period: "MONTHLY", resetDay: 1, usedBytes: 0, remainingBytes: 0, blocked: false },
}));

// Every read of the list, in order.
const reads: string[] = [];
const fixture = { reads, refusing: params.has("refuse") };
Object.assign(window, { providersFixture: fixture });

const refused = () => new CombinedError({
  graphQLErrors: [{ message: "admin scope required", extensions: { code: "FORBIDDEN" } }],
});

const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  if (name !== "Servers") {
    return fromValue({ operation, data: {} });
  }
  reads.push(name);
  if (partial) {
    return fromValue({ operation, data: { servers }, error: refused() });
  }
  if (fixture.refusing) {
    return fromValue({ operation, data: undefined, error: refused() });
  }
  return fromValue({ operation, data: { servers } });
}))] });

const dictionary = { ...en, ...nextEn } as Record<string, string>;
const status = document.getElementById("status")!;
const shell = {
  search: "", actionsRef: { current: null }, controlsHost: document.getElementById("controls"), registry: new Map(),
  setFlags: (next: { status: string | null; failed: boolean }) => {
    status.textContent = `${next.failed ? "failed" : "ok"} ${next.status ?? ""}`.trim();
  },
};

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key),
    uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8"><ProvidersPanel /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider>,
);

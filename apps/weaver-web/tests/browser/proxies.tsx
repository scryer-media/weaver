import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { ProxyKind } from "@/lib/proxies";
import { nextEn } from "@/next/i18n/en";
import { ProxiesPanel } from "@/next/pages/settings/panels/ProxiesPanel";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

/* The fixture's daemon: the proxies it holds, secrets included, and every save it was sent. */

const SECRETS = ["username", "password", "privateKey", "passphrase", "presharedKey"] as const;
type Stored = {
  id: number; name: string; kind: ProxyKind; enabled: boolean; host: string; port: number;
  dnsServers: string[]; tunnelAddresses: string[]; peerPublicKey: string | null; mtu: number;
  keepaliveSeconds: number | null; timeoutSeconds: number; hostKeyFingerprint: string | null;
  secrets: Partial<Record<(typeof SECRETS)[number], string>>;
};
const stored = (id: number, name: string, kind: ProxyKind, port: number, extra: Partial<Stored> = {}): Stored => ({
  id, name, kind, enabled: true, host: `${name.toLowerCase()}.fixture.invalid`, port, dnsServers: [],
  tunnelAddresses: [], peerPublicKey: null, mtu: 1280, keepaliveSeconds: 25, timeoutSeconds: 30,
  hostKeyFingerprint: null, secrets: {}, ...extra,
});
const profiles: Stored[] = [
  stored(1, "Amsterdam", "WIRE_GUARD", 51820, {
    tunnelAddresses: ["10.0.0.1/32"], peerPublicKey: `${"p".repeat(43)}=`, secrets: { privateKey: `${"k".repeat(43)}=` },
  }),
  stored(2, "Seedbox", "SSH", 22, { secrets: { username: "operator", privateKey: "fixture-ed25519-key" } }),
  stored(3, "Relay", "SOCKS5", 1080, { secrets: { username: "relay-user", password: "relay-password" } }),
];
const requests: { name: string; variables: Record<string, unknown> }[] = [];

const view = (profile: Stored) => {
  const { secrets, ...shown } = profile;
  return {
    ...shown, tunnelPublicKey: null,
    hasUsername: secrets.username !== undefined, hasPassword: secrets.password !== undefined,
    hasPrivateKey: secrets.privateKey !== undefined, hasPassphrase: secrets.passphrase !== undefined,
    hasPresharedKey: secrets.presharedKey !== undefined,
  };
};

function graphql(name: string, variables: Record<string, unknown>) {
  if (name !== "ProxyProfiles") requests.push({ name, variables: structuredClone(variables) });
  if (name === "SaveProxyProfile") {
    const input = variables.input as Record<string, unknown>;
    const existing = profiles.find((profile) => profile.id === variables.id);
    const secrets = { ...existing?.secrets };
    // Absent keeps what is stored, null removes it, and a value replaces it.
    for (const key of SECRETS) {
      if (input[key] === null || input[key] === "") delete secrets[key];
      else if (typeof input[key] === "string") secrets[key] = input[key];
    }
    // As the daemon does: SSH is refused a password, and holds none.
    if (input.kind === "SSH" && typeof input.password === "string" && input.password !== "") {
      return { errors: [{ message: "SSH requires an Ed25519 private key and takes no password" }] };
    }
    const saved: Stored = {
      id: existing?.id ?? Math.max(0, ...profiles.map((profile) => profile.id)) + 1,
      name: input.name as string, kind: input.kind as ProxyKind, enabled: input.enabled as boolean,
      host: input.host as string, port: input.port as number, dnsServers: input.dnsServers as string[],
      tunnelAddresses: input.tunnelAddresses as string[], peerPublicKey: input.peerPublicKey as string | null,
      mtu: input.mtu as number, keepaliveSeconds: input.keepaliveSeconds as number,
      timeoutSeconds: input.timeoutSeconds as number, hostKeyFingerprint: existing?.hostKeyFingerprint ?? null, secrets,
    };
    if (existing) Object.assign(existing, saved);
    else profiles.push(saved);
    return { data: { saveProxyProfile: { id: saved.id } } };
  }
  return { data: { proxyProfiles: profiles.map(view), testProxyProfile: { success: true, message: "" } } };
}
Object.assign(window, { proxiesFixture: { profiles, requests } });

const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  if (new URL(String(request), document.baseURI).pathname !== "/graphql") return originalFetch(request, init);
  const body = JSON.parse(String(init?.body));
  return Response.json(graphql(body.operationName, body.variables));
};
const dictionary: Record<string, string> = { ...en, ...nextEn };
const shell = {
  search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: document.getElementById("controls"),
};
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto flex max-w-[1400px] flex-col p-8"><ProxiesPanel /></main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);

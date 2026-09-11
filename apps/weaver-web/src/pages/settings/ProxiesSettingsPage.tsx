import { useRef, useState } from "react";
import { Copy, Upload } from "lucide-react";
import { useMutation, useQuery } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DELETE_PROXY_MUTATION, PROXY_PROFILES_QUERY, RESET_PROXY_TRUST_MUTATION, SAVE_PROXY_MUTATION, TEST_PROXY_MUTATION } from "@/graphql/proxies";
import { proxyLabels, type ProxyKind, type ProxyProfile } from "@/lib/proxies";
import { parseWireguardConfig, stripConfigAssignment } from "@/lib/wireguard-config";

type Secret = "username" | "password" | "privateKey" | "passphrase" | "presharedKey";
// MTU and keepalive stay text while editing: blank means the default, and a pasted `MTU = 1420` line still lands.
type Draft = { name: string; kind: ProxyKind; enabled: boolean; host: string; port: number; dns: string; addresses: string; peerPublicKey: string; mtu: string; keepaliveSeconds: string; timeoutSeconds: number; secrets: Partial<Record<Secret, string | null>> };
const initial: Draft = { name: "", kind: "WIRE_GUARD", enabled: true, host: "", port: 51820, dns: "", addresses: "", peerPublicKey: "", mtu: "", keepaliveSeconds: "", timeoutSeconds: 30, secrets: {} };
const defaultPorts: Record<ProxyKind, number> = { HTTP_CONNECT: 3128, HTTP3_CONNECT: 443, SOCKS5: 1080, SSH: 22, WIRE_GUARD: 51820 };
const MTU_MIN = 1280, MTU_MAX = 3800, MTU_DEFAULT = 1280, KEEPALIVE_DEFAULT = 25, KEEPALIVE_MAX = 65535;
const CONFIG_KEYS = { privateKey: ["privatekey"], presharedKey: ["presharedkey"], peerPublicKey: ["publickey", "peerpublickey"], endpoint: ["endpoint"], mtu: ["mtu"], keepalive: ["persistentkeepalive", "keepalive"], list: ["address", "addresses", "dns"] } as const;
const CONFIG_SAMPLE = "[Interface]\nPrivateKey = …\nAddress = 10.6.0.2/32\n\n[Peer]\nPublicKey = …\nEndpoint = vpn.example.com:51820";
const WIREGUARD_KEY = /^[A-Za-z0-9+/]{43}=$/;
const noPasswordManager = { "data-1p-ignore": "true", "data-lpignore": "true", "data-bwignore": "true", "data-form-type": "other", "data-protonpass-ignore": "true" };
const monoArea = "block min-h-20 w-full rounded-md border border-input bg-background p-3 font-mono text-xs";
const hint = "block text-xs text-muted-foreground";
// One entry per line or comma; a pasted `Address = …` or `DNS = …` line counts as its value.
const splitList = (value: string) => value.split(/\r?\n/).flatMap(line => stripConfigAssignment(line, CONFIG_KEYS.list).split(/[\s,]+/)).filter(Boolean);
const digits = (value: string, keys: readonly string[]) => stripConfigAssignment(value, keys).replace(/\D/g, "");
const redact = (value: string) => value.length <= 10 ? (value && "••••••") : `${value.slice(0, 6)}••••••••${value.slice(-4)}`;
function splitEndpoint(raw: string): { host: string; port?: number } {
  const value = stripConfigAssignment(raw, CONFIG_KEYS.endpoint);
  const match = /^(?:\[([^\]]+)\]|([^:\s]+))(?::(\d+))?$/.exec(value);
  return match ? { host: match[1] ?? match[2]!, port: match[3] ? Number(match[3]) : undefined } : { host: value };
}
function draftFor(p: ProxyProfile): Draft {
  return { name: p.name, kind: p.kind, enabled: p.enabled, host: p.host, port: p.port, dns: p.dnsServers.join("\n"), addresses: p.tunnelAddresses.join("\n"), peerPublicKey: p.peerPublicKey ?? "", mtu: p.mtu ? String(p.mtu) : "", keepaliveSeconds: String(p.keepaliveSeconds ?? 0), timeoutSeconds: p.timeoutSeconds, secrets: {} };
}
function inputFor(d: Draft) {
  return { name: d.name, kind: d.kind, enabled: d.enabled, host: d.host, port: d.port, dnsServers: splitList(d.dns), tunnelAddresses: splitList(d.addresses), peerPublicKey: d.peerPublicKey || null, mtu: Number(d.mtu || MTU_DEFAULT), keepaliveSeconds: Number(d.keepaliveSeconds || KEEPALIVE_DEFAULT), timeoutSeconds: d.timeoutSeconds, ...d.secrets };
}
// A line copied out of a configuration is worth as much as the bare value, in any WireGuard field.
function normalized(d: Draft): Draft {
  if (d.kind !== "WIRE_GUARD") return d;
  const endpoint = splitEndpoint(d.host);
  const secrets = { ...d.secrets };
  for (const key of ["privateKey", "presharedKey"] as const) {
    const value = secrets[key];
    if (typeof value !== "string") continue;
    const bare = stripConfigAssignment(value, CONFIG_KEYS[key]);
    if (bare) secrets[key] = bare; else delete secrets[key];
  }
  return { ...d, host: endpoint.host, port: endpoint.port ?? d.port, peerPublicKey: stripConfigAssignment(d.peerPublicKey, CONFIG_KEYS.peerPublicKey), secrets };
}
function wireguardProblem(d: Draft, p: ProxyProfile | null): string | null {
  const privateKey = d.secrets.privateKey ?? "";
  if (!privateKey && !p?.hasPrivateKey) return "WireGuard needs a private key.";
  const keys: [string, string][] = [["The private key", privateKey], ["The peer public key", d.peerPublicKey], ["The preshared key", d.secrets.presharedKey ?? ""]];
  const malformed = keys.find(([, key]) => key && !WIREGUARD_KEY.test(key));
  if (malformed) return `${malformed[0]} does not look like a WireGuard key: they are 32 bytes of base64, exactly as wg genkey prints them (44 characters ending in =).`;
  if (!d.peerPublicKey) return "WireGuard needs the peer's public key.";
  if (!splitList(d.addresses).length) return "WireGuard needs at least one interface address, for example 10.6.0.2/32.";
  const mtu = Number(d.mtu || MTU_DEFAULT);
  if (mtu < MTU_MIN || mtu > MTU_MAX) return `The tunnel MTU must be between ${MTU_MIN} and ${MTU_MAX}.`;
  if (Number(d.keepaliveSeconds || 0) > KEEPALIVE_MAX) return `Keepalive is a whole number of seconds up to ${KEEPALIVE_MAX}, and 0 switches it off.`;
  return null;
}

export function ProxiesSettingsPage() {
  const [{ data, error }, refresh] = useQuery<{ proxyProfiles: ProxyProfile[] }>({ query: PROXY_PROFILES_QUERY });
  const [saveState, save] = useMutation(SAVE_PROXY_MUTATION);
  const [deleteState, remove] = useMutation(DELETE_PROXY_MUTATION);
  const [trustState, resetTrust] = useMutation(RESET_PROXY_TRUST_MUTATION);
  const [testState, test] = useMutation(TEST_PROXY_MUTATION);
  const [editor, setEditor] = useState<{ profile: ProxyProfile | null; draft: Draft } | null>(null);
  const [confirm, setConfirm] = useState<{ profile: ProxyProfile; kind: "delete" | "trust" } | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [health, setHealth] = useState<Record<number, string>>({});
  const [configText, setConfigText] = useState("");
  const [wgDetailsOpen, setWgDetailsOpen] = useState(false);
  const [privateKeyFocused, setPrivateKeyFocused] = useState(false);
  const configFileRef = useRef<HTMLInputElement>(null);
  const reload = () => refresh({ requestPolicy: "network-only" });
  const update = (patch: Partial<Draft>) => setEditor(e => e && ({ ...e, draft: { ...e.draft, ...patch } }));
  const setSecret = (key: Secret, value: string | null) => setEditor(e => {
    if (!e) return e;
    const secrets = { ...e.draft.secrets };
    if (value === "") delete secrets[key]; else secrets[key] = value;
    return { ...e, draft: { ...e.draft, secrets } };
  });
  const open = (profile: ProxyProfile | null) => {
    setEditor({ profile, draft: profile ? draftFor(profile) : { ...initial, secrets: {} } });
    setMessage(null); setConfigText(""); setWgDetailsOpen(!!profile);
  };
  const importConfig = (text: string) => {
    if (text.length > 65536) { setMessage("Configuration must be smaller than 64 KiB."); return; }
    const parsed = parseWireguardConfig(text);
    if (!parsed) { setMessage("That does not look like a WireGuard configuration."); return; }
    const endpoint = parsed.endpoint ? splitEndpoint(parsed.endpoint) : null;
    // Fill only what the file names, so a partial snippet never blanks a field already entered.
    setEditor(e => e && ({ ...e, draft: { ...e.draft, host: endpoint?.host || e.draft.host, port: endpoint?.port ?? e.draft.port, addresses: parsed.tunnelAddresses || e.draft.addresses, dns: parsed.tunnelDnsServers || e.draft.dns, peerPublicKey: parsed.peerPublicKey || e.draft.peerPublicKey, mtu: digits(parsed.tunnelMtu, CONFIG_KEYS.mtu) || e.draft.mtu, keepaliveSeconds: digits(parsed.tunnelKeepaliveSeconds, CONFIG_KEYS.keepalive) || e.draft.keepaliveSeconds, secrets: { ...e.draft.secrets, ...(parsed.privateKey ? { privateKey: parsed.privateKey } : {}), ...(parsed.presharedKey ? { presharedKey: parsed.presharedKey } : {}) } } }));
    setMessage(["Filled the form from the WireGuard configuration.", ...(parsed.peerCount > 1 ? [`It has ${parsed.peerCount} peers; only the first was imported.`] : []), ...(parsed.ignored.length ? [`Ignored ${parsed.ignored.join(", ")}, which a tunnel proxy does not use.`] : [])].join(" "));
    setConfigText(""); setWgDetailsOpen(true);
  };
  const saveEditor = async () => {
    if (!editor) return;
    const draft = normalized(editor.draft);
    setEditor({ ...editor, draft });
    const problem = draft.kind === "WIRE_GUARD" ? wireguardProblem(draft, editor.profile) : null;
    if (problem) { setMessage(problem); return; }
    const result = await save({ id: editor.profile?.id, input: inputFor(draft) });
    if (result.error) { setMessage(result.error.message); return; }
    setEditor(null); setMessage("Proxy saved. Affected connections now use the new revision."); reload();
  };
  const copyTunnelKey = async (key: string) => {
    try { await navigator.clipboard.writeText(key); setMessage("Public key copied."); } catch { setMessage("Unable to copy the public key."); }
  };
  const doConfirm = async () => {
    if (!confirm) return;
    const result = await (confirm.kind === "delete" ? remove : resetTrust)({ id: confirm.profile.id });
    setMessage(result.error?.message ?? (confirm.kind === "delete" ? "Proxy deleted." : "Host-key trust reset. The next successful connection will persist the new key."));
    setConfirm(null); reload();
  };
  const secretField = (key: Secret, label: string, present?: boolean, multiline = false) => {
    if (!editor) return null;
    const value = editor.draft.secrets[key];
    const change = (text: string) => setSecret(key, text);
    const props = { value: value ?? "", disabled: !!present && value === null, placeholder: present ? "Stored securely · leave blank to keep" : "", autoComplete: "off" };
    return <div className="space-y-2" key={key}>
      <label className="block space-y-2 text-sm"><span>{label}</span>{multiline ? <textarea {...props} className="min-h-28 w-full rounded-md border border-input bg-background p-3 font-mono text-xs" onChange={e => change(e.target.value)} /> : <Input {...props} type={key === "username" ? "text" : "password"} onChange={e => change(e.target.value)} />}</label>
      {present && <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={value === null} onChange={e => setSecret(key, e.target.checked ? null : "")} />Clear stored {label.toLowerCase()}</label>}
    </div>;
  };
  const d = editor?.draft;
  const p = editor?.profile;
  const showDetails = !!d && (d.kind !== "WIRE_GUARD" || wgDetailsOpen);
  return <div className="space-y-6">
    <PageHeader title="Proxies" description="Create reusable profiles, then assign an ordered route in each server or RSS feed editor." actions={<Button onClick={() => open(null)}>Add proxy</Button>} />
    {(message || error) && <p role="status" className="rounded-md border border-border p-4 text-sm">{message ?? error?.message}</p>}
    {d && <SectionCard title={p ? `Edit ${p.name}` : "New proxy"} actions={<label className="flex items-center gap-2 text-sm font-medium"><input type="checkbox" className="size-4" checked={d.enabled} onChange={e => update({ enabled: e.target.checked })} />Enabled</label>}>
      <form className="space-y-5" onSubmit={e => { e.preventDefault(); if (showDetails) void saveEditor(); }}>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-[12rem_minmax(0,1fr)_minmax(0,1.4fr)_7rem_9rem]">
          <label className="space-y-2 text-sm">Type<select className="h-10 w-full rounded-md border border-input bg-background px-3" value={d.kind} disabled={!!p} onChange={e => { const kind = e.target.value as ProxyKind; update({ ...initial, name: d.name, enabled: d.enabled, timeoutSeconds: d.timeoutSeconds, kind, port: defaultPorts[kind], secrets: {} }); setWgDetailsOpen(kind !== "WIRE_GUARD"); }}>
            {Object.entries(proxyLabels).map(([kind, label]) => <option key={kind} value={kind}>{label}</option>)}
          </select></label>
          <label className="space-y-2 text-sm">Name<Input required maxLength={128} value={d.name} onChange={e => update({ name: e.target.value })} /></label>
          <label className="space-y-2 text-sm">Endpoint host<Input required value={d.host} onChange={e => update({ host: e.target.value })} /></label>
          <label className="space-y-2 text-sm">Port<Input required type="number" min={1} max={65535} value={d.port} onChange={e => update({ port: Number(e.target.value) })} /></label>
          <label className="space-y-2 text-sm">Test timeout (s)<Input type="number" min={1} max={300} value={d.timeoutSeconds} onChange={e => update({ timeoutSeconds: Number(e.target.value) })} /></label>
        </div>
        {d.kind !== "WIRE_GUARD" && <div className="grid gap-4 md:grid-cols-2">
          <label className="space-y-2 text-sm md:col-span-2">DNS server IPs<textarea className={monoArea} spellCheck={false} rows={2} value={d.dns} onChange={e => update({ dns: e.target.value })} /><span className={hint}>Required for RSS. These servers must be reachable through this proxy.</span></label>
          {secretField("username", "Username", p?.hasUsername)}
          {(d.kind === "HTTP_CONNECT" || d.kind === "HTTP3_CONNECT" || d.kind === "SOCKS5") && secretField("password", "Password", p?.hasPassword)}
          {d.kind === "HTTP3_CONNECT" && <p className="text-xs text-muted-foreground md:col-span-2">Requires an HTTP/3 forward proxy with a publicly trusted TLS certificate and a reachable UDP port. Configure a DNS server IP reachable through the proxy to use the connection test. The proxy does not automatically downgrade to HTTP or direct access.</p>}
          {d.kind === "SSH" && <>{secretField("privateKey", "Ed25519 private key", p?.hasPrivateKey, true)}{secretField("passphrase", "Key passphrase", p?.hasPassphrase)}<p className="text-xs text-muted-foreground md:col-span-2">An Ed25519 private key is required. The first successful connection pins the host key; changed keys are rejected.</p></>}
        </div>}
        {d.kind === "WIRE_GUARD" && !wgDetailsOpen && <div className="space-y-3 rounded-md border border-border bg-background/60 p-4">
          <div><p className="text-sm font-medium">Import a WireGuard configuration</p><p className="mt-1 text-xs text-muted-foreground">Paste the whole configuration your VPN provider gave you, or choose the .conf file, and the fields below are filled from it. Every field also takes a single line pasted on its own.</p></div>
          <textarea className={monoArea} spellCheck={false} autoComplete="off" rows={7} value={configText} placeholder={CONFIG_SAMPLE} onChange={e => setConfigText(e.target.value)} {...noPasswordManager} />
          <input ref={configFileRef} type="file" accept=".conf,.txt,text/plain" className="hidden" onChange={async e => { const file = e.target.files?.[0]; e.target.value = ""; if (!file) return; if (file.size > 65536) setMessage("Configuration must be smaller than 64 KiB."); else importConfig(await file.text()); }} />
          <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" disabled={!configText.trim()} onClick={() => importConfig(configText)}>{configText.trim() ? "Next" : "Fill the form"}</Button>
            <Button type="button" variant="outline" onClick={() => configFileRef.current?.click()}><Upload className="size-4" />Choose a file</Button>
            <Button type="button" variant="outline" onClick={() => setWgDetailsOpen(true)}>Enter details manually</Button>
          </div>
        </div>}
        {d.kind === "WIRE_GUARD" && wgDetailsOpen && <div className="space-y-4">
          <label className="block space-y-2 text-sm">Private key<Input className="font-mono text-xs" spellCheck={false} autoComplete="off" {...noPasswordManager} required={!p?.hasPrivateKey} value={privateKeyFocused ? d.secrets.privateKey ?? "" : redact(d.secrets.privateKey ?? "")} placeholder={p?.hasPrivateKey ? "A private key is stored. Paste a new one to replace it." : "PrivateKey = …"} onFocus={() => setPrivateKeyFocused(true)} onBlur={() => setPrivateKeyFocused(false)} onChange={e => setSecret("privateKey", e.target.value)} /><span className={hint}>The PrivateKey line from the [Interface] section. WireGuard keys are 32 bytes of base64, exactly as wg genkey prints them (44 characters ending in =).</span></label>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="space-y-2 text-sm">Peer public key<Input className="font-mono text-xs" spellCheck={false} autoComplete="off" {...noPasswordManager} required value={d.peerPublicKey} placeholder="PublicKey = …" onChange={e => update({ peerPublicKey: e.target.value })} /><span className={hint}>{"The PublicKey line from the server's [Peer] section. A public key is public, so it is shown in full."}</span></label>
            <div className="space-y-2 text-sm">
              <label className="block space-y-2">Preshared key<Input className="font-mono text-xs" spellCheck={false} autoComplete="off" {...noPasswordManager} value={d.secrets.presharedKey ?? ""} disabled={d.secrets.presharedKey === null} placeholder={p?.hasPresharedKey ? "A preshared key is stored. Paste a new one to replace it." : "PresharedKey = …"} onChange={e => setSecret("presharedKey", e.target.value)} /></label>
              <span className={hint}>Optional. The PresharedKey line, when the server uses one.</span>
              {p?.hasPresharedKey && <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={d.secrets.presharedKey === null} onChange={e => setSecret("presharedKey", e.target.checked ? null : "")} />Clear stored preshared key</label>}
            </div>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="space-y-2 text-sm">Addresses<textarea className={monoArea} spellCheck={false} rows={3} required value={d.addresses} placeholder="10.6.0.2/32" onChange={e => update({ addresses: e.target.value })} /><span className={hint}>The Address line from the [Interface] section, for example 10.6.0.2/32. One per line, or comma separated.</span></label>
            <label className="space-y-2 text-sm">DNS servers<textarea className={monoArea} spellCheck={false} rows={3} value={d.dns} placeholder="10.6.0.1" onChange={e => update({ dns: e.target.value })} /><span className={hint}>Resolvers reached through the tunnel, one per line or comma separated. Required for RSS; without one, destinations must be addressed by IP.</span></label>
          </div>
          <div className="grid gap-4 md:grid-cols-[10rem_10rem_minmax(0,1fr)]">
            <label className="space-y-2 text-sm">MTU<Input inputMode="numeric" value={d.mtu} placeholder={String(MTU_DEFAULT)} onChange={e => update({ mtu: digits(e.target.value, CONFIG_KEYS.mtu) })} /><span className={hint}>Optional, {MTU_MIN}–{MTU_MAX}. Blank uses {MTU_DEFAULT}.</span></label>
            <label className="space-y-2 text-sm">Keepalive (s)<Input inputMode="numeric" value={d.keepaliveSeconds} placeholder={String(KEEPALIVE_DEFAULT)} onChange={e => update({ keepaliveSeconds: digits(e.target.value, CONFIG_KEYS.keepalive) })} /><span className={hint}>Blank uses {KEEPALIVE_DEFAULT}; 0 switches it off.</span></label>
            <div className="space-y-2 text-sm">
              <label htmlFor="proxy-tunnel-public-key" className="block">{"This tunnel's public key"}</label>
              <div className="relative"><Input id="proxy-tunnel-public-key" readOnly className="pr-11 font-mono text-xs" value={p?.tunnelPublicKey ?? ""} placeholder="Saved after the private key is stored." /><Button type="button" variant="ghost" size="icon" className="absolute top-1/2 right-1 size-8 -translate-y-1/2" aria-label="Copy public key" title="Copy public key" disabled={!p?.tunnelPublicKey} onClick={() => void copyTunnelKey(p?.tunnelPublicKey ?? "")}><Copy className="size-4" /></Button></div>
              <span className={hint}>{"Paste this into the server's [Peer] section as its PublicKey line."}</span>
            </div>
          </div>
        </div>}
        <div className="flex gap-3">{showDetails && <Button disabled={saveState.fetching} type="submit">Save proxy</Button>}<Button type="button" variant="ghost" onClick={() => { setEditor(null); setConfigText(""); }}>Cancel</Button></div>
      </form>
    </SectionCard>}
    {!data?.proxyProfiles.length && !editor && <EmptyState title="No proxies configured." description="Add a proxy to route server or RSS traffic. Servers and feeds use direct access until then." actionLabel="Add proxy" onAction={() => open(null)} />}
    {data?.proxyProfiles.map(profile => <SectionCard key={profile.id} title={profile.name} description={`${proxyLabels[profile.kind]} · ${profile.host}:${profile.port} · ${profile.enabled ? "Enabled" : "Disabled"}`}>
      <div className="space-y-3">
        {profile.tunnelPublicKey && <p className="break-all text-xs">Tunnel public key: {profile.tunnelPublicKey}</p>}
        {profile.kind === "SSH" && <p className="break-all text-xs">Host-key trust: {profile.hostKeyFingerprint ?? "Not pinned yet"}</p>}
        {health[profile.id] && <p role="status" className="text-sm">{health[profile.id]}</p>}
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => open(profile)}>Edit</Button>
          <Button variant="outline" disabled={testState.fetching} onClick={async () => { const result = await test({ id: profile.id }); setHealth(h => ({ ...h, [profile.id]: result.error?.message ?? result.data?.testProxyProfile.message ?? "Test failed" })); reload(); }}>Test connection</Button>
          <Button variant="outline" disabled={saveState.fetching} onClick={async () => { const result = await save({ id: profile.id, input: inputFor({ ...draftFor(profile), enabled: !profile.enabled }) }); setMessage(result.error?.message ?? `Proxy ${profile.enabled ? "disabled" : "enabled"}.`); reload(); }}>{profile.enabled ? "Disable" : "Enable"}</Button>
          {profile.kind === "SSH" && profile.hostKeyFingerprint && <Button variant="ghost" onClick={() => setConfirm({ profile, kind: "trust" })}>Reset host-key trust</Button>}
          <Button variant="ghost" onClick={() => setConfirm({ profile, kind: "delete" })}>Delete</Button>
        </div>
      </div>
    </SectionCard>)}
    <ConfirmDialog open={!!confirm} title={confirm?.kind === "trust" ? "Reset SSH host-key trust?" : "Delete proxy?"} message={confirm?.kind === "trust" ? "Verify the server's new host key independently first. Resetting closes existing sessions and lets the next successful connection pin a new key." : "Referenced profiles cannot be deleted. Remove this profile from every server and RSS route first."} confirmLabel={confirm?.kind === "trust" ? "Reset trust" : "Delete"} confirmDisabled={deleteState.fetching || trustState.fetching} onConfirm={() => void doConfirm()} onCancel={() => setConfirm(null)} />
  </div>;
}

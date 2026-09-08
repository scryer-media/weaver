import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DELETE_PROXY_MUTATION, PROXY_PROFILES_QUERY, RESET_PROXY_TRUST_MUTATION, SAVE_PROXY_MUTATION, TEST_PROXY_MUTATION } from "@/graphql/proxies";
import { proxyLabels, type ProxyKind, type ProxyProfile } from "@/lib/proxies";
import { parseWireguardConfig } from "@/lib/wireguard-config";

type Secret = "username" | "password" | "privateKey" | "passphrase" | "presharedKey";
type Draft = { name: string; kind: ProxyKind; enabled: boolean; host: string; port: number; dns: string; addresses: string; peerPublicKey: string; mtu: number; keepaliveSeconds: number; timeoutSeconds: number; secrets: Partial<Record<Secret, string | null>> };
const initial: Draft = { name: "", kind: "HTTP_CONNECT", enabled: true, host: "", port: 3128, dns: "", addresses: "", peerPublicKey: "", mtu: 1280, keepaliveSeconds: 25, timeoutSeconds: 30, secrets: {} };
const splitList = (value: string) => value.split(/[\s,]+/).filter(Boolean);
function draftFor(p: ProxyProfile): Draft {
  return { name: p.name, kind: p.kind, enabled: p.enabled, host: p.host, port: p.port, dns: p.dnsServers.join("\n"), addresses: p.tunnelAddresses.join("\n"), peerPublicKey: p.peerPublicKey ?? "", mtu: p.mtu, keepaliveSeconds: p.keepaliveSeconds ?? 0, timeoutSeconds: p.timeoutSeconds, secrets: {} };
}
function inputFor(d: Draft) {
  return { name: d.name, kind: d.kind, enabled: d.enabled, host: d.host, port: d.port, dnsServers: splitList(d.dns), tunnelAddresses: splitList(d.addresses), peerPublicKey: d.peerPublicKey || null, mtu: d.mtu, keepaliveSeconds: d.keepaliveSeconds, timeoutSeconds: d.timeoutSeconds, ...d.secrets };
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
  const [warnings, setWarnings] = useState<string[]>([]);
  const reload = () => refresh({ requestPolicy: "network-only" });
  const update = (patch: Partial<Draft>) => setEditor(e => e && ({ ...e, draft: { ...e.draft, ...patch } }));
  const open = (profile: ProxyProfile | null) => {
    setEditor({ profile, draft: profile ? draftFor(profile) : { ...initial, secrets: {} } });
    setMessage(null); setConfigText(""); setWarnings([]);
  };
  const importConfig = (text: string) => {
    if (text.length > 65536) { setMessage("Configuration must be smaller than 64 KiB."); return; }
    const parsed = parseWireguardConfig(text);
    if (!parsed) { setMessage("No WireGuard configuration fields were found."); return; }
    const endpoint = /^(?:\[([^\]]+)\]|([^:]+)):(\d+)$/.exec(parsed.endpoint);
    if (!endpoint) { setMessage("WireGuard Endpoint must contain a host and port."); return; }
    update({ kind: "WIRE_GUARD", host: endpoint[1] ?? endpoint[2]!, port: Number(endpoint[3]), addresses: parsed.tunnelAddresses, dns: parsed.tunnelDnsServers, peerPublicKey: parsed.peerPublicKey, mtu: Number(parsed.tunnelMtu || 1280), keepaliveSeconds: Number(parsed.tunnelKeepaliveSeconds || 25), secrets: { privateKey: parsed.privateKey, presharedKey: parsed.presharedKey || null } });
    setWarnings([
      ...(parsed.ignored.length ? [`Ignored fields: ${parsed.ignored.join(", ")}. AllowedIPs does not restrict this profile; all selected destinations use its peer.`] : []),
      ...(parsed.peerCount > 1 ? [`Only the first peer was imported; ${parsed.peerCount - 1} additional peer(s) were ignored.`] : []),
      "Configuration hooks are never executed.",
    ]);
    setMessage(null); setConfigText("");
  };
  const saveEditor = async () => {
    if (!editor) return;
    const result = await save({ id: editor.profile?.id, input: inputFor(editor.draft) });
    if (result.error) { setMessage(result.error.message); return; }
    setEditor(null); setMessage("Proxy saved. Affected connections now use the new revision."); reload();
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
    const change = (text: string) => {
      const secrets = { ...editor.draft.secrets };
      if (text === "") delete secrets[key]; else secrets[key] = text;
      update({ secrets });
    };
    const props = { value: value ?? "", disabled: !!present && value === null, placeholder: present ? "Stored securely · leave blank to keep" : "", autoComplete: "off" };
    return <div className="space-y-2" key={key}>
      <label className="block space-y-2 text-sm"><span>{label}</span>{multiline ? <textarea {...props} className="min-h-28 w-full rounded-md border border-input bg-background p-3 font-mono text-xs" onChange={e => change(e.target.value)} /> : <Input {...props} type={key === "username" ? "text" : "password"} onChange={e => change(e.target.value)} />}</label>
      {present && <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={value === null} onChange={e => { const secrets = { ...editor.draft.secrets }; if (e.target.checked) secrets[key] = null; else delete secrets[key]; update({ secrets }); }} />Clear stored {label.toLowerCase()}</label>}
    </div>;
  };
  const d = editor?.draft;
  const p = editor?.profile;
  return <div className="space-y-6">
    <PageHeader title="Proxies" description="Create reusable profiles, then assign an ordered route in each server or RSS feed editor." actions={<Button onClick={() => open(null)}>Add proxy</Button>} />
    {(message || error) && <p role="status" className="rounded-md border border-border p-4 text-sm">{message ?? error?.message}</p>}
    {d && <SectionCard title={p ? `Edit ${p.name}` : "New proxy"}>
      <form className="space-y-5" onSubmit={e => { e.preventDefault(); void saveEditor(); }}>
        <div className="grid gap-4 md:grid-cols-2">
          <label className="space-y-2 text-sm">Name<Input required maxLength={128} value={d.name} onChange={e => update({ name: e.target.value })} /></label>
          <label className="space-y-2 text-sm">Type<select className="h-10 w-full rounded-md border border-input bg-background px-3" value={d.kind} disabled={!!p} onChange={e => { const kind = e.target.value as ProxyKind; update({ ...initial, name: d.name, kind, port: { HTTP_CONNECT: 3128, HTTP3_CONNECT: 443, SOCKS5: 1080, SSH: 22, WIRE_GUARD: 51820 }[kind], secrets: {} }); setWarnings([]); }}>
            {Object.entries(proxyLabels).map(([kind, label]) => <option key={kind} value={kind}>{label}</option>)}
          </select></label>
          <label className="space-y-2 text-sm">Endpoint host<Input required value={d.host} onChange={e => update({ host: e.target.value })} /></label>
          <label className="space-y-2 text-sm">Endpoint port<Input required type="number" min={1} max={65535} value={d.port} onChange={e => update({ port: Number(e.target.value) })} /></label>
          <label className="space-y-2 text-sm">DNS server IPs<textarea className="min-h-20 w-full rounded-md border border-input bg-background p-3" value={d.dns} onChange={e => update({ dns: e.target.value })} /><span className="block text-xs text-muted-foreground">Required for RSS. These servers must be reachable through this proxy.</span></label>
          <label className="space-y-2 text-sm">Connection test timeout (seconds)<Input type="number" min={1} max={300} value={d.timeoutSeconds} onChange={e => update({ timeoutSeconds: Number(e.target.value) })} /></label>
          {d.kind !== "WIRE_GUARD" && secretField("username", "Username", p?.hasUsername)}
          {(d.kind === "HTTP_CONNECT" || d.kind === "HTTP3_CONNECT" || d.kind === "SOCKS5") && secretField("password", "Password", p?.hasPassword)}
          {d.kind === "HTTP3_CONNECT" && <p className="text-xs text-muted-foreground md:col-span-2">Requires an HTTP/3 forward proxy with a publicly trusted TLS certificate and a reachable UDP port. Configure a DNS server IP reachable through the proxy to use the connection test. The proxy does not automatically downgrade to HTTP or direct access.</p>}
          {d.kind === "SSH" && <>{secretField("privateKey", "Ed25519 private key", p?.hasPrivateKey, true)}{secretField("passphrase", "Key passphrase", p?.hasPassphrase)}<p className="text-xs text-muted-foreground md:col-span-2">An Ed25519 private key is required. The first successful connection pins the host key; changed keys are rejected.</p></>}
        </div>
        {d.kind === "WIRE_GUARD" && <div className="space-y-4">
          <fieldset className="space-y-3 rounded-md border border-border p-4"><legend className="px-2 text-sm font-semibold">Import WireGuard configuration</legend>
            <label className="block space-y-2 text-sm">Upload .conf<input className="block w-full" type="file" accept=".conf,text/plain" onChange={async e => { const file = e.target.files?.[0]; if (file) { if (file.size > 65536) setMessage("Configuration must be smaller than 64 KiB."); else importConfig(await file.text()); } e.target.value = ""; }} /></label>
            <label className="block space-y-2 text-sm">Paste .conf<textarea className="min-h-24 w-full rounded-md border border-input bg-background p-3 font-mono text-xs" autoComplete="off" value={configText} onChange={e => setConfigText(e.target.value)} /></label>
            <Button type="button" variant="outline" onClick={() => importConfig(configText)}>Import configuration</Button>
            {warnings.length > 0 && <ul role="status" className="space-y-1 text-sm text-amber-600">{warnings.map(w => <li key={w}>{w}</li>)}</ul>}
          </fieldset>
          <div className="grid gap-4 md:grid-cols-2">
            {secretField("privateKey", "Private key (base64)", p?.hasPrivateKey)}
            <label className="space-y-2 text-sm">Peer public key<Input required value={d.peerPublicKey} onChange={e => update({ peerPublicKey: e.target.value })} /></label>
            {secretField("presharedKey", "Preshared key", p?.hasPresharedKey)}
            <label className="space-y-2 text-sm">Tunnel addresses (CIDR)<Input required value={d.addresses} onChange={e => update({ addresses: e.target.value })} /></label>
            <label className="space-y-2 text-sm">MTU<Input required type="number" min={1280} max={3800} value={d.mtu} onChange={e => update({ mtu: Number(e.target.value) })} /></label>
            <label className="space-y-2 text-sm">Keepalive seconds (0 disables)<Input type="number" min={0} max={65535} value={d.keepaliveSeconds} onChange={e => update({ keepaliveSeconds: Number(e.target.value) })} /></label>
          </div>
        </div>}
        <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={d.enabled} onChange={e => update({ enabled: e.target.checked })} />Enabled</label>
        <div className="flex gap-3"><Button disabled={saveState.fetching} type="submit">Save proxy</Button><Button type="button" variant="ghost" onClick={() => { setEditor(null); setConfigText(""); }}>Cancel</Button></div>
      </form>
    </SectionCard>}
    {!data?.proxyProfiles.length && !editor && <p className="text-sm text-muted-foreground">No proxy profiles yet. Existing servers and feeds use direct access.</p>}
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

import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  DELETE_PROXY_MUTATION,
  PROXY_PROFILES_QUERY,
  RESET_PROXY_TRUST_MUTATION,
  SAVE_PROXY_MUTATION,
  TEST_PROXY_MUTATION,
} from "@/graphql/proxies";
import { proxyLabels, type ProxyKind, type ProxyProfile } from "@/lib/proxies";
import { parseWireguardConfig, stripConfigAssignment } from "@/lib/wireguard-config";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { SecondaryButton, TextArea } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WV } from "../../../data/palette";
import { PanelControls, SettingsBlocks, type FieldSpec, type SettingsBlock } from "../framework";

/**
 * Proxies: the tunnels a provider or a feed may be routed through.
 *
 * Secrets are write-only — the daemon reports only whether it holds one — so
 * every secret field is blank on open and an untouched field is never sent.
 * WireGuard profiles accept a pasted configuration file, which is how anyone
 * actually has these details to hand.
 */

const SECRET_KEYS = ["username", "password", "privateKey", "passphrase", "presharedKey"] as const;
type SecretKey = (typeof SECRET_KEYS)[number];

interface ProxyForm {
  name: string;
  kind: ProxyKind;
  enabled: boolean;
  host: string;
  port: number;
  dns: string;
  addresses: string;
  peerPublicKey: string;
  mtu: string;
  keepaliveSeconds: string;
  timeoutSeconds: number;
  secrets: Partial<Record<SecretKey, string>>;
}

const KINDS: { value: string; label: string }[] = (
  ["WIRE_GUARD", "SOCKS5", "HTTP_CONNECT", "HTTP3_CONNECT", "SSH"] as ProxyKind[]
).map((kind) => ({ value: kind, label: proxyLabels[kind] }));

const DEFAULT_PORTS: Record<ProxyKind, number> = {
  HTTP_CONNECT: 3128,
  HTTP3_CONNECT: 443,
  SOCKS5: 1080,
  SSH: 22,
  WIRE_GUARD: 51820,
};

const MTU_DEFAULT = 1280;
const KEEPALIVE_DEFAULT = 25;
const WIREGUARD_KEY = /^[A-Za-z0-9+/]{43}=$/;
const CONFIG_KEYS = {
  privateKey: ["privatekey"],
  presharedKey: ["presharedkey"],
  peerPublicKey: ["publickey", "peerpublickey"],
  endpoint: ["endpoint"],
  mtu: ["mtu"],
  keepalive: ["persistentkeepalive", "keepalive"],
  list: ["address", "addresses", "dns"],
} as const;

const NEW_PROXY: ProxyForm = {
  name: "",
  kind: "WIRE_GUARD",
  enabled: true,
  host: "",
  port: DEFAULT_PORTS.WIRE_GUARD,
  dns: "",
  addresses: "",
  peerPublicKey: "",
  mtu: "",
  keepaliveSeconds: "",
  timeoutSeconds: 30,
  secrets: {},
};

/** One entry per line or comma; a pasted `Address = …` line counts as its value. */
function splitList(value: string): string[] {
  return value
    .split(/\r?\n/)
    .flatMap((line) => stripConfigAssignment(line, CONFIG_KEYS.list).split(/[\s,]+/))
    .filter(Boolean);
}

/** A number out of a pasted `MTU = 1280` line, or out of a bare `1280`. */
function digits(value: string, keys: readonly string[]): string {
  return stripConfigAssignment(value, keys).replace(/\D/g, "");
}

function splitEndpoint(raw: string): { host: string; port?: number } {
  const value = stripConfigAssignment(raw, CONFIG_KEYS.endpoint);
  const match = /^(?:\[([^\]]+)\]|([^:\s]+))(?::(\d+))?$/.exec(value);
  return match
    ? { host: match[1] ?? match[2] ?? value, port: match[3] ? Number(match[3]) : undefined }
    : { host: value };
}

function formFor(profile: ProxyProfile): ProxyForm {
  return {
    name: profile.name,
    kind: profile.kind,
    enabled: profile.enabled,
    host: profile.host,
    port: profile.port,
    dns: profile.dnsServers.join("\n"),
    addresses: profile.tunnelAddresses.join("\n"),
    peerPublicKey: profile.peerPublicKey ?? "",
    mtu: profile.mtu ? String(profile.mtu) : "",
    keepaliveSeconds: profile.keepaliveSeconds == null ? "" : String(profile.keepaliveSeconds),
    timeoutSeconds: profile.timeoutSeconds,
    secrets: {},
  };
}

/** A line copied out of a configuration is worth as much as the bare value. */
function normalized(form: ProxyForm): ProxyForm {
  if (form.kind !== "WIRE_GUARD") {
    return form;
  }
  const endpoint = splitEndpoint(form.host);
  const secrets = { ...form.secrets };
  for (const key of ["privateKey", "presharedKey"] as const) {
    const value = secrets[key];
    if (typeof value === "string") {
      const bare = stripConfigAssignment(value, CONFIG_KEYS[key]);
      if (bare) {
        secrets[key] = bare;
      } else {
        delete secrets[key];
      }
    }
  }
  return {
    ...form,
    host: endpoint.host,
    port: endpoint.port ?? form.port,
    peerPublicKey: stripConfigAssignment(form.peerPublicKey, CONFIG_KEYS.peerPublicKey),
    secrets,
  };
}

function wireguardProblem(form: ProxyForm, profile: ProxyProfile | null): string | null {
  const privateKey = form.secrets.privateKey ?? "";
  if (!privateKey && !profile?.hasPrivateKey) {
    return "WireGuard needs a private key.";
  }
  const keys: [string, string][] = [
    ["The private key", privateKey],
    ["The peer public key", form.peerPublicKey],
    ["The preshared key", form.secrets.presharedKey ?? ""],
  ];
  const malformed = keys.find(([, key]) => key && !WIREGUARD_KEY.test(key));
  if (malformed) {
    return `${malformed[0]} does not look like a WireGuard key: 32 bytes of base64, exactly as wg genkey prints them.`;
  }
  if (!form.peerPublicKey) {
    return "WireGuard needs the peer's public key.";
  }
  if (splitList(form.addresses).length === 0) {
    return "WireGuard needs at least one interface address, for example 10.6.0.2/32.";
  }
  return null;
}

function proxyInput(form: ProxyForm) {
  return {
    name: form.name.trim(),
    kind: form.kind,
    enabled: form.enabled,
    host: form.host.trim(),
    port: form.port,
    dnsServers: splitList(form.dns),
    tunnelAddresses: splitList(form.addresses),
    peerPublicKey: form.peerPublicKey || null,
    mtu: Number(form.mtu || MTU_DEFAULT),
    keepaliveSeconds: Number(form.keepaliveSeconds || KEEPALIVE_DEFAULT),
    timeoutSeconds: form.timeoutSeconds,
    ...form.secrets,
  };
}

export function ProxiesPanel() {
  const [{ data }, reexecute] = useQuery<{ proxyProfiles: ProxyProfile[] }>({
    query: PROXY_PROFILES_QUERY,
  });
  const [, saveProxy] = useMutation(SAVE_PROXY_MUTATION);
  const [, deleteProxy] = useMutation(DELETE_PROXY_MUTATION);
  const [, resetTrust] = useMutation(RESET_PROXY_TRUST_MUTATION);
  const [, testProxy] = useMutation(TEST_PROXY_MUTATION);

  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [form, setForm] = useState<ProxyForm>(NEW_PROXY);
  const [configText, setConfigText] = useState("");
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [health, setHealth] = useState<Record<number, string>>({});
  const [confirmRemove, setConfirmRemove] = useState<ProxyProfile | null>(null);
  const [confirmTrust, setConfirmTrust] = useState<ProxyProfile | null>(null);

  const profiles = data?.proxyProfiles ?? [];
  const editing = typeof editingId === "number"
    ? (profiles.find((profile) => profile.id === editingId) ?? null)
    : null;

  const patch = (next: Partial<ProxyForm>) => setForm((current) => ({ ...current, ...next }));
  const setSecret = (key: SecretKey, value: string) =>
    setForm((current) => {
      const secrets = { ...current.secrets };
      if (value) {
        secrets[key] = value;
      } else {
        delete secrets[key];
      }
      return { ...current, secrets };
    });

  const open = (profile: ProxyProfile | null) => {
    setError(null);
    setNote(null);
    setConfigText("");
    setForm(profile ? formFor(profile) : NEW_PROXY);
    setEditingId(profile ? profile.id : "new");
  };

  /**
   * Fill the form from a pasted `wg-quick` file.
   *
   * Only what the file names is written, so a fragment never blanks a field
   * that was typed by hand, and whatever the file carried that a tunnel proxy
   * has no use for is reported rather than silently dropped.
   */
  const applyConfig = () => {
    if (configText.length > 65536) {
      setError("That configuration is larger than 64 KiB.");
      return;
    }
    const parsed = parseWireguardConfig(configText);
    if (!parsed) {
      setError("That does not look like a WireGuard configuration.");
      return;
    }
    const endpoint = parsed.endpoint ? splitEndpoint(parsed.endpoint) : null;
    setError(null);
    setForm((current) => ({
      ...current,
      kind: "WIRE_GUARD",
      host: endpoint?.host || current.host,
      port: endpoint?.port ?? current.port,
      addresses: parsed.tunnelAddresses || current.addresses,
      dns: parsed.tunnelDnsServers || current.dns,
      peerPublicKey: parsed.peerPublicKey || current.peerPublicKey,
      mtu: digits(parsed.tunnelMtu, CONFIG_KEYS.mtu) || current.mtu,
      keepaliveSeconds:
        digits(parsed.tunnelKeepaliveSeconds, CONFIG_KEYS.keepalive) || current.keepaliveSeconds,
      secrets: {
        ...current.secrets,
        ...(parsed.privateKey ? { privateKey: parsed.privateKey } : {}),
        ...(parsed.presharedKey ? { presharedKey: parsed.presharedKey } : {}),
      },
    }));
    setNote(
      [
        "Filled the form from the configuration.",
        ...(parsed.peerCount > 1 ? [`It names ${parsed.peerCount} peers; only the first was read.`] : []),
        ...(parsed.ignored.length
          ? [`Ignored ${parsed.ignored.join(", ")}, which a tunnel proxy does not use.`]
          : []),
      ].join(" "),
    );
    setConfigText("");
  };

  const save = async () => {
    const clean = normalized(form);
    if (!clean.name.trim()) {
      setError("A proxy needs a name.");
      return;
    }
    if (!clean.host.trim()) {
      setError("A proxy needs a host.");
      return;
    }
    if (clean.kind === "WIRE_GUARD") {
      const problem = wireguardProblem(clean, editing);
      if (problem) {
        setError(problem);
        return;
      }
    }
    setBusy(true);
    const result = await saveProxy({
      id: editingId === "new" ? null : editingId,
      input: proxyInput(clean),
    });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const runTest = async (profile: ProxyProfile) => {
    setHealth((current) => ({ ...current, [profile.id]: "testing…" }));
    const result = await testProxy({ id: profile.id });
    const outcome = result.data?.testProxyProfile;
    setHealth((current) => ({
      ...current,
      [profile.id]: outcome ? (outcome.message || (outcome.success ? "reachable" : "failed"))
        : (result.error?.message ?? "failed"),
    }));
  };

  const isWireguard = form.kind === "WIRE_GUARD";

  const connectionFields: FieldSpec[] = [
    {
      id: "name",
      label: "Name",
      control: {
        kind: "text",
        mono: false,
        value: form.name,
        onChange: (next) => patch({ name: next }),
      },
    },
    {
      id: "kind",
      label: "Type",
      control: {
        kind: "select",
        value: form.kind,
        options: KINDS,
        onChange: (next) =>
          patch({ kind: next as ProxyKind, port: DEFAULT_PORTS[next as ProxyKind] }),
      },
    },
    {
      id: "host",
      label: isWireguard ? "Endpoint" : "Host",
      help: isWireguard ? "A pasted `Endpoint = host:port` line works here too." : undefined,
      control: { kind: "text", value: form.host, onChange: (next) => patch({ host: next }) },
    },
    {
      id: "port",
      label: "Port",
      control: {
        kind: "number",
        value: form.port,
        min: 1,
        max: 65535,
        onChange: (next) => patch({ port: next }),
      },
    },
    {
      id: "timeoutSeconds",
      label: "Timeout",
      control: {
        kind: "number",
        value: form.timeoutSeconds,
        min: 1,
        max: 600,
        onChange: (next) => patch({ timeoutSeconds: next }),
        suffix: "seconds",
      },
    },
    {
      id: "enabled",
      label: "Enabled",
      help: "A disabled proxy keeps its configuration but is skipped when routing.",
      control: { kind: "toggle", value: form.enabled, onChange: (next) => patch({ enabled: next }) },
    },
  ];

  const credentialFields: FieldSpec[] = isWireguard
    ? [
        {
          id: "privateKey",
          label: "Private key",
          help: editing?.hasPrivateKey ? "Stored. Leave blank to keep it." : undefined,
          control: {
            kind: "text",
            type: "password",
            value: form.secrets.privateKey ?? "",
            placeholder: editing?.hasPrivateKey ? "••••••••" : "",
            onChange: (next) => setSecret("privateKey", next),
          },
        },
        {
          id: "peerPublicKey",
          label: "Peer public key",
          control: {
            kind: "text",
            value: form.peerPublicKey,
            onChange: (next) => patch({ peerPublicKey: next }),
          },
        },
        {
          id: "presharedKey",
          label: "Preshared key",
          help: editing?.hasPresharedKey ? "Stored. Leave blank to keep it." : "Optional.",
          control: {
            kind: "text",
            type: "password",
            value: form.secrets.presharedKey ?? "",
            placeholder: editing?.hasPresharedKey ? "••••••••" : "",
            onChange: (next) => setSecret("presharedKey", next),
          },
        },
        {
          id: "addresses",
          label: "Interface addresses",
          help: "One per line, for example 10.6.0.2/32.",
          control: {
            kind: "textarea",
            value: form.addresses,
            rows: 2,
            onChange: (next) => patch({ addresses: next }),
          },
        },
        {
          id: "dns",
          label: "DNS servers",
          help: "One per line. Optional.",
          control: {
            kind: "textarea",
            value: form.dns,
            rows: 2,
            onChange: (next) => patch({ dns: next }),
          },
        },
        {
          id: "mtu",
          label: "MTU",
          help: `Blank uses ${MTU_DEFAULT}.`,
          control: {
            kind: "text",
            value: form.mtu,
            className: "w-[110px]",
            onChange: (next) => patch({ mtu: next }),
          },
        },
        {
          id: "keepaliveSeconds",
          label: "Keepalive",
          help: `Blank uses ${KEEPALIVE_DEFAULT} seconds; 0 switches it off.`,
          control: {
            kind: "text",
            value: form.keepaliveSeconds,
            className: "w-[110px]",
            onChange: (next) => patch({ keepaliveSeconds: next }),
          },
        },
      ]
    : [
        {
          id: "username",
          label: "Username",
          help: editing?.hasUsername ? "Stored. Leave blank to keep it." : "Optional.",
          control: {
            kind: "text",
            value: form.secrets.username ?? "",
            placeholder: editing?.hasUsername ? "••••••••" : "",
            onChange: (next) => setSecret("username", next),
          },
        },
        {
          id: "password",
          label: "Password",
          help: editing?.hasPassword ? "Stored. Leave blank to keep it." : "Optional.",
          control: {
            kind: "text",
            type: "password",
            value: form.secrets.password ?? "",
            placeholder: editing?.hasPassword ? "••••••••" : "",
            onChange: (next) => setSecret("password", next),
          },
        },
        ...(form.kind === "SSH"
          ? [
              {
                id: "privateKey",
                label: "Private key",
                help: editing?.hasPrivateKey ? "Stored. Leave blank to keep it." : "OpenSSH format.",
                control: {
                  kind: "textarea" as const,
                  value: form.secrets.privateKey ?? "",
                  rows: 3,
                  onChange: (next: string) => setSecret("privateKey", next),
                },
              },
              {
                id: "passphrase",
                label: "Key passphrase",
                help: editing?.hasPassphrase ? "Stored. Leave blank to keep it." : "Optional.",
                control: {
                  kind: "text" as const,
                  type: "password" as const,
                  value: form.secrets.passphrase ?? "",
                  placeholder: editing?.hasPassphrase ? "••••••••" : "",
                  onChange: (next: string) => setSecret("passphrase", next),
                },
              },
            ]
          : []),
      ];

  const sections: EditorSection[] = [
    { id: "connection", title: "Connection", fields: connectionFields },
    {
      id: "credentials",
      title: isWireguard ? "Tunnel" : "Credentials",
      note: "secrets are stored write-only",
      fields: credentialFields,
    },
  ];

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "proxies",
      title: "Proxies",
      note: "assigned to providers and feeds on their own panels",
      columns: "minmax(0, 1fr) 150px minmax(0, 1fr) minmax(0, 1fr) 82px",
      headers: ["Name", "Type", "Endpoint", "Last test", ""],
      empty: "No proxies. Providers connect directly.",
      onRowClick: (id) => {
        const profile = profiles.find((entry) => String(entry.id) === id);
        if (profile) {
          open(profile);
        }
      },
      rows: profiles.map((profile) => ({
        id: String(profile.id),
        searchText: `${profile.name} ${proxyLabels[profile.kind]} ${profile.host}`,
        cells: [
          <span key="name" className="flex min-w-0 items-center gap-[10px]">
            <Square color={profile.enabled ? WV.accent : WV.inert} />
            <span className="min-w-0 truncate">{profile.name}</span>
          </span>,
          <Cell key="kind" className="text-wv-secondary">
            {proxyLabels[profile.kind]}
          </Cell>,
          <Cell key="host" mono className="text-wv-muted">
            {profile.host}:{profile.port}
          </Cell>,
          <Cell key="health" mono className="text-wv-muted">
            {health[profile.id] ?? "—"}
          </Cell>,
          <span key="test" onClick={(event) => event.stopPropagation()}>
            <SecondaryButton className="h-7 px-2" onClick={() => void runTest(profile)}>
              Test
            </SecondaryButton>
          </span>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton onClick={() => open(null)}>Add proxy</SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? "Add proxy" : (editing?.name ?? "Proxy")}
        note={editingId === "new" ? "new profile" : proxyLabels[form.kind]}
        width={620}
        sections={sections}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel="Remove proxy"
        extraActions={
          editing?.hostKeyFingerprint ? (
            <SecondaryButton onClick={() => setConfirmTrust(editing)}>Forget host key</SecondaryButton>
          ) : null
        }
      >
        {isWireguard ? (
          <div className="flex flex-none flex-col gap-2 border-t border-wv-hairline px-4 sm:px-6 py-4">
            <div className="text-[12.5px] text-wv-muted">
              Paste a WireGuard configuration to fill this in.
            </div>
            <TextArea
              label="WireGuard configuration"
              value={configText}
              rows={4}
              className="w-full"
              placeholder={"[Interface]\nPrivateKey = …\nAddress = 10.6.0.2/32\n\n[Peer]\nPublicKey = …\nEndpoint = vpn.example.com:51820"}
              onChange={setConfigText}
            />
            <div className="flex items-center justify-end gap-4">
              {note === null ? null : (
                <div className="min-w-0 flex-1 text-[12px] leading-[1.45] text-wv-muted">{note}</div>
              )}
              <SecondaryButton onClick={applyConfig} disabled={!configText.trim()}>
                Read configuration
              </SecondaryButton>
            </div>
          </div>
        ) : null}
      </RecordEditor>

      <ConfirmDialog
        open={confirmRemove !== null}
        title="Remove proxy"
        note={confirmRemove?.name}
        busy={busy}
        confirmLabel="Remove proxy"
        body="Anything routed through this proxy falls back to its next route, or to a direct connection when one is allowed."
        onConfirm={() => {
          if (confirmRemove) {
            void deleteProxy({ id: confirmRemove.id }).then(() => {
              setConfirmRemove(null);
              setEditingId(null);
              void reexecute({ requestPolicy: "network-only" });
            });
          }
        }}
        onDismiss={() => setConfirmRemove(null)}
      />

      <ConfirmDialog
        open={confirmTrust !== null}
        title="Forget host key"
        note={confirmTrust?.name}
        busy={busy}
        confirmLabel="Forget key"
        body="The next connection will trust whatever key the host presents. Only do this when you know the host was rebuilt."
        onConfirm={() => {
          if (confirmTrust) {
            void resetTrust({ id: confirmTrust.id }).then(() => {
              setConfirmTrust(null);
              void reexecute({ requestPolicy: "network-only" });
            });
          }
        }}
        onDismiss={() => setConfirmTrust(null)}
      />
    </>
  );
}

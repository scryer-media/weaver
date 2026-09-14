import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  DELETE_PROXY_MUTATION,
  PROXY_PROFILES_QUERY,
  RESET_PROXY_TRUST_MUTATION,
  SAVE_PROXY_MUTATION,
  TEST_PROXY_MUTATION,
} from "@/graphql/proxies";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { proxyLabels, type ProxyKind, type ProxyProfile } from "@/lib/proxies";
import { parseWireguardConfig, stripConfigAssignment } from "@/lib/wireguard-config";
import { BetaNotice, BetaTag, Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { PrimaryButton, SecondaryButton, TextArea, TextField } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WV } from "../../../data/palette";
import {
  PanelControls,
  SettingsBlocks,
  type FieldControl,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Proxies: the tunnels a provider or a feed may be routed through.
 *
 * Secrets are write-only — the daemon reports only whether it holds one — so
 * every secret field is blank on open and an untouched field is never sent. A
 * stored optional secret can be cleared, which sends `null` for it.
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
  /** A string replaces the stored secret, `null` clears it, absent keeps it. */
  secrets: Partial<Record<SecretKey, string | null>>;
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
/** The daemon accepts a connect timeout of 1–300 seconds. */
const TIMEOUT_MAX = 300;
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

function wireguardProblem(t: Translate, form: ProxyForm, profile: ProxyProfile | null): string | null {
  const privateKey = form.secrets.privateKey ?? "";
  if (!privateKey && !profile?.hasPrivateKey) {
    return t("next.proxies.needsPrivateKey");
  }
  const keys: [string, string][] = [
    ["next.proxies.malformedPrivateKey", privateKey],
    ["next.proxies.malformedPeerKey", form.peerPublicKey],
    ["next.proxies.malformedPresharedKey", form.secrets.presharedKey ?? ""],
  ];
  const malformed = keys.find(([, key]) => key && !WIREGUARD_KEY.test(key));
  if (malformed) {
    return t(malformed[0]);
  }
  if (!form.peerPublicKey) {
    return t("next.proxies.needsPeerKey");
  }
  if (splitList(form.addresses).length === 0) {
    return t("next.proxies.needsAddress");
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
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ proxyProfiles: ProxyProfile[] }>({
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
  const setCleared = (key: SecretKey, cleared: boolean) =>
    setForm((current) => {
      const secrets = { ...current.secrets };
      if (cleared) {
        secrets[key] = null;
      } else {
        delete secrets[key];
      }
      return { ...current, secrets };
    });

  /** A secret the daemon holds: blank keeps it, a value replaces it, Clear removes it. */
  const storedSecret = (
    key: SecretKey,
    label: string,
    options: { password?: boolean; multiline?: boolean } = {},
  ): FieldControl => {
    const cleared = form.secrets[key] === null;
    const value = form.secrets[key] ?? "";
    const placeholder = cleared ? t("next.proxies.clearedPlaceholder") : "••••••••";
    return {
      kind: "custom",
      control: (
        <div className="flex items-start justify-end gap-2">
          {options.multiline ? (
            <TextArea
              label={label}
              value={value}
              rows={3}
              placeholder={placeholder}
              secret
              className="w-[190px]"
              onChange={(next) => setSecret(key, next)}
            />
          ) : (
            <TextField
              label={label}
              value={value}
              type={options.password ? "password" : "text"}
              secret
              placeholder={placeholder}
              className="w-[190px] max-w-full"
              onChange={(next) => setSecret(key, next)}
            />
          )}
          <SecondaryButton onClick={() => setCleared(key, !cleared)}>
            {cleared ? t("next.proxies.keepStored") : t("next.proxies.clearStored")}
          </SecondaryButton>
        </div>
      ),
    };
  };
  const storedHelp = (key: SecretKey) =>
    form.secrets[key] === null ? t("next.proxies.storedCleared") : t("next.proxies.storedKeep");

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
      setError(t("next.proxies.configTooLarge"));
      return;
    }
    const parsed = parseWireguardConfig(configText);
    if (!parsed) {
      setError(t("next.proxies.configNotWireguard"));
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
        t("next.proxies.configFilled"),
        ...(parsed.peerCount > 1 ? [t("next.proxies.configPeers", { count: parsed.peerCount })] : []),
        ...(parsed.ignored.length
          ? [t("next.proxies.configIgnored", { fields: parsed.ignored.join(", ") })]
          : []),
      ].join(" "),
    );
    setConfigText("");
  };

  const save = async () => {
    const clean = normalized(form);
    if (!clean.name.trim()) {
      setError(t("next.proxies.nameRequired"));
      return;
    }
    if (!clean.host.trim()) {
      setError(t("next.proxies.hostRequired"));
      return;
    }
    if (clean.kind === "WIRE_GUARD") {
      const problem = wireguardProblem(t, clean, editing);
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
    setHealth((current) => ({ ...current, [profile.id]: t("next.proxies.testing") }));
    const result = await testProxy({ id: profile.id });
    const outcome = result.data?.testProxyProfile;
    setHealth((current) => ({
      ...current,
      [profile.id]: outcome ? (outcome.message || (outcome.success ? t("next.proxies.reachable") : t("next.proxies.failed")))
        : (result.error?.message ?? t("next.proxies.failed")),
    }));
  };

  const isWireguard = form.kind === "WIRE_GUARD";

  const connectionFields: FieldSpec[] = [
    {
      id: "name",
      label: t("next.proxies.name"),
      control: {
        kind: "text",
        mono: false,
        value: form.name,
        onChange: (next) => patch({ name: next }),
      },
    },
    {
      id: "kind",
      label: t("next.proxies.type"),
      // The daemon keeps a profile's type for life; another type is another profile.
      help: editing ? t("next.proxies.typeLocked") : undefined,
      control: editing
        ? { kind: "static", value: proxyLabels[form.kind] }
        : {
            kind: "select",
            value: form.kind,
            options: KINDS,
            onChange: (next) =>
              patch({ kind: next as ProxyKind, port: DEFAULT_PORTS[next as ProxyKind] }),
          },
    },
    {
      id: "host",
      label: isWireguard ? t("next.proxies.endpoint") : t("next.proxies.host"),
      help: isWireguard ? t("next.proxies.endpointHelp") : undefined,
      control: { kind: "text", value: form.host, onChange: (next) => patch({ host: next }) },
    },
    {
      id: "port",
      label: t("next.proxies.port"),
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
      label: t("next.proxies.timeout"),
      control: {
        kind: "number",
        value: form.timeoutSeconds,
        min: 1,
        max: TIMEOUT_MAX,
        onChange: (next) => patch({ timeoutSeconds: next }),
        suffix: t("next.general.seconds"),
      },
    },
    {
      id: "enabled",
      label: t("next.proxies.enabled"),
      help: t("next.proxies.enabledHelp"),
      control: { kind: "toggle", value: form.enabled, onChange: (next) => patch({ enabled: next }) },
    },
  ];

  const credentialFields: FieldSpec[] = isWireguard
    ? [
        {
          id: "privateKey",
          label: t("next.proxies.privateKey"),
          help: editing?.hasPrivateKey ? t("next.proxies.storedKeep") : undefined,
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
          label: t("next.proxies.peerPublicKey"),
          control: {
            kind: "text",
            value: form.peerPublicKey,
            onChange: (next) => patch({ peerPublicKey: next }),
          },
        },
        {
          id: "presharedKey",
          label: t("next.proxies.presharedKey"),
          help: editing?.hasPresharedKey ? storedHelp("presharedKey") : t("next.proxies.optional"),
          control: editing?.hasPresharedKey
            ? storedSecret("presharedKey", t("next.proxies.presharedKey"), { password: true })
            : {
                kind: "text",
                type: "password",
                value: form.secrets.presharedKey ?? "",
                onChange: (next) => setSecret("presharedKey", next),
              },
        },
        {
          id: "addresses",
          label: t("next.proxies.addresses"),
          help: t("next.proxies.addressesHelp"),
          control: {
            kind: "textarea",
            value: form.addresses,
            rows: 2,
            onChange: (next) => patch({ addresses: next }),
          },
        },
        {
          id: "dns",
          label: t("next.proxies.dns"),
          help: t("next.proxies.dnsHelp"),
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
          help: t("next.proxies.mtuHelp", { value: MTU_DEFAULT }),
          control: {
            kind: "text",
            value: form.mtu,
            className: "w-[110px]",
            onChange: (next) => patch({ mtu: next }),
          },
        },
        {
          id: "keepaliveSeconds",
          label: t("next.proxies.keepalive"),
          help: t("next.proxies.keepaliveHelp", { seconds: KEEPALIVE_DEFAULT }),
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
          label: t("next.proxies.username"),
          help: editing?.hasUsername ? storedHelp("username") : t("next.proxies.optional"),
          control: editing?.hasUsername
            ? storedSecret("username", t("next.proxies.username"))
            : {
                kind: "text",
                secret: true,
                value: form.secrets.username ?? "",
                onChange: (next) => setSecret("username", next),
              },
        },
        {
          id: "password",
          label: t("next.proxies.password"),
          help: editing?.hasPassword ? storedHelp("password") : t("next.proxies.optional"),
          control: editing?.hasPassword
            ? storedSecret("password", t("next.proxies.password"), { password: true })
            : {
                kind: "text",
                type: "password",
                value: form.secrets.password ?? "",
                onChange: (next) => setSecret("password", next),
              },
        },
        ...(form.kind === "SSH"
          ? [
              {
                id: "privateKey",
                label: t("next.proxies.privateKey"),
                help: editing?.hasPrivateKey ? storedHelp("privateKey") : t("next.proxies.sshKeyHelp"),
                control: editing?.hasPrivateKey
                  ? storedSecret("privateKey", t("next.proxies.privateKey"), { multiline: true })
                  : {
                      kind: "textarea" as const,
                      secret: true,
                      value: form.secrets.privateKey ?? "",
                      rows: 3,
                      onChange: (next: string) => setSecret("privateKey", next),
                    },
              },
              {
                id: "passphrase",
                label: t("next.proxies.passphrase"),
                help: editing?.hasPassphrase ? storedHelp("passphrase") : t("next.proxies.optional"),
                control: editing?.hasPassphrase
                  ? storedSecret("passphrase", t("next.proxies.passphrase"), { password: true })
                  : {
                      kind: "text" as const,
                      type: "password" as const,
                      value: form.secrets.passphrase ?? "",
                      onChange: (next: string) => setSecret("passphrase", next),
                    },
              },
            ]
          : []),
      ];

  const sections: EditorSection[] = [
    { id: "connection", title: t("next.proxies.connection"), tag: <BetaTag />, fields: connectionFields },
    {
      id: "credentials",
      title: isWireguard ? t("next.proxies.tunnel") : t("next.proxies.credentials"),
      tag: <BetaTag />,
      note: t("next.proxies.secretsNote"),
      fields: credentialFields,
    },
  ];

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "proxies",
      title: t("next.settings.panel.proxies"),
      tag: <BetaTag />,
      note: t("next.proxies.tableNote"),
      columns: "minmax(0, 1fr) 150px minmax(0, 1fr) minmax(0, 1fr) 82px",
      headers: [
        t("next.proxies.name"),
        t("next.proxies.type"),
        t("next.proxies.endpoint"),
        t("next.proxies.lastTest"),
        "",
      ],
      empty: t("next.proxies.empty"),
      emptyAction: { label: t("next.proxies.add"), onClick: () => open(null) },
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
            <SecondaryButton icon="test" className="h-7 px-2" onClick={() => void runTest(profile)}>
              {t("next.proxies.test")}
            </SecondaryButton>
          </span>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => open(null)}>{t("next.proxies.add")}</PrimaryButton>
      </PanelControls>

      <BetaNotice>{t("next.proxies.betaNotice")}</BetaNotice>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? t("next.proxies.add") : (editing?.name ?? t("next.proxies.proxy"))}
        note={
          <span className="inline-flex items-center gap-2">
            <BetaTag />
            {editingId === "new" ? t("next.proxies.newNote") : proxyLabels[form.kind]}
          </span>
        }
        width={620}
        sections={sections}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel={t("next.proxies.remove")}
        extraActions={
          editing?.hostKeyFingerprint ? (
            <SecondaryButton icon="forget" onClick={() => setConfirmTrust(editing)}>
              {t("next.proxies.forgetHostKey")}
            </SecondaryButton>
          ) : null
        }
      >
        {isWireguard ? (
          <div className="flex flex-none flex-col gap-2 border-t border-wv-hairline px-4 sm:px-6 py-4">
            <div className="text-[12.5px] text-wv-muted">
              {t("next.proxies.pasteNote")}
            </div>
            <TextArea
              label={t("next.proxies.configLabel")}
              value={configText}
              rows={4}
              secret
              className="w-full"
              placeholder={"[Interface]\nPrivateKey = …\nAddress = 10.6.0.2/32\n\n[Peer]\nPublicKey = …\nEndpoint = vpn.example.com:51820"}
              onChange={setConfigText}
            />
            <div className="flex items-center justify-end gap-4">
              {note === null ? null : (
                <div className="min-w-0 flex-1 text-[12px] leading-[1.45] text-wv-muted">{note}</div>
              )}
              <SecondaryButton icon="inspectFile" onClick={applyConfig} disabled={!configText.trim()}>
                {t("next.proxies.readConfig")}
              </SecondaryButton>
            </div>
          </div>
        ) : null}
      </RecordEditor>

      <ConfirmDialog
        open={confirmRemove !== null}
        title={t("next.proxies.remove")}
        note={confirmRemove?.name}
        busy={busy}
        confirmLabel={t("next.proxies.remove")}
        body={t("next.proxies.removeBody")}
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
        title={t("next.proxies.forgetHostKey")}
        note={confirmTrust?.name}
        busy={busy}
        confirmLabel={t("next.proxies.forgetKey")}
        body={t("next.proxies.forgetBody")}
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

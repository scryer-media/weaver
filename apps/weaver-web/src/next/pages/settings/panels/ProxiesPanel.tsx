import { useId, useRef, useState } from "react";
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
import { cn } from "@/lib/utils";
import {
  WIREGUARD_KEY,
  parseWireguardConfig,
  stripConfigAssignment,
  wireguardConfigProblems,
  type WireguardConfigProblem,
} from "@/lib/wireguard-config";
import { BetaNotice, BetaTag, Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { PrimaryButton, SecondaryButton, Segmented, TextArea, TextField } from "../../../components/controls";
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
 * A WireGuard profile starts from a configuration file, uploaded or pasted,
 * which is how anyone actually has these details to hand, or from an empty
 * form. SSH signs in with an Ed25519 key and nothing else, so it has no
 * password.
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
/** The largest configuration read: no real one comes near it. */
const CONFIG_MAX = 65536;
/** Where a WireGuard profile's details come from. */
type WireguardStart = "upload" | "paste" | "manual";
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
  const secrets = { ...form.secrets };
  if (form.kind === "SSH") {
    // Typed while the profile was still another type; SSH takes none.
    delete secrets.password;
  }
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
    ...secrets,
  };
}

export function ProxiesPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ proxyProfiles: ProxyProfile[] }>({
    query: PROXY_PROFILES_QUERY,
  });
  const [, testProxy] = useMutation(TEST_PROXY_MUTATION);

  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [health, setHealth] = useState<Record<number, string>>({});

  const profiles = data?.proxyProfiles ?? [];
  const editing = typeof editingId === "number"
    ? (profiles.find((profile) => profile.id === editingId) ?? null)
    : null;

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
      emptyAction: { label: t("next.proxies.add"), onClick: () => setEditingId("new") },
      onRowClick: (id) => {
        const profile = profiles.find((entry) => String(entry.id) === id);
        if (profile) {
          setEditingId(profile.id);
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
        <PrimaryButton icon="add" onClick={() => setEditingId("new")}>{t("next.proxies.add")}</PrimaryButton>
      </PanelControls>

      <BetaNotice>{t("next.proxies.betaNotice")}</BetaNotice>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      {editingId === null ? null : (
        <ProxyEditor
          key={editingId}
          id={editingId}
          profile={editing}
          onClose={() => setEditingId(null)}
          onChanged={() => void reexecute({ requestPolicy: "network-only" })}
        />
      )}
    </>
  );
}

/**
 * A stored proxy's editor, opened by its id from outside the proxies table:
 * a proxy picked in the network flow. It reads the profiles itself, because
 * an editor needs more of a profile than a diagram does.
 */
export function ProxyEditorFor({
  id,
  onClose,
  onChanged,
}: {
  id: number;
  onClose: () => void;
  onChanged: () => void;
}) {
  const [{ data }, reexecute] = useQuery<{ proxyProfiles: ProxyProfile[] }>({ query: PROXY_PROFILES_QUERY });
  const profile = data?.proxyProfiles.find((candidate) => candidate.id === id);
  return profile ? (
    <ProxyEditor
      id={id}
      profile={profile}
      onClose={onClose}
      onChanged={() => {
        void reexecute({ requestPolicy: "network-only" });
        onChanged();
      }}
    />
  ) : null;
}

/**
 * One proxy in its editor, mounted while it is open: a stored profile's, or a
 * new one's.
 */
export function ProxyEditor({
  id: editingId,
  profile: editing,
  onClose,
  onChanged,
}: {
  id: number | "new";
  /** The stored profile, or null for a proxy that has not been saved yet. */
  profile: ProxyProfile | null;
  onClose: () => void;
  /** A save, a removal, or a forgotten host key has landed. */
  onChanged: () => void;
}) {
  const t = useTranslate();
  const [, saveProxy] = useMutation(SAVE_PROXY_MUTATION);
  const [, deleteProxy] = useMutation(DELETE_PROXY_MUTATION);
  const [, resetTrust] = useMutation(RESET_PROXY_TRUST_MUTATION);

  const [form, setForm] = useState<ProxyForm>(() => (editing ? formFor(editing) : NEW_PROXY));
  // A stored profile opens on its details; a new one on the file most people have them in.
  const [start, setStart] = useState<WireguardStart>(editing ? "manual" : "upload");
  const [dragging, setDragging] = useState(false);
  const fileNameId = useId();
  const [configText, setConfigText] = useState("");
  const [fileName, setFileName] = useState<string | null>(null);
  const [fileProblems, setFileProblems] = useState<string[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<ProxyProfile | null>(null);
  const [confirmTrust, setConfirmTrust] = useState<ProxyProfile | null>(null);

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

  const describeProblem = (problem: WireguardConfigProblem) =>
    problem.kind === "missing"
      ? t("next.proxies.configMissing", { key: problem.key, section: problem.section })
      : problem.kind === "key"
        ? t("next.proxies.configBadKey", { key: problem.key })
        : problem.kind === "number"
          ? t("next.proxies.configBadNumber", { key: problem.key })
          : t("next.proxies.configBadEndpoint");

  /** Everything wrong with a configuration, in the order its keys are written. */
  const configProblems = (text: string): string[] => {
    if (text.length > CONFIG_MAX) {
      return [t("next.proxies.configTooLarge")];
    }
    const parsed = parseWireguardConfig(text);
    return parsed ? wireguardConfigProblems(parsed).map(describeProblem) : [t("next.proxies.configNotWireguard")];
  };

  /**
   * Fill the form from a `wg-quick` configuration that has nothing wrong with
   * it, and open the form for it to be checked.
   *
   * Only what the file names is written, so what it leaves out keeps what the
   * form held, and whatever the file carried that a tunnel proxy has no use
   * for is reported rather than silently dropped.
   */
  const applyConfig = (text: string) => {
    const parsed = parseWireguardConfig(text);
    if (!parsed) {
      return;
    }
    const endpoint = parsed.endpoint ? splitEndpoint(parsed.endpoint) : null;
    const keepalive = /^off$/i.test(parsed.tunnelKeepaliveSeconds) ? "0" : parsed.tunnelKeepaliveSeconds;
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
      keepaliveSeconds: digits(keepalive, CONFIG_KEYS.keepalive) || current.keepaliveSeconds,
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
    setFileName(null);
    setFileProblems([]);
    setStart("manual");
  };

  const readFile = async (file: File) => {
    setFileName(file.name);
    setFileProblems([]);
    if (file.size > CONFIG_MAX) {
      setFileProblems([t("next.proxies.configTooLarge")]);
      return;
    }
    let content: string;
    try {
      content = await file.text();
    } catch {
      setFileProblems([t("next.proxies.configUnreadable")]);
      return;
    }
    const problems = configProblems(content);
    if (problems.length > 0) {
      setFileProblems(problems);
      return;
    }
    applyConfig(content);
  };

  const pasteProblems = configText.trim() ? configProblems(configText) : [];
  const pasteReady = configText.trim() !== "" && pasteProblems.length === 0;

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
    onClose();
    onChanged();
  };

  const isWireguard = form.kind === "WIRE_GUARD";
  const isSsh = form.kind === "SSH";
  /** A WireGuard profile's details stay shut until it is known where they come from. */
  const detailsOpen = !isWireguard || start === "manual";

  const endpointFields: FieldSpec[] = [
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
  ];

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
    // A WireGuard endpoint is one of the details its configuration carries, so it sits with them.
    ...(isWireguard ? [] : endpointFields),
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
        ...endpointFields,
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
          help: editing?.hasUsername
            ? storedHelp("username")
            : isSsh ? t("next.proxies.sshUsernameHelp") : t("next.proxies.optional"),
          control: editing?.hasUsername
            ? storedSecret("username", t("next.proxies.username"))
            : {
                kind: "text",
                secret: true,
                value: form.secrets.username ?? "",
                onChange: (next) => setSecret("username", next),
              },
        },
        // SSH signs in with its key alone: there is no password to give it.
        ...(isSsh
          ? []
          : [
              {
                id: "password",
                label: t("next.proxies.password"),
                help: editing?.hasPassword ? storedHelp("password") : t("next.proxies.optional"),
                control: editing?.hasPassword
                  ? storedSecret("password", t("next.proxies.password"), { password: true })
                  : {
                      kind: "text" as const,
                      type: "password" as const,
                      value: form.secrets.password ?? "",
                      onChange: (next: string) => setSecret("password", next),
                    },
              },
            ]),
        ...(isSsh
          ? [
              {
                id: "privateKey",
                label: t("next.proxies.privateKey"),
                help: editing?.hasPrivateKey
                  ? `${t("next.proxies.sshKeyHelp")} ${storedHelp("privateKey")}`
                  : t("next.proxies.sshKeyHelp"),
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

  const problemList = (problems: readonly string[]) =>
    problems.length === 0 ? null : (
      <ul role="alert" className="flex flex-col gap-1 text-[12.5px] leading-[1.5] text-wv-error-text">
        {problems.map((problem) => (
          <li key={problem}>{problem}</li>
        ))}
      </ul>
    );

  const startBody = (
    <div className="flex flex-none flex-col gap-3 border-b border-wv-hairline px-4 py-4 sm:px-6">
      <Segmented
        label={t("next.proxies.wgStart")}
        value={start}
        className="w-full [&>button]:flex-1 [&>button]:justify-center"
        options={[
          { value: "upload", label: t("next.proxies.wgUpload") },
          { value: "paste", label: t("next.proxies.wgPaste") },
          { value: "manual", label: t("next.proxies.wgManual") },
        ]}
        onChange={(next) => setStart(next as WireguardStart)}
      />
      {start === "upload" ? (
        <>
          <div className="text-[12.5px] leading-[1.5] text-wv-muted">{t("next.proxies.wgUploadNote")}</div>
          <input
            ref={fileRef}
            type="file"
            accept=".conf,.txt,text/plain"
            aria-label={t("next.proxies.configLabel")}
            className="hidden"
            onChange={(event) => {
              const file = event.target.files?.[0];
              // Cleared so the same file, corrected, can be chosen again.
              event.target.value = "";
              if (file) {
                void readFile(file);
              }
            }}
          />
          <button
            type="button"
            // The file last read is described, not named, so the target keeps one name.
            aria-label={t("next.proxies.wgDropZone")}
            aria-describedby={fileName === null ? undefined : fileNameId}
            onClick={() => fileRef.current?.click()}
            onDragOver={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={(event) => {
              event.preventDefault();
              setDragging(false);
              const file = event.dataTransfer.files[0];
              if (file) {
                void readFile(file);
              }
            }}
            className={cn(
              "flex h-[104px] flex-none cursor-pointer flex-col items-center justify-center gap-2 border border-dashed px-4 text-[13px]",
              dragging
                ? "border-wv-accent bg-wv-selected text-wv-strong"
                : "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover-strong",
            )}
          >
            <span className="font-medium text-wv-fg">{t("next.proxies.wgDropZone")}</span>
            {fileName === null ? null : (
              <span id={fileNameId} className="max-w-full truncate font-wv-mono text-[11px] text-wv-faint">
                {fileName}
              </span>
            )}
          </button>
          {problemList(fileProblems)}
        </>
      ) : null}
      {start === "paste" ? (
        <>
          <div className="text-[12.5px] leading-[1.5] text-wv-muted">{t("next.proxies.wgPasteNote")}</div>
          <TextArea
            label={t("next.proxies.configLabel")}
            value={configText}
            rows={9}
            secret
            className="w-full"
            placeholder={"[Interface]\nPrivateKey = …\nAddress = 10.6.0.2/32\n\n[Peer]\nPublicKey = …\nEndpoint = vpn.example.com:51820"}
            onChange={setConfigText}
          />
          {problemList(pasteProblems)}
          <div className="flex items-center justify-end gap-4">
            {pasteReady ? (
              <div className="min-w-0 flex-1 text-[12px] leading-[1.45] text-wv-muted">
                {t("next.proxies.configValid")}
              </div>
            ) : null}
            <SecondaryButton icon="inspectFile" onClick={() => applyConfig(configText)} disabled={!pasteReady}>
              {t("next.proxies.parse")}
            </SecondaryButton>
          </div>
        </>
      ) : null}
      {start === "manual" && note !== null ? (
        <div className="text-[12px] leading-[1.45] text-wv-muted">{note}</div>
      ) : null}
    </div>
  );

  const sections: EditorSection[] = [
    { id: "connection", title: t("next.proxies.connection"), tag: <BetaTag />, fields: connectionFields },
    ...(isWireguard
      ? [{ id: "start", title: t("next.proxies.wgStart"), tag: <BetaTag />, fields: [], body: startBody }]
      : []),
    ...(detailsOpen
      ? [
          {
            id: "credentials",
            title: isWireguard ? t("next.proxies.tunnel") : t("next.proxies.credentials"),
            tag: <BetaTag />,
            note: t("next.proxies.secretsNote"),
            fields: credentialFields,
          },
        ]
      : []),
  ];

  return (
    <>
      <RecordEditor
        open
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
        saveDisabled={!detailsOpen}
        onSave={() => void save()}
        onDismiss={onClose}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel={t("next.proxies.remove")}
        extraActions={
          editing?.hostKeyFingerprint ? (
            <SecondaryButton icon="forget" onClick={() => setConfirmTrust(editing)}>
              {t("next.proxies.forgetHostKey")}
            </SecondaryButton>
          ) : null
        }
      />

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
              onClose();
              onChanged();
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
              onChanged();
            });
          }
        }}
        onDismiss={() => setConfirmTrust(null)}
      />
    </>
  );
}

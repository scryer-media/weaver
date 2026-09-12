import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  ACCESS_POLICY_QUERY,
  API_KEYS_QUERY,
  CHANGE_PASSWORD_MUTATION,
  CREATE_API_KEY_MUTATION,
  DELETE_API_KEY_MUTATION,
  DISABLE_LOGIN_MUTATION,
  ENABLE_LOGIN_MUTATION,
  HTTP_BIND_ADDRESS_QUERY,
  LOGIN_STATUS_QUERY,
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
} from "@/graphql/queries";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { Dialog } from "../../../components/Dialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { PrimaryButton, SecondaryButton, TextField } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WV } from "../../../data/palette";
import { formatDate } from "../../../data/format";
import {
  PanelControls,
  SettingsBlocks,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Security: who may reach this weaver, and with what.
 *
 * The two settings the daemon stores — the listen address and the access
 * policy — are one draft behind the top bar's Save, even though they are two
 * mutations. Everything else here is an action rather than a setting, so it
 * lives behind its own button: turning login on needs a username and password,
 * turning it off needs a confirmation, and a key can only be read once.
 */

interface LoginStatus {
  enabled: boolean;
  username: string | null;
}

interface BindAddressStatus {
  address: string;
  storedAddress: string | null;
  source: string;
  editable: boolean;
  exposedWithoutLogin: boolean;
  restartRequired: boolean;
  bindFallback: string | null;
}

interface AccessPolicyStatus {
  mode: string;
  trustedNetworks: string[];
  editable: boolean;
  envPinned: boolean;
}

interface ApiKey {
  id: number;
  name: string;
  scope: string;
  createdAt: number;
  lastUsedAt: number | null;
}

const ACCESS_MODES: { value: string; label: string }[] = [
  { value: "login_required", label: "Login required for every browser" },
  { value: "login_except_local", label: "Login required, except trusted networks" },
  { value: "no_login", label: "No login (this machine only)" },
];

const KEY_SCOPES: { value: string; label: string }[] = [
  { value: "READ", label: "Read only" },
  { value: "CONTROL", label: "Control the queue" },
  { value: "ADMIN", label: "Full administration" },
];

function cleanMessage(message: string): string {
  return message.replace(/^\[GraphQL\]\s*/, "");
}

export function SecurityPanel() {
  const [{ data: loginData }, refetchLogin] = useQuery<{ adminLoginStatus: LoginStatus }>({
    query: LOGIN_STATUS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: bindData }, refetchBind] = useQuery<{ httpBindAddress: BindAddressStatus }>({
    query: HTTP_BIND_ADDRESS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: policyData }, refetchPolicy] = useQuery<{ accessPolicy: AccessPolicyStatus }>({
    query: ACCESS_POLICY_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: keysData }, refetchKeys] = useQuery<{ apiKeys: ApiKey[] }>({
    query: API_KEYS_QUERY,
  });

  const [, setBindAddress] = useMutation(SET_HTTP_BIND_ADDRESS_MUTATION);
  const [, setAccessPolicy] = useMutation(SET_ACCESS_POLICY_MUTATION);
  const [, enableLogin] = useMutation(ENABLE_LOGIN_MUTATION);
  const [, disableLogin] = useMutation(DISABLE_LOGIN_MUTATION);
  const [, changePassword] = useMutation(CHANGE_PASSWORD_MUTATION);
  const [, createApiKey] = useMutation(CREATE_API_KEY_MUTATION);
  const [, deleteApiKey] = useMutation(DELETE_API_KEY_MUTATION);

  const login = loginData?.adminLoginStatus;
  const bind = bindData?.httpBindAddress;
  const policy = policyData?.accessPolicy;
  const keys = keysData?.apiKeys ?? [];

  const [address, setAddress] = useState("");
  const [mode, setMode] = useState("");
  const [networks, setNetworks] = useState("");
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  // The address draft mirrors the STORED value only: an empty box means "not
  // configured", and re-proposing the running address would let Save quietly
  // undo a deliberate clear.
  useEffect(() => {
    if (!dirty && bind) {
      setAddress(bind.storedAddress ?? "");
    }
  }, [bind, dirty]);

  useEffect(() => {
    if (!dirty && policy) {
      setMode(policy.mode);
      setNetworks(policy.trustedNetworks.join("\n"));
    }
  }, [dirty, policy]);

  const edit = (apply: () => void) => {
    apply();
    setDirty(true);
    setStatus(null);
    setFailed(false);
  };

  const storedNetworks = useMemo(() => policy?.trustedNetworks.join("\n") ?? "", [policy]);

  usePanelState({
    dirty,
    busy,
    status,
    failed,
    revert: () => {
      setAddress(bind?.storedAddress ?? "");
      setMode(policy?.mode ?? "");
      setNetworks(storedNetworks);
      setDirty(false);
      setStatus(null);
      setFailed(false);
    },
    save: () => {
      setBusy(true);
      setFailed(false);
      void (async () => {
        const messages: string[] = [];
        if (bind?.editable && address.trim() !== (bind.storedAddress ?? "")) {
          const result = await setBindAddress({ address: address.trim() });
          if (result.error) {
            setStatus(cleanMessage(result.error.message));
            setFailed(true);
            setBusy(false);
            return;
          }
          messages.push(
            address.trim()
              ? "Listen address saved — restart weaver to apply it"
              : "Listen address cleared — weaver returns to this machine at its next restart",
          );
          void refetchBind({ requestPolicy: "network-only" });
        }
        if (policy && !policy.envPinned && (mode !== policy.mode || networks !== storedNetworks)) {
          const result = await setAccessPolicy({
            mode,
            trustedNetworks:
              mode === "login_except_local"
                ? networks
                    .split("\n")
                    .map((line) => line.trim())
                    .filter((line) => line.length > 0)
                : undefined,
          });
          if (result.error) {
            setStatus(cleanMessage(result.error.message));
            setFailed(true);
            setBusy(false);
            return;
          }
          messages.push("Access policy applied");
          void refetchPolicy({ requestPolicy: "network-only" });
        }
        setBusy(false);
        setDirty(false);
        setStatus(messages.join(" · ") || "Saved");
      })();
    },
  });

  /* ------------------------------------------------------------- dialogs */

  const [enableOpen, setEnableOpen] = useState(false);
  const [enableForm, setEnableForm] = useState({ username: "", password: "", confirm: "" });
  const [enableError, setEnableError] = useState<string | null>(null);

  const [passwordOpen, setPasswordOpen] = useState(false);
  const [passwordForm, setPasswordForm] = useState({ current: "", next: "", confirm: "" });
  const [passwordError, setPasswordError] = useState<string | null>(null);

  const [disableOpen, setDisableOpen] = useState(false);

  const [keyOpen, setKeyOpen] = useState(false);
  const [keyForm, setKeyForm] = useState({ name: "", scope: "CONTROL" });
  const [keyError, setKeyError] = useState<string | null>(null);
  const [createdKey, setCreatedKey] = useState<{ name: string; rawKey: string } | null>(null);
  const [copied, setCopied] = useState(false);
  const [removeKey, setRemoveKey] = useState<ApiKey | null>(null);

  const submitEnable = async () => {
    if (enableForm.password !== enableForm.confirm) {
      setEnableError("The two passwords do not match.");
      return;
    }
    if (!enableForm.username.trim() || !enableForm.password) {
      setEnableError("A username and a password are both required.");
      return;
    }
    setBusy(true);
    const result = await enableLogin({
      username: enableForm.username.trim(),
      password: enableForm.password,
    });
    setBusy(false);
    if (result.error) {
      setEnableError(cleanMessage(result.error.message));
      return;
    }
    setEnableOpen(false);
    setEnableForm({ username: "", password: "", confirm: "" });
    void refetchLogin({ requestPolicy: "network-only" });
  };

  const submitPassword = async () => {
    if (passwordForm.next !== passwordForm.confirm) {
      setPasswordError("The two passwords do not match.");
      return;
    }
    setBusy(true);
    const result = await changePassword({
      currentPassword: passwordForm.current,
      newPassword: passwordForm.next,
    });
    setBusy(false);
    if (result.error) {
      setPasswordError(cleanMessage(result.error.message));
      return;
    }
    setPasswordOpen(false);
    setPasswordForm({ current: "", next: "", confirm: "" });
  };

  const submitKey = async () => {
    if (!keyForm.name.trim()) {
      setKeyError("Name the key after whatever will use it.");
      return;
    }
    setBusy(true);
    const result = await createApiKey({ name: keyForm.name.trim(), scope: keyForm.scope });
    setBusy(false);
    if (result.error || !result.data?.createApiKey?.rawKey) {
      setKeyError(result.error ? cleanMessage(result.error.message) : "The key could not be created.");
      return;
    }
    setKeyOpen(false);
    setCreatedKey({ name: keyForm.name.trim(), rawKey: result.data.createApiKey.rawKey });
    setCopied(false);
    setKeyForm({ name: "", scope: "CONTROL" });
    void refetchKeys({ requestPolicy: "network-only" });
  };

  /* -------------------------------------------------------------- blocks */

  const accessFields: FieldSpec[] = [
    {
      id: "bindAddress",
      label: "Listen address",
      help: bind?.editable
        ? "Weaver answers on 127.0.0.1 by default, which only this machine can reach. Use 0.0.0.0 for every interface. Takes effect at the next restart."
        : "Pinned by WEAVER_HTTP_BIND_ADDRESS in weaver's environment; change it in your deployment instead.",
      keywords: `${bind?.address ?? ""} bind interface host port`,
      control: bind?.editable
        ? {
            kind: "text",
            value: address,
            placeholder: "127.0.0.1 (default)",
            onChange: (next) => edit(() => setAddress(next)),
          }
        : { kind: "static", value: bind?.address ?? "—" },
    },
    {
      id: "accessMode",
      label: "Browser access",
      help: policy?.envPinned
        ? "Managed by WEAVER_TRUSTED_CIDRS in weaver's environment."
        : "Who may use the web interface without signing in.",
      keywords: "login policy trusted network cidr",
      control: policy?.envPinned
        ? { kind: "static", value: policy.trustedNetworks.join(", ") || "none" }
        : {
            kind: "select",
            value: mode,
            options: ACCESS_MODES,
            className: "min-w-0 sm:min-w-[320px]",
            onChange: (next) => edit(() => setMode(next)),
          },
    },
    ...(!policy?.envPinned && mode === "login_except_local"
      ? [
          {
            id: "trustedNetworks",
            label: "Trusted networks",
            help: "One CIDR per line. Browsers inside these ranges skip the sign-in.",
            keywords: "cidr subnet lan",
            control: {
              kind: "textarea" as const,
              value: networks,
              rows: 4,
              placeholder: "192.168.1.0/24",
              onChange: (next: string) => edit(() => setNetworks(next)),
            },
          },
        ]
      : []),
  ];

  const blocks: SettingsBlock[] = [
    {
      kind: "section",
      id: "sign-in",
      title: "Sign-in",
      note: login?.enabled ? `signed in as ${login.username ?? "admin"}` : "not configured",
      fields: [
        {
          id: "loginEnabled",
          label: "Require a login",
          help: login?.enabled
            ? "Every browser must sign in before it can see anything."
            : "Anyone who can reach weaver has full administrative access.",
          keywords: "password admin authentication",
          control: {
            kind: "custom",
            control: login?.enabled ? (
              <>
                <SecondaryButton onClick={() => setPasswordOpen(true)}>
                  Change password
                </SecondaryButton>
                <SecondaryButton onClick={() => setDisableOpen(true)}>Turn off</SecondaryButton>
              </>
            ) : (
              <PrimaryButton onClick={() => setEnableOpen(true)}>Set up a login</PrimaryButton>
            ),
          },
        },
      ],
    },
    { kind: "section", id: "access", title: "Network access", fields: accessFields },
    {
      kind: "table",
      id: "keys",
      title: "API keys",
      note: "shown once, at the moment they are created",
      columns: "minmax(0, 1fr) 160px 150px 150px 92px",
      headers: ["Name", "Scope", "Created", "Last used", ""],
      empty: "No API keys. Sonarr, Radarr and the NZBGet facade each need one.",
      rows: keys.map((key) => ({
        id: String(key.id),
        searchText: `${key.name} ${key.scope}`,
        cells: [
          <Cell key="name">{key.name}</Cell>,
          <Cell key="scope" mono className="text-wv-secondary">
            {KEY_SCOPES.find((scope) => scope.value === key.scope)?.label ?? key.scope}
          </Cell>,
          <Cell key="created" mono className="text-wv-muted">
            {formatDate(key.createdAt)}
          </Cell>,
          <Cell key="used" mono className="text-wv-muted">
            {key.lastUsedAt ? formatDate(key.lastUsedAt) : "never"}
          </Cell>,
          <SecondaryButton key="revoke" className="h-7 px-2" onClick={() => setRemoveKey(key)}>
            Revoke
          </SecondaryButton>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton onClick={() => setKeyOpen(true)}>New API key</SecondaryButton>
      </PanelControls>

      {bind?.bindFallback ? (
        <div className="flex flex-none items-center gap-[9px] border-b border-wv-hairline bg-wv-cell-hover px-4 sm:px-6 py-3 text-[12.5px] text-wv-warn">
          <Square color={WV.warn} />
          {bind.bindFallback}
        </div>
      ) : null}
      {bind?.exposedWithoutLogin && !login?.enabled ? (
        <div className="flex flex-none items-center gap-[9px] border-b border-wv-hairline bg-wv-cell-hover px-4 sm:px-6 py-3 text-[12.5px] text-wv-warn">
          <Square color={WV.warn} />
          Weaver will be reachable beyond this machine after the next restart while no login is
          configured.
        </div>
      ) : null}

      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={enableOpen}
        title="Set up a login"
        note="applies immediately"
        error={enableError}
        busy={busy}
        saveLabel="Turn on login"
        onSave={() => void submitEnable()}
        onDismiss={() => setEnableOpen(false)}
        sections={[
          {
            id: "credentials",
            title: "Administrator",
            fields: [
              {
                id: "username",
                label: "Username",
                control: {
                  kind: "text",
                  mono: false,
                  value: enableForm.username,
                  onChange: (next) =>
                    setEnableForm((current) => ({ ...current, username: next })),
                },
              },
              {
                id: "password",
                label: "Password",
                control: {
                  kind: "text",
                  type: "password",
                  value: enableForm.password,
                  onChange: (next) =>
                    setEnableForm((current) => ({ ...current, password: next })),
                },
              },
              {
                id: "confirm",
                label: "Repeat the password",
                control: {
                  kind: "text",
                  type: "password",
                  value: enableForm.confirm,
                  onChange: (next) => setEnableForm((current) => ({ ...current, confirm: next })),
                },
              },
            ],
          },
        ]}
      />

      <RecordEditor
        open={passwordOpen}
        title="Change password"
        error={passwordError}
        busy={busy}
        saveLabel="Change password"
        onSave={() => void submitPassword()}
        onDismiss={() => setPasswordOpen(false)}
        sections={[
          {
            id: "password",
            title: "Password",
            fields: [
              {
                id: "current",
                label: "Current password",
                control: {
                  kind: "text",
                  type: "password",
                  value: passwordForm.current,
                  onChange: (next) =>
                    setPasswordForm((current) => ({ ...current, current: next })),
                },
              },
              {
                id: "next",
                label: "New password",
                control: {
                  kind: "text",
                  type: "password",
                  value: passwordForm.next,
                  onChange: (next) => setPasswordForm((current) => ({ ...current, next })),
                },
              },
              {
                id: "confirm",
                label: "Repeat the new password",
                control: {
                  kind: "text",
                  type: "password",
                  value: passwordForm.confirm,
                  onChange: (next) =>
                    setPasswordForm((current) => ({ ...current, confirm: next })),
                },
              },
            ],
          },
        ]}
      />

      <RecordEditor
        open={keyOpen}
        title="New API key"
        note="you will only see it once"
        error={keyError}
        busy={busy}
        saveLabel="Create key"
        onSave={() => void submitKey()}
        onDismiss={() => setKeyOpen(false)}
        sections={[
          {
            id: "key",
            title: "Key",
            fields: [
              {
                id: "name",
                label: "Name",
                help: "Name it after whatever will use it — Sonarr, a script, a phone.",
                control: {
                  kind: "text",
                  mono: false,
                  value: keyForm.name,
                  onChange: (next) => setKeyForm((current) => ({ ...current, name: next })),
                },
              },
              {
                id: "scope",
                label: "Scope",
                help: "Read sees the queue; Control may add and remove work; Admin may change settings.",
                control: {
                  kind: "select",
                  value: keyForm.scope,
                  options: KEY_SCOPES,
                  onChange: (next) => setKeyForm((current) => ({ ...current, scope: next })),
                },
              },
            ],
          },
        ]}
      />

      <Dialog
        open={createdKey !== null}
        title="API key created"
        note={createdKey?.name}
        width={560}
        onDismiss={() => setCreatedKey(null)}
        footer={
          <>
            <SecondaryButton
              onClick={() => {
                if (createdKey) {
                  void navigator.clipboard.writeText(createdKey.rawKey).then(() => setCopied(true));
                }
              }}
            >
              {copied ? "Copied" : "Copy key"}
            </SecondaryButton>
            <PrimaryButton onClick={() => setCreatedKey(null)}>Done</PrimaryButton>
          </>
        }
      >
        <div className="flex flex-col gap-3 px-4 sm:px-6 py-5">
          <div className="text-[13px] leading-[1.55] text-wv-secondary">
            Copy this now. Weaver stores only a hash of it, so it cannot be shown again.
          </div>
          <TextField
            label="API key"
            value={createdKey?.rawKey ?? ""}
            onChange={() => undefined}
            className="w-full"
          />
        </div>
      </Dialog>

      <ConfirmDialog
        open={disableOpen}
        title="Turn off the login"
        busy={busy}
        confirmLabel="Turn off login"
        body="Anyone who can reach weaver will have full administrative access without signing in."
        onConfirm={() => {
          void disableLogin({}).then(() => {
            setDisableOpen(false);
            void refetchLogin({ requestPolicy: "network-only" });
          });
        }}
        onDismiss={() => setDisableOpen(false)}
      />

      <ConfirmDialog
        open={removeKey !== null}
        title="Revoke API key"
        note={removeKey?.name}
        busy={busy}
        confirmLabel="Revoke key"
        body={`Anything still using ${removeKey?.name ?? "this key"} will stop working immediately.`}
        onConfirm={() => {
          if (removeKey) {
            void deleteApiKey({ id: removeKey.id }).then(() => {
              setRemoveKey(null);
              void refetchKeys({ requestPolicy: "network-only" });
            });
          }
        }}
        onDismiss={() => setRemoveKey(null)}
      />
    </>
  );
}

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
  NETWORK_ACCESS_QUERY,
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
} from "@/graphql/queries";
import { SecuritySettingsPage } from "@/pages/settings/SecuritySettingsPage";
import { useTranslate } from "@/lib/context/translate-context";
import { noteLoginEnabled } from "@/lib/login-required";
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

/** Labels are translation keys, resolved when the panel renders. */
const ACCESS_MODES: { value: string; label: string }[] = [
  { value: "login_required", label: "next.security.mode.required" },
  { value: "login_except_local", label: "next.security.mode.exceptTrusted" },
  { value: "no_login", label: "next.security.mode.noLogin" },
];

const KEY_SCOPES: { value: string; label: string }[] = [
  { value: "READ", label: "next.security.scope.read" },
  { value: "CONTROL", label: "next.security.scope.control" },
  { value: "ADMIN", label: "next.security.scope.admin" },
];

function cleanMessage(message: string): string {
  return message.replace(/^\[GraphQL\]\s*/, "");
}

export function SecurityPanel() {
  const [{ data, error }] = useQuery<{ networkAccess: { authenticatedAccess: boolean } }>({
    query: NETWORK_ACCESS_QUERY,
    requestPolicy: "network-only",
  });
  if (error) {
    return <p role="alert" className="p-6 text-wv-error-text">{cleanMessage(error.message)}</p>;
  }
  if (!data) return null;
  // Share the authenticated controls so preview, reauthentication and drafts
  // have the same behavior in both interfaces. Legacy deployments retain their panel.
  if (data.networkAccess.authenticatedAccess) {
    return <div className="min-h-0 flex-1 overflow-auto p-4 sm:p-6"><SecuritySettingsPage embedded /></div>;
  }
  return <LegacySecurityPanel />;
}

function LegacySecurityPanel() {
  const t = useTranslate();
  const [{ data: loginData, fetching: loginFetching }, refetchLogin] = useQuery<{ adminLoginStatus: LoginStatus }>({
    query: LOGIN_STATUS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: bindData, fetching: bindFetching }, refetchBind] = useQuery<{ httpBindAddress: BindAddressStatus }>({
    query: HTTP_BIND_ADDRESS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: policyData, fetching: policyFetching }, refetchPolicy] = useQuery<{ accessPolicy: AccessPolicyStatus }>({
    query: ACCESS_POLICY_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: keysData, fetching: keysFetching }, refetchKeys] = useQuery<{ apiKeys: ApiKey[] }>({
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
  const loginEnabled = login?.enabled;

  // The shell's sign-out button follows the login this panel turns on and off.
  useEffect(() => {
    if (loginEnabled !== undefined) {
      noteLoginEnabled(loginEnabled);
    }
  }, [loginEnabled]);
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
              ? t("next.security.listenSaved")
              : t("next.security.listenCleared"),
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
          messages.push(t("next.security.policyApplied"));
          void refetchPolicy({ requestPolicy: "network-only" });
        }
        setBusy(false);
        setDirty(false);
        setStatus(messages.join(" · ") || t("next.settings.saved"));
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
      setEnableError(t("next.security.passwordsMismatch"));
      return;
    }
    if (!enableForm.username.trim() || !enableForm.password) {
      setEnableError(t("next.security.credentialsRequired"));
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
      setPasswordError(t("next.security.passwordsMismatch"));
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
      setKeyError(t("next.security.keyNameRequired"));
      return;
    }
    setBusy(true);
    const result = await createApiKey({ name: keyForm.name.trim(), scope: keyForm.scope });
    setBusy(false);
    if (result.error || !result.data?.createApiKey?.rawKey) {
      setKeyError(result.error ? cleanMessage(result.error.message) : t("next.security.keyCreateFailed"));
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
      label: t("next.security.listenAddress"),
      help: bind?.editable
        ? t("next.security.listenHelp")
        : t("next.security.listenPinnedHelp", { variable: "WEAVER_HTTP_BIND_ADDRESS" }),
      keywords: `${bind?.address ?? ""} bind interface host port`,
      control: bind?.editable
        ? {
            kind: "text",
            value: address,
            placeholder: t("next.security.listenPlaceholder", { address: "127.0.0.1" }),
            onChange: (next) => edit(() => setAddress(next)),
          }
        : { kind: "static", value: bind?.address ?? "—" },
    },
    {
      id: "accessMode",
      label: t("next.security.browserAccess"),
      help: policy?.envPinned
        ? t("next.security.accessPinnedHelp", { variable: "WEAVER_TRUSTED_CIDRS" })
        : t("next.security.accessHelp"),
      keywords: "login policy trusted network cidr",
      control: policy?.envPinned
        ? { kind: "static", value: policy.trustedNetworks.join(", ") || t("next.job.none") }
        : {
            kind: "select",
            value: mode,
            options: ACCESS_MODES.map((option) => ({ ...option, label: t(option.label) })),
            className: "min-w-0 sm:min-w-[320px]",
            onChange: (next) => edit(() => setMode(next)),
          },
    },
    ...(!policy?.envPinned && mode === "login_except_local"
      ? [
          {
            id: "trustedNetworks",
            label: t("next.security.trustedNetworks"),
            help: t("next.security.trustedNetworksHelp"),
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

  const scopeLabel = (scope: string) => {
    const entry = KEY_SCOPES.find((option) => option.value === scope);
    return entry ? t(entry.label) : scope;
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "section",
      id: "sign-in",
      title: t("next.security.signIn"),
      note: login?.enabled
        ? t("next.security.signedInAs", { name: login.username ?? "admin" })
        : t("next.security.notConfigured"),
      fields: [
        {
          id: "loginEnabled",
          label: t("next.security.requireLogin"),
          help: login?.enabled
            ? t("next.security.requireLoginOnHelp")
            : t("next.security.requireLoginOffHelp"),
          keywords: "password admin authentication",
          control: {
            kind: "custom",
            control: login?.enabled ? (
              <>
                <SecondaryButton icon="password" onClick={() => setPasswordOpen(true)}>
                  {t("next.security.changePassword")}
                </SecondaryButton>
                <SecondaryButton icon="unlock" onClick={() => setDisableOpen(true)}>
                  {t("next.security.turnOff")}
                </SecondaryButton>
              </>
            ) : (
              <PrimaryButton icon="lock" onClick={() => setEnableOpen(true)}>
                {t("next.security.setUpLogin")}
              </PrimaryButton>
            ),
          },
        },
      ],
    },
    { kind: "section", id: "access", title: t("next.security.networkAccess"), fields: accessFields },
    {
      kind: "table",
      id: "keys",
      title: t("next.security.apiKeys"),
      note: t("next.security.keysNote"),
      columns: "minmax(0, 1fr) 160px 150px 150px 92px",
      headers: [
        t("next.security.name"),
        t("next.security.scope"),
        t("next.security.created"),
        t("next.security.lastUsed"),
        "",
      ],
      empty: t("next.security.keysEmpty"),
      emptyAction: { label: t("next.security.addKey"), onClick: () => setKeyOpen(true) },
      rows: keys.map((key) => ({
        id: String(key.id),
        searchText: `${key.name} ${key.scope} ${scopeLabel(key.scope)}`,
        cells: [
          <Cell key="name">{key.name}</Cell>,
          <Cell key="scope" mono className="text-wv-secondary">
            {scopeLabel(key.scope)}
          </Cell>,
          <Cell key="created" mono className="text-wv-muted">
            {formatDate(key.createdAt)}
          </Cell>,
          <Cell key="used" mono className="text-wv-muted">
            {key.lastUsedAt ? formatDate(key.lastUsedAt) : t("next.security.never")}
          </Cell>,
          <SecondaryButton icon="remove" key="revoke" className="h-7 px-2" onClick={() => setRemoveKey(key)}>
            {t("next.security.revoke")}
          </SecondaryButton>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => setKeyOpen(true)}>{t("next.security.addKey")}</PrimaryButton>
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
          {t("next.security.exposedWarning")}
        </div>
      ) : null}

      <SettingsBlocks
        blocks={blocks}
        loading={
          (loginFetching && !loginData)
          || (bindFetching && !bindData)
          || (policyFetching && !policyData)
          || (keysFetching && !keysData)
        }
      />

      <RecordEditor
        open={enableOpen}
        title={t("next.security.setUpLogin")}
        note={t("next.security.appliesImmediately")}
        error={enableError}
        busy={busy}
        saveLabel={t("next.security.turnOnLogin")}
        onSave={() => void submitEnable()}
        onDismiss={() => setEnableOpen(false)}
        sections={[
          {
            id: "credentials",
            title: t("next.security.administrator"),
            fields: [
              {
                id: "username",
                label: t("next.security.username"),
                control: {
                  kind: "text",
                  mono: false,
                  autoComplete: "username",
                  value: enableForm.username,
                  onChange: (next) =>
                    setEnableForm((current) => ({ ...current, username: next })),
                },
              },
              {
                id: "password",
                label: t("next.security.password"),
                control: {
                  kind: "text",
                  type: "password",
                  autoComplete: "new-password",
                  value: enableForm.password,
                  onChange: (next) =>
                    setEnableForm((current) => ({ ...current, password: next })),
                },
              },
              {
                id: "confirm",
                label: t("next.security.repeatPassword"),
                control: {
                  kind: "text",
                  type: "password",
                  autoComplete: "new-password",
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
        title={t("next.security.changePassword")}
        error={passwordError}
        busy={busy}
        saveLabel={t("next.security.changePassword")}
        onSave={() => void submitPassword()}
        onDismiss={() => setPasswordOpen(false)}
        sections={[
          {
            id: "password",
            title: t("next.security.password"),
            fields: [
              {
                id: "current",
                label: t("next.security.currentPassword"),
                control: {
                  kind: "text",
                  type: "password",
                  autoComplete: "current-password",
                  value: passwordForm.current,
                  onChange: (next) =>
                    setPasswordForm((current) => ({ ...current, current: next })),
                },
              },
              {
                id: "next",
                label: t("next.security.newPassword"),
                control: {
                  kind: "text",
                  type: "password",
                  autoComplete: "new-password",
                  value: passwordForm.next,
                  onChange: (next) => setPasswordForm((current) => ({ ...current, next })),
                },
              },
              {
                id: "confirm",
                label: t("next.security.repeatNewPassword"),
                control: {
                  kind: "text",
                  type: "password",
                  autoComplete: "new-password",
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
        title={t("next.security.newKey")}
        note={t("next.security.newKeyNote")}
        error={keyError}
        busy={busy}
        saveLabel={t("next.security.createKey")}
        onSave={() => void submitKey()}
        onDismiss={() => setKeyOpen(false)}
        sections={[
          {
            id: "key",
            title: t("next.security.key"),
            fields: [
              {
                id: "name",
                label: t("next.security.name"),
                help: t("next.security.keyNameHelp"),
                control: {
                  kind: "text",
                  mono: false,
                  value: keyForm.name,
                  onChange: (next) => setKeyForm((current) => ({ ...current, name: next })),
                },
              },
              {
                id: "scope",
                label: t("next.security.scope"),
                help: t("next.security.scopeHelp"),
                control: {
                  kind: "select",
                  value: keyForm.scope,
                  options: KEY_SCOPES.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (next) => setKeyForm((current) => ({ ...current, scope: next })),
                },
              },
            ],
          },
        ]}
      />

      <Dialog
        open={createdKey !== null}
        title={t("next.security.keyCreated")}
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
              {copied ? t("next.security.copied") : t("next.security.copyKey")}
            </SecondaryButton>
            <PrimaryButton onClick={() => setCreatedKey(null)}>{t("next.security.done")}</PrimaryButton>
          </>
        }
      >
        <div className="flex flex-col gap-3 px-4 sm:px-6 py-5">
          <div className="text-[13px] leading-[1.55] text-wv-secondary">
            {t("next.security.copyNow")}
          </div>
          <TextField
            label={t("next.security.apiKey")}
            value={createdKey?.rawKey ?? ""}
            onChange={() => undefined}
            secret
            className="w-full"
          />
        </div>
      </Dialog>

      <ConfirmDialog
        open={disableOpen}
        title={t("next.security.turnOffTitle")}
        busy={busy}
        confirmLabel={t("next.security.turnOffLogin")}
        body={t("next.security.turnOffBody")}
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
        title={t("next.security.revokeTitle")}
        note={removeKey?.name}
        busy={busy}
        confirmLabel={t("next.security.revokeKey")}
        body={t("next.security.revokeBody", { name: removeKey?.name ?? t("next.security.thisKey") })}
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

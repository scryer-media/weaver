import { useEffect, useMemo, useState } from "react";
import { useClient, useMutation, useQuery } from "urql";
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
  PREVIEW_NETWORK_ACCESS_QUERY,
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
  UPDATE_NETWORK_ACCESS_MUTATION,
} from "@/graphql/queries";
import { authHeaders } from "@/graphql/client";
import { useTranslate } from "@/lib/context/translate-context";
import { noteLoginEnabled } from "@/lib/login-required";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { Dialog } from "../../../components/Dialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { DangerButton, PrimaryButton, SecondaryButton, TextField } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WV } from "../../../data/palette";
import { formatDate } from "../../../data/format";
import { needsPasswordCheck, usePasswordCheck } from "../../../features/PasswordCheckDialog";
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
 * Weaver runs one of two access models. An install that requires sign-in
 * limits which networks may keep a remembered session and which proxies it
 * believes; an install on the older model picks a browser access policy and
 * may run without a login. Either way the stored settings are one draft behind
 * the top bar's Save. Everything else here is an action rather than a setting,
 * so it lives behind its own button, and a key can only be read once.
 *
 * On the sign-in model the server refuses security changes until the password
 * has been checked recently; every change runs through the password check,
 * which asks only when the server refuses.
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

interface NetworkAccessStatus {
  authenticatedAccess: boolean;
  trustedNetworks: string[];
  trustedProxies: string[];
  editable: boolean;
  envPinned: boolean;
  proxiesEditable: boolean;
  proxiesEnvPinned: boolean;
  rememberedPolicyValid: boolean;
  currentClient: {
    available: boolean;
    peer: string | null;
    resolvedClient: string | null;
    forwardingHeadersIgnored: boolean;
  };
  bindAddress: BindAddressStatus;
}

interface NetworkAccessPreview {
  trustedNetworks: string[];
  trustedProxies: string[];
  restartRequired: boolean;
  currentClientAllowed: boolean | null;
}

interface NetworkAccessInput {
  trustedNetworks: string[];
  trustedProxies: string[];
  bindAddress?: string;
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

/** The entries of a one-per-line list, blank lines dropped. */
function lines(text: string): string[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

export function SecurityPanel() {
  const [{ data, error }, refetchNetwork] = useQuery<{ networkAccess: NetworkAccessStatus }>({
    query: NETWORK_ACCESS_QUERY,
    requestPolicy: "network-only",
  });
  if (error) {
    return <p role="alert" className="p-6 text-wv-error-text">{cleanMessage(error.message)}</p>;
  }
  if (!data) return null;
  return (
    <SecurityPanelBody
      network={data.networkAccess}
      refetchNetwork={() => refetchNetwork({ requestPolicy: "network-only" })}
    />
  );
}

function SecurityPanelBody({
  network,
  refetchNetwork,
}: {
  network: NetworkAccessStatus;
  refetchNetwork: () => void;
}) {
  const t = useTranslate();
  const client = useClient();
  const authenticated = network.authenticatedAccess;
  const passwordCheck = usePasswordCheck();

  const [{ data: loginData, fetching: loginFetching }, refetchLogin] = useQuery<{ adminLoginStatus: LoginStatus }>({
    query: LOGIN_STATUS_QUERY,
    requestPolicy: "network-only",
  });
  // The sign-in model reports its listener with the rest of its network state.
  const [{ data: bindData, fetching: bindFetching }, refetchBind] = useQuery<{ httpBindAddress: BindAddressStatus }>({
    query: HTTP_BIND_ADDRESS_QUERY,
    requestPolicy: "network-only",
    pause: authenticated,
  });
  const [{ data: policyData, fetching: policyFetching }, refetchPolicy] = useQuery<{ accessPolicy: AccessPolicyStatus }>({
    query: ACCESS_POLICY_QUERY,
    requestPolicy: "network-only",
    pause: authenticated,
  });
  const [{ data: keysData, fetching: keysFetching }, refetchKeys] = useQuery<{ apiKeys: ApiKey[] }>({
    query: API_KEYS_QUERY,
  });

  const [, setBindAddress] = useMutation(SET_HTTP_BIND_ADDRESS_MUTATION);
  const [, setAccessPolicy] = useMutation(SET_ACCESS_POLICY_MUTATION);
  const [, updateNetworkAccess] = useMutation(UPDATE_NETWORK_ACCESS_MUTATION);
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
  const bind = authenticated ? network.bindAddress : bindData?.httpBindAddress;
  const policy = policyData?.accessPolicy;
  const keys = keysData?.apiKeys ?? [];

  const [address, setAddress] = useState("");
  const [mode, setMode] = useState("");
  const [networks, setNetworks] = useState("");
  const [proxies, setProxies] = useState("");
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  const storedNetworks = useMemo(
    () => (authenticated ? network.trustedNetworks : (policy?.trustedNetworks ?? [])).join("\n"),
    [authenticated, network.trustedNetworks, policy],
  );
  const storedProxies = useMemo(() => network.trustedProxies.join("\n"), [network.trustedProxies]);

  // The address draft mirrors the STORED value only: an empty box means "not
  // configured", and re-proposing the running address would let Save quietly
  // undo a deliberate clear.
  useEffect(() => {
    if (!dirty && bind) {
      setAddress(bind.storedAddress ?? "");
    }
  }, [bind, dirty]);

  useEffect(() => {
    if (dirty) {
      return;
    }
    setNetworks(storedNetworks);
    setProxies(storedProxies);
    if (policy) {
      setMode(policy.mode);
    }
  }, [dirty, policy, storedNetworks, storedProxies]);

  const edit = (apply: () => void) => {
    apply();
    setDirty(true);
    setStatus(null);
    setFailed(false);
  };

  const revert = () => {
    setAddress(bind?.storedAddress ?? "");
    setMode(policy?.mode ?? "");
    setNetworks(storedNetworks);
    setProxies(storedProxies);
    setDirty(false);
    setStatus(null);
    setFailed(false);
  };

  const fail = (message: string) => {
    setStatus(message);
    setFailed(true);
    setBusy(false);
  };

  /* -------------------------------------------- sign-in model: network */

  const networkInput = (): NetworkAccessInput => {
    // Both lists always go together: that is what repairs an invalid stored policy.
    const input: NetworkAccessInput = { trustedNetworks: lines(networks), trustedProxies: lines(proxies) };
    return bind?.editable && address.trim() !== (bind.storedAddress ?? "").trim()
      ? { ...input, bindAddress: address.trim() }
      : input;
  };

  // A draft is checked as it is typed, so a malformed entry or a change that
  // would drop this browser's remembered session shows before Save.
  const [preview, setPreview] = useState<NetworkAccessPreview | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const draftKey = authenticated && dirty ? JSON.stringify(networkInput()) : null;
  useEffect(() => {
    setPreview(null);
    setPreviewError(null);
    if (draftKey === null) {
      return;
    }
    let cancelled = false;
    const timer = window.setTimeout(() => {
      void client
        .query<{ previewNetworkAccess: NetworkAccessPreview }>(PREVIEW_NETWORK_ACCESS_QUERY, {
          input: JSON.parse(draftKey) as NetworkAccessInput,
        }, { requestPolicy: "network-only" })
        .toPromise()
        .then((result) => {
          if (cancelled) return;
          if (result.error) {
            setPreviewError(cleanMessage(result.error.message));
          } else {
            setPreview(result.data?.previewNetworkAccess ?? null);
          }
        });
    }, 350);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [client, draftKey]);

  const [lockoutOpen, setLockoutOpen] = useState(false);

  const commitNetwork = async (): Promise<"done" | "password"> => {
    setBusy(true);
    setFailed(false);
    const result = await updateNetworkAccess({ input: networkInput() });
    if (needsPasswordCheck(result.error)) {
      setBusy(false);
      return "password";
    }
    if (result.error) {
      fail(cleanMessage(result.error.message));
      return "done";
    }
    setBusy(false);
    setDirty(false);
    setStatus(t("next.security.networkSaved"));
    refetchNetwork();
    return "done";
  };

  const saveNetwork = async () => {
    setBusy(true);
    setFailed(false);
    const result = await client
      .query<{ previewNetworkAccess: NetworkAccessPreview }>(PREVIEW_NETWORK_ACCESS_QUERY, {
        input: networkInput(),
      }, { requestPolicy: "network-only" })
      .toPromise();
    if (result.error) {
      fail(cleanMessage(result.error.message));
      return;
    }
    setBusy(false);
    if (result.data?.previewNetworkAccess.currentClientAllowed === false) {
      setLockoutOpen(true);
      return;
    }
    await passwordCheck.run(commitNetwork);
  };

  /* ----------------------------------------- older model: bind + policy */

  const saveLegacy = async (): Promise<"done" | "password"> => {
    setBusy(true);
    setFailed(false);
    const messages: string[] = [];
    if (bind?.editable && address.trim() !== (bind.storedAddress ?? "")) {
      const result = await setBindAddress({ address: address.trim() });
      if (needsPasswordCheck(result.error)) {
        setBusy(false);
        return "password";
      }
      if (result.error) {
        fail(cleanMessage(result.error.message));
        return "done";
      }
      messages.push(address.trim() ? t("next.security.listenSaved") : t("next.security.listenCleared"));
      void refetchBind({ requestPolicy: "network-only" });
    }
    if (policy && !policy.envPinned && (mode !== policy.mode || networks !== storedNetworks)) {
      const result = await setAccessPolicy({
        mode,
        trustedNetworks: mode === "login_except_local" ? lines(networks) : undefined,
      });
      if (needsPasswordCheck(result.error)) {
        setBusy(false);
        return "password";
      }
      if (result.error) {
        fail(cleanMessage(result.error.message));
        return "done";
      }
      messages.push(t("next.security.policyApplied"));
      void refetchPolicy({ requestPolicy: "network-only" });
    }
    setBusy(false);
    setDirty(false);
    setStatus(messages.join(" · ") || t("next.settings.saved"));
    return "done";
  };

  usePanelState({
    dirty,
    busy,
    status,
    failed,
    revert,
    save: () => {
      void (authenticated ? saveNetwork() : passwordCheck.run(saveLegacy));
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
  const [signOutAllOpen, setSignOutAllOpen] = useState(false);
  const [signOutAllError, setSignOutAllError] = useState<string | null>(null);

  const [keyOpen, setKeyOpen] = useState(false);
  const [keyForm, setKeyForm] = useState({ name: "", scope: "CONTROL" });
  const [keyError, setKeyError] = useState<string | null>(null);
  const [createdKey, setCreatedKey] = useState<{ name: string; rawKey: string } | null>(null);
  const [copied, setCopied] = useState(false);
  const [removeKey, setRemoveKey] = useState<ApiKey | null>(null);

  const submitEnable = async (): Promise<"done" | "password"> => {
    if (enableForm.password !== enableForm.confirm) {
      setEnableError(t("next.security.passwordsMismatch"));
      return "done";
    }
    if (!enableForm.username.trim() || !enableForm.password) {
      setEnableError(t("next.security.credentialsRequired"));
      return "done";
    }
    setBusy(true);
    const result = await enableLogin({
      username: enableForm.username.trim(),
      password: enableForm.password,
    });
    setBusy(false);
    if (needsPasswordCheck(result.error)) return "password";
    if (result.error) {
      setEnableError(cleanMessage(result.error.message));
      return "done";
    }
    setEnableOpen(false);
    setEnableForm({ username: "", password: "", confirm: "" });
    void refetchLogin({ requestPolicy: "network-only" });
    return "done";
  };

  const submitPassword = async (): Promise<"done" | "password"> => {
    if (passwordForm.next !== passwordForm.confirm) {
      setPasswordError(t("next.security.passwordsMismatch"));
      return "done";
    }
    setBusy(true);
    const result = await changePassword({
      currentPassword: passwordForm.current,
      newPassword: passwordForm.next,
    });
    setBusy(false);
    if (needsPasswordCheck(result.error)) return "password";
    if (result.error) {
      setPasswordError(cleanMessage(result.error.message));
      return "done";
    }
    setPasswordOpen(false);
    setPasswordForm({ current: "", next: "", confirm: "" });
    return "done";
  };

  const submitKey = async (): Promise<"done" | "password"> => {
    if (!keyForm.name.trim()) {
      setKeyError(t("next.security.keyNameRequired"));
      return "done";
    }
    setBusy(true);
    const result = await createApiKey({ name: keyForm.name.trim(), scope: keyForm.scope });
    setBusy(false);
    if (needsPasswordCheck(result.error)) return "password";
    if (result.error || !result.data?.createApiKey?.rawKey) {
      setKeyError(result.error ? cleanMessage(result.error.message) : t("next.security.keyCreateFailed"));
      return "done";
    }
    setKeyOpen(false);
    setCreatedKey({ name: keyForm.name.trim(), rawKey: result.data.createApiKey.rawKey });
    setCopied(false);
    setKeyForm({ name: "", scope: "CONTROL" });
    void refetchKeys({ requestPolicy: "network-only" });
    return "done";
  };

  const submitRemoveKey = async (key: ApiKey): Promise<"done" | "password"> => {
    setBusy(true);
    const result = await deleteApiKey({ id: key.id });
    setBusy(false);
    if (needsPasswordCheck(result.error)) return "password";
    setRemoveKey(null);
    void refetchKeys({ requestPolicy: "network-only" });
    return "done";
  };

  const submitDisable = async (): Promise<"done" | "password"> => {
    setBusy(true);
    const result = await disableLogin({});
    setBusy(false);
    if (needsPasswordCheck(result.error)) return "password";
    setDisableOpen(false);
    void refetchLogin({ requestPolicy: "network-only" });
    return "done";
  };

  const submitSignOutAll = async (): Promise<"done" | "password"> => {
    setBusy(true);
    setSignOutAllError(null);
    try {
      const response = await fetch(new URL("api/auth/signout-all", document.baseURI), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
      });
      if (response.status === 428) return "password";
      if (!response.ok) {
        setSignOutAllError(t("next.security.signOutEverywhereFailed"));
        return "done";
      }
    } catch {
      setSignOutAllError(t("next.security.signOutEverywhereFailed"));
      return "done";
    } finally {
      setBusy(false);
    }
    // This browser's session went with the rest; the entry page asks it to sign in.
    window.location.assign(new URL(".", document.baseURI));
    return "done";
  };

  /* -------------------------------------------------------------- blocks */

  const listenField: FieldSpec = {
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
  };

  const legacyAccessFields: FieldSpec[] = [
    listenField,
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

  const draftSummary = (() => {
    if (previewError) {
      return <span className="text-wv-error-text [word-break:normal] break-words">{previewError}</span>;
    }
    if (!preview) {
      return <span className="text-wv-muted">{t("next.security.checking")}</span>;
    }
    return (
      // Sentences, not an address: wrap between words.
      <span className="flex flex-col gap-1 [word-break:normal] break-words">
        <span>
          {t("next.security.draftResolves", {
            networks: preview.trustedNetworks.join(", ") || t("next.security.anyClient"),
            proxies: preview.trustedProxies.join(", ") || t("next.job.none"),
          })}
        </span>
        {preview.restartRequired ? (
          <span className="text-wv-muted">{t("next.security.draftRestart")}</span>
        ) : null}
        {preview.currentClientAllowed === false ? (
          <span className="text-wv-warn">{t("next.security.draftLocksOut")}</span>
        ) : null}
      </span>
    );
  })();

  const connection = network.currentClient;
  const authenticatedAccessFields: FieldSpec[] = [
    listenField,
    // Only while the running listener and the saved one disagree.
    ...(bind?.restartRequired
      ? [
          {
            id: "listeningNow",
            label: t("next.security.listeningNow"),
            help: t("next.security.listenPending"),
            keywords: "bind listener running restart",
            control: { kind: "static" as const, value: bind.address },
          },
        ]
      : []),
    {
      id: "rememberedNetworks",
      label: t("next.security.rememberedNetworks"),
      help: network.editable
        ? t("next.security.rememberedNetworksHelp")
        : t("next.security.accessPinnedHelp", { variable: "WEAVER_TRUSTED_CIDRS" }),
      keywords: "cidr subnet lan remember me session trusted networks",
      control: network.editable
        ? {
            kind: "textarea",
            value: networks,
            rows: 4,
            placeholder: "192.168.1.0/24",
            onChange: (next) => edit(() => setNetworks(next)),
          }
        : { kind: "static", value: network.trustedNetworks.join(", ") || t("next.security.anyClient") },
    },
    {
      id: "trustedProxies",
      label: t("next.security.trustedProxies"),
      help: network.proxiesEditable
        ? t("next.security.trustedProxiesHelp")
        : t("next.security.accessPinnedHelp", { variable: "WEAVER_TRUSTED_PROXIES" }),
      keywords: "reverse proxy forwarded x-forwarded-for headers",
      control: network.proxiesEditable
        ? {
            kind: "textarea",
            value: proxies,
            rows: 3,
            placeholder: "172.18.0.2",
            onChange: (next) => edit(() => setProxies(next)),
          }
        : { kind: "static", value: network.trustedProxies.join(", ") || t("next.job.none") },
    },
    ...(dirty
      ? [
          {
            id: "draft",
            label: t("next.security.draft"),
            help: t("next.security.draftHelp"),
            control: { kind: "static" as const, value: draftSummary },
          },
        ]
      : []),
    ...(connection.available
      ? [
          {
            id: "thisBrowser",
            label: t("next.security.thisBrowser"),
            help: connection.forwardingHeadersIgnored
              ? `${t("next.security.connectionHelp")} ${t("next.security.headersIgnored")}`
              : t("next.security.connectionHelp"),
            keywords: "peer client address ip",
            control: {
              kind: "static" as const,
              value: `${connection.peer ?? t("next.security.unknown")} → ${connection.resolvedClient ?? t("next.security.unknown")}`,
            },
          },
        ]
      : []),
  ];

  const scopeLabel = (scope: string) => {
    const entry = KEY_SCOPES.find((option) => option.value === scope);
    return entry ? t(entry.label) : scope;
  };

  const signInField: FieldSpec = authenticated
    ? {
        id: "password",
        label: t("next.security.password"),
        help: t("next.security.passwordHelp"),
        keywords: "password admin authentication sessions sign out",
        control: {
          kind: "custom",
          control: (
            <>
              <SecondaryButton icon="password" onClick={() => setPasswordOpen(true)}>
                {t("next.security.changePassword")}
              </SecondaryButton>
              <DangerButton
                icon="signOut"
                onClick={() => {
                  setSignOutAllError(null);
                  setSignOutAllOpen(true);
                }}
              >
                {t("next.security.signOutEverywhere")}
              </DangerButton>
            </>
          ),
        },
      }
    : {
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
      };

  const blocks: SettingsBlock[] = [
    {
      kind: "section",
      id: "sign-in",
      title: t("next.security.signIn"),
      note: login?.enabled
        ? t("next.security.signedInAs", { name: login.username ?? "admin" })
        : t("next.security.notConfigured"),
      fields: [signInField],
    },
    {
      kind: "section",
      id: "access",
      title: t("next.security.networkAccess"),
      fields: authenticated ? authenticatedAccessFields : legacyAccessFields,
    },
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

  const banner = (text: string, color: string, tone: string) => (
    <div
      className={`flex flex-none items-center gap-[9px] border-b border-wv-hairline bg-wv-cell-hover px-4 py-3 text-[12.5px] sm:px-6 ${tone}`}
    >
      <Square color={color} />
      {text}
    </div>
  );

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => setKeyOpen(true)}>{t("next.security.addKey")}</PrimaryButton>
      </PanelControls>

      {authenticated && !network.rememberedPolicyValid
        ? banner(t("next.security.policyInvalid"), WV.error, "text-wv-error-text")
        : null}
      {bind?.bindFallback ? banner(bind.bindFallback, WV.warn, "text-wv-warn") : null}
      {!authenticated && bind?.exposedWithoutLogin && !login?.enabled
        ? banner(t("next.security.exposedWarning"), WV.warn, "text-wv-warn")
        : null}

      <SettingsBlocks
        blocks={blocks}
        loading={
          (loginFetching && !loginData)
          || (!authenticated && bindFetching && !bindData)
          || (!authenticated && policyFetching && !policyData)
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
        onSave={() => void passwordCheck.run(submitEnable)}
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
                  secret: true,
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
                  secret: true,
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
                  secret: true,
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
        note={authenticated ? t("next.security.changePasswordNote") : undefined}
        error={passwordError}
        busy={busy}
        saveLabel={t("next.security.changePassword")}
        onSave={() => void passwordCheck.run(submitPassword)}
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
                  secret: true,
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
                  secret: true,
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
                  secret: true,
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
        onSave={() => void passwordCheck.run(submitKey)}
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
        onConfirm={() => void passwordCheck.run(submitDisable)}
        onDismiss={() => setDisableOpen(false)}
      />

      <ConfirmDialog
        open={signOutAllOpen}
        title={t("next.security.signOutEverywhereTitle")}
        busy={busy}
        destructive
        confirmLabel={t("next.security.signOutEverywhere")}
        body={
          signOutAllError ? (
            <>
              {t("next.security.signOutEverywhereBody")}
              <span role="alert" className="mt-2 block text-wv-error-text">{signOutAllError}</span>
            </>
          ) : (
            t("next.security.signOutEverywhereBody")
          )
        }
        onConfirm={() => void passwordCheck.run(submitSignOutAll)}
        onDismiss={() => setSignOutAllOpen(false)}
      />

      <ConfirmDialog
        open={lockoutOpen}
        title={t("next.security.lockoutTitle")}
        busy={busy}
        confirmLabel={t("next.security.lockoutConfirm")}
        body={t("next.security.lockoutBody")}
        onConfirm={() => {
          setLockoutOpen(false);
          void passwordCheck.run(commitNetwork);
        }}
        onDismiss={() => setLockoutOpen(false)}
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
            void passwordCheck.run(() => submitRemoveKey(removeKey));
          }
        }}
        onDismiss={() => setRemoveKey(null)}
      />

      {passwordCheck.dialog}
    </>
  );
}

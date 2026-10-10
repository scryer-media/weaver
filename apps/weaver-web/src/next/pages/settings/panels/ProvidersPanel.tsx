import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { useClient, useMutation, useQuery } from "urql";
import {
  ADD_SERVER_MUTATION,
  REMOVE_SERVER_MUTATION,
  RESET_SERVER_DOWNLOAD_QUOTA_USAGE_MUTATION,
  SERVERS_QUERY,
  SERVER_QUERY,
  TEST_CONNECTION_MUTATION,
  UPDATE_SERVER_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { LoadingMark } from "@/lib/loading-mark";
import type { DownloadQuota } from "@/lib/networking";
import { blockedRouting, directRouting, routeFields, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";
import { BetaTag, Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { Icon } from "../../../components/icons";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { RouteView } from "../../../features/networking/RouteView";
import { PrimaryButton, SecondaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WorkingOverlay } from "../../../components/WorkingOverlay";
import { WV } from "../../../data/palette";
import { formatHostnames, formatLatency } from "../../../data/format";
import { PanelControls, SettingsBlocks, useSettingsPanelActive, type SettingsBlock } from "../framework";
import { quotaDraft, quotaFields, quotaInput, trimNumber, type QuotaDraft } from "../quota";

/**
 * Providers: the news servers weaver downloads from.
 *
 * The table is the panel; everything editable about a server lives in the
 * record editor behind a row, which is the only way a form this wide fits the
 * design's row rhythm. Toggling a server on or off writes immediately, so the
 * top bar's Save stays out of it.
 */

const MIB = 1024 * 1024;

interface ServerQuota extends DownloadQuota {
  usedBytes: number;
  remainingBytes: number;
  blocked: boolean;
}

interface Server {
  id: number;
  host: string;
  port: number;
  tls: boolean;
  connections: number;
  active: boolean;
  supportsPipelining: boolean;
  priority: number;
  backfill: boolean;
  retentionDays: number;
  maxDownloadSpeed: number;
  routing: RoutingPolicy | null;
  routingStatus?: RoutingStatus;
  downloadQuota: ServerQuota;
  tlsCipherSuite?: string | null;
  tlsNameMismatchCertificateFingerprint?: string | null;
}

interface ServerDetails extends Server {
  username: string | null;
  tlsNameMismatchCertificateDerBase64: string | null;
}

interface TestResult {
  success: boolean;
  message: string;
  latencyMs: number | null;
  supportsPipelining: boolean;
  tlsCipherSuite: string | null;
  adoptableTlsNameMismatchCertificate: {
    derBase64: string;
    sha256Fingerprint: string;
    names: string[];
  } | null;
}

interface ServerForm {
  host: string;
  port: number;
  tls: boolean;
  username: string;
  password: string;
  connections: number;
  active: boolean;
  priority: number;
  backfill: boolean;
  retentionDays: number;
  speedUnlimited: boolean;
  speedMib: string;
  quota: QuotaDraft;
  /** What the quota's current window has spent. */
  quotaUsedBytes: number;
  /** The server's route as stored. It is set under Networking; here it only decides how a test leaves. */
  routing: RoutingPolicy;
  /**
   * A new server only: save it switched off behind a route nothing can take,
   * untested, so it cannot dial before its route is set under Networking.
   */
  killSwitch: boolean;
  certificateDerBase64: string | null;
  certificateFingerprint: string | null;
}

/**
 * What a save says when the provider's certificate names another host. Saving
 * probes the connection, and the probe's message is fixed English text, so the
 * editor recognises it and runs a test, whose result carries the certificate
 * to trust.
 */
const CERTIFICATE_NAME_MISMATCH = "certificate belongs to a different hostname";

const NEW_SERVER: ServerForm = {
  host: "",
  port: 563,
  tls: true,
  username: "",
  password: "",
  connections: 20,
  active: true,
  priority: 0,
  backfill: false,
  retentionDays: 0,
  speedUnlimited: true,
  speedMib: "10",
  quota: quotaDraft(null),
  quotaUsedBytes: 0,
  routing: directRouting,
  killSwitch: false,
  certificateDerBase64: null,
  certificateFingerprint: null,
};

/** `nntps://news.example:563/` and `news.example` both mean the same host. */
function normalizeHost(host: string): string {
  const trimmed = host.trim();
  const match = /^(?:https?|nntps?):\/\/(.*)$/i.exec(trimmed);
  return match ? match[1].replace(/\/+$/, "") : trimmed;
}

/** `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256` → `AES-128-GCM`. */
function shortCipher(suite: string): string {
  return suite
    .replace(/^TLS_(ECDHE_(RSA|ECDSA)_WITH_)?/, "")
    .replace(/_SHA(256|384)$/, "")
    .replace(/_/g, "-");
}

function transportLabel(t: Translate, server: Server): string {
  if (!server.tls) {
    return t("next.providers.plain");
  }
  return server.tlsCipherSuite ? `TLS · ${shortCipher(server.tlsCipherSuite)}` : "TLS";
}

function roleLabel(t: Translate, server: Server): string {
  if (server.backfill) {
    return t("next.providers.blockAccount");
  }
  return server.priority === 0
    ? t("next.providers.primary")
    : server.priority === 1
      ? t("next.providers.secondary")
      : t("next.providers.priorityN", { priority: server.priority });
}

function formToState(server: ServerDetails | Server, username: string): ServerForm {
  return {
    host: server.host,
    port: server.port,
    tls: server.tls,
    username,
    password: "",
    connections: server.connections,
    active: server.active,
    priority: server.priority,
    backfill: server.backfill,
    retentionDays: server.retentionDays,
    speedUnlimited: server.maxDownloadSpeed === 0,
    speedMib: server.maxDownloadSpeed === 0 ? "10" : trimNumber(server.maxDownloadSpeed / MIB),
    quota: quotaDraft(server.downloadQuota),
    quotaUsedBytes: server.downloadQuota?.usedBytes ?? 0,
    routing: server.routing ?? directRouting,
    killSwitch: false,
    certificateDerBase64:
      "tlsNameMismatchCertificateDerBase64" in server
        ? server.tlsNameMismatchCertificateDerBase64
        : null,
    certificateFingerprint: server.tlsNameMismatchCertificateFingerprint ?? null,
  };
}

/** A server as saved. Its route is left out: saving a server keeps the route it has. */
function serverInput(form: ServerForm) {
  return {
    host: normalizeHost(form.host),
    port: form.port,
    tls: form.tls,
    username: form.username.trim() || null,
    password: form.password.trim() || null,
    connections: form.connections,
    active: form.active,
    priority: form.priority,
    backfill: form.backfill,
    retentionDays: form.retentionDays,
    tlsNameMismatchCertificateDerBase64: form.certificateDerBase64,
    maxDownloadSpeed: form.speedUnlimited ? 0 : Math.round(Number(form.speedMib || 0) * MIB),
    downloadQuota: quotaInput(form.quota),
  };
}

export function ProvidersPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ servers: Server[] }>({ query: SERVERS_QUERY });
  const [, addServer] = useMutation(ADD_SERVER_MUTATION);
  const [, updateServer] = useMutation(UPDATE_SERVER_MUTATION);
  const [, removeServer] = useMutation(REMOVE_SERVER_MUTATION);
  const [, resetQuota] = useMutation(RESET_SERVER_DOWNLOAD_QUOTA_USAGE_MUTATION);
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);
  const client = useClient();

  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const editorSession = useRef(0);
  const [form, setForm] = useState<ServerForm | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [testing, setTesting] = useState(false);
  const [submittedTestResult, setTestResult] = useState<(TestResult & { values: ServerForm }) | null>(null);
  const testResult = submittedTestResult && JSON.stringify(submittedTestResult.values) === JSON.stringify(form)
    ? submittedTestResult
    : null;
  const [confirmRemove, setConfirmRemove] = useState<Server | null>(null);
  const [confirmTrust, setConfirmTrust] = useState<{ derBase64: string; fingerprint: string; names: string[] } | null>(null);

  // Only the single-server query carries the stored username; the list does not.
  const [{ data: detailsData }] = useQuery<{ server: ServerDetails | null }>({
    query: SERVER_QUERY,
    variables: { id: typeof editingId === "number" ? editingId : 0 },
    pause: typeof editingId !== "number",
  });

  const servers = useMemo(
    () =>
      [...(data?.servers ?? [])].sort(
        (left, right) => left.priority - right.priority || left.host.localeCompare(right.host),
      ),
    [data?.servers],
  );

  // urql retains the previous result while a different provider is loading.
  const details = typeof editingId === "number" && detailsData?.server?.id === editingId
    ? detailsData.server
    : null;
  const editing = typeof editingId === "number" ? servers.find((s) => s.id === editingId) : null;

  // `updateServer` keeps a password it is not given but clears a username it is
  // not given, so no edit may be sent from the list's copy of a row: the form
  // is only seeded once the single-server query has returned the stored name.
  useEffect(() => {
    if (details && form === null) {
      setForm(formToState(details, details.username ?? ""));
    }
  }, [details, form]);

  // A link can ask for the add form outright, as the rail's call to action does
  // when nothing is configured. Open it once, then take the ask out of the URL
  // so neither a reload nor Back opens it again.
  const [searchParams, setSearchParams] = useSearchParams();
  const panelActive = useSettingsPanelActive();
  // Only the open panel answers; a search mounts this one beside it.
  const askedToAdd = searchParams.has("add") && panelActive;
  useEffect(() => {
    if (!askedToAdd) {
      return;
    }
    editorSession.current += 1;
    setTesting(false);
    setBusy(false);
    setConfirmTrust(null);
    setForm(NEW_SERVER);
    setTestResult(null);
    setEditingId("new");
    setSearchParams(
      (params) => {
        params.delete("add");
        return params;
      },
      { replace: true },
    );
  }, [askedToAdd, setSearchParams]);

  const values = form;

  const patch = (next: Partial<ServerForm>) => {
    if (values) {
      setConfirmTrust(null);
      setForm({ ...values, ...next });
    }
  };

  const closeEditor = () => {
    editorSession.current += 1;
    setTesting(false);
    setBusy(false);
    setConfirmTrust(null);
    setConfirmRemove(null);
    setEditingId(null);
    setForm(null);
    setError(null);
    setTestResult(null);
  };

  const setActive = async (server: Server, active: boolean) => {
    // The update replaces the whole record, so it is built from the stored
    // details: the list omits the username and the adopted TLS certificate,
    // and sending the list row would clear both.
    const stored = await client
      .query<{ server: ServerDetails | null }>(
        SERVER_QUERY,
        { id: server.id },
        { requestPolicy: "network-only" },
      )
      .toPromise();
    const details = stored.data?.server;
    if (!details) {
      void reexecute({ requestPolicy: "network-only" });
      return;
    }
    await updateServer({
      id: server.id,
      input: { ...serverInput(formToState(details, details.username ?? "")), active },
    });
    void reexecute({ requestPolicy: "network-only" });
  };

  const save = async () => {
    if (!values) {
      return;
    }
    if (!normalizeHost(values.host)) {
      setError(t("next.providers.hostRequired"));
      return;
    }
    setBusy(true);
    setError(null);
    const input = serverInput(values);
    const session = editorSession.current;
    const result =
      editingId === "new"
        ? // Behind a kill switch there is no way out to test, so the server is kept off.
          await addServer({ input: values.killSwitch ? { ...input, active: false, routing: blockedRouting } : input })
        : await updateServer({ id: editingId, input });
    if (session !== editorSession.current) {
      void reexecute({ requestPolicy: "network-only" });
      return;
    }
    setBusy(false);
    if (result.error) {
      const message = result.error.graphQLErrors[0]?.message ?? result.error.message;
      if (values.tls && !values.certificateDerBase64 && message.includes(CERTIFICATE_NAME_MISMATCH)) {
        // Show the certificate the way a test does, so it can be trusted here.
        await runTest();
        return;
      }
      setError(message);
      return;
    }
    void reexecute({ requestPolicy: "network-only" });
    closeEditor();
  };

  const runTest = async (provider: ServerForm | null = values) => {
    if (!provider) {
      return;
    }
    setTesting(true);
    setTestResult(null);
    setError(null);
    const session = editorSession.current;
    // A test saves nothing, so it is told the route to leave by.
    const result = await testConnection({ input: { ...serverInput(provider), ...routeFields(provider.routing) } });
    if (session !== editorSession.current) return;
    setTesting(false);
    setTestResult(result.data?.testConnection ? { ...(result.data.testConnection as TestResult), values: provider } : null);
  };

  const trust = () => {
    if (!values || !confirmTrust || !testResult) {
      return;
    }
    const next = {
      ...values,
      certificateDerBase64: confirmTrust.derBase64,
      certificateFingerprint: confirmTrust.fingerprint,
    };
    setConfirmTrust(null);
    setForm(next);
    // Test again with the certificate, so the result shows whether it connects now.
    void runTest(next);
  };

  const adoptable = values?.certificateDerBase64 ? null : (testResult?.adoptableTlsNameMismatchCertificate ?? null);

  // A trusted certificate belongs to one host and port over TLS.
  const forgetCertificate = { certificateDerBase64: null, certificateFingerprint: null };

  const remove = async () => {
    if (!confirmRemove) {
      return;
    }
    const session = editorSession.current;
    setBusy(true);
    await removeServer({ id: confirmRemove.id });
    if (session !== editorSession.current) {
      void reexecute({ requestPolicy: "network-only" });
      return;
    }
    setBusy(false);
    setConfirmRemove(null);
    closeEditor();
    void reexecute({ requestPolicy: "network-only" });
  };

  const addProvider = () => {
    closeEditor();
    setForm(NEW_SERVER);
    setTestResult(null);
    setEditingId("new");
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "servers",
      title: t("next.providers.servers"),
      note: t("next.settings.panel.serversNote"),
      columns: "minmax(0, 1fr) 132px 190px 150px 44px",
      headers: [
        t("next.providers.host"),
        t("next.providers.connections"),
        t("next.providers.transport"),
        t("next.providers.role"),
        "",
      ],
      empty: t("next.providers.empty"),
      emptyAction: { label: t("next.providers.add"), onClick: addProvider },
      onRowClick: (id) => {
        closeEditor();
        setForm(null);
        setTestResult(null);
        setEditingId(Number(id));
      },
      rows: servers.map((server) => ({
        id: String(server.id),
        searchText: `${server.host} ${server.port} ${roleLabel(t, server)} ${transportLabel(t, server)}`,
        cells: [
          <span key="host" className="flex min-w-0 items-center gap-[10px]">
            <Square color={server.active ? WV.accent : WV.inert} />
            <span className="min-w-0 truncate font-wv-mono text-[12.5px]">{server.host}</span>
            <span className="flex-none font-wv-mono text-[11px] text-wv-faint">:{server.port}</span>
          </span>,
          <Cell key="connections" mono>
            {server.connections}
          </Cell>,
          <Cell key="transport" mono className="text-wv-secondary">
            {transportLabel(t, server)}
          </Cell>,
          <Cell key="role">{roleLabel(t, server)}</Cell>,
          <span key="active" onClick={(event) => event.stopPropagation()}>
            <Toggle
              size="table"
              checked={server.active}
              onChange={(next) => void setActive(server, next)}
              label={t("next.providers.enabledAria", { host: server.host })}
            />
          </span>,
        ],
      })),
    },
  ];

  const sections: EditorSection[] = values
    ? [
        {
          id: "connection",
          title: t("next.providers.connection"),
          fields: [
            {
              id: "host",
              label: t("next.providers.host"),
              help: t("next.providers.hostHelp"),
              control: {
                kind: "text",
                value: values.host,
                placeholder: "news.example.com",
                onChange: (next) => patch({ host: next, ...forgetCertificate }),
              },
            },
            {
              id: "port",
              label: t("next.providers.port"),
              control: {
                kind: "number",
                value: values.port,
                min: 1,
                max: 65535,
                onChange: (next) => patch({ port: next, ...forgetCertificate }),
              },
            },
            {
              id: "tls",
              label: "TLS",
              help: t("next.providers.tlsHelp"),
              control: {
                kind: "toggle",
                value: values.tls,
                onChange: (next) => patch({ tls: next, port: next ? 563 : 119, ...forgetCertificate }),
              },
            },
            {
              id: "username",
              label: t("next.providers.username"),
              control: {
                kind: "text",
                secret: true,
                value: values.username,
                onChange: (next) => patch({ username: next }),
              },
            },
            {
              id: "password",
              label: t("next.providers.password"),
              help: editingId === "new" ? undefined : t("next.providers.passwordKeep"),
              control: {
                kind: "text",
                type: "password",
                value: values.password,
                placeholder: editingId === "new" ? "" : "••••••••",
                onChange: (next) => patch({ password: next }),
              },
            },
            {
              id: "connections",
              label: t("next.providers.connections"),
              control: {
                kind: "number",
                value: values.connections,
                min: 1,
                max: 200,
                onChange: (next) => patch({ connections: next }),
              },
            },
          ],
        },
        {
          id: "role",
          title: t("next.providers.role"),
          fields: [
            {
              id: "active",
              label: t("next.providers.enabled"),
              help: t("next.providers.enabledHelp"),
              control: {
                kind: "toggle",
                value: values.active && !values.killSwitch,
                disabled: values.killSwitch,
                onChange: (next) => patch({ active: next }),
              },
            },
            {
              id: "priority",
              label: t("next.providers.priority"),
              help: t("next.providers.priorityHelp"),
              control: {
                kind: "number",
                value: values.priority,
                min: 0,
                max: 99,
                onChange: (next) => patch({ priority: next }),
              },
            },
            {
              id: "backfill",
              label: t("next.providers.blockAccount"),
              help: t("next.providers.blockAccountHelp"),
              control: {
                kind: "toggle",
                value: values.backfill,
                onChange: (next) => patch({ backfill: next }),
              },
            },
            {
              id: "retentionDays",
              label: t("next.providers.retention"),
              help: t("next.providers.retentionHelp"),
              control: {
                kind: "number",
                value: values.retentionDays,
                min: 0,
                onChange: (next) => patch({ retentionDays: next }),
                suffix: t("next.providers.days"),
              },
            },
          ],
        },
        {
          id: "limits",
          title: t("next.bandwidth.limits"),
          fields: [
            {
              id: "speedUnlimited",
              label: t("next.providers.unlimitedSpeed"),
              help: t("next.providers.unlimitedSpeedHelp"),
              control: {
                kind: "toggle",
                value: values.speedUnlimited,
                onChange: (next) => patch({ speedUnlimited: next }),
              },
            },
            ...(values.speedUnlimited
              ? []
              : [
                  {
                    id: "speedMib",
                    label: t("next.providers.speedCeiling"),
                    control: {
                      kind: "text" as const,
                      value: values.speedMib,
                      onChange: (next: string) => patch({ speedMib: next }),
                      className: "w-[110px]",
                    },
                    help: t("next.schedules.speedLimitHelp"),
                  },
                ]),
            ...quotaFields(t, values.quota, (quota) => patch({ quota }), {
              label: t("next.providers.quota"),
              help: t("next.providers.quotaHelp"),
              usedBytes: values.quotaUsedBytes,
            }),
          ],
        },
        {
          id: "routing",
          title: t("next.providers.networkRoute"),
          tag: <BetaTag />,
          fields:
            editingId === "new"
              ? [
                  {
                    id: "killSwitch",
                    label: t("next.networking.killSwitch"),
                    help: t("next.providers.killSwitchHelp"),
                    control: {
                      kind: "toggle",
                      value: values.killSwitch,
                      onChange: (next) => patch({ killSwitch: next }),
                    },
                  },
                ]
              : [],
          body: (
            <RouteView
              consumer={typeof editingId === "number" ? `server:${editingId}` : undefined}
              killSwitch={values.killSwitch}
            />
          ),
        },
      ]
    : [];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={addProvider}>
          {t("next.providers.add")}
        </PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? t("next.providers.add") : (editing?.host ?? t("next.providers.provider"))}
        note={
          editingId === "new"
            ? t("next.providers.newNote")
            : t("next.providers.priorityNote", { priority: values?.priority ?? 0 })
        }
        width={620}
        overlay={
          testing ? (
            <WorkingOverlay label={t("next.providers.testing")} />
          ) : busy && confirmRemove === null ? (
            // Saving tests the connection too, so it is as slow as a test.
            <WorkingOverlay label={t("next.providers.saving")} />
          ) : undefined
        }
        sections={sections}
        error={error}
        busy={busy}
        saveDisabled={values === null}
        onSave={() => void save()}
        onDismiss={closeEditor}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel={t("next.providers.remove")}
        extraActions={
          <>
            {editing && values?.quota.quota.enabled ? (
              <SecondaryButton
                icon="reset"
                onClick={() => {
                  void resetQuota({ id: editing.id }).then(() =>
                    reexecute({ requestPolicy: "network-only" }),
                  );
                }}
              >
                {t("next.providers.resetUsage")}
              </SecondaryButton>
            ) : null}
            <SecondaryButton icon="test" onClick={() => void runTest()} disabled={testing || values?.killSwitch}>
              {testing ? t("next.providers.testing") : t("next.providers.test")}
            </SecondaryButton>
          </>
        }
      >
        {values === null ? (
          <div role="status" className="flex items-center gap-3 px-4 sm:px-6 py-5 font-wv-mono text-[12px] text-wv-muted">
            <LoadingMark className="h-5" />
            {t("next.providers.loading")}
          </div>
        ) : null}
        {testResult ? (
          <div className="flex flex-none flex-col gap-1.5 border-t border-wv-hairline px-4 sm:px-6 py-4">
            {adoptable ? (
              // A certificate on offer is the whole story: say whose certificate
              // it is instead of repeating the server's failure above it.
              <div className="flex items-start gap-[9px] text-[13px] leading-[1.45]">
                <Square color={WV.warn} className="mt-[6px] flex-none" />
                <span className="text-wv-warn">
                  {adoptable.names.length
                    ? t("next.providers.certIssuedFor", {
                        names: formatHostnames(adoptable.names),
                        host: values?.host.trim() ?? "",
                      })
                    : t("next.providers.certMismatch")}
                </span>
              </div>
            ) : (
              <div className="flex items-center gap-[9px] text-[13px]">
                <Square color={testResult.success ? WV.green : WV.error} />
                <span className={testResult.success ? "text-wv-fg" : "text-wv-error-text"}>
                  {testResult.message}
                </span>
              </div>
            )}
            {testResult.latencyMs === null ? null : (
              <div className="font-wv-mono text-[11.5px] text-wv-muted">
                {formatLatency(testResult.latencyMs)}
                {testResult.supportsPipelining ? ` · ${t("next.providers.pipelining")}` : ""}
                {testResult.tlsCipherSuite ? ` · ${shortCipher(testResult.tlsCipherSuite)}` : ""}
              </div>
            )}
            {adoptable ? (
              <div className="flex flex-col gap-2 pt-1.5">
                <span className="font-wv-mono text-[11px] break-all text-wv-muted">
                  {t("next.providers.certFingerprint", { fingerprint: adoptable.sha256Fingerprint })}
                </span>
                <SecondaryButton
                  icon="trust"
                  className="self-start"
                  onClick={() =>
                    setConfirmTrust({
                      derBase64: adoptable.derBase64,
                      fingerprint: adoptable.sha256Fingerprint,
                      names: adoptable.names,
                    })
                  }
                >
                  {t("next.providers.trustCert")}
                </SecondaryButton>
              </div>
            ) : null}
          </div>
        ) : null}
        {values?.certificateDerBase64 ? (
          <div className="flex flex-none flex-col gap-1.5 border-t border-wv-hairline px-4 py-4 text-[12px] text-wv-muted sm:px-6">
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
              <span className="flex items-center gap-2">
                <Icon name="trust" size={13} className="flex-none" />
                {t("next.providers.certificateTrusted")}
              </span>
              <button
                type="button"
                onClick={() => patch(forgetCertificate)}
                className="cursor-pointer text-wv-secondary underline underline-offset-2 hover:text-wv-fg"
              >
                {t("next.providers.forgetCertificate")}
              </button>
            </div>
            {values.certificateFingerprint ? (
              <span className="font-wv-mono text-[11px] break-all">
                {t("next.providers.certFingerprint", { fingerprint: values.certificateFingerprint })}
              </span>
            ) : null}
          </div>
        ) : null}
      </RecordEditor>

      <ConfirmDialog
        open={confirmTrust !== null}
        title={t("next.providers.trustTitle")}
        body={
          <span className="flex flex-col gap-2.5">
            <span>{t("next.providers.trustBody")}</span>
            <span className="flex flex-col gap-1 font-wv-mono text-[11px] break-all text-wv-muted">
              {confirmTrust?.names.length ? (
                <span>{t("next.providers.certNames", { names: formatHostnames(confirmTrust.names) })}</span>
              ) : null}
              <span>{t("next.providers.certFingerprint", { fingerprint: confirmTrust?.fingerprint ?? "" })}</span>
            </span>
          </span>
        }
        confirmLabel={t("next.providers.trustConfirm")}
        onConfirm={trust}
        onDismiss={() => setConfirmTrust(null)}
      />

      <ConfirmDialog
        open={confirmRemove !== null}
        title={t("next.providers.remove")}
        note={confirmRemove?.host}
        busy={busy}
        confirmLabel={t("next.providers.remove")}
        body={t("next.providers.removeBody", {
          host: confirmRemove?.host ?? t("next.providers.thisProvider"),
        })}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmRemove(null)}
      />
    </>
  );
}

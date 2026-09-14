import { useEffect, useMemo, useState } from "react";
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
import { directRouting, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";
import { BetaTag, Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { RoutingEditor } from "../../../components/RoutingEditor";
import { PrimaryButton, SecondaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WorkingOverlay } from "../../../components/WorkingOverlay";
import { WV } from "../../../data/palette";
import { formatLatency, formatSize } from "../../../data/format";
import { PanelControls, SettingsBlocks, type FieldSpec, type SettingsBlock } from "../framework";

/**
 * Providers: the news servers weaver downloads from.
 *
 * The table is the panel; everything editable about a server lives in the
 * record editor behind a row, which is the only way a form this wide fits the
 * design's row rhythm. Toggling a server on or off writes immediately, so the
 * top bar's Save stays out of it.
 */

const MIB = 1024 * 1024;
const GIB = 1024 ** 3;
const TIB = 1024 ** 4;

type QuotaPeriod = "ONE_TIME" | "DAILY" | "WEEKLY" | "MONTHLY";
type Weekday = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";

interface ServerQuota {
  enabled: boolean;
  period: QuotaPeriod;
  limitBytes: number;
  resetTimeMinutesLocal: number;
  weeklyResetWeekday: Weekday;
  monthlyResetDay: number;
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
  quota: ServerQuota;
  quotaLimit: string;
  quotaUnit: "GB" | "TB";
  quotaResetTime: string;
  routing: RoutingPolicy;
  certificateDerBase64: string | null;
}

const EMPTY_QUOTA: ServerQuota = {
  enabled: false,
  period: "MONTHLY",
  limitBytes: 0,
  resetTimeMinutesLocal: 0,
  weeklyResetWeekday: "MON",
  monthlyResetDay: 1,
  usedBytes: 0,
  remainingBytes: 0,
  blocked: false,
};

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
  quota: EMPTY_QUOTA,
  quotaLimit: "",
  quotaUnit: "GB",
  quotaResetTime: "00:00",
  routing: directRouting,
  certificateDerBase64: null,
};

/** Labels are translation keys, resolved when the panel renders. */
const QUOTA_PERIODS: { value: string; label: string }[] = [
  { value: "ONE_TIME", label: "next.providers.oneBlock" },
  { value: "DAILY", label: "next.bandwidth.daily" },
  { value: "WEEKLY", label: "next.bandwidth.weekly" },
  { value: "MONTHLY", label: "next.bandwidth.monthly" },
];

const WEEKDAYS: { value: string; label: string }[] = [
  { value: "MON", label: "next.weekday.mon" },
  { value: "TUE", label: "next.weekday.tue" },
  { value: "WED", label: "next.weekday.wed" },
  { value: "THU", label: "next.weekday.thu" },
  { value: "FRI", label: "next.weekday.fri" },
  { value: "SAT", label: "next.weekday.sat" },
  { value: "SUN", label: "next.weekday.sun" },
];

function minutesToTime(minutes: number): string {
  const clamped = Math.max(0, Math.min(23 * 60 + 59, Math.round(minutes)));
  return `${String(Math.floor(clamped / 60)).padStart(2, "0")}:${String(clamped % 60).padStart(2, "0")}`;
}

function timeToMinutes(raw: string): number {
  const [hours, minutes] = raw.split(":").map(Number);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) {
    return 0;
  }
  return Math.max(0, Math.min(23 * 60 + 59, hours * 60 + minutes));
}

function trimNumber(value: number): string {
  return String(Number(value.toFixed(4)));
}

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
  const quota = server.downloadQuota ?? EMPTY_QUOTA;
  const unit = quota.limitBytes >= TIB ? "TB" : "GB";
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
    quota,
    quotaLimit: quota.limitBytes === 0 ? "" : trimNumber(quota.limitBytes / (unit === "TB" ? TIB : GIB)),
    quotaUnit: unit,
    quotaResetTime: minutesToTime(quota.resetTimeMinutesLocal),
    routing: server.routing ?? directRouting,
    certificateDerBase64:
      "tlsNameMismatchCertificateDerBase64" in server
        ? server.tlsNameMismatchCertificateDerBase64
        : null,
  };
}

function serverInput(form: ServerForm) {
  const quotaUnitBytes = form.quotaUnit === "TB" ? TIB : GIB;
  return {
    routing: { proxyIds: form.routing.proxyIds, allowDirect: form.routing.allowDirect },
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
    downloadQuota: {
      enabled: form.quota.enabled,
      limitBytes: Math.max(0, Math.round(Number(form.quotaLimit || 0) * quotaUnitBytes)),
      period: form.quota.period,
      resetTimeMinutesLocal: timeToMinutes(form.quotaResetTime),
      weeklyResetWeekday: form.quota.weeklyResetWeekday,
      monthlyResetDay: Math.min(31, Math.max(1, Math.trunc(form.quota.monthlyResetDay))),
    },
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
  const [form, setForm] = useState<ServerForm | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [confirmRemove, setConfirmRemove] = useState<Server | null>(null);

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

  const details = typeof editingId === "number" ? detailsData?.server : null;
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
  const askedToAdd = searchParams.has("add");
  useEffect(() => {
    if (!askedToAdd) {
      return;
    }
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
      setForm({ ...values, ...next });
    }
  };

  const closeEditor = () => {
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
    const result =
      editingId === "new"
        ? await addServer({ input })
        : await updateServer({ id: editingId, input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    void reexecute({ requestPolicy: "network-only" });
    closeEditor();
  };

  const runTest = async () => {
    if (!values) {
      return;
    }
    setTesting(true);
    setTestResult(null);
    const result = await testConnection({ input: serverInput(values) });
    setTesting(false);
    setTestResult((result.data?.testConnection as TestResult) ?? null);
  };

  const remove = async () => {
    if (!confirmRemove) {
      return;
    }
    setBusy(true);
    await removeServer({ id: confirmRemove.id });
    setBusy(false);
    setConfirmRemove(null);
    closeEditor();
    void reexecute({ requestPolicy: "network-only" });
  };

  const addProvider = () => {
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
      columns: "minmax(0, 1fr) 92px 190px 150px 44px",
      headers: [
        t("next.providers.host"),
        t("next.providers.threads"),
        t("next.providers.transport"),
        t("next.providers.role"),
        "",
      ],
      empty: t("next.providers.empty"),
      emptyAction: { label: t("next.providers.add"), onClick: addProvider },
      onRowClick: (id) => {
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
          <Cell key="threads" mono>
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
                onChange: (next) => patch({ host: next }),
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
                onChange: (next) => patch({ port: next }),
              },
            },
            {
              id: "tls",
              label: "TLS",
              help: t("next.providers.tlsHelp"),
              control: {
                kind: "toggle",
                value: values.tls,
                onChange: (next) => patch({ tls: next, port: next ? 563 : 119 }),
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
              label: t("next.providers.threads"),
              help: t("next.providers.threadsHelp"),
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
              control: { kind: "toggle", value: values.active, onChange: (next) => patch({ active: next }) },
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
            {
              id: "quotaEnabled",
              label: t("next.providers.quota"),
              help: t("next.providers.quotaHelp"),
              control: {
                kind: "toggle",
                value: values.quota.enabled,
                onChange: (next) => patch({ quota: { ...values.quota, enabled: next } }),
              },
            },
            ...(values.quota.enabled ? quotaFields(t, values, patch) : []),
          ],
        },
        {
          id: "routing",
          title: t("next.providers.networkRoute"),
          tag: <BetaTag />,
          fields: [
            {
              id: "routing",
              label: t("next.providers.proxyRoute"),
              help: t("next.providers.proxyRouteHelp"),
              control: {
                kind: "custom",
                control: (
                  <RoutingEditor
                    value={values.routing}
                    onChange={(next) => patch({ routing: next })}
                  />
                ),
              },
            },
          ],
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
            {editing && values?.quota.enabled ? (
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
            <SecondaryButton icon="test" onClick={() => void runTest()} disabled={testing}>
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
            <div className="flex items-center gap-[9px] text-[13px]">
              <Square color={testResult.success ? WV.green : WV.error} />
              <span className={testResult.success ? "text-wv-fg" : "text-wv-error-text"}>
                {testResult.message}
              </span>
            </div>
            <div className="font-wv-mono text-[11.5px] text-wv-muted">
              {formatLatency(testResult.latencyMs)}
              {testResult.supportsPipelining ? ` · ${t("next.providers.pipelining")}` : ""}
              {testResult.tlsCipherSuite ? ` · ${shortCipher(testResult.tlsCipherSuite)}` : ""}
            </div>
            {testResult.adoptableTlsNameMismatchCertificate ? (
              <div className="flex flex-wrap items-center gap-3 pt-1">
                <span className="text-[12px] text-wv-warn">
                  {t("next.providers.certMismatch")}
                </span>
                <SecondaryButton
                  icon="trust"
                  onClick={() =>
                    patch({
                      certificateDerBase64:
                        testResult.adoptableTlsNameMismatchCertificate?.derBase64 ?? null,
                    })
                  }
                >
                  {t("next.providers.trustCert")}
                </SecondaryButton>
              </div>
            ) : null}
          </div>
        ) : null}
      </RecordEditor>

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

function quotaFields(
  t: Translate,
  values: ServerForm,
  patch: (next: Partial<ServerForm>) => void,
): FieldSpec[] {
  return [
    {
      id: "quotaPeriod",
      label: t("next.providers.quotaWindow"),
      control: {
        kind: "select",
        value: values.quota.period,
        options: QUOTA_PERIODS.map((option) => ({ ...option, label: t(option.label) })),
        onChange: (next) => patch({ quota: { ...values.quota, period: next as QuotaPeriod } }),
      },
    },
    {
      id: "quotaLimit",
      label: t("next.bandwidth.allowance"),
      help: t("next.providers.usedSoFar", { size: formatSize(values.quota.usedBytes) }),
      control: {
        kind: "custom",
        control: (
          <div className="flex items-center gap-[10px]">
            <input
              type="text"
              inputMode="decimal"
              aria-label={t("next.bandwidth.allowance")}
              value={values.quotaLimit}
              placeholder="0"
              onChange={(event) => patch({ quotaLimit: event.target.value })}
              className="h-[34px] w-[110px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus"
            />
            <div className="flex border border-wv-control bg-wv-input">
              {(["GB", "TB"] as const).map((unit, index) => (
                <button
                  key={unit}
                  type="button"
                  aria-pressed={values.quotaUnit === unit}
                  onClick={() => patch({ quotaUnit: unit })}
                  className={`flex h-8 cursor-pointer items-center px-[13px] font-wv-mono text-[12px] ${
                    index > 0 ? "border-l border-wv-control " : ""
                  }${
                    values.quotaUnit === unit
                      ? "bg-wv-segment-active font-medium text-wv-strong"
                      : "text-wv-muted hover:text-wv-strong"
                  }`}
                >
                  {unit}
                </button>
              ))}
            </div>
          </div>
        ),
      },
    },
    ...(values.quota.period === "ONE_TIME"
      ? []
      : [
          {
            id: "quotaResetTime",
            label: t("next.bandwidth.resetAt"),
            control: {
              kind: "time" as const,
              value: values.quotaResetTime,
              onChange: (next: string) => patch({ quotaResetTime: next }),
            },
          },
        ]),
    ...(values.quota.period === "WEEKLY"
      ? [
          {
            id: "quotaWeekday",
            label: t("next.bandwidth.resetDay"),
            control: {
              kind: "select" as const,
              value: values.quota.weeklyResetWeekday,
              options: WEEKDAYS.map((option) => ({ ...option, label: t(option.label) })),
              onChange: (next: string) =>
                patch({ quota: { ...values.quota, weeklyResetWeekday: next as Weekday } }),
            },
          },
        ]
      : []),
    ...(values.quota.period === "MONTHLY"
      ? [
          {
            id: "quotaMonthDay",
            label: t("next.bandwidth.resetDayOfMonth"),
            control: {
              kind: "number" as const,
              value: values.quota.monthlyResetDay,
              min: 1,
              max: 31,
              onChange: (next: number) =>
                patch({ quota: { ...values.quota, monthlyResetDay: next } }),
            },
          },
        ]
      : []),
  ];
}

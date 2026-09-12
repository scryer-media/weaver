import { useEffect, useMemo, useState } from "react";
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
import { directRouting, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { RoutingEditor } from "../../../components/RoutingEditor";
import { SecondaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
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

const QUOTA_PERIODS: { value: string; label: string }[] = [
  { value: "ONE_TIME", label: "One block" },
  { value: "DAILY", label: "Daily" },
  { value: "WEEKLY", label: "Weekly" },
  { value: "MONTHLY", label: "Monthly" },
];

const WEEKDAYS: { value: string; label: string }[] = [
  { value: "MON", label: "Monday" },
  { value: "TUE", label: "Tuesday" },
  { value: "WED", label: "Wednesday" },
  { value: "THU", label: "Thursday" },
  { value: "FRI", label: "Friday" },
  { value: "SAT", label: "Saturday" },
  { value: "SUN", label: "Sunday" },
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

function transportLabel(server: Server): string {
  if (!server.tls) {
    return "Plain";
  }
  return server.tlsCipherSuite ? `TLS · ${shortCipher(server.tlsCipherSuite)}` : "TLS";
}

function roleLabel(server: Server): string {
  if (server.backfill) {
    return "Block account";
  }
  return server.priority === 0
    ? "Primary"
    : server.priority === 1
      ? "Secondary"
      : `Priority ${server.priority}`;
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
  const [{ data }, reexecute] = useQuery<{ servers: Server[] }>({ query: SERVERS_QUERY });
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
    const stored = await client.query<{ server: ServerDetails | null }>(SERVER_QUERY, {
      id: server.id,
    }).toPromise();
    const username = stored.data?.server?.username ?? "";
    await updateServer({
      id: server.id,
      input: { ...serverInput(formToState(server, username)), active },
    });
    void reexecute({ requestPolicy: "network-only" });
  };

  const save = async () => {
    if (!values) {
      return;
    }
    if (!normalizeHost(values.host)) {
      setError("A provider needs a hostname.");
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

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "servers",
      title: "Servers",
      note: "tried in priority order",
      columns: "minmax(0, 1fr) 92px 190px 150px 44px",
      headers: ["Host", "Threads", "Transport", "Role", ""],
      empty: "No providers yet. Add one to start downloading.",
      onRowClick: (id) => {
        setForm(null);
        setTestResult(null);
        setEditingId(Number(id));
      },
      rows: servers.map((server) => ({
        id: String(server.id),
        searchText: `${server.host} ${server.port} ${roleLabel(server)} ${transportLabel(server)}`,
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
            {transportLabel(server)}
          </Cell>,
          <Cell key="role">{roleLabel(server)}</Cell>,
          <span key="active" onClick={(event) => event.stopPropagation()}>
            <Toggle
              size="table"
              checked={server.active}
              onChange={(next) => void setActive(server, next)}
              label={`${server.host} enabled`}
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
          title: "Connection",
          fields: [
            {
              id: "host",
              label: "Host",
              help: "The provider's news server address.",
              control: {
                kind: "text",
                value: values.host,
                placeholder: "news.example.com",
                onChange: (next) => patch({ host: next }),
              },
            },
            {
              id: "port",
              label: "Port",
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
              help: "Encrypt the connection. Plain text is only sensible on a local relay.",
              control: {
                kind: "toggle",
                value: values.tls,
                onChange: (next) => patch({ tls: next, port: next ? 563 : 119 }),
              },
            },
            {
              id: "username",
              label: "Username",
              control: {
                kind: "text",
                value: values.username,
                onChange: (next) => patch({ username: next }),
              },
            },
            {
              id: "password",
              label: "Password",
              help: editingId === "new" ? undefined : "Leave blank to keep the stored password.",
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
              label: "Threads",
              help: "Connections weaver may open at once. Never exceed what the plan allows.",
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
          title: "Role",
          fields: [
            {
              id: "active",
              label: "Enabled",
              help: "A disabled provider is kept but never dialled.",
              control: { kind: "toggle", value: values.active, onChange: (next) => patch({ active: next }) },
            },
            {
              id: "priority",
              label: "Priority",
              help: "Lower is tried first. Providers sharing a number are used together.",
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
              label: "Block account",
              help: "Only used to fill articles the other providers could not supply.",
              control: {
                kind: "toggle",
                value: values.backfill,
                onChange: (next) => patch({ backfill: next }),
              },
            },
            {
              id: "retentionDays",
              label: "Retention",
              help: "Skip this provider for articles older than this. Zero means no limit.",
              control: {
                kind: "number",
                value: values.retentionDays,
                min: 0,
                onChange: (next) => patch({ retentionDays: next }),
                suffix: "days",
              },
            },
          ],
        },
        {
          id: "limits",
          title: "Limits",
          fields: [
            {
              id: "speedUnlimited",
              label: "Unlimited speed",
              help: "Turn off to hold this provider to a fixed rate.",
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
                    label: "Speed ceiling",
                    control: {
                      kind: "text" as const,
                      value: values.speedMib,
                      onChange: (next: string) => patch({ speedMib: next }),
                      className: "w-[110px]",
                    },
                    help: "Mebibytes per second.",
                  },
                ]),
            {
              id: "quotaEnabled",
              label: "Data quota",
              help: "Stop using this provider once its allowance is spent.",
              control: {
                kind: "toggle",
                value: values.quota.enabled,
                onChange: (next) => patch({ quota: { ...values.quota, enabled: next } }),
              },
            },
            ...(values.quota.enabled ? quotaFields(values, patch) : []),
          ],
        },
        {
          id: "routing",
          title: "Network route",
          fields: [
            {
              id: "routing",
              label: "Proxy route",
              help: "Each route is tried in order; new connections return to the first when it recovers.",
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
        <SecondaryButton
          onClick={() => {
            setForm(NEW_SERVER);
            setTestResult(null);
            setEditingId("new");
          }}
        >
          Add provider
        </SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? "Add provider" : (editing?.host ?? "Provider")}
        note={editingId === "new" ? "new server" : `priority ${values?.priority ?? 0}`}
        width={620}
        sections={sections}
        error={error}
        busy={busy}
        saveDisabled={values === null}
        onSave={() => void save()}
        onDismiss={closeEditor}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel="Remove provider"
        extraActions={
          <>
            {editing && values?.quota.enabled ? (
              <SecondaryButton
                onClick={() => {
                  void resetQuota({ id: editing.id }).then(() =>
                    reexecute({ requestPolicy: "network-only" }),
                  );
                }}
              >
                Reset usage
              </SecondaryButton>
            ) : null}
            <SecondaryButton onClick={() => void runTest()} disabled={testing}>
              {testing ? "Testing…" : "Test connection"}
            </SecondaryButton>
          </>
        }
      >
        {values === null ? (
          <div className="px-4 sm:px-6 py-5 font-wv-mono text-[12px] text-wv-muted">Loading provider…</div>
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
              {testResult.supportsPipelining ? " · pipelining" : ""}
              {testResult.tlsCipherSuite ? ` · ${shortCipher(testResult.tlsCipherSuite)}` : ""}
            </div>
            {testResult.adoptableTlsNameMismatchCertificate ? (
              <div className="flex flex-wrap items-center gap-3 pt-1">
                <span className="text-[12px] text-wv-warn">
                  The certificate belongs to a different hostname.
                </span>
                <SecondaryButton
                  onClick={() =>
                    patch({
                      certificateDerBase64:
                        testResult.adoptableTlsNameMismatchCertificate?.derBase64 ?? null,
                    })
                  }
                >
                  Trust this certificate
                </SecondaryButton>
              </div>
            ) : null}
          </div>
        ) : null}
      </RecordEditor>

      <ConfirmDialog
        open={confirmRemove !== null}
        title="Remove provider"
        note={confirmRemove?.host}
        busy={busy}
        confirmLabel="Remove provider"
        body={`Weaver will stop using ${confirmRemove?.host ?? "this provider"}. Downloads in flight fall back to the remaining providers.`}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmRemove(null)}
      />
    </>
  );
}

function quotaFields(values: ServerForm, patch: (next: Partial<ServerForm>) => void): FieldSpec[] {
  return [
    {
      id: "quotaPeriod",
      label: "Quota window",
      control: {
        kind: "select",
        value: values.quota.period,
        options: QUOTA_PERIODS,
        onChange: (next) => patch({ quota: { ...values.quota, period: next as QuotaPeriod } }),
      },
    },
    {
      id: "quotaLimit",
      label: "Allowance",
      help: `Used so far: ${formatSize(values.quota.usedBytes)}.`,
      control: {
        kind: "custom",
        control: (
          <div className="flex items-center gap-[10px]">
            <input
              type="text"
              inputMode="decimal"
              aria-label="Allowance"
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
            label: "Reset at",
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
            label: "Reset day",
            control: {
              kind: "select" as const,
              value: values.quota.weeklyResetWeekday,
              options: WEEKDAYS,
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
            label: "Reset day of month",
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

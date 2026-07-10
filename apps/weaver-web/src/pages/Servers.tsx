import { useEffect, useMemo, useState, type ReactNode } from "react";
import { FilePenLine, Loader2, Trash2 } from "lucide-react";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { formatBytes, formatSpeed } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ADD_SERVER_MUTATION,
  REMOVE_SERVER_MUTATION,
  RESET_SERVER_DOWNLOAD_QUOTA_USAGE_MUTATION,
  SERVER_QUERY,
  SERVERS_QUERY,
  TEST_CONNECTION_MUTATION,
  UPDATE_SERVER_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

type ServerDownloadQuotaPeriod = "ONE_TIME" | "DAILY" | "WEEKLY" | "MONTHLY";
type ServerDownloadQuotaWeekday = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";

type ServerDownloadQuota = {
  enabled: boolean;
  period: ServerDownloadQuotaPeriod;
  limitBytes: number;
  resetTimeMinutesLocal: number;
  weeklyResetWeekday: ServerDownloadQuotaWeekday;
  monthlyResetDay: number;
  usedBytes: number;
  reservedBytes: number;
  remainingBytes: number;
  blocked: boolean;
  windowStartsAtEpochMs: number | null;
  windowEndsAtEpochMs: number | null;
  timezoneName: string;
};

type Server = {
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
  downloadQuota: ServerDownloadQuota;
};

type ServerDetails = Server & {
  username: string | null;
};

type ServerFormValues = {
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
  maxDownloadSpeedUnlimited: boolean;
  maxDownloadSpeedMib: string;
  maxDownloadSpeedBytes: number;
  maxDownloadSpeedEdited: boolean;
  quotaEnabled: boolean;
  quotaLimit: string;
  quotaUnit: "GB" | "TB";
  quotaLimitBytes: number;
  quotaLimitEdited: boolean;
  quotaPeriod: ServerDownloadQuotaPeriod;
  quotaResetTime: string;
  quotaWeeklyResetWeekday: ServerDownloadQuotaWeekday;
  quotaMonthlyResetDay: number;
};

const defaultForm: ServerFormValues = {
  host: "",
  port: 443,
  tls: true,
  username: "",
  password: "",
  connections: 20,
  active: true,
  priority: 0,
  backfill: false,
  retentionDays: 0,
  maxDownloadSpeedUnlimited: true,
  maxDownloadSpeedMib: "10",
  maxDownloadSpeedBytes: 0,
  maxDownloadSpeedEdited: false,
  quotaEnabled: false,
  quotaLimit: "100",
  quotaUnit: "GB",
  quotaLimitBytes: 107_374_182_400,
  quotaLimitEdited: false,
  quotaPeriod: "MONTHLY",
  quotaResetTime: "00:00",
  quotaWeeklyResetWeekday: "MON",
  quotaMonthlyResetDay: 1,
};

const MIB = 1024 * 1024;
const GIB = 1024 * 1024 * 1024;
const TIB = 1024 * 1024 * 1024 * 1024;

function normalizeMonthlyResetDay(value: number) {
  return Math.min(31, Math.max(1, Math.trunc(Number.isFinite(value) ? value : 1)));
}

function minutesToTimeInput(minutes: number) {
  const clamped = Math.max(0, Math.min(23 * 60 + 59, minutes));
  return `${String(Math.floor(clamped / 60)).padStart(2, "0")}:${String(clamped % 60).padStart(2, "0")}`;
}

function timeInputToMinutes(raw: string) {
  const [hours, minutes] = raw.split(":").map(Number);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return 0;
  return Math.max(0, Math.min(23 * 60 + 59, hours * 60 + minutes));
}

function decimalInput(value: number) {
  return String(Number(value.toFixed(4)));
}

function serverToFormValues(server: ServerDetails | Server): ServerFormValues {
  const quotaUnit = server.downloadQuota.limitBytes >= TIB ? "TB" : "GB";
  const quotaUnitBytes = quotaUnit === "TB" ? TIB : GIB;
  return {
    host: server.host,
    port: server.port,
    tls: server.tls,
    username: "username" in server ? (server.username ?? "") : "",
    password: "",
    connections: server.connections,
    active: server.active,
    priority: server.priority,
    backfill: server.backfill,
    retentionDays: server.retentionDays,
    maxDownloadSpeedUnlimited: server.maxDownloadSpeed === 0,
    maxDownloadSpeedMib: server.maxDownloadSpeed === 0
      ? defaultForm.maxDownloadSpeedMib
      : decimalInput(server.maxDownloadSpeed / MIB),
    maxDownloadSpeedBytes: server.maxDownloadSpeed,
    maxDownloadSpeedEdited: false,
    quotaEnabled: server.downloadQuota.enabled,
    quotaLimit: server.downloadQuota.limitBytes === 0
      ? defaultForm.quotaLimit
      : decimalInput(server.downloadQuota.limitBytes / quotaUnitBytes),
    quotaUnit,
    quotaLimitBytes: server.downloadQuota.limitBytes,
    quotaLimitEdited: false,
    quotaPeriod: server.downloadQuota.period,
    quotaResetTime: minutesToTimeInput(server.downloadQuota.resetTimeMinutesLocal),
    quotaWeeklyResetWeekday: server.downloadQuota.weeklyResetWeekday,
    quotaMonthlyResetDay: server.downloadQuota.monthlyResetDay,
  };
}

function downloadLimitsInput(values: ServerFormValues) {
  const quotaUnitBytes = values.quotaUnit === "TB" ? TIB : GIB;
  const editedSpeedBytes = Math.round(Number(values.maxDownloadSpeedMib) * MIB);
  const editedQuotaBytes = Math.round(Number(values.quotaLimit) * quotaUnitBytes);
  return {
    maxDownloadSpeed: values.maxDownloadSpeedUnlimited
      ? 0
      : !values.maxDownloadSpeedEdited && values.maxDownloadSpeedBytes > 0
        ? values.maxDownloadSpeedBytes
        : editedSpeedBytes,
    downloadQuota: {
      enabled: values.quotaEnabled,
      limitBytes: !values.quotaLimitEdited
        && (values.quotaLimitBytes > 0 || !values.quotaEnabled)
        ? values.quotaLimitBytes
        : editedQuotaBytes,
      period: values.quotaPeriod,
      resetTimeMinutesLocal: timeInputToMinutes(values.quotaResetTime),
      weeklyResetWeekday: values.quotaWeeklyResetWeekday,
      monthlyResetDay: normalizeMonthlyResetDay(values.quotaMonthlyResetDay),
    },
  };
}

export function Servers({ embedded = false }: { embedded?: boolean }) {
  const t = useTranslate();
  const [{ data }, reexecuteServers] = useQuery<{ servers: Server[] }>({ query: SERVERS_QUERY });
  const [editingServerId, setEditingServerId] = useState<number | null>(null);
  const [{ data: editingServerData, fetching: editingServerFetching }] = useQuery<{
    server: ServerDetails | null;
  }>({
    query: SERVER_QUERY,
    variables: { id: editingServerId ?? 0 },
    pause: editingServerId == null,
  });
  const [addServerState, addServer] = useMutation(ADD_SERVER_MUTATION);
  const [updateServerState, updateServer] = useMutation(UPDATE_SERVER_MUTATION);
  const [, removeServer] = useMutation(REMOVE_SERVER_MUTATION);
  const [resetQuotaState, resetServerDownloadQuotaUsage] = useMutation(
    RESET_SERVER_DOWNLOAD_QUOTA_USAGE_MUTATION,
  );
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);

  const [servers, setServers] = useState<Server[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [resetConfirmId, setResetConfirmId] = useState<number | null>(null);
  const [testing, setTesting] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
    latencyMs?: number;
    supportsPipelining?: boolean;
  } | null>(null);

  useEffect(() => {
    if (data?.servers) {
      setServers(data.servers);
    }
  }, [data?.servers]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      void reexecuteServers({ requestPolicy: "network-only" });
    }, 5_000);
    return () => window.clearInterval(interval);
  }, [reexecuteServers]);

  const isSavingServer = addServerState.fetching || updateServerState.fetching;

  const groupedServers = useMemo(() => {
    const groups = new Map<number, Server[]>();
    for (const server of servers) {
      const group = groups.get(server.priority) ?? [];
      group.push(server);
      groups.set(server.priority, group);
    }
    return [...groups.entries()].sort(([left], [right]) => left - right);
  }, [servers]);

  const openAdd = () => {
    setEditingServerId(null);
    setSaveError(null);
    setTestResult(null);
    setShowForm(true);
  };

  const openEdit = (server: Server) => {
    setEditingServerId(server.id);
    setSaveError(null);
    setTestResult(null);
    setShowForm(true);
  };

  const closeForm = () => {
    setEditingServerId(null);
    setSaveError(null);
    setTestResult(null);
    setShowForm(false);
  };

  const handleSave = async (values: ServerFormValues) => {
    setSaveError(null);
    const input = {
      host: values.host.trim(),
      port: values.port,
      tls: values.tls,
      username: values.username.trim() || null,
      password: values.password.trim() || null,
      connections: values.connections,
      active: values.active,
      priority: values.priority,
      backfill: values.backfill,
      retentionDays: values.retentionDays,
      ...downloadLimitsInput(values),
    };

    if (editingServerId != null) {
      const result = await updateServer({ id: editingServerId, input });
      if (result.data?.updateServer) {
        setServers((current) =>
          current.map((server) =>
            server.id === editingServerId ? result.data.updateServer : server,
          ),
        );
        void reexecuteServers({ requestPolicy: "network-only" });
        closeForm();
        return;
      }
      setSaveError(
        result.error?.graphQLErrors[0]?.message
          ?? result.error?.message
          ?? "Unable to save server settings. Fix the connection details and try again.",
      );
      return;
    } else {
      const result = await addServer({ input });
      if (result.data?.addServer) {
        setServers((current) =>
          [...current, result.data.addServer].sort((left, right) =>
            left.priority - right.priority || left.host.localeCompare(right.host),
          ),
        );
        void reexecuteServers({ requestPolicy: "network-only" });
        closeForm();
        return;
      }
      setSaveError(
        result.error?.graphQLErrors[0]?.message
          ?? result.error?.message
          ?? "Unable to save server settings. Fix the connection details and try again.",
      );
      return;
    }
  };

  const handleDelete = async (id: number) => {
    const result = await removeServer({ id });
    if (result.data?.removeServer) {
      setServers(result.data.removeServer);
      void reexecuteServers({ requestPolicy: "network-only" });
    }
    setDeleteConfirmId(null);
  };

  const handleResetQuotaUsage = async (id: number) => {
    if (resetQuotaState.fetching) return;
    setSaveError(null);
    const result = await resetServerDownloadQuotaUsage({ id });
    if (result.data?.resetServerDownloadQuotaUsage) {
      setServers((current) =>
        current.map((server) =>
          server.id === id ? result.data.resetServerDownloadQuotaUsage : server,
        ),
      );
      void reexecuteServers({ requestPolicy: "network-only" });
    } else {
      setSaveError(
        result.error?.graphQLErrors[0]?.message
          ?? result.error?.message
          ?? t("servers.resetQuotaError"),
      );
    }
    setResetConfirmId(null);
  };

  const handleTest = async (values: ServerFormValues) => {
    setTesting(true);
    setSaveError(null);
    setTestResult(null);
    const result = await testConnection({
      input: {
        host: values.host.trim(),
        port: values.port,
        tls: values.tls,
        username: values.username.trim() || null,
        password: values.password.trim() || null,
        connections: values.connections,
        active: values.active,
        priority: values.priority,
        backfill: values.backfill,
        retentionDays: values.retentionDays,
      },
    });
    setTestResult(result.data?.testConnection ?? null);
    setTesting(false);
  };

  const editingServer = useMemo(
    () => servers.find((server) => server.id === editingServerId) ?? null,
    [editingServerId, servers],
  );
  const editingServerDetail = editingServerData?.server ?? null;

  return (
    <div className={embedded ? "space-y-5" : "space-y-6"}>
      <PageHeader
        title={t("servers.title")}
        description={embedded ? t("settings.serversDesc") : t("servers.description")}
        actions={<Button onClick={openAdd}>{t("servers.addServer")}</Button>}
      />

      {showForm ? (
        editingServerId != null && !editingServerDetail ? (
          <SectionCard title={t("servers.editServer")} description={t("settings.serversDesc")}>
            <div className="flex items-center justify-between gap-4">
              <div className="flex items-center gap-3 text-sm text-muted-foreground">
                {editingServerFetching ? <Loader2 className="size-4 animate-spin" /> : null}
                <span>
                  {editingServerFetching
                    ? t("label.loading")
                    : "Unable to load server details for editing."}
                </span>
              </div>
              <Button variant="ghost" onClick={closeForm}>
                {t("action.cancel")}
              </Button>
            </div>
          </SectionCard>
        ) : (
          <ServerFormCard
            key={editingServerId ?? "new"}
            initialValues={
              editingServerDetail
                ? serverToFormValues(editingServerDetail)
                : editingServer
                  ? serverToFormValues(editingServer)
                  : defaultForm
            }
            runtimeServer={editingServer ?? editingServerDetail}
            editing={editingServerId != null}
            saving={isSavingServer}
            testing={testing}
            resettingQuota={resetQuotaState.fetching}
            saveError={saveError}
            testResult={testResult}
            onCancel={closeForm}
            onSave={handleSave}
            onTest={handleTest}
            onRequestQuotaReset={() => {
              if (editingServerId != null) setResetConfirmId(editingServerId);
            }}
          />
        )
      ) : null}

      {servers.length === 0 && !showForm ? (
        <EmptyState
          title={t("servers.empty")}
          description={t("servers.emptyHint")}
          actionLabel={t("servers.addServer")}
          onAction={openAdd}
        />
      ) : (
        groupedServers.map(([priority, items]) => (
          <SectionCard
            key={priority}
            title={`${t("servers.group")} ${priority}`}
            description={t("servers.groupDescription")}
            contentClassName="-mx-5 -mb-5 sm:-mx-6 sm:-mb-6"
          >
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>{t("servers.host")}</TableHead>
                    <TableHead>{t("servers.connections")}</TableHead>
                    <TableHead>{t("servers.tls")}</TableHead>
                    <TableHead>{t("servers.limits")}</TableHead>
                    <TableHead>{t("servers.active")}</TableHead>
                    <TableHead>{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((server) => (
                    <TableRow key={server.id}>
                      <TableCell>
                        <div className="flex items-center gap-2.5">
                          <span
                            className={cn(
                              "size-2 shrink-0 rounded-pill",
                              server.active ? "bg-status-completed animate-status-pulse" : "bg-muted-foreground/40",
                            )}
                            aria-hidden="true"
                          />
                          <div className="min-w-0">
                            <div className="flex items-center gap-2">
                              <span
                                className={cn(
                                  "rounded-chip px-1.5 py-px text-[10px] font-bold uppercase tracking-[0.06em]",
                                  server.backfill
                                    ? "bg-status-queued/15 text-status-queued"
                                    : priority === 0
                                      ? "bg-priority-high/15 text-priority-high"
                                      : "bg-secondary text-muted-foreground",
                                )}
                              >
                                {server.backfill
                                  ? t("servers.backfillBadge")
                                  : priority === 0
                                    ? "Primary"
                                    : "Backup"}
                              </span>
                              {server.retentionDays > 0 ? (
                                <span className="text-[10px] text-muted-foreground">
                                  {t("servers.retentionBadge").replace(
                                    "{days}",
                                    String(server.retentionDays),
                                  )}
                                </span>
                              ) : null}
                            </div>
                            <div className="mt-1 truncate font-mono text-[13px] text-foreground">
                              {server.host}:{server.port}
                            </div>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell className="tabular-nums">{server.connections}</TableCell>
                      <TableCell>{server.tls ? t("label.enabled") : t("label.disabled")}</TableCell>
                      <TableCell><ServerLimitsSummary server={server} /></TableCell>
                      <TableCell>{server.active ? t("label.enabled") : t("label.disabled")}</TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-2">
                          <Button variant="outline" size="sm" onClick={() => openEdit(server)}>
                            <FilePenLine className="size-4" />
                            {t("action.edit")}
                          </Button>
                          <Button variant="destructive" size="sm" onClick={() => setDeleteConfirmId(server.id)}>
                            <Trash2 className="size-4" />
                            {t("action.delete")}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </SectionCard>
        ))
      )}

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteServer")}
        message={t("confirm.deleteServerMessage")}
        confirmLabel={t("confirm.deleteServerConfirm")}
        cancelLabel={t("confirm.deleteServerDismiss")}
        onConfirm={() => deleteConfirmId != null && void handleDelete(deleteConfirmId)}
        onCancel={() => setDeleteConfirmId(null)}
      />
      <ConfirmDialog
        open={resetConfirmId != null}
        title={t("servers.resetQuotaTitle")}
        message={t("servers.resetQuotaMessage")}
        confirmLabel={t("servers.resetQuotaConfirm")}
        cancelLabel={t("action.cancel")}
        confirmDisabled={resetQuotaState.fetching}
        cancelDisabled={resetQuotaState.fetching}
        onConfirm={() => {
          if (!resetQuotaState.fetching && resetConfirmId != null) {
            void handleResetQuotaUsage(resetConfirmId);
          }
        }}
        onCancel={() => {
          if (!resetQuotaState.fetching) setResetConfirmId(null);
        }}
      />
    </div>
  );
}

function ServerLimitsSummary({ server }: { server: Server }) {
  const t = useTranslate();
  const quota = server.downloadQuota;
  const quotaActivelyBlocked = quota.blocked && server.active;
  const unlimited = server.maxDownloadSpeed === 0 && !quota.enabled;
  if (unlimited) {
    return <span className="text-xs text-muted-foreground">{t("servers.unlimited")}</span>;
  }
  return (
    <div className="min-w-32 space-y-1 text-xs" aria-label={t("servers.limits")}>
      <div className="whitespace-nowrap text-foreground">
        {server.maxDownloadSpeed === 0 ? t("servers.unlimitedSpeed") : formatSpeed(server.maxDownloadSpeed)}
      </div>
      {quota.enabled ? (
        <div className={cn(
          "whitespace-nowrap",
          quotaActivelyBlocked ? "font-medium text-destructive" : "text-muted-foreground",
        )}>
          {quota.blocked
            ? t("servers.quotaReached")
            : t("servers.quotaCompact", {
                used: formatBytes(quota.usedBytes),
                limit: formatBytes(quota.limitBytes),
              })}
        </div>
      ) : null}
    </div>
  );
}

function formatQuotaResetAt(epochMs: number | null, timezoneName: string) {
  if (epochMs == null) return null;
  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
      timeZone: timezoneName,
    }).format(new Date(epochMs));
  } catch {
    return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
      .format(new Date(epochMs));
  }
}

function ServerFormCard({
  initialValues,
  runtimeServer,
  editing,
  saving,
  testing,
  resettingQuota,
  saveError,
  testResult,
  onSave,
  onTest,
  onRequestQuotaReset,
  onCancel,
}: {
  initialValues: ServerFormValues;
  runtimeServer: Server | ServerDetails | null;
  editing: boolean;
  saving: boolean;
  testing: boolean;
  resettingQuota: boolean;
  saveError: string | null;
  testResult: {
    success: boolean;
    message: string;
    latencyMs?: number;
    supportsPipelining?: boolean;
  } | null;
  onSave: (values: ServerFormValues) => Promise<void>;
  onTest: (values: ServerFormValues) => Promise<void>;
  onRequestQuotaReset: () => void;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);
  const [showTlsWarning, setShowTlsWarning] = useState(false);
  const speedLimitValue = Number(values.maxDownloadSpeedMib);
  const quotaLimitValue = Number(values.quotaLimit);
  const enteredSpeedBytes = Math.round(speedLimitValue * MIB);
  const enteredQuotaBytes = Math.round(
    quotaLimitValue * (values.quotaUnit === "TB" ? TIB : GIB),
  );
  const effectiveSpeedBytes = !values.maxDownloadSpeedEdited && values.maxDownloadSpeedBytes > 0
    ? values.maxDownloadSpeedBytes
    : enteredSpeedBytes;
  const effectiveQuotaBytes = !values.quotaLimitEdited
    && (values.quotaLimitBytes > 0 || !values.quotaEnabled)
    ? values.quotaLimitBytes
    : enteredQuotaBytes;
  const resetTimeValid = /^([01]\d|2[0-3]):[0-5]\d$/.test(values.quotaResetTime);
  const monthDayValid = Number.isInteger(values.quotaMonthlyResetDay)
    && values.quotaMonthlyResetDay >= 1
    && values.quotaMonthlyResetDay <= 31;
  const limitsError = !values.maxDownloadSpeedUnlimited
    && (!Number.isFinite(speedLimitValue) || speedLimitValue <= 0)
    ? t("servers.speedValidation")
    : !values.maxDownloadSpeedUnlimited && !Number.isSafeInteger(effectiveSpeedBytes)
      ? t("servers.byteRangeValidation")
    : values.quotaEnabled && (!Number.isFinite(quotaLimitValue) || quotaLimitValue <= 0)
      ? t("servers.quotaValidation")
      : !Number.isSafeInteger(effectiveQuotaBytes) || effectiveQuotaBytes < 0
        ? t("servers.byteRangeValidation")
      : values.quotaEnabled && values.quotaPeriod !== "ONE_TIME" && !resetTimeValid
        ? t("servers.resetTimeValidation")
        : values.quotaEnabled
          && values.quotaPeriod === "MONTHLY"
          && !monthDayValid
          ? t("servers.monthDayValidation")
          : null;
  const runtimeQuota = runtimeServer?.downloadQuota ?? null;
  const runtimeQuotaActivelyBlocked = runtimeQuota?.blocked === true && runtimeServer?.active !== false;
  const quotaProgress = runtimeQuota?.limitBytes
    ? Math.min(100, ((runtimeQuota.usedBytes + runtimeQuota.reservedBytes) / runtimeQuota.limitBytes) * 100)
    : 0;
  const quotaResetAt = runtimeQuota
    ? formatQuotaResetAt(runtimeQuota.windowEndsAtEpochMs, runtimeQuota.timezoneName)
    : null;

  const handleTlsChange = (checked: boolean) => {
    if (!checked && values.tls) {
      setShowTlsWarning(true);
    } else {
      setValues((current) => ({
        ...current,
        tls: checked,
        port: checked && current.port === 119 ? 443 : current.port,
      }));
    }
  };

  const confirmDisableTls = () => {
    setValues((current) => ({
      ...current,
      tls: false,
      port: (current.port === 443 || current.port === 563) ? 119 : current.port,
    }));
    setShowTlsWarning(false);
  };

  return (
    <SectionCard
      title={editing ? t("servers.editServer") : t("servers.addServer")}
      description={t("settings.serversDesc")}
    >
      <div className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <Field label={t("servers.host")}>
            <Input
              value={values.host}
              placeholder="news.example.com"
              onChange={(event) => setValues((current) => ({ ...current, host: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.port")}>
            <Input
              type="number"
              value={values.port}
              onChange={(event) => setValues((current) => ({ ...current, port: Number(event.target.value) }))}
            />
          </Field>
          <Field label={t("servers.username")}>
            <Input
              value={values.username}
              onChange={(event) => setValues((current) => ({ ...current, username: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.password")}>
            <Input
              type="password"
              value={values.password}
              placeholder={editing ? "Leave blank to keep" : ""}
              onChange={(event) => setValues((current) => ({ ...current, password: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.connections")}>
            <Input
              type="number"
              min={1}
              max={50}
              value={values.connections}
              onChange={(event) => setValues((current) => ({ ...current, connections: Number(event.target.value) }))}
            />
          </Field>
          <Field label={t("servers.group")} description={t("servers.groupDescription")}>
            <Input
              type="number"
              min={0}
              value={values.priority}
              onChange={(event) => setValues((current) => ({ ...current, priority: Number(event.target.value) }))}
            />
          </Field>
          <Field label={t("servers.retention")} description={t("servers.retentionDescription")}>
            <Input
              type="number"
              min={0}
              value={values.retentionDays}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  retentionDays: Math.max(0, Number(event.target.value) || 0),
                }))
              }
            />
          </Field>
        </div>

        <div className="flex flex-wrap gap-6 rounded-inner border border-border bg-background/40 p-4">
          <ToggleField
            label={t("servers.tls")}
            checked={values.tls}
            onCheckedChange={handleTlsChange}
          />
          <ToggleField
            label={t("servers.active")}
            checked={values.active}
            onCheckedChange={(checked) => setValues((current) => ({ ...current, active: checked }))}
          />
          <ToggleField
            label={t("servers.backfill")}
            description={t("servers.backfillDescription")}
            checked={values.backfill}
            onCheckedChange={(checked) => setValues((current) => ({ ...current, backfill: checked }))}
          />
        </div>

        <div
          className="space-y-5 rounded-inner border border-border bg-background/40 p-4"
          role="group"
          aria-labelledby="server-download-limits-heading"
          aria-describedby="server-download-limits-description"
        >
          <div>
            <h3 id="server-download-limits-heading" className="text-sm font-semibold text-foreground">
              {t("servers.downloadLimits")}
            </h3>
            <p id="server-download-limits-description" className="mt-1 text-xs text-muted-foreground">
              {t("servers.downloadLimitsDescription")}
            </p>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-3">
              <Label htmlFor="server-speed-limit-mib">{t("servers.speedLimit")}</Label>
              <label htmlFor="server-speed-unlimited" className="flex cursor-pointer items-center gap-2 text-sm">
                <Checkbox
                  id="server-speed-unlimited"
                  checked={values.maxDownloadSpeedUnlimited}
                  onCheckedChange={(checked) =>
                    setValues((current) => ({
                      ...current,
                      maxDownloadSpeedUnlimited: checked === true,
                    }))
                  }
                />
                {t("servers.unlimitedSpeed")}
              </label>
              <div className="flex items-center gap-2">
                <Input
                  id="server-speed-limit-mib"
                  type="number"
                  inputMode="decimal"
                  min="0.01"
                  step="0.01"
                  disabled={values.maxDownloadSpeedUnlimited}
                  value={values.maxDownloadSpeedMib}
                  aria-describedby="server-speed-limit-description"
                  aria-invalid={!values.maxDownloadSpeedUnlimited
                    && (!Number.isFinite(speedLimitValue)
                      || speedLimitValue <= 0
                      || !Number.isSafeInteger(effectiveSpeedBytes))}
                  onChange={(event) =>
                    setValues((current) => ({
                      ...current,
                      maxDownloadSpeedMib: event.target.value,
                      maxDownloadSpeedEdited: true,
                    }))
                  }
                />
                <span className="shrink-0 text-sm text-muted-foreground">MB/s</span>
              </div>
              <p id="server-speed-limit-description" className="text-xs text-muted-foreground">
                {t("servers.speedLimitDescription")}
              </p>
            </div>

            <div className="space-y-3">
              <Label htmlFor="server-quota-limit">{t("servers.downloadQuota")}</Label>
              <label htmlFor="server-quota-enabled" className="flex cursor-pointer items-center gap-2 text-sm">
                <Checkbox
                  id="server-quota-enabled"
                  checked={values.quotaEnabled}
                  onCheckedChange={(checked) =>
                    setValues((current) => ({ ...current, quotaEnabled: checked === true }))
                  }
                />
                {t("servers.enableQuota")}
              </label>
              <div className="flex gap-2">
                <Input
                  id="server-quota-limit"
                  type="number"
                  inputMode="decimal"
                  min="0.01"
                  step="0.01"
                  disabled={!values.quotaEnabled}
                  value={values.quotaLimit}
                  aria-invalid={values.quotaEnabled
                    && (!Number.isFinite(quotaLimitValue)
                      || quotaLimitValue <= 0
                      || !Number.isSafeInteger(effectiveQuotaBytes))}
                  onChange={(event) =>
                    setValues((current) => ({
                      ...current,
                      quotaLimit: event.target.value,
                      quotaLimitEdited: true,
                    }))
                  }
                />
                <Select
                  value={values.quotaUnit}
                  disabled={!values.quotaEnabled}
                  onValueChange={(quotaUnit) =>
                    setValues((current) => ({
                      ...current,
                      quotaUnit: quotaUnit as "GB" | "TB",
                      quotaLimitEdited: true,
                    }))
                  }
                >
                  <SelectTrigger className="w-24" aria-label={t("servers.downloadQuota")}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="GB">GB</SelectItem>
                    <SelectItem value="TB">TB</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {values.quotaEnabled ? (
            <div className="grid gap-4 border-t border-border pt-4 md:grid-cols-2 xl:grid-cols-4">
              <Field label={t("servers.quotaPeriod")} htmlFor="server-quota-period">
                <Select
                  value={values.quotaPeriod}
                  onValueChange={(quotaPeriod) =>
                    setValues((current) => ({
                      ...current,
                      quotaPeriod: quotaPeriod as ServerDownloadQuotaPeriod,
                    }))
                  }
                >
                  <SelectTrigger id="server-quota-period"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ONE_TIME">{t("servers.periodOneTime")}</SelectItem>
                    <SelectItem value="DAILY">{t("servers.periodDaily")}</SelectItem>
                    <SelectItem value="WEEKLY">{t("servers.periodWeekly")}</SelectItem>
                    <SelectItem value="MONTHLY">{t("servers.periodMonthly")}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              {values.quotaPeriod !== "ONE_TIME" ? (
                <Field
                  label={t("servers.resetTime")}
                  description={t("servers.resetTimeLocalDescription")}
                  htmlFor="server-quota-reset-time"
                  descriptionId="server-quota-reset-time-description"
                >
                  <Input
                    id="server-quota-reset-time"
                    type="time"
                    value={values.quotaResetTime}
                    aria-describedby="server-quota-reset-time-description"
                    aria-invalid={!resetTimeValid}
                    onChange={(event) =>
                      setValues((current) => ({ ...current, quotaResetTime: event.target.value }))
                    }
                  />
                </Field>
              ) : null}
              {values.quotaPeriod === "WEEKLY" ? (
                <Field label={t("servers.resetWeekday")} htmlFor="server-quota-reset-weekday">
                  <Select
                    value={values.quotaWeeklyResetWeekday}
                    onValueChange={(quotaWeeklyResetWeekday) =>
                      setValues((current) => ({
                        ...current,
                        quotaWeeklyResetWeekday: quotaWeeklyResetWeekday as ServerDownloadQuotaWeekday,
                      }))
                    }
                  >
                    <SelectTrigger id="server-quota-reset-weekday"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {(["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"] as const).map((day) => (
                        <SelectItem key={day} value={day}>{t(`servers.weekday${day}`)}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </Field>
              ) : null}
              {values.quotaPeriod === "MONTHLY" ? (
                <Field
                  label={t("servers.resetMonthDay")}
                  description={t("servers.resetMonthDayDescription")}
                  htmlFor="server-quota-reset-month-day"
                  descriptionId="server-quota-reset-month-day-description"
                >
                  <Input
                    id="server-quota-reset-month-day"
                    type="number"
                    min={1}
                    max={31}
                    step={1}
                    value={values.quotaMonthlyResetDay}
                    aria-describedby="server-quota-reset-month-day-description"
                    aria-invalid={!monthDayValid}
                    onChange={(event) =>
                      setValues((current) => ({
                        ...current,
                        quotaMonthlyResetDay: Number(event.target.value),
                      }))
                    }
                  />
                </Field>
              ) : null}
            </div>
          ) : null}

          {limitsError ? (
            <p id="server-download-limits-error" role="alert" className="text-sm text-destructive">
              {limitsError}
            </p>
          ) : null}

          {editing && runtimeQuota?.enabled ? (
            <div className={cn(
              "space-y-3 rounded-inner border p-4",
              runtimeQuotaActivelyBlocked
                ? "border-destructive/40 bg-destructive/5"
                : "border-border bg-card/50",
            )} role="status" aria-live="polite">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <div className={cn("text-sm font-semibold", runtimeQuotaActivelyBlocked && "text-destructive")}>
                    {runtimeQuota.blocked ? t("servers.quotaReached") : t("servers.quotaUsage")}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    {t("servers.quotaUsageSummary", {
                      used: formatBytes(runtimeQuota.usedBytes),
                      limit: formatBytes(runtimeQuota.limitBytes),
                      remaining: formatBytes(runtimeQuota.remainingBytes),
                    })}
                  </div>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={resettingQuota}
                  onClick={onRequestQuotaReset}
                >
                  {resettingQuota ? t("servers.resettingQuota") : t("servers.resetQuota")}
                </Button>
              </div>
              <Progress
                value={quotaProgress}
                aria-label={t("servers.quotaUsage")}
                aria-valuetext={t("servers.quotaUsageSummary", {
                  used: formatBytes(runtimeQuota.usedBytes),
                  limit: formatBytes(runtimeQuota.limitBytes),
                  remaining: formatBytes(runtimeQuota.remainingBytes),
                })}
              />
              <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-muted-foreground">
                {runtimeQuota.reservedBytes > 0 ? (
                  <span>{t("servers.quotaReserved", { bytes: formatBytes(runtimeQuota.reservedBytes) })}</span>
                ) : null}
                <span>
                  {quotaResetAt
                    ? t("servers.quotaResetsAt", { resetAt: quotaResetAt, timezone: runtimeQuota.timezoneName })
                    : t("servers.quotaManualReset")}
                </span>
              </div>
            </div>
          ) : null}
        </div>

        <ConfirmDialog
          open={showTlsWarning}
          title={t("confirm.disableTls")}
          message={t("confirm.disableTlsMessage")}
          confirmLabel={t("confirm.disableTlsConfirm")}
          cancelLabel={t("confirm.disableTlsDismiss")}
          onConfirm={confirmDisableTls}
          onCancel={() => setShowTlsWarning(false)}
        />

        {testResult ? (
          <div
            className={cn(
              "rounded-inner border p-4 text-sm",
              testResult.success
                ? "border-status-completed/30 bg-status-completed/10 text-status-completed"
                : "border-destructive/30 bg-destructive/10 text-destructive",
            )}
          >
            {testResult.success
              ? `${t("servers.testSuccess")} (${testResult.latencyMs}ms${testResult.supportsPipelining ? ", pipelining supported" : ""})`
              : `${t("servers.testFailed")}: ${testResult.message}`}
          </div>
        ) : null}

        {saveError ? (
          <div className="rounded-inner border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
            {saveError}
          </div>
        ) : null}

        <div className="flex flex-wrap gap-3">
          <Button onClick={() => void onSave(values)} disabled={!values.host.trim() || saving || limitsError != null}>
            {editing ? t("settings.save") : t("servers.addServer")}
          </Button>
          <Button variant="outline" onClick={() => void onTest(values)} disabled={!values.host.trim() || testing}>
            {testing ? t("servers.testing") : t("servers.testConnection")}
          </Button>
          <Button variant="ghost" onClick={onCancel}>
            {t("action.cancel")}
          </Button>
        </div>
      </div>
    </SectionCard>
  );
}

function Field({
  label,
  description,
  htmlFor,
  descriptionId,
  children,
}: {
  label: string;
  description?: string;
  htmlFor?: string;
  descriptionId?: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {description ? (
        <p id={descriptionId} className="text-xs text-muted-foreground">{description}</p>
      ) : null}
    </div>
  );
}

function ToggleField({
  label,
  description,
  checked,
  onCheckedChange,
}: {
  label: string;
  description?: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="space-y-1">
      <Label className="gap-3">
        <Checkbox checked={checked} onCheckedChange={(value) => onCheckedChange(value === true)} />
        {label}
      </Label>
      {description ? <p className="text-xs text-muted-foreground">{description}</p> : null}
    </div>
  );
}

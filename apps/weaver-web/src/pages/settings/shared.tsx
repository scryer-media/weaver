import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { useLocation } from "react-router";
import { useMutation, useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import {
  API_KEYS_QUERY,
  CREATE_API_KEY_MUTATION,
  DELETE_API_KEY_MUTATION,
} from "@/graphql/queries";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { FolderPathInput } from "@/components/FolderPathInput";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { saveResponseAsDownload } from "@/lib/download";

export function SettingsPageHeader({
  title,
  description,
}: {
  title: string;
  description?: string;
}) {
  return <PageHeader title={title} description={description} className="mb-6" />;
}

/** Boxed sub-card used throughout Settings for grouped fields / rows. */
export function SettingsInnerBox({
  className,
  children,
}: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cn("rounded-inner border border-border bg-background/40 p-5", className)}>
      {children}
    </div>
  );
}

type BackupStatusResponse = {
  can_restore: boolean;
  busy: boolean;
  reason?: string | null;
  pending_restore?: string | null;
  pending_restore_error?: string | null;
};

type BackupManifest = {
  format_version: number | string;
  scope: string;
  created_at_epoch_ms: number;
  source_weaver_version?: string;
  source_engine?: string;
  weaver_schema_version: number;
  included_tables: string[];
  tables?: Record<string, { rows: number; columns: string[]; checksum: string }>;
  source_paths: {
    data_dir: string;
    intermediate_dir: string;
    complete_dir: string;
  };
  encrypted: boolean;
  managed_packages?: Array<{ digest: string }>;
  notes: string[];
};

type CategoryRemapRequirement = {
  category_name: string;
  current_dest_dir: string;
};

type BackupInspectResult = {
  manifest: BackupManifest;
  required_category_remaps: CategoryRemapRequirement[];
  key_compatible: boolean;
  warnings: string[];
};

type RestoreReport = {
  staged: boolean;
  restart_required: boolean;
  pending_restore_id?: string | null;
  history_jobs: number;
  warnings: string[];
};

type ApiKeyScope = "CONTROL" | "READ" | "ADMIN";

const API_KEY_GENERATE_PARAM = "apiKeyGenerate";
const API_KEY_NAME_PARAM = "apiKeyName";
const API_KEY_SCOPE_PARAM = "apiKeyScope";
const API_KEY_GENERATE_PARAMS = [API_KEY_GENERATE_PARAM, "createApiKey"] as const;
const API_KEY_NAME_PARAMS = [API_KEY_NAME_PARAM, "name"] as const;
const API_KEY_SCOPE_PARAMS = [API_KEY_SCOPE_PARAM, "scope"] as const;

function parseApiKeyScope(value: string | null): ApiKeyScope | null {
  switch (value?.trim().toLowerCase()) {
    case "control":
    case "integration":
      return "CONTROL";
    case "read":
      return "READ";
    case "admin":
      return "ADMIN";
    default:
      return null;
  }
}

function isTruthyQueryParam(value: string | null) {
  return value === "1" || value?.trim().toLowerCase() === "true";
}

function firstQueryParam(params: URLSearchParams, names: readonly string[]) {
  for (const name of names) {
    const value = params.get(name);
    if (value !== null) {
      return value;
    }
  }
  return null;
}

function deleteQueryParams(params: URLSearchParams, names: readonly string[]) {
  for (const name of names) {
    params.delete(name);
  }
}

export function BackupRestoreSection({
  currentDataDir,
}: {
  currentDataDir: string;
}) {
  const t = useTranslate();
  const [status, setStatus] = useState<BackupStatusResponse | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [backupPassword, setBackupPassword] = useState("");
  const [backupBusy, setBackupBusy] = useState(false);
  const [backupMessage, setBackupMessage] = useState<string | null>(null);
  const [backupError, setBackupError] = useState<string | null>(null);

  const [restoreFile, setRestoreFile] = useState<File | null>(null);
  const [restorePassword, setRestorePassword] = useState("");
  const [dataDir, setDataDir] = useState(currentDataDir);
  const [restoreIntermediateDir, setRestoreIntermediateDir] = useState("");
  const [restoreCompleteDir, setRestoreCompleteDir] = useState("");
  const [inspectResult, setInspectResult] = useState<BackupInspectResult | null>(null);
  const [inspectBusy, setInspectBusy] = useState(false);
  const [restoreBusy, setRestoreBusy] = useState(false);
  const [restoreError, setRestoreError] = useState<string | null>(null);
  const [restoreMessage, setRestoreMessage] = useState<string | null>(null);
  const [restoreConfirmOpen, setRestoreConfirmOpen] = useState(false);
  const [categoryRemaps, setCategoryRemaps] = useState<Record<string, string>>({});

  useEffect(() => {
    if (!dataDir && currentDataDir) {
      setDataDir(currentDataDir);
    }
  }, [currentDataDir, dataDir]);

  useEffect(() => {
    void loadStatus();
  }, []);

  const loadStatus = async () => {
    setStatusLoading(true);
    try {
      const response = await fetch(new URL("api/backup/status", document.baseURI).href, {
        headers: authHeaders(),
      });
      const payload = (await readJsonOrThrow(response)) as BackupStatusResponse;
      setStatus(payload);
    } catch (error) {
      setStatus(null);
      setBackupError(error instanceof Error ? error.message : String(error));
    } finally {
      setStatusLoading(false);
    }
  };

  const handleDownloadBackup = async () => {
    setBackupBusy(true);
    setBackupError(null);
    setBackupMessage(null);
    try {
      const response = await fetch(new URL("api/backup/export", document.baseURI).href, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          password: backupPassword.trim() ? backupPassword : null,
        }),
      });
      if (!response.ok) {
        await throwJsonError(response);
      }
      await saveResponseAsDownload(response, `weaver_backup_${Date.now()}.enc`);
      setBackupMessage(t("settings.backupDownloadReady"));
    } catch (error) {
      setBackupError(error instanceof Error ? error.message : String(error));
    } finally {
      setBackupBusy(false);
    }
  };

  const handleAnalyzeBackup = async () => {
    if (!restoreFile) return;
    setInspectBusy(true);
    setRestoreError(null);
    setRestoreMessage(null);
    try {
      const form = new FormData();
      form.append("file", restoreFile);
      if (restorePassword.trim()) {
        form.append("password", restorePassword);
      }
      const response = await fetch(new URL("api/backup/inspect", document.baseURI).href, {
        method: "POST",
        headers: authHeaders(),
        body: form,
      });
      const payload = (await readJsonOrThrow(response)) as BackupInspectResult;
      setInspectResult(payload);
      setCategoryRemaps(
        Object.fromEntries(
          payload.required_category_remaps.map((entry) => [entry.category_name, ""]),
        ),
      );
    } catch (error) {
      setInspectResult(null);
      setRestoreError(error instanceof Error ? error.message : String(error));
    } finally {
      setInspectBusy(false);
    }
  };

  const executeRestore = async () => {
    if (!restoreFile) return;
    setRestoreBusy(true);
    setRestoreError(null);
    setRestoreMessage(null);
    try {
      const form = new FormData();
      form.append("file", restoreFile);
      if (restorePassword.trim()) {
        form.append("password", restorePassword);
      }
      form.append("data_dir", dataDir.trim());
      if (restoreIntermediateDir.trim()) {
        form.append("intermediate_dir", restoreIntermediateDir.trim());
      }
      if (restoreCompleteDir.trim()) {
        form.append("complete_dir", restoreCompleteDir.trim());
      }
      if (inspectResult?.required_category_remaps.length) {
        form.append(
          "category_remaps",
          JSON.stringify(
            inspectResult.required_category_remaps
              .map((entry) => ({
                category_name: entry.category_name,
                new_dest_dir: categoryRemaps[entry.category_name]?.trim() ?? "",
              }))
              .filter((entry) => entry.new_dest_dir.length > 0),
          ),
        );
      }

      const response = await fetch(new URL("api/backup/restore", document.baseURI).href, {
        method: "POST",
        headers: authHeaders(),
        body: form,
      });
      const payload = (await readJsonOrThrow(response)) as RestoreReport;
      setRestoreMessage(
        t("settings.restoreSuccess").replace(
          "{{count}}",
          String(payload.history_jobs ?? 0),
        ),
      );
      await loadStatus();
    } catch (error) {
      setRestoreError(error instanceof Error ? error.message : String(error));
    } finally {
      setRestoreBusy(false);
      setRestoreConfirmOpen(false);
    }
  };

  const remapRequirements = inspectResult?.required_category_remaps ?? [];
  const missingRequiredRemaps = remapRequirements.filter(
    (entry) => !(categoryRemaps[entry.category_name] ?? "").trim(),
  );
  const restoreDisabled =
    !restoreFile ||
    !inspectResult ||
    !inspectResult.key_compatible ||
    !dataDir.trim() ||
    !!statusLoading ||
    !status?.can_restore ||
    missingRequiredRemaps.length > 0 ||
    inspectBusy ||
    restoreBusy;

  return (
    <SectionCard title={t("settings.backupTitle")} description={t("settings.backupDesc")}>
      <div className="grid gap-4 2xl:grid-cols-2">
        <SettingsInnerBox>
          <h3 className="text-sm font-semibold text-foreground">
            {t("settings.backupExport")}
          </h3>
          <p className="mt-1 text-[12.5px] text-muted-foreground">
            {t("settings.backupExportDesc")}
          </p>
          <div className="mt-4 space-y-1.5">
            <Label>{t("settings.backupPassword")}</Label>
            <Input
              type="password"
              value={backupPassword}
              onChange={(e) => setBackupPassword(e.target.value)}
              placeholder={t("settings.backupPasswordPlaceholder")}
            />
          </div>
          <Button
            className="mt-3"
            onClick={handleDownloadBackup}
            disabled={backupBusy || status?.busy || !backupPassword.trim()}
          >
            {backupBusy ? t("settings.backupDownloading") : t("settings.backupDownload")}
          </Button>
          {backupMessage && <p className="mt-3 text-xs text-status-completed">{backupMessage}</p>}
          {backupError && <p className="mt-3 text-xs text-destructive">{backupError}</p>}
        </SettingsInnerBox>

        <SettingsInnerBox>
          <h3 className="text-sm font-semibold text-foreground">
            {t("settings.restoreTitle")}
          </h3>
          <p className="mt-1 text-[12.5px] text-muted-foreground">{t("settings.restoreDesc")}</p>

          <div className="mt-4 space-y-1.5">
            <Label>{t("settings.restoreFile")}</Label>
            <input
              type="file"
              accept=".enc,.tar.zst,.age,.db,.zst"
              onChange={(e) => {
                setRestoreFile(e.target.files?.[0] ?? null);
                setInspectResult(null);
                setRestoreError(null);
                setRestoreMessage(null);
              }}
              className="block w-full text-sm text-foreground file:mr-3 file:rounded-inner file:border file:border-border file:bg-card file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-foreground"
            />
          </div>

          <div className="mt-3 space-y-1.5">
            <Label>{t("settings.backupPassword")}</Label>
            <Input
              type="password"
              value={restorePassword}
              onChange={(e) => setRestorePassword(e.target.value)}
              placeholder={t("settings.restorePasswordPlaceholder")}
            />
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-2">
            <Button
              variant="secondary"
              onClick={handleAnalyzeBackup}
              disabled={!restoreFile || inspectBusy || !!status?.busy}
            >
              {inspectBusy ? t("settings.restoreAnalyzing") : t("settings.restoreAnalyze")}
            </Button>
            {statusLoading ? (
              <span className="text-xs text-muted-foreground">
                {t("label.loading")}
              </span>
            ) : (
              <span
                className={cn(
                  "text-xs",
                  status?.can_restore ? "text-muted-foreground" : "text-destructive",
                )}
              >
                {status?.can_restore
                  ? t("settings.restoreAllowed")
                  : (status?.reason ?? t("settings.restoreBlocked"))}
              </span>
            )}
          </div>

          {status?.pending_restore && (
            <div className="mt-2 space-y-1 text-xs">
              <p className="text-status-paused">
                Restore {status.pending_restore} is staged. Restart Weaver to apply it.
              </p>
              {status.pending_restore_error && (
                <p className="text-destructive">
                  The last startup attempt failed: {status.pending_restore_error}
                </p>
              )}
            </div>
          )}

          <div className="mt-3 space-y-1.5">
            <Label>{t("settings.restoreDataDir")}</Label>
            <FolderPathInput
              value={dataDir}
              onChange={setDataDir}
            />
          </div>

          <div className="mt-3 space-y-1.5">
            <Label>{t("settings.restoreIntermediateDir")}</Label>
            <FolderPathInput
              value={restoreIntermediateDir}
              onChange={setRestoreIntermediateDir}
              placeholder={`${dataDir || currentDataDir}/intermediate`}
            />
          </div>

          <div className="mt-3 space-y-1.5">
            <Label>{t("settings.restoreCompleteDir")}</Label>
            <FolderPathInput
              value={restoreCompleteDir}
              onChange={setRestoreCompleteDir}
              placeholder={`${dataDir || currentDataDir}/complete`}
            />
          </div>

          {inspectResult && (
            <div className="mt-3 rounded-inner border border-border bg-card/60 p-3">
              <div className="text-sm font-medium text-foreground">
                {t("settings.restorePreview")}
              </div>
              <div className="mt-2 space-y-1 text-xs text-muted-foreground">
                <div>
                  {t("settings.restoreSourceDataDir")}:{" "}
                  <span className="font-mono">{inspectResult.manifest.source_paths.data_dir}</span>
                </div>
                <div>
                  {t("settings.restoreSourceCompleteDir")}:{" "}
                  <span className="font-mono">
                    {inspectResult.manifest.source_paths.complete_dir}
                  </span>
                </div>
                <div>
                  {t("settings.restoreSourceIntermediateDir")}:{" "}
                  <span className="font-mono">
                    {inspectResult.manifest.source_paths.intermediate_dir}
                  </span>
                </div>
                <div>
                  {t("settings.restoreSchema")}:{" "}
                  {inspectResult.manifest.weaver_schema_version}
                </div>
                {inspectResult.manifest.source_engine && (
                  <div>Source database: {inspectResult.manifest.source_engine}</div>
                )}
                <div>
                  Exported rows:{" "}
                  {Object.values(inspectResult.manifest.tables ?? {}).reduce(
                    (total, table) => total + table.rows,
                    0,
                  )}
                </div>
                <div>
                  Managed packages: {inspectResult.manifest.managed_packages?.length ?? 0}
                </div>
                <div>
                  {t("settings.restoreEncrypted")}:{" "}
                  {inspectResult.manifest.encrypted
                    ? t("settings.restoreYes")
                    : t("settings.restoreNo")}
                </div>
                <div>
                  {t("settings.restoreCreatedAt")}:{" "}
                  {formatEpoch(inspectResult.manifest.created_at_epoch_ms)}
                </div>
              </div>
              {inspectResult.manifest.notes.length > 0 && (
                <div className="mt-3 space-y-1 text-xs text-muted-foreground">
                  {inspectResult.manifest.notes.map((note) => (
                    <div key={note}>{note}</div>
                  ))}
                </div>
              )}
              {inspectResult.warnings.length > 0 && (
                <div className="mt-3 space-y-1 text-xs text-status-paused">
                  {inspectResult.warnings.map((warning) => (
                    <div key={warning}>{warning}</div>
                  ))}
                </div>
              )}
              {!inspectResult.key_compatible && (
                <p className="mt-3 text-xs text-destructive">
                  This backup&apos;s encryption key cannot be promoted into the configured key
                  store.
                </p>
              )}
            </div>
          )}

          {remapRequirements.length > 0 && (
            <div className="mt-3 rounded-inner border border-status-paused/40 bg-status-paused/10 p-3">
              <div className="text-sm font-medium text-foreground">
                {t("settings.restoreRemapsTitle")}
              </div>
              <div className="mt-1 text-xs text-muted-foreground">
                {t("settings.restoreRemapsDesc")}
              </div>
              <div className="mt-3 space-y-3">
                {remapRequirements.map((entry) => (
                  <div key={entry.category_name}>
                    <div className="mb-1 text-xs text-muted-foreground">
                      {entry.category_name}:{" "}
                      <span className="font-mono">{entry.current_dest_dir}</span>
                    </div>
                    <FolderPathInput
                      value={categoryRemaps[entry.category_name] ?? ""}
                      onChange={(destDir) =>
                        setCategoryRemaps((current) => ({
                          ...current,
                          [entry.category_name]: destDir,
                        }))
                      }
                      placeholder={t("settings.restoreRemapPlaceholder")}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          <Button
            className="mt-3"
            variant="destructive"
            onClick={() => setRestoreConfirmOpen(true)}
            disabled={restoreDisabled}
          >
            {restoreBusy ? t("settings.restoreRunning") : t("settings.restoreButton")}
          </Button>

          {restoreMessage && <p className="mt-3 text-xs text-status-completed">{restoreMessage}</p>}
          {restoreError && <p className="mt-3 text-xs text-destructive">{restoreError}</p>}
        </SettingsInnerBox>
      </div>

      <ConfirmDialog
        open={restoreConfirmOpen}
        title={t("settings.restoreConfirmTitle")}
        message={t("settings.restoreConfirmMessage")}
        confirmLabel={t("settings.restoreButton")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => void executeRestore()}
        onCancel={() => setRestoreConfirmOpen(false)}
      />
    </SectionCard>
  );
}

export function ApiKeysSection() {
  const t = useTranslate();
  const location = useLocation();
  const [{ data }] = useQuery({ query: API_KEYS_QUERY });
  const [, createApiKey] = useMutation(CREATE_API_KEY_MUTATION);
  const [, deleteApiKey] = useMutation(DELETE_API_KEY_MUTATION);

  const [newKeyName, setNewKeyName] = useState("");
  const [newKeyScope, setNewKeyScope] = useState<ApiKeyScope>("CONTROL");
  const [createdKey, setCreatedKey] = useState<{
    rawKey: string;
    name: string;
    scope: ApiKeyScope;
  } | null>(null);
  const [createdKeyOpen, setCreatedKeyOpen] = useState(false);
  const [createBusy, setCreateBusy] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [copiedRawKey, setCopiedRawKey] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [keys, setKeys] = useState<
    {
      id: number;
      name: string;
      scope: string;
      createdAt: number;
      lastUsedAt: number | null;
    }[]
  >([]);
  const keyFieldRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (data?.apiKeys) {
      setKeys(data.apiKeys);
    }
  }, [data?.apiKeys]);

  useEffect(() => {
    if (!createdKeyOpen || !createdKey || !keyFieldRef.current) return;
    const handle = window.requestAnimationFrame(() => {
      keyFieldRef.current?.focus();
      keyFieldRef.current?.select();
    });
    return () => window.cancelAnimationFrame(handle);
  }, [createdKey, createdKeyOpen]);

  const createKey = useCallback(async (name: string, scope: ApiKeyScope) => {
    const trimmedName = name.trim();
    if (!trimmedName) return;
    setCreateBusy(true);
    setCreateError(null);
    setCopiedRawKey(false);
    const result = await createApiKey({
      name: trimmedName,
      scope,
    });
    if (result.data?.createApiKey?.rawKey) {
      setCreatedKey({
        rawKey: result.data.createApiKey.rawKey,
        name: trimmedName,
        scope,
      });
      setCreatedKeyOpen(true);
      setNewKeyName("");
      setKeys((current) => [result.data.createApiKey.key, ...current]);
    } else {
      setCreateError(t("settings.apiKeyCreateFailed"));
    }
    if (result.error) {
      setCreateError(result.error.message);
    }
    setCreateBusy(false);
  }, [createApiKey, t]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (!isTruthyQueryParam(firstQueryParam(params, API_KEY_GENERATE_PARAMS))) {
      return;
    }

    const name = firstQueryParam(params, API_KEY_NAME_PARAMS)?.trim() ?? "";
    if (!name) {
      return;
    }

    const scope = parseApiKeyScope(firstQueryParam(params, API_KEY_SCOPE_PARAMS)) ?? "CONTROL";
    setNewKeyName(name);
    setNewKeyScope(scope);

    deleteQueryParams(params, API_KEY_GENERATE_PARAMS);
    deleteQueryParams(params, API_KEY_NAME_PARAMS);
    deleteQueryParams(params, API_KEY_SCOPE_PARAMS);

    const nextSearch = params.toString();
    const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ""}${window.location.hash}`;

    // Consume the deep link once so refresh doesn't silently mint another key.
    window.history.replaceState(window.history.state, "", nextUrl);
    void createKey(name, scope);
  }, [createKey, location.pathname, location.search]);

  const handleCreate = async () => {
    await createKey(newKeyName.trim(), newKeyScope);
  };

  const handleCopy = () => {
    if (createdKey) {
      void navigator.clipboard.writeText(createdKey.rawKey);
      setCopiedRawKey(true);
      window.setTimeout(() => setCopiedRawKey(false), 2000);
    }
  };

  const handleKeyFieldFocus = () => {
    if (keyFieldRef.current) {
      keyFieldRef.current.select();
    }
  };

  const handleDelete = (id: number) => {
    void deleteApiKey({ id }).then((result) => {
      if (result.error) {
        setCreateError(result.error.message);
        return;
      }
      if (result.data?.deleteApiKey) {
        setKeys(result.data.deleteApiKey);
      }
    });
    setDeleteConfirmId(null);
  };

  const formatTime = (ms: number) => {
    const d = new Date(ms);
    return d.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  return (
    <SectionCard title={t("settings.apiKeys")} description={t("settings.apiKeysDesc")}>
      <div className="space-y-5">
      {keys.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {t("settings.apiKeyNone")}
        </p>
      ) : (
        <div className="space-y-2">
          {keys.map(
            (key: {
              id: number;
              name: string;
              scope: string;
              createdAt: number;
              lastUsedAt: number | null;
            }) => (
              <div
                key={key.id}
                className="flex items-center justify-between gap-4 rounded-inner border border-border px-4 py-3"
              >
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-semibold text-foreground">{key.name}</div>
                  <div className="mt-1 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
                    <span className="rounded-chip bg-muted px-1.5 py-0.5 text-[10.5px] font-semibold uppercase tracking-[0.1em]">
                      {key.scope === "ADMIN"
                        ? t("settings.scopeAdmin")
                        : key.scope === "READ"
                          ? t("settings.scopeRead")
                          : t("settings.scopeIntegration")}
                    </span>
                    <span>
                      {t("settings.apiKeyLastUsed")}:{" "}
                      {key.lastUsedAt
                        ? formatTime(key.lastUsedAt)
                        : t("settings.apiKeyNeverUsed")}
                    </span>
                  </div>
                </div>
                <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(key.id)}>
                  {t("action.delete")}
                </Button>
              </div>
            ),
          )}
        </div>
      )}

      <div className="space-y-4">
        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-52 flex-1">
            <Label className="mb-2">{t("settings.apiKeyName")}</Label>
            <Input
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              placeholder={t("settings.apiKeyNamePlaceholder")}
              onKeyDown={(e) => e.key === "Enter" && void handleCreate()}
            />
          </div>
          <div>
            <Label className="mb-2">{t("settings.apiKeyScope")}</Label>
            <Select
              value={newKeyScope}
              onValueChange={(value) =>
                setNewKeyScope(value as ApiKeyScope)
              }
            >
              <SelectTrigger className="min-w-44">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="CONTROL">{t("settings.scopeIntegration")}</SelectItem>
                <SelectItem value="READ">Read</SelectItem>
                <SelectItem value="ADMIN">{t("settings.scopeAdmin")}</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Button
            onClick={() => void handleCreate()}
            disabled={!newKeyName.trim() || createBusy}
          >
            {createBusy ? t("settings.apiKeyCreating") : t("settings.createApiKey")}
          </Button>
        </div>

        {createError ? (
          <p className="text-sm text-destructive">{createError}</p>
        ) : null}
      </div>

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteApiKey")}
        message={t("confirm.deleteApiKeyMessage")}
        confirmLabel={t("confirm.deleteApiKeyConfirm")}
        cancelLabel={t("confirm.deleteApiKeyDismiss")}
        onConfirm={() => deleteConfirmId != null && handleDelete(deleteConfirmId)}
        onCancel={() => setDeleteConfirmId(null)}
      />

      <Dialog open={createdKeyOpen} onOpenChange={setCreatedKeyOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>{t("settings.apiKeyCreated")}</DialogTitle>
            <DialogDescription>{t("settings.apiKeyCopyWarning")}</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="grid gap-1 text-xs text-muted-foreground sm:grid-cols-2">
              <div>
                <span className="font-medium text-foreground">{t("settings.apiKeyName")}:</span>{" "}
                {createdKey?.name ?? ""}
              </div>
              <div>
                <span className="font-medium text-foreground">{t("settings.apiKeyScope")}:</span>{" "}
                {createdKey?.scope === "ADMIN"
                  ? t("settings.scopeAdmin")
                  : createdKey?.scope === "READ"
                    ? t("settings.scopeRead")
                  : t("settings.scopeIntegration")}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Input
                ref={keyFieldRef}
                readOnly
                value={createdKey?.rawKey ?? ""}
                onFocus={handleKeyFieldFocus}
                onClick={handleKeyFieldFocus}
                className="font-mono text-xs"
              />
              <Button
                type="button"
                variant="outline"
                size="icon"
                onClick={handleCopy}
                aria-label={t("settings.apiKeyCopyRawKey")}
                title={t("settings.apiKeyCopyRawKey")}
              >
                {copiedRawKey ? <CheckIcon className="size-4" /> : <CopyIcon className="size-4" />}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              {t("settings.apiKeyCopyShortcutHint")}
            </p>
          </div>
        </DialogContent>
      </Dialog>
      </div>
    </SectionCard>
  );
}

async function readJsonOrThrow(response: Response) {
  if (!response.ok) {
    await throwJsonError(response);
  }
  return response.json();
}

async function throwJsonError(response: Response): Promise<never> {
  try {
    const payload = (await response.json()) as { error?: string };
    throw new Error(payload.error || `Request failed with status ${response.status}`);
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(`Request failed with status ${response.status}`, { cause: error });
  }
}

function formatEpoch(ms: number): string {
  return new Date(ms).toLocaleString();
}

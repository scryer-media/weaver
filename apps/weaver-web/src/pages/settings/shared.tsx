import { useEffect, useRef, useState } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { useMutation, useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import {
  API_KEYS_QUERY,
  CREATE_API_KEY_MUTATION,
  DELETE_API_KEY_MUTATION,
} from "@/graphql/queries";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { PageHeader } from "@/components/PageHeader";
import { useTranslate } from "@/lib/context/translate-context";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
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

export function SettingsPageHeader({
  title,
  description,
}: {
  title: string;
  description?: string;
}) {
  return <PageHeader title={title} description={description} className="mb-6" />;
}

type BackupStatusResponse = {
  can_restore: boolean;
  busy: boolean;
  reason?: string | null;
};

type BackupManifest = {
  format_version: number;
  scope: string;
  created_at_epoch_ms: number;
  weaver_schema_version: number;
  included_tables: string[];
  source_paths: {
    data_dir: string;
    intermediate_dir: string;
    complete_dir: string;
  };
  encrypted: boolean;
  notes: string[];
};

type CategoryRemapRequirement = {
  category_name: string;
  current_dest_dir: string;
};

type BackupInspectResult = {
  manifest: BackupManifest;
  required_category_remaps: CategoryRemapRequirement[];
};

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
      const response = await fetch("/admin/backup/status", {
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
      const response = await fetch("/admin/backup/export", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          password: backupPassword.trim() ? backupPassword.trim() : null,
        }),
      });
      if (!response.ok) {
        await throwJsonError(response);
      }
      const blob = await response.blob();
      const filename =
        parseAttachmentFilename(response.headers.get("content-disposition")) ??
        `weaver_backup_${Date.now()}.tar.zst`;
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
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
        form.append("password", restorePassword.trim());
      }
      const response = await fetch("/admin/backup/inspect", {
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
        form.append("password", restorePassword.trim());
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

      const response = await fetch("/admin/backup/restore", {
        method: "POST",
        headers: authHeaders(),
        body: form,
      });
      const payload = (await readJsonOrThrow(response)) as { history_jobs: number };
      setRestoreMessage(
        t("settings.restoreSuccess").replace(
          "{{count}}",
          String(payload.history_jobs ?? 0),
        ),
      );
      setTimeout(() => window.location.reload(), 1200);
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
    !dataDir.trim() ||
    !!statusLoading ||
    !status?.can_restore ||
    missingRequiredRemaps.length > 0 ||
    inspectBusy ||
    restoreBusy;

  return (
    <section className="rounded-2xl border border-border bg-card p-5 shadow-sm sm:p-6">
      <div className="mb-5 max-w-3xl">
        <h2 className="text-lg font-semibold text-card-foreground">
          {t("settings.backupTitle")}
        </h2>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          {t("settings.backupDesc")}
        </p>
      </div>

      <div className="grid gap-4 2xl:grid-cols-2">
        <div className="rounded-xl border border-border bg-background/60 p-4">
          <h3 className="mb-2 text-sm font-semibold text-foreground">
            {t("settings.backupExport")}
          </h3>
          <p className="mb-3 text-xs text-muted-foreground">
            {t("settings.backupExportDesc")}
          </p>
          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.backupPassword")}
          </label>
          <input
            type="password"
            value={backupPassword}
            onChange={(e) => setBackupPassword(e.target.value)}
            placeholder={t("settings.backupPasswordPlaceholder")}
            className="mb-3 w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
          />
          <button
            onClick={handleDownloadBackup}
            disabled={backupBusy || status?.busy}
            className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            {backupBusy ? t("settings.backupDownloading") : t("settings.backupDownload")}
          </button>
          {backupMessage && <p className="mt-3 text-xs text-green-600">{backupMessage}</p>}
          {backupError && <p className="mt-3 text-xs text-destructive">{backupError}</p>}
        </div>

        <div className="rounded-xl border border-border bg-background/60 p-4">
          <h3 className="mb-2 text-sm font-semibold text-foreground">
            {t("settings.restoreTitle")}
          </h3>
          <p className="mb-3 text-xs text-muted-foreground">{t("settings.restoreDesc")}</p>

          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.restoreFile")}
          </label>
          <input
            type="file"
            accept=".tar.zst,.age,.db,.zst"
            onChange={(e) => {
              setRestoreFile(e.target.files?.[0] ?? null);
              setInspectResult(null);
              setRestoreError(null);
              setRestoreMessage(null);
            }}
            className="mb-3 block w-full text-sm text-foreground"
          />

          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.backupPassword")}
          </label>
          <input
            type="password"
            value={restorePassword}
            onChange={(e) => setRestorePassword(e.target.value)}
            placeholder={t("settings.restorePasswordPlaceholder")}
            className="mb-3 w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
          />

          <div className="mb-3 flex flex-wrap gap-2">
            <button
              onClick={handleAnalyzeBackup}
              disabled={!restoreFile || inspectBusy || !!status?.busy}
              className="rounded-md bg-secondary px-4 py-2 text-sm font-medium text-secondary-foreground hover:bg-secondary/80 disabled:opacity-50"
            >
              {inspectBusy ? t("settings.restoreAnalyzing") : t("settings.restoreAnalyze")}
            </button>
            {statusLoading ? (
              <span className="self-center text-xs text-muted-foreground">
                {t("label.loading")}
              </span>
            ) : (
              <span
                className={`self-center text-xs ${
                  status?.can_restore ? "text-muted-foreground" : "text-destructive"
                }`}
              >
                {status?.can_restore
                  ? t("settings.restoreAllowed")
                  : (status?.reason ?? t("settings.restoreBlocked"))}
              </span>
            )}
          </div>

          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.restoreDataDir")}
          </label>
          <input
            type="text"
            value={dataDir}
            onChange={(e) => setDataDir(e.target.value)}
            className="mb-3 w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
          />

          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.restoreIntermediateDir")}
          </label>
          <input
            type="text"
            value={restoreIntermediateDir}
            onChange={(e) => setRestoreIntermediateDir(e.target.value)}
            placeholder={`${dataDir || currentDataDir}/intermediate`}
            className="mb-3 w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
          />

          <label className="mb-1 block text-xs text-muted-foreground">
            {t("settings.restoreCompleteDir")}
          </label>
          <input
            type="text"
            value={restoreCompleteDir}
            onChange={(e) => setRestoreCompleteDir(e.target.value)}
            placeholder={`${dataDir || currentDataDir}/complete`}
            className="mb-3 w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
          />

          {inspectResult && (
            <div className="mb-3 rounded-md border border-border bg-field/40 p-3">
              <div className="mb-2 text-sm font-medium text-foreground">
                {t("settings.restorePreview")}
              </div>
              <div className="space-y-1 text-xs text-muted-foreground">
                <div>
                  {t("settings.restoreSourceDataDir")}:{" "}
                  {inspectResult.manifest.source_paths.data_dir}
                </div>
                <div>
                  {t("settings.restoreSourceCompleteDir")}:{" "}
                  {inspectResult.manifest.source_paths.complete_dir}
                </div>
                <div>
                  {t("settings.restoreSourceIntermediateDir")}:{" "}
                  {inspectResult.manifest.source_paths.intermediate_dir}
                </div>
                <div>
                  {t("settings.restoreSchema")}:{" "}
                  {inspectResult.manifest.weaver_schema_version}
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
            </div>
          )}

          {remapRequirements.length > 0 && (
            <div className="mb-3 rounded-md border border-amber-500/40 bg-amber-500/10 p-3">
              <div className="mb-2 text-sm font-medium text-foreground">
                {t("settings.restoreRemapsTitle")}
              </div>
              <div className="mb-2 text-xs text-muted-foreground">
                {t("settings.restoreRemapsDesc")}
              </div>
              <div className="space-y-3">
                {remapRequirements.map((entry) => (
                  <div key={entry.category_name}>
                    <div className="mb-1 text-xs text-muted-foreground">
                      {entry.category_name}: {entry.current_dest_dir}
                    </div>
                    <input
                      type="text"
                      value={categoryRemaps[entry.category_name] ?? ""}
                      onChange={(e) =>
                        setCategoryRemaps((current) => ({
                          ...current,
                          [entry.category_name]: e.target.value,
                        }))
                      }
                      placeholder={t("settings.restoreRemapPlaceholder")}
                      className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          <button
            onClick={() => setRestoreConfirmOpen(true)}
            disabled={restoreDisabled}
            className="rounded-md bg-destructive px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50"
          >
            {restoreBusy ? t("settings.restoreRunning") : t("settings.restoreButton")}
          </button>

          {restoreMessage && <p className="mt-3 text-xs text-green-600">{restoreMessage}</p>}
          {restoreError && <p className="mt-3 text-xs text-destructive">{restoreError}</p>}
        </div>
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
    </section>
  );
}

export function ApiKeysSection() {
  const t = useTranslate();
  const [{ data }] = useQuery({ query: API_KEYS_QUERY });
  const [, createApiKey] = useMutation(CREATE_API_KEY_MUTATION);
  const [, deleteApiKey] = useMutation(DELETE_API_KEY_MUTATION);

  const [newKeyName, setNewKeyName] = useState("");
  const [newKeyScope, setNewKeyScope] = useState<"INTEGRATION" | "ADMIN">(
    "INTEGRATION",
  );
  const [createdKey, setCreatedKey] = useState<{
    rawKey: string;
    name: string;
    scope: "INTEGRATION" | "ADMIN";
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

  const createKey = async (
    name: string,
    scope: "INTEGRATION" | "ADMIN",
  ) => {
    if (!name.trim()) return;
    setCreateBusy(true);
    setCreateError(null);
    setCopiedRawKey(false);
    const result = await createApiKey({
      name: name.trim(),
      scope,
    });
    if (result.data?.createApiKey?.rawKey) {
      setCreatedKey({
        rawKey: result.data.createApiKey.rawKey,
        name: name.trim(),
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
  };

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
    <Card>
      <CardHeader>
        <CardTitle>{t("settings.apiKeys")}</CardTitle>
        <CardDescription>{t("settings.apiKeysDesc")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
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
                className="flex items-center justify-between rounded-xl border border-border/70 px-3 py-3"
              >
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-medium text-foreground">{key.name}</div>
                  <div className="flex gap-3 text-xs text-muted-foreground">
                    <span className="rounded bg-muted px-1.5 py-0.5">
                      {key.scope === "ADMIN"
                        ? t("settings.scopeAdmin")
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
                setNewKeyScope(value as "INTEGRATION" | "ADMIN")
              }
            >
              <SelectTrigger className="min-w-44">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="INTEGRATION">{t("settings.scopeIntegration")}</SelectItem>
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
      </CardContent>
    </Card>
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
    throw new Error(`Request failed with status ${response.status}`);
  }
}

function parseAttachmentFilename(contentDisposition: string | null): string | null {
  if (!contentDisposition) return null;
  const match = /filename="?([^"]+)"?/.exec(contentDisposition);
  return match?.[1] ?? null;
}

function formatEpoch(ms: number): string {
  return new Date(ms).toLocaleString();
}

import { useEffect, useState } from "react";
import { useMutation, useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { BACKUP_LIBRARY_QUERY, UPDATE_BACKUP_SETTINGS_MUTATION, UPDATE_AUTO_BACKUP_SETTINGS_MUTATION, DELETE_BACKUP_MUTATION, BACKUP_DOWNLOAD_TOKEN_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { saveResponseAsDownload } from "@/lib/download";
import { SecondaryButton, DangerButton } from "../../../components/controls";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { Cell } from "../../../components/rows";
import { formatDate } from "../../../data/format";
import { SettingsBlocks, type SettingsBlock } from "../framework";
import { useBackupAdminAction } from "./useBackupAdminAction";

interface BackupInfo {
  filename: string; sizeBytes: number; createdAt: string; sourceWeaverVersion: string;
  trigger: "MANUAL" | "AUTO"; status: "CREATING" | "READY" | "FAILED"; error: string | null;
}
interface AutoSettings { enabled: boolean; dailyTimeLocal: string; autoBackupKeyPresent: boolean; nextRunAt: string | null }
interface BackupLibrary { backups: BackupInfo[]; backupSettings: { customBackupPath: string | null; backupPath: string }; autoBackupSettings: AutoSettings }

export function StoredBackups({ generation }: { generation: number }) {
  const t = useTranslate();
  const [{ data, error: queryError }, refresh] = useQuery<BackupLibrary>({ query: BACKUP_LIBRARY_QUERY, requestPolicy: "network-only" });
  const [, updatePath] = useMutation(UPDATE_BACKUP_SETTINGS_MUTATION);
  const [, updateAuto] = useMutation(UPDATE_AUTO_BACKUP_SETTINGS_MUTATION);
  const [, deleteBackup] = useMutation(DELETE_BACKUP_MUTATION);
  const [, downloadToken] = useMutation(BACKUP_DOWNLOAD_TOKEN_MUTATION);
  const [path, setPath] = useState<string | null>(null);
  const [auto, setAuto] = useState<AutoSettings | null>(null);
  const [key, setKey] = useState("");
  const [clearKey, setClearKey] = useState(false);
  const { run, busy, error, reauthentication } = useBackupAdminAction(() => refresh({ requestPolicy: "network-only" }));
  const [deleting, setDeleting] = useState<string | null>(null);
  const settings = auto ?? data?.autoBackupSettings;
  const creating = data?.backups.some((backup) => backup.status === "CREATING") ?? false;
  const automaticEnabled = data?.autoBackupSettings.enabled ?? false;
  useEffect(() => { refresh({ requestPolicy: "network-only" }); }, [generation, refresh]);
  useEffect(() => {
    if (!creating && !automaticEnabled) return;
    const timer = window.setInterval(() => refresh({ requestPolicy: "network-only" }), creating ? 2000 : 30_000);
    return () => window.clearInterval(timer);
  }, [creating, automaticEnabled, refresh]);
  useEffect(() => {
    const onFocus = () => refresh({ requestPolicy: "network-only" });
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [refresh]);

  const patch = (value: Partial<AutoSettings>) => { if (settings) setAuto({ ...settings, ...value }); };
  const blocks: SettingsBlock[] = [
    { kind: "section", id: "backupStorage", title: t("next.backup.storage"), fields: [
      { id: "backupPath", label: t("next.backup.customPath"), help: data?.backupSettings.backupPath, control: { kind: "path", value: path ?? data?.backupSettings.customBackupPath ?? "", onChange: setPath } },
      { id: "saveBackupPath", label: t("next.backup.saveStorage"), control: { kind: "custom", control: <SecondaryButton disabled={busy || path === null} onClick={() => void run(async () => { const result = await updatePath({ path: path?.trim() || null }); if (result.error) throw result.error; setPath(null); })}>{t("next.settings.saveChanges")}</SecondaryButton> } },
    ] },
    { kind: "section", id: "automaticBackups", title: t("next.backup.automatic"), note: t("next.backup.retention"), fields: [
      { id: "autoEnabled", label: t("next.backup.autoEnabled"), control: { kind: "toggle", value: settings?.enabled ?? false, onChange: (enabled) => patch({ enabled }) } },
      { id: "autoTime", label: t("next.backup.dailyTime"), control: { kind: "time", value: settings?.dailyTimeLocal ?? "03:00", onChange: (dailyTimeLocal) => patch({ dailyTimeLocal }) } },
      { id: "autoKey", label: t("next.backup.autoKey"), help: t(data?.autoBackupSettings.autoBackupKeyPresent ? "next.backup.keyPresent" : "next.backup.keyAbsent"), control: { kind: "text", type: "password", value: key, onChange: setKey } },
      { id: "clearAutoKey", label: t("next.backup.clearKey"), control: { kind: "toggle", value: clearKey, onChange: setClearKey } },
      { id: "nextAutoRun", label: t("next.backup.nextRun"), control: { kind: "custom", control: <span>{data?.autoBackupSettings.nextRunAt ? formatDate(Date.parse(data.autoBackupSettings.nextRunAt)) : t("next.backup.notScheduled")}</span> } },
      { id: "saveAuto", label: t("next.backup.saveAutomatic"), control: { kind: "custom", control: <SecondaryButton disabled={busy || !settings} onClick={() => void run(async () => {
        if (!settings) return;
        const result = await updateAuto({ input: { enabled: settings.enabled, dailyTimeLocal: settings.dailyTimeLocal, setAutoBackupKey: key || null, clearAutoBackupKey: clearKey } });
        if (result.error) throw result.error;
        setAuto(null); setKey(""); setClearKey(false);
      })}>{t("next.settings.saveChanges")}</SecondaryButton> } },
    ] },
    { kind: "table", id: "storedBackups", title: t("next.backup.stored"), columns: "minmax(140px, 1fr) 90px 90px 100px 90px 180px", headers: [t("next.backup.taken"), t("next.backup.trigger"), t("next.backup.status"), t("next.backup.size"), t("next.backup.version"), t("next.backup.actions")], rows: (data?.backups ?? []).map((backup) => ({ id: backup.filename, searchText: `${backup.filename} ${backup.trigger} ${backup.status} ${backup.sourceWeaverVersion}`, cells: [
      <Cell key="created" title={backup.filename}>{formatDate(Date.parse(backup.createdAt))}</Cell>,
      <Cell key="trigger">{t(backup.trigger === "AUTO" ? "next.backup.triggerAuto" : "next.backup.triggerManual")}</Cell>,
      <Cell key="status" title={backup.error ?? undefined}>{t(backup.status === "READY" ? "next.backup.ready" : backup.status === "CREATING" ? "next.backup.creating" : "next.backup.failed")}{backup.error ? <span className="text-wv-error-text">: {backup.error}</span> : null}</Cell>,
      <Cell key="size">{(backup.sizeBytes / 1024 / 1024).toFixed(2)} MiB</Cell>,
      <Cell key="version">{backup.sourceWeaverVersion}</Cell>,
      <div key="actions" className="flex gap-2"><SecondaryButton disabled={busy || backup.status !== "READY"} onClick={() => void run(async () => {
        const token = await downloadToken({ filename: backup.filename });
        if (token.error) throw token.error;
        const url = new URL(`api/backup/download/${encodeURIComponent(backup.filename)}`, document.baseURI);
        const response = await fetch(url, { headers: { ...authHeaders(), "X-Weaver-Backup-Token": token.data.createBackupDownloadToken } });
        if (!response.ok) throw new Error(t("next.backup.requestFailed", { status: response.status }));
        await saveResponseAsDownload(response, backup.filename);
      })}>{t("next.backup.downloadStored")}</SecondaryButton><DangerButton disabled={busy || backup.status === "CREATING"} onClick={() => setDeleting(backup.filename)}>{t("next.backup.deleteStored")}</DangerButton></div>,
    ] })) },
  ];
  return <>
    {error || queryError ? <p role="alert" className="p-4 text-wv-error-text">{error ?? queryError?.message}</p> : null}
    <SettingsBlocks blocks={blocks} />
    <ConfirmDialog open={deleting !== null} title={t("next.backup.deleteStored")} note={deleting ?? undefined} body={t("next.backup.deleteBody")} busy={busy} confirmLabel={t("next.backup.deleteStored")} onDismiss={() => setDeleting(null)} onConfirm={() => void run(async () => { const result = await deleteBackup({ filename: deleting }); if (result.error) throw result.error; setDeleting(null); })} />
    {reauthentication}
  </>;
}

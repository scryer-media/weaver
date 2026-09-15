import { useEffect, useRef, useState } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { SETTINGS_QUERY } from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { saveResponseAsDownload } from "@/lib/download";
import { KeyValueRow } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { DangerButton, SecondaryButton } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { formatDate } from "../../../data/format";
import { countLabel } from "../../../i18n/labels";
import { PathField } from "../../../features/DirectoryBrowserDialog";
import {
  PanelControls,
  SettingsBlocks,
  usePanelStatus,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Backup: take an encrypted snapshot of everything weaver knows, and put one
 * back.
 *
 * The only panel that talks to the REST API rather than GraphQL, because that
 * is where the archive endpoints live — a backup is a file, not a field.
 * Restoring is staged: the daemon writes the archive aside and applies it on
 * its next start, so the button says so.
 */

interface BackupStatus {
  can_restore: boolean;
  busy: boolean;
  reason?: string | null;
  pending_restore?: string | null;
  pending_restore_error?: string | null;
}

interface BackupManifest {
  format_version: number | string;
  scope: string;
  created_at_epoch_ms: number;
  source_weaver_version?: string;
  source_engine?: string;
  weaver_schema_version: number;
  included_tables: string[];
  tables?: Record<string, { rows: number; columns: string[]; checksum: string }>;
  source_paths: { data_dir: string; intermediate_dir: string; complete_dir: string };
  encrypted: boolean;
  notes: string[];
}

interface InspectResult {
  manifest: BackupManifest;
  required_category_remaps: { category_name: string; current_dest_dir: string }[];
  key_compatible: boolean;
  warnings: string[];
}

interface RestoreReport {
  staged: boolean;
  restart_required: boolean;
  pending_restore_id?: string | null;
  history_jobs: number;
  warnings: string[];
}

function endpoint(path: string): string {
  return new URL(path, document.baseURI).href;
}

async function readJsonOrThrow(t: Translate, response: Response): Promise<unknown> {
  if (!response.ok) {
    await throwJsonError(t, response);
  }
  return response.json();
}

async function throwJsonError(t: Translate, response: Response): Promise<never> {
  const fallback = t("next.backup.requestFailed", { status: response.status });
  try {
    const payload = (await response.json()) as { error?: string };
    throw new Error(payload.error || fallback);
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(fallback, { cause: error });
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function BackupPanel() {
  const t = useTranslate();
  const [{ data, fetching }] = useQuery<{ settings: { dataDir: string } }>({ query: SETTINGS_QUERY });
  const currentDataDir = data?.settings?.dataDir ?? "";

  const [status, setStatus] = useState<BackupStatus | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");

  const fileRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [restorePassword, setRestorePassword] = useState("");
  const [dataDir, setDataDir] = useState("");
  const [intermediateDir, setIntermediateDir] = useState("");
  const [completeDir, setCompleteDir] = useState("");
  const [inspected, setInspected] = useState<InspectResult | null>(null);
  const [remaps, setRemaps] = useState<Record<string, string>>({});
  const [confirmRestore, setConfirmRestore] = useState(false);

  usePanelStatus(error ?? note, error !== null);

  useEffect(() => {
    setDataDir((current) => current || currentDataDir);
  }, [currentDataDir]);

  const loadStatus = async () => {
    try {
      setStatus((await readJsonOrThrow(t, await fetch(endpoint("api/backup/status"), {
        headers: authHeaders(),
      }))) as BackupStatus);
    } catch (failure) {
      setStatus(null);
      setError(message(failure));
    }
  };

  useEffect(() => {
    void loadStatus();
    // The status is read once when the panel opens; every action that can
    // change it reads it again itself.
    // eslint-disable-next-line react-hooks/exhaustive-deps -- a language change must not re-read the status
  }, []);

  const mismatch = passwordConfirm !== "" && passwordConfirm !== password;

  const download = async () => {
    setBusy(true);
    setError(null);
    setNote(t("next.backup.building"));
    try {
      const response = await fetch(endpoint("api/backup/export"), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ password: password.trim() ? password : null }),
      });
      if (!response.ok) {
        await throwJsonError(t, response);
      }
      const filename = await saveResponseAsDownload(response, `weaver_backup_${Date.now()}.enc`);
      setNote(t("next.backup.savedFile", { name: filename }));
    } catch (failure) {
      setNote(null);
      setError(message(failure));
    } finally {
      setBusy(false);
    }
  };

  const inspect = async () => {
    if (!file) {
      return;
    }
    setBusy(true);
    setError(null);
    setNote(t("next.backup.reading"));
    try {
      const form = new FormData();
      form.append("file", file);
      if (restorePassword.trim()) {
        form.append("password", restorePassword);
      }
      if (dataDir.trim()) {
        form.append("data_dir", dataDir.trim());
      }
      const result = (await readJsonOrThrow(
        t,
        await fetch(endpoint("api/backup/inspect"), {
          method: "POST",
          headers: authHeaders(),
          body: form,
        }),
      )) as InspectResult;
      setInspected(result);
      setRemaps(
        Object.fromEntries(result.required_category_remaps.map((entry) => [entry.category_name, ""])),
      );
      setNote(
        result.required_category_remaps.length > 0
          ? t("next.backup.readNeedsRemaps")
          : t("next.backup.readDone"),
      );
    } catch (failure) {
      setInspected(null);
      setNote(null);
      setError(message(failure));
    } finally {
      setBusy(false);
    }
  };

  const restore = async () => {
    if (!file) {
      return;
    }
    setConfirmRestore(false);
    setBusy(true);
    setError(null);
    setNote(t("next.backup.staging"));
    try {
      const form = new FormData();
      form.append("file", file);
      if (restorePassword.trim()) {
        form.append("password", restorePassword);
      }
      form.append("data_dir", dataDir.trim());
      if (intermediateDir.trim()) {
        form.append("intermediate_dir", intermediateDir.trim());
      }
      if (completeDir.trim()) {
        form.append("complete_dir", completeDir.trim());
      }
      const required = inspected?.required_category_remaps ?? [];
      if (required.length > 0) {
        form.append(
          "category_remaps",
          JSON.stringify(
            required
              .map((entry) => ({
                category_name: entry.category_name,
                new_dest_dir: remaps[entry.category_name]?.trim() ?? "",
              }))
              .filter((entry) => entry.new_dest_dir.length > 0),
          ),
        );
      }
      const report = (await readJsonOrThrow(
        t,
        await fetch(endpoint("api/backup/restore"), {
          method: "POST",
          headers: authHeaders(),
          body: form,
        }),
      )) as RestoreReport;
      setNote(
        report.restart_required
          ? countLabel(t, "next.backup.staged", report.history_jobs)
          : countLabel(t, "next.backup.restored", report.history_jobs),
      );
      await loadStatus();
    } catch (failure) {
      setNote(null);
      setError(message(failure));
    } finally {
      setBusy(false);
    }
  };

  const missingRemaps = (inspected?.required_category_remaps ?? []).filter(
    (entry) => !(remaps[entry.category_name] ?? "").trim(),
  );
  const restoreBlocked =
    !file
    || !inspected
    || !inspected.key_compatible
    || !dataDir.trim()
    || !status?.can_restore
    || missingRemaps.length > 0
    || busy;

  const manifest = inspected?.manifest;
  const exportedRows = Object.values(manifest?.tables ?? {}).reduce(
    (total, table) => total + table.rows,
    0,
  );

  const baseDir = dataDir || currentDataDir || t("next.backup.theDataDirectory");

  const restoreFields: FieldSpec[] = [
    {
      id: "file",
      label: t("next.backup.archive"),
      help: t("next.backup.archiveHelp"),
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 flex-wrap items-center gap-3">
            <span className="max-w-[220px] truncate font-wv-mono text-[11.5px] text-wv-muted">
              {file ? file.name : t("next.backup.noFile")}
            </span>
            <input
              ref={fileRef}
              type="file"
              accept=".enc,.tar.zst,.age,.db,.zst"
              className="hidden"
              onChange={(event) => {
                setFile(event.target.files?.[0] ?? null);
                setInspected(null);
                setError(null);
                setNote(null);
              }}
            />
            <SecondaryButton icon="chooseFile" onClick={() => fileRef.current?.click()}>
              {t("next.backup.chooseFile")}
            </SecondaryButton>
          </div>
        ),
      },
    },
    {
      id: "restorePassword",
      label: t("next.backup.password"),
      help: t("next.backup.restorePasswordHelp"),
      control: {
        kind: "text",
        type: "password",
        value: restorePassword,
        onChange: setRestorePassword,
      },
    },
    {
      id: "dataDir",
      label: t("next.settings.dataDirectory"),
      help: t("next.backup.dataDirHelp"),
      keywords: dataDir,
      control: { kind: "path", value: dataDir, onChange: setDataDir },
    },
    {
      id: "intermediateDir",
      label: t("next.backup.intermediateDir"),
      help: t("next.backup.blankUses", { path: `${baseDir}/intermediate` }),
      control: {
        kind: "path",
        value: intermediateDir,
        placeholder: `${dataDir || currentDataDir}/intermediate`,
        onChange: setIntermediateDir,
      },
    },
    {
      id: "completeDir",
      label: t("next.general.completeDir"),
      help: t("next.backup.blankUses", { path: `${baseDir}/complete` }),
      control: {
        kind: "path",
        value: completeDir,
        placeholder: `${dataDir || currentDataDir}/complete`,
        onChange: setCompleteDir,
      },
    },
    {
      id: "inspect",
      label: t("next.backup.readTheArchive"),
      help: t("next.backup.readTheArchiveHelp"),
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 flex-wrap items-center gap-3">
            <span className="min-w-0 font-wv-mono text-[11.5px] text-wv-muted">
              {status === null
                ? t("next.backup.checking")
                : status.can_restore
                  ? t("next.backup.restoreAvailable")
                  : (status.reason ?? t("next.backup.restoreUnavailable"))}
            </span>
            <SecondaryButton icon="inspectFile" disabled={!file || busy} onClick={() => void inspect()}>
              {t("next.backup.readArchive")}
            </SecondaryButton>
          </div>
        ),
      },
    },
    {
      id: "restore",
      label: t("next.backup.restore"),
      help: t("next.backup.restoreHelp"),
      control: {
        kind: "custom",
        control: (
          <DangerButton
            icon="restore"
            disabled={restoreBlocked}
            onClick={() => setConfirmRestore(true)}
          >
            {t("next.backup.restoreFromArchive")}
          </DangerButton>
        ),
      },
    },
  ];

  const blocks: (SettingsBlock | null)[] = [
    {
      kind: "section",
      id: "export",
      title: t("next.settings.panel.backup"),
      note: t("next.backup.exportNote"),
      fields: [
        {
          id: "password",
          label: t("next.backup.password"),
          help: t("next.backup.passwordHelp"),
          control: { kind: "text", type: "password", value: password, onChange: setPassword },
        },
        {
          id: "passwordConfirm",
          label: t("next.backup.confirmPassword"),
          help: mismatch ? t("next.security.passwordsMismatch") : undefined,
          control: {
            kind: "text",
            type: "password",
            value: passwordConfirm,
            onChange: setPasswordConfirm,
          },
        },
      ],
    },
    status?.pending_restore
      ? {
          kind: "custom",
          id: "pending",
          title: t("next.backup.stagedRestore"),
          note: status.pending_restore,
          searchText: "pending staged restore restart",
          body: (
            <div className="flex flex-col">
              <KeyValueRow
                label={t("next.backup.waitingRestart")}
                value={<span className="text-wv-warn">{status.pending_restore}</span>}
              />
              {status.pending_restore_error ? (
                <KeyValueRow
                  label={t("next.backup.lastAttemptFailed")}
                  value={
                    <span className="text-wv-error-text">{status.pending_restore_error}</span>
                  }
                />
              ) : null}
            </div>
          ),
        }
      : null,
    {
      kind: "section",
      id: "restore",
      title: t("next.backup.restore"),
      note: t("next.backup.restoreNote"),
      fields: restoreFields,
    },
    manifest
      ? {
          kind: "custom",
          id: "manifest",
          title: t("next.backup.holds"),
          note: inspected?.key_compatible ? undefined : t("next.backup.keyIncompatibleNote"),
          searchText: "manifest archive contents preview",
          body: (
            <div className="flex flex-col">
              <KeyValueRow label={t("next.backup.taken")} value={formatDate(manifest.created_at_epoch_ms)} />
              <KeyValueRow label={t("next.backup.rows")} value={exportedRows.toLocaleString()} />
              <KeyValueRow label={t("next.backup.tables")} value={manifest.included_tables.length} />
              <KeyValueRow label={t("next.backup.schema")} value={manifest.weaver_schema_version} />
              <KeyValueRow
                label={t("next.backup.encrypted")}
                value={manifest.encrypted ? t("next.common.yes") : t("next.common.no")}
              />
              {manifest.source_weaver_version ? (
                <KeyValueRow label={t("next.backup.takenBy")} value={manifest.source_weaver_version} />
              ) : null}
              {manifest.source_engine ? (
                <KeyValueRow label={t("next.backup.sourceDatabase")} value={manifest.source_engine} />
              ) : null}
              <KeyValueRow
                label={t("next.backup.sourceDataDir")}
                value={
                  <span className="break-all">{manifest.source_paths.data_dir}</span>
                }
              />
              <KeyValueRow
                label={t("next.backup.sourceCompleteDir")}
                value={
                  <span className="break-all">{manifest.source_paths.complete_dir}</span>
                }
              />
              <KeyValueRow
                label={t("next.backup.sourceIntermediateDir")}
                value={
                  <span className="break-all">{manifest.source_paths.intermediate_dir}</span>
                }
              />
              {manifest.notes.map((entry) => (
                <div
                  key={entry}
                  className="border-b border-wv-hairline px-4 sm:px-6 py-[11px] text-[12.5px] text-wv-muted"
                >
                  {entry}
                </div>
              ))}
              {(inspected?.warnings ?? []).map((warning) => (
                <div
                  key={warning}
                  className="border-b border-wv-hairline px-4 sm:px-6 py-[11px] text-[12.5px] text-wv-warn"
                >
                  {warning}
                </div>
              ))}
              {inspected && !inspected.key_compatible ? (
                <div className="border-b border-wv-hairline px-4 sm:px-6 py-[11px] text-[12.5px] text-wv-error-text">
                  {t("next.backup.keyIncompatible")}
                </div>
              ) : null}
            </div>
          ),
        }
      : null,
    (inspected?.required_category_remaps.length ?? 0) > 0
      ? {
          kind: "table",
          id: "remaps",
          title: t("next.backup.categoryDestinations"),
          note: t("next.backup.categoryDestinationsNote"),
          columns: "minmax(0, 1fr) minmax(0, 1.2fr) 280px",
          headers: [t("next.categories.category"), t("next.backup.was"), t("next.backup.now")],
          rows: (inspected?.required_category_remaps ?? []).map((entry) => ({
            id: entry.category_name,
            searchText: `${entry.category_name} ${entry.current_dest_dir}`,
            cells: [
              <Cell key="name" className="text-wv-fg">
                {entry.category_name}
              </Cell>,
              <Cell key="was" mono className="text-wv-muted" title={entry.current_dest_dir}>
                {entry.current_dest_dir}
              </Cell>,
              <PathField
                key="now"
                compact
                label={t("next.backup.destinationFor", { name: entry.category_name })}
                value={remaps[entry.category_name] ?? ""}
                className="w-full"
                placeholder="/media/library"
                onChange={(next) =>
                  setRemaps((current) => ({ ...current, [entry.category_name]: next }))
                }
              />,
            ],
          })),
        }
      : null,
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton
          icon="downloadFile"
          disabled={busy || status?.busy || !password.trim() || mismatch}
          onClick={() => void download()}
        >
          {t("next.backup.download")}
        </SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <ConfirmDialog
        open={confirmRestore}
        title={t("next.backup.restoreFromArchive")}
        note={file?.name}
        busy={busy}
        confirmLabel={t("next.backup.stageRestore")}
        body={t("next.backup.restoreBody")}
        onConfirm={() => void restore()}
        onDismiss={() => setConfirmRestore(false)}
      />
    </>
  );
}

import { useEffect, useRef, useState } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { SETTINGS_QUERY } from "@/graphql/queries";
import { saveResponseAsDownload } from "@/lib/download";
import { KeyValueRow } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { DangerButton, SecondaryButton, TextField } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { formatDate } from "../../../data/format";
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

async function readJsonOrThrow(response: Response): Promise<unknown> {
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

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function BackupPanel() {
  const [{ data }] = useQuery<{ settings: { dataDir: string } }>({ query: SETTINGS_QUERY });
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
      setStatus((await readJsonOrThrow(await fetch(endpoint("api/backup/status"), {
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
     
  }, []);

  const mismatch = passwordConfirm !== "" && passwordConfirm !== password;

  const download = async () => {
    setBusy(true);
    setError(null);
    setNote("Building the archive…");
    try {
      const response = await fetch(endpoint("api/backup/export"), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ password: password.trim() ? password : null }),
      });
      if (!response.ok) {
        await throwJsonError(response);
      }
      const filename = await saveResponseAsDownload(response, `weaver_backup_${Date.now()}.enc`);
      setNote(`Saved ${filename}`);
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
    setNote("Reading the archive…");
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
          ? "Archive read. Give the categories below a destination on this machine."
          : "Archive read.",
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
    setNote("Staging the restore…");
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
        await fetch(endpoint("api/backup/restore"), {
          method: "POST",
          headers: authHeaders(),
          body: form,
        }),
      )) as RestoreReport;
      setNote(
        report.restart_required
          ? `Restore staged · ${report.history_jobs} downloads in history · restart weaver to apply it`
          : `Restored ${report.history_jobs} downloads from history`,
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

  const restoreFields: FieldSpec[] = [
    {
      id: "file",
      label: "Archive",
      help: "The .enc file a backup produced, from this machine or another one.",
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 flex-wrap items-center gap-3">
            <span className="max-w-[220px] truncate font-wv-mono text-[11.5px] text-wv-muted">
              {file ? file.name : "no file chosen"}
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
            <SecondaryButton onClick={() => fileRef.current?.click()}>Choose file</SecondaryButton>
          </div>
        ),
      },
    },
    {
      id: "restorePassword",
      label: "Password",
      help: "Whatever the archive was encrypted with.",
      control: {
        kind: "text",
        type: "password",
        value: restorePassword,
        onChange: setRestorePassword,
      },
    },
    {
      id: "dataDir",
      label: "Data directory",
      help: "Where the restored database and state are written on this machine.",
      keywords: dataDir,
      control: { kind: "text", value: dataDir, onChange: setDataDir },
    },
    {
      id: "intermediateDir",
      label: "Intermediate directory",
      help: `Blank uses ${dataDir || currentDataDir || "the data directory"}/intermediate.`,
      control: {
        kind: "text",
        value: intermediateDir,
        placeholder: `${dataDir || currentDataDir}/intermediate`,
        onChange: setIntermediateDir,
      },
    },
    {
      id: "completeDir",
      label: "Completed directory",
      help: `Blank uses ${dataDir || currentDataDir || "the data directory"}/complete.`,
      control: {
        kind: "text",
        value: completeDir,
        placeholder: `${dataDir || currentDataDir}/complete`,
        onChange: setCompleteDir,
      },
    },
    {
      id: "inspect",
      label: "Read the archive",
      help: "Weaver checks the archive and reports what restoring it would do, before it does it.",
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 flex-wrap items-center gap-3">
            <span className="min-w-0 font-wv-mono text-[11.5px] text-wv-muted">
              {status === null
                ? "checking…"
                : status.can_restore
                  ? "restore available"
                  : (status.reason ?? "restore unavailable")}
            </span>
            <SecondaryButton disabled={!file || busy} onClick={() => void inspect()}>
              Read archive
            </SecondaryButton>
          </div>
        ),
      },
    },
    {
      id: "restore",
      label: "Restore",
      help: "Replaces this machine's settings, providers, categories and history with the archive's.",
      control: {
        kind: "custom",
        control: (
          <DangerButton
            className="px-[14px]"
            disabled={restoreBlocked}
            onClick={() => setConfirmRestore(true)}
          >
            Restore from archive
          </DangerButton>
        ),
      },
    },
  ];

  const blocks: (SettingsBlock | null)[] = [
    {
      kind: "section",
      id: "export",
      title: "Backup",
      note: "settings, providers, categories, schedules and history",
      fields: [
        {
          id: "password",
          label: "Password",
          help: "The archive is encrypted with it. Weaver cannot recover it for you.",
          control: { kind: "text", type: "password", value: password, onChange: setPassword },
        },
        {
          id: "passwordConfirm",
          label: "Confirm password",
          help: mismatch ? "The two passwords do not match." : undefined,
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
          title: "Staged restore",
          note: status.pending_restore,
          searchText: "pending staged restore restart",
          body: (
            <div className="flex flex-col">
              <KeyValueRow
                label="Waiting for a restart"
                value={<span className="text-wv-warn">{status.pending_restore}</span>}
              />
              {status.pending_restore_error ? (
                <KeyValueRow
                  label="The last attempt failed"
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
      title: "Restore",
      note: "applied on the daemon's next start",
      fields: restoreFields,
    },
    manifest
      ? {
          kind: "custom",
          id: "manifest",
          title: "What this archive holds",
          note: inspected?.key_compatible ? undefined : "key not compatible",
          searchText: "manifest archive contents preview",
          body: (
            <div className="flex flex-col">
              <KeyValueRow label="Taken" value={formatDate(manifest.created_at_epoch_ms)} />
              <KeyValueRow label="Rows" value={exportedRows.toLocaleString()} />
              <KeyValueRow label="Tables" value={manifest.included_tables.length} />
              <KeyValueRow label="Schema" value={manifest.weaver_schema_version} />
              <KeyValueRow label="Encrypted" value={manifest.encrypted ? "yes" : "no"} />
              {manifest.source_weaver_version ? (
                <KeyValueRow label="Taken by" value={manifest.source_weaver_version} />
              ) : null}
              {manifest.source_engine ? (
                <KeyValueRow label="Source database" value={manifest.source_engine} />
              ) : null}
              <KeyValueRow
                label="Source data directory"
                value={
                  <span className="break-all">{manifest.source_paths.data_dir}</span>
                }
              />
              <KeyValueRow
                label="Source completed directory"
                value={
                  <span className="break-all">{manifest.source_paths.complete_dir}</span>
                }
              />
              <KeyValueRow
                label="Source intermediate directory"
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
                  This archive&apos;s encryption key cannot be promoted into the configured key
                  store, so it cannot be restored here.
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
          title: "Category destinations",
          note: "these folders do not exist on this machine",
          columns: "minmax(0, 1fr) minmax(0, 1.2fr) 280px",
          headers: ["Category", "Was", "Now"],
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
              <TextField
                key="now"
                label={`Destination for ${entry.category_name}`}
                value={remaps[entry.category_name] ?? ""}
                className="h-7 w-full"
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
          disabled={busy || status?.busy || !password.trim() || mismatch}
          onClick={() => void download()}
        >
          Download backup
        </SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <ConfirmDialog
        open={confirmRestore}
        title="Restore from archive"
        note={file?.name}
        busy={busy}
        confirmLabel="Stage restore"
        body="Everything weaver knows — providers, categories, schedules, history — is replaced by the archive's. The restore is written aside and applied the next time the daemon starts."
        onConfirm={() => void restore()}
        onDismiss={() => setConfirmRestore(false)}
      />
    </>
  );
}

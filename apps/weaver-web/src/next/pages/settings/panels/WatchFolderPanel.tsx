import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  SCAN_WATCH_FOLDER_MUTATION,
  UPDATE_SETTINGS_MUTATION,
  WATCH_FOLDER_SETTINGS_QUERY,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { KeyValueRow } from "../../../components/chrome";
import { SecondaryButton } from "../../../components/controls";
import {
  PanelControls,
  SettingsBlocks,
  useDraft,
  usePanelState,
  type SettingsBlock,
} from "../framework";

/**
 * Watch folder: a directory weaver picks NZBs out of.
 *
 * "Scan now" runs the same sweep the daemon runs on its own schedule, and its
 * report is kept on screen until the next one — a folder that quietly refuses
 * a file is the whole reason this screen exists.
 */

type WatchFolderMode = "off" | "polling" | "realtime";

interface WatchFolder {
  mode: WatchFolderMode;
  path: string | null;
  pollIntervalSecs: number;
  stabilitySecs: number;
  categoryFromSubfolders: boolean;
  scanningPaused: boolean;
}

interface WatchFolderForm extends Omit<WatchFolder, "path"> {
  path: string;
}

interface PathProblem {
  path: string;
  reason: string;
}

interface ScanReport {
  discoveredFiles: number;
  queuedNzbs: number;
  skippedInputs: PathProblem[];
  permanentErrors: PathProblem[];
  transientErrors: PathProblem[];
}

const DEFAULTS: WatchFolder = {
  mode: "off",
  path: null,
  pollIntervalSecs: 60,
  stabilitySecs: 3,
  categoryFromSubfolders: true,
  scanningPaused: false,
};

/** Option labels are translation keys, resolved when the panel renders. */
const MODES: { value: string; label: string }[] = [
  { value: "off", label: "next.common.off" },
  { value: "polling", label: "next.watchFolder.polling" },
  { value: "realtime", label: "next.watchFolder.live" },
];

export function WatchFolderPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ settings: { watchFolder: WatchFolder } }>({
    query: WATCH_FOLDER_SETTINGS_QUERY,
  });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [scanState, scanWatchFolder] = useMutation(SCAN_WATCH_FOLDER_MUTATION);
  const [report, setReport] = useState<ScanReport | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const source = useMemo<WatchFolderForm>(() => {
    const watchFolder = data?.settings?.watchFolder ?? DEFAULTS;
    return { ...watchFolder, path: watchFolder.path ?? "" };
  }, [data?.settings?.watchFolder]);

  const draft = useDraft<WatchFolderForm>(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value ?? source;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching,
    status: error ?? status,
    failed: error !== null,
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      if (values.mode !== "off" && !values.path.trim()) {
        setError(t("next.watchFolder.folderRequired"));
        return;
      }
      setError(null);
      void updateSettings({
        input: {
          watchFolder: {
            mode: values.mode,
            path: values.path.trim() || null,
            pollIntervalSecs: Math.max(1, Math.round(values.pollIntervalSecs || 60)),
            stabilitySecs: Math.max(0, Math.round(values.stabilitySecs || 0)),
            categoryFromSubfolders: values.categoryFromSubfolders,
            scanningPaused: values.scanningPaused,
          },
        },
      }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.watchFolder.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const runScan = async () => {
    setError(null);
    const result = await scanWatchFolder({});
    if (result.error) {
      setError(result.error.message);
      return;
    }
    setReport((result.data?.scanWatchFolder as ScanReport) ?? null);
  };

  const problems = report
    ? [
        ...report.permanentErrors.map((entry) => ({ ...entry, tone: "error" as const })),
        ...report.transientErrors.map((entry) => ({ ...entry, tone: "warn" as const })),
        ...report.skippedInputs.map((entry) => ({ ...entry, tone: "muted" as const })),
      ]
    : [];

  const blocks: (SettingsBlock | null)[] = [
    {
      kind: "section",
      id: "folder",
      title: t("next.settings.panel.watchFolder"),
      fields: [
        {
          id: "mode",
          label: t("next.watchFolder.watching"),
          help: t("next.watchFolder.watchingHelp"),
          keywords: "off polling realtime inotify",
          control: {
            kind: "segmented",
            value: values.mode,
            options: MODES.map((option) => ({ ...option, label: t(option.label) })),
            onChange: (next) => draft.set({ mode: next as WatchFolderMode }),
          },
        },
        {
          id: "path",
          label: t("next.watchFolder.folder"),
          help: t("next.watchFolder.folderHelp"),
          keywords: values.path,
          control: {
            kind: "path",
            value: values.path,
            placeholder: "/downloads/watch",
            onChange: (next) => draft.set({ path: next }),
          },
        },
        ...(values.mode === "polling"
          ? [
              {
                id: "pollIntervalSecs",
                label: t("next.watchFolder.pollInterval"),
                help: t("next.watchFolder.pollIntervalHelp"),
                control: {
                  kind: "number" as const,
                  value: values.pollIntervalSecs,
                  min: 1,
                  onChange: (next: number) => draft.set({ pollIntervalSecs: next }),
                  suffix: t("next.general.seconds"),
                },
              },
            ]
          : []),
        {
          id: "stabilitySecs",
          label: t("next.watchFolder.settleTime"),
          help: t("next.watchFolder.settleTimeHelp"),
          control: {
            kind: "number",
            value: values.stabilitySecs,
            min: 0,
            onChange: (next) => draft.set({ stabilitySecs: next }),
            suffix: t("next.general.seconds"),
          },
        },
        {
          id: "categoryFromSubfolders",
          label: t("next.watchFolder.categoryFromSubfolder"),
          help: t("next.watchFolder.categoryFromSubfolderHelp"),
          control: {
            kind: "toggle",
            value: values.categoryFromSubfolders,
            onChange: (next) => draft.set({ categoryFromSubfolders: next }),
          },
        },
        {
          id: "scanningPaused",
          label: t("next.watchFolder.pauseScanning"),
          help: t("next.watchFolder.pauseScanningHelp"),
          control: {
            kind: "toggle",
            value: values.scanningPaused,
            onChange: (next) => draft.set({ scanningPaused: next }),
          },
        },
      ],
    },
    report
      ? {
          kind: "custom",
          id: "report",
          title: t("next.watchFolder.lastScan"),
          note: t("next.watchFolder.lastScanNote", {
            queued: report.queuedNzbs,
            found: report.discoveredFiles,
          }),
          searchText: "scan report queued discovered errors skipped",
          body: (
            <>
              <KeyValueRow label={t("next.watchFolder.filesFound")} value={report.discoveredFiles} />
              <KeyValueRow label={t("next.watchFolder.queued")} value={report.queuedNzbs} />
              {problems.length === 0 ? (
                <KeyValueRow label={t("next.watchFolder.problems")} value={t("next.job.none")} />
              ) : (
                problems.map((problem) => (
                  <div
                    key={`${problem.tone}:${problem.path}:${problem.reason}`}
                    className="flex flex-wrap items-baseline gap-x-6 gap-y-1 border-b border-wv-hairline px-4 sm:px-6 py-[11px]"
                  >
                    <div className="min-w-0 flex-[1_1_260px] truncate font-wv-mono text-[12px] text-wv-secondary">
                      {problem.path}
                    </div>
                    <div
                      className={`ml-auto max-w-[420px] text-right text-[12.5px] ${
                        problem.tone === "error"
                          ? "text-wv-error-text"
                          : problem.tone === "warn"
                            ? "text-wv-warn"
                            : "text-wv-muted"
                      }`}
                    >
                      {problem.reason}
                    </div>
                  </div>
                ))
              )}
            </>
          ),
        }
      : null,
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton icon="refresh" onClick={() => void runScan()} disabled={scanState.fetching}>
          {scanState.fetching ? t("next.watchFolder.scanning") : t("next.watchFolder.scanNow")}
        </SecondaryButton>
      </PanelControls>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />
    </>
  );
}

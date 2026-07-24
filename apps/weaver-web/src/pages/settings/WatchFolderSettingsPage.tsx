import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { FolderPathInput } from "@/components/FolderPathInput";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
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
import { Switch } from "@/components/ui/switch";
import {
  SCAN_WATCH_FOLDER_MUTATION,
  UPDATE_SETTINGS_MUTATION,
  WATCH_FOLDER_SETTINGS_QUERY,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type WatchFolderMode = "off" | "polling" | "realtime";

type WatchFolderSettings = {
  mode: WatchFolderMode;
  path: string | null;
  pollIntervalSecs: number;
  stabilitySecs: number;
  categoryFromSubfolders: boolean;
  scanningPaused: boolean;
};

type WatchFolderFormValues = {
  mode: WatchFolderMode;
  path: string;
  pollIntervalSecs: number;
  stabilitySecs: number;
  categoryFromSubfolders: boolean;
  scanningPaused: boolean;
};

type WatchFolderScanIssue = {
  path: string;
  reason: string;
};

type WatchFolderMarkerRename = {
  from: string;
  to: string;
  marker: string;
};

type WatchFolderScanReport = {
  discoveredFiles: string[];
  queuedNzbs: string[];
  skippedInputs: WatchFolderScanIssue[];
  permanentErrors: WatchFolderScanIssue[];
  transientErrors: WatchFolderScanIssue[];
  markerRenamedSources: WatchFolderMarkerRename[];
};

type WatchFolderSettingsData = {
  settings: {
    watchFolder: WatchFolderSettings;
  };
};

const defaultWatchFolderSettings: WatchFolderSettings = {
  mode: "off",
  path: null,
  pollIntervalSecs: 60,
  stabilitySecs: 3,
  categoryFromSubfolders: true,
  scanningPaused: false,
};

function watchFolderFormFromSettings(settings: WatchFolderSettings): WatchFolderFormValues {
  return {
    mode: settings.mode,
    path: settings.path ?? "",
    pollIntervalSecs: settings.pollIntervalSecs,
    stabilitySecs: settings.stabilitySecs,
    categoryFromSubfolders: settings.categoryFromSubfolders,
    scanningPaused: settings.scanningPaused,
  };
}

export function WatchFolderSettingsPage() {
  const t = useTranslate();
  const [{ data, error }, reexecuteQuery] = useQuery<WatchFolderSettingsData>({
    query: WATCH_FOLDER_SETTINGS_QUERY,
  });
  const [updateSettingsState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [scanWatchFolderState, scanWatchFolder] = useMutation(SCAN_WATCH_FOLDER_MUTATION);
  const watchFolderSettings = data?.settings?.watchFolder ?? defaultWatchFolderSettings;

  const [values, setValues] = useState<WatchFolderFormValues>(
    watchFolderFormFromSettings(defaultWatchFolderSettings),
  );
  const [scanReport, setScanReport] = useState<WatchFolderScanReport | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (error) {
      setErrorMessage(error.message);
    }
  }, [error]);

  useEffect(() => {
    setValues(watchFolderFormFromSettings(watchFolderSettings));
  }, [watchFolderSettings]);

  const refresh = () => {
    reexecuteQuery({ requestPolicy: "network-only" });
  };

  const resetFeedback = () => {
    setErrorMessage(null);
    setNotice(null);
  };

  const handleSave = async (nextValues: WatchFolderFormValues) => {
    resetFeedback();
    const result = await updateSettings({
      input: {
        watchFolder: {
          mode: nextValues.mode,
          path: nextValues.path.trim() || null,
          pollIntervalSecs: Math.max(1, Math.round(nextValues.pollIntervalSecs || 60)),
          stabilitySecs: Math.max(0, Math.round(nextValues.stabilitySecs || 0)),
          categoryFromSubfolders: nextValues.categoryFromSubfolders,
          scanningPaused: nextValues.scanningPaused,
        },
      },
    });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setNotice(t("watchFolder.settingsSaved"));
    refresh();
  };

  const handleScan = async () => {
    resetFeedback();
    const result = await scanWatchFolder({});
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    if (result.data?.scanWatchFolder) {
      setScanReport(result.data.scanWatchFolder);
      setNotice(t("watchFolder.scanComplete"));
      refresh();
    }
  };

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader title={t("watchFolder.title")} description={t("watchFolder.desc")} />

      {errorMessage ? (
        <StatusBanner variant="destructive">{errorMessage}</StatusBanner>
      ) : null}

      {notice ? <StatusBanner variant="success">{notice}</StatusBanner> : null}

      <WatchFolderSettingsCard
        values={values}
        saving={updateSettingsState.fetching}
        scanning={scanWatchFolderState.fetching}
        scanReport={scanReport}
        onChange={setValues}
        onSave={handleSave}
        onScan={handleScan}
      />
    </div>
  );
}

function WatchFolderSettingsCard({
  values,
  saving,
  scanning,
  scanReport,
  onChange,
  onSave,
  onScan,
}: {
  values: WatchFolderFormValues;
  saving: boolean;
  scanning: boolean;
  scanReport: WatchFolderScanReport | null;
  onChange: (values: WatchFolderFormValues) => void;
  onSave: (values: WatchFolderFormValues) => Promise<void>;
  onScan: () => Promise<void>;
}) {
  const t = useTranslate();
  const hasPath = values.path.trim().length > 0;
  const automaticEnabled = values.mode !== "off" && !values.scanningPaused;

  return (
    <SectionCard
      title={t("watchFolder.title")}
      description={t("watchFolder.desc")}
      actions={
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => void onScan()} disabled={!hasPath || scanning}>
            {scanning ? t("watchFolder.scanning") : t("watchFolder.scanNow")}
          </Button>
          <Button onClick={() => void onSave(values)} disabled={saving}>
            {saving ? t("watchFolder.saving") : t("action.save")}
          </Button>
        </div>
      }
    >
      <div className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <Field label={t("watchFolder.mode")} htmlFor="watch-folder-mode">
            <Select
              value={values.mode}
              onValueChange={(mode) =>
                onChange({ ...values, mode: mode as WatchFolderMode })
              }
            >
              <SelectTrigger id="watch-folder-mode">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="off">{t("watchFolder.modeOff")}</SelectItem>
                <SelectItem value="polling">{t("watchFolder.modePolling")}</SelectItem>
                <SelectItem value="realtime">{t("watchFolder.modeRealtime")}</SelectItem>
              </SelectContent>
            </Select>
          </Field>

          <Field label={t("watchFolder.folder")} htmlFor="watch-folder-path">
            <FolderPathInput
              inputId="watch-folder-path"
              value={values.path}
              onChange={(path) => onChange({ ...values, path })}
              browseLabel={t("watchFolder.browse")}
            />
          </Field>

          {values.mode === "polling" ? (
            <Field label={t("watchFolder.pollInterval")} htmlFor="watch-folder-poll-interval">
              <Input
                id="watch-folder-poll-interval"
                type="number"
                min={1}
                value={values.pollIntervalSecs}
                onChange={(event) =>
                  onChange({
                    ...values,
                    pollIntervalSecs: Number(event.target.value),
                  })
                }
              />
            </Field>
          ) : null}
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
            <div>
              <div className="text-sm font-semibold text-foreground">
                {t("watchFolder.categorySubfolders")}
              </div>
              <div className="mt-1 text-[12.5px] text-muted-foreground">
                {t("watchFolder.categorySubfoldersDesc")}
              </div>
            </div>
            <Switch
              checked={values.categoryFromSubfolders}
              onCheckedChange={(checked) =>
                onChange({ ...values, categoryFromSubfolders: checked })
              }
            />
          </div>

          <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
            <div>
              <div className="text-sm font-semibold text-foreground">
                {t("watchFolder.automaticScanning")}
              </div>
              <div className="mt-1 text-[12.5px] text-muted-foreground">
                {automaticEnabled
                  ? t("watchFolder.automaticEnabled")
                  : t("watchFolder.automaticPausedOrOff")}
              </div>
            </div>
            <Switch
              checked={!values.scanningPaused && values.mode !== "off"}
              disabled={values.mode === "off"}
              onCheckedChange={(checked) =>
                onChange({ ...values, scanningPaused: !checked })
              }
            />
          </div>
        </div>

        {scanReport ? <WatchFolderScanReportCard report={scanReport} /> : null}
      </div>
    </SectionCard>
  );
}

function WatchFolderScanReportCard({ report }: { report: WatchFolderScanReport }) {
  const t = useTranslate();
  const issueCount =
    report.skippedInputs.length + report.permanentErrors.length + report.transientErrors.length;

  return (
    <div
      role="region"
      aria-label={t("watchFolder.scanReport")}
      data-testid="watch-folder-scan-report"
      className="space-y-4 rounded-inner border border-border p-5"
    >
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <DetailCard label={t("watchFolder.discovered")}>{report.discoveredFiles.length}</DetailCard>
        <DetailCard label={t("watchFolder.queued")}>{report.queuedNzbs.length}</DetailCard>
        <DetailCard label={t("watchFolder.markers")}>{report.markerRenamedSources.length}</DetailCard>
        <DetailCard label={t("watchFolder.skipped")}>{report.skippedInputs.length}</DetailCard>
        <DetailCard label={t("watchFolder.issues")}>{issueCount}</DetailCard>
      </div>

      {report.queuedNzbs.length > 0 ? (
        <ScanList title={t("watchFolder.queuedNzbs")} items={report.queuedNzbs} />
      ) : null}
      {report.markerRenamedSources.length > 0 ? (
        <ScanList
          title={t("watchFolder.markerRenames")}
          items={report.markerRenamedSources.map((rename) => `${rename.from} -> ${rename.to}`)}
        />
      ) : null}
      {report.permanentErrors.length > 0 ? (
        <ScanIssueList title={t("watchFolder.permanentErrors")} items={report.permanentErrors} />
      ) : null}
      {report.transientErrors.length > 0 ? (
        <ScanIssueList title={t("watchFolder.transientErrors")} items={report.transientErrors} />
      ) : null}
      {report.skippedInputs.length > 0 ? (
        <ScanIssueList title={t("watchFolder.skippedInputs")} items={report.skippedInputs} />
      ) : null}
    </div>
  );
}

function ScanList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="space-y-2">
      <div className="text-sm font-semibold text-foreground">{title}</div>
      <div className="space-y-1">
        {items.slice(0, 8).map((item) => (
          <div key={item} className="break-all font-mono text-xs text-muted-foreground">
            {item}
          </div>
        ))}
      </div>
    </div>
  );
}

function ScanIssueList({ title, items }: { title: string; items: WatchFolderScanIssue[] }) {
  return (
    <ScanList
      title={title}
      items={items.map((item) => `${item.path}: ${item.reason}`)}
    />
  );
}

function Field({
  label,
  description,
  children,
  htmlFor,
}: {
  label: string;
  description?: string;
  children: ReactNode;
  htmlFor?: string;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={htmlFor} className="text-sm font-semibold">
        {label}
      </Label>
      {children}
      {description ? <p className="text-[12.5px] text-muted-foreground">{description}</p> : null}
    </div>
  );
}

function DetailCard({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-inner border border-border bg-card px-3 py-3">
      <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 text-sm font-semibold text-foreground">{children}</div>
    </div>
  );
}

function StatusBanner({
  variant,
  children,
}: {
  variant: "success" | "destructive";
  children: ReactNode;
}) {
  const classes =
    variant === "success"
      ? "border-status-complete/40 bg-status-complete/10 text-status-complete"
      : "border-destructive/40 bg-destructive/10 text-destructive";
  return <div className={`rounded-inner border px-4 py-3 text-sm ${classes}`}>{children}</div>;
}

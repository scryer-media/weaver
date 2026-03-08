import { useState, useEffect } from "react";
import { useQuery, useMutation } from "urql";
import {
  SETTINGS_PAGE_QUERY,
  SETTINGS_QUERY,
  SET_SPEED_LIMIT_MUTATION,
  UPDATE_SETTINGS_MUTATION,
  PAUSE_ALL_MUTATION,
  RESUME_ALL_MUTATION,
} from "@/graphql/queries";
import { formatBytes, formatSpeed } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";

const MAX_SPEED = 100 * 1024 * 1024; // 100 MB/s

export function Settings() {
  const [{ data }] = useQuery({
    query: SETTINGS_PAGE_QUERY,
  });
  const [{ data: settingsData }, reexecute] = useQuery({
    query: SETTINGS_QUERY,
  });

  const [, setSpeedLimit] = useMutation(SET_SPEED_LIMIT_MUTATION);
  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);

  const [speedValue, setSpeedValue] = useState(0);
  const [initialized, setInitialized] = useState(false);

  // General settings form state
  const [intermediateDir, setIntermediateDir] = useState("");
  const [completeDir, setCompleteDir] = useState("");
  const [cleanup, setCleanup] = useState(true);
  const [maxRetries, setMaxRetries] = useState(3);
  const [settingsSaved, setSettingsSaved] = useState(false);

  const isPaused = data?.isPaused ?? false;
  const metrics = data?.metrics;
  const settings = settingsData?.settings;

  useEffect(() => {
    if (!initialized && metrics) {
      setInitialized(true);
    }
  }, [initialized, metrics]);

  // Populate general settings form from query data
  useEffect(() => {
    if (settings) {
      setIntermediateDir(settings.intermediateDir ?? "");
      setCompleteDir(settings.completeDir ?? "");
      setCleanup(settings.cleanupAfterExtract ?? true);
      setMaxRetries(settings.maxRetries ?? 3);
    }
  }, [settings]);

  const handleSpeedChange = (value: number) => {
    setSpeedValue(value);
  };

  const handleSpeedCommit = () => {
    setSpeedLimit({ bytesPerSec: speedValue });
  };

  const handleSaveSettings = async () => {
    await updateSettings({
      input: {
        intermediateDir: intermediateDir || null,
        completeDir: completeDir || null,
        cleanupAfterExtract: cleanup,
        maxDownloadSpeed: null,
        maxRetries,
      },
    });
    reexecute({ requestPolicy: "network-only" });
    setSettingsSaved(true);
    setTimeout(() => setSettingsSaved(false), 2000);
  };

  const t = useTranslate();

  return (
    <div className="p-4 sm:p-6">
      <h1 className="mb-4 text-xl font-bold text-foreground sm:mb-6 sm:text-2xl">{t("settings.title")}</h1>

      <div className="max-w-2xl space-y-6 sm:space-y-8">
        {/* Global Pause */}
        <section className="rounded-lg border border-border bg-card p-5">
          <h2 className="mb-3 text-lg font-semibold text-card-foreground">{t("settings.downloadControl")}</h2>
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm text-foreground">{t("settings.globalPause")}</div>
              <div className="text-xs text-muted-foreground">
                {isPaused ? t("settings.pausedDesc") : t("settings.activeDesc")}
              </div>
            </div>
            <button
              onClick={() => (isPaused ? resumeAll() : pauseAll())}
              className={`rounded-md px-4 py-2 text-sm font-medium transition-colors ${
                isPaused
                  ? "bg-green-600 text-white hover:bg-green-500"
                  : "bg-yellow-600 text-white hover:bg-yellow-500"
              }`}
            >
              {isPaused ? t("action.resumeAll") : t("action.pauseAll")}
            </button>
          </div>
        </section>

        {/* Speed Limit */}
        <section className="rounded-lg border border-border bg-card p-5">
          <h2 className="mb-3 text-lg font-semibold text-card-foreground">{t("settings.speedLimit")}</h2>
          <div className="mb-2 flex items-center justify-between text-sm">
            <span className="text-muted-foreground">
              {speedValue === 0 ? t("settings.unlimited") : formatSpeed(speedValue)}
            </span>
            <button
              onClick={handleSpeedCommit}
              className="rounded bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90"
            >
              {t("action.apply")}
            </button>
          </div>
          <input
            type="range"
            min={0}
            max={MAX_SPEED}
            step={1024 * 1024}
            value={speedValue}
            onChange={(e) => handleSpeedChange(Number(e.target.value))}
            className="w-full accent-primary"
          />
          <div className="mt-1 flex justify-between text-xs text-muted-foreground">
            <span>{t("settings.unlimited")}</span>
            <span>100 MB/s</span>
          </div>
        </section>

        {/* General Settings */}
        {settings && (
          <section className="rounded-lg border border-border bg-card p-5">
            <h2 className="mb-4 text-lg font-semibold text-card-foreground">{t("settings.general")}</h2>
            <div className="space-y-4">
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("settings.dataDir")}</label>
                <div className="rounded-md border border-input bg-field/50 px-3 py-2 text-sm text-muted-foreground">
                  {settings.dataDir}
                </div>
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("settings.intermediateDir")}</label>
                <input
                  type="text"
                  value={intermediateDir}
                  onChange={(e) => setIntermediateDir(e.target.value)}
                  placeholder={`${settings.dataDir}/intermediate`}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
                <div className="mt-0.5 text-xs text-muted-foreground">{t("settings.intermediateDirDesc")}</div>
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("settings.completeDir")}</label>
                <input
                  type="text"
                  value={completeDir}
                  onChange={(e) => setCompleteDir(e.target.value)}
                  placeholder={`${settings.dataDir}/complete`}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
                <div className="mt-0.5 text-xs text-muted-foreground">{t("settings.completeDirDesc")}</div>
              </div>
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-sm text-foreground">{t("settings.cleanupAfterExtract")}</div>
                  <div className="text-xs text-muted-foreground">{t("settings.cleanupDesc")}</div>
                </div>
                <label className="relative inline-flex cursor-pointer items-center">
                  <input
                    type="checkbox"
                    checked={cleanup}
                    onChange={(e) => setCleanup(e.target.checked)}
                    className="peer sr-only"
                  />
                  <div className="h-5 w-9 rounded-full bg-muted-foreground/30 after:absolute after:left-[2px] after:top-[2px] after:h-4 after:w-4 after:rounded-full after:bg-white after:transition-all peer-checked:bg-primary peer-checked:after:translate-x-full" />
                </label>
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("settings.maxRetries")}</label>
                <input
                  type="number"
                  min={0}
                  max={20}
                  value={maxRetries}
                  onChange={(e) => setMaxRetries(Number(e.target.value))}
                  className="w-24 rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <button
                onClick={handleSaveSettings}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              >
                {settingsSaved ? t("settings.saved") : t("settings.save")}
              </button>
            </div>
          </section>
        )}

        {/* Metrics */}
        {metrics && (
          <section className="rounded-lg border border-border bg-card p-5">
            <h2 className="mb-4 text-lg font-semibold text-card-foreground">{t("settings.metrics")}</h2>
            <div className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-3">
              <MetricItem label={t("metrics.downloaded")} value={formatBytes(metrics.bytesDownloaded)} />
              <MetricItem label={t("metrics.decoded")} value={formatBytes(metrics.bytesDecoded)} />
              <MetricItem label={t("metrics.committed")} value={formatBytes(metrics.bytesCommitted)} />
              <MetricItem label={t("metrics.downloadSpeed")} value={formatSpeed(metrics.currentDownloadSpeed)} />
              <MetricItem label={t("metrics.downloadQueue")} value={String(metrics.downloadQueueDepth)} />
              <MetricItem label={t("metrics.decodePending")} value={String(metrics.decodePending)} />
              <MetricItem label={t("metrics.commitPending")} value={String(metrics.commitPending)} />
              <MetricItem label={t("metrics.segmentsDownloaded")} value={String(metrics.segmentsDownloaded)} />
              <MetricItem label={t("metrics.segmentsDecoded")} value={String(metrics.segmentsDecoded)} />
              <MetricItem label={t("metrics.segmentsCommitted")} value={String(metrics.segmentsCommitted)} />
              <MetricItem label={t("metrics.articlesNotFound")} value={String(metrics.articlesNotFound)} />
              <MetricItem label={t("metrics.decodeErrors")} value={String(metrics.decodeErrors)} />
              <MetricItem label={t("metrics.verifyActive")} value={String(metrics.verifyActive)} />
              <MetricItem label={t("metrics.repairActive")} value={String(metrics.repairActive)} />
              <MetricItem label={t("metrics.extractActive")} value={String(metrics.extractActive)} />
              <MetricItem label={t("metrics.segmentsRetried")} value={String(metrics.segmentsRetried)} />
              <MetricItem label={t("metrics.failedPermanent")} value={String(metrics.segmentsFailedPermanent)} />
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function MetricItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-muted-foreground">{label}</div>
      <div className="font-mono text-foreground">{value}</div>
    </div>
  );
}

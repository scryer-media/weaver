import { memo, useEffect, useMemo, useRef, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { useTheme } from "next-themes";
import {
  Activity,
  Clock3,
  FolderUp,
  ListOrdered,
  Menu,
  Monitor,
  MoonStar,
  RefreshCw,
  ScrollText,
  Settings,
  Sun,
  Unplug,
} from "lucide-react";
import { useQuery, useSubscription } from "urql";
import {
  requestGraphqlClientRestart,
  useGraphqlConnectionState,
} from "@/graphql/client";
import {
  JOB_UPDATES_SUBSCRIPTION,
  JOBS_PAGE_QUERY,
  LIVE_METRICS_QUERY,
  LIVE_METRICS_SUBSCRIPTION,
  VERSION_QUERY,
} from "@/graphql/queries";
import { formatSpeed } from "@/components/SpeedDisplay";
import { Sparkline } from "@/components/ui/sparkline";
import { UploadModal } from "@/components/UploadModal";
import { useSpeedHistory } from "@/lib/hooks/use-speed-history";
import { LiveDataProvider, type DownloadBlockState } from "@/lib/context/live-data-context";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import {
  normalizeFacadeJobProgress,
  normalizeFacadeJobStatus,
  normalizeGraphqlTimestamp,
  normalizeJobData,
  type GraphqlJobData,
  type JobData,
} from "@/lib/job-types";
import { useTranslate } from "@/lib/context/translate-context";
import { usePwa } from "@/lib/context/pwa-context";
import { settingsNav } from "@/pages/settings/settings-nav";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";

const navItems = [
  { to: "/", labelKey: "nav.jobs", icon: ListOrdered },
  { to: "/history", labelKey: "nav.history", icon: Clock3 },
  { to: "/monitoring", labelKey: "nav.monitoring", icon: Activity },
  { to: "/logs", labelKey: "nav.logs", icon: ScrollText },
  { to: "/settings", labelKey: "nav.settings", icon: Settings },
];

interface QueueSnapshotPayload {
  items: GraphqlJobData[];
  latestCursor: string;
  globalState: {
    isPaused: boolean;
    downloadBlock: DownloadBlockState;
  };
}

interface Snapshot {
  jobs: JobData[];
  latestCursor: string;
  globalState: {
    isPaused: boolean;
    downloadBlock: DownloadBlockState;
  };
}

interface LiveMetricsSnapshot {
  metrics: { currentDownloadSpeed: number };
  globalState: Snapshot["globalState"];
}

const EMPTY_JOBS: JobData[] = [];
const DEFAULT_DOWNLOAD_BLOCK: DownloadBlockState = {
  kind: "NONE",
  capEnabled: false,
  period: null,
  usedBytes: 0,
  limitBytes: 0,
  remainingBytes: 0,
  reservedBytes: 0,
  windowStartsAtEpochMs: null,
  windowEndsAtEpochMs: null,
  timezoneName: "",
  scheduledSpeedLimit: 0,
};
const DEFAULT_GLOBAL_STATE: Snapshot["globalState"] = {
  isPaused: false,
  downloadBlock: DEFAULT_DOWNLOAD_BLOCK,
};

const RoutedOutlet = memo(function RoutedOutlet() {
  return <Outlet />;
});

function sameStringArray(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function sameEpisode(
  left: JobData["parsedRelease"]["episode"],
  right: GraphqlJobData["parsedRelease"]["episode"],
): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return left == null && right == null;
  }
  return left.season === right.season
    && left.absoluteEpisode === right.absoluteEpisode
    && left.raw === right.raw
    && left.episodeNumbers.length === right.episodeNumbers.length
    && left.episodeNumbers.every((value, index) => value === right.episodeNumbers[index]);
}

function sameParsedRelease(left: JobData["parsedRelease"], right: GraphqlJobData["parsedRelease"]): boolean {
  return left.normalizedTitle === right.normalizedTitle
    && left.releaseGroup === right.releaseGroup
    && sameStringArray(left.languagesAudio, right.languagesAudio)
    && sameStringArray(left.languagesSubtitles, right.languagesSubtitles)
    && left.year === right.year
    && left.quality === right.quality
    && left.source === right.source
    && left.videoCodec === right.videoCodec
    && left.videoEncoding === right.videoEncoding
    && left.audio === right.audio
    && sameStringArray(left.audioCodecs, right.audioCodecs)
    && left.audioChannels === right.audioChannels
    && left.isDualAudio === right.isDualAudio
    && left.isAtmos === right.isAtmos
    && left.isDolbyVision === right.isDolbyVision
    && left.detectedHdr === right.detectedHdr
    && left.isHdr10Plus === right.isHdr10Plus
    && left.isHlg === right.isHlg
    && left.fps === right.fps
    && left.isProperUpload === right.isProperUpload
    && left.isRepack === right.isRepack
    && left.isRemux === right.isRemux
    && left.isBdDisk === right.isBdDisk
    && left.isAiEnhanced === right.isAiEnhanced
    && left.isHardcodedSubs === right.isHardcodedSubs
    && left.streamingService === right.streamingService
    && left.edition === right.edition
    && left.animeVersion === right.animeVersion
    && left.parseConfidence === right.parseConfidence
    && sameEpisode(left.episode, right.episode);
}

function sameMetadata(
  left: { key: string; value: string }[],
  right: { key: string; value: string }[] | undefined,
): boolean {
  const resolvedRight = right ?? [];
  return left.length === resolvedRight.length
    && left.every((entry, index) =>
      entry.key === resolvedRight[index]?.key && entry.value === resolvedRight[index]?.value);
}

function samePhaseProgress(
  left: JobData["phaseProgress"],
  right: GraphqlJobData["phaseProgress"],
): boolean {
  const resolvedRight = right ?? [];
  return left.length === resolvedRight.length
    && left.every((phase, index) => {
      const other = resolvedRight[index];
      return other !== undefined
        && phase.phase === other.phase
        && phase.completedBytes === other.completedBytes
        && phase.totalBytes === other.totalBytes
        && phase.progressPercent === other.progressPercent
        && phase.rateBps === other.rateBps
        && phase.estimatedRemainingMs === other.estimatedRemainingMs
        && phase.startedAtEpochMs === other.startedAtEpochMs
        && phase.updatedAtEpochMs === other.updatedAtEpochMs;
    });
}

function sameDeleteOperation(
  left: JobData["deleteOperation"],
  right: GraphqlJobData["deleteOperation"],
): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return left == null && right == null;
  }
  return left.operationId === right.operationId
    && left.state === right.state
    && left.locked === right.locked
    && left.deleteFiles === right.deleteFiles
    && left.errorMessage === right.errorMessage;
}

function sameDownloadBlock(left: DownloadBlockState, right: DownloadBlockState): boolean {
  return left.kind === right.kind
    && left.capEnabled === right.capEnabled
    && left.period === right.period
    && left.usedBytes === right.usedBytes
    && left.limitBytes === right.limitBytes
    && left.remainingBytes === right.remainingBytes
    && left.reservedBytes === right.reservedBytes
    && left.windowStartsAtEpochMs === right.windowStartsAtEpochMs
    && left.windowEndsAtEpochMs === right.windowEndsAtEpochMs
    && left.timezoneName === right.timezoneName
    && left.scheduledSpeedLimit === right.scheduledSpeedLimit;
}

function sameGlobalState(left: Snapshot["globalState"], right: QueueSnapshotPayload["globalState"]): boolean {
  return left.isPaused === right.isPaused && sameDownloadBlock(left.downloadBlock, right.downloadBlock);
}

function matchesNormalizedJob(previous: JobData, next: GraphqlJobData): boolean {
  return previous.id === next.id
    && previous.name === next.name
    && previous.displayTitle === next.displayTitle
    && previous.originalTitle === next.originalTitle
    && previous.parsedRelease !== undefined
    && sameParsedRelease(previous.parsedRelease, next.parsedRelease)
    && previous.status === normalizeFacadeJobStatus(next.status)
    && previous.progress === normalizeFacadeJobProgress(next.progressPercent, next.progress)
    && previous.progressPercent === (next.progressPercent ?? null)
    && previous.totalBytes === next.totalBytes
    && previous.downloadedBytes === next.downloadedBytes
    && previous.optionalRecoveryBytes === next.optionalRecoveryBytes
    && previous.optionalRecoveryDownloadedBytes === next.optionalRecoveryDownloadedBytes
    && previous.failedBytes === next.failedBytes
    && previous.health === next.health
    && previous.hasPassword === next.hasPassword
    && previous.category === next.category
    && previous.createdAt === normalizeGraphqlTimestamp(next.createdAt)
    && previous.completedAt === normalizeGraphqlTimestamp(next.completedAt)
    && previous.error === (next.error ?? null)
    && previous.outputDir === (next.outputDir ?? null)
    && sameMetadata(previous.metadata, next.metadata ?? next.attributes)
    && samePhaseProgress(previous.phaseProgress, next.phaseProgress)
    && sameDeleteOperation(previous.deleteOperation ?? null, next.deleteOperation ?? null);
}

function mapQueueSnapshot(
  snapshot: QueueSnapshotPayload | undefined,
  previous?: Snapshot,
): Snapshot | undefined {
  if (!snapshot) {
    return undefined;
  }

  const previousJobsById = new Map(previous?.jobs.map((job) => [job.id, job]) ?? []);
  let reusedAllJobs = Boolean(previous) && snapshot.items.length === (previous?.jobs.length ?? 0);
  const nextJobs = snapshot.items.map((job, index) => {
    const previousAtIndex = previous?.jobs[index];
    const candidate = previousAtIndex?.id === job.id
      ? previousAtIndex
      : previousJobsById.get(job.id);
    if (candidate && matchesNormalizedJob(candidate, job)) {
      return candidate;
    }
    reusedAllJobs = false;
    return normalizeJobData(job);
  });

  const jobs = previous && reusedAllJobs && nextJobs.every((job, index) => job === previous.jobs[index])
    ? previous.jobs
    : nextJobs;
  const globalState = previous && sameGlobalState(previous.globalState, snapshot.globalState)
    ? previous.globalState
    : snapshot.globalState;
  const latestCursor = previous && previous.latestCursor === snapshot.latestCursor
    ? previous.latestCursor
    : (snapshot.latestCursor ?? previous?.latestCursor ?? "");

  if (
    previous
    && jobs === previous.jobs
    && globalState === previous.globalState
    && latestCursor === previous.latestCursor
  ) {
    return previous;
  }

  return {
    latestCursor,
    globalState,
    jobs,
  };
}

function ThemeToggle({ className }: { className?: string }) {
  const { theme, setTheme } = useTheme();

  return (
    <button
      type="button"
      onClick={() => {
        if (theme === "dark") {
          setTheme("light");
          return;
        }
        if (theme === "light") {
          setTheme("system");
          return;
        }
        setTheme("dark");
      }}
      title={theme === "dark" ? "Dark" : theme === "light" ? "Light" : "System"}
      aria-label="Toggle theme"
      className={cn(
        "flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-[9px] border border-border bg-card text-muted-foreground transition-colors hover:text-foreground",
        className,
      )}
    >
      {theme === "dark" ? (
        <Sun className="size-4" />
      ) : theme === "light" ? (
        <MoonStar className="size-4" />
      ) : (
        <Monitor className="size-4" />
      )}
    </button>
  );
}

function DisconnectBanner({
  title,
  message,
}: {
  title: string;
  message: string;
}) {
  return (
    <div className="border-b border-amber-500/30 bg-amber-500/10 px-4 py-3 text-amber-950 dark:text-amber-100">
      <div className="flex items-center gap-3">
        <div className="flex size-8 items-center justify-center rounded-full bg-amber-500/20">
          <Unplug className="size-4" />
        </div>
        <div className="min-w-0">
          <div className="text-sm font-semibold">{title}</div>
          <div className="text-sm text-amber-900/80 dark:text-amber-100/80">{message}</div>
        </div>
      </div>
    </div>
  );
}

function PwaUpdateBanner() {
  const t = useTranslate();
  const { updateAvailable, applyUpdate } = usePwa();

  if (!updateAvailable) {
    return null;
  }

  return (
    <div className="fixed right-4 bottom-4 z-50 max-w-sm rounded-2xl border border-border/80 bg-background/95 p-4 shadow-[0_18px_60px_rgba(8,18,36,0.28)] backdrop-blur-md">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-sm font-semibold text-foreground">
            {t("pwa.updateTitle")}
          </div>
          <div className="mt-1 text-sm text-muted-foreground">
            {t("pwa.updateBody")}
          </div>
          <div className="mt-3 flex items-center gap-2">
            <Button size="sm" onClick={applyUpdate}>
              <RefreshCw className="size-4" />
              {t("pwa.reload")}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

export function Layout() {
  const t = useTranslate();
  const location = useLocation();
  const [uploadOpen, setUploadOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [polledSnapshot, setPolledSnapshot] = useState<QueueSnapshotPayload | undefined>();
  const [polledMetrics, setPolledMetrics] = useState<LiveMetricsSnapshot | undefined>();
  const connectionState = useGraphqlConnectionState();

  const [{ data: queryData }] = useQuery<{ queueSnapshot: QueueSnapshotPayload }>({
    query: JOBS_PAGE_QUERY,
  });
  const [{ data: metricsQueryData }] = useQuery<LiveMetricsSnapshot>({
    query: LIVE_METRICS_QUERY,
  });
  const [{ data: versionData }] = useQuery<{ version: string }>({
    query: VERSION_QUERY,
  });
  const [{ data: snapshotSubscriptionData }] = useSubscription<{
    queueSnapshots: QueueSnapshotPayload;
  }>({
    query: JOB_UPDATES_SUBSCRIPTION,
    pause: connectionState.status === "disconnected",
  });
  const [{ data: metricsSubscriptionData }] = useSubscription<{
    systemMetricsUpdates: LiveMetricsSnapshot;
  }>({
    query: LIVE_METRICS_SUBSCRIPTION,
    pause: connectionState.status === "disconnected",
  });

  const reconnectPolling = useReconnectPolling<{ queueSnapshot: QueueSnapshotPayload }>({
    enabled: connectionState.status === "disconnected",
    query: JOBS_PAGE_QUERY,
    onData: (nextSnapshot) => {
      setPolledSnapshot(nextSnapshot.queueSnapshot);
      requestGraphqlClientRestart();
    },
  });
  const reconnectMetricsPolling = useReconnectPolling<LiveMetricsSnapshot>({
    enabled: connectionState.status === "disconnected",
    query: LIVE_METRICS_QUERY,
    onData: (nextSnapshot) => {
      setPolledMetrics(nextSnapshot);
    },
  });

  useEffect(() => {
    if (connectionState.status === "connected" && snapshotSubscriptionData?.queueSnapshots) {
      setPolledSnapshot(undefined);
    }
  }, [connectionState.status, snapshotSubscriptionData]);

  useEffect(() => {
    if (connectionState.status === "connected" && metricsSubscriptionData?.systemMetricsUpdates) {
      setPolledMetrics(undefined);
    }
  }, [connectionState.status, metricsSubscriptionData]);

  const rawSnapshot =
    polledSnapshot ?? snapshotSubscriptionData?.queueSnapshots ?? queryData?.queueSnapshot;
  const previousSnapshotRef = useRef<Snapshot | undefined>(undefined);
  const snapshot = useMemo(() => {
    // eslint-disable-next-line react-hooks/refs -- queue snapshots are reconciled against the previous render to preserve row identity without scheduling another update
    const previousSnapshot = previousSnapshotRef.current;
    const nextSnapshot = mapQueueSnapshot(rawSnapshot, previousSnapshot);
    // eslint-disable-next-line react-hooks/refs -- cache the reconciled snapshot for the next render
    previousSnapshotRef.current = nextSnapshot;
    return nextSnapshot;
  }, [rawSnapshot]);

  const snapshotJobs = snapshot?.jobs ?? EMPTY_JOBS;
  const metricsSnapshot =
    polledMetrics ?? metricsSubscriptionData?.systemMetricsUpdates ?? metricsQueryData;
  const currentGlobalState =
    metricsSnapshot?.globalState ?? snapshot?.globalState ?? DEFAULT_GLOBAL_STATE;
  const isPolling = reconnectPolling.isPolling || reconnectMetricsPolling.isPolling;
  const liveData = useMemo(
    () => ({
      jobs: snapshotJobs,
      speed: metricsSnapshot?.metrics?.currentDownloadSpeed ?? 0,
      isPaused: currentGlobalState.isPaused,
      downloadBlock: currentGlobalState.downloadBlock,
      connection: {
        status: connectionState.status,
        isDisconnected: connectionState.status === "disconnected",
        isPolling,
      },
    }),
    [
      connectionState.status,
      currentGlobalState.downloadBlock,
      currentGlobalState.isPaused,
      isPolling,
      metricsSnapshot?.metrics?.currentDownloadSpeed,
      snapshotJobs,
    ],
  );
  const disconnectBannerMessage = liveData.connection.isPolling
    ? t("connection.pollingBody")
    : t("connection.retryingBody");
  const speedHistory = useSpeedHistory(liveData.speed);
  const titleEtaSpeed = useStableEtaSpeed(liveData.jobs, liveData.speed);

  const lastTitleUpdate = useRef(0);
  useEffect(() => {
    const now = Date.now();
    const hasActive = liveData.jobs.some(
      (job) => job.status !== "COMPLETE" && job.status !== "FAILED",
    );

    const isIdle = !liveData.isPaused && liveData.speed === 0;
    const isPaused = liveData.isPaused && hasActive;
    if (!isPaused && !isIdle && now - lastTitleUpdate.current < 2500) return;
    lastTitleUpdate.current = now;

    if (isPaused) {
      document.title = "Paused - Weaver";
      return;
    }

    if (liveData.speed > 0) {
      const downloading = liveData.jobs.filter((job) => job.status === "DOWNLOADING");
      const remaining = downloading.reduce(
        (sum, job) => sum + (job.totalBytes - job.downloadedBytes),
        0,
      );
      const formattedEta = formatEtaFromRemainingBytes(remaining, titleEtaSpeed);
      const eta = formattedEta !== "\u2014" ? ` - ${formattedEta}` : "";
      document.title = `${formatSpeed(liveData.speed)}${eta} - Weaver`;
      return;
    }

    document.title = "Weaver";
  }, [liveData, titleEtaSpeed]);

  const isActive = (to: string) =>
    to === "/"
      ? location.pathname === "/" || location.pathname.startsWith("/jobs")
      : location.pathname.startsWith(to);
  const settingsOpen = location.pathname.startsWith("/settings");

  return (
    <LiveDataProvider
      jobs={liveData.jobs}
      speed={liveData.speed}
      isPaused={liveData.isPaused}
      downloadBlock={liveData.downloadBlock}
      connection={liveData.connection}
    >
      <div className="flex h-screen overflow-hidden bg-background text-foreground">
        {/* Desktop sidebar */}
        <aside className="hidden w-52 shrink-0 flex-col border-r border-border bg-card/40 backdrop-blur-md md:flex lg:w-[248px]">
          <div className="flex items-start justify-between border-b border-border px-5 py-5">
            <Link to="/" className="min-w-0">
              <div className="font-space-grotesk text-[22px] font-bold leading-none tracking-tight text-foreground">
                Weaver
              </div>
              <div className="mt-1.5 text-[10.5px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">
                Queue Control
              </div>
            </Link>
            <ThemeToggle />
          </div>

          <nav className="flex-1 overflow-y-auto px-3 py-3.5">
            <div className="space-y-1">
              {navItems.map((item) => {
                const Icon = item.icon;
                const topLevelActive = isActive(item.to);
                return (
                  <div key={item.to}>
                    <Link
                      to={item.to}
                      className={cn(
                        "flex items-center gap-3 rounded-[10px] px-3 py-2.5 text-[13.5px] transition-colors",
                        topLevelActive
                          ? "bg-primary font-semibold text-primary-foreground shadow-[0_8px_20px_-10px_var(--primary)]"
                          : "font-medium text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                      )}
                    >
                      <Icon className="size-[18px]" />
                      <span>{t(item.labelKey)}</span>
                    </Link>

                    {item.to === "/settings" && settingsOpen ? (
                      <div className="mt-1 mb-1 ml-5 space-y-0.5 border-l border-border pl-3">
                        {settingsNav.map((entry) => {
                          const childActive = location.pathname === entry.to;
                          return (
                            <Link
                              key={entry.to}
                              to={entry.to}
                              className={cn(
                                "block rounded-lg px-2.5 py-1.5 text-[13px] transition-colors",
                                childActive
                                  ? "bg-accent font-semibold text-foreground"
                                  : "font-medium text-muted-foreground hover:bg-accent/40 hover:text-foreground",
                              )}
                            >
                              {t(entry.labelKey)}
                            </Link>
                          );
                        })}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </nav>

          <div className="mt-auto flex flex-col gap-3 border-t border-border p-4">
            <div className="rounded-inner border border-border bg-card px-3.5 py-3">
              <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                {t("label.downloadSpeed")}
              </div>
              <div className="mt-1 font-space-grotesk text-xl font-bold text-foreground">
                {formatSpeed(liveData.speed)}
              </div>
              <Sparkline
                values={speedHistory}
                className="mt-1.5 text-status-completed"
                height={26}
                capValue={liveData.downloadBlock.scheduledSpeedLimit || null}
              />
            </div>
            <Button onClick={() => setUploadOpen(true)} className="w-full">
              <FolderUp className="size-4" />
              {t("nav.upload")}
            </Button>
            {versionData?.version ? (
              <div className="text-center text-[11px] tracking-wide text-muted-foreground/70">
                v{versionData.version}
              </div>
            ) : null}
          </div>
        </aside>

        {/* Main column */}
        <main className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <header className="flex flex-none items-center gap-3 border-b border-border bg-card/50 px-4 py-3 backdrop-blur md:hidden">
            <button
              type="button"
              onClick={() => setMobileNavOpen(true)}
              aria-label="Open navigation"
              className="flex size-9 items-center justify-center rounded-[9px] border border-border bg-card text-foreground"
            >
              <Menu className="size-4" />
            </button>
            <span className="font-space-grotesk text-lg font-bold tracking-tight">Weaver</span>
            <span className="ml-auto font-space-grotesk text-[15px] font-bold text-foreground">
              {formatSpeed(liveData.speed)}
            </span>
          </header>

          {liveData.connection.isDisconnected ? (
            <div className="flex-none">
              <DisconnectBanner
                title={t("connection.disconnectedTitle")}
                message={disconnectBannerMessage}
              />
            </div>
          ) : null}

          <div className="flex-1 overflow-y-auto">
            <div className="mx-auto w-full max-w-[1600px] px-4 py-5 sm:px-6 lg:px-8 lg:py-8">
              <RoutedOutlet />
            </div>
          </div>
        </main>
      </div>

      <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
        <SheetContent side="left" className="w-[280px] border-border bg-card sm:max-w-[280px]">
          <SheetHeader className="border-b border-border px-5 py-5 text-left">
            <SheetTitle className="font-space-grotesk text-xl font-bold text-foreground">
              Weaver
            </SheetTitle>
            <SheetDescription className="text-[10.5px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">
              Queue Control
            </SheetDescription>
          </SheetHeader>
          <div className="flex min-h-0 flex-1 flex-col">
            <nav className="flex-1 overflow-y-auto px-3 py-4">
              <div className="space-y-1">
                {navItems.map((item) => {
                  const Icon = item.icon;
                  const topLevelActive = isActive(item.to);
                  return (
                    <div key={item.to}>
                      <Link
                        to={item.to}
                        onClick={() => setMobileNavOpen(false)}
                        className={cn(
                          "flex items-center gap-3 rounded-[10px] px-4 py-3 text-sm transition-colors",
                          topLevelActive
                            ? "bg-primary font-semibold text-primary-foreground"
                            : "font-medium text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                        )}
                      >
                        <Icon className="size-[18px]" />
                        <span>{t(item.labelKey)}</span>
                      </Link>

                      {item.to === "/settings" && settingsOpen ? (
                        <div className="mt-1 mb-1 ml-5 space-y-0.5 border-l border-border pl-4">
                          {settingsNav.map((entry) => {
                            const childActive = location.pathname === entry.to;
                            return (
                              <Link
                                key={entry.to}
                                to={entry.to}
                                onClick={() => setMobileNavOpen(false)}
                                className={cn(
                                  "block rounded-lg px-3 py-2 text-[13px] transition-colors",
                                  childActive
                                    ? "bg-accent font-semibold text-foreground"
                                    : "font-medium text-muted-foreground hover:bg-accent/40 hover:text-foreground",
                                )}
                              >
                                {t(entry.labelKey)}
                              </Link>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            </nav>

            <div className="mt-auto flex flex-col gap-3 border-t border-border p-4">
              <div className="rounded-inner border border-border bg-card px-3.5 py-3">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                  {t("label.downloadSpeed")}
                </div>
                <div className="mt-1 font-space-grotesk text-xl font-bold text-foreground">
                  {formatSpeed(liveData.speed)}
                </div>
                <Sparkline
                  values={speedHistory}
                  className="mt-1.5 text-status-completed"
                  height={26}
                  capValue={liveData.downloadBlock.scheduledSpeedLimit || null}
                />
              </div>
              <Button
                onClick={() => {
                  setMobileNavOpen(false);
                  setUploadOpen(true);
                }}
                className="w-full"
              >
                <FolderUp className="size-4" />
                {t("nav.upload")}
              </Button>
            </div>
          </div>
        </SheetContent>
      </Sheet>

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
      <PwaUpdateBanner />
    </LiveDataProvider>
  );
}

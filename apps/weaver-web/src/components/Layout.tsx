import { useEffect, useMemo, useRef, useState } from "react";
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
import { SpeedDisplay, formatSpeed } from "@/components/SpeedDisplay";
import { UploadModal } from "@/components/UploadModal";
import { LiveDataContext, type DownloadBlockState } from "@/lib/context/live-data-context";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import {
  normalizeJobData,
  type GraphqlJobData,
  type JobData,
} from "@/lib/job-types";
import { useTranslate } from "@/lib/context/translate-context";
import { usePwa } from "@/lib/context/pwa-context";
import { settingsNav } from "@/pages/settings/SettingsLayout";
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

function mapQueueSnapshot(snapshot: QueueSnapshotPayload | undefined): Snapshot | undefined {
  if (!snapshot) {
    return undefined;
  }

  return {
    latestCursor: snapshot.latestCursor,
    globalState: snapshot.globalState,
    jobs: (snapshot.items ?? []).map((job) => normalizeJobData(job)),
  };
}

function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <Button
      variant="ghost"
      size="icon"
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
    >
      {theme === "dark" ? (
        <MoonStar className="size-4" />
      ) : theme === "light" ? (
        <Sun className="size-4" />
      ) : (
        <Monitor className="size-4" />
      )}
    </Button>
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

  const snapshot = useMemo(
    () =>
      mapQueueSnapshot(
        polledSnapshot ?? snapshotSubscriptionData?.queueSnapshots ?? queryData?.queueSnapshot,
      ),
    [polledSnapshot, queryData, snapshotSubscriptionData],
  );

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
    <LiveDataContext.Provider value={liveData}>
      <div className="min-h-screen bg-background text-foreground">
        <div className="mx-auto w-full max-w-[1587px] px-3 py-3 sm:px-4 sm:py-4">
          <div className="relative overflow-hidden rounded-[28px] border border-border/70 bg-card/60 shadow-[0_20px_80px_rgba(15,23,42,0.12)] backdrop-blur-md dark:shadow-[0_24px_90px_rgba(2,6,23,0.45)]">
            <div className="pointer-events-none absolute inset-x-0 top-0 h-56 bg-gradient-to-br from-primary/10 via-transparent to-transparent" />
            {liveData.connection.isDisconnected ? (
              <div className="relative z-20">
                <DisconnectBanner
                  title={t("connection.disconnectedTitle")}
                  message={disconnectBannerMessage}
                />
              </div>
            ) : null}
            <div className="relative grid min-h-[calc(100vh-1.5rem)] md:grid-cols-[224px_minmax(0,1fr)]">
              <aside className="hidden border-r border-border/60 bg-background/90 md:flex md:flex-col">
                <div className="flex items-center justify-between border-b border-border/60 px-4 py-4">
                  <Link to="/">
                    <div className="font-space-grotesk text-lg font-semibold tracking-tight text-foreground">
                      Weaver
                    </div>
                    <div className="text-xs uppercase tracking-[0.22em] text-muted-foreground">
                      Queue Control
                    </div>
                  </Link>
                  <ThemeToggle />
                </div>

                <nav className="flex-1 px-2 py-3">
                  <div className="space-y-1">
                    {navItems.map((item) => {
                      const Icon = item.icon;
                      const topLevelActive = isActive(item.to);
                      return (
                        <div key={item.to}>
                          <Link
                            to={item.to}
                            className={`flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm transition ${
                              topLevelActive
                                ? "bg-primary/14 font-medium text-foreground"
                                : "text-muted-foreground hover:bg-accent/40 hover:text-foreground"
                            }`}
                          >
                            <Icon className="size-4" />
                            <span>{t(item.labelKey)}</span>
                          </Link>

                          {item.to === "/settings" && settingsOpen ? (
                            <div className="mt-1 ml-4 space-y-1 border-l border-border/50 pl-3">
                              {settingsNav.map((entry) => {
                                const childActive = location.pathname === entry.to;
                                return (
                                  <Link
                                    key={entry.to}
                                    to={entry.to}
                                    className={`block rounded-lg px-2.5 py-1.5 text-sm transition ${
                                      childActive
                                        ? "bg-primary/10 font-medium text-foreground"
                                        : "text-muted-foreground hover:bg-accent/30 hover:text-foreground"
                                    }`}
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

                <div className="mt-auto space-y-3 border-t border-border/60 px-3 py-3">
                  <div className="rounded-2xl border border-border/70 bg-background/70 px-3 py-2.5">
                    <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                      {t("label.downloadSpeed")}
                    </div>
                    <SpeedDisplay
                      bytesPerSec={liveData.speed}
                      className="mt-1 text-base font-semibold text-foreground"
                    />
                  </div>
                  <Button onClick={() => setUploadOpen(true)} className="w-full">
                    <FolderUp className="size-4" />
                    {t("nav.upload")}
                  </Button>
                  {versionData?.version ? (
                    <div className="text-center text-[10px] text-muted-foreground/60">
                      v{versionData.version}
                    </div>
                  ) : null}
                </div>
              </aside>

              <main className="min-w-0 bg-transparent">
                <header className="sticky top-0 z-10 flex items-center justify-between border-b border-border/60 bg-background/75 px-4 py-3 backdrop-blur md:hidden">
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setMobileNavOpen(true)}
                    >
                      <Menu className="size-4" />
                    </Button>
                    <span className="font-space-grotesk text-lg font-semibold tracking-tight">
                      Weaver
                    </span>
                  </div>
                  <ThemeToggle />
                </header>

                <div className="w-full px-4 py-6 sm:px-6 md:px-8 md:py-8">
                  <Outlet />
                </div>
              </main>
            </div>
          </div>
        </div>

        <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
          <SheetContent side="left" className="w-[280px] p-0 sm:max-w-[280px]">
            <SheetHeader className="border-b border-border/60 px-5 py-5 text-left">
              <SheetTitle className="font-space-grotesk text-lg">Weaver</SheetTitle>
              <SheetDescription>Queue Control</SheetDescription>
            </SheetHeader>
            <div className="flex h-full flex-col bg-background">
              <nav className="flex-1 px-3 py-4">
                <div className="space-y-1">
                  {navItems.map((item) => {
                    const Icon = item.icon;
                    const topLevelActive = isActive(item.to);
                    return (
                      <div key={item.to}>
                        <Link
                          to={item.to}
                          onClick={() => setMobileNavOpen(false)}
                          className={`flex items-center gap-3 rounded-xl px-4 py-3 text-sm transition ${
                            topLevelActive
                              ? "bg-primary/14 font-medium text-foreground"
                              : "text-muted-foreground hover:bg-accent/40 hover:text-foreground"
                          }`}
                        >
                          <Icon className="size-4" />
                          <span>{t(item.labelKey)}</span>
                        </Link>

                        {item.to === "/settings" && settingsOpen ? (
                          <div className="mt-1 ml-5 space-y-1 border-l border-border/50 pl-4">
                            {settingsNav.map((entry) => {
                              const childActive = location.pathname === entry.to;
                              return (
                                <Link
                                  key={entry.to}
                                  to={entry.to}
                                  onClick={() => setMobileNavOpen(false)}
                                  className={`block rounded-lg px-3 py-2 text-sm transition ${
                                    childActive
                                      ? "bg-primary/10 font-medium text-foreground"
                                      : "text-muted-foreground hover:bg-accent/30 hover:text-foreground"
                                  }`}
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

              <div className="space-y-4 border-t border-border/60 px-4 py-4">
                <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3">
                  <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                    {t("label.downloadSpeed")}
                  </div>
                  <SpeedDisplay
                    bytesPerSec={liveData.speed}
                    className="mt-1 text-base font-semibold text-foreground"
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
      </div>
    </LiveDataContext.Provider>
  );
}

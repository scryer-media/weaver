import { memo, useEffect, useMemo, useRef, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { useTheme } from "next-themes";
import { toast } from "sonner";
import {
  Activity,
  Clock3,
  FolderUp,
  Heart,
  Info,
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
import { useGraphqlConnectionState } from "@/graphql/client";
import {
  LIVE_METRICS_QUERY,
  LIVE_METRICS_SUBSCRIPTION,
  VERSION_QUERY,
} from "@/graphql/queries";
import { formatSpeed } from "@/components/SpeedDisplay";
import { Badge } from "@/components/ui/badge";
import { Sparkline } from "@/components/ui/sparkline";
import { UploadModal } from "@/components/UploadModal";
import { useSpeedHistory } from "@/lib/hooks/use-speed-history";
import { LiveDataProvider, type DownloadBlockState } from "@/lib/context/live-data-context";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import type { JobData } from "@/lib/job-types";
import { useTranslate } from "@/lib/context/translate-context";
import { usePwa } from "@/lib/context/pwa-context";
import { settingsNav } from "@/pages/settings/settings-nav";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";

const navItems = [
  { to: "/", labelKey: "nav.jobs", icon: ListOrdered },
  { to: "/history", labelKey: "nav.history", icon: Clock3 },
  { to: "/monitoring", labelKey: "nav.monitoring", icon: Activity },
  { to: "/system-info", labelKey: "nav.systemInfo", icon: Info },
  { to: "/logs", labelKey: "nav.logs", icon: ScrollText },
  { to: "/settings", labelKey: "nav.settings", icon: Settings },
];

interface GlobalQueueState {
  globalState: {
    isPaused: boolean;
    downloadBlock: DownloadBlockState;
  };
}

interface LiveMetricsSnapshot {
  metrics: { currentDownloadSpeed: number };
  globalState: GlobalQueueState["globalState"];
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
const DEFAULT_GLOBAL_STATE: GlobalQueueState["globalState"] = {
  isPaused: false,
  downloadBlock: DEFAULT_DOWNLOAD_BLOCK,
};
const RECONNECT_TOAST_ID = "graphql-connection";

const RoutedOutlet = memo(function RoutedOutlet() {
  return <Outlet />;
});

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

function SponsorLink({ label }: { label: string }) {
  return (
    <a
      href="https://www.scryer.media/weaver/donate/"
      target="_blank"
      rel="noreferrer"
      className="flex items-center justify-center gap-1.5 rounded-[9px] px-2 py-1 text-[11.5px] font-medium text-muted-foreground/80 transition-colors hover:bg-accent/40 hover:text-foreground"
    >
      <Heart className="size-3.5 text-status-failed/70" aria-hidden="true" />
      <span>{label}</span>
    </a>
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
  const [polledMetrics, setPolledMetrics] = useState<LiveMetricsSnapshot | undefined>();
  const [subscriptionMetrics, setSubscriptionMetrics] = useState<LiveMetricsSnapshot>();
  const lastMetricsSubscriptionErrorRef = useRef<string | null>(null);
  const lastMetricsConnectionAtRef = useRef<number | null | undefined>(undefined);
  const connectionState = useGraphqlConnectionState();

  const [{ data: metricsQueryData }, reexecuteMetricsQuery] = useQuery<LiveMetricsSnapshot>({
    query: LIVE_METRICS_QUERY,
  });
  const [{ data: versionData }] = useQuery<{ version: string }>({
    query: VERSION_QUERY,
  });
  const [{ data: metricsSubscriptionData, error: metricsSubscriptionError }] = useSubscription<{
    systemMetricsUpdates: LiveMetricsSnapshot;
  }>({
    query: LIVE_METRICS_SUBSCRIPTION,
  });

  const reconnectMetricsPolling = useReconnectPolling<LiveMetricsSnapshot>({
    enabled: connectionState.status === "disconnected",
    query: LIVE_METRICS_QUERY,
    onData: (nextSnapshot) => {
      setPolledMetrics(nextSnapshot);
    },
  });

  useEffect(() => {
    if (connectionState.status === "connected" && metricsSubscriptionData?.systemMetricsUpdates) {
      setSubscriptionMetrics(metricsSubscriptionData.systemMetricsUpdates);
      setPolledMetrics(undefined);
    }
  }, [connectionState.status, metricsSubscriptionData]);

  useEffect(() => {
    if (connectionState.status !== "connected") {
      setSubscriptionMetrics(undefined);
      return;
    }
    if (connectionState.lastConnectedAt === null) {
      return;
    }
    if (lastMetricsConnectionAtRef.current === undefined) {
      lastMetricsConnectionAtRef.current = connectionState.lastConnectedAt;
      return;
    }
    if (lastMetricsConnectionAtRef.current === connectionState.lastConnectedAt) {
      return;
    }
    lastMetricsConnectionAtRef.current = connectionState.lastConnectedAt;
    setSubscriptionMetrics(undefined);
    void reexecuteMetricsQuery({ requestPolicy: "network-only" });
  }, [
    connectionState.lastConnectedAt,
    connectionState.status,
    reexecuteMetricsQuery,
  ]);

  useEffect(() => {
    if (!metricsSubscriptionError) {
      lastMetricsSubscriptionErrorRef.current = null;
      return;
    }
    const errorKey = metricsSubscriptionError.message;
    if (lastMetricsSubscriptionErrorRef.current === errorKey) {
      return;
    }
    lastMetricsSubscriptionErrorRef.current = errorKey;
    setSubscriptionMetrics(undefined);
    void reexecuteMetricsQuery({ requestPolicy: "network-only" });
  }, [metricsSubscriptionError, reexecuteMetricsQuery]);

  const metricsSnapshot =
    polledMetrics ?? subscriptionMetrics ?? metricsQueryData;
  const currentGlobalState = metricsSnapshot?.globalState ?? DEFAULT_GLOBAL_STATE;
  const isPolling = reconnectMetricsPolling.isPolling;
  const liveData = useMemo(
    () => ({
      jobs: EMPTY_JOBS,
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
    ],
  );
  const disconnectBannerMessage = liveData.connection.isPolling
    ? t("connection.pollingBody")
    : t("connection.retryingBody");
  const speedHistory = useSpeedHistory(liveData.speed);

  useEffect(() => {
    if (!liveData.connection.isDisconnected) {
      toast.dismiss(RECONNECT_TOAST_ID);
      return;
    }

    toast.loading(t("connection.disconnectedTitle"), {
      id: RECONNECT_TOAST_ID,
      description: disconnectBannerMessage,
      duration: Infinity,
      dismissible: true,
      icon: <Unplug className="size-4 text-amber-500" />,
    });
  }, [disconnectBannerMessage, liveData.connection.isDisconnected, t]);

  useEffect(() => () => {
    toast.dismiss(RECONNECT_TOAST_ID);
  }, []);

  const lastTitleUpdate = useRef(0);
  useEffect(() => {
    const now = Date.now();
    const isIdle = !liveData.isPaused && liveData.speed === 0;
    const isPaused = liveData.isPaused;
    if (!isPaused && !isIdle && now - lastTitleUpdate.current < 2500) return;
    lastTitleUpdate.current = now;

    if (isPaused) {
      document.title = "Paused - Weaver";
      return;
    }

    if (liveData.speed > 0) {
      document.title = `${formatSpeed(liveData.speed)} - Weaver`;
      return;
    }

    document.title = "Weaver";
  }, [liveData]);

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
                                "flex items-center justify-between gap-2 rounded-lg px-2.5 py-1.5 text-[13px] transition-colors",
                                childActive
                                  ? "bg-accent font-semibold text-foreground"
                                  : "font-medium text-muted-foreground hover:bg-accent/40 hover:text-foreground",
                              )}
                            >
                              <span>{t(entry.labelKey)}</span>
                              {entry.beta ? (
                                <Badge variant="secondary" className="px-1 py-0 text-[9px] uppercase tracking-[0.08em]">
                                  Beta
                                </Badge>
                              ) : null}
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
            <div className="flex flex-col gap-1">
              <SponsorLink label={t("nav.sponsor")} />
              {versionData?.version ? (
                <div className="text-center text-[11px] tracking-wide text-muted-foreground/70">
                  v{versionData.version}
                </div>
              ) : null}
            </div>
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
                                "flex items-center justify-between gap-2 rounded-lg px-3 py-2 text-[13px] transition-colors",
                                childActive
                                  ? "bg-accent font-semibold text-foreground"
                                  : "font-medium text-muted-foreground hover:bg-accent/40 hover:text-foreground",
                              )}
                            >
                              <span>{t(entry.labelKey)}</span>
                              {entry.beta ? (
                                <Badge variant="secondary" className="px-1 py-0 text-[9px] uppercase tracking-[0.08em]">
                                  Beta
                                </Badge>
                              ) : null}
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
              <SponsorLink label={t("nav.sponsor")} />
            </div>
          </div>
        </SheetContent>
      </Sheet>

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
      <PwaUpdateBanner />
    </LiveDataProvider>
  );
}

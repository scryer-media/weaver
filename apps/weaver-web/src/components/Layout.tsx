import { Link, useLocation, Outlet } from "react-router";
import { useSubscription } from "urql";
import { useTheme } from "next-themes";
import { useCallback, useState } from "react";
import { JOB_UPDATES_SUBSCRIPTION } from "@/graphql/queries";
import { SpeedDisplay } from "@/components/SpeedDisplay";
import { UploadModal } from "@/components/UploadModal";
import { useTranslate } from "@/lib/context/translate-context";
import { LiveDataContext } from "@/lib/context/live-data-context";

const navItems = [
  { to: "/", labelKey: "nav.jobs", icon: "jobs" },
  { to: "/history", labelKey: "nav.history", icon: "history" },
  { to: "/upload", labelKey: "nav.upload", icon: "upload" },
  { to: "/servers", labelKey: "nav.servers", icon: "servers" },
  { to: "/settings", labelKey: "nav.settings", icon: "settings" },
];

interface Snapshot {
  jobs: { id: number; name: string; status: string; progress: number; totalBytes: number; downloadedBytes: number; hasPassword: boolean; category: string | null }[];
  metrics: { currentDownloadSpeed: number };
  isPaused: boolean;
}

function NavIcon({ icon, size = 20 }: { icon: string; size?: number }) {
  const props = { xmlns: "http://www.w3.org/2000/svg", width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 2, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  switch (icon) {
    case "jobs":
      return <svg {...props}><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" /><polyline points="3.27 6.96 12 12.01 20.73 6.96" /><line x1="12" y1="22.08" x2="12" y2="12" /></svg>;
    case "history":
      return <svg {...props}><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>;
    case "upload":
      return <svg {...props}><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="17 8 12 3 7 8" /><line x1="12" y1="3" x2="12" y2="15" /></svg>;
    case "servers":
      return <svg {...props}><rect x="2" y="2" width="20" height="8" rx="2" ry="2" /><rect x="2" y="14" width="20" height="8" rx="2" ry="2" /><line x1="6" y1="6" x2="6.01" y2="6" /><line x1="6" y1="18" x2="6.01" y2="18" /></svg>;
    case "settings":
      return <svg {...props}><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" /></svg>;
    default:
      return null;
  }
}

function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  const cycle = () => {
    if (theme === "dark") setTheme("light");
    else if (theme === "light") setTheme("system");
    else setTheme("dark");
  };

  return (
    <button
      onClick={cycle}
      className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
      title={theme === "dark" ? "Dark" : theme === "light" ? "Light" : "System"}
    >
      {theme === "dark" ? (
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z" /></svg>
      ) : theme === "light" ? (
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="4" /><path d="M12 2v2" /><path d="M12 20v2" /><path d="m4.93 4.93 1.41 1.41" /><path d="m17.66 17.66 1.41 1.41" /><path d="M2 12h2" /><path d="M20 12h2" /><path d="m6.34 17.66-1.41 1.41" /><path d="m19.07 4.93-1.41 1.41" /></svg>
      ) : (
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect width="20" height="14" x="2" y="3" rx="2" /><line x1="8" x2="16" y1="21" y2="21" /><line x1="12" x2="12" y1="17" y2="21" /></svg>
      )}
    </button>
  );
}

export function Layout() {
  const t = useTranslate();
  const location = useLocation();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);

  const handleSubscription = useCallback(
    (_prev: Snapshot | undefined, response: { jobUpdates: Snapshot }) => {
      return response.jobUpdates;
    },
    [],
  );

  const [{ data }] = useSubscription(
    { query: JOB_UPDATES_SUBSCRIPTION },
    handleSubscription,
  );

  const liveData = {
    jobs: data?.jobs ?? [],
    speed: data?.metrics?.currentDownloadSpeed ?? 0,
    isPaused: data?.isPaused ?? false,
  };

  const isActive = (to: string) =>
    to === "/"
      ? location.pathname === "/" || location.pathname.startsWith("/jobs")
      : location.pathname.startsWith(to);

  return (
    <LiveDataContext.Provider value={liveData}>
      <div className="flex min-h-screen bg-background text-foreground">
        {/* Desktop sidebar */}
        <aside className="hidden w-56 flex-col border-r border-sidebar-border bg-sidebar md:flex">
          <div className="flex h-14 items-center justify-between border-b border-sidebar-border px-5">
            <span className="text-lg font-bold tracking-tight text-foreground">Weaver</span>
            <ThemeToggle />
          </div>
          <nav className="flex flex-1 flex-col gap-1 p-3">
            {navItems.map((item) =>
              item.to === "/upload" ? (
                <button
                  key={item.to}
                  onClick={() => setUploadOpen(true)}
                  className="flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
                >
                  <NavIcon icon={item.icon} size={18} />
                  {t(item.labelKey)}
                </button>
              ) : (
                <Link
                  key={item.to}
                  to={item.to}
                  className={`flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors ${
                    isActive(item.to)
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
                  }`}
                >
                  <NavIcon icon={item.icon} size={18} />
                  {t(item.labelKey)}
                </Link>
              ),
            )}
          </nav>
          <div className="border-t border-sidebar-border p-4">
            <div className="text-xs text-muted-foreground">{t("label.downloadSpeed")}</div>
            <SpeedDisplay
              bytesPerSec={liveData.speed}
              className="text-foreground"
            />
          </div>
        </aside>

        {/* Mobile header */}
        <div className="flex flex-1 flex-col md:overflow-hidden">
          <header className="flex h-14 items-center justify-between border-b border-border bg-sidebar px-4 md:hidden">
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="rounded-md p-2 text-foreground hover:bg-accent"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                {mobileMenuOpen ? (
                  <><path d="M18 6 6 18" /><path d="m6 6 12 12" /></>
                ) : (
                  <><line x1="4" x2="20" y1="12" y2="12" /><line x1="4" x2="20" y1="6" y2="6" /><line x1="4" x2="20" y1="18" y2="18" /></>
                )}
              </svg>
            </button>
            <span className="text-lg font-bold tracking-tight text-foreground">Weaver</span>
            <ThemeToggle />
          </header>

          {/* Mobile nav dropdown */}
          {mobileMenuOpen && (
            <nav className="border-b border-border bg-sidebar p-2 md:hidden">
              {navItems.map((item) =>
                item.to === "/upload" ? (
                  <button
                    key={item.to}
                    onClick={() => {
                      setMobileMenuOpen(false);
                      setUploadOpen(true);
                    }}
                    className="flex w-full items-center gap-3 rounded-md px-3 py-3 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
                  >
                    <NavIcon icon={item.icon} size={18} />
                    {t(item.labelKey)}
                  </button>
                ) : (
                  <Link
                    key={item.to}
                    to={item.to}
                    onClick={() => setMobileMenuOpen(false)}
                    className={`flex items-center gap-3 rounded-md px-3 py-3 text-sm font-medium transition-colors ${
                      isActive(item.to)
                        ? "bg-sidebar-accent text-sidebar-accent-foreground"
                        : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground"
                    }`}
                  >
                    <NavIcon icon={item.icon} size={18} />
                    {t(item.labelKey)}
                  </Link>
                ),
              )}
              <div className="mt-2 border-t border-sidebar-border px-3 pt-2">
                <div className="text-xs text-muted-foreground">{t("label.downloadSpeed")}</div>
                <SpeedDisplay
                  bytesPerSec={liveData.speed}
                  className="text-foreground"
                />
              </div>
            </nav>
          )}

          <main className="flex-1 overflow-auto">
            <Outlet />
          </main>
        </div>
      </div>
      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
    </LiveDataContext.Provider>
  );
}

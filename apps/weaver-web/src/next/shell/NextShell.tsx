import { useEffect, useState, type ReactNode } from "react";
import { NavLink, useLocation } from "react-router";
import { cn } from "@/lib/utils";
import { useNextData } from "../data/next-data";
import { Eyebrow } from "../components/chrome";

/**
 * The chrome every Next screen shares: a 236px rail, a 56px top bar, the
 * scrolling content region, and a 34px status bar.
 *
 * Only the rail and the content region scroll. Anything a screen wants to keep
 * pinned above its list — a metric strip, a tab bar, a filter bar — is passed
 * as `beforeContent` so it sits outside the scroller.
 *
 * Below 1024px the rail is the one region that cannot simply narrow: at 236px
 * it is most of a phone. It becomes a drawer over the content instead, holding
 * the same blocks, and the top bar grows the button that opens it. The button
 * lives here rather than in the header markup so a screen that replaces the
 * whole title bar (job detail) keeps it without knowing the drawer exists.
 */

interface NavEntry {
  to: string;
  label: string;
  /** Right-aligned mono count; omitted entries render nothing. */
  count?: number;
  end?: boolean;
}

export function NextShell({
  title,
  note,
  controls,
  header,
  railMiddle,
  railFooter,
  beforeContent,
  afterContent,
  statusNote,
  statusRight,
  contentClassName,
  children,
}: {
  /** Required unless `header` replaces the whole title bar. */
  title?: string;
  note?: ReactNode;
  controls?: ReactNode;
  /**
   * Replaces the title bar wholesale. Job detail needs a header that wraps —
   * a back link, a title, a state chip and four actions — which is a different
   * shape from `title` + `controls`, not a longer version of it.
   */
  header?: ReactNode;
  railMiddle?: ReactNode;
  railFooter?: ReactNode;
  beforeContent?: ReactNode;
  /** Pinned below the scrolling region: a pagination bar, nothing else so far. */
  afterContent?: ReactNode;
  /** Sits after the connection chip, behind a separator. */
  statusNote?: ReactNode;
  statusRight?: ReactNode;
  contentClassName?: string;
  children: ReactNode;
}) {
  const { version, queue, historyCount, connection } = useNextData();
  const [navOpen, setNavOpen] = useState(false);
  const { pathname } = useLocation();

  // Following a link out of the drawer should leave the drawer behind.
  useEffect(() => {
    setNavOpen(false);
  }, [pathname]);

  useEffect(() => {
    if (!navOpen) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setNavOpen(false);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [navOpen]);

  // The prototypes each wire up only their own screens, so their rails show
  // different subsets. In the product the list is always whole — otherwise
  // Monitoring is unreachable from Transfers.
  const nav: NavEntry[] = [
    { to: "/", label: "Transfers", count: queue.summary.totalItems, end: false },
    { to: "/history", label: "Completed", count: historyCount },
    { to: "/monitoring", label: "Monitoring" },
    { to: "/system-info", label: "System info" },
    { to: "/logs", label: "Logs" },
    { to: "/settings", label: "Settings" },
  ];

  // Not a component: the aside and the drawer must render the same blocks, and
  // splitting it into one would put the rail's props back in a second signature.
  const renderRail = (onDismiss?: () => void) => (
    <>
      <div className="flex h-14 flex-none items-center gap-[9px] border-b border-wv-line-strong px-[18px]">
        <span aria-hidden="true" className="size-[18px] flex-none bg-wv-accent" />
        <span className="text-[14px] font-semibold tracking-[-0.01em]">Weaver</span>
        <span className="ml-auto font-wv-mono text-[10px] text-wv-faint">{version || "—"}</span>
        {onDismiss === undefined ? null : (
          <button
            type="button"
            onClick={onDismiss}
            aria-label="Close navigation"
            className="-mr-[7px] flex size-7 flex-none cursor-pointer items-center justify-center text-[15px] text-wv-muted hover:text-wv-fg"
          >
            <span aria-hidden="true">×</span>
          </button>
        )}
      </div>

        <nav className="flex flex-col gap-0.5 px-[10px] py-[14px]">
          {nav.map((entry) => (
            <NavLink
              key={entry.to}
              to={entry.to}
              end={entry.to === "/"}
              className={({ isActive }) =>
                cn(
                  "flex h-8 items-center gap-[10px] px-[10px] text-[13.5px] hover:bg-wv-nav-hover",
                  isActive ? "bg-wv-nav-active font-semibold text-wv-strong" : "text-wv-fg",
                )
              }
            >
              {({ isActive }) => (
                <>
                  <span
                    aria-hidden="true"
                    className={cn("h-[14px] w-[3px] flex-none", isActive && "bg-wv-accent")}
                  />
                  <span className="truncate">{entry.label}</span>
                  {entry.count === undefined ? null : (
                    <span className="ml-auto font-wv-mono text-[11px] text-wv-faint">
                      {entry.count}
                    </span>
                  )}
                </>
              )}
            </NavLink>
          ))}
        </nav>

      {railMiddle}
      <div className="mt-auto flex flex-none flex-col">{railFooter}</div>
    </>
  );

  return (
    <div className="flex h-dvh overflow-hidden bg-wv-app text-wv-fg">
      <aside className="hidden min-h-0 w-[236px] flex-none flex-col overflow-y-auto border-r border-wv-line-strong bg-wv-rail lg:flex">
        {renderRail()}
      </aside>

      {navOpen ? (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div
            className="absolute inset-0 bg-[rgb(0_0_0_/_0.55)]"
            onPointerDown={() => setNavOpen(false)}
          />
          <aside className="absolute inset-y-0 left-0 flex w-[264px] max-w-[86vw] flex-col overflow-y-auto border-r border-wv-line-strong bg-wv-rail">
            {renderRail(() => setNavOpen(false))}
          </aside>
        </div>
      ) : null}

      <div className="relative flex min-w-0 flex-1 flex-col">
        <button
          type="button"
          onClick={() => setNavOpen(true)}
          aria-label="Open navigation"
          aria-expanded={navOpen}
          className="absolute top-0 left-0 z-20 flex h-14 w-12 cursor-pointer items-center justify-center text-wv-muted hover:text-wv-fg lg:hidden"
        >
          <span aria-hidden="true" className="flex w-[15px] flex-col gap-[3.5px]">
            <span className="h-px bg-current" />
            <span className="h-px bg-current" />
            <span className="h-px bg-current" />
          </span>
        </button>

        {header ?? (
          <header className="flex min-h-14 flex-none flex-wrap items-center gap-x-4 gap-y-2 border-b border-wv-line-strong bg-wv-chrome py-2 pr-4 pl-12 sm:py-0 sm:pr-6 lg:pl-6">
            <div className="flex min-w-0 items-baseline gap-[10px]">
              <h1 className="flex-none text-[15px] font-semibold tracking-[-0.01em]">{title}</h1>
              {note === undefined ? null : (
                <span className="hidden truncate font-wv-mono text-[11.5px] text-wv-muted sm:inline">
                  {note}
                </span>
              )}
            </div>
            {controls === undefined ? null : (
              <div className="ml-auto flex min-w-0 flex-wrap items-center justify-end gap-x-[10px] gap-y-2">{controls}</div>
            )}
          </header>
        )}

        {beforeContent}

        <div className={cn("flex min-h-0 flex-1", contentClassName)}>{children}</div>

        {afterContent}

        <footer className="flex h-[34px] flex-none items-center gap-3 border-t border-wv-line-strong bg-wv-chrome px-4 font-wv-mono text-[11px] text-wv-muted sm:gap-4 sm:px-6">
          <span className="flex flex-none items-center gap-[7px] whitespace-nowrap">
            <span
              aria-hidden="true"
              className={cn(
                "size-1.5 flex-none",
                connection.isDisconnected ? "bg-wv-warn" : "bg-wv-accent",
              )}
            />
            {connection.isDisconnected
              ? connection.isPolling
                ? "Reconnecting — polling"
                : "Reconnecting"
              : "Connected"}
          </span>
          {statusNote === undefined ? null : (
            // The note is the first thing to go: on a phone the connection chip
            // and the right-hand figure are the two that have to survive.
            <>
              <span aria-hidden="true" className="hidden flex-none text-wv-inert md:inline">
                |
              </span>
              <span className="hidden min-w-0 truncate whitespace-nowrap md:inline">
                {statusNote}
              </span>
            </>
          )}
          {statusRight === undefined ? null : (
            <span className="ml-auto truncate whitespace-nowrap text-right">{statusRight}</span>
          )}
        </footer>
      </div>
    </div>
  );
}

/**
 * A rail block: the bordered, padded unit the rail is built from.
 *
 * `position="middle"` is the contextual block between the nav and the footer
 * (categories, settings panels, attention); `position="footer"` is one of the
 * bottom-pinned blocks, which carry a top border.
 */
export function RailBlock({
  eyebrow,
  position = "footer",
  className,
  children,
}: {
  eyebrow?: string;
  position?: "middle" | "footer";
  className?: string;
  children: ReactNode;
}) {
  return (
    <div
      className={cn(
        "flex flex-none flex-col",
        position === "footer"
          ? "gap-[7px] border-t border-wv-line-strong px-5 py-[18px]"
          : "gap-[10px] px-5 pt-2 pb-4",
        className,
      )}
    >
      {eyebrow ? <Eyebrow tone="rail">{eyebrow}</Eyebrow> : null}
      {children}
    </div>
  );
}

/** The rail's 25px metric: big value, small unit, mono note. */
export function RailMetric({
  value,
  unit,
  note,
  valueClassName,
}: {
  value: ReactNode;
  unit?: ReactNode;
  note?: ReactNode;
  valueClassName?: string;
}) {
  return (
    <>
      <div className="flex items-baseline gap-1.5">
        <span
          className={cn(
            "text-[25px] font-semibold leading-none tracking-[-0.02em]",
            valueClassName ?? "text-wv-fg",
          )}
        >
          {value}
        </span>
        {unit === undefined ? null : (
          <span className="text-[12px] text-wv-muted">{unit}</span>
        )}
      </div>
      {note === undefined ? null : (
        <div className="font-wv-mono text-[11px] text-wv-muted">{note}</div>
      )}
    </>
  );
}

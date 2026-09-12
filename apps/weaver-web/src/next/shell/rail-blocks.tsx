import type { ReactNode } from "react";
import { NavLink } from "react-router";
import { useQuery } from "urql";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { cn } from "@/lib/utils";
import { RailBlock, RailMetric } from "./NextShell";
import { Bar, Square } from "../components/chrome";
import { useNextData, type ProviderHealth } from "../data/next-data";
import { WV } from "../data/palette";
import { formatClock, formatLatency, splitSpeed, splitUptime } from "../data/format";

/**
 * The rail's reusable bottom blocks.
 *
 * Screens pick the ones that belong to them rather than each building its own:
 * Downloads takes Throughput + Providers, Settings takes the config path, and
 * the diagnostic screens take Attention + Uptime.
 */

export function ThroughputBlock() {
  const { speed, peakSpeed } = useNextData();
  const now = splitSpeed(speed);
  const peak = splitSpeed(peakSpeed);
  return (
    <RailBlock eyebrow="Throughput">
      <RailMetric
        value={now.value}
        unit={now.unit}
        note={peakSpeed > 0 ? `peak ${peak.value} ${peak.unit}` : "no traffic yet"}
      />
    </RailBlock>
  );
}

export function providerLoadPercent(provider: ProviderHealth): number {
  const max = provider.connectionsMax || provider.connectionsConfigured;
  if (!max) {
    return 0;
  }
  return (provider.connectionsActive / max) * 100;
}

export function ProvidersBlock() {
  const { providers } = useNextData();

  return (
    <RailBlock eyebrow="Providers" className="gap-3">
      {providers.length === 0 ? (
        <div className="font-wv-mono text-[11px] text-wv-muted">none configured</div>
      ) : (
        providers.map((provider) => {
          const load = providerLoadPercent(provider);
          const idle = provider.connectionsActive === 0;
          return (
            <div key={`${provider.host}:${provider.port}`} className="flex flex-col gap-[5px]">
              <div className="flex items-baseline justify-between gap-2 text-[12.5px]">
                <span className="truncate text-wv-tertiary">{provider.host}</span>
                <span className="flex-none font-wv-mono text-[11px] text-wv-muted">
                  {idle
                    ? "idle"
                    : `${provider.connectionsActive} / ${provider.connectionsMax || provider.connectionsConfigured}`}
                </span>
              </div>
              <Bar percent={load} color={idle ? WV.inert : WV.accent} height={7} />
            </div>
          );
        })
      )}
    </RailBlock>
  );
}

export interface AttentionItem {
  id: string;
  text: string;
  meta: string;
  color: string;
}

/**
 * What the daemon currently wants someone to know.
 *
 * Derived from state weaver already publishes — provider holdoffs from the
 * metrics stream, unhealthy servers from server health, and an active download
 * block — rather than from a dedicated alerts API.
 */
export function useAttentionItems(): AttentionItem[] {
  const { providers, holdoffs, downloadBlock, isPaused } = useNextData();
  const items: AttentionItem[] = [];

  for (const holdoff of holdoffs) {
    items.push({
      id: `holdoff:${holdoff.label}`,
      text: `${holdoff.label} is over its connection limit`,
      meta: `backing off until ${formatClock(holdoff.untilEpochMs)}`,
      color: WV.warn,
    });
  }

  for (const provider of providers) {
    if (provider.state === "healthy") {
      continue;
    }
    const consecutive = provider.consecutiveFailures;
    items.push({
      id: `provider:${provider.host}:${provider.port}`,
      text:
        provider.state === "disabled"
          ? `${provider.host} is disabled`
          : `${provider.host} is ${provider.state.replace("_", " ")}`,
      meta: `${consecutive} consecutive ${consecutive === 1 ? "failure" : "failures"} · ${formatLatency(provider.latencyMs)}`,
      color: provider.state === "disabled" ? WV.error : WV.warn,
    });
  }

  if (downloadBlock.kind !== "NONE" && !isPaused) {
    items.push({
      id: "download-block",
      text:
        downloadBlock.kind === "ISP_CAP"
          ? "Download cap reached"
          : downloadBlock.kind === "SERVER_QUOTA"
            ? "Provider quota reached"
            : "Downloads are held by a schedule",
      meta: downloadBlock.windowEndsAtEpochMs
        ? `resumes ${formatClock(downloadBlock.windowEndsAtEpochMs)}`
        : "see Settings → Bandwidth",
      color: WV.warn,
    });
  }

  return items;
}

export function AttentionBlock() {
  const items = useAttentionItems();

  return (
    <RailBlock eyebrow="Attention" position="middle" className="gap-3">
      {items.length === 0 ? (
        <div className="flex items-center gap-[9px] text-[12.5px] text-wv-muted">
          <Square color={WV.accent} />
          <span>Nothing needs attention</span>
        </div>
      ) : (
        items.map((item) => (
          <div key={item.id} className="flex gap-[9px]">
            <Square color={item.color} className="mt-[5px]" />
            <div className="flex min-w-0 flex-col gap-1">
              <div className="text-[12.5px] leading-[1.35] text-wv-secondary">{item.text}</div>
              <div className="font-wv-mono text-[10.5px] leading-[1.35] text-wv-faint">
                {item.meta}
              </div>
            </div>
          </div>
        ))
      )}
    </RailBlock>
  );
}

export function UptimeBlock() {
  const [{ data }] = useQuery<{ systemInfo: { uptimeSeconds: number; version: string } }>({
    query: SYSTEM_INFO_QUERY,
  });
  const uptime = splitUptime(data?.systemInfo?.uptimeSeconds ?? 0);

  return (
    <RailBlock eyebrow="Uptime">
      <RailMetric value={uptime.value} unit={uptime.unit} note="since the last restart" />
    </RailBlock>
  );
}

/** A rail footer block whose whole content is a path — Settings uses it. */
export function PathBlock({
  eyebrow,
  path,
  note,
}: {
  eyebrow: string;
  path: string | null | undefined;
  note?: string;
}) {
  return (
    <RailBlock eyebrow={eyebrow}>
      <div className="font-wv-mono text-[11px] leading-[1.5] break-all text-wv-tertiary">
        {path || "—"}
      </div>
      {note === undefined ? null : (
        <div className="font-wv-mono text-[10.5px] text-wv-faint">{note}</div>
      )}
    </RailBlock>
  );
}

export interface CategoryEntry {
  /** `null` is the "all categories" row; anything else is a real category name. */
  key: string | null;
  label: string;
  count: number;
  color: string;
}

/**
 * The rail's contextual middle block on the list screens.
 *
 * Counts come from `queuePage.categories` plus the jobs themselves, which the
 * server computes before it applies any filter — so switching category never
 * changes the numbers next to the other categories.
 */
export function CategoryListBlock({
  items,
  active,
  onSelect,
}: {
  items: readonly CategoryEntry[];
  active: string | null;
  onSelect: (key: string | null) => void;
}) {
  return (
    <RailBlock eyebrow="Categories" position="middle" className="gap-[9px]">
      {items.map((item) => {
        const isActive = item.key === active;
        return (
          <button
            key={item.key ?? "*"}
            type="button"
            onClick={() => onSelect(item.key)}
            aria-pressed={isActive}
            className={cn(
              "flex items-center gap-[9px] text-left text-[12.5px]",
              isActive ? "text-wv-strong" : "text-wv-fg hover:text-wv-secondary",
            )}
          >
            <Square color={item.color} />
            <span className="min-w-0 truncate">{item.label}</span>
            <span className="ml-auto flex-none font-wv-mono text-[11px] text-wv-faint">
              {item.count}
            </span>
          </button>
        );
      })}
    </RailBlock>
  );
}

/**
 * The rail's contextual middle block on Settings: one row per panel, with the
 * active background bleeding to the rail's edge.
 */
export function PanelListBlock({
  eyebrow,
  items,
}: {
  eyebrow: string;
  items: readonly { to: string; label: string; tag?: ReactNode }[];
}) {
  return (
    <RailBlock eyebrow={eyebrow} position="middle" className="gap-0">
      {items.map((item) => (
        <NavLink
          key={item.to}
          to={item.to}
          className={({ isActive }) =>
            cn(
              "-mx-[10px] flex h-[30px] items-center gap-[10px] px-[10px] text-[12.5px] hover:bg-wv-nav-hover",
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
              <span className="min-w-0 truncate">{item.label}</span>
              {item.tag === undefined ? null : (
                <span className="ml-auto flex-none font-wv-mono text-[10.5px] tracking-[0.1em] text-wv-faint uppercase">
                  {item.tag}
                </span>
              )}
            </>
          )}
        </NavLink>
      ))}
    </RailBlock>
  );
}

/**
 * The rail's contextual middle block on a detail screen: the blocks of the
 * page below, with whatever each one counts.
 *
 * The screens this serves are one long scroller, so the rail is where you see
 * the whole shape of the page at once — and clicking a line is the only way to
 * reach the event log without dragging past the pipeline.
 */
export function JumpListBlock({
  eyebrow,
  items,
}: {
  eyebrow: string;
  items: readonly { id: string; label: string; meta?: ReactNode }[];
}) {
  return (
    <RailBlock eyebrow={eyebrow} position="middle" className="gap-[11px]">
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          onClick={() => {
            document.getElementById(item.id)?.scrollIntoView({ block: "start" });
          }}
          className="flex cursor-pointer items-baseline gap-[9px] text-left text-[13px] text-wv-tertiary hover:text-wv-strong"
        >
          <span aria-hidden="true" className="size-[3px] flex-none bg-wv-dim" />
          <span className="min-w-0 truncate">{item.label}</span>
          {item.meta === undefined ? null : (
            <span className="ml-auto flex-none font-wv-mono text-[11px] text-wv-faint">
              {item.meta}
            </span>
          )}
        </button>
      ))}
    </RailBlock>
  );
}

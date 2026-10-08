import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import { Link, useNavigate } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import type { Egress, LegFlow, NetworkFlow as Flow, NetworkRoute, ProxyPool, Rung } from "@/lib/networking";
import { proxyLabels, type ProxyProfile } from "@/lib/proxies";
import { EmptyState, Field } from "../../components/chrome";
import { SecondaryButton, Select } from "../../components/controls";
import { countLabel } from "../../i18n/labels";
import { formatRate, stateColor, stateLabel, StatusMark } from "./presentation";

type FlowConsumer = { key: string; name: string; cap?: number; route?: NetworkRoute | null };

/**
 * The flow is drawn as SVG and can be saved as a file, so its colours and
 * fonts are literal values rather than classes: a stylesheet does not travel
 * with an exported diagram. They are the theme's own tokens. The accent stays
 * a variable on screen and is resolved when the file is written.
 */
const INK = {
  ground: "#1a1b1e",
  card: "#1f2024",
  row: "#191a1d",
  rowActive: "#1f2b29",
  line: "#2a2b30",
  control: "#3a3b41",
  fg: "#ecebe6",
  secondary: "#d6d3cc",
  muted: "#a09d96",
  faint: "#84817a",
  accent: "var(--wv-accent)",
  warn: "#d3a43e",
  track: "#3d3e45",
} as const;
const FONT = '"Fira Code Variable", ui-monospace, "SFMono-Regular", Menlo, monospace';
const TITLE_FONT = '"Sora Variable", ui-sans-serif, system-ui, sans-serif';

const legKey = (leg: LegFlow) => `${leg.consumer}:${leg.position}`;

/** A ribbon whose thickness is the open connections it carries. */
function band(x: number, y: number, endX: number, endY: number, width: number) {
  const mid = (x + endX) / 2;
  const half = width / 2;
  return `M ${x} ${y - half} C ${mid} ${y - half},${mid} ${endY - half},${endX} ${endY - half} L ${endX} ${endY + half} C ${mid} ${endY + half},${mid} ${y + half},${x} ${y + half} Z`;
}

/** A linear map from [0, domain] onto [0, range], clamped at both ends. */
function scale(domain: number, range: number) {
  return (value: number) => Math.max(0, Math.min(range, (value / Math.max(1, domain)) * range));
}

/** Animate measurements only: configuration and card positions change once per sample. */
function useMeasurements(legs: LegFlow[]) {
  const [counts, setCounts] = useState<Record<string, number>>({});
  const current = useRef<Record<string, number>>({});
  useEffect(() => {
    const values = legs.map((leg) => ({
      key: legKey(leg),
      from: current.current[legKey(leg)] ?? leg.open,
      to: leg.open,
    }));
    let start: number | undefined;
    let frame: number;
    const step = (now: number) => {
      start ??= now;
      const progress = Math.min(1, (now - start) / 300);
      const next = Object.fromEntries(values.map((value) => [value.key, value.from + (value.to - value.from) * progress]));
      current.current = next;
      setCounts(next);
      if (progress < 1) {
        frame = requestAnimationFrame(step);
      }
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [legs]);
  return counts;
}

/** A state in the diagram: the square, then the word, in the state's colour. */
function StateText({ x, y, state, label, size = 10.5 }: { x: number; y: number; state: string; label: string; size?: number }) {
  const color = stateColor(state);
  return (
    <g>
      <rect x={x} y={y - 7} width="7" height="7" fill={color} />
      <text x={x + 13} y={y} fill={color} fontSize={size} fontFamily={FONT}>
        {label}
      </text>
    </g>
  );
}

/**
 * Where every connection leaves, which route leg carries it, and which
 * server or feed it serves: egress cards on the left, the legs and their
 * ladders in the middle, consumers on the right, joined by ribbons as thick
 * as the connections they carry. A leg with nothing open is a dashed line.
 */
export function NetworkFlow({
  flow,
  egresses,
  profiles,
  pools,
  consumers,
}: {
  flow: Flow;
  egresses: Egress[];
  profiles: ProxyProfile[];
  pools: ProxyPool[];
  consumers: FlowConsumer[];
}) {
  const t = useTranslate();
  const navigate = useNavigate();
  const svg = useRef<SVGSVGElement>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string[]>([]);
  const [filter, setFilter] = useState("");
  const [group, setGroup] = useState<number | null>(null);
  const counts = useMeasurements(flow.legs);
  const grouped = egresses.length > 6 || flow.legs.length > 40;

  const layout = useMemo(() => {
    const legs = flow.legs
      .filter((leg) => (!filter || leg.consumer === filter) && (!grouped || group === leg.egressId))
      .sort((a, b) => a.egressId - b.egressId || a.consumer.localeCompare(b.consumer) || a.position - b.position);
    let offset = 58;
    const rows = legs.map((leg) => {
      const rungs: (Rung | null)[] =
        leg.path.kind === "DIRECT" ? [null] : [...leg.path.rungs, ...(leg.path.directFallback ? [null] : [])];
      let rowY = 48;
      const details = rungs.map((rung, rungIndex) => {
        const pool = rung?.kind === "POOL" ? pools.find((candidate) => candidate.id === rung.poolId) : null;
        const members = pool && expanded.includes(`${legKey(leg)}:${rungIndex}`) ? pool.memberIds : [];
        const y = rowY;
        rowY += 36 + members.length * 32;
        return { rung, rungIndex, members, y };
      });
      const y = offset;
      const height = rowY + 12;
      offset += height + 18;
      return { leg, y, height, details };
    });
    return {
      rows,
      height: Math.max(offset + 36, egresses.length * 128 + 90, consumers.length * 144 + 90, 260),
    };
  }, [flow.legs, filter, grouped, group, pools, expanded, egresses.length, consumers.length]);

  const width = scale(Math.max(1, ...consumers.map((consumer) => consumer.cap ?? 1)), 28);
  const name = (id: number) => profiles.find((profile) => profile.id === id)?.name ?? t("next.networking.proxyNumber", { id });
  const poolName = (id: number | null) =>
    pools.find((pool) => pool.id === id)?.name ?? t("next.networking.flow.pool");
  const rungName = (rung: Rung | null) => {
    if (!rung) {
      return t("next.networking.flow.direct");
    }
    if (rung.kind === "POOL") {
      return t("next.networking.flow.poolRung", { name: poolName(rung.poolId) });
    }
    if (rung.kind === "CHAIN") {
      return rung.chainIds.map(name).join(" → ");
    }
    const profile = profiles.find((candidate) => candidate.id === rung.proxyId);
    return `${name(rung.proxyId ?? 0)} · ${profile ? proxyLabels[profile.kind] : t("next.networking.flow.proxy")}`;
  };
  const active = flow.legs.find((leg) => legKey(leg) === selected);
  const consumerName = (leg: LegFlow) => consumers.find((consumer) => consumer.key === leg.consumer)?.name ?? leg.consumer;

  function download() {
    if (!svg.current) {
      return;
    }
    const accent =
      getComputedStyle(document.documentElement).getPropertyValue("--wv-accent").trim() || "#3fb39c";
    const markup = new XMLSerializer().serializeToString(svg.current).replaceAll("var(--wv-accent)", accent);
    const url = URL.createObjectURL(new Blob([markup], { type: "image/svg+xml" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "weaver-network-flow.svg";
    link.click();
    URL.revokeObjectURL(url);
  }

  function toggle(key: string) {
    setExpanded((values) => (values.includes(key) ? values.filter((value) => value !== key) : [...values, key]));
  }

  /** Cards are real links, so the exported file keeps them; on screen they route in place. */
  const follow = (href: string) => (event: MouseEvent) => {
    event.preventDefault();
    navigate(href);
  };

  if (consumers.length === 0 && egresses.length === 0) {
    return <EmptyState title={t("next.networking.flow.emptyTitle")} body={t("next.networking.flow.emptyBody")} />;
  }

  return (
    <div className="flex min-w-0 flex-col">
      <div className="flex flex-wrap items-center gap-[10px] border-b border-wv-hairline px-4 py-3 sm:px-6">
        <Select
          label={t("next.networking.flow.consumer")}
          value={filter}
          onChange={setFilter}
          className="min-w-[180px]"
          options={[
            { value: "", label: t("next.networking.flow.allConsumers") },
            ...consumers.map((consumer) => ({ value: consumer.key, label: consumer.name })),
          ]}
        />
        <SecondaryButton icon="downloadFile" onClick={download} className="ml-auto">
          {t("next.networking.flow.download")}
        </SecondaryButton>
      </div>
      {grouped ? (
        <div
          aria-label={t("next.networking.flow.groups")}
          className="flex flex-wrap gap-2 border-b border-wv-hairline px-4 py-3 sm:px-6"
        >
          {egresses.map((egress) => (
            <SecondaryButton
              key={egress.id}
              size="compact"
              icon={group === egress.id ? "dropdown" : "expand"}
              onClick={() => setGroup(group === egress.id ? null : egress.id)}
              className={group === egress.id ? "!border-wv-accent" : undefined}
            >
              {egress.name} · {countLabel(t, "next.networking.legCount", flow.legs.filter((leg) => leg.egressId === egress.id).length)}
            </SecondaryButton>
          ))}
        </div>
      ) : null}
      <div className="min-w-0 overflow-x-auto bg-wv-app">
        <svg
          ref={svg}
          xmlns="http://www.w3.org/2000/svg"
          viewBox={`0 0 1040 ${layout.height}`}
          role="group"
          aria-label={t("next.networking.flow.diagram")}
          className="block w-full min-w-[820px]"
          style={{ background: INK.ground, fontFamily: FONT }}
        >
          {[
            [20, t("next.networking.flow.egressColumn")],
            [310, t("next.networking.flow.legsColumn")],
            [810, t("next.networking.flow.consumersColumn")],
          ].map(([x, label]) => (
            <text key={String(x)} x={x} y="30" fill={INK.faint} fontSize="10.5" fontWeight="600" letterSpacing="1.5" fontFamily={TITLE_FONT}>
              {String(label).toUpperCase()}
            </text>
          ))}

          {layout.rows.map(({ leg, y, height }) => {
            const egressY = 112 + Math.max(0, egresses.findIndex((egress) => egress.id === leg.egressId)) * 128;
            const consumerY = 120 + Math.max(0, consumers.findIndex((consumer) => consumer.key === leg.consumer)) * 144;
            const legY = y + height / 2;
            const count = counts[legKey(leg)] ?? leg.open;
            const color = stateColor(leg.state);
            const tip = [
              t("next.networking.flow.connections", { open: leg.open, target: leg.target }),
              formatRate(leg.bytesPerSecond),
              leg.sourceAddress ?? t("next.networking.flow.noSource"),
              ...(leg.reason ? [leg.reason] : []),
            ].join(" · ");
            const holding = consumers.find((consumer) => consumer.key === leg.consumer)?.route?.failover === "HOLD";
            return (
              <g key={`ribbon:${legKey(leg)}`}>
                <title>{tip}</title>
                {[
                  [230, egressY, 310, legY],
                  [730, legY, 810, consumerY],
                ].map(([x, startY, endX, endY], index) =>
                  count > 0 ? (
                    <path key={index} d={band(x!, startY!, endX!, endY!, width(count))} fill={color} opacity="0.28" />
                  ) : (
                    <path
                      key={index}
                      d={`M ${x} ${startY} C ${(x! + endX!) / 2} ${startY},${(x! + endX!) / 2} ${endY},${endX} ${endY}`}
                      fill="none"
                      stroke={color}
                      strokeWidth="1.4"
                      strokeDasharray="4 5"
                    />
                  ),
                )}
                {leg.open ? null : (
                  <text x="736" y={legY - 7} fill={holding ? INK.warn : INK.muted} fontSize="9.5">
                    {holding && leg.state === "DOWN"
                      ? t("next.networking.flow.parkedHold")
                      : leg.state === "DOWN"
                        ? t("next.networking.flow.shareMoved")
                        : t("next.networking.flow.planned", { count: leg.target })}
                  </text>
                )}
              </g>
            );
          })}

          {egresses.map((egress, index) => {
            const legs = flow.legs.filter((leg) => leg.egressId === egress.id);
            const y = 58 + index * 128;
            const down = egress.health === "DOWN";
            const open = Math.round(legs.reduce((sum, leg) => sum + (counts[legKey(leg)] ?? leg.open), 0));
            return (
              <g key={egress.id}>
                <title>{egress.reason ?? egress.name}</title>
                <a href="/settings/networking/egress" onClick={follow("/settings/networking/egress")}>
                  <rect x="16" y={y} width="214" height="108" fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} strokeDasharray={down ? "4 4" : undefined} />
                  <rect x="16" y={y} width="3" height="108" fill={stateColor(egress.health)} />
                  <text x="30" y={y + 24} fill={INK.fg} fontSize="13" fontFamily={TITLE_FONT} fontWeight="600">
                    {egress.name.slice(0, 24)}
                  </text>
                  <StateText x={30} y={y + 46} state={egress.health} label={stateLabel(t, egress.health)} />
                  <text x="30" y={y + 66} fill={INK.muted} fontSize="10">
                    {(egress.addresses?.join(", ") || egress.sourceAddress || egress.interfaceName || t("next.networking.binding.system")).slice(0, 30)}
                  </text>
                  <text x="30" y={y + 86} fill={INK.muted} fontSize="10">
                    {down
                      ? t("next.networking.flow.carryingNothing")
                      : t("next.networking.flow.egressLoad", {
                          open,
                          rate: formatRate(legs.reduce((sum, leg) => sum + (leg.bytesPerSecond ?? 0), 0)),
                        })}
                  </text>
                </a>
              </g>
            );
          })}

          {layout.rows.map(({ leg, y, height, details }) => {
            const key = legKey(leg);
            const isSelected = selected === key;
            const open = Math.round(counts[key] ?? leg.open);
            return (
              <g key={key}>
                <rect x="310" y={y} width="420" height={height} fill={INK.card} stroke={isSelected ? INK.accent : INK.line} />
                <g
                  role="button"
                  tabIndex={0}
                  aria-label={t("next.networking.flow.inspect", { name: consumerName(leg), position: leg.position + 1 })}
                  style={{ cursor: "pointer" }}
                  onClick={() => setSelected(key)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setSelected(key);
                    }
                  }}
                >
                  <rect x="320" y={y + 6} width="400" height="35" fill="transparent" />
                  <text x="324" y={y + 21} fill={INK.fg} fontSize="12">
                    {t("next.networking.flow.legTitle", {
                      name: consumerName(leg).slice(0, 27),
                      position: leg.position + 1,
                      weight: leg.weight,
                    })}
                  </text>
                  <StateText
                    x={324}
                    y={y + 38}
                    state={leg.state}
                    label={`${stateLabel(t, leg.state)} · ${t("next.networking.flow.connections", { open, target: leg.target })} · ${formatRate(leg.bytesPerSecond)}`}
                  />
                </g>
                {details.map(({ rung, rungIndex, members, y: rowY }) => {
                  const isActive = leg.open > 0 && (rung ? leg.selectedRung === rungIndex : leg.selectedRung == null);
                  const pool = flow.pools.find((candidate) => candidate.poolId === rung?.poolId && candidate.egressId === leg.egressId);
                  const rowKey = `${key}:${rungIndex}`;
                  const rungState = leg.rungStates?.[rungIndex];
                  const status = isActive
                    ? "ACTIVE"
                    : leg.state === "PROBING"
                      ? "PROBING"
                      : rungState === "COOLDOWN"
                        ? "COOLDOWN"
                        : leg.state === "DOWN" || rungState === "FAILING"
                          ? "FAILING"
                          : "STANDBY";
                  const isPool = rung?.kind === "POOL";
                  const unfolded = expanded.includes(rowKey);
                  return (
                    <g key={rungIndex}>
                      <rect x="320" y={y + rowY - 1} width="400" height="31" fill={isActive ? INK.rowActive : INK.row} />
                      <g
                        {...(isPool
                          ? {
                              role: "button",
                              tabIndex: 0,
                              "aria-expanded": unfolded,
                              "aria-label": t("next.networking.flow.poolMembers", { name: poolName(rung.poolId) }),
                              style: { cursor: "pointer" },
                              onClick: () => toggle(rowKey),
                              onKeyDown: (event: React.KeyboardEvent) => {
                                if (event.key === "Enter" || event.key === " ") {
                                  event.preventDefault();
                                  toggle(rowKey);
                                }
                              },
                            }
                          : {})}
                      >
                        <rect x="320" y={y + rowY - 1} width="400" height="31" fill="transparent" />
                        <text x="329" y={y + rowY + 12} fill={INK.secondary} fontSize="10">
                          {rungName(rung).slice(0, 52)}
                          {isPool ? (unfolded ? " ▾" : " ▸") : ""}
                        </text>
                        <StateText
                          x={329}
                          y={y + rowY + 25}
                          size={9.5}
                          state={status}
                          label={`${stateLabel(t, status)}${pool?.pinnedMember != null ? ` · ${t("next.networking.flow.pinnedTo", { name: name(pool.pinnedMember) })}` : ""}`}
                        />
                      </g>
                      {members.map((id, index) => {
                        const member = pool?.members.find((candidate) => candidate.id === id);
                        const memberState = member?.blocked
                          ? "BLOCKED"
                          : pool?.pinnedMember === id
                            ? "PINNED"
                            : member?.state === "SUSPECT"
                              ? "SUSPECT"
                              : member?.state === "CHALLENGER"
                                ? "CHALLENGER"
                                : member?.state === "PROBING"
                                  ? "PROBING"
                                  : member?.failures
                                    ? "FAILING"
                                    : member?.warmed
                                      ? "READY"
                                      : "UNMEASURED";
                        const memberY = y + rowY + 44 + index * 32;
                        return (
                          <g key={id}>
                            <rect x="337" y={memberY - 7} width="7" height="7" fill={stateColor(memberState)} />
                            <text x="350" y={memberY} fill={INK.secondary} fontSize="10">
                              {`${name(id).slice(0, 26)} · ${stateLabel(t, memberState)}`}
                            </text>
                            <text x="350" y={memberY + 13} fill={INK.muted} fontSize="9.5">
                              {t("next.networking.flow.memberTiming", {
                                session: member?.handshakeMs?.toFixed(0) ?? "—",
                                open: member?.connectMs?.toFixed(0) ?? "—",
                                rate:
                                  member?.bytesPerSecond == null
                                    ? t("next.networking.flow.noEvidence")
                                    : formatRate(member.bytesPerSecond),
                              })}
                            </text>
                          </g>
                        );
                      })}
                    </g>
                  );
                })}
              </g>
            );
          })}

          {consumers.map((consumer, index) => {
            const legs = flow.legs.filter((leg) => leg.consumer === consumer.key);
            const cap = consumer.cap ?? legs.reduce((sum, leg) => sum + leg.target, 0);
            const open = legs.reduce((sum, leg) => sum + (counts[legKey(leg)] ?? leg.open), 0);
            const parked =
              consumer.route?.failover === "HOLD" ? Math.max(0, cap - legs.reduce((sum, leg) => sum + leg.target, 0)) : 0;
            const y = 58 + index * 144;
            const down = legs.length > 0 && legs.every((leg) => leg.state === "DOWN" || leg.state === "BLOCKED");
            const cells = Math.min(cap, 100);
            const cellX = scale(Math.max(1, cells), 188);
            const href = `/settings/networking/routes?consumer=${encodeURIComponent(consumer.key)}`;
            const rss = consumer.key.startsWith("rss:");
            return (
              <g key={consumer.key} opacity={down ? 0.65 : 1}>
                <a href={href} onClick={follow(href)}>
                  <rect x="810" y={y} width="214" height="124" fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} />
                  <text x="824" y={y + 23} fill={INK.fg} fontSize="12.5" fontFamily={TITLE_FONT} fontWeight="600">
                    {consumer.name.slice(0, 24)}
                  </text>
                  <text x="824" y={y + 41} fill={INK.muted} fontSize="9.5">
                    {rss
                      ? t("next.networking.flow.rssKind")
                      : t("next.networking.flow.serverKind", {
                          failover:
                            consumer.route?.failover === "HOLD"
                              ? t("next.networking.failover.hold")
                              : t("next.networking.failover.redistribute"),
                        })}
                  </text>
                  <text x="824" y={y + 67} fill={INK.fg} fontSize="20">
                    {`${Math.round(open)} / ${cap}`}
                  </text>
                  <text x="824" y={y + 85} fill={INK.muted} fontSize="10">
                    {formatRate(legs.reduce((sum, leg) => sum + (leg.bytesPerSecond ?? 0), 0))}
                    {parked ? ` · ${t("next.networking.flow.parked", { count: parked })}` : ""}
                  </text>
                  {Array.from({ length: cells }, (_, cell) => {
                    const boundary = (cell * cap) / cells;
                    const reserved = boundary >= cap - parked;
                    return (
                      <rect
                        key={cell}
                        x={824 + cellX(cell)}
                        y={y + 98}
                        width={Math.max(1, cellX(1) - 2)}
                        height="10"
                        fill={boundary < open ? INK.accent : "none"}
                        stroke={reserved ? INK.warn : INK.track}
                        strokeDasharray={reserved ? "2 2" : undefined}
                      />
                    );
                  })}
                </a>
              </g>
            );
          })}

          {layout.rows.length ? null : (
            <text x="310" y="85" fill={INK.muted} fontSize="12">
              {grouped ? t("next.networking.flow.expandGroup") : t("next.networking.flow.noLegs")}
            </text>
          )}
          <text x="20" y={layout.height - 15} fill={INK.faint} fontSize="10">
            {`Weaver · ${flow.sampledAt ? new Date(flow.sampledAt * 1000).toISOString() : t("next.networking.flow.waiting")}`}
          </text>
        </svg>
      </div>
      {active ? (
        <section
          aria-label={t("next.networking.flow.legDetails")}
          className="flex flex-col gap-3 border-t border-wv-line-strong bg-wv-chrome px-4 py-4 sm:px-6"
        >
          <div className="flex flex-wrap items-center gap-3">
            <span className="font-wv-title text-[13.5px] font-semibold text-wv-fg">
              {t("next.networking.flow.legHeading", { name: consumerName(active), position: active.position + 1 })}
            </span>
            <StatusMark state={active.state} label={stateLabel(t, active.state)} detail={active.reason ?? undefined} />
            <SecondaryButton size="compact" icon="close" onClick={() => setSelected(null)} className="ml-auto">
              {t("next.networking.close")}
            </SecondaryButton>
          </div>
          <div className="grid gap-2 sm:grid-cols-2">
            <Field
              variant="inline"
              label={t("next.networking.flow.detailConnections")}
              value={t("next.networking.flow.detailConnectionsValue", {
                open: active.open,
                opening: active.opening,
                target: active.target,
                rate: formatRate(active.bytesPerSecond),
              })}
            />
            <Field
              variant="inline"
              label={t("next.networking.flow.detailSource")}
              value={active.sourceAddress ?? t("next.networking.flow.noSocket")}
            />
            <Field
              variant="inline"
              label={t("next.networking.flow.detailPin")}
              value={active.pinnedAddress ?? t("next.networking.flow.notPinned")}
            />
          </div>
          <Link
            to={`/settings/networking/routes?consumer=${encodeURIComponent(active.consumer)}`}
            className="self-start text-[12.5px] text-wv-accent hover:underline"
          >
            {t("next.networking.flow.editRoute")}
          </Link>
        </section>
      ) : null}
    </div>
  );
}

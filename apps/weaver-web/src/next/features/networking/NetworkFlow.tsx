import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import { Link, useNavigate } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import type {
  Egress,
  FailingHop,
  LegFlow,
  NetworkFlow as Flow,
  NetworkRoute,
  PoolMemberFlow,
  ProxyPool,
  Rung,
} from "@/lib/networking";
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

/** The four lanes, left to right. Ribbons cross the gaps between them. */
const EGRESS = { x: 16, width: 214, end: 230 } as const;
const LEGS = { x: 286, width: 300, end: 586 } as const;
const PROXY = { x: 642, width: 440, end: 1082 } as const;
const ENDPOINTS = { x: 1138, width: 214, end: 1352 } as const;
const WIDTH = ENDPOINTS.end + 16;
const LEG_HEIGHT = 64;
const BOX_HEIGHT = 44;
const PROXIES_PAGE = "/settings/networking/proxies";

type Point = [number, number];
type Member = { id: number; state: string; live: PoolMemberFlow | undefined; measured: boolean };
/** One ladder rung as the proxy lane draws it. */
type Block = {
  rung: Rung;
  rungIndex: number;
  top: number;
  height: number;
  failing: FailingHop | undefined;
  pinned: number | null;
  members: Member[];
};

/**
 * The box the reader is pointing at. Its routes are the legs that pass
 * through it; a rung narrows that to the one way through its own leg.
 */
type Focus =
  | { kind: "egress"; id: number }
  | { kind: "endpoint"; key: string }
  | { kind: "leg"; key: string }
  | { kind: "way"; key: string; index: number };
/** How far everything off the pointed-at routes fades. */
const FADE = 0.2;

const legKey = (leg: LegFlow) => `${leg.consumer}:${leg.position}`;

/** Monospaced text cut to the width it has, ending in an ellipsis when it was cut. */
function fit(text: string, width: number, size: number) {
  const room = Math.max(1, Math.floor(width / (size * 0.6)));
  return text.length > room ? `${text.slice(0, room - 1)}…` : text;
}

/** The steps from each waypoint to the next: level stretches run straight, the rest ease between heights. */
function steps(points: Point[], shift: number) {
  return points
    .slice(1)
    .map(([x, y], index) => {
      const [fromX, fromY] = points[index]!;
      const mid = (fromX + x) / 2;
      return fromY === y
        ? `L ${x} ${y + shift}`
        : `C ${mid} ${fromY + shift},${mid} ${y + shift},${x} ${y + shift}`;
    })
    .join(" ");
}

/** The line a path with nothing open is drawn as. */
function thread(points: Point[]) {
  const [x, y] = points[0]!;
  return `M ${x} ${y} ${steps(points, 0)}`;
}

/** A ribbon whose thickness is the open connections it carries. */
function band(points: Point[], width: number) {
  const half = width / 2;
  const back = [...points].reverse();
  const [x, y] = points[0]!;
  const [endX, endY] = back[0]!;
  return `M ${x} ${y - half} ${steps(points, -half)} L ${endX} ${endY + half} ${steps(back, half)} Z`;
}

/** A linear map from [0, domain] onto [0, range], clamped at both ends. */
function scale(domain: number, range: number) {
  return (value: number) => Math.max(0, Math.min(range, (value / Math.max(1, domain)) * range));
}

/** What a rung is doing for its leg. `direct` is the rung with no tunnel. */
function rungStatus(leg: LegFlow, rungIndex: number, direct: boolean): string {
  if (leg.open > 0 && (direct ? leg.selectedRung == null : leg.selectedRung === rungIndex)) {
    return "ACTIVE";
  }
  if (leg.state === "PROBING") {
    return "PROBING";
  }
  const state = leg.rungStates?.[rungIndex];
  if (state === "COOLDOWN") {
    return "COOLDOWN";
  }
  return leg.state === "DOWN" || state === "FAILING" ? "FAILING" : "STANDBY";
}

/** What a pool member is doing, in the one word the diagram shows for it. */
function memberState(member: PoolMemberFlow | undefined, pinned: boolean, enabled: boolean): string {
  if (!enabled || member?.state === "DISABLED") {
    return "DISABLED";
  }
  if (member?.blocked) {
    return "BLOCKED";
  }
  if (pinned) {
    return "PINNED";
  }
  if (member?.state === "SUSPECT" || member?.state === "CHALLENGER" || member?.state === "PROBING") {
    return member.state;
  }
  if (member?.failures) {
    return "FAILING";
  }
  return member?.warmed ? "READY" : "UNMEASURED";
}

/**
 * A chain hop's state. Only the first failing hop can be known: the hops
 * before it carried the attempt that far, and the hops after it were never
 * reached.
 */
function hopState(position: number, failingAt: number, status: string, enabled: boolean): string {
  if (!enabled) {
    return "DISABLED";
  }
  if (position === failingAt) {
    return "FAILING";
  }
  if (failingAt < 0 || status === "ACTIVE") {
    return status;
  }
  return position < failingAt ? "UP" : "UNREACHED";
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

/** A proxy in the lane: its name, then its state, behind a bar in the state's colour. */
function HopBox({
  x,
  y,
  width,
  title,
  state,
  label,
  active,
}: {
  x: number;
  y: number;
  width: number;
  title: string;
  state: string;
  label: string;
  active: boolean;
}) {
  return (
    <g opacity={state === "DISABLED" ? 0.6 : 1}>
      <rect x={x} y={y} width={width} height={BOX_HEIGHT} fill={active ? INK.rowActive : INK.card} stroke={INK.line} />
      <rect x={x} y={y} width="3" height={BOX_HEIGHT} fill={stateColor(state)} />
      <text x={x + 14} y={y + 18} fill={INK.fg} fontSize="11">
        {fit(title, width - 26, 11)}
      </text>
      <StateText x={x + 14} y={y + 35} size={9.5} state={state} label={fit(label, width - 39, 9.5)} />
    </g>
  );
}

/**
 * Where every connection leaves, which route leg carries it, what it tunnels
 * through, and which endpoint it serves: egress cards on the left, then the
 * route legs, then each leg's proxies, then the endpoints, joined by ribbons
 * as thick as the connections they carry. A path with nothing open is a
 * dashed line. Only what a route uses is drawn: an egress with no leg on it
 * has no card.
 *
 * The proxy lane draws a leg's ladder top to bottom. A proxy is one box, a
 * chain is its hops in order with a `>` between them, and a pool lists its
 * members. Every rung has a line in from its leg and out to the endpoint,
 * whether or not it is the one carrying the leg. Going direct has no box:
 * the line crosses the lane untouched.
 *
 * Pointing at a box lights the routes through it, from egress to endpoint,
 * and fades the rest. Inspecting a leg holds its route lit the same way.
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
  const [hover, setHover] = useState<Focus | null>(null);
  const [filter, setFilter] = useState("");
  const [group, setGroup] = useState<number | null>(null);
  const counts = useMeasurements(flow.legs);
  // An egress no route leaves through is not part of the flow, so it has no card.
  const used = useMemo(
    () => egresses.filter((egress) => flow.legs.some((leg) => leg.egressId === egress.id)),
    [egresses, flow.legs],
  );
  const grouped = used.length > 6 || flow.legs.length > 40;

  const layout = useMemo(() => {
    const legs = flow.legs
      .filter((leg) => (!filter || leg.consumer === filter) && (!grouped || group === leg.egressId))
      .sort((a, b) => a.egressId - b.egressId || a.consumer.localeCompare(b.consumer) || a.position - b.position);
    let offset = 58;
    const rows = legs.map((leg) => {
      let bottom = 0;
      const blocks: Block[] = (leg.path.kind === "DIRECT" ? [] : leg.path.rungs).map((rung, rungIndex) => {
        const live =
          rung.kind === "POOL"
            ? flow.pools.find((candidate) => candidate.poolId === rung.poolId && candidate.egressId === leg.egressId)
            : undefined;
        const configured = rung.kind === "POOL" ? pools.find((candidate) => candidate.id === rung.poolId) : undefined;
        const members: Member[] = (configured?.memberIds ?? live?.members.map((member) => member.id) ?? []).map((id) => {
          const member = live?.members.find((candidate) => candidate.id === id);
          return {
            id,
            live: member,
            state: memberState(
              member,
              live?.pinnedMember === id,
              profiles.find((profile) => profile.id === id)?.enabled !== false,
            ),
            measured: member != null && (member.handshakeMs != null || member.connectMs != null || member.bytesPerSecond != null),
          };
        });
        const failing = leg.failingHops?.find((hop) => hop.rung === rungIndex);
        const height =
          rung.kind === "POOL"
            ? BOX_HEIGHT + members.reduce((sum, member) => sum + (member.measured ? 32 : 20), 0) + (members.length ? 8 : 0)
            : BOX_HEIGHT + (failing ? 18 : 0);
        const top = bottom;
        bottom = top + height + 8;
        return { rung, rungIndex, top, height, failing, pinned: live?.pinnedMember ?? null, members };
      });
      // Direct has no box: its line crosses level with the leg, or beneath the ladder it backs up.
      const bypass = leg.path.kind === "DIRECT" ? LEG_HEIGHT / 2 : leg.path.directFallback ? bottom + 26 : null;
      const y = offset;
      const height = Math.max(LEG_HEIGHT, bypass == null ? bottom - 8 : bypass + 16);
      offset += height + 18;
      return { leg, y, height, blocks, bypass };
    });
    return {
      rows,
      height: Math.max(offset + 36, used.length * 128 + 90, consumers.length * 144 + 90, 260),
    };
  }, [flow.legs, flow.pools, filter, grouped, group, pools, profiles, used.length, consumers.length]);

  const width = scale(Math.max(1, ...consumers.map((consumer) => consumer.cap ?? 1)), 28);
  const profile = (id: number | null) => profiles.find((candidate) => candidate.id === id);
  const name = (id: number) => profile(id)?.name ?? t("next.networking.proxyNumber", { id });
  const poolName = (id: number | null) =>
    pools.find((pool) => pool.id === id)?.name ?? t("next.networking.flow.pool");
  const active = flow.legs.find((leg) => legKey(leg) === selected);
  const consumerName = (leg: LegFlow) => consumers.find((consumer) => consumer.key === leg.consumer)?.name ?? leg.consumer;
  const holds = (leg: LegFlow) => consumers.find((consumer) => consumer.key === leg.consumer)?.route?.failover === "HOLD";

  // The box under the pointer or the keyboard, or else the leg being inspected, picks the routes
  // to light. Everything else fades, and with nothing picked the whole flow is lit.
  const focus: Focus | null =
    hover ?? (selected != null && layout.rows.some(({ leg }) => legKey(leg) === selected) ? { kind: "leg", key: selected } : null);
  const onRoute = (leg: LegFlow) =>
    !focus ||
    (focus.kind === "egress"
      ? leg.egressId === focus.id
      : focus.kind === "endpoint"
        ? leg.consumer === focus.key
        : legKey(leg) === focus.key);
  /** A leg's ways are its rungs in order, then the one with no tunnel. */
  const onWay = (leg: LegFlow, index: number) => onRoute(leg) && (focus?.kind !== "way" || focus.index === index);
  const lit = layout.rows.filter(({ leg }) => onRoute(leg));
  const egressLit = (id: number) =>
    !focus || (focus.kind === "egress" ? focus.id === id : lit.some(({ leg }) => leg.egressId === id));
  const endpointLit = (key: string) =>
    !focus || (focus.kind === "endpoint" ? focus.key === key : lit.some(({ leg }) => leg.consumer === key));
  const point = (target: Focus) => ({
    onPointerEnter: () => setHover(target),
    onPointerLeave: () => setHover(null),
    onFocus: () => setHover(target),
    onBlur: () => setHover(null),
  });
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

  /** Cards are real links, so the exported file keeps them; on screen they route in place. */
  const follow = (href: string) => (event: MouseEvent) => {
    event.preventDefault();
    navigate(href);
  };

  /** One rung of a leg's ladder, drawn in the proxy lane. */
  function rungBlock(leg: LegFlow, rowY: number, block: Block) {
    const { rung, rungIndex, failing, members } = block;
    const y = rowY + block.top;
    const status = rungStatus(leg, rungIndex, false);
    const isActive = status === "ACTIVE";
    const way = {
      opacity: onWay(leg, rungIndex) ? undefined : FADE,
      ...point({ kind: "way", key: legKey(leg), index: rungIndex }),
    };
    // The whole block answers the pointer, the gaps between its boxes included.
    const ground = <rect x={PROXY.x} y={y} width={PROXY.width} height={block.height} fill="transparent" />;
    const reason = failing ? (
      <text x={PROXY.x + 14} y={y + BOX_HEIGHT + 13} fill={INK.muted} fontSize="9.5">
        {fit(failing.reason, PROXY.width - 28, 9.5)}
      </text>
    ) : null;

    if (rung.kind === "CHAIN") {
      const gap = 26;
      const hopWidth = (PROXY.width - (rung.chainIds.length - 1) * gap) / rung.chainIds.length;
      const failingAt = failing ? rung.chainIds.indexOf(failing.proxyId) : -1;
      return (
        <a key={rungIndex} href={PROXIES_PAGE} onClick={follow(PROXIES_PAGE)} {...way}>
          <title>{[rung.chainIds.map(name).join(" > "), ...(failing ? [failing.reason] : [])].join(" · ")}</title>
          {ground}
          {rung.chainIds.map((id, position) => {
            const x = PROXY.x + position * (hopWidth + gap);
            const state = hopState(position, failingAt, status, profile(id)?.enabled !== false);
            return (
              <g key={id}>
                <HopBox x={x} y={y} width={hopWidth} title={name(id)} state={state} label={stateLabel(t, state)} active={isActive} />
                {position + 1 < rung.chainIds.length ? (
                  <text x={x + hopWidth + gap / 2} y={y + 27} fill={INK.muted} fontSize="15" textAnchor="middle">
                    {">"}
                  </text>
                ) : null}
              </g>
            );
          })}
          {reason}
        </a>
      );
    }

    if (rung.kind === "POOL") {
      const configured = pools.find((pool) => pool.id === rung.poolId);
      const state = configured?.enabled === false ? "DISABLED" : status;
      let memberY = y + BOX_HEIGHT;
      return (
        <a key={rungIndex} href={PROXIES_PAGE} onClick={follow(PROXIES_PAGE)} {...way}>
          <rect x={PROXY.x} y={y} width={PROXY.width} height={block.height} fill={INK.card} stroke={INK.line} />
          <HopBox
            x={PROXY.x}
            y={y}
            width={PROXY.width}
            title={t("next.networking.flow.poolRung", { name: poolName(rung.poolId) })}
            state={state}
            label={`${stateLabel(t, state)}${block.pinned != null ? ` · ${t("next.networking.flow.pinnedTo", { name: name(block.pinned) })}` : ""}`}
            active={isActive}
          />
          <g role="group" aria-label={t("next.networking.flow.poolMembers", { name: poolName(rung.poolId) })}>
            {members.map((member) => {
              const top = memberY;
              memberY += member.measured ? 32 : 20;
              const held = member.live?.open ?? 0;
              return (
                <g key={member.id} opacity={member.state === "DISABLED" ? 0.6 : 1}>
                  {member.live?.blocked ? <title>{member.live.blocked}</title> : null}
                  <rect
                    x={PROXY.x + 8}
                    y={top}
                    width={PROXY.width - 16}
                    height={member.measured ? 30 : 18}
                    fill={held > 0 ? INK.rowActive : INK.row}
                  />
                  <rect x={PROXY.x + 16} y={top + 6} width="7" height="7" fill={stateColor(member.state)} />
                  <text x={PROXY.x + 29} y={top + 13} fill={INK.secondary} fontSize="10">
                    {`${fit(name(member.id), 160, 10)} · ${stateLabel(t, member.state)}`}
                  </text>
                  {held > 0 ? (
                    <text x={PROXY.end - 16} y={top + 13} fill={INK.fg} fontSize="10" textAnchor="end">
                      {countLabel(t, "next.networking.flow.memberHolding", held)}
                    </text>
                  ) : null}
                  {member.measured ? (
                    <text x={PROXY.x + 29} y={top + 25} fill={INK.muted} fontSize="9.5">
                      {t("next.networking.flow.memberTiming", {
                        session: member.live?.handshakeMs?.toFixed(0) ?? "—",
                        open: member.live?.connectMs?.toFixed(0) ?? "—",
                        rate:
                          member.live?.bytesPerSecond == null
                            ? t("next.networking.flow.noEvidence")
                            : formatRate(member.live.bytesPerSecond),
                      })}
                    </text>
                  ) : null}
                </g>
              );
            })}
          </g>
        </a>
      );
    }

    const hop = profile(rung.proxyId);
    const state = hop?.enabled === false ? "DISABLED" : failing && status !== "COOLDOWN" ? "FAILING" : status;
    return (
      <a key={rungIndex} href={PROXIES_PAGE} onClick={follow(PROXIES_PAGE)} {...way}>
        {failing ? <title>{failing.reason}</title> : null}
        {ground}
        <HopBox
          x={PROXY.x}
          y={y}
          width={PROXY.width}
          title={`${name(rung.proxyId ?? 0)} · ${hop ? proxyLabels[hop.kind] : t("next.networking.flow.proxy")}`}
          state={state}
          label={stateLabel(t, state)}
          active={isActive}
        />
        {reason}
      </a>
    );
  }

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
          {used.map((egress) => (
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
          viewBox={`0 0 ${WIDTH} ${layout.height}`}
          role="group"
          aria-label={t("next.networking.flow.diagram")}
          className="block w-full min-w-[1160px]"
          style={{ background: INK.ground, fontFamily: FONT }}
        >
          {[
            [EGRESS.x, t("next.networking.flow.egressColumn")],
            [LEGS.x, t("next.networking.flow.legsColumn")],
            [PROXY.x, t("next.networking.flow.proxyColumn")],
            [ENDPOINTS.x, t("next.networking.flow.consumersColumn")],
          ].map(([x, label]) => (
            <text key={String(x)} x={Number(x) + 4} y="30" fill={INK.faint} fontSize="10.5" fontWeight="600" letterSpacing="1.5" fontFamily={TITLE_FONT}>
              {String(label).toUpperCase()}
            </text>
          ))}

          {layout.rows.map(({ leg, y, blocks, bypass }) => {
            const egressY = 112 + Math.max(0, used.findIndex((egress) => egress.id === leg.egressId)) * 128;
            const consumerY = 120 + Math.max(0, consumers.findIndex((consumer) => consumer.key === leg.consumer)) * 144;
            const legY = y + LEG_HEIGHT / 2;
            const count = counts[legKey(leg)] ?? leg.open;
            const color = stateColor(leg.state);
            const tip = [
              t("next.networking.flow.connections", { open: leg.open, target: leg.target }),
              formatRate(leg.bytesPerSecond),
              leg.sourceAddress ?? t("next.networking.flow.noSource"),
              ...(leg.reason ? [leg.reason] : []),
            ].join(" · ");
            // Every way the leg can take is drawn: a line stops at a rung's box and resumes past it, and
            // the way with no tunnel runs straight across. The one carrying the leg is the ribbon.
            const ways = [
              ...blocks.map((block) => ({
                at: y + block.top + BOX_HEIGHT / 2,
                boxed: true,
                status: rungStatus(leg, block.rungIndex, false),
              })),
              ...(bypass == null ? [] : [{ at: y + bypass, boxed: false, status: rungStatus(leg, blocks.length, true) }]),
            ];
            const taken = ways.find((way) => way.status === "ACTIVE") ?? ways[leg.selectedRung ?? 0] ?? ways[0];
            const lines: { points: Point[]; carries: boolean; color: string; lit: boolean }[] = [
              {
                points: [
                  [EGRESS.end, egressY],
                  [LEGS.x, legY],
                ],
                carries: true,
                color,
                lit: onRoute(leg),
              },
              ...ways.flatMap((way, index) => {
                const line = {
                  carries: way === taken,
                  color: way === taken ? color : stateColor(way.status),
                  lit: onWay(leg, index),
                };
                const enter: Point[] = [
                  [LEGS.end, legY],
                  [PROXY.x, way.at],
                ];
                const leave: Point[] = [
                  [PROXY.end, way.at],
                  [ENDPOINTS.x, consumerY],
                ];
                return way.boxed
                  ? [
                      { ...line, points: enter },
                      { ...line, points: leave },
                    ]
                  : [{ ...line, points: [...enter, ...leave] }];
              }),
            ];
            return (
              <g key={`ribbon:${legKey(leg)}`}>
                <title>{tip}</title>
                {lines.map((line, index) =>
                  line.carries && count > 0 ? (
                    <path
                      key={index}
                      d={band(line.points, Math.max(2, width(count)))}
                      fill={line.color}
                      opacity={line.lit ? (focus ? 0.5 : 0.28) : 0.28 * FADE}
                    />
                  ) : (
                    <path
                      key={index}
                      d={thread(line.points)}
                      fill="none"
                      stroke={line.color}
                      strokeWidth={focus && line.lit ? 2 : 1.4}
                      strokeDasharray="4 5"
                      opacity={line.lit ? undefined : FADE}
                    />
                  ),
                )}
              </g>
            );
          })}

          {used.map((egress, index) => {
            const legs = flow.legs.filter((leg) => leg.egressId === egress.id);
            const y = 58 + index * 128;
            const down = egress.health === "DOWN";
            const open = Math.round(legs.reduce((sum, leg) => sum + (counts[legKey(leg)] ?? leg.open), 0));
            return (
              <g key={egress.id} opacity={egressLit(egress.id) ? undefined : FADE} {...point({ kind: "egress", id: egress.id })}>
                <title>{egress.reason ?? egress.name}</title>
                <a href="/settings/networking/egress" onClick={follow("/settings/networking/egress")}>
                  <rect x={EGRESS.x} y={y} width={EGRESS.width} height="108" fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} strokeDasharray={down ? "4 4" : undefined} />
                  <rect x={EGRESS.x} y={y} width="3" height="108" fill={stateColor(egress.health)} />
                  <text x={EGRESS.x + 14} y={y + 24} fill={INK.fg} fontSize="13" fontFamily={TITLE_FONT} fontWeight="600">
                    {egress.name.slice(0, 24)}
                  </text>
                  <StateText x={EGRESS.x + 14} y={y + 46} state={egress.health} label={stateLabel(t, egress.health)} />
                  <text x={EGRESS.x + 14} y={y + 66} fill={INK.muted} fontSize="10">
                    {(egress.addresses?.join(", ") || egress.sourceAddress || egress.interfaceName || t("next.networking.binding.system")).slice(0, 30)}
                  </text>
                  <text x={EGRESS.x + 14} y={y + 86} fill={INK.muted} fontSize="10">
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

          {layout.rows.map(({ leg, y }) => {
            const key = legKey(leg);
            const open = Math.round(counts[key] ?? leg.open);
            // Inspecting a leg keeps its route lit, so picking it again lets go of it.
            const inspect = () => setSelected(selected === key ? null : key);
            return (
              <g key={key} opacity={onRoute(leg) ? undefined : FADE} {...point({ kind: "leg", key })}>
                <rect x={LEGS.x} y={y} width={LEGS.width} height={LEG_HEIGHT} fill={INK.card} stroke={selected === key ? INK.accent : INK.line} />
                <g
                  role="button"
                  tabIndex={0}
                  aria-label={t("next.networking.flow.inspect", { name: consumerName(leg), position: leg.position + 1 })}
                  aria-pressed={selected === key}
                  style={{ cursor: "pointer" }}
                  onClick={inspect}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      inspect();
                    }
                  }}
                >
                  <rect x={LEGS.x} y={y} width={LEGS.width} height={LEG_HEIGHT} fill="transparent" />
                  <text x={LEGS.x + 14} y={y + 21} fill={INK.fg} fontSize="12">
                    {t("next.networking.flow.legTitle", {
                      name: fit(consumerName(leg), 130, 12),
                      position: leg.position + 1,
                      weight: leg.weight,
                    })}
                  </text>
                  <StateText
                    x={LEGS.x + 14}
                    y={y + 38}
                    state={leg.state}
                    label={`${stateLabel(t, leg.state)} · ${t("next.networking.flow.connections", { open, target: leg.target })} · ${formatRate(leg.bytesPerSecond)}`}
                  />
                  {leg.open ? (
                    <text x={LEGS.x + 14} y={y + 54} fill={INK.muted} fontSize="9.5">
                      {leg.sourceAddress ?? t("next.networking.flow.noSource")}
                    </text>
                  ) : (
                    <text x={LEGS.x + 14} y={y + 54} fill={holds(leg) ? INK.warn : INK.muted} fontSize="9.5">
                      {holds(leg) && leg.state === "DOWN"
                        ? t("next.networking.flow.parkedHold")
                        : leg.state === "DOWN"
                          ? t("next.networking.flow.shareMoved")
                          : t("next.networking.flow.planned", { count: leg.target })}
                    </text>
                  )}
                </g>
              </g>
            );
          })}

          {layout.rows.map(({ leg, y, blocks, bypass }) => {
            const status = rungStatus(leg, blocks.length, true);
            return (
              <g key={`proxies:${legKey(leg)}`}>
                {blocks.map((block) => rungBlock(leg, y, block))}
                {bypass == null ? null : (
                  <g opacity={onWay(leg, blocks.length) ? undefined : FADE}>
                    <StateText
                      x={PROXY.x + 14}
                      y={y + bypass - 18}
                      size={9.5}
                      state={status}
                      label={`${t("next.networking.flow.direct")} · ${stateLabel(t, status)}`}
                    />
                  </g>
                )}
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
            const x = ENDPOINTS.x + 14;
            return (
              <g
                key={consumer.key}
                opacity={(down ? 0.65 : 1) * (endpointLit(consumer.key) ? 1 : FADE)}
                {...point({ kind: "endpoint", key: consumer.key })}
              >
                <a href={href} onClick={follow(href)}>
                  <rect x={ENDPOINTS.x} y={y} width={ENDPOINTS.width} height="124" fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} />
                  <text x={x} y={y + 23} fill={INK.fg} fontSize="12.5" fontFamily={TITLE_FONT} fontWeight="600">
                    {consumer.name.slice(0, 24)}
                  </text>
                  <text x={x} y={y + 41} fill={INK.muted} fontSize="9.5">
                    {rss
                      ? t("next.networking.flow.rssKind")
                      : t("next.networking.flow.serverKind", {
                          failover:
                            consumer.route?.failover === "HOLD"
                              ? t("next.networking.failover.hold")
                              : t("next.networking.failover.redistribute"),
                        })}
                  </text>
                  <text x={x} y={y + 67} fill={INK.fg} fontSize="20">
                    {`${Math.round(open)} / ${cap}`}
                  </text>
                  <text x={x} y={y + 85} fill={INK.muted} fontSize="10">
                    {formatRate(legs.reduce((sum, leg) => sum + (leg.bytesPerSecond ?? 0), 0))}
                    {parked ? ` · ${t("next.networking.flow.parked", { count: parked })}` : ""}
                  </text>
                  {Array.from({ length: cells }, (_, cell) => {
                    const boundary = (cell * cap) / cells;
                    const reserved = boundary >= cap - parked;
                    return (
                      <rect
                        key={cell}
                        x={x + cellX(cell)}
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
            <text x={LEGS.x} y="85" fill={INK.muted} fontSize="12">
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

import { memo, useEffect, useMemo, useRef, useState, useSyncExternalStore, type ReactNode } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import {
  closedPath,
  isDown,
  OUTBOUND_CONSUMER,
  type DownTarget,
  type Egress,
  type FailingHop,
  type LegFlow,
  type NetworkFlow as Flow,
  type NetworkRoute,
  type PathOverride,
  type PoolMemberFlow,
  type ProxyPool,
  type Rung,
} from "@/lib/networking";
import { proxyLabels, type ProxyProfile } from "@/lib/proxies";
import { EmptyState } from "../../components/chrome";
import { SecondaryButton, Select } from "../../components/controls";
import { Icon } from "../../components/icons";
import { countLabel } from "../../i18n/labels";
import { metered, quotaSummary } from "./egress-quota";
import { formatRate, stateColor, stateLabel, stateTone } from "./presentation";

type FlowConsumer = { key: string; name: string; cap?: number; route?: NetworkRoute | null };

/** The far end a speed test measures against: what it is, where it is, and what it is doing. */
export type FlowEndpoint = { title: string; detail: string; state: string; label: string };

/** What a box in the flow stands for, and so what picking it edits. */
export type FlowTarget =
  | { kind: "egress"; id: number }
  | { kind: "route"; consumer: string }
  | { kind: "pool"; id: number }
  | { kind: "proxy"; id: number };

/** The flow's colours: the theme's own tokens, as literal values. The accent stays a variable. */
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

type Lane = { x: number; width: number; end: number };
const lane = (x: number, width: number): Lane => ({ x, width, end: x + width });
type Lanes = { EGRESS: Lane; LEGS: Lane; PROXY: Lane; ENDPOINTS: Lane | null; WIDTH: number; HOP_GAP: number };
/** The four lanes, left to right. Ribbons cross the gaps between them. */
const FULL: Lanes = {
  EGRESS: lane(16, 214),
  LEGS: lane(286, 300),
  PROXY: lane(642, 440),
  ENDPOINTS: lane(1138, 214),
  WIDTH: 1368,
  HOP_GAP: 26,
};
/**
 * One endpoint's own flow, narrow enough to sit inside that endpoint's editor.
 * The editor is the endpoint, so the last lane is left out.
 */
const COMPACT: Lanes = {
  EGRESS: lane(8, 170),
  LEGS: lane(202, 246),
  PROXY: lane(472, 270),
  ENDPOINTS: null,
  WIDTH: 750,
  HOP_GAP: 14,
};
/** A compact flow that ends at the one endpoint a speed test measures against. */
const COMPACT_ENDPOINT: Lanes = { ...COMPACT, ENDPOINTS: lane(798, 170), WIDTH: 976 };
const ENDPOINT_BOX_HEIGHT = 76;
const EGRESS_HEIGHT = 108;
/** The row an egress card adds at its bottom for each limit it is held to. */
const EGRESS_LIMIT_ROW = 20;
const EGRESS_GAP = 20;
/** The line each address after an egress's first adds to its card: they stack, one to a line. */
const ADDRESS_LINE = 14;
const LEG_HEIGHT = 64;
const BOX_HEIGHT = 44;
const NOTE_LINE = 12;
/** The space a pool box keeps between its member rows and its head above, and its edge below. */
const MEMBER_GAP = 8;
/** The connections that fill the widest ribbon, unless a cap is larger: twenty is not many. */
const RIBBON_FULL = 100;
const EGRESS_PAGE = "/settings/networking/egress";
const PROXIES_PAGE = "/settings/networking/proxies";
const routePage = (consumer: string) => `/settings/networking/routes?consumer=${encodeURIComponent(consumer)}`;

type Point = [number, number];
type Member = {
  id: number;
  state: string;
  live: PoolMemberFlow | undefined;
  /** The pool has timed the member's connections or measured what they deliver. */
  measured: boolean;
  /** The member holds connections for the pool right now. */
  carrying: boolean;
  /** Why the member is out of use, broken to fit its row. */
  note: string[];
  height: number;
};
/** One ladder rung as the proxy lane draws it. */
type Block = {
  rung: Rung;
  rungIndex: number;
  top: number;
  height: number;
  failing: FailingHop | undefined;
  /** The failing hop's reason, broken to fit the box that hop is drawn in. */
  note: string[];
  members: Member[];
  /** Where each member's row starts, below the block's top. */
  memberTops: number[];
  /** Where a line carrying the rung meets it, below the block's top: the member carrying the pool, or the head. */
  anchor: number;
};
/** One leg's row: where it stands, how tall it is, and its ladder. */
type Row = {
  leg: LegFlow;
  y: number;
  height: number;
  cardHeight: number;
  note: string[];
  blocks: Block[];
  /** Where the line with no tunnel crosses, or null when the leg has none. */
  bypass: number | null;
  closed: boolean;
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

/** The limits an egress is held to, each a row at the bottom of its card: its speed limit, then its quota. */
const heldTo = (egress: Egress) => [
  ...(egress.maxDownloadSpeed > 0 ? (["speed"] as const) : []),
  ...(metered(egress) ? (["quota"] as const) : []),
];

/** Monospaced text cut to the width it has, ending in an ellipsis when it was cut. */
function fit(text: string, width: number, size: number) {
  const room = Math.max(1, Math.floor(width / (size * 0.6)));
  return text.length > room ? `${text.slice(0, room - 1)}…` : text;
}

/** Monospaced text broken at its spaces into at most `limit` lines of the width it has. */
function wrap(text: string, width: number, size: number, limit: number) {
  const room = Math.max(1, Math.floor(width / (size * 0.6)));
  const lines: string[] = [];
  for (const word of text.split(/\s+/).filter(Boolean)) {
    const last = lines.length - 1;
    // Whatever is past the last line joins it and is cut there.
    if (last >= 0 && (lines.length === limit || `${lines[last]} ${word}`.length <= room)) {
      lines[last] = `${lines[last]} ${word}`;
    } else {
      lines.push(word);
    }
  }
  return lines.map((line) => fit(line, width, size));
}

/** The room a box makes for the lines written under its state. */
const noteHeight = (lines: number) => (lines ? lines * NOTE_LINE + 4 : 0);

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

// A route card draws one box per connection. A full row holds this many; more
// connections wrap onto further rows of the same size instead of thinning the boxes.
const CELLS_PER_ROW = 20;
const CELL_ROW_PITCH = 13;
const CELL_HEIGHT = 10;
const MAX_CELLS = 100;
const ENDPOINT_HEIGHT = 124;
const ENDPOINT_GAP = 20;
// Only a server's card counts its connections. A feed's card is its name over
// what it is; Default outbound's is its name alone.
const FEED_HEIGHT = 56;
const OUTBOUND_HEIGHT = 38;

type EndpointCard = { cap: number; cells: number; rows: number; y: number; height: number; anchor: number };

/**
 * Where each route card sits, and where its legs meet it: halfway down, or on
 * a server's card halfway down its first row of connection boxes. A server's
 * card is as tall as those rows need.
 */
function endpointCards(consumers: FlowConsumer[], legs: LegFlow[]): Map<string, EndpointCard> {
  const cards = new Map<string, EndpointCard>();
  let y = 58;
  for (const consumer of consumers) {
    const own = legs.filter((leg) => leg.consumer === consumer.key);
    const cap = consumer.cap ?? own.reduce((sum, leg) => sum + leg.target, 0);
    const cells = Math.min(cap, MAX_CELLS);
    const rows = Math.max(1, Math.ceil(cells / CELLS_PER_ROW));
    const height =
      consumer.key === OUTBOUND_CONSUMER
        ? OUTBOUND_HEIGHT
        : consumer.key.startsWith("rss:")
          ? FEED_HEIGHT
          : ENDPOINT_HEIGHT + (rows - 1) * CELL_ROW_PITCH;
    cards.set(consumer.key, { cap, cells, rows, y, height, anchor: y + Math.min(height, ENDPOINT_HEIGHT) / 2 });
    y += height + ENDPOINT_GAP;
  }
  return cards;
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
  if (state === "DOWN") {
    return "DOWN";
  }
  if (state === "COOLDOWN") {
    return "COOLDOWN";
  }
  return leg.state === "DOWN" || state === "FAILING" ? "FAILING" : "STANDBY";
}

/**
 * What a pool member is doing, in the one word the diagram shows for it. A
 * member holding connections is active; one held in reserve is on standby,
 * as an idle rung is, whether or not the pool has measured it yet.
 */
function memberState(member: PoolMemberFlow | undefined, pinned: boolean, enabled: boolean): string {
  if (!enabled || member?.state === "DISABLED") {
    return "DISABLED";
  }
  if (member?.blocked) {
    return "BLOCKED";
  }
  if ((member?.open ?? 0) > 0) {
    return "ACTIVE";
  }
  if (pinned) {
    return "PINNED";
  }
  if ((member?.opening ?? 0) > 0) {
    return "PROBING";
  }
  if (member?.state === "SUSPECT" || member?.state === "CHALLENGER" || member?.state === "PROBING") {
    return member.state;
  }
  if (member?.failures) {
    return "FAILING";
  }
  return "STANDBY";
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

/**
 * Animate measurements only: configuration and card positions change once per sample.
 * A flow that arrives with the same legs, each count where it already stands, draws nothing more.
 */
function useMeasurements(legs: LegFlow[]) {
  const [counts, setCounts] = useState<Record<string, number>>({});
  const current = useRef<Record<string, number>>({});
  useEffect(() => {
    const values = legs.map((leg) => ({
      key: legKey(leg),
      from: current.current[legKey(leg)] ?? leg.open,
      to: leg.open,
    }));
    // Every frame would set the counts already held, key for key.
    if (
      Object.keys(current.current).length === new Set(values.map((value) => value.key)).size &&
      values.every((value) => Object.hasOwn(current.current, value.key) && value.from === value.to)
    ) {
      return;
    }
    let start: number | undefined;
    let frame: number;
    const step = (now: number) => {
      if (start === undefined) {
        start = now;
      }
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
const StateText = memo(function StateText({
  x,
  y,
  state,
  label,
  size = 10.5,
}: {
  x: number;
  y: number;
  state: string;
  label: string;
  size?: number;
}) {
  const color = stateColor(state);
  return (
    <g>
      <rect x={x} y={y - 7} width="7" height="7" fill={color} />
      <text x={x + 13} y={y} fill={color} fontSize={size} fontFamily={FONT}>
        {label}
      </text>
    </g>
  );
});

/** The bar down a box's left edge, in the colour of the state the box is in. */
const StateBar = memo(function StateBar({ x, y, height, state }: { x: number; y: number; height: number; state: string }) {
  return <rect x={x} y={y} width="3" height={height} fill={stateColor(state)} />;
});

/** Lines written under a box's state: what went wrong with what the box stands for. */
const Note = memo(
  function Note({ x, y, lines }: { x: number; y: number; lines: string[] }) {
    return (
      <>
        {lines.map((line, index) => (
          <text key={index} x={x} y={y + index * NOTE_LINE} fill={INK.muted} fontSize="9.5">
            {line}
          </text>
        ))}
      </>
    );
  },
  // The lines are worked out afresh on every sample; the same words draw nothing more.
  (before, after) =>
    before.x === after.x &&
    before.y === after.y &&
    before.lines.length === after.lines.length &&
    before.lines.every((line, index) => line === after.lines[index]),
);

/** A proxy in the lane: its name, then its state, then what is wrong with it, behind its state bar. */
function HopBox({
  x,
  y,
  width,
  height = BOX_HEIGHT,
  title,
  aside,
  state,
  label,
  note = [],
  active,
  toggle,
  toggleWidth = 0,
}: {
  x: number;
  y: number;
  width: number;
  height?: number;
  title: string;
  /** A figure set against the right edge of the title's line. */
  aside?: string;
  state: string;
  label: string;
  note?: string[];
  active: boolean;
  /** The box's Up/Down switch, drawn at the right of the title's line. */
  toggle?: (x: number, y: number) => ReactNode;
  toggleWidth?: number;
}) {
  const down = state === "DOWN";
  const switchRoom = toggle ? toggleWidth + 8 : 0;
  return (
    <g opacity={state === "DISABLED" ? 0.6 : 1}>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={active ? INK.rowActive : INK.card}
        stroke={down ? stateColor("DOWN") : INK.line}
        strokeDasharray={down ? "4 4" : undefined}
      />
      <StateBar x={x} y={y} height={height} state={state} />
      <text x={x + 14} y={y + 18} fill={INK.fg} fontSize="11">
        {fit(title, width - 26 - switchRoom - (aside ? aside.length * 6 + 10 : 0), 11)}
      </text>
      {aside ? (
        <text x={x + width - 12 - switchRoom} y={y + 18} fill={INK.fg} fontSize="10" textAnchor="end">
          {aside}
        </text>
      ) : null}
      <StateText x={x + 14} y={y + 35} size={9.5} state={state} label={fit(label, width - 39, 9.5)} />
      <Note x={x + 14} y={y + BOX_HEIGHT + 5} lines={note} />
      {toggle ? toggle(x + width - 10 - toggleWidth, y + 6) : null}
    </g>
  );
}

/** How wide each half of an Up/Down switch is: room for the longer of its two words. */
const switchHalf = (words: readonly string[]) => Math.max(24, Math.ceil(Math.max(...words.map((word) => word.length)) * 9 * 0.6) + 8);

/**
 * An Up/Down switch drawn inside a box, Up until it is turned: Down leaves the
 * box out of the route a speed test measures. A click does not leave a focus
 * ring behind; the keyboard still shows where it is.
 */
function UpDownSwitch({
  x,
  y,
  half,
  down,
  label,
  words,
  onToggle,
}: {
  x: number;
  y: number;
  half: number;
  down: boolean;
  label: string;
  words: readonly [string, string];
  onToggle: () => void;
}) {
  const pick = (wantDown: boolean) => {
    if (wantDown !== down) {
      onToggle();
    }
  };
  return (
    <g
      role="radiogroup"
      aria-label={label}
      onKeyDown={(event) => {
        const wantDown = ["ArrowRight", "ArrowDown"].includes(event.key) ? true : ["ArrowLeft", "ArrowUp"].includes(event.key) ? false : null;
        if (wantDown === null) {
          return;
        }
        event.preventDefault();
        event.stopPropagation();
        pick(wantDown);
        (event.currentTarget.children[wantDown ? 1 : 0] as SVGGElement | undefined)?.focus();
      }}
    >
      {words.map((word, index) => {
        const isDown = index === 1;
        const checked = isDown === down;
        const color = stateColor(isDown ? "DOWN" : "UP");
        const left = x + index * half;
        return (
          <g
            key={index}
            role="radio"
            aria-checked={checked}
            tabIndex={checked ? 0 : -1}
            className="group cursor-pointer outline-none"
            onMouseDown={(event) => event.preventDefault()}
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              pick(isDown);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                event.stopPropagation();
                pick(isDown);
              }
            }}
          >
            <rect x={left} y={y} width={half} height="16" fill={checked ? color : INK.row} stroke={checked ? color : INK.control} />
            <rect
              x={left + 1.5}
              y={y + 1.5}
              width={half - 3}
              height="13"
              fill="none"
              stroke={INK.fg}
              className="opacity-0 group-focus-visible:opacity-100"
            />
            <text x={left + half / 2} y={y + 11.5} textAnchor="middle" fontSize="9" fill={checked ? INK.ground : INK.muted}>
              {word}
            </text>
          </g>
        );
      })}
    </g>
  );
}

const LESS_MOTION = "(prefers-reduced-motion: reduce)";
const lessMotion = () => window.matchMedia?.(LESS_MOTION).matches === true;
const onMotionChange = (changed: () => void) => {
  const query = window.matchMedia?.(LESS_MOTION);
  query?.addEventListener("change", changed);
  return () => query?.removeEventListener("change", changed);
};

/** Whether the reader asks for less motion, followed while the flow is shown; traffic then stands still. */
const useLessMotion = () => useSyncExternalStore(onMotionChange, lessMotion, () => false);

/** Traffic on a line: dashes that crawl from the egress out toward the endpoint. */
function Traffic({ points, width, still }: { points: Point[]; width: number; still: boolean }) {
  return (
    <path d={thread(points)} fill="none" stroke={stateColor("ACTIVE")} strokeWidth={width} strokeLinecap="round" strokeDasharray="18 30">
      {still ? null : <animate attributeName="stroke-dashoffset" from="48" to="0" dur="0.8s" repeatCount="indefinite" />}
    </path>
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
 * the line crosses the lane untouched. A leg behind a kill switch has no rung
 * and may not go direct, so the kill switch is the box its line stops at.
 *
 * Every box wears its state as a bar down its left edge, and says what is
 * wrong with it inside itself. Pointing at a box lights the routes through
 * it, from egress to endpoint, and fades the rest. Picking a box opens the
 * editor for what it stands for, through `onEdit`; without one the flow is
 * only shown.
 *
 * `compact` draws one endpoint's flow for that endpoint's own editor: no
 * controls, and no endpoint lane, since the editor is the endpoint. With
 * `endpoint` it ends at that one box instead, the far end a speed test
 * measures against. With `traffic` the lines carrying a leg that is moving
 * bytes move, except those a pointed-at box fades. With `trace`, pointing at
 * a box sets the routes it lights moving the same way, each in its ribbon,
 * along the way each leg takes (or the one way pointed at); a line that is
 * down, failing or blocked stays still. Nothing moves for a reader who asks
 * for less motion.
 */
export const NetworkFlow = memo(function NetworkFlow({
  flow,
  egresses,
  profiles,
  pools,
  consumers,
  onEdit,
  compact = false,
  endpoint,
  traffic = false,
  trace = false,
  toggles,
}: {
  flow: Flow;
  egresses: Egress[];
  profiles: ProxyProfile[];
  pools: ProxyPool[];
  consumers: FlowConsumer[];
  onEdit?: (target: FlowTarget) => void;
  compact?: boolean;
  endpoint?: FlowEndpoint;
  traffic?: boolean;
  trace?: boolean;
  /**
   * What a speed test leaves out, and with `onToggle` a Down switch on every
   * egress, leg, rung, chain hop and pool member to change it. Downed proxies
   * are drawn down here; the caller draws downed legs and rungs into `flow`.
   */
  toggles?: { overrides: PathOverride; onToggle?: (target: DownTarget) => void };
}) {
  const t = useTranslate();
  const [hover, setHover] = useState<Focus | null>(null);
  const [filter, setFilter] = useState("");
  const [group, setGroup] = useState<number | null>(null);
  const counts = useMeasurements(flow.legs);
  const still = useLessMotion();
  const lanes = compact ? (endpoint ? COMPACT_ENDPOINT : COMPACT) : FULL;
  const onToggle = toggles?.onToggle;
  const overrides = toggles?.overrides;
  const switchWords = [t("next.networking.speed.upSwitch"), t("next.networking.speed.downSwitch")] as const;
  const half = switchHalf(switchWords);
  const toggleWidth = onToggle ? 2 * half : 0;
  const { EGRESS, LEGS, PROXY, ENDPOINTS, WIDTH, HOP_GAP } = lanes;
  // With one route picked, the flow is that route alone: its legs, the
  // egresses they leave through and its own card.
  const shownLegs = useMemo(
    () => (filter ? flow.legs.filter((leg) => leg.consumer === filter) : flow.legs),
    [flow.legs, filter],
  );
  const shown = useMemo(
    () => (filter ? consumers.filter((consumer) => consumer.key === filter) : consumers),
    [consumers, filter],
  );
  // An egress no route leaves through is not part of the flow, so it has no card.
  const used = useMemo(
    () => egresses.filter((egress) => shownLegs.some((leg) => leg.egressId === egress.id)),
    [egresses, shownLegs],
  );
  // Each egress card in the lane, from the top; one with a speed limit is a row taller.
  const egressCards = useMemo(() => {
    const placed: { egress: Egress; y: number; shift: number; height: number }[] = [];
    let top = 58;
    for (const egress of used) {
      // Everything below the addresses moves down by the ones stacked after the first.
      const shift = Math.max(0, (egress.addresses?.length ?? 0) - 1) * ADDRESS_LINE;
      const card = { egress, y: top, shift, height: EGRESS_HEIGHT + shift + heldTo(egress).length * EGRESS_LIMIT_ROW };
      placed.push(card);
      top += card.height + EGRESS_GAP;
    }
    return placed;
  }, [used]);
  const egressBottom = egressCards.reduce((bottom, card) => Math.max(bottom, card.y + card.height), 38);
  const grouped = !compact && (used.length > 6 || shownLegs.length > 40);
  const cards = useMemo(() => endpointCards(shown, flow.legs), [shown, flow.legs]);
  const cardsBottom = [...cards.values()].reduce((bottom, card) => Math.max(bottom, card.y + card.height), 0);

  const layout = useMemo(() => {
    const legs = shownLegs
      .filter((leg) => !grouped || group === leg.egressId)
      .sort((a, b) => a.egressId - b.egressId || a.consumer.localeCompare(b.consumer) || a.position - b.position);
    let offset = 58;
    const rows: Row[] = [];
    for (const leg of legs) {
      let bottom = 0;
      const blocks: Block[] = [];
      const rungs: Rung[] = leg.path.kind === "DIRECT" ? [] : leg.path.rungs;
      for (const [rungIndex, rung] of rungs.entries()) {
        const live =
          rung.kind === "POOL"
            ? flow.pools.find((candidate) => candidate.poolId === rung.poolId && candidate.egressId === leg.egressId)
            : undefined;
        const configured = rung.kind === "POOL" ? pools.find((candidate) => candidate.id === rung.poolId) : undefined;
        const failing = leg.failingHops?.find((hop) => hop.rung === rungIndex);
        const members: Member[] = (configured?.memberIds ?? live?.members.map((member) => member.id) ?? []).map((id) => {
          const member = live?.members.find((candidate) => candidate.id === id);
          const measured = member != null && (member.connectMs != null || member.bytesPerSecond != null);
          const carrying = (member?.open ?? 0) > 0;
          const said = member?.blocked ?? (failing?.proxyId === id ? failing.reason : null);
          const note = said ? wrap(said, lanes.PROXY.width - 45 - toggleWidth, 9.5, 2) : [];
          return {
            id,
            live: member,
            state:
              overrides && isDown(overrides, { kind: "proxy", id })
                ? "DOWN"
                : memberState(
                    member,
                    live?.pinnedMember === id,
                    profiles.find((profile) => profile.id === id)?.enabled !== false,
                  ),
            measured,
            carrying,
            note,
            height: (measured || carrying ? 32 : 20) + note.length * NOTE_LINE,
          };
        });
        // A failing hop says why inside its own box: the hop's in a chain, the member's row in a pool.
        const boxWidth =
          rung.kind === "CHAIN"
            ? (lanes.PROXY.width - (rung.chainIds.length - 1) * lanes.HOP_GAP) / rung.chainIds.length
            : lanes.PROXY.width;
        const note =
          failing && !members.some((member) => member.id === failing.proxyId)
            ? wrap(failing.reason, boxWidth - 26, 9.5, rung.kind === "CHAIN" ? 4 : 2)
            : [];
        // What carries the pool's traffic first, then the quickest to connect; the unmeasured last.
        const latency = (member: Member) => member.live?.connectMs ?? Number.POSITIVE_INFINITY;
        members.sort(
          (a, b) => Number(b.state === "ACTIVE") - Number(a.state === "ACTIVE") || latency(a) - latency(b) || 0,
        );
        // An active head and a carrying member share a tint, so the rows start a gap below the head.
        const memberTops: number[] = [];
        let memberTop = BOX_HEIGHT + noteHeight(note.length) + MEMBER_GAP;
        for (const member of members) {
          memberTops.push(memberTop);
          memberTop += member.height;
        }
        const carrier = members.findIndex((member) => member.carrying);
        const anchor = carrier >= 0 ? memberTops[carrier]! + (members[carrier]!.height - 2) / 2 : BOX_HEIGHT / 2;
        const height =
          BOX_HEIGHT +
          noteHeight(note.length) +
          members.reduce((sum, member) => sum + member.height, 0) +
          (members.length ? 2 * MEMBER_GAP : 0);
        const top = bottom;
        bottom = top + height + 8;
        blocks.push({ rung, rungIndex, top, height, failing, note, members, memberTops, anchor });
      }
      // Direct has no box: its line crosses level with the leg, or beneath the ladder it backs up.
      const bypass = leg.path.kind === "DIRECT" ? LEG_HEIGHT / 2 : leg.path.directFallback ? bottom + 26 : null;
      // A kill switch stands where the ladder's first rung would.
      const closed = closedPath(leg.path);
      const note = leg.reason ? wrap(leg.reason, lanes.LEGS.width - 28, 9.5, 2) : [];
      const cardHeight = LEG_HEIGHT + noteHeight(note.length);
      const y = offset;
      const height = Math.max(cardHeight, closed ? BOX_HEIGHT : bypass == null ? bottom - 8 : bypass + 16);
      offset += height + 18;
      rows.push({ leg, y, height, cardHeight, note, blocks, bypass, closed });
    }
    // The one endpoint stands level with the middle of everything that leads to it.
    const middle = (58 + Math.max(offset - 18, egressBottom)) / 2;
    const endpointY = Math.max(58, Math.round(middle - ENDPOINT_BOX_HEIGHT / 2));
    const bottom = Math.max(
      offset - 16,
      egressBottom,
      compact ? 0 : cardsBottom,
      lanes === COMPACT_ENDPOINT ? endpointY + ENDPOINT_BOX_HEIGHT : 0,
    );
    return { rows, endpointY, height: compact ? bottom + 12 : Math.max(bottom + 52, 260) };
  }, [shownLegs, flow.pools, grouped, group, pools, profiles, egressBottom, cardsBottom, lanes, compact, toggleWidth, overrides]);

  const width = scale(Math.max(RIBBON_FULL, ...consumers.map((consumer) => consumer.cap ?? 1)), 28);
  const profile = (id: number | null) => profiles.find((candidate) => candidate.id === id);
  const name = (id: number) => profile(id)?.name ?? t("next.networking.proxyNumber", { id });
  const poolName = (id: number | null) =>
    pools.find((pool) => pool.id === id)?.name ?? t("next.networking.flow.pool");
  const consumerName = (leg: LegFlow) => consumers.find((consumer) => consumer.key === leg.consumer)?.name ?? leg.consumer;
  const holds = (leg: LegFlow) => consumers.find((consumer) => consumer.key === leg.consumer)?.route?.failover === "HOLD";

  // The box under the pointer picks the routes to light. Everything else fades, and with nothing
  // pointed at the whole flow is lit.
  const focus = hover;
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
  });
  /**
   * A box that opens the editor for what it stands for. It is a real link to that thing's page; on
   * screen the editor opens over the flow instead. Where the flow is only shown, it is just the box.
   */
  function card(target: FlowTarget | null, href: string, children: ReactNode, label?: string, key?: number) {
    if (!onEdit || !target) {
      return <g key={key}>{children}</g>;
    }
    return (
      <a
        key={key}
        href={href}
        aria-label={label}
        onClick={(event) => {
          event.preventDefault();
          // The editor covers the box, so the pointer leaves it without saying so.
          setHover(null);
          onEdit(target);
        }}
      >
        {children}
      </a>
    );
  }
  const proxyTarget = (id: number | null): FlowTarget | null => (id == null ? null : { kind: "proxy", id });
  /** The Up/Down switch for one box, while a speed test is being set up. */
  const toggle = (target: DownTarget, label: string) =>
    toggles && onToggle
      ? (x: number, y: number) => (
          <UpDownSwitch
            x={x}
            y={y}
            half={half}
            down={isDown(toggles.overrides, target)}
            label={label}
            words={switchWords}
            onToggle={() => onToggle(target)}
          />
        )
      : undefined;
  const proxyDown = (id: number) => !!toggles && isDown(toggles.overrides, { kind: "proxy", id });

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

    if (rung.kind === "CHAIN") {
      const hopWidth = (PROXY.width - (rung.chainIds.length - 1) * HOP_GAP) / rung.chainIds.length;
      const failingAt = failing ? rung.chainIds.indexOf(failing.proxyId) : -1;
      return (
        <g key={rungIndex} role="group" aria-label={rung.chainIds.map(name).join(" > ")} {...way}>
          {ground}
          {rung.chainIds.map((id, position) => {
            const x = PROXY.x + position * (hopWidth + HOP_GAP);
            const state = proxyDown(id) ? "DOWN" : hopState(position, failingAt, status, profile(id)?.enabled !== false);
            return (
              <g key={id}>
                <title>{[name(id), ...(failing && position === failingAt ? [failing.reason] : [])].join(" · ")}</title>
                {card(
                  proxyTarget(id),
                  PROXIES_PAGE,
                  <HopBox
                    x={x}
                    y={y}
                    width={hopWidth}
                    height={block.height}
                    title={name(id)}
                    state={state}
                    label={stateLabel(t, state)}
                    note={position === Math.max(0, failingAt) ? block.note : []}
                    active={isActive}
                    toggle={toggle({ kind: "proxy", id }, name(id))}
                    toggleWidth={toggleWidth}
                  />,
                )}
                {position + 1 < rung.chainIds.length ? (
                  <text x={x + hopWidth + HOP_GAP / 2} y={y + 27} fill={INK.muted} fontSize="15" textAnchor="middle">
                    {">"}
                  </text>
                ) : null}
              </g>
            );
          })}
        </g>
      );
    }

    if (rung.kind === "POOL") {
      const configured = pools.find((pool) => pool.id === rung.poolId);
      const state = configured?.enabled === false ? "DISABLED" : status;
      const held = members.reduce((sum, member) => sum + (member.live?.open ?? 0), 0);
      const head = BOX_HEIGHT + noteHeight(block.note.length);
      return (
        <g key={rungIndex} {...way}>
          {failing ? <title>{failing.reason}</title> : null}
          <rect x={PROXY.x} y={y} width={PROXY.width} height={block.height} fill={INK.card} stroke={INK.line} />
          {card(
            rung.poolId == null ? null : { kind: "pool", id: rung.poolId },
            PROXIES_PAGE,
            <HopBox
              x={PROXY.x}
              y={y}
              width={PROXY.width}
              height={head}
              title={t("next.networking.flow.poolRung", { name: poolName(rung.poolId) })}
              aside={held > 0 ? countLabel(t, "next.networking.flow.holding", held) : undefined}
              state={state}
              label={stateLabel(t, state)}
              note={block.note}
              active={isActive}
              toggleWidth={toggleWidth}
              toggle={toggle(
                { kind: "rung", leg: leg.position, rung: rungIndex },
                t("next.networking.route.rungTarget", { leg: leg.position + 1, rung: rungIndex + 1 }),
              )}
            />,
          )}
          <StateBar x={PROXY.x} y={y} height={block.height} state={state} />
          <g role="group" aria-label={t("next.networking.flow.poolMembers", { name: poolName(rung.poolId) })}>
            {members.map((member, index) => {
              const top = y + block.memberTops[index]!;
              // Only what the pool has measured: a figure it has not taken is left out, not dashed.
              const ms = member.measured ? member.live?.connectMs : null;
              const speed =
                member.measured && member.live?.bytesPerSecond != null
                  ? t("next.networking.flow.memberSpeed", { rate: formatRate(member.live.bytesPerSecond) })
                  : null;
              const timings = (latency: string | null) =>
                member.measured ? [latency, speed].filter((part) => part !== null).join(" · ") : null;
              // The hover has no room for the signal mark, so it says what the figure is.
              const timing = timings(ms == null ? null : t("next.networking.flow.memberConnect", { ms: ms.toFixed(0) }));
              // A member carrying traffic shows what it carries now; its timings move to the hover.
              const signal = !member.carrying && ms != null;
              const detail = member.carrying
                ? t("next.networking.flow.egressLoad", {
                    open: member.live?.open ?? 0,
                    rate: formatRate(member.live?.liveBytesPerSecond ?? 0),
                  })
                : timings(ms == null ? null : t("next.networking.flow.memberLatency", { ms: ms.toFixed(0) }));
              const detailX = PROXY.x + 29 + (signal ? 14 : 0);
              return card(
                proxyTarget(member.id),
                PROXIES_PAGE,
                <g opacity={member.state === "DISABLED" ? 0.6 : 1}>
                  {member.carrying && timing ? <title>{timing}</title> : null}
                  <rect
                    x={PROXY.x + 8}
                    y={top}
                    width={PROXY.width - 16}
                    height={member.height - 2}
                    fill={member.carrying ? INK.rowActive : INK.row}
                  />
                  <rect x={PROXY.x + 16} y={top + 6} width="7" height="7" fill={stateColor(member.state)} />
                  <text x={PROXY.x + 29} y={top + 13} fill={INK.secondary} fontSize="10">
                    {`${fit(name(member.id), PROXY.width / 2 - 20, 10)} · ${stateLabel(t, member.state)}`}
                  </text>
                  {signal ? (
                    <g transform={`translate(${PROXY.x + 29} ${top + 17})`} color={INK.muted}>
                      <Icon name="signal" size={10} />
                    </g>
                  ) : null}
                  {detail ? (
                    <text x={detailX} y={top + 25} fill={INK.muted} fontSize="9.5">
                      {fit(detail, PROXY.end - 16 - detailX, 9.5)}
                    </text>
                  ) : null}
                  <Note x={PROXY.x + 29} y={top + (detail ? 37 : 25)} lines={member.note} />
                  {toggle({ kind: "proxy", id: member.id }, name(member.id))?.(
                    PROXY.end - 16 - toggleWidth,
                    top + 2,
                  )}
                </g>,
                undefined,
                member.id,
              );
            })}
          </g>
        </g>
      );
    }

    const hop = profile(rung.proxyId);
    const state =
      hop?.enabled === false
        ? "DISABLED"
        : status === "DOWN"
          ? "DOWN"
          : failing && status !== "COOLDOWN"
            ? "FAILING"
            : status;
    return (
      <g key={rungIndex} {...way}>
        {failing ? <title>{failing.reason}</title> : null}
        {ground}
        {card(
          proxyTarget(rung.proxyId),
          PROXIES_PAGE,
          <HopBox
            x={PROXY.x}
            y={y}
            width={PROXY.width}
            height={block.height}
            title={`${name(rung.proxyId ?? 0)} · ${hop ? proxyLabels[hop.kind] : t("next.networking.flow.proxy")}`}
            state={state}
            label={stateLabel(t, state)}
            note={block.note}
            active={isActive}
            toggleWidth={toggleWidth}
            toggle={toggle(
              { kind: "rung", leg: leg.position, rung: rungIndex },
              t("next.networking.route.rungTarget", { leg: leg.position + 1, rung: rungIndex + 1 }),
            )}
          />,
        )}
      </g>
    );
  }

  if (consumers.length === 0 && egresses.length === 0) {
    return <EmptyState title={t("next.networking.flow.emptyTitle")} body={t("next.networking.flow.emptyBody")} />;
  }

  return (
    <div className="flex min-w-0 flex-col">
      {compact ? null : (
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
        </div>
      )}
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
              {egress.name} · {countLabel(t, "next.networking.legCount", shownLegs.filter((leg) => leg.egressId === egress.id).length)}
            </SecondaryButton>
          ))}
        </div>
      ) : null}
      <div className="min-w-0 overflow-x-auto bg-wv-app">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox={`0 0 ${WIDTH} ${layout.height}`}
          role="group"
          aria-label={t("next.networking.flow.diagram")}
          className={
            compact ? (endpoint ? "block w-full min-w-[720px]" : "block w-full min-w-[560px]") : "block w-full min-w-[1160px]"
          }
          style={{ background: INK.ground, fontFamily: FONT }}
        >
          {[
            [EGRESS.x, t("next.networking.flow.egressColumn")],
            [LEGS.x, t("next.networking.flow.legsColumn")],
            [PROXY.x, t("next.networking.flow.proxyColumn")],
            ...(ENDPOINTS
              ? [[ENDPOINTS.x, t(endpoint ? "next.networking.flow.endpointColumn" : "next.networking.flow.consumersColumn")]]
              : []),
          ].map(([x, label]) => (
            <text key={String(x)} x={Number(x) + 4} y="30" fill={INK.faint} fontSize="10.5" fontWeight="600" letterSpacing="1.5" fontFamily={TITLE_FONT}>
              {String(label).toUpperCase()}
            </text>
          ))}

          {layout.rows.map(({ leg, y, blocks, bypass, closed }) => {
            const egressCard = egressCards.find((card) => card.egress.id === leg.egressId) ?? egressCards[0];
            const egressY = egressCard ? egressCard.y + EGRESS_HEIGHT / 2 : 112;
            const consumerY = endpoint
              ? layout.endpointY + ENDPOINT_BOX_HEIGHT / 2
              : (cards.get(leg.consumer)?.anchor ?? 58 + ENDPOINT_HEIGHT / 2);
            const legY = y + LEG_HEIGHT / 2;
            const count = counts[legKey(leg)] ?? leg.open;
            const flowing = traffic && leg.open > 0 && (leg.bytesPerSecond ?? 0) > 0;
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
              ...blocks.map((block) => {
                const status = rungStatus(leg, block.rungIndex, false);
                // The rung carrying the leg is met where its traffic goes: a pool's at the member carrying it.
                return { at: y + block.top + (status === "ACTIVE" ? block.anchor : BOX_HEIGHT / 2), boxed: true, status };
              }),
              ...(bypass == null ? [] : [{ at: y + bypass, boxed: false, status: rungStatus(leg, blocks.length, true) }]),
              ...(closed ? [{ at: y + BOX_HEIGHT / 2, boxed: true, status: "BLOCKED" }] : []),
            ];
            const taken = ways.find((way) => way.status === "ACTIVE") ?? ways[leg.selectedRung ?? 0] ?? ways[0];
            const passable = stateTone(leg.state) !== "bad";
            // `moves`: what a pointed-at route sets moving, the way the leg takes or the one pointed at.
            const lines: { points: Point[]; carries: boolean; color: string; lit: boolean; moves: boolean }[] = [
              {
                points: [
                  [EGRESS.end, egressY],
                  [LEGS.x, legY],
                ],
                carries: true,
                color,
                lit: onRoute(leg),
                moves: passable,
              },
              ...ways.flatMap((way, index) => {
                const line = {
                  carries: way === taken,
                  color: way === taken ? color : stateColor(way.status),
                  lit: onWay(leg, index),
                  moves: passable && stateTone(way.status) !== "bad" && (way === taken || focus?.kind === "way"),
                };
                const enter: Point[] = [
                  [LEGS.end, legY],
                  [PROXY.x, way.at],
                ];
                // With no endpoint lane the flow ends at the far edge of the proxies.
                const leave: Point[] = ENDPOINTS
                  ? [
                      [PROXY.end, way.at],
                      [ENDPOINTS.x, consumerY],
                    ]
                  : [];
                if (!way.boxed) {
                  return [{ ...line, points: ENDPOINTS ? [...enter, ...leave] : [...enter, [PROXY.end, way.at] as Point] }];
                }
                return [{ ...line, points: enter }, ...(ENDPOINTS ? [{ ...line, points: leave }] : [])];
              }),
            ];
            return (
              <g key={`ribbon:${legKey(leg)}`}>
                <title>{tip}</title>
                {lines.map((line, index) => {
                  const traced = trace && focus !== null && line.lit && line.moves;
                  // What a pointed-at box fades stands still, so only what it lights moves.
                  const moving = traced || (flowing && line.lit);
                  const open = line.carries && count > 0;
                  // A traced line with nothing open is drawn as the live ribbon its leg's connections would fill.
                  const carried = open ? count : traced ? Math.max(1, leg.target) : 0;
                  const thickness = Math.max(2, width(carried));
                  return carried > 0 ? (
                    <g key={index}>
                      <path
                        d={band(line.points, thickness)}
                        fill={open ? line.color : stateColor("ACTIVE")}
                        opacity={line.lit ? (focus ? 0.5 : 0.28) : 0.28 * FADE}
                      />
                      {moving ? (
                        <Traffic points={line.points} width={Math.min(4, Math.max(2, thickness / 2))} still={still} />
                      ) : null}
                    </g>
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
                  );
                })}
              </g>
            );
          })}

          {egressCards.map(({ egress, y, shift, height }) => {
            const legs = flow.legs.filter((leg) => leg.egressId === egress.id);
            const down = egress.health === "DOWN";
            const open = Math.round(legs.reduce((sum, leg) => sum + (counts[legKey(leg)] ?? leg.open), 0));
            const x = EGRESS.x + 14;
            const room = EGRESS.width - 28;
            return (
              <g key={egress.id} opacity={egressLit(egress.id) ? undefined : FADE} {...point({ kind: "egress", id: egress.id })}>
                <title>{egress.reason ?? egress.name}</title>
                {card(
                  { kind: "egress", id: egress.id },
                  EGRESS_PAGE,
                  <>
                    <rect x={EGRESS.x} y={y} width={EGRESS.width} height={height} fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} strokeDasharray={down ? "4 4" : undefined} />
                    <StateBar x={EGRESS.x} y={y} height={height} state={egress.health} />
                    <text x={x} y={y + 24} fill={INK.fg} fontSize="13" fontFamily={TITLE_FONT} fontWeight="600">
                      {fit(egress.name, room - (toggleWidth ? toggleWidth + 6 : 0), 13)}
                    </text>
                    {toggle({ kind: "egress", id: egress.id }, egress.name)?.(EGRESS.end - 12 - toggleWidth, y + 10)}
                    <StateText x={x} y={y + 46} state={egress.health} label={stateLabel(t, egress.health)} />
                    {(egress.addresses?.length
                      ? egress.addresses
                      : [egress.sourceAddress || egress.interfaceName || t("next.networking.binding.system")]
                    ).map((address, line) => (
                      <text key={line} x={x} y={y + 66 + line * ADDRESS_LINE} fill={INK.muted} fontSize="10">
                        {fit(address, room, 10)}
                      </text>
                    ))}
                    {down && egress.reason ? (
                      <Note x={x} y={y + shift + 85} lines={wrap(egress.reason, room, 9.5, 2)} />
                    ) : (
                      <text x={x} y={y + shift + 86} fill={INK.muted} fontSize="10">
                        {down
                          ? t("next.networking.state.down")
                          : t("next.networking.flow.egressLoad", {
                              open,
                              rate: formatRate(legs.reduce((sum, leg) => sum + (leg.bytesPerSecond ?? 0), 0)),
                            })}
                      </text>
                    )}
                    {heldTo(egress).map((limit, row) => {
                      const baseline = y + EGRESS_HEIGHT + shift + 7 + row * EGRESS_LIMIT_ROW;
                      const tone = limit === "quota" && egress.downloadQuotaUsage?.blocked ? stateColor("HOLD") : INK.muted;
                      return (
                        <g key={limit} color={tone}>
                          <title>
                            {t(limit === "speed" ? "next.networking.egress.downloadLimit" : "next.networking.egress.quotaSection")}
                          </title>
                          <g transform={`translate(${x} ${baseline - 10})`}>
                            <Icon name={limit === "speed" ? "bandwidth" : "quota"} size={12} />
                          </g>
                          <text x={x + 18} y={baseline} fill={tone} fontSize="10">
                            {limit === "speed"
                              ? formatRate(egress.maxDownloadSpeed)
                              : egress.downloadQuotaUsage?.blocked
                                ? `${quotaSummary(egress)} · ${t("next.quota.blocked")}`
                                : quotaSummary(egress)}
                          </text>
                        </g>
                      );
                    })}
                  </>,
                )}
              </g>
            );
          })}

          {layout.rows.map(({ leg, y, cardHeight: height, note }) => {
            const key = legKey(leg);
            const open = Math.round(counts[key] ?? leg.open);
            const x = LEGS.x + 14;
            const room = LEGS.width - 28;
            return (
              <g key={key} opacity={onRoute(leg) ? undefined : FADE} {...point({ kind: "leg", key })}>
                <title>
                  {[
                    t("next.networking.flow.detailConnectionsValue", {
                      open: leg.open,
                      opening: leg.opening,
                      target: leg.target,
                      rate: formatRate(leg.bytesPerSecond),
                    }),
                    leg.sourceAddress ?? t("next.networking.flow.noSocket"),
                    ...(leg.pinnedAddress ? [`${t("next.networking.flow.detailPin")} ${leg.pinnedAddress}`] : []),
                    ...(leg.reason ? [leg.reason] : []),
                  ].join(" · ")}
                </title>
                {card(
                  { kind: "route", consumer: leg.consumer },
                  routePage(leg.consumer),
                  <>
                    <rect x={LEGS.x} y={y} width={LEGS.width} height={height} fill={INK.card} stroke={INK.line} />
                    <StateBar x={LEGS.x} y={y} height={height} state={leg.state} />
                    <text x={x} y={y + 21} fill={INK.fg} fontSize="12">
                      {compact
                        ? t("next.networking.flow.legShort", { position: leg.position + 1, weight: leg.weight })
                        : t("next.networking.flow.legTitle", {
                            name: fit(consumerName(leg), 130, 12),
                            position: leg.position + 1,
                            weight: leg.weight,
                          })}
                    </text>
                    <StateText
                      x={x}
                      y={y + 38}
                      state={leg.state}
                      label={fit(
                        `${stateLabel(t, leg.state)} · ${t("next.networking.flow.connections", { open, target: leg.target })} · ${formatRate(leg.bytesPerSecond)}`,
                        room - 13,
                        10.5,
                      )}
                    />
                    {leg.open ? (
                      <text x={x} y={y + 54} fill={INK.muted} fontSize="9.5">
                        {fit(leg.sourceAddress ?? t("next.networking.flow.noSource"), room, 9.5)}
                      </text>
                    ) : (
                      <text x={x} y={y + 54} fill={holds(leg) ? INK.warn : INK.muted} fontSize="9.5">
                        {holds(leg) && leg.state === "DOWN"
                          ? t("next.networking.flow.parkedHold")
                          : leg.state === "DOWN"
                            ? t("next.networking.flow.shareMoved")
                            : t("next.networking.flow.planned", { count: leg.target })}
                      </text>
                    )}
                    <Note x={x} y={y + LEG_HEIGHT + 4} lines={note} />
                    {toggle(
                      { kind: "leg", position: leg.position },
                      t("next.networking.route.leg", { position: leg.position + 1 }),
                    )?.(LEGS.end - 12 - toggleWidth, y + 8)}
                  </>,
                  t("next.networking.flow.editLeg", { name: consumerName(leg), position: leg.position + 1 }),
                )}
              </g>
            );
          })}

          {layout.rows.map(({ leg, y, blocks, bypass, closed }) => {
            const status = rungStatus(leg, blocks.length, true);
            return (
              <g key={`proxies:${legKey(leg)}`}>
                {blocks.map((block) => rungBlock(leg, y, block))}
                {closed ? (
                  <g
                    opacity={onWay(leg, 0) ? undefined : FADE}
                    {...point({ kind: "way", key: legKey(leg), index: 0 })}
                  >
                    {card(
                      { kind: "route", consumer: leg.consumer },
                      routePage(leg.consumer),
                      <HopBox
                        x={PROXY.x}
                        y={y}
                        width={PROXY.width}
                        title={t("next.networking.killSwitch")}
                        state="BLOCKED"
                        label={`${stateLabel(t, "BLOCKED")} · ${t("next.networking.flow.killSwitchNote")}`}
                        active={false}
                      />,
                      t("next.networking.flow.openKillSwitch", { name: consumerName(leg) }),
                    )}
                  </g>
                ) : null}
                {bypass == null ? null : (
                  <g opacity={onWay(leg, blocks.length) ? undefined : FADE}>
                    <StateText
                      x={PROXY.x + 14}
                      y={y + bypass - 18}
                      size={9.5}
                      state={status}
                      label={`${t("next.networking.route.direct")} · ${stateLabel(t, status)}`}
                    />
                  </g>
                )}
              </g>
            );
          })}

          {ENDPOINTS && endpoint ? (
            <g
              opacity={consumers[0] && !endpointLit(consumers[0].key) ? FADE : undefined}
              {...(consumers[0] ? point({ kind: "endpoint", key: consumers[0].key }) : {})}
            >
              <title>{`${endpoint.title} · ${endpoint.detail}`}</title>
              <rect x={ENDPOINTS.x} y={layout.endpointY} width={ENDPOINTS.width} height={ENDPOINT_BOX_HEIGHT} fill={INK.card} stroke={INK.line} />
              <StateBar x={ENDPOINTS.x} y={layout.endpointY} height={ENDPOINT_BOX_HEIGHT} state={endpoint.state} />
              <text x={ENDPOINTS.x + 14} y={layout.endpointY + 23} fill={INK.fg} fontSize="12.5" fontFamily={TITLE_FONT} fontWeight="600">
                {fit(endpoint.title, ENDPOINTS.width - 28, 12.5)}
              </text>
              <text x={ENDPOINTS.x + 14} y={layout.endpointY + 41} fill={INK.muted} fontSize="9.5">
                {fit(endpoint.detail, ENDPOINTS.width - 28, 9.5)}
              </text>
              <StateText
                x={ENDPOINTS.x + 14}
                y={layout.endpointY + 61}
                state={endpoint.state}
                label={fit(endpoint.label, ENDPOINTS.width - 41, 10.5)}
              />
            </g>
          ) : ENDPOINTS
            ? shown.map((consumer) => {
                const legs = flow.legs.filter((leg) => leg.consumer === consumer.key);
                const { cap, cells, y, height } = cards.get(consumer.key)!;
                const open = legs.reduce((sum, leg) => sum + (counts[legKey(leg)] ?? leg.open), 0);
                const parked =
                  consumer.route?.failover === "HOLD" ? Math.max(0, cap - legs.reduce((sum, leg) => sum + leg.target, 0)) : 0;
                const down = legs.length > 0 && legs.every((leg) => leg.state === "DOWN" || leg.state === "BLOCKED");
                // One row stretches across the card; past a full row the boxes keep their size and wrap.
                const cellX = scale(Math.max(1, Math.min(cells, CELLS_PER_ROW)), 188);
                const rss = consumer.key.startsWith("rss:");
                // Default outbound's name already says what it is.
                const outbound = consumer.key === OUTBOUND_CONSUMER;
                // Only a server's card counts its connections.
                const server = !rss && !outbound;
                const x = ENDPOINTS.x + 14;
                return (
                  <g
                    key={consumer.key}
                    opacity={(down ? 0.65 : 1) * (endpointLit(consumer.key) ? 1 : FADE)}
                    {...point({ kind: "endpoint", key: consumer.key })}
                  >
                    {card(
                      { kind: "route", consumer: consumer.key },
                      routePage(consumer.key),
                      <>
                        <rect x={ENDPOINTS.x} y={y} width={ENDPOINTS.width} height={height} fill={INK.card} stroke={down ? stateColor("DOWN") : INK.line} />
                        <StateBar x={ENDPOINTS.x} y={y} height={height} state={down ? "DOWN" : open > 0 ? "UP" : "IDLE"} />
                        <text x={x} y={y + 23} fill={INK.fg} fontSize="12.5" fontFamily={TITLE_FONT} fontWeight="600">
                          {rss ? fit(consumer.name, ENDPOINTS.width - 50, 12.5) : consumer.name.slice(0, 24)}
                        </text>
                        {rss ? (
                          <g transform={`translate(${ENDPOINTS.end - 26} ${y + 12})`} color={INK.muted}>
                            <Icon name="rss" size={12} />
                          </g>
                        ) : null}
                        {outbound ? null : (
                          <text x={x} y={y + 41} fill={INK.muted} fontSize="9.5">
                            {rss
                              ? t("next.networking.routes.rssFeed")
                              : t("next.networking.flow.serverKind", {
                                  failover:
                                    consumer.route?.failover === "HOLD"
                                      ? t("next.networking.failover.hold")
                                      : t("next.networking.failover.redistribute"),
                                })}
                          </text>
                        )}
                        {server ? (
                          <>
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
                                  x={x + cellX(cell % CELLS_PER_ROW)}
                                  y={y + 98 + Math.floor(cell / CELLS_PER_ROW) * CELL_ROW_PITCH}
                                  width={Math.max(1, cellX(1) - 2)}
                                  height={CELL_HEIGHT}
                                  fill={boundary < open ? INK.accent : "none"}
                                  stroke={reserved ? INK.warn : INK.track}
                                  strokeDasharray={reserved ? "2 2" : undefined}
                                />
                              );
                            })}
                          </>
                        ) : null}
                      </>,
                    )}
                  </g>
                );
              })
            : null}

          {layout.rows.length ? null : (
            <text x={LEGS.x} y="85" fill={INK.muted} fontSize="12">
              {grouped ? t("next.networking.flow.expandGroup") : t("next.networking.flow.noLegs")}
            </text>
          )}
          {compact ? null : (
            <text x="20" y={layout.height - 15} fill={INK.faint} fontSize="10">
              {`Weaver · ${flow.sampledAt ? new Date(flow.sampledAt * 1000).toISOString() : t("next.networking.flow.waiting")}`}
            </text>
          )}
        </svg>
      </div>
    </div>
  );
});

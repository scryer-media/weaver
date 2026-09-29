import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router";
import { interpolateNumber } from "d3-interpolate";
import { scaleLinear } from "d3-scale";
import type { Egress, LegFlow, NetworkFlow as Flow, NetworkRoute, ProxyPool, Rung } from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";

type Consumer = { key: string; name: string; cap?: number; route?: NetworkRoute | null };
type Props = { flow: Flow; egresses: Egress[]; profiles: ProxyProfile[]; pools: ProxyPool[]; consumers: Consumer[]; compact?: boolean };
const stateText = (state: string) => ({UP:"● Up",DOWN:"× Down",PROBING:"◐ Probing",BLOCKED:"■ Blocked",IDLE:"○ Idle",UNKNOWN:"? Unknown"}[state] ?? `? ${state}`);
const color = (state: string) => state === "UP" ? "#66cbb8" : state === "IDLE" ? "#8c929d" : state === "PROBING" ? "#85b7ea" : "#ef9290";
const legKey = (leg: LegFlow) => `${leg.consumer}:${leg.position}`;
const rate = (bytes = 0) => `${(bytes / 1048576).toFixed(2)} MiB/s`;
const band = (x: number, y: number, endX: number, endY: number, width: number) => {
  const mid = (x + endX) / 2, half = width / 2;
  return `M ${x} ${y-half} C ${mid} ${y-half},${mid} ${endY-half},${endX} ${endY-half} L ${endX} ${endY+half} C ${mid} ${endY+half},${mid} ${y+half},${x} ${y+half} Z`;
};

/** Animate measurements only: configuration and card positions change once per sample. */
function useMeasurements(legs: LegFlow[]) {
  const [counts, setCounts] = useState<Record<string, number>>({});
  const current = useRef<Record<string, number>>({});
  useEffect(() => {
    const values = legs.map(leg => ({ key: legKey(leg), value: interpolateNumber(current.current[legKey(leg)] ?? leg.open, leg.open) }));
    let start: number | undefined;
    let frame: number;
    const step = (now: number) => {
      start ??= now;
      const progress = Math.min(1, (now-start)/300);
      const next = Object.fromEntries(values.map(v => [v.key, v.value(progress)]));
      current.current = next;
      setCounts(next);
      if (progress < 1) frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [legs]);
  return counts;
}

export function NetworkFlow({ flow, egresses, profiles, pools, consumers, compact = false }: Props) {
  const svg = useRef<SVGSVGElement>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string[]>([]);
  const [filter, setFilter] = useState("");
  const [group, setGroup] = useState<number | null>(null);
  const counts = useMeasurements(flow.legs);
  const grouped = egresses.length > 6 || flow.legs.length > 40;
  const layout = useMemo(() => {
    const legs = flow.legs.filter(l => (!filter || l.consumer === filter) && (!grouped || group === l.egressId))
      .sort((a,b) => a.egressId-b.egressId || a.consumer.localeCompare(b.consumer) || a.position-b.position);
    let offset = 58;
    const rows = legs.map(leg => {
      const rungs: (Rung | null)[] = leg.path.kind === "DIRECT" ? [null] : [...leg.path.rungs, ...(leg.path.directFallback ? [null] : [])];
      const shown = compact ? [rungs[leg.selectedRung ?? (rungs.length-1)] ?? rungs[0] ?? null] : rungs;
      let rowY = 48;
      const details = shown.map((rung, index) => {
        const rungIndex = compact ? leg.selectedRung : index;
        const pool = rung?.kind === "POOL" ? pools.find(p => p.id === rung.poolId) : null;
        const members = pool && expanded.includes(`${legKey(leg)}:${rungIndex}`) ? pool.memberIds : [];
        const y = rowY; rowY += 36 + members.length * 32;
        return {rung, rungIndex, members, y};
      });
      const y = offset, height = rowY + 20; offset += height + 18;
      return { leg, y, height, details };
    });
    return { rows, height: Math.max(offset + 36, egresses.length*128+90, consumers.length*144+90, 260) };
  }, [flow.legs, filter, grouped, group, compact, pools, expanded, egresses.length, consumers.length]);
  const width = scaleLinear().domain([0, Math.max(1,...consumers.map(c => c.cap ?? 1))]).range([0, 28]).clamp(true);
  const name = (id: number) => profiles.find(p => p.id === id)?.name ?? `Proxy ${id}`;
  const rungName = (rung: Rung | null) => !rung ? "Direct · no tunnel" : rung.kind === "POOL" ? `${pools.find(p => p.id === rung.poolId)?.name ?? "Pool"} · pool` : rung.kind === "CHAIN" ? rung.chainIds.map(name).join(" → ") : `${name(rung.proxyId ?? 0)} · ${profiles.find(p => p.id === rung.proxyId)?.kind ?? "proxy"}`;
  const active = flow.legs.find(l => legKey(l) === selected);
  const consumerName = (leg: LegFlow) => consumers.find(c => c.key === leg.consumer)?.name ?? leg.consumer;
  function download() {
    if (!svg.current) return;
    const url = URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(svg.current)], {type:"image/svg+xml"}));
    const link = document.createElement("a"); link.href = url; link.download = "weaver-network-flow.svg"; link.click(); URL.revokeObjectURL(url);
  }
  function toggle(key: string) { setExpanded(values => values.includes(key) ? values.filter(v => v !== key) : [...values,key]); }
  return <section aria-label="Live network flow" className="network-flow" style={compact ? {maxWidth:560} : undefined}>
    {!compact && <div className="network-actions"><span>Connection flow <small>Ribbon height = open connections</small></span><label>Consumer<select value={filter} onChange={e => setFilter(e.target.value)}><option value="">All consumers</option>{consumers.map(c => <option key={c.key} value={c.key}>{c.name}</option>)}</select></label><button type="button" onClick={download}>Download SVG</button></div>}
    {grouped && <div className="network-actions" aria-label="Egress groups">{egresses.map(e => <button type="button" key={e.id} aria-expanded={group === e.id} onClick={() => setGroup(group === e.id ? null : e.id)}>{group === e.id ? "▾" : "▸"} {e.name} · {flow.legs.filter(l => l.egressId === e.id).length} legs</button>)}</div>}
    <div className="network-diagram-scroll"><svg ref={svg} xmlns="http://www.w3.org/2000/svg" viewBox={`0 0 1040 ${layout.height}`} role="group" aria-label="Egress interfaces connect through route legs to NNTP servers and RSS feeds" style={{background:"#1a1b1e",fontFamily:"Inter, sans-serif",minWidth:compact ? 0 : 820}}>
      <text x="20" y="28" fill="#9399a4" fontSize="11" letterSpacing="2">EGRESS INTERFACES</text><text x="310" y="28" fill="#9399a4" fontSize="11" letterSpacing="2">ROUTE LEGS</text><text x="810" y="28" fill="#9399a4" fontSize="11" letterSpacing="2">CONSUMERS</text>
      {layout.rows.map(({leg,y,height}) => {
        const ey = 105 + Math.max(0,egresses.findIndex(e => e.id === leg.egressId))*128;
        const cy = 112 + Math.max(0,consumers.findIndex(c => c.key === leg.consumer))*144;
        const ly = y + height/2, count = counts[legKey(leg)] ?? leg.open;
        const tip = `${leg.open}/${leg.target} connections · ${rate(leg.bytesPerSecond)} · ${leg.sourceAddress ?? "No source address"}${leg.reason ? ` · ${leg.reason}` : ""}`;
        const holding = consumers.find(c => c.key === leg.consumer)?.route?.failover === "HOLD";
        return <g key={`ribbon:${legKey(leg)}`}><title>{tip}</title>{[[230,ey,310,ly],[730,ly,810,cy]].map(([x,sy,ex,dy],i) => count > 0 ? <path key={i} d={band(x!,sy!,ex!,dy!,width(count))} fill={color(leg.state)} opacity="0.28"/> : <path key={i} d={`M ${x} ${sy} C ${(x!+ex!)/2} ${sy},${(x!+ex!)/2} ${dy},${ex} ${dy}`} fill="none" stroke={color(leg.state)} strokeWidth="1.4" strokeDasharray="4 5"/>)}{!leg.open && !compact && <text x="735" y={ly-7} fill={holding ? "#e7bd70" : "#9399a4"} fontSize="9">{holding && leg.state === "DOWN" ? "parked · HOLD" : leg.state === "DOWN" ? "share moved" : `${leg.target} planned`}</text>}</g>;
      })}
      {egresses.map((e,i) => {
        const legs = flow.legs.filter(l => l.egressId === e.id), y = 58+i*128;
        return <g key={e.id}><title>{e.reason ?? e.name}</title><a href={compact ? undefined : "/settings/networking/egress"}><rect x="16" y={y} width="214" height="108" rx="8" fill="#23252a" stroke="#353940" strokeDasharray={e.health === "DOWN" ? "4 4" : undefined}/><rect x="16" y={y+8} width="3" height="92" fill={color(e.health)}/><text x="30" y={y+23} fill="#e5e7eb" fontSize="13">{e.name.slice(0,25)}</text><text x="30" y={y+43} fill={color(e.health)} fontSize="11">{stateText(e.health)}</text><text x="30" y={y+63} fill="#a2a8b3" fontSize="10">{(e.addresses?.join(", ") || e.sourceAddress || e.interfaceName || "System routing").slice(0,30)}</text><text x="30" y={y+83} fill="#a2a8b3" fontSize="10">{e.health === "DOWN" ? "Carrying nothing" : `${Math.round(legs.reduce((n,l) => n+(counts[legKey(l)] ?? l.open),0))} open · ${rate(legs.reduce((n,l) => n+(l.bytesPerSecond ?? 0),0))}`}</text></a></g>;
      })}
      {layout.rows.map(({leg,y,height,details}) => <g key={legKey(leg)}>
        <rect x="310" y={y} width="420" height={height} rx="8" fill="#23252a" stroke={selected === legKey(leg) ? "#66cbb8" : "#353940"}/>
        <g role={compact ? undefined : "button"} tabIndex={compact ? undefined : 0} aria-label={`Inspect ${consumerName(leg)}, leg ${leg.position+1}`} onClick={() => !compact && setSelected(legKey(leg))} onKeyDown={e => {if(!compact && (e.key === "Enter" || e.key === " ")) {e.preventDefault();setSelected(legKey(leg));}}}>
          <rect x="320" y={y+6} width="400" height="35" fill="transparent"/>
          <text x="324" y={y+21} fill="#e5e7eb" fontSize="12">{consumerName(leg).slice(0,27)} · LEG {leg.position+1} · {leg.weight}%</text><text x="324" y={y+38} fill={color(leg.state)} fontSize="11">{stateText(leg.state)} · {Math.round(counts[legKey(leg)] ?? leg.open)}/{leg.target} open · {rate(leg.bytesPerSecond)}</text>
        </g>
        {details.map(({rung,rungIndex,members,y:rowY},index) => {
          const isActive = leg.open > 0 && (rung ? leg.selectedRung === rungIndex : leg.selectedRung == null);
          const pool = flow.pools.find(p => p.poolId === rung?.poolId && p.egressId === leg.egressId);
          const key = `${legKey(leg)}:${rungIndex}`;
          const rungState = rungIndex == null ? undefined : leg.rungStates?.[rungIndex];
          const status = isActive ? "● ACTIVE" : leg.state === "PROBING" ? "◐ PROBING" : rungState === "COOLDOWN" ? "◷ COOLDOWN" : leg.state === "DOWN" || rungState === "FAILING" ? "× FAILING" : "○ STANDBY";
          return <g key={index}><rect x="320" y={y+rowY-1} width="400" height="31" rx="3" fill={isActive ? "#27453f" : "#1c1e22"}/><g role={rung?.kind === "POOL" && !compact ? "button" : undefined} tabIndex={rung?.kind === "POOL" && !compact ? 0 : undefined} aria-label={rung?.kind === "POOL" ? `Toggle ${rungName(rung)} members` : undefined} onClick={() => rung?.kind === "POOL" && !compact && toggle(key)} onKeyDown={e => {if(rung?.kind === "POOL" && !compact && (e.key === "Enter" || e.key === " ")) {e.preventDefault();toggle(key);}}}><rect x="320" y={y+rowY-1} width="400" height="31" fill="transparent"/><text x="329" y={y+rowY+12} fill="#bdc3cf" fontSize="10">{rungName(rung).slice(0,53)}{rung?.kind === "POOL" ? expanded.includes(key) ? " ▾" : " ▸" : ""}</text><text x="329" y={y+rowY+25} fill={isActive ? "#66cbb8" : "#9399a4"} fontSize="9">{status}{pool?.pinnedMember != null ? ` · pin ${name(pool.pinnedMember)}` : ""}</text></g>{members.map((id,j) => {
            const member = pool?.members.find(m => m.id === id);
            const status = member?.blocked ? "■ BLOCKED" : pool?.pinnedMember === id ? "◆ PINNED" : member?.state === "SUSPECT" ? "! SUSPECT" : member?.state === "CHALLENGER" ? "◇ CHALLENGER" : member?.state === "PROBING" ? "◐ PROBING" : member?.failures ? "× FAILING" : member?.warmed ? "○ READY" : "○ UNMEASURED";
            return <g key={id}><text x="337" y={y+rowY+44+j*32} fill="#bdc3cf" fontSize="10">{name(id).slice(0,26)} · {status}</text><text x="337" y={y+rowY+57+j*32} fill="#9399a4" fontSize="9">Session {member?.handshakeMs?.toFixed(0) ?? "—"} ms · open {member?.connectMs?.toFixed(0) ?? "—"} ms · {member?.bytesPerSecond == null ? "No delivery evidence" : rate(member.bytesPerSecond)}</text></g>;
          })}</g>;
        })}
      </g>)}
      {consumers.map((consumer,i) => {
        const legs = flow.legs.filter(l => l.consumer === consumer.key), cap = consumer.cap ?? legs.reduce((n,l) => n+l.target,0);
        const open = legs.reduce((n,l) => n+(counts[legKey(l)] ?? l.open),0), parked = consumer.route?.failover === "HOLD" ? Math.max(0,cap-legs.reduce((n,l) => n+l.target,0)) : 0;
        const y = 58+i*144, down = legs.length > 0 && legs.every(l => l.state === "DOWN" || l.state === "BLOCKED");
        const cells = Math.min(cap,100), cellWidth = scaleLinear().domain([0,Math.max(1,cells)]).range([0,188]);
        return <g key={consumer.key} opacity={down ? 0.65 : 1}><a href={compact ? undefined : `/settings/networking/routes?consumer=${encodeURIComponent(consumer.key)}`}><rect x="810" y={y} width="214" height="124" rx="8" fill="#23252a" stroke="#353940"/><text x="824" y={y+22} fill="#e5e7eb" fontSize="12">{consumer.name.slice(0,26)}</text><text x="824" y={y+40} fill="#9399a4" fontSize="9">{consumer.key.startsWith("server:") ? `Server · ${consumer.route?.failover ?? "REDISTRIBUTE"}` : "RSS feed · first healthy leg"}</text><text x="824" y={y+65} fill="#e5e7eb" fontSize="20">{Math.round(open)} / {cap}</text><text x="824" y={y+83} fill="#9399a4" fontSize="10">{rate(legs.reduce((n,l) => n+(l.bytesPerSecond ?? 0),0))}{parked ? ` · ${parked} parked` : ""}</text>{Array.from({length:cells},(_,cell) => {const boundary = cell*cap/cells; return <rect key={cell} x={824+cellWidth(cell)} y={y+97} width={Math.max(1,cellWidth(1)-2)} height="10" fill={boundary < open ? "#66cbb8" : "none"} stroke={boundary >= cap-parked ? "#e7bd70" : "#454a54"} strokeDasharray={boundary >= cap-parked ? "2 2" : undefined}/>;})}</a></g>;
      })}
      {!layout.rows.length && <text x="310" y="85" fill="#a2a8b3" fontSize="12">{grouped ? "Expand an egress group to inspect its legs." : "Add a server or feed to see its route."}</text>}
      <text x="20" y={layout.height-15} fill="#9399a4" fontSize="10">Weaver · {flow.sampledAt ? new Date(flow.sampledAt*1000).toISOString() : "Waiting for live sample"}</text>
    </svg></div>
    {active && !compact && <aside className="network-inspector" aria-label="Route leg details"><div className="network-actions"><strong>{consumerName(active)} · leg {active.position+1}</strong><button type="button" onClick={() => setSelected(null)}>Close</button></div><p>{stateText(active.state)} {active.reason}</p><p>{active.open} open · {active.opening} opening · target {active.target} · {rate(active.bytesPerSecond)}</p><p>Source: {active.sourceAddress ?? "No socket open"} · destination pin: {active.pinnedAddress ?? "Not pinned"}</p><Link to={`/settings/networking/routes?consumer=${encodeURIComponent(active.consumer)}`}>Edit route in Networking</Link></aside>}
  </section>;
}

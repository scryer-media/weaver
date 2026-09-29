import { useState } from "react";
import { adjustWeight, appendLeg, removeLeg, routeTargets, routeProblem, type Egress, type Leg, type LegFlow, type NetworkRoute, type ProxyPool, type Rung } from "@/lib/networking";
import { proxyLabels, type ProxyProfile } from "@/lib/proxies";

type Props = { value: NetworkRoute; onChange: (route: NetworkRoute) => void; egresses: Egress[]; profiles: ProxyProfile[]; pools: ProxyPool[]; cap?: number; rss?: boolean; status?:LegFlow[] };
export function RouteEditor({ value, onChange, egresses, profiles, pools, cap = 1, rss = false, status=[] }: Props) {
  const [down,setDown]=useState<Set<number>>(new Set());
  const update = (i:number, leg:Leg) => onChange({ ...value, legs:value.legs.map((l,n) => n===i?leg:l) });
  const targets = routeTargets(value,cap,down);
  const weight=(i:number,next:number)=>onChange({...value,legs:adjustWeight(value.legs,i,next)});
  const problem = routeProblem(value);
  function move<T>(items:T[],i:number,delta:number):T[] { const next=[...items]; [next[i],next[i+delta]]=[next[i+delta]!,next[i]!]; return next; }
  return <div className="network-route-editor">
    <p>{rss ? "Feeds try healthy legs in order. Weights do not affect RSS requests." : `Split up to ${cap} connections across independent paths. Existing transfers finish when weights change.`}</p>
    <div className="network-weight-bar" aria-label="Leg weight distribution">{value.legs.map((leg,i)=><div key={i} style={{flex:leg.weight,background:["#407d72","#536fa1","#98714a","#766398"][i%4]}}><span>{leg.weight}%</span>{i<value.legs.length-1&&<div role="slider" tabIndex={0} aria-label={`Leg ${i+1} weight`} aria-valuemin={1} aria-valuemax={leg.weight+value.legs[i+1]!.weight-1} aria-valuenow={leg.weight} className="network-weight-handle" onPointerDown={e=>e.currentTarget.setPointerCapture(e.pointerId)} onPointerMove={e=>{if(!e.currentTarget.hasPointerCapture(e.pointerId))return;const bounds=e.currentTarget.parentElement!.parentElement!.getBoundingClientRect();const prior=value.legs.slice(0,i).reduce((sum,l)=>sum+l.weight,0);weight(i,(e.clientX-bounds.left)/bounds.width*100-prior);}} onPointerUp={e=>e.currentTarget.releasePointerCapture(e.pointerId)} onKeyDown={e=>{if(e.key==="ArrowLeft"||e.key==="ArrowRight"){e.preventDefault();weight(i,leg.weight+(e.key==="ArrowLeft"?-1:1));}}}/>}</div>)}</div>
    {down.size>0&&<p role="status">Preview only: {targets.reduce((sum,n)=>sum+n,0)} assigned, {cap-targets.reduce((sum,n)=>sum+n,0)} parked. The running route is unchanged.</p>}
    {value.legs.map((leg,i) => {
      const path=leg.path;
      const setRungs=(rungs:Rung[]) => update(i,{...leg,path:{...path,rungs}});
      return <fieldset className="network-card" key={i}>
        <legend>Leg {i+1}</legend>
        <div className="network-fields">
          <label>Egress<select value={leg.egressId} onChange={e=>update(i,{...leg,egressId:Number(e.target.value)})}>{egresses.map(e=><option key={e.id} value={e.id}>{e.name}{e.enabled?"":" · disabled"}</option>)}</select></label>
          <label>Weight (%)<input type="number" min={1} max={100} disabled={value.legs.length===1} value={leg.weight} onChange={e=>weight(i,Number(e.target.value))}/></label>
          <label>Path<select value={path.kind} onChange={e=>update(i,{...leg,path:{kind:e.target.value as "DIRECT"|"LADDER",rungs:[],directFallback:false}})}><option value="DIRECT">Direct</option><option value="LADDER">Proxy ladder</option></select></label>
        </div>
        {!rss && <small>Target: {targets[i]} connections</small>}
        <label className="network-check"><input type="checkbox" checked={down.has(i)} onChange={e=>setDown(current=>{const next=new Set(current);if(e.target.checked)next.add(i);else next.delete(i);return next;})}/>Simulate leg down</label>
        {status.find(s=>s.position===i)&&<p className="network-live-leg">Live: {status.find(s=>s.position===i)!.state} · {status.find(s=>s.position===i)!.open}/{status.find(s=>s.position===i)!.target} connections · {status.find(s=>s.position===i)!.sourceAddress??"No source address yet"}</p>}
        {path.kind==="LADDER" && <>
          <ol className="network-rungs">{path.rungs.map((rung,j)=><li key={j}>
            <span>Rung {j+1}</span>
            <select aria-label={`Leg ${i+1} rung ${j+1} type`} value={rung.kind} onChange={e=>setRungs(path.rungs.map((r,n)=>n===j?{kind:e.target.value as Rung["kind"],proxyId:null,poolId:null,chainIds:[]}:r))}><option value="PROXY">Proxy</option><option value="POOL">Pool</option><option value="CHAIN">Chain</option></select>
            {rung.kind==="CHAIN" ? <div className="network-chain">{[0,1,2].map(k=><select key={k} aria-label={`Chain hop ${k+1}`} value={rung.chainIds[k]??""} onChange={e=>{const ids=[...rung.chainIds];if(e.target.value)ids[k]=Number(e.target.value);else ids.splice(k,1);setRungs(path.rungs.map((r,n)=>n===j?{...r,chainIds:ids}:r));}}><option value="">{k===2?"Optional third hop":"Choose proxy"}</option>{profiles.filter(p=>k===0||!["WIRE_GUARD","HTTP3_CONNECT"].includes(p.kind)).map(p=><option key={p.id} value={p.id}>{p.name} · {proxyLabels[p.kind]}</option>)}</select>)}</div>
              : <select aria-label={`Leg ${i+1} rung ${j+1}`} value={(rung.kind==="PROXY"?rung.proxyId:rung.poolId)??""} onChange={e=>setRungs(path.rungs.map((r,n)=>n===j?{...r,[r.kind==="PROXY"?"proxyId":"poolId"]:Number(e.target.value)}:r))}><option value="">Choose {rung.kind.toLowerCase()}</option>{(rung.kind==="PROXY"?profiles:pools).map(p=><option key={p.id} value={p.id}>{p.name}{p.enabled?"":" · disabled"}</option>)}</select>}
            <button type="button" aria-label="Move rung up" disabled={j===0} onClick={()=>setRungs(move(path.rungs,j,-1))}>↑</button><button type="button" aria-label="Move rung down" disabled={j===path.rungs.length-1} onClick={()=>setRungs(move(path.rungs,j,1))}>↓</button><button type="button" onClick={()=>setRungs(path.rungs.filter((_,n)=>n!==j))}>Remove rung</button>
          </li>)}</ol>
          <button type="button" disabled={path.rungs.length>=8} onClick={()=>setRungs([...path.rungs,{kind:"PROXY",proxyId:null,poolId:null,chainIds:[]}])}>Add rung</button>
          <label className="network-check"><input type="checkbox" checked={path.directFallback} onChange={e=>update(i,{...leg,path:{...path,directFallback:e.target.checked}})}/>Allow direct fallback on this egress</label>
        </>}
        <div className="network-actions"><button type="button" disabled={i===0} onClick={()=>{setDown(new Set());onChange({...value,legs:move(value.legs,i,-1)});}}>Move up</button><button type="button" disabled={i===value.legs.length-1} onClick={()=>{setDown(new Set());onChange({...value,legs:move(value.legs,i,1)});}}>Move down</button><button type="button" disabled={value.legs.length===1} onClick={()=>{setDown(new Set());onChange({...value,legs:removeLeg(value.legs,i)});}}>Remove leg</button></div>
      </fieldset>;
    })}
    <div className="network-actions"><button type="button" disabled={value.legs.length>=8} onClick={()=>onChange({...value,legs:appendLeg(value.legs)})}>Add leg</button><fieldset><legend>When a leg goes down</legend>{(["REDISTRIBUTE","HOLD"] as const).map(mode=><label key={mode} className="network-check"><input type="radio" checked={value.failover===mode} onChange={()=>onChange({...value,failover:mode})}/>{mode==="HOLD"?"Hold its share unused":"Redistribute connections"}</label>)}</fieldset></div>
    {problem && <p role="alert">{problem}</p>}
  </div>;
}

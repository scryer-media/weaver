import { useState, type ReactNode } from "react";
import { Link, Navigate, useLocation } from "react-router";
import { useClient, useQuery, useSubscription } from "urql";
import type { DocumentNode } from "graphql";
import { CREATE_EGRESS, CREATE_POOL, DELETE_EGRESS, DELETE_POOL, NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY, SAVE_ROUTE, TEST_EGRESS, TEST_POOL, UPDATE_EGRESS, UPDATE_POOL } from "@/graphql/networking";
import { allocation, directLeg, routeInput, routeProblem, type Egress, type NetworkFlow as Flow, type NetworkRoute, type ProxyPool } from "@/lib/networking";
import { proxyLabels, type ProxyKind, type ProxyProfile } from "@/lib/proxies";
import { RouteEditor } from "./RouteEditor";
import { NetworkFlow } from "./NetworkFlow";
import "./networking.css";

export type NetworkingData = {
  egressInterfaces:Egress[]; proxyProfiles:ProxyProfile[]; proxyPools:ProxyPool[];
  discoverNetworkInterfaces:{name:string;index:number|null;up:boolean;addresses:string[]}[];
  platformNetworking:{platform:string;egressBindingKinds:Egress["bindingKind"][];sourceAddressHint:string;container:boolean;bridgeNetworkSuspected:boolean;maxWireguardInstances:number;notes:string[]};
  servers:{id:number;host:string;connections:number;active:boolean;routing:NetworkRoute|null}[];
  rssFeeds:{id:number;name:string;enabled:boolean;routing:NetworkRoute|null}[];
};
const pages=["overview","egress","proxies","routes","bandwidth"] as const;
type Action=(document:DocumentNode,variables:Record<string,unknown>)=>Promise<boolean>;
const EMPTY_FLOW:Flow={legs:[],pools:[]};
export function NetworkingWorkspace({ proxies, bandwidth }: {proxies:ReactNode;bandwidth:ReactNode}) {
  const location=useLocation();const page=location.pathname.split("/networking/")[1]??"overview";
  const [{data,error,fetching},refresh]=useQuery<NetworkingData>({query:NETWORKING_QUERY,requestPolicy:"cache-and-network"});
  const [initial]=useQuery<{networkFlow:Flow}>({query:NETWORK_FLOW_QUERY,requestPolicy:"cache-and-network",pause:!["overview","routes","egress","bandwidth"].includes(page)});
  const [live]=useSubscription<{networkFlow:Flow}>({query:NETWORK_FLOW_SUBSCRIPTION,pause:!["overview","routes","egress","bandwidth"].includes(page)});
  const client=useClient();const [busy,setBusy]=useState(false);const [message,setMessage]=useState<string|null>(null);const [failed,setFailed]=useState(false);
  async function action(document:DocumentNode,variables:Record<string,unknown>):Promise<boolean> {
    setBusy(true);setMessage(null);
    try {const result=await client.mutation(document,variables).toPromise();setFailed(!!result.error);setMessage(result.error?.message??"Saved");if(!result.error){refresh({requestPolicy:"network-only"});await client.query(NETWORK_FLOW_QUERY,{}, {requestPolicy:"network-only"}).toPromise();}return !result.error;}
    catch(error){setFailed(true);setMessage(error instanceof Error?error.message:"Request failed");return false;}
    finally{setBusy(false);}
  }
  if(!pages.includes(page as typeof pages[number]))return <Navigate to="/settings/networking/overview" replace/>;
  const measured=live.data?.networkFlow??initial.data?.networkFlow??EMPTY_FLOW;
  const consumers=measured.consumers??(data?[...data.servers.map(s=>({key:`server:${s.id}`,name:s.host,route:s.routing,cap:s.connections,kind:"SERVER" as const,id:s.id})),...data.rssFeeds.map(f=>({key:`rss:${f.id}`,name:f.name,route:f.routing,cap:1,kind:"RSS" as const,id:f.id}))]:[]);
  const flow:Flow={...measured,legs:consumers.flatMap(c=>{const route=c.route??{legs:[directLeg()],failover:"REDISTRIBUTE"};const targets=allocation(route.legs.map(l=>l.weight),c.cap);return route.legs.map((leg,position)=>measured.legs.find(l=>l.consumer===c.key&&l.position===position)??{...leg,consumer:c.key,position,target:targets[position]??0,open:0,opening:0,state:"IDLE",reason:null,pinnedAddress:null});})};
  return <div className="networking-workspace">
    <header className="network-header"><div><h1>Networking</h1><p>Choose where connections leave and how they reach your servers and feeds.</p></div><span className="network-beta">Beta</span></header>
    <nav className="network-tabs" aria-label="Networking pages">{pages.map(p=><Link key={p} to={`/settings/networking/${p}`} aria-current={page===p?"page":undefined}>{p==="egress"?"Egress interfaces":p[0]!.toUpperCase()+p.slice(1)}</Link>)}</nav>
    <div className="network-content">
      {message&&<p role={failed?"alert":"status"}>{message}</p>}
      {error&&<p role="alert">Unable to load networking: {error.message} <button type="button" onClick={()=>refresh({requestPolicy:"network-only"})}>Retry</button></p>}
      {!data&&fetching&&<p role="status">Loading networking…</p>}
      {data&&<>
        {data.platformNetworking.bridgeNetworkSuspected&&<p role="status">Container bridge networking exposes container interfaces. Use host networking or attach multiple container networks to select independent links.</p>}
        {page==="overview"&&<><div className="network-summary"><div><strong>{data.egressInterfaces.length}</strong><span>Egress interfaces</span></div><div><strong>{data.proxyProfiles.length}</strong><span>Proxy profiles</span></div><div><strong>{flow.legs.length}</strong><span>Route legs</span></div><div><strong>{flow.legs.reduce((s,l)=>s+l.open,0)}</strong><span>Open connections</span></div></div>{live.error&&<p role="status">Live updates unavailable. Showing the last received state.</p>}<NetworkFlow flow={flow} egresses={measured.egresses??data.egressInterfaces} profiles={measured.proxies??data.proxyProfiles} pools={measured.proxyPools??data.proxyPools} consumers={consumers}/></>}
        {page==="egress"&&<EgressPage data={{...data,egressInterfaces:measured.egresses??data.egressInterfaces}} action={action} busy={busy}/>}
        {page==="proxies"&&<><PoolPage data={data} action={action} busy={busy}/><div className="network-existing-panel">{proxies}</div></>}
        {page==="routes"&&<RoutesPage key={location.search} data={data} consumers={consumers} action={action} busy={busy} flow={flow}/>}
        {page==="bandwidth"&&<><p>Global and server limits still apply. Each egress can also limit the traffic that leaves through it.</p><EgressLimits data={{...data,egressInterfaces:measured.egresses??data.egressInterfaces}} action={action} busy={busy}/>{bandwidth}</>}
      </>}
    </div>
  </div>;
}
const egressInput=(e:Egress)=>({name:e.name,bindingKind:e.bindingKind,interfaceName:e.bindingKind==="INTERFACE"?e.interfaceName:null,sourceAddress:e.bindingKind==="SOURCE_ADDRESS"?e.sourceAddress:null,enabled:e.enabled,maxDownloadSpeed:e.maxDownloadSpeed});
const NEW_EGRESS:Egress={id:-1,name:"",bindingKind:"INTERFACE",interfaceName:"",sourceAddress:"",enabled:true,maxDownloadSpeed:0,health:"UNKNOWN",reason:null};
function EgressPage({data,action,busy}:{data:NetworkingData;action:Action;busy:boolean}) {
  const [editing,setEditing]=useState<Egress|null>(null);const [deleting,setDeleting]=useState<number|null>(null);
  const [test,setTest]=useState<Egress|null>(null);const [host,setHost]=useState("");const [port,setPort]=useState(443);const [testResult,setTestResult]=useState("");const [testing,setTesting]=useState(false);const client=useClient();
  return <section><div className="network-actions"><h2>Egress interfaces</h2><button type="button" onClick={()=>setEditing({...NEW_EGRESS,bindingKind:data.platformNetworking.egressBindingKinds.includes("INTERFACE")?"INTERFACE":"SOURCE_ADDRESS"})}>Add egress</button></div><p>{data.platformNetworking.sourceAddressHint}</p>
    <div className="network-table-wrap"><table><thead><tr><th>Name</th><th>Binding / live addresses</th><th>Health</th><th>Limit</th><th>Actions</th></tr></thead><tbody>{data.egressInterfaces.map(e=><tr key={e.id}><td>{e.name}</td><td>{e.interfaceName??e.sourceAddress??"System routing"}<small>{(e.addresses??data.discoverNetworkInterfaces.filter(i=>e.bindingKind==="SYSTEM"||i.name===e.interfaceName||i.addresses.includes(e.sourceAddress??" ")).flatMap(i=>i.addresses)).join(", ")}</small></td><td title={e.reason??undefined}>{e.health==="UP"?"●":e.health==="DOWN"?"×":"?"} {e.health.toLowerCase()}{!e.enabled?" · disabled":""}</td><td>{e.maxDownloadSpeed?`${(e.maxDownloadSpeed/1024/1024).toFixed(1)} MiB/s`:"Unlimited"}</td><td><button type="button" onClick={()=>setEditing(e)}>Edit</button><button type="button" onClick={()=>{setTest(e);setTestResult("");}}>Test</button>{e.id!==0&&<button type="button" onClick={()=>setDeleting(e.id)}>Delete</button>}</td></tr>)}</tbody></table></div>
    {deleting!==null&&<div className="network-card" role="alert"><p>Delete this egress? Routes that reference it must be updated first.</p><button disabled={busy} type="button" onClick={async()=>{if(await action(DELETE_EGRESS,{id:deleting}))setDeleting(null);}}>Delete egress</button><button type="button" onClick={()=>setDeleting(null)}>Cancel</button></div>}
    {editing&&<form className="network-card" onSubmit={async e=>{e.preventDefault();if(await action(editing.id<0?CREATE_EGRESS:UPDATE_EGRESS,{id:editing.id,input:egressInput(editing)}))setEditing(null);}}><h3>{editing.id<0?"Add egress":`Edit ${editing.name}`}</h3><div className="network-fields"><label>Name<input required disabled={editing.id===0} value={editing.name} onChange={e=>setEditing({...editing,name:e.target.value})}/></label><label>Binding<select disabled={editing.id===0} value={editing.bindingKind} onChange={e=>setEditing({...editing,bindingKind:e.target.value as Egress["bindingKind"]})}>{data.platformNetworking.egressBindingKinds.filter(k=>k!=="SYSTEM"||editing.id===0).map(k=><option key={k} value={k}>{k.replace("_"," ").toLowerCase()}</option>)}</select></label>
      {editing.bindingKind==="INTERFACE"&&<label>Interface<select required value={editing.interfaceName??""} onChange={e=>setEditing({...editing,interfaceName:e.target.value})}><option value="">Select an interface</option>{editing.interfaceName&&!data.discoverNetworkInterfaces.some(i=>i.name===editing.interfaceName)&&<option value={editing.interfaceName}>{editing.interfaceName} · missing</option>}{data.discoverNetworkInterfaces.map(i=><option key={i.name} value={i.name}>{i.name} · {i.up?"up":"down"} · {i.addresses.join(", ")}</option>)}</select></label>}
      {editing.bindingKind==="SOURCE_ADDRESS"&&<label>Source address<input required list="network-source-addresses" value={editing.sourceAddress??""} onChange={e=>setEditing({...editing,sourceAddress:e.target.value})}/><datalist id="network-source-addresses">{data.discoverNetworkInterfaces.flatMap(i=>i.addresses.map(a=><option key={`${i.name}:${a}`} value={a}>{i.name}</option>))}</datalist></label>}
      <label>Limit (MiB/s, 0 = unlimited)<input type="number" min={0} step="0.1" value={editing.maxDownloadSpeed/1024/1024} onChange={e=>setEditing({...editing,maxDownloadSpeed:Math.round(Number(e.target.value)*1024*1024)})}/></label></div><label className="network-check"><input type="checkbox" disabled={editing.id===0} checked={editing.enabled} onChange={e=>setEditing({...editing,enabled:e.target.checked})}/>Enabled</label><div className="network-actions"><button disabled={busy} type="submit">Save egress</button><button type="button" onClick={()=>setEditing(null)}>Cancel</button></div></form>}
    {test&&<form className="network-card" onSubmit={async e=>{e.preventDefault();setTesting(true);try{const r=await client.mutation(TEST_EGRESS,{id:test.id,host,port}).toPromise();const result=r.data?.testEgressInterface;setTestResult(r.error?.message??(result?`${result.message}${result.sourceAddress?` · source ${result.sourceAddress}`:""}${result.connectMillis!=null?` · ${result.connectMillis} ms`:""}`:"Test failed"));}finally{setTesting(false);}}}><h3>Test {test.name}</h3><p>Connect to a destination through this egress and report the actual source address.</p><div className="network-fields"><label>Destination host<input required value={host} onChange={e=>setHost(e.target.value)}/></label><label>Port<input required type="number" min={1} max={65535} value={port} onChange={e=>setPort(Number(e.target.value))}/></label></div><button disabled={testing} type="submit">{testing?"Connecting…":"Connect"}</button><button type="button" onClick={()=>setTest(null)}>Close</button>{testResult&&<p role="status">{testResult}</p>}</form>}
  </section>;
}
function PoolPage({data,action,busy}:{data:NetworkingData;action:Action;busy:boolean}) {
  const [pool,setPool]=useState<ProxyPool|null>(null);const [deleting,setDeleting]=useState<number|null>(null);
  const [testing,setTesting]=useState<ProxyPool|null>(null);

  return <section><div className="network-actions"><h2>Proxy pools</h2><button type="button" onClick={()=>setPool({id:-1,name:"",kind:"SOCKS5",memberIds:[],enabled:true})}>Add pool</button></div><p>Pools race proxies of the same type, then prefer members backed by delivery evidence. WireGuard allows up to {data.platformNetworking.maxWireguardInstances} instances across egress paths; each reserves a budget of 516 MiB.</p>{!data.proxyPools.length&&<p>No pools yet. A single proxy can also be used directly in a route.</p>}
    <div className="network-grid">{data.proxyPools.map(p=><article key={p.id} className="network-card"><h3>{p.name}</h3><p>{proxyLabels[p.kind]} · {p.memberIds.length} members · {p.enabled?"Enabled":"Disabled"}</p><p>{p.memberIds.map(id=>data.proxyProfiles.find(p=>p.id===id)?.name??`Proxy ${id}`).join(", ")}</p><button type="button" onClick={()=>setPool(p)}>Edit pool</button><button type="button" onClick={()=>setTesting(p)}>Test pool</button><button type="button" onClick={()=>setDeleting(p.id)}>Delete</button></article>)}</div>
    {deleting!==null&&<div className="network-card" role="alert"><p>Delete this pool? It must be removed from all routes first.</p><button disabled={busy} type="button" onClick={async()=>{if(await action(DELETE_POOL,{id:deleting}))setDeleting(null);}}>Delete pool</button><button type="button" onClick={()=>setDeleting(null)}>Cancel</button></div>}
    {testing&&<PoolTest key={testing.id} pool={testing} data={data} close={()=>setTesting(null)}/>}
    {pool&&<form className="network-card" onSubmit={async e=>{e.preventDefault();const {id,...input}=pool;if(await action(id<0?CREATE_POOL:UPDATE_POOL,{id,input}))setPool(null);}}><h3>{pool.id<0?"Add pool":"Edit pool"}</h3><div className="network-fields"><label>Name<input required value={pool.name} onChange={e=>setPool({...pool,name:e.target.value})}/></label><label>Proxy type<select value={pool.kind} onChange={e=>setPool({...pool,kind:e.target.value as ProxyKind,memberIds:[]})}>{Object.entries(proxyLabels).map(([kind,name])=><option key={kind} value={kind}>{name}</option>)}</select></label></div><fieldset><legend>Members</legend>{data.proxyProfiles.filter(p=>p.kind===pool.kind).map(p=><label className="network-check" key={p.id}><input type="checkbox" checked={pool.memberIds.includes(p.id)} onChange={e=>setPool({...pool,memberIds:e.target.checked?[...pool.memberIds,p.id]:pool.memberIds.filter(id=>id!==p.id)})}/>{p.name}{!p.enabled?" · disabled":""}</label>)}</fieldset><label className="network-check"><input type="checkbox" checked={pool.enabled} onChange={e=>setPool({...pool,enabled:e.target.checked})}/>Enabled</label><button disabled={busy||pool.memberIds.length<2} type="submit">Save pool</button><button type="button" onClick={()=>setPool(null)}>Cancel</button></form>}
  </section>;
}
type ProbeResult={proxyId:number;success:boolean;message:string;sourceAddress:string|null;connectMillis:number|null};
function PoolTest({pool,data,close}:{pool:ProxyPool;data:NetworkingData;close:()=>void}) {
  const client=useClient();const [egress,setEgress]=useState(0);const [host,setHost]=useState("");const [port,setPort]=useState(443);const [busy,setBusy]=useState(false);const [results,setResults]=useState<ProbeResult[]>([]);const [error,setError]=useState("");
  const destinationRequired=pool.kind==="SOCKS5"||pool.kind==="HTTP_CONNECT";
  return <form className="network-card" onSubmit={async e=>{e.preventDefault();setBusy(true);setError("");try{const result=await client.mutation(TEST_POOL,{id:pool.id,egressId:egress,host:host||null,port:host?port:null}).toPromise();setError(result.error?.message??"");setResults(result.data?.testProxyPool??[]);}catch(error){setError(String(error));}finally{setBusy(false);}}}><h3>Test {pool.name}</h3><p>Each member uses an isolated session. Live selection and connections are preserved.</p><div className="network-fields"><label>Egress<select value={egress} onChange={e=>setEgress(Number(e.target.value))}>{data.egressInterfaces.filter(e=>e.enabled).map(e=><option key={e.id} value={e.id}>{e.name}</option>)}</select></label><label>Destination host{!destinationRequired&&" (optional)"}<input required={destinationRequired} value={host} onChange={e=>setHost(e.target.value)}/></label><label>Port<input type="number" min={1} max={65535} value={port} onChange={e=>setPort(Number(e.target.value))}/></label></div><button disabled={busy} type="submit">{busy?"Testing…":"Test members"}</button><button type="button" onClick={close}>Close</button>{error&&<p role="alert">{error}</p>}<div role="status">{results.map(result=><p key={result.proxyId}>{result.success?"●":"×"} {data.proxyProfiles.find(p=>p.id===result.proxyId)?.name}: {result.message}{result.sourceAddress&&` · source ${result.sourceAddress}`}{result.connectMillis!=null&&` · ${result.connectMillis} ms`}</p>)}</div></form>;
}
type ConsumerEditor={key:string;name:string;kind:"SERVER"|"RSS";id:number;cap:number;route:NetworkRoute|null};
function RoutesPage({data,consumers,action,busy,flow}:{data:NetworkingData;consumers:ConsumerEditor[];action:Action;busy:boolean;flow:Flow}) {
  const location=useLocation();const requested=new URLSearchParams(location.search).get("consumer");
  const selected=consumers.find(c=>c.key===requested)??null;
  const [editing,setEditing]=useState<ConsumerEditor|null>(selected);const [route,setRoute]=useState<NetworkRoute>(selected?.route??{legs:[directLeg()],failover:"REDISTRIBUTE"});
  return <section><h2>Consumer routes</h2><p>Each server or feed owns its route. Egress interfaces and proxy pools are shared.</p><div className="network-grid">{consumers.map(c=><article className="network-card" key={c.key}><h3>{c.name}</h3><p>{c.kind==="SERVER"?`${c.cap} NNTP connections`:"RSS feed"} · {c.route?.legs.length??1} legs</p><button type="button" onClick={()=>{setEditing(c);setRoute(c.route??{legs:[directLeg()],failover:"REDISTRIBUTE"});}}>Edit route</button></article>)}</div>{!consumers.length&&<p>Add a server or RSS feed before assigning a route.</p>}{editing&&<form className="network-card" onSubmit={async e=>{e.preventDefault();if(await action(SAVE_ROUTE,{kind:editing.kind,id:editing.id,input:routeInput(route)}))setEditing(null);}}><h3>Route for {editing.name}</h3><RouteEditor value={route} onChange={setRoute} egresses={data.egressInterfaces} profiles={data.proxyProfiles} pools={data.proxyPools} cap={editing.cap} rss={editing.kind==="RSS"} status={flow.legs.filter(leg=>leg.consumer===editing.key)}/><button type="submit" disabled={busy||!!routeProblem(route)}>Save route</button><button type="button" onClick={()=>setEditing(null)}>Cancel</button></form>}</section>;
}
function EgressLimits({data,action,busy}:{data:NetworkingData;action:Action;busy:boolean}) {
  return <div className="network-grid">{data.egressInterfaces.map(e=><EgressLimit key={`${e.id}:${e.maxDownloadSpeed}`} egress={e} action={action} busy={busy}/>)}</div>;
}
function EgressLimit({egress,action,busy}:{egress:Egress;action:Action;busy:boolean}) {
  const [limit,setLimit]=useState(egress.maxDownloadSpeed/1024/1024);
  return <form className="network-card" onSubmit={async e=>{e.preventDefault();await action(UPDATE_EGRESS,{id:egress.id,input:egressInput({...egress,maxDownloadSpeed:Math.round(limit*1024*1024)})});}}><h3>{egress.name}</h3><label>Limit (MiB/s, 0 = unlimited)<input type="number" min={0} step="0.1" value={limit} onChange={e=>setLimit(Number(e.target.value))}/></label><button disabled={busy||limit===egress.maxDownloadSpeed/1024/1024} type="submit">Save limit</button></form>;
}

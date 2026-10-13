import { useState } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import { translateDictionary } from "@/lib/i18n";
import en from "@/lib/i18n/locales/en";
import { plannedFlow, type Consumer, type NetworkingAction, type NetworkingData } from "@/next/features/networking/data";
import { FlowEditor } from "@/next/features/networking/FlowEditor";
import { NetworkFlow, type FlowTarget } from "@/next/features/networking/NetworkFlow";
import { RouteEditor } from "@/next/features/networking/RouteEditor";
import { RouteView } from "@/next/features/networking/RouteView";
import { ProxyEditorFor } from "@/next/pages/settings/panels/ProxiesPanel";
import { directLeg, type Egress, type NetworkFlow as Flow, type NetworkRoute } from "@/lib/networking";
import type { ProxyKind, ProxyProfile } from "@/lib/proxies";
import "@/next/fonts.css";
import "@/next/theme.css";
const egresses:Egress[]=[{id:0,name:"System",bindingKind:"SYSTEM",interfaceName:null,sourceAddress:null,enabled:true,maxDownloadSpeed:0,health:"UP",reason:null},{id:1,name:"WAN Fiber",bindingKind:"SOURCE_ADDRESS",interfaceName:null,sourceAddress:"192.0.2.10",enabled:true,maxDownloadSpeed:0,health:"UP",reason:null},{id:2,name:"LTE standby",bindingKind:"SOURCE_ADDRESS",interfaceName:null,sourceAddress:"198.51.100.10",enabled:true,maxDownloadSpeed:0,health:"DOWN",reason:"Interface is down"},{id:3,name:"Spare uplink",bindingKind:"SOURCE_ADDRESS",interfaceName:null,sourceAddress:"203.0.113.10",enabled:true,maxDownloadSpeed:0,health:"UP",reason:null}];
const profile=(id:number,name:string,kind:ProxyKind,enabled=true):ProxyProfile=>({id,name,kind,enabled,host:`${name.toLowerCase().replaceAll(" ","-")}.fixture.invalid`,port:kind==="WIRE_GUARD"?51820:1080,dnsServers:[],tunnelAddresses:kind==="WIRE_GUARD"?[`10.0.0.${id}/32`]:[],peerPublicKey:null,tunnelPublicKey:null,mtu:1420,keepaliveSeconds:null,timeoutSeconds:10,hostKeyFingerprint:null,hasUsername:false,hasPassword:false,hasPrivateKey:kind==="WIRE_GUARD",hasPassphrase:false,hasPresharedKey:false});
const profiles=[profile(1,"Amsterdam","WIRE_GUARD"),profile(2,"Frankfurt","WIRE_GUARD"),profile(3,"Lisbon","WIRE_GUARD",false),profile(4,"Relay one","SOCKS5"),profile(5,"Relay two","SOCKS5"),profile(6,"Exit","HTTP_CONNECT")];
const pools=[{id:1,name:"Europe",kind:"WIRE_GUARD" as const,memberIds:[1,2,3],enabled:true}];
const initial:NetworkRoute={failover:"HOLD",legs:[{egressId:1,weight:60,path:{kind:"LADDER",directFallback:false,rungs:[{kind:"POOL",poolId:1,proxyId:null,chainIds:[]}]}},{...directLeg(),weight:30},{...directLeg(),egressId:2,weight:10}]};
// A backup server that leaves through one proxy.
const backup:NetworkRoute={failover:"REDISTRIBUTE",legs:[{egressId:0,weight:100,path:{kind:"LADDER",directFallback:false,rungs:[{kind:"PROXY",proxyId:1,poolId:null,chainIds:[]}]}}]};
const backupLeg={...backup.legs[0]!,consumer:"server:2",position:0,target:20,open:8,opening:0,state:"UP",reason:null,pinnedAddress:null,sourceAddress:"127.0.0.1:45125",bytesPerSecond:6_000_000,selectedRung:0,selectedProxyId:1,rungStates:["STANDBY"]};
// A server made behind a kill switch and not given a route since: switched off, so the daemon has only its egress to report.
const held:NetworkRoute={failover:"REDISTRIBUTE",legs:[{egressId:0,weight:100,path:{kind:"LADDER",directFallback:false,rungs:[]}}]};
const heldLeg={...held.legs[0]!,consumer:"server:3",position:0,target:0,open:0,opening:0,state:"IDLE",reason:null,pinnedAddress:null,sourceAddress:null,bytesPerSecond:0,selectedRung:null,selectedProxyId:null,rungStates:[],failingHops:[]};
// A feed whose chain fails at its second hop and whose next proxy fails too, so it has fallen back to going direct.
const feed:NetworkRoute={failover:"REDISTRIBUTE",legs:[{egressId:0,weight:100,path:{kind:"LADDER",directFallback:true,rungs:[{kind:"CHAIN",proxyId:null,poolId:null,chainIds:[4,5,6]},{kind:"PROXY",proxyId:2,poolId:null,chainIds:[]}]}}]};
const feedLeg={...feed.legs[0]!,consumer:"rss:1",position:0,target:1,open:1,opening:0,state:"UP",reason:null,pinnedAddress:null,sourceAddress:"127.0.0.1:45130",bytesPerSecond:0,selectedRung:null,selectedProxyId:null,rungStates:["COOLDOWN","FAILING","STANDBY"],failingHops:[{rung:0,proxyId:5,reason:"endpoint unreachable: connection refused"},{rung:1,proxyId:2,reason:"handshake did not complete"}]};
const consumersFor=(route:NetworkRoute):Consumer[]=>[{key:"server:1",id:1,name:"News primary",kind:"SERVER",cap:50,route},{key:"server:2",id:2,name:"News backup",kind:"SERVER",cap:20,route:backup},{key:"server:3",id:3,name:"News archive",kind:"SERVER",cap:10,route:held},{key:"rss:1",id:1,name:"Feed mirror",kind:"RSS",cap:1,route:feed}];
const flowFor=(route:NetworkRoute,down:boolean):Flow=>({sampledAt:1790640000,legs:[...route.legs.map((leg,position)=>({...leg,consumer:"server:1",position,target:position===2||down?0:[30,15][position]??0,open:position===2||down?0:[24,12][position]??0,opening:0,state:position===2||down?"DOWN":"UP",reason:position===2?"Interface is down":null,pinnedAddress:null,sourceAddress:position===0?"192.0.2.10:45123":"127.0.0.1:45124",bytesPerSecond:position===0?24_000_000:0,selectedRung:position===0?0:null,selectedProxyId:position===0?1:null})),backupLeg,heldLeg,feedLeg],pools:[{poolId:1,egressId:1,pinnedMember:1,members:[{id:1,open:24,opening:0,warmed:true,blocked:null,handshakeMs:30,connectMs:20,bytesPerSecond:1_000_000,samples:40,failures:0},{id:2,open:0,opening:0,warmed:true,blocked:null,handshakeMs:40,connectMs:25,bytesPerSecond:800_000,samples:20,failures:0},{id:3,state:"DISABLED",open:0,opening:0,warmed:false,blocked:null,handshakeMs:null,connectMs:null,bytesPerSecond:null,samples:0,failures:0}]}]});
const data:NetworkingData={egressInterfaces:egresses,proxyProfiles:profiles,proxyPools:pools,discoverNetworkInterfaces:[],platformNetworking:{platform:"fixture",egressBindingKinds:["SYSTEM","INTERFACE","SOURCE_ADDRESS"],sourceAddressHint:null,container:false,bridgeNetworkSuspected:false,maxWireguardInstances:4,notes:[]},servers:[],rssFeeds:[]};
// Whatever the editors and the route view ask for, answered at once from the fixture.
const client=new Client({url:"http://fixture.invalid/graphql",exchanges:[()=>operations=>pipe(operations,mergeMap(operation=>{
 const definition=operation.query.definitions.find(entry=>entry.kind==="OperationDefinition");
 const name=definition?.kind==="OperationDefinition"?definition.name?.value??"":"";
 const flow={...flowFor(initial,false),consumers:consumersFor(initial),proxies:profiles,proxyPools:pools,egresses};
 return fromValue({operation,data:name==="ProxyProfiles"?{proxyProfiles:profiles}:name==="Networking"?data:name==="NetworkFlow"||name==="NetworkFlowUpdates"?{networkFlow:flow}:{}});
}))]});
const action:NetworkingAction=async()=>null;
const width=Number(new URLSearchParams(location.search).get("width")??1400);
function Fixture(){
 const [route,setRoute]=useState(initial);const [down,setDown]=useState(false);const [editing,setEditing]=useState<FlowTarget|null>(null);
 const consumers=consumersFor(route);const flow=plannedFlow(flowFor(route,down),consumers);
 return <MemoryRouter><main className="bg-wv-bg text-wv-primary" style={{padding:16,maxWidth:width,margin:"auto",display:"grid",gap:16}}><button onClick={()=>setDown(!down)}>Toggle all down</button><NetworkFlow flow={flow} egresses={egresses} profiles={profiles} pools={pools} consumers={consumers} onEdit={setEditing}/><FlowEditor target={editing} onClose={()=>setEditing(null)} data={data} egresses={egresses} consumers={consumers} flow={flow} action={action} busy={false} proxyEditor={(id,onClose)=><ProxyEditorFor key={id} id={id} onClose={onClose} onChanged={()=>{}}/>}/>
 {/* A server's own editor is this wide; its route is shown there, not edited. */}
 <section aria-label="Server editor route" className="border border-wv-control" style={{width:620}}><RouteView consumer="server:1"/></section>
 <section aria-label="New server route" className="border border-wv-control" style={{width:620}}><RouteView/></section>
 <section aria-label="Held server route" className="border border-wv-control" style={{width:620}}><RouteView consumer="server:3"/></section>
 <section aria-label="New held server route" className="border border-wv-control" style={{width:620}}><RouteView killSwitch/></section>
 <RouteEditor value={route} onChange={setRoute} egresses={egresses} profiles={profiles} pools={pools} cap={50} status={flow.legs.filter(leg=>leg.consumer==="server:1")}/><output aria-label="Route weights">{route.legs.map(l=>l.weight).join(",")}</output></main></MemoryRouter>;
}
createRoot(document.getElementById("root")!).render(<TranslateContext.Provider value={{t:(key,values)=>translateDictionary(en,key,values),uiLanguage:"eng",selectedLanguage:{code:"eng",label:"English"},setLanguagePreference:()=>{}}}><Provider value={client}><Fixture/></Provider></TranslateContext.Provider>);

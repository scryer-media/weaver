import { useState } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { TranslateContext } from "@/lib/context/translate-context";
import { translateDictionary } from "@/lib/i18n";
import en from "@/lib/i18n/locales/en";
import { NetworkFlow } from "@/next/features/networking/NetworkFlow";
import { RouteEditor } from "@/next/features/networking/RouteEditor";
import { directLeg, type Egress, type NetworkFlow as Flow, type NetworkRoute } from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import "@/next/fonts.css";
import "@/next/theme.css";
const egresses:Egress[]=[{id:0,name:"System",bindingKind:"SYSTEM",interfaceName:null,sourceAddress:null,enabled:true,maxDownloadSpeed:0,health:"UP",reason:null},{id:1,name:"WAN Fiber",bindingKind:"SOURCE_ADDRESS",interfaceName:null,sourceAddress:"192.0.2.10",enabled:true,maxDownloadSpeed:0,health:"UP",reason:null},{id:2,name:"LTE standby",bindingKind:"SOURCE_ADDRESS",interfaceName:null,sourceAddress:"198.51.100.10",enabled:true,maxDownloadSpeed:0,health:"DOWN",reason:"Interface is down"}];
const profiles=[{id:1,name:"Amsterdam",kind:"WIRE_GUARD",enabled:true},{id:2,name:"Frankfurt",kind:"WIRE_GUARD",enabled:true},{id:3,name:"Lisbon",kind:"WIRE_GUARD",enabled:false},{id:4,name:"Relay one",kind:"SOCKS5",enabled:true},{id:5,name:"Relay two",kind:"SOCKS5",enabled:true},{id:6,name:"Exit",kind:"HTTP_CONNECT",enabled:true}] as ProxyProfile[];
const pools=[{id:1,name:"Europe",kind:"WIRE_GUARD" as const,memberIds:[1,2,3],enabled:true}];
const initial:NetworkRoute={failover:"HOLD",legs:[{egressId:1,weight:60,path:{kind:"LADDER",directFallback:false,rungs:[{kind:"POOL",poolId:1,proxyId:null,chainIds:[]}]}},{...directLeg(),weight:30},{...directLeg(),egressId:2,weight:10}]};
// A feed whose chain fails at its second hop and whose next proxy fails too, so it has fallen back to going direct.
const feed:NetworkRoute={failover:"REDISTRIBUTE",legs:[{egressId:0,weight:100,path:{kind:"LADDER",directFallback:true,rungs:[{kind:"CHAIN",proxyId:null,poolId:null,chainIds:[4,5,6]},{kind:"PROXY",proxyId:2,poolId:null,chainIds:[]}]}}]};
const feedLeg={...feed.legs[0]!,consumer:"rss:1",position:0,target:1,open:1,opening:0,state:"UP",reason:null,pinnedAddress:null,sourceAddress:"127.0.0.1:45130",bytesPerSecond:0,selectedRung:null,selectedProxyId:null,rungStates:["COOLDOWN","FAILING","STANDBY"],failingHops:[{rung:0,proxyId:5,reason:"endpoint unreachable: connection refused"},{rung:1,proxyId:2,reason:"handshake did not complete"}]};
const width=Number(new URLSearchParams(location.search).get("width")??1400);
function Fixture(){
 const [route,setRoute]=useState(initial);const [down,setDown]=useState(false);
 const flow:Flow={sampledAt:1790640000,legs:route.legs.map((leg,position)=>({...leg,consumer:"server:1",position,target:position===2||down?0:[30,15][position]??0,open:position===2||down?0:[24,12][position]??0,opening:0,state:position===2||down?"DOWN":"UP",reason:position===2?"Interface is down":null,pinnedAddress:null,sourceAddress:position===0?"192.0.2.10:45123":"127.0.0.1:45124",bytesPerSecond:position===0?24_000_000:0,selectedRung:position===0?0:null,selectedProxyId:position===0?1:null})).concat(feedLeg),pools:[{poolId:1,egressId:1,pinnedMember:1,members:[{id:1,open:24,opening:0,warmed:true,blocked:null,handshakeMs:30,connectMs:20,bytesPerSecond:1_000_000,samples:40,failures:0},{id:2,open:0,opening:0,warmed:true,blocked:null,handshakeMs:40,connectMs:25,bytesPerSecond:800_000,samples:20,failures:0},{id:3,state:"DISABLED",open:0,opening:0,warmed:false,blocked:null,handshakeMs:null,connectMs:null,bytesPerSecond:null,samples:0,failures:0}]}]};
 return <MemoryRouter><main className="bg-wv-bg text-wv-primary" style={{padding:16,maxWidth:width,margin:"auto",display:"grid",gap:16}}><button onClick={()=>setDown(!down)}>Toggle all down</button><NetworkFlow flow={flow} egresses={egresses} profiles={profiles} pools={pools} consumers={[{key:"server:1",name:"News primary",cap:50,route},{key:"rss:1",name:"Feed mirror",cap:1,route:feed}]}/><RouteEditor value={route} onChange={setRoute} egresses={egresses} profiles={profiles} pools={pools} cap={50} status={flow.legs.filter(leg=>leg.consumer==="server:1")}/><output aria-label="Route weights">{route.legs.map(l=>l.weight).join(",")}</output></main></MemoryRouter>;
}
createRoot(document.getElementById("root")!).render(<TranslateContext.Provider value={{t:(key,values)=>translateDictionary(en,key,values),uiLanguage:"eng",selectedLanguage:{code:"eng",label:"English"},setLanguagePreference:()=>{}}}><Fixture/></TranslateContext.Provider>);

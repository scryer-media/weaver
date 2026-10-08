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
const profiles=[{id:1,name:"Amsterdam",kind:"WIRE_GUARD",enabled:true},{id:2,name:"Frankfurt",kind:"WIRE_GUARD",enabled:true}] as ProxyProfile[];
const pools=[{id:1,name:"Europe",kind:"WIRE_GUARD" as const,memberIds:[1,2],enabled:true}];
const initial:NetworkRoute={failover:"HOLD",legs:[{egressId:1,weight:60,path:{kind:"LADDER",directFallback:false,rungs:[{kind:"POOL",poolId:1,proxyId:null,chainIds:[]}]}},{...directLeg(),weight:30},{...directLeg(),egressId:2,weight:10}]};
const width=Number(new URLSearchParams(location.search).get("width")??1400);
function Fixture(){
 const [route,setRoute]=useState(initial);const [down,setDown]=useState(false);
 const flow:Flow={sampledAt:1790640000,legs:route.legs.map((leg,position)=>({...leg,consumer:"server:1",position,target:position===2||down?0:[30,15][position]??0,open:position===2||down?0:[24,12][position]??0,opening:0,state:position===2||down?"DOWN":"UP",reason:position===2?"Interface is down":null,pinnedAddress:null,sourceAddress:position===0?"192.0.2.10:45123":"127.0.0.1:45124",bytesPerSecond:position===0?24_000_000:0,selectedRung:position===0?0:null,selectedProxyId:position===0?1:null})),pools:[{poolId:1,egressId:1,pinnedMember:1,members:[{id:1,open:24,opening:0,warmed:true,blocked:null,handshakeMs:30,connectMs:20,bytesPerSecond:1_000_000,samples:40,failures:0},{id:2,open:0,opening:0,warmed:true,blocked:null,handshakeMs:40,connectMs:25,bytesPerSecond:800_000,samples:20,failures:0}]}]};
 return <MemoryRouter><main className="bg-wv-bg text-wv-primary" style={{padding:16,maxWidth:width,margin:"auto",display:"grid",gap:16}}><button onClick={()=>setDown(!down)}>Toggle all down</button><NetworkFlow flow={flow} egresses={egresses} profiles={profiles} pools={pools} consumers={[{key:"server:1",name:"News primary",cap:50,route}]}/><RouteEditor value={route} onChange={setRoute} egresses={egresses} profiles={profiles} pools={pools} cap={50} status={flow.legs}/><output aria-label="Route weights">{route.legs.map(l=>l.weight).join(",")}</output></main></MemoryRouter>;
}
createRoot(document.getElementById("root")!).render(<TranslateContext.Provider value={{t:(key,values)=>translateDictionary(en,key,values),uiLanguage:"eng",selectedLanguage:{code:"eng",label:"English"},setLanguagePreference:()=>{}}}><Fixture/></TranslateContext.Provider>);

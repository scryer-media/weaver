import { useState } from "react";
import { createRoot } from "react-dom/client";
import { Link, MemoryRouter } from "react-router";
import { cacheExchange, createClient, fetchExchange, subscriptionExchange, Provider } from "urql";
import { createClient as createWSClient } from "graphql-ws";
import { TranslateContext } from "@/lib/context/translate-context";
import { translateDictionary } from "@/lib/i18n";
import en from "@/lib/i18n/locales/en";
import { NetworkingWorkspace } from "@/next/features/networking/NetworkingWorkspace";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";
const ws=createWSClient({url:`ws://${window.location.host}/graphql`});
const client=createClient({url:"/graphql",preferGetMethod:false,requestPolicy:"cache-and-network",exchanges:[cacheExchange,fetchExchange,subscriptionExchange({forwardSubscription(operation){return {subscribe(sink){return {unsubscribe:ws.subscribe({...operation,query:operation.query!},sink)};}};}})]});
const actionsRef={current:null};
const setFlags=()=>{};
function Fixture(){
 const [host,setHost]=useState<HTMLDivElement|null>(null);
 return <main className="bg-wv-bg text-wv-primary" style={{padding:16,display:"grid",gap:16}}><nav style={{display:"flex",gap:16}}><Link to="/settings/networking/overview">Overview</Link><Link to="/settings/networking/egress">Egress interfaces</Link><Link to="/settings/networking/routes">Routes</Link></nav><div ref={setHost}/>{host&&<SettingsShellProvider search="" actionsRef={actionsRef} setFlags={setFlags} controlsHost={host}><NetworkingWorkspace proxies={null} bandwidth={null}/></SettingsShellProvider>}</main>;
}
createRoot(document.getElementById("root")!).render(<Provider value={client}><TranslateContext.Provider value={{t:(key,values)=>translateDictionary(en,key,values),uiLanguage:"eng",selectedLanguage:{code:"eng",label:"English"},setLanguagePreference:()=>{}}}><MemoryRouter initialEntries={["/settings/networking/egress"]}><Fixture/></MemoryRouter></TranslateContext.Provider></Provider>);

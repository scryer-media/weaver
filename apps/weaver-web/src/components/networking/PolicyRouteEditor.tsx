import { useQuery, useSubscription } from "urql";
import { Link } from "react-router";
import { NETWORKING_QUERY, NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION } from "@/graphql/networking";
import { policyAsRoute, type RoutingPolicy } from "@/lib/proxies";
import type { NetworkFlow as Flow } from "@/lib/networking";
import type { NetworkingData } from "./NetworkingWorkspace";
import { NetworkFlow } from "./NetworkFlow";
import { RouteEditor } from "./RouteEditor";
import "./networking.css";

export type PolicyRouteProps = {value:RoutingPolicy;onChange:(value:RoutingPolicy)=>void;consumer?:string;cap?:number;rss?:boolean};
export function PolicyRouteEditor({value,onChange,consumer,cap=1,rss=false}:PolicyRouteProps) {
  const [{data,error}]=useQuery<NetworkingData>({query:NETWORKING_QUERY});
  const [initial]=useQuery<{networkFlow:Flow}>({query:NETWORK_FLOW_QUERY,pause:!consumer});
  const [live]=useSubscription<{networkFlow:Flow}>({query:NETWORK_FLOW_SUBSCRIPTION,pause:!consumer});
  const route=policyAsRoute(value);
  const measured=live.data?.networkFlow??initial.data?.networkFlow;
  const flow=measured?{...measured,legs:measured.legs.filter(l=>l.consumer===consumer)}:undefined;
  return <div className="networking-workspace network-card"><h3>Network route</h3>{error&&<p role="alert">Unable to load networking resources. Existing routing is preserved.</p>}{consumer?<><p>{route.legs.length} {route.legs.length===1?"leg":"legs"} · {rss?"first healthy leg":route.failover.toLowerCase()}</p>{data&&flow&&<NetworkFlow compact flow={flow} egresses={data.egressInterfaces.filter(e=>route.legs.some(l=>l.egressId===e.id))} profiles={data.proxyProfiles} pools={data.proxyPools} consumers={[{key:consumer,name:rss?"RSS feed":"NNTP server",cap,route}]}/>}</>:data?<RouteEditor value={route} onChange={route=>onChange({...value,...route})} egresses={data.egressInterfaces} profiles={data.proxyProfiles} pools={data.proxyPools} cap={cap} rss={rss}/>:<p>Loading route editor…</p>}<Link to={`/settings/networking/routes${consumer?`?consumer=${encodeURIComponent(consumer)}`:""}`}>Edit in Networking</Link></div>;
}

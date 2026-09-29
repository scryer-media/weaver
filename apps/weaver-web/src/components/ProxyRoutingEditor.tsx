import { PolicyRouteEditor, type PolicyRouteProps } from "@/components/networking/PolicyRouteEditor";
import type { RoutingStatus } from "@/lib/proxies";

export function ProxyRoutingEditor(props:PolicyRouteProps) {
  return <PolicyRouteEditor {...props}/>;
}

export function ProxyRoutingStatus({ status }: { status?: RoutingStatus }) {
  if (!status) return null;
  return <div className="text-xs text-muted-foreground" role="status">
    Route: {status.state.toLowerCase()}{status.selectedProxyId != null ? ` · proxy #${status.selectedProxyId}` : ""}
    {status.failures.map(f => <p key={f.proxyId}>Proxy #{f.proxyId}: {f.message}</p>)}
  </div>;
}

import { useQuery } from "urql";
import { Link } from "react-router";
import { Button } from "@/components/ui/button";
import { PROXY_PROFILES_QUERY } from "@/graphql/proxies";
import { appendProxy, moveProxy, proxyLabels, type ProxyProfile, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";

export function ProxyRoutingEditor({ value, onChange }: { value: RoutingPolicy; onChange: (value: RoutingPolicy) => void }) {
  const [{ data, error }] = useQuery<{ proxyProfiles: ProxyProfile[] }>({ query: PROXY_PROFILES_QUERY });
  const profiles = data?.proxyProfiles ?? [];
  return <fieldset className="space-y-3 rounded-inner border border-border p-5">
    <legend className="px-2 text-sm font-semibold">Proxy route</legend>
    <p className="text-sm text-muted-foreground">Try each route in order. New connections return to the primary when it recovers.</p>
    {error && <p role="alert">Unable to load proxy profiles. Existing assignments are preserved.</p>}
    <ol className="space-y-2">
      {value.proxyIds.map((id, index) => {
        const profile = profiles.find(p => p.id === id);
        return <li key={id} className="flex flex-wrap items-center gap-2">
          <span className="flex-1 text-sm">{index === 0 ? "Primary" : index === 1 ? "Secondary" : `Route ${index + 1}`}: {profile?.name ?? `Proxy #${id}`} {profile && !profile.enabled ? "(disabled)" : ""}</span>
          <Button type="button" size="sm" variant="outline" aria-label={`Move proxy ${id} up`} disabled={index === 0} onClick={() => onChange(moveProxy(value, index, -1))}>↑</Button>
          <Button type="button" size="sm" variant="outline" aria-label={`Move proxy ${id} down`} disabled={index === value.proxyIds.length - 1} onClick={() => onChange(moveProxy(value, index, 1))}>↓</Button>
          <Button type="button" size="sm" variant="ghost" aria-label={`Remove proxy ${id}`} onClick={() => onChange({ ...value, proxyIds: value.proxyIds.filter(v => v !== id) })}>Remove</Button>
        </li>;
      })}
    </ol>
    <select aria-label="Add proxy route" className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm" value="" disabled={value.proxyIds.length >= 8} onChange={event => onChange(appendProxy(value, Number(event.target.value)))}>
      <option value="" disabled>Add a proxy…</option>
      {profiles.filter(p => !value.proxyIds.includes(p.id)).map(p => <option key={p.id} value={p.id}>{p.name} · {proxyLabels[p.kind]}{!p.enabled ? " (disabled)" : ""}</option>)}
    </select>
    <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={value.allowDirect} onChange={e => onChange({ ...value, allowDirect: e.target.checked })} />Allow direct host access as the final fallback</label>
    <p className="text-sm font-medium">Final route: {value.allowDirect ? "Direct" : "Blocked"}</p>
    {!value.allowDirect && value.proxyIds.length === 0 && <p className="text-sm text-muted-foreground">This consumer cannot connect until a proxy is assigned or direct access is allowed.</p>}
    <Link className="text-sm text-primary underline" to="/settings/proxies">Manage proxy profiles</Link>
  </fieldset>;
}

export function ProxyRoutingStatus({ status }: { status?: RoutingStatus }) {
  if (!status) return null;
  return <div className="text-xs text-muted-foreground" role="status">
    Route: {status.state.toLowerCase()}{status.selectedProxyId != null ? ` · proxy #${status.selectedProxyId}` : ""}
    {status.failures.map(f => <p key={f.proxyId}>Proxy #{f.proxyId}: {f.message}</p>)}
  </div>;
}

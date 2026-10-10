import type { RoutingStatus } from "@/lib/proxies";
import { useTranslate } from "@/lib/context/translate-context";

export function RoutingState({ status }: { status?: RoutingStatus }) {
  const t = useTranslate();
  if (!status) {
    return null;
  }
  const stateKey = `next.routing.state.${status.state.toLowerCase()}`;
  const state = t(stateKey);
  return (
    <span className="font-wv-mono text-[11px] text-wv-muted">
      {state === stateKey ? status.state.toLowerCase() : state}
      {status.selectedProxyId == null
        ? ""
        : ` · ${t("next.routing.proxyId", { id: status.selectedProxyId })}`}
    </span>
  );
}

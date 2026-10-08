import { Link } from "react-router";
import { useQuery, useSubscription } from "urql";
import { NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { NetworkFlow } from "@/lib/networking";
import { policyAsRoute, type RoutingPolicy } from "@/lib/proxies";
import { Square } from "../../components/chrome";
import { countLabel } from "../../i18n/labels";
import type { NetworkingData } from "./data";
import { legColor, RouteEditor } from "./RouteEditor";
import { formatRate, stateLabel, StatusMark } from "./presentation";

export type PolicyRouteProps = {
  value: RoutingPolicy;
  onChange: (value: RoutingPolicy) => void;
  /** The saved consumer's key; absent while the server or feed is still being created. */
  consumer?: string;
  cap?: number;
  rss?: boolean;
};

/**
 * The route field inside a server's or a feed's own editor.
 *
 * A saved consumer shows its route and what each leg is doing, and links to
 * the routes page to change it, so a route is edited in one place. A new one
 * has no key to route yet, so it gets the editor here and the route is saved
 * with the record.
 */
export function PolicyRouteEditor({ value, onChange, consumer, cap = 1, rss = false }: PolicyRouteProps) {
  const t = useTranslate();
  const [{ data, error }] = useQuery<NetworkingData>({ query: NETWORKING_QUERY });
  const [initial] = useQuery<{ networkFlow: NetworkFlow }>({ query: NETWORK_FLOW_QUERY, pause: !consumer });
  const [live] = useSubscription<{ networkFlow: NetworkFlow }>({ query: NETWORK_FLOW_SUBSCRIPTION, pause: !consumer });
  const route = policyAsRoute(value);
  const measured = live.data?.networkFlow ?? initial.data?.networkFlow;
  const legs = measured?.legs.filter((leg) => leg.consumer === consumer) ?? [];
  const egressName = (id: number) => data?.egressInterfaces.find((egress) => egress.id === id)?.name ?? `#${id}`;

  return (
    <div className="flex w-[340px] max-w-full min-w-0 flex-col gap-3">
      {error ? (
        <p role="alert" className="text-[12.5px] leading-[1.5] text-wv-error-text">
          {t("next.networking.policy.loadFailed")}
        </p>
      ) : null}
      {consumer ? (
        <>
          <span className="font-wv-mono text-[11.5px] text-wv-muted">
            {countLabel(t, "next.networking.legCount", route.legs.length)} ·{" "}
            {rss
              ? t("next.networking.policy.firstHealthy")
              : route.failover === "HOLD"
                ? t("next.networking.failover.hold")
                : t("next.networking.failover.redistribute")}
          </span>
          <ul className="flex flex-col border border-wv-control">
            {route.legs.map((leg, position) => {
              const sample = legs.find((candidate) => candidate.position === position);
              return (
                <li key={position} className="flex flex-col gap-[5px] border-t border-wv-hairline bg-wv-input px-3 py-[9px] first:border-t-0">
                  <span className="flex min-w-0 items-center gap-2 text-[12.5px] text-wv-fg">
                    <Square color={legColor(position)} />
                    <span className="min-w-0 truncate">
                      {t("next.networking.policy.leg", {
                        position: position + 1,
                        egress: egressName(leg.egressId),
                        weight: leg.weight,
                      })}
                    </span>
                  </span>
                  {sample ? (
                    <StatusMark
                      state={sample.state}
                      label={stateLabel(t, sample.state)}
                      detail={`${sample.open}/${sample.target} · ${formatRate(sample.bytesPerSecond)}`}
                    />
                  ) : (
                    <StatusMark state="IDLE" label={stateLabel(t, "IDLE")} />
                  )}
                </li>
              );
            })}
          </ul>
        </>
      ) : data ? (
        <RouteEditor
          value={route}
          onChange={(next) => onChange({ ...value, ...next })}
          egresses={data.egressInterfaces}
          profiles={data.proxyProfiles}
          pools={data.proxyPools}
          cap={cap}
          rss={rss}
        />
      ) : (
        <span className="text-[12.5px] text-wv-muted">{t("next.networking.policy.loading")}</span>
      )}
      <Link
        to={`/settings/networking/routes${consumer ? `?consumer=${encodeURIComponent(consumer)}` : ""}`}
        className="self-start text-[12.5px] text-wv-accent hover:underline"
      >
        {t("next.networking.policy.editInNetworking")}
      </Link>
    </div>
  );
}

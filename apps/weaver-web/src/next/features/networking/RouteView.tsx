import { Link } from "react-router";
import { useQuery, useSubscription } from "urql";
import { NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { NetworkFlow as Flow } from "@/lib/networking";
import { EMPTY_FLOW, flowConsumers, plannedFlow, type Consumer, type NetworkingData } from "./data";
import { NetworkFlow } from "./NetworkFlow";

/**
 * A server's or a feed's route, shown inside its own editor.
 *
 * Routes are made and changed under Networking, so this only shows the one
 * this consumer has: the egress each leg leaves through and the proxies it
 * tunnels through, with what each is doing now. A consumer that has not been
 * saved has no route to show; it starts on the default one, or behind a kill
 * switch if it is being made with one.
 */
export function RouteView({ consumer, killSwitch = false }: { consumer?: string; killSwitch?: boolean }) {
  const t = useTranslate();
  const [{ data, error }] = useQuery<NetworkingData>({ query: NETWORKING_QUERY, pause: !consumer });
  const [initial] = useQuery<{ networkFlow: Flow }>({ query: NETWORK_FLOW_QUERY, pause: !consumer });
  const [live] = useSubscription<{ networkFlow: Flow }>({ query: NETWORK_FLOW_SUBSCRIPTION, pause: !consumer });
  if (!consumer) {
    return (
      <p className="px-4 py-4 text-[12.5px] leading-[1.5] text-wv-muted sm:px-6">
        {t(killSwitch ? "next.networking.policy.unsavedBlocked" : "next.networking.policy.unsaved")}
      </p>
    );
  }

  const measured = live.data?.networkFlow ?? initial.data?.networkFlow ?? EMPTY_FLOW;
  const find = (consumers: Consumer[]) => consumers.find((candidate) => candidate.key === consumer);
  // The flow names the consumers it has sampled; one it has not is still in the configuration.
  const own = find(flowConsumers(measured, data)) ?? find(flowConsumers(EMPTY_FLOW, data));
  return (
    <div className="flex min-w-0 flex-col">
      {error ? (
        <p role="alert" className="px-4 py-4 text-[12.5px] leading-[1.5] text-wv-error-text sm:px-6">
          {t("next.networking.policy.loadFailed")}
        </p>
      ) : null}
      {own ? (
        <NetworkFlow
          compact
          flow={plannedFlow(measured, [own])}
          egresses={measured.egresses ?? data?.egressInterfaces ?? []}
          profiles={measured.proxies ?? data?.proxyProfiles ?? []}
          pools={measured.proxyPools ?? data?.proxyPools ?? []}
          consumers={[own]}
        />
      ) : error ? null : (
        <p className="px-4 py-4 text-[12.5px] leading-[1.5] text-wv-muted sm:px-6">{t("next.networking.policy.loading")}</p>
      )}
      <Link
        to={`/settings/networking/routes?consumer=${encodeURIComponent(consumer)}`}
        className="self-start px-4 py-3 text-[12.5px] text-wv-accent hover:underline sm:px-6"
      >
        {t("next.networking.policy.editInNetworking")}
      </Link>
    </div>
  );
}

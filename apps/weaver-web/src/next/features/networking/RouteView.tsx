import { Link } from "react-router";
import { useQuery, useSubscription } from "urql";
import { NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import {
  closedPath,
  testedPlan,
  type DownTarget,
  type Egress,
  type NetworkFlow as Flow,
  type NetworkRoute,
  type PathOverride,
  type ProxyPool,
} from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import { EMPTY_FLOW, flowConsumers, plannedFlow, speedPage, type Consumer, type NetworkingData } from "./data";
import { NetworkFlow, type FlowEndpoint } from "./NetworkFlow";

/** One leg while a test measures: its connections, the rung they went by (null for direct), and its rate now. */
export type LiveLeg = { connections: number; rung: number | null | undefined; bytesPerSecond: number };

/**
 * A server's or a feed's route, shown inside its own editor.
 *
 * Routes are made and changed under Networking, so this only shows the one
 * this consumer has: the egress each leg leaves through and the proxies it
 * tunnels through, with what each is doing now. A consumer that has not been
 * saved has no route to show; it starts on the default one, or behind a kill
 * switch if it is being made with one.
 */
export function RouteView({
  consumer,
  killSwitch = false,
  speedLink = false,
}: {
  consumer?: string;
  killSwitch?: boolean;
  /** Also link to the speed test with this consumer selected. */
  speedLink?: boolean;
}) {
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
      <div className="flex flex-wrap items-center">
        <Link
          to={`/settings/networking/routes?consumer=${encodeURIComponent(consumer)}`}
          className="self-start px-4 py-3 text-[12.5px] text-wv-accent hover:underline sm:px-6"
        >
          {t("next.networking.policy.editInNetworking")}
        </Link>
        {speedLink ? (
          <Link to={speedPage(consumer)} className="self-start px-4 py-3 text-[12.5px] text-wv-accent hover:underline sm:px-6">
            {t("next.networking.speed.title")}
          </Link>
        ) : null}
      </div>
    </div>
  );
}

/**
 * A route as a speed test takes it: the compact flow of one consumer, drawn
 * from a route that need not be the saved one (a draft, or the snapshot a run
 * kept), with what is down drawn down and each leg's share as the test gives it.
 * With `onToggle` every box carries the Down switch that changes it. While a
 * test measures, `live` has each leg's connections and rate, and the lines
 * that carry them move.
 */
export function RoutePlanView({
  consumer,
  route,
  targets,
  overrides,
  egresses,
  profiles,
  pools,
  legDown,
  live,
  endpoint,
  onToggle,
}: {
  consumer: Pick<Consumer, "key" | "name" | "cap">;
  route: NetworkRoute;
  /** The connections each leg is given. */
  targets: number[];
  overrides: PathOverride;
  egresses: Egress[];
  profiles: ProxyProfile[];
  pools: ProxyPool[];
  /** Which legs were down, when the daemon has said; otherwise worked out from `overrides`. */
  legDown?: boolean[];
  live?: LiveLeg[];
  /** What the test measures against, drawn as the box the flow ends at. */
  endpoint?: FlowEndpoint;
  onToggle?: (target: DownTarget) => void;
}) {
  const plan = testedPlan(route, overrides, pools);
  const down = legDown ?? plan.legDown;
  const flow: Flow = {
    pools: [],
    legs: route.legs.map((leg, position) => {
      const rungs = leg.path.kind === "DIRECT" ? [] : leg.path.rungs;
      const now = down[position] ? undefined : live?.[position];
      const open = now?.connections ?? 0;
      const firstUp = plan.rungDown[position]?.findIndex((rungDown) => !rungDown) ?? 0;
      return {
        ...leg,
        consumer: consumer.key,
        position,
        target: Math.max(targets[position] ?? 0, open),
        open,
        opening: 0,
        bytesPerSecond: now?.bytesPerSecond ?? 0,
        // Unknown until a connection says, so the first rung still up stands in, then going direct.
        selectedRung: now?.rung !== undefined ? now.rung : rungs.length && firstUp >= 0 ? firstUp : null,
        state: down[position] ? "DOWN" : closedPath(leg.path) ? "BLOCKED" : open > 0 ? "ACTIVE" : "IDLE",
        reason: null,
        pinnedAddress: null,
        // The rungs, then going direct.
        rungStates: down[position]
          ? [...rungs, null].map(() => "DOWN")
          : [...rungs.map((_, rung) => (plan.rungDown[position]?.[rung] ? "DOWN" : "STANDBY")), "STANDBY"],
      };
    }),
  };
  const drawn = egresses.map((egress) =>
    overrides.downEgresses.includes(egress.id) ? { ...egress, health: "DOWN", reason: null } : egress,
  );
  return (
    <NetworkFlow
      compact
      flow={flow}
      egresses={drawn}
      profiles={profiles}
      pools={pools}
      consumers={[{ ...consumer, route }]}
      endpoint={endpoint}
      traffic={!!live}
      toggles={{ overrides, onToggle }}
    />
  );
}

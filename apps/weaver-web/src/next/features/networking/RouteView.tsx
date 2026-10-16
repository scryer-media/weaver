// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import {
  closedPath,
  testedPlan,
  type DownTarget,
  type Egress,
  type NetworkFlow as Flow,
  type NetworkRoute,
  type PathOverride,
  type PoolFlow,
  type ProxyPool,
} from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import type { Consumer } from "./data";
import { NetworkFlow, type FlowEndpoint } from "./NetworkFlow";

/** One leg while a test measures: its connections, the rung they went by (null for direct), and its rate now. */
export type LiveLeg = { connections: number; rung: number | null | undefined; bytesPerSecond: number };

/**
 * A route as a speed test takes it: the overview's flow of one consumer, drawn
 * from a route that need not be the saved one (a draft, or the snapshot a run
 * kept), with what is down drawn down and each leg's share as the test gives it.
 * With `onToggle` every box carries the Down switch that changes it. While a
 * test measures, `live` has each leg's connections and rate, and the lines
 * that carry them move; `livePools` has the pool members those connections hold.
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
  livePools,
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
  livePools?: PoolFlow[];
  /** What the test measures against, drawn as the box the flow ends at. */
  endpoint?: FlowEndpoint;
  onToggle?: (target: DownTarget) => void;
}) {
  const plan = testedPlan(route, overrides, pools);
  const down = legDown ?? plan.legDown;
  const flow: Flow = {
    pools: livePools ?? [],
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
      flow={flow}
      egresses={drawn}
      profiles={profiles}
      pools={pools}
      consumers={[{ ...consumer, route }]}
      endpoint={endpoint}
      traffic={!!live}
      trace
      toggles={{ overrides, onToggle }}
    />
  );
}

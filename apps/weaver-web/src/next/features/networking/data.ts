// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useMemo, useRef, useState } from "react";
import { useClient, useQuery, useSubscription, type Client } from "urql";
import type { DocumentNode } from "graphql";
import { NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY } from "@/graphql/networking";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  allocation,
  closedPath,
  directLeg,
  OUTBOUND_CONSUMER,
  TORRENT_DEFAULT_CONSUMER,
  type ConsumerKind,
  type Egress,
  type NetworkFlow,
  type NetworkRoute,
  type ProxyPool,
} from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import { refusalMessage } from "@/lib/refusal";

export type NetworkingData = {
  egressInterfaces: Egress[];
  proxyProfiles: ProxyProfile[];
  proxyPools: ProxyPool[];
  discoverNetworkInterfaces: { name: string; index: number | null; up: boolean; addresses: string[] }[];
  platformNetworking: {
    platform: string;
    egressBindingKinds: Egress["bindingKind"][];
    sourceAddressHint: string | null;
    container: boolean;
    bridgeNetworkSuspected: boolean;
    maxWireguardInstances: number;
    notes: string[];
  };
  servers: { id: number; host: string; connections: number; active: boolean; routing: NetworkRoute | null }[];
  rssFeeds: { id: number; name: string; enabled: boolean; routing: NetworkRoute | null }[];
};

/** A server, a feed, or Default outbound: something that owns a route. */
export type Consumer = {
  key: string;
  id: number;
  name: string;
  kind: ConsumerKind;
  cap: number;
  route: NetworkRoute | null;
};

export const NETWORKING_PAGES = ["overview", "egress", "proxies", "routes", "speed"] as const;
export type NetworkingPage = (typeof NETWORKING_PAGES)[number];

/** The speed test page with a route selected. */
export const speedPage = (consumer: string) => `/settings/networking/speed?consumer=${encodeURIComponent(consumer)}`;

/** Run a mutation; resolves to the failure message, or null once it has landed. */
/** `onData` hears what a mutation that landed returned, before the pages are read again. */
export type NetworkingAction = (
  document: DocumentNode,
  variables: Record<string, unknown>,
  onData?: (data: unknown) => void,
) => Promise<string | null>;

export const EMPTY_FLOW: NetworkFlow = { legs: [], pools: [] };
const NO_EGRESSES: Egress[] = [];
const NO_PROFILES: ProxyProfile[] = [];
const NO_POOLS: ProxyPool[] = [];

/** A category's own torrent route is keyed by this and the category's id. */
const TORRENT_CATEGORY_PREFIX = "torrent:category:";

export const defaultRoute = (): NetworkRoute => ({ legs: [directLeg()], failover: "REDISTRIBUTE" });

/** A consumer's name. Default outbound and the torrent routes are named here, in the reader's language, not by the daemon. */
export const consumerName = (t: Translate, key: string, name: string) =>
  key === OUTBOUND_CONSUMER
    ? t("next.networking.defaultOutbound")
    : key === TORRENT_DEFAULT_CONSUMER
      ? t("next.networking.defaultTorrent")
      : key.startsWith(TORRENT_CATEGORY_PREFIX)
        ? t("next.networking.torrentCategory", { name })
        : name;

/**
 * Everything that owns a route. The flow names them once the daemon has
 * sampled one; before that they are the servers and feeds as configured.
 * Default outbound has no configuration read here, so only the flow brings
 * it: with no route to start from, its editor could only start on the
 * default one and save over what is there.
 */
export function flowConsumers(measured: NetworkFlow, data: NetworkingData | undefined, t: Translate): Consumer[] {
  return (
    measured.consumers?.map((consumer) => ({ ...consumer, name: consumerName(t, consumer.key, consumer.name) })) ??
    (data
      ? [
          ...data.servers.map((server) => ({
            key: `server:${server.id}`,
            id: server.id,
            name: server.host,
            kind: "SERVER" as const,
            cap: server.connections,
            route: server.routing,
          })),
          ...data.rssFeeds.map((feed) => ({
            key: `rss:${feed.id}`,
            id: feed.id,
            name: feed.name,
            kind: "RSS" as const,
            cap: 1,
            route: feed.routing,
          })),
        ]
      : [])
  );
}

/**
 * The flow with a leg for every leg these consumers' routes have. One the
 * daemon has not sampled stands idle at the share its weight would give it,
 * so a route that has never carried traffic still shows where it would go.
 *
 * A leg behind a kill switch is blocked whatever was sampled: until something
 * dials through it the daemon knows only that its egress is up.
 */
export function plannedFlow(measured: NetworkFlow, consumers: Consumer[]): NetworkFlow {
  return {
    ...measured,
    legs: consumers.flatMap((consumer) => {
      const route = consumer.route ?? defaultRoute();
      const targets = allocation(
        route.legs.map((leg) => leg.weight),
        consumer.cap,
      );
      return route.legs.map((leg, position) => {
        const sampled = measured.legs.find(
          (sample) => sample.consumer === consumer.key && sample.position === position,
        ) ?? {
          ...leg,
          consumer: consumer.key,
          position,
          target: targets[position] ?? 0,
          open: 0,
          opening: 0,
          state: "IDLE",
          reason: null,
          pinnedAddress: null,
        };
        return closedPath(sampled.path) ? { ...sampled, state: "BLOCKED" } : sampled;
      });
    }),
  };
}

/**
 * Runs one mutation. `report` hears how it went as soon as it lands; once it
 * has, the configuration and the flow are read again before this resolves to
 * the failure message, or null. Outside the hook, so the hook itself compiles.
 */
async function runNetworkingAction(
  client: Client,
  t: Translate,
  document: DocumentNode,
  variables: Record<string, unknown>,
  refresh: () => void,
  report: (failure: string | null) => void,
  onData?: (data: unknown) => void,
): Promise<string | null> {
  try {
    const result = await client.mutation(document, variables).toPromise();
    const failure = result.error ? refusalMessage(t, result.error) : null;
    report(failure);
    if (failure === null) {
      onData?.(result.data);
      refresh();
      await client.query(NETWORK_FLOW_QUERY, {}, { requestPolicy: "network-only" }).toPromise();
    }
    return failure;
  } catch (caught) {
    const failure = caught instanceof Error ? caught.message : t("next.networking.requestFailed");
    report(failure);
    return failure;
  }
}

/**
 * Everything the networking pages read, and the one way they write.
 *
 * The live flow is only subscribed to on the pages that draw it.
 */
export function useNetworkingWorkspace(page: string) {
  const t = useTranslate();
  const client = useClient();
  const live = page !== "proxies";
  const [{ data, error, fetching }, refresh] = useQuery<NetworkingData>({
    query: NETWORKING_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [initial] = useQuery<{ networkFlow: NetworkFlow }>({
    query: NETWORK_FLOW_QUERY,
    requestPolicy: "cache-and-network",
    pause: !live,
  });
  const [subscription] = useSubscription<{ networkFlow: NetworkFlow }>({
    query: NETWORK_FLOW_SUBSCRIPTION,
    pause: !live,
  });
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  // Kept the same across renders, as is everything derived below, so a page
  // drawn again for some other reason hands its rows the same props.
  const action = useCallback<NetworkingAction>(async (document, variables, onData) => {
    setBusy(true);
    setMessage(null);
    const failure = await runNetworkingAction(
      client,
      t,
      document,
      variables,
      () => refresh({ requestPolicy: "network-only" }),
      (outcome) => {
        setFailed(outcome !== null);
        setMessage(outcome ?? t("next.networking.saved"));
      },
      onData,
    );
    setBusy(false);
    return failure;
  }, [client, refresh, t]);
  const retry = useCallback(() => refresh({ requestPolicy: "network-only" }), [refresh]);

  const measured = subscription.data?.networkFlow ?? initial.data?.networkFlow ?? EMPTY_FLOW;
  // Every sample carries the consumers, egresses and proxies afresh, and they
  // seldom change; what draws them is drawn again only when one of them has.
  const consumers = useSameWhileUnchanged(flowConsumers(measured, data, t));
  const flow = useMemo(() => plannedFlow(measured, consumers), [consumers, measured]);
  const egresses = useSameWhileUnchanged(measured.egresses ?? data?.egressInterfaces ?? NO_EGRESSES);
  const profiles = useSameWhileUnchanged(measured.proxies ?? data?.proxyProfiles ?? NO_PROFILES);
  const pools = useSameWhileUnchanged(measured.proxyPools ?? data?.proxyPools ?? NO_POOLS);

  return {
    data,
    error,
    fetching,
    retry,
    liveError: subscription.error,
    measured,
    consumers,
    flow,
    egresses,
    profiles,
    pools,
    action,
    busy,
    message,
    failed,
  };
}

/** The first of a run of values that serialize the same. */
export function useSameWhileUnchanged<T>(value: T): T {
  const key = JSON.stringify(value);
  const held = useRef({ key, value });
  if (held.current.key !== key) {
    held.current = { key, value };
  }
  return held.current.value;
}

/** The addresses an egress is leaving from right now, or would. */
export function liveAddresses(egress: Egress, data: NetworkingData): string[] {
  return (
    egress.addresses ??
    data.discoverNetworkInterfaces
      .filter(
        (candidate) =>
          egress.bindingKind === "SYSTEM" ||
          candidate.name === egress.interfaceName ||
          candidate.addresses.includes(egress.sourceAddress ?? " "),
      )
      .flatMap((candidate) => candidate.addresses)
  );
}

export function egressInput(egress: Egress) {
  return {
    name: egress.name,
    bindingKind: egress.bindingKind,
    interfaceName: egress.bindingKind === "INTERFACE" ? egress.interfaceName : null,
    sourceAddress: egress.bindingKind === "SOURCE_ADDRESS" ? egress.sourceAddress : null,
    enabled: egress.enabled,
    downloadBps: egress.downloadBps,
    uploadBps: egress.uploadBps,
  };
}

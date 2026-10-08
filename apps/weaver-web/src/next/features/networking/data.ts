import { useState } from "react";
import { useClient, useQuery, useSubscription } from "urql";
import type { DocumentNode } from "graphql";
import { NETWORK_FLOW_QUERY, NETWORK_FLOW_SUBSCRIPTION, NETWORKING_QUERY } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import {
  allocation,
  directLeg,
  type Egress,
  type NetworkFlow,
  type NetworkRoute,
  type ProxyPool,
} from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";

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

/** A server or feed: something that owns a route. */
export type Consumer = {
  key: string;
  id: number;
  name: string;
  kind: "SERVER" | "RSS";
  cap: number;
  route: NetworkRoute | null;
};

export const NETWORKING_PAGES = ["overview", "egress", "proxies", "routes", "bandwidth"] as const;
export type NetworkingPage = (typeof NETWORKING_PAGES)[number];

/** Run a mutation; resolves to the failure message, or null once it has landed. */
export type NetworkingAction = (document: DocumentNode, variables: Record<string, unknown>) => Promise<string | null>;

const EMPTY_FLOW: NetworkFlow = { legs: [], pools: [] };

export const defaultRoute = (): NetworkRoute => ({ legs: [directLeg()], failover: "REDISTRIBUTE" });

/**
 * Everything the networking pages read, and the one way they write.
 *
 * The live flow is only subscribed to on the pages that draw it. Consumers
 * come from the flow when the daemon has sampled one; before that they are
 * built from the servers and feeds, and their legs are filled in as idle
 * placeholders at the share their weights would give them, so a route that
 * has never carried traffic still shows where it would go.
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

  const action: NetworkingAction = async (document, variables) => {
    setBusy(true);
    setMessage(null);
    try {
      const result = await client.mutation(document, variables).toPromise();
      const failure = result.error ? (result.error.graphQLErrors[0]?.message ?? result.error.message) : null;
      setFailed(failure !== null);
      setMessage(failure ?? t("next.networking.saved"));
      if (failure === null) {
        refresh({ requestPolicy: "network-only" });
        await client.query(NETWORK_FLOW_QUERY, {}, { requestPolicy: "network-only" }).toPromise();
      }
      return failure;
    } catch (caught) {
      const failure = caught instanceof Error ? caught.message : t("next.networking.requestFailed");
      setFailed(true);
      setMessage(failure);
      return failure;
    } finally {
      setBusy(false);
    }
  };

  const measured = subscription.data?.networkFlow ?? initial.data?.networkFlow ?? EMPTY_FLOW;
  const consumers: Consumer[] =
    measured.consumers ??
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
      : []);
  const flow: NetworkFlow = {
    ...measured,
    legs: consumers.flatMap((consumer) => {
      const route = consumer.route ?? defaultRoute();
      const targets = allocation(
        route.legs.map((leg) => leg.weight),
        consumer.cap,
      );
      return route.legs.map(
        (leg, position) =>
          measured.legs.find((sample) => sample.consumer === consumer.key && sample.position === position) ?? {
            ...leg,
            consumer: consumer.key,
            position,
            target: targets[position] ?? 0,
            open: 0,
            opening: 0,
            state: "IDLE",
            reason: null,
            pinnedAddress: null,
          },
      );
    }),
  };
  const egresses = measured.egresses ?? data?.egressInterfaces ?? [];

  return {
    data,
    error,
    fetching,
    retry: () => refresh({ requestPolicy: "network-only" }),
    liveError: subscription.error,
    measured,
    consumers,
    flow,
    egresses,
    action,
    busy,
    message,
    failed,
  };
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
    maxDownloadSpeed: egress.maxDownloadSpeed,
  };
}

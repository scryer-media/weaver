// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback } from "react";
import { useClient } from "urql";
import { ROUTE_NAMES_QUERY } from "@/graphql/dns";
import { useTranslate } from "@/lib/context/translate-context";
import { OUTBOUND_CONSUMER, TORRENT_DEFAULT_CONSUMER } from "@/lib/networking";
import {
  refusalMessage,
  refusalNamesSomething,
  type FailedRequest,
} from "@/lib/refusal";
import { consumerName } from "./data";

type RouteNames = {
  networkFlow: { consumers: { key: string; name: string }[] | null } | null;
  servers: { id: number; host: string }[];
  rssFeeds: { id: number; name: string }[];
  proxyProfiles: { id: number; name: string }[];
};

/**
 * The sentence for a refusal, on a page that does not hold the routes or
 * proxies it may name. A refusal names them by key and id; their names are
 * read only when one does.
 */
export function useNamedRefusal(): (failed: FailedRequest) => Promise<string> {
  const t = useTranslate();
  const client = useClient();
  return useCallback(
    async (failed: FailedRequest) => {
      if (!refusalNamesSomething(failed)) return refusalMessage(t, failed);
      const { data: names } = await client
        .query<RouteNames>(
          ROUTE_NAMES_QUERY,
          {},
          { requestPolicy: "network-only" },
        )
        .toPromise();
      const routes = new Map<string, string>([
        ...(names?.servers ?? []).map(
          (server) => [`server:${server.id}`, server.host] as const,
        ),
        ...(names?.rssFeeds ?? []).map(
          (feed) => [`rss:${feed.id}`, feed.name] as const,
        ),
        ...(names?.networkFlow?.consumers ?? []).map(
          (consumer) => [consumer.key, consumer.name] as const,
        ),
      ]);
      const proxies = new Map(
        (names?.proxyProfiles ?? []).map(
          (proxy) => [proxy.id, proxy.name] as const,
        ),
      );
      return refusalMessage(t, failed, {
        route: (key) => {
          const name = routes.get(key);
          if (name !== undefined) return consumerName(t, key, name);
          // Default outbound and the torrent default are named by key; any other route with no name is gone.
          return key === OUTBOUND_CONSUMER || key === TORRENT_DEFAULT_CONSUMER
            ? consumerName(t, key, key)
            : t("next.networking.deletedRoute");
        },
        proxy: (id) => proxies.get(id),
      });
    },
    [client, t],
  );
}

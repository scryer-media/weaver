// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useEffect, useMemo } from "react";
import { useQuery } from "urql";
import { FLOW_DNS_QUERY } from "@/graphql/dns";
import type {
  DnsProfileChoice,
  DnsProfileName,
  FlowDnsList,
  FlowDnsProfile,
} from "@/lib/dns";
import { useNextData } from "@/next/data/next-data";

// The saved profiles are held by NextDataProvider, read once for every editor
// that offers them and again after a profile is saved or deleted. Each read
// asks the daemon for one kind, so it holds System and that kind alone.

/** The profiles a route can follow: System, then the saved route profiles. */
export function useRouteDnsProfiles(): DnsProfileChoice[] | undefined {
  const { dnsRouteProfiles, wantDnsProfiles } = useNextData(
    "dnsRouteProfiles",
    "wantDnsProfiles",
  );
  useEffect(() => {
    wantDnsProfiles("ROUTE");
  }, [wantDnsProfiles]);
  return dnsRouteProfiles;
}

/** The profiles a proxy can be found with: System, then the saved proxy name profiles. Not read while `enabled` is false. */
export function useProxyNameProfiles(
  enabled = true,
): DnsProfileName[] | undefined {
  const { dnsProfileNames, wantDnsProfiles } = useNextData(
    "dnsProfileNames",
    "wantDnsProfiles",
  );
  useEffect(() => {
    if (enabled) {
      wantDnsProfiles("PROXY_NAME");
    }
  }, [enabled, wantDnsProfiles]);
  return dnsProfileNames;
}

/**
 * What the flow draws each leg's lookups with: the route profiles and the
 * server lists they ask. Not read while `pause` is true; `refresh` reads again
 * after a DNS edit made from the flow.
 */
export function useFlowDns(pause: boolean): {
  dns: { profiles: FlowDnsProfile[]; lists: FlowDnsList[] } | undefined;
  refresh: () => void;
} {
  const [read, reread] = useQuery<{
    dnsServerLists: FlowDnsList[];
    dnsProfiles: FlowDnsProfile[];
  }>({
    query: FLOW_DNS_QUERY,
    pause,
  });
  const dns = useMemo(
    () =>
      read.data
        ? { profiles: read.data.dnsProfiles, lists: read.data.dnsServerLists }
        : undefined,
    [read.data],
  );
  const refresh = useCallback(
    () => reread({ requestPolicy: "network-only" }),
    [reread],
  );
  return { dns, refresh };
}

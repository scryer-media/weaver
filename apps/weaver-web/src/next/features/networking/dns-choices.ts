// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useMemo } from "react";
import { profileChoices, type DnsProfileChoice, type DnsProfileName } from "@/lib/dns";
import { useNextData } from "@/next/data/next-data";

// The saved profiles are held by NextDataProvider, read once for every editor
// that offers them and again after a profile is saved or deleted.

/** The profiles a route can follow: System, then the saved route profiles. */
export function useRouteDnsProfiles(): DnsProfileChoice[] | undefined {
  const { dnsRouteProfiles: all, wantDnsProfiles } = useNextData("dnsRouteProfiles", "wantDnsProfiles");
  useEffect(() => {
    wantDnsProfiles("ROUTE");
  }, [wantDnsProfiles]);
  return useMemo(() => (all ? profileChoices(all, "ROUTE") : undefined), [all]);
}

/** The profiles a proxy can be found with: System, then the saved proxy name profiles. Not read while `enabled` is false. */
export function useProxyNameProfiles(enabled = true): DnsProfileName[] | undefined {
  const { dnsProfileNames: all, wantDnsProfiles } = useNextData("dnsProfileNames", "wantDnsProfiles");
  useEffect(() => {
    if (enabled) {
      wantDnsProfiles("PROXY_NAME");
    }
  }, [enabled, wantDnsProfiles]);
  return useMemo(() => (all ? profileChoices(all, "PROXY_NAME") : undefined), [all]);
}

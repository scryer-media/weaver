// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect } from "react";
import { useQuery } from "urql";
import { USAGE_HISTORY_QUERY } from "@/graphql/queries";
import type { UsageHistory, UsagePeriod } from "../../data/dashboards";

/** How often an open dashboard reads the ledger again; it is written every five seconds. */
const USAGE_REFRESH_MS = 30_000;

export interface UsageRangeInput {
  period: UsagePeriod;
  fromDay?: string;
  toDay?: string;
}

/** The data-used ledger over `range`, read again while the dashboard is open. */
export function useUsageHistory(range: UsageRangeInput | null, kind?: "SERVER" | "CATEGORY") {
  const [{ data, fetching, error }, reexecute] = useQuery<{ usageHistory: UsageHistory }>({
    query: USAGE_HISTORY_QUERY,
    variables: { range, kind },
    pause: range === null,
  });
  useEffect(() => {
    const timer = window.setInterval(() => {
      reexecute({ requestPolicy: "network-only" });
    }, USAGE_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [reexecute]);
  return {
    usage: data?.usageHistory ?? null,
    isLoading: fetching && data === undefined,
    error: error?.message ?? null,
  };
}

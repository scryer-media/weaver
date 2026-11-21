// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect } from "react";
import { useQuery } from "urql";
import { INTEGRITY_HISTORY_QUERY, TORRENT_HISTORY_QUERY, USAGE_HISTORY_QUERY } from "@/graphql/queries";
import type { IntegrityHistory, TorrentHistory, UsageHistory, UsagePeriod } from "../../data/dashboards";

/** How often an open dashboard reads the ledger again; it is written every five seconds. */
const USAGE_REFRESH_MS = 30_000;

export interface UsageRangeInput {
  period: UsagePeriod;
  fromDay?: string;
  toDay?: string;
}

/** Read again while the dashboard is open. */
function useRefreshed(reexecute: (opts: { requestPolicy: "network-only" }) => void) {
  useEffect(() => {
    const timer = window.setInterval(() => {
      reexecute({ requestPolicy: "network-only" });
    }, USAGE_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [reexecute]);
}

/** The data-used ledger over `range`, read again while the dashboard is open. */
export function useUsageHistory(range: UsageRangeInput | null, kind?: "SERVER" | "CATEGORY") {
  const [{ data, fetching, error }, reexecute] = useQuery<{ usageHistory: UsageHistory }>({
    query: USAGE_HISTORY_QUERY,
    variables: { range, kind },
    pause: range === null,
  });
  useRefreshed(reexecute);
  return {
    usage: data?.usageHistory ?? null,
    isLoading: fetching && data === undefined,
    error: error?.message ?? null,
  };
}

/** Jobs, repairs, stage times, recovery and failures over `range`. */
export function useIntegrityHistory(range: UsageRangeInput | null) {
  const [{ data, fetching, error }, reexecute] = useQuery<{ integrityHistory: IntegrityHistory }>({
    query: INTEGRITY_HISTORY_QUERY,
    variables: { range },
    pause: range === null,
  });
  useRefreshed(reexecute);
  return {
    integrity: data?.integrityHistory ?? null,
    isLoading: fetching && data === undefined,
    error: error?.message ?? null,
  };
}

/** Torrent data, seeding and trackers over `range`, and the peers now. */
export function useTorrentHistory(range: UsageRangeInput | null) {
  const [{ data, fetching, error }, reexecute] = useQuery<{ torrentHistory: TorrentHistory }>({
    query: TORRENT_HISTORY_QUERY,
    variables: { range },
    pause: range === null,
  });
  useRefreshed(reexecute);
  return {
    torrents: data?.torrentHistory ?? null,
    isLoading: fetching && data === undefined,
    error: error?.message ?? null,
  };
}

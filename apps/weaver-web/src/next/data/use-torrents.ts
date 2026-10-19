// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useMemo } from "react";
import { useQuery } from "urql";
import { TORRENT_SUMMARIES_QUERY } from "@/graphql/queries";
import type { TorrentSummaryData } from "@/lib/torrents";

const NO_TORRENTS: ReadonlyMap<number, TorrentSummaryData> = new Map();
/** Ratio and upload rate move on their own, with nothing in the queue to say so. */
const REFRESH_MS = 3000;

/**
 * Each torrent's seeding figures and force flag, by job id, read while the
 * queue holds a torrent and read again as they move.
 */
export function useTorrentSummaries(active: boolean): ReadonlyMap<number, TorrentSummaryData> {
  const [{ data }, refresh] = useQuery<{ torrents: TorrentSummaryData[] }>({
    query: TORRENT_SUMMARIES_QUERY,
    pause: !active,
    requestPolicy: "network-only",
  });
  useEffect(() => {
    if (!active) return;
    const timer = window.setInterval(() => refresh({ requestPolicy: "network-only" }), REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [active, refresh]);
  return useMemo(
    () => (active && data ? new Map(data.torrents.map((torrent) => [torrent.id, torrent])) : NO_TORRENTS),
    [active, data],
  );
}

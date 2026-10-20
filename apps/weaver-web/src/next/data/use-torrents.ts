// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo } from "react";
import { useSubscription } from "urql";
import { TORRENT_SUMMARY_UPDATES_SUBSCRIPTION } from "@/graphql/queries";
import type { TorrentSummaryData } from "@/lib/torrents";
import { useNextData } from "@/next/data/next-data";

const NO_TORRENTS: ReadonlyMap<number, TorrentSummaryData> = new Map();

/**
 * Each torrent's seeding figures and force flag, by job id, followed while
 * the queue holds a torrent.
 */
export function useTorrentSummaries(active: boolean): ReadonlyMap<number, TorrentSummaryData> {
  const { connection } = useNextData("connection");
  const [{ data }] = useSubscription<{ torrentUpdates: TorrentSummaryData[] }>({
    query: TORRENT_SUMMARY_UPDATES_SUBSCRIPTION,
    pause: !active || connection.isDisconnected,
  });
  return useMemo(
    () => (active && data ? new Map(data.torrentUpdates.map((torrent) => [torrent.id, torrent])) : NO_TORRENTS),
    [active, data],
  );
}

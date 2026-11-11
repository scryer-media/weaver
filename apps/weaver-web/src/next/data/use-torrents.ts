// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo } from "react";
import { useSubscription } from "urql";
import { TORRENT_SUMMARY_UPDATES_SUBSCRIPTION } from "@/graphql/queries";
import { usePageVisible } from "@/lib/page-visible";
import { applyTorrentChanges, type TorrentChangesData, type TorrentSummaryData } from "@/lib/torrents";
import { useNextData } from "@/next/data/next-data";

const NO_TORRENTS: ReadonlyMap<number, TorrentSummaryData> = new Map();

/**
 * The seeding figures and force flag of the torrents `ids` names, by job
 * id: the torrents a page shows, followed while it shows any and the tab
 * is visible. Shown again, the tab reads them afresh.
 */
export function useTorrentSummaries(ids: readonly number[]): ReadonlyMap<number, TorrentSummaryData> {
  const { connection } = useNextData("connection");
  const visible = usePageVisible();
  // The same torrents in another order are the same follow.
  const key = [...new Set(ids)].sort((a, b) => a - b).join(",");
  const variables = useMemo(() => ({ ids: key === "" ? [] : key.split(",").map(Number) }), [key]);
  const active = key !== "";
  // Each push carries only what moved, so the torrents are kept between them.
  const [{ data }] = useSubscription<
    { torrentUpdates: TorrentChangesData<TorrentSummaryData> },
    ReadonlyMap<number, TorrentSummaryData>
  >(
    {
      query: TORRENT_SUMMARY_UPDATES_SUBSCRIPTION,
      variables,
      pause: !active || !visible || connection.isDisconnected,
    },
    (held, pushed) => applyTorrentChanges(held, pushed.torrentUpdates),
  );
  return useMemo(() => (active && data ? data : NO_TORRENTS), [active, data]);
}

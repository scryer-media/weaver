// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect } from "react";
import { useQuery } from "urql";
import { SCHEDULE_SEEDING_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { TorrentSummaryData } from "@/lib/torrents";

interface ScheduleSeeding {
  hostPattern: string;
  torrents: number;
}

/**
 * How many torrents the schedule's pause leaves seeding, and under which
 * tracker rules. Read again whenever the torrents move; draws nothing while
 * none are left seeding.
 */
export function ScheduleSeedingNote({ torrents }: { torrents: ReadonlyMap<number, TorrentSummaryData> }) {
  const t = useTranslate();
  const [{ data }, reexecute] = useQuery<{ scheduleSeeding: ScheduleSeeding[] }>({
    query: SCHEDULE_SEEDING_QUERY,
    pause: torrents.size === 0,
  });
  useEffect(() => {
    if (torrents.size > 0) {
      reexecute({ requestPolicy: "network-only" });
    }
  }, [torrents, reexecute]);
  const seeding = torrents.size > 0 ? (data?.scheduleSeeding ?? []) : [];
  if (seeding.length === 0) {
    return null;
  }
  return (
    <span className="font-wv-mono text-[11.5px] text-wv-muted">
      {seeding
        .map((rule) => t("next.downloads.scheduleSeeding", { count: rule.torrents, rule: rule.hostPattern }))
        .join(" · ")}
    </span>
  );
}

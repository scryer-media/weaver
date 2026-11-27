// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useRef } from "react";
import { useQuery } from "urql";
import { SCHEDULE_SEEDING_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { TorrentSummaryData } from "@/lib/torrents";
import { useTorrentsAvailable } from "@/next/data/use-torrents-shown";
import { rereadOnChange } from "@/next/features/torrents/reread";

interface ScheduleSeeding {
  ruleId: number;
  name: string;
  torrents: number;
}

/**
 * How many torrents the schedule's pause leaves seeding, and under which
 * tracker rules. Read again whenever the torrents move; draws nothing while
 * none are left seeding. Asked only while the server has torrents: without
 * them it refuses the question.
 */
export function ScheduleSeedingNote({
  torrents,
}: {
  torrents: ReadonlyMap<number, TorrentSummaryData>;
}) {
  const t = useTranslate();
  const available = useTorrentsAvailable();
  const asked = available && torrents.size > 0;
  // Asked afresh each time the question starts; while it runs, asked again
  // only when the torrents move.
  const [{ data }, reexecute] = useQuery<{
    torrentScheduleSeeding: ScheduleSeeding[];
  }>({
    query: SCHEDULE_SEEDING_QUERY,
    pause: !asked,
    requestPolicy: "network-only",
  });
  const askedFor = useRef(asked ? torrents : null);
  useEffect(() => {
    const before = askedFor.current;
    const now = asked ? torrents : null;
    askedFor.current = now;
    if (rereadOnChange(before, now)) {
      reexecute({ requestPolicy: "network-only" });
    }
  }, [asked, torrents, reexecute]);
  const seeding = asked ? (data?.torrentScheduleSeeding ?? []) : [];
  if (seeding.length === 0) {
    return null;
  }
  return (
    <span className="font-wv-mono text-[11.5px] text-wv-muted">
      {seeding
        .map((rule) =>
          t("next.downloads.scheduleSeeding", {
            count: rule.torrents,
            rule: rule.name,
          }),
        )
        .join(" · ")}
    </span>
  );
}

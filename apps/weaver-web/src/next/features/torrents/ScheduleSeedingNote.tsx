// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect } from "react";
import { useQuery } from "urql";
import { SCHEDULE_SEEDING_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { TorrentSummaryData } from "@/lib/torrents";
import { useTorrentsAvailable } from "@/next/data/use-torrents-shown";

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
  const [{ data }, reexecute] = useQuery<{
    torrentScheduleSeeding: ScheduleSeeding[];
  }>({
    query: SCHEDULE_SEEDING_QUERY,
    pause: !asked,
  });
  useEffect(() => {
    if (asked) {
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

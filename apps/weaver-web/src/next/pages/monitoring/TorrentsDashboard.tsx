// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useTranslate } from "@/lib/context/translate-context";
import { EmptyState, MetricCell, SectionHeader } from "../../components/chrome";
import { columnStyle } from "../../components/columns";
import { useNow } from "../../data/clock";
import { noTorrents, shareRatio, trackersInOrder, type UsageTotals } from "../../data/dashboards";
import { EM_DASH, formatCount, formatDuration, formatSize } from "../../data/format";
import { NextShell } from "../../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../../shell/rail-blocks";
import { DailyBars } from "./DailyBars";
import { DashboardNav } from "./dashboards";
import { UsageRangeBar, useUsageRange } from "./RangeBar";
import { useTorrentHistory } from "./usage";

const COLUMNS = "minmax(180px, 2fr) repeat(4, minmax(76px, 1fr))";

const cell = "truncate text-right font-wv-mono text-[12.5px] text-wv-secondary";

function ratioText(totals: UsageTotals | undefined): string {
  const ratio = totals ? shareRatio(totals) : null;
  return ratio === null ? EM_DASH : ratio.toFixed(2);
}

export function TorrentsDashboard() {
  const t = useTranslate();
  const now = useNow(60_000);
  const rangeState = useUsageRange(now);
  const { torrents, isLoading, error } = useTorrentHistory(rangeState.range);

  const range = torrents?.total.range;
  const days = torrents?.total.days ?? [];
  const peers = torrents?.peers ?? { torrents: 0, connected: 0, seeds: 0 };
  const trackers = trackersInOrder(torrents?.trackers ?? []);
  const empty = torrents !== null && noTorrents(torrents);

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={<DashboardNav active="torrents" />}
      statusRight={error ?? undefined}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <UsageRangeBar state={rangeState} rangeStart={torrents?.rangeStart} rangeEnd={torrents?.rangeEnd} />

        {empty || torrents === null ? (
          <EmptyState
            centered
            testId="torrents-none"
            title={t("next.torrentDash.none")}
            body={t("next.torrentDash.noneBody")}
            loading={isLoading}
          />
        ) : (
          <>
            <div
              className="wv-cols grid flex-none border-b border-wv-hairline bg-wv-app"
              style={columnStyle({ base: "repeat(1, minmax(0, 1fr))", sm: "repeat(3, minmax(0, 1fr))" })}
            >
              <MetricCell
                eyebrow={t("next.torrentDash.ratio")}
                value={ratioText(range)}
                note={t("next.torrentDash.upDown", {
                  up: formatSize(range?.bytesUploaded ?? 0),
                  down: formatSize(range?.bytes ?? 0),
                })}
              />
              <MetricCell
                eyebrow={t("next.torrentDash.seedTime")}
                value={formatDuration((range?.durationMs ?? 0) / 1000)}
                note={t("next.torrentDash.allTime", {
                  time: formatDuration(torrents.total.allTime.durationMs / 1000),
                })}
              />
              <MetricCell
                last
                eyebrow={t("next.torrentDash.peers")}
                value={formatCount(peers.connected)}
                note={t("next.torrentDash.peersNote", {
                  seeds: formatCount(peers.seeds),
                  torrents: formatCount(peers.torrents),
                })}
              />
            </div>

            <section className="flex flex-none flex-col">
              <SectionHeader label={t("next.torrentDash.uploadedPerDay")} sticky={false} />
              <DailyBars
                days={days.map((day) => day.day)}
                values={days.map((day) => day.bytesUploaded)}
                format={formatSize}
                label={t("next.torrentDash.uploadedPerDay")}
              />
            </section>

            <section className="flex flex-none flex-col">
              <SectionHeader label={t("next.torrentDash.trackers")} note={t("next.torrentDash.trackersNote")} sticky={false} />
              <div className="overflow-x-auto">
                <div className="min-w-[560px]">
                  <div
                    className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-2 text-[10.5px] uppercase tracking-[0.12em] text-wv-faint sm:px-6"
                    style={{ gridTemplateColumns: COLUMNS }}
                  >
                    <span>{t("next.torrentDash.tracker")}</span>
                    <span className="text-right">{t("next.torrentDash.downloaded")}</span>
                    <span className="text-right">{t("next.torrentDash.uploaded")}</span>
                    <span className="text-right">{t("next.torrentDash.ratio")}</span>
                    <span className="text-right">{t("next.torrentDash.seedTime")}</span>
                  </div>
                  {trackers.map((tracker) => (
                    <div
                      key={tracker.id}
                      data-testid={`torrents-tracker-${tracker.id || "none"}`}
                      className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-3 hover:bg-wv-cell-hover sm:px-6"
                      style={{ gridTemplateColumns: COLUMNS }}
                    >
                      <span
                        className={`truncate font-wv-mono text-[12.5px] ${tracker.id === "" ? "text-wv-muted" : "text-wv-fg"}`}
                      >
                        {tracker.id === "" ? t("next.torrentDash.noTracker") : tracker.id}
                      </span>
                      <span className={cell}>{formatSize(tracker.range.bytes)}</span>
                      <span className={cell}>{formatSize(tracker.range.bytesUploaded)}</span>
                      <span className={cell}>{ratioText(tracker.range)}</span>
                      <span className={cell}>{formatDuration(tracker.range.durationMs / 1000)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </section>
          </>
        )}
      </div>
    </NextShell>
  );
}

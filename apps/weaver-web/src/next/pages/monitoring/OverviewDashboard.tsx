// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo } from "react";
import { Link } from "react-router";
import { useQuery } from "urql";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import { MetricCell, SectionHeader, Square } from "../../components/chrome";
import { columnStyle } from "../../components/columns";
import { StorageMounts, type StorageVolume } from "../../components/storage";
import { useNow, useStartOfToday } from "../../data/clock";
import { NO_FACETS } from "../../data/categories";
import { downloadsState, lastDays, providersUp, sumDays } from "../../data/dashboards";
import {
  EM_DASH,
  formatClock,
  formatCount,
  formatDayClock,
  formatRate,
  formatSize,
  splitBytes,
  splitSpeed,
} from "../../data/format";
import { useNextData } from "../../data/next-data";
import { WV } from "../../data/palette";
import { queuePageInput, queueSelection } from "../../data/queue-page";
import { useQueuePage } from "../../data/use-live-queue";
import { countLabel } from "../../i18n/labels";
import { NextShell } from "../../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../../shell/rail-blocks";
import { DailyBars } from "./DailyBars";
import { DashboardNav } from "./dashboards";
import { useUsageHistory } from "./usage";

// The tile needs the queue's totals, not its rows.
const QUEUE_TOTALS_INPUT = queuePageInput(
  queueSelection({ tab: "all", facets: NO_FACETS, query: "" }),
  "priority",
  0,
  1,
);

/** The tightest limit in force: the one set by hand or the one a schedule sets. */
function speedLimitInForce(manual: number | null, scheduled: number | null): number | null {
  const limits = [manual, scheduled].filter((limit): limit is number => limit != null && limit > 0);
  return limits.length === 0 ? null : Math.min(...limits);
}

function downloadsWord(t: Translate, state: ReturnType<typeof downloadsState>["state"], egressName: string) {
  switch (state) {
    case "paused":
      return t("status.paused");
    case "egressQuota":
      return t("next.attention.egressQuotaReached", { name: egressName });
    case "serverQuota":
      return t("next.attention.quotaReached");
    case "schedule":
      return t("next.attention.scheduleHold");
    default:
      return t("next.overview.running");
  }
}

export function OverviewDashboard() {
  const t = useTranslate();
  const { speed, peakSpeed, speedLimit, downloadBlock, isPaused, queue, providers } = useNextData(
    "speed",
    "peakSpeed",
    "speedLimit",
    "downloadBlock",
    "isPaused",
    "queue",
    "providers",
  );
  const midnight = useStartOfToday();
  const range = useMemo(() => ({ period: "CUSTOM" as const, ...lastDays(midnight, 30) }), [midnight]);
  const { usage, error } = useUsageHistory(range, "SERVER");
  const page = useQueuePage(QUEUE_TOTALS_INPUT);
  const [{ data: systemInfo }] = useQuery<{
    systemInfo: { configuredStorage: StorageVolume[] };
  }>({ query: SYSTEM_INFO_QUERY });
  const counting = providers.some((provider) => provider.activityUntilEpochMs != null);
  const now = useNow(1000, counting);

  const etaSample = useMemo(
    () =>
      page.etaTotalBytes > page.etaDownloadedBytes
        ? [{ id: 0, status: "DOWNLOADING", totalBytes: page.etaTotalBytes, downloadedBytes: page.etaDownloadedBytes }]
        : [],
    [page.etaDownloadedBytes, page.etaTotalBytes],
  );
  const etaSpeed = useStableEtaSpeed(etaSample, speed);
  const remaining = page.remainingBytes;

  const speedNow = splitSpeed(speed);
  const peak = splitSpeed(peakSpeed);
  const limit = speedLimitInForce(speedLimit, downloadBlock.scheduledSpeedLimit);
  const total = usage?.total ?? null;
  const today = splitBytes(total?.today.bytes ?? 0);
  const finished = usage?.finishedToday ?? { complete: 0, partial: 0, failed: 0 };
  const finishedCount = finished.complete + finished.partial + finished.failed;
  const up = providersUp(providers, t, now);
  const block = downloadsState(isPaused, downloadBlock);
  const days = total?.days ?? [];
  const last30 = sumDays(days);

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={<DashboardNav active="overview" />}
      statusRight={error ?? undefined}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <div
          className="wv-cols grid flex-none border-b border-wv-hairline bg-wv-app"
          style={columnStyle({
            base: "repeat(2, minmax(0, 1fr))",
            sm: "repeat(3, minmax(0, 1fr))",
          })}
        >
          <MetricCell
            eyebrow={t("next.overview.speed")}
            value={speedNow.value}
            unit={speedNow.unit}
            note={[
              peakSpeed > 0 ? t("next.monitoring.peak", { value: peak.value, unit: peak.unit }) : t("next.monitoring.noTraffic"),
              limit === null ? null : t("next.overview.limit", { rate: formatRate(limit) }),
            ]
              .filter(Boolean)
              .join(" · ")}
          />
          <MetricCell
            eyebrow={t("next.overview.queue")}
            value={remaining > 0 ? formatEtaFromRemainingBytes(remaining, etaSpeed) : EM_DASH}
            note={
              queue.summary.totalItems > 0
                ? countLabel(t, "next.overview.jobs", queue.summary.totalItems, { size: formatSize(remaining) })
                : t("next.overview.queueEmpty")
            }
          />
          <MetricCell
            last
            eyebrow={t("next.overview.finishedToday")}
            value={formatCount(finishedCount)}
            note={
              usage ? (
                <Link
                  to={`/history?since=${usage.todayStartEpochSec}`}
                  className="hover:text-wv-accent"
                >
                  {t("next.overview.finished", { ...finished })}
                </Link>
              ) : (
                t("next.overview.finished", { ...finished })
              )
            }
          />
          <MetricCell
            eyebrow={t("next.overview.dataToday")}
            value={today.value}
            unit={today.unit}
            note={t("next.overview.dataPeriods", {
              week: formatSize(total?.week.bytes ?? 0),
              month: formatSize(total?.month.bytes ?? 0),
              all: formatSize(total?.allTime.bytes ?? 0),
            })}
          />
          <MetricCell
            eyebrow={t("next.rail.providers")}
            value={formatCount(up.up)}
            unit={`/ ${up.total}`}
            valueClassName={up.down.length > 0 ? "text-wv-warn" : undefined}
            note={
              up.down.length === 0 ? (
                t("next.overview.allUp")
              ) : (
                <span className="flex flex-col gap-1">
                  {up.down.map((line) => (
                    <span key={line.key} className="flex items-center gap-[7px]">
                      <Square color={WV.warn} />
                      <span className="truncate">
                        {line.host} · {line.reason}
                      </span>
                    </span>
                  ))}
                </span>
              )
            }
          />
          <MetricCell
            last
            eyebrow={t("next.overview.downloads")}
            value={downloadsWord(t, block.state, downloadBlock.egressName ?? "")}
            valueClassName={block.state === "running" ? "text-wv-accent" : "text-wv-warn"}
            note={
              block.state === "running"
                ? EM_DASH
                : block.liftsAtEpochMs != null
                  ? t("next.attention.resumes", {
                      time:
                        block.state === "egressQuota"
                          ? formatDayClock(block.liftsAtEpochMs)
                          : formatClock(block.liftsAtEpochMs),
                    })
                  : t("next.overview.untilResumed")
            }
          />
        </div>

        <section className="flex flex-none flex-col">
          <SectionHeader label={t("next.overview.last30")} note={formatSize(last30.bytes)} sticky={false} />
          <DailyBars
            days={days.map((day) => day.day)}
            values={days.map((day) => day.bytes)}
            format={formatSize}
            label={t("next.overview.last30")}
          />
        </section>

        <section className="flex flex-none flex-col border-t border-wv-hairline">
          <StorageMounts
            volumes={systemInfo?.systemInfo?.configuredStorage ?? []}
            className="px-4 py-4 sm:px-6"
          />
        </section>
      </div>
    </NextShell>
  );
}

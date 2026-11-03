// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { SectionHeader, Square } from "../../components/chrome";
import { Chart, type ChartSeries } from "../../components/Chart";
import { Segmented } from "../../components/controls";
import { useNow } from "../../data/clock";
import {
  averageDailySpeed,
  dayKey,
  foundPercent,
  providerRows,
  providerUp,
  USAGE_KEPT_DAYS,
  type ProviderRow,
  type UsagePeriod,
} from "../../data/dashboards";
import {
  EM_DASH,
  formatClock,
  formatDayClock,
  formatDuration,
  formatLatency,
  formatRate,
  formatSize,
  startOfDay,
} from "../../data/format";
import { useNextData } from "../../data/next-data";
import { WV } from "../../data/palette";
import { providerStateLabel } from "../../i18n/labels";
import { NextShell } from "../../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../../shell/rail-blocks";
import { DashboardNav } from "./dashboards";
import { useUsageHistory, type UsageRangeInput } from "./usage";

const PERIODS: readonly UsagePeriod[] = ["TODAY", "WEEK", "MONTH", "YEAR", "ALL", "CUSTOM"];

// One colour per server line, in row order, wrapping after the last.
const SERIES_COLORS = [WV.accent, WV.info, WV.violet, WV.gold, WV.green, WV.warn, WV.error, WV.slate];

const COLUMNS =
  "minmax(180px, 2fr) repeat(5, minmax(76px, 1fr)) minmax(64px, 0.8fr) minmax(76px, 0.9fr) minmax(64px, 0.8fr) minmax(130px, 1.4fr) minmax(64px, 0.8fr)";

const cell = "truncate text-right font-wv-mono text-[12.5px] text-wv-secondary";

function rowName(row: ProviderRow, removed: (id: string) => string): string {
  return row.provider?.host ?? removed(row.serverId ?? "");
}

export function ProvidersDashboard() {
  const t = useTranslate();
  const { providers } = useNextData("providers");
  const now = useNow(60_000);
  const today = dayKey(now);
  const [period, setPeriod] = useState<UsagePeriod>("MONTH");
  const [fromDay, setFromDay] = useState(() => dayKey(now - 29 * 86_400_000));
  const [toDay, setToDay] = useState(today);
  // Daily rows go back only as far as the server keeps them.
  const earliestDay = dayKey(now - (USAGE_KEPT_DAYS - 1) * 86_400_000);

  const range = useMemo<UsageRangeInput | null>(() => {
    if (period !== "CUSTOM") return { period };
    // Wait for both dates before asking; a half-typed range is not a range.
    if (!fromDay || !toDay) return null;
    return { period, fromDay: fromDay < earliestDay ? earliestDay : fromDay, toDay };
  }, [period, fromDay, toDay, earliestDay]);
  const { usage, isLoading, error } = useUsageHistory(range, "SERVER");

  const rows = useMemo(
    () => providerRows(providers, usage?.consumers ?? []),
    [providers, usage?.consumers],
  );
  const removed = (id: string) => t("next.providers.removed", { id });

  const secondsIntoToday = Math.max(0, (now - startOfDay(now)) / 1000);
  const series: ChartSeries[] = rows
    .filter((row) => row.usage !== null)
    .map((row, index) => {
      const values = averageDailySpeed(row.usage!.days, usage?.today ?? today, secondsIntoToday);
      const last = values[values.length - 1] ?? 0;
      return {
        key: row.key,
        label: rowName(row, removed),
        color: SERIES_COLORS[index % SERIES_COLORS.length]!,
        values,
        value: formatRate(last),
      };
    });
  const chartDays = rows.find((row) => row.usage !== null)?.usage?.days ?? [];
  const xLabels = [
    chartDays[0]?.day ?? "",
    chartDays[Math.floor(chartDays.length / 2)]?.day ?? "",
    chartDays[chartDays.length - 1]?.day ?? "",
  ];

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={<DashboardNav active="providers" />}
      statusRight={error ?? undefined}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <div className="flex flex-none flex-wrap items-center gap-3 border-b border-wv-hairline bg-wv-app px-4 py-3 sm:px-6">
          <Segmented
            label={t("next.usage.range")}
            value={period}
            onChange={setPeriod}
            options={PERIODS.map((value) => ({
              value,
              label: t(`next.usage.period.${value.toLowerCase()}`),
            }))}
          />
          {period === "CUSTOM" ? (
            <div className="flex items-center gap-2 font-wv-mono text-[12.5px] text-wv-secondary">
              <input
                type="date"
                aria-label={t("next.usage.from")}
                value={fromDay}
                min={earliestDay}
                max={toDay || today}
                onChange={(event) => setFromDay(event.target.value)}
                className="border border-wv-control bg-wv-input px-2 py-1"
              />
              <span aria-hidden>–</span>
              <input
                type="date"
                aria-label={t("next.usage.to")}
                value={toDay}
                min={fromDay || earliestDay}
                max={today}
                onChange={(event) => setToDay(event.target.value)}
                className="border border-wv-control bg-wv-input px-2 py-1"
              />
            </div>
          ) : null}
          {usage ? (
            <span className="ml-auto font-wv-mono text-[11.5px] text-wv-muted">
              {usage.rangeStart === usage.rangeEnd
                ? usage.rangeStart
                : `${usage.rangeStart} – ${usage.rangeEnd}`}
            </span>
          ) : null}
        </div>

        <section className="flex flex-none flex-col">
          <SectionHeader label={t("next.rail.providers")} sticky={false} />
          <div className="overflow-x-auto">
            <div className="min-w-[1040px]">
              <div
                className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-2 text-[10.5px] uppercase tracking-[0.12em] text-wv-faint sm:px-6"
                style={{ gridTemplateColumns: COLUMNS }}
              >
                <span>{t("next.providers.server")}</span>
                <span className="text-right">{t("next.usage.period.today")}</span>
                <span className="text-right">{t("next.usage.period.week")}</span>
                <span className="text-right">{t("next.usage.period.month")}</span>
                <span className="text-right">{t("next.usage.allTime")}</span>
                <span className="text-right">{t("next.usage.inRange")}</span>
                <span className="text-right">{t("next.providers.found")}</span>
                <span className="text-right">{t("next.monitoring.roundTrip")}</span>
                <span className="text-right">{t("next.providers.inUse")}</span>
                <span className="text-right">{t("next.providers.state")}</span>
                <span className="text-right">{t("next.providers.down")}</span>
              </div>
              {rows.map((row) => {
                const provider = row.provider;
                const totals = row.usage;
                const up = provider ? providerUp(provider) : false;
                const found = totals ? foundPercent(totals.range) : null;
                const since = provider?.stateSinceEpochMs ?? null;
                const sinceText =
                  since === null
                    ? null
                    : t("next.providers.since", {
                        time: since >= startOfDay(now) ? formatClock(since) : formatDayClock(since),
                      });
                return (
                  <div
                    key={row.key}
                    className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-3 hover:bg-wv-cell-hover sm:px-6"
                    style={{ gridTemplateColumns: COLUMNS }}
                  >
                    <span className="flex min-w-0 items-center gap-[9px]">
                      <Square color={provider === null ? WV.muted : up ? WV.accent : WV.warn} />
                      <span
                        className={`truncate font-wv-mono text-[12.5px] ${provider === null ? "text-wv-muted" : "text-wv-fg"}`}
                      >
                        {rowName(row, removed)}
                      </span>
                    </span>
                    <span className={cell}>{totals ? formatSize(totals.today.bytes) : EM_DASH}</span>
                    <span className={cell}>{totals ? formatSize(totals.week.bytes) : EM_DASH}</span>
                    <span className={cell}>{totals ? formatSize(totals.month.bytes) : EM_DASH}</span>
                    <span className={cell}>{totals ? formatSize(totals.allTime.bytes) : EM_DASH}</span>
                    <span className={`${cell} text-wv-fg`}>
                      {totals ? formatSize(totals.range.bytes) : EM_DASH}
                    </span>
                    <span className={cell}>{found === null ? EM_DASH : `${found.toFixed(1)}%`}</span>
                    <span className={cell}>{provider ? formatLatency(provider.latencyMs) : EM_DASH}</span>
                    <span className={cell}>
                      {provider
                        ? `${provider.connectionsActive} / ${provider.connectionsConfigured}`
                        : EM_DASH}
                    </span>
                    <span
                      className={`${cell} ${provider && !up ? "text-wv-warn" : ""}`}
                      title={sinceText ?? undefined}
                    >
                      {provider ? providerStateLabel(t, provider.state) : EM_DASH}
                      {sinceText ? (
                        <span className="block text-[11px] text-wv-muted">{sinceText}</span>
                      ) : null}
                    </span>
                    <span className={cell}>
                      {totals ? formatDuration(totals.range.downSeconds) : EM_DASH}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </section>

        <section className="flex flex-none flex-col">
          <Chart
            title={t("next.providers.speed")}
            series={series}
            xLabels={xLabels}
            formatValue={formatRate}
            loading={isLoading}
          />
        </section>
      </div>
    </NextShell>
  );
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { Segmented } from "../../components/controls";
import { dayKey, USAGE_KEPT_DAYS, type UsagePeriod } from "../../data/dashboards";
import type { UsageRangeInput } from "./usage";

const PERIODS: readonly UsagePeriod[] = ["TODAY", "WEEK", "MONTH", "YEAR", "ALL", "CUSTOM"];

export interface UsageRangeState {
  period: UsagePeriod;
  setPeriod: (period: UsagePeriod) => void;
  fromDay: string;
  setFromDay: (day: string) => void;
  toDay: string;
  setToDay: (day: string) => void;
  today: string;
  earliestDay: string;
  /** What to ask the server for; `null` while a custom range is half typed. */
  range: UsageRangeInput | null;
}

/** The range a dashboard reads, this month to start. */
export function useUsageRange(now: number): UsageRangeState {
  const today = dayKey(now);
  const [period, setPeriod] = useState<UsagePeriod>("MONTH");
  const [fromDay, setFromDay] = useState(() => dayKey(now - 29 * 86_400_000));
  const [toDay, setToDay] = useState(today);
  // Daily rows go back only as far as the server keeps them.
  const earliestDay = dayKey(now - (USAGE_KEPT_DAYS - 1) * 86_400_000);
  const range = useMemo<UsageRangeInput | null>(() => {
    if (period !== "CUSTOM") return { period };
    if (!fromDay || !toDay) return null;
    return { period, fromDay: fromDay < earliestDay ? earliestDay : fromDay, toDay };
  }, [period, fromDay, toDay, earliestDay]);
  return { period, setPeriod, fromDay, setFromDay, toDay, setToDay, today, earliestDay, range };
}

/** The period picker, the custom dates, and the dates the server read. */
export function UsageRangeBar({
  state,
  rangeStart,
  rangeEnd,
}: {
  state: UsageRangeState;
  rangeStart?: string;
  rangeEnd?: string;
}) {
  const t = useTranslate();
  const { period, setPeriod, fromDay, setFromDay, toDay, setToDay, today, earliestDay } = state;
  return (
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
      {rangeStart && rangeEnd ? (
        <span className="ml-auto font-wv-mono text-[11.5px] text-wv-muted">
          {rangeStart === rangeEnd ? rangeStart : `${rangeStart} – ${rangeEnd}`}
        </span>
      ) : null}
    </div>
  );
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo } from "react";
import { barHeights } from "../../data/dashboards";
import { WV } from "../../data/palette";

/** One measure drawn per day: its name, its colour, and a value for each day. */
export type DailySeries = {
  label: string;
  color: string;
  values: readonly number[];
};

/**
 * One slot per day, oldest first, with a bar for each series side by side.
 * Every day of the range keeps its slot, so a quiet day reads as a gap, not
 * as a bar that is missing. The tallest value is written on the top line, so
 * a bar reads against it, and each bar is titled with its date and value.
 */
export const DailyBars = memo(function DailyBars({
  days,
  series,
  format,
  label,
}: {
  days: readonly string[];
  series: readonly DailySeries[];
  format: (value: number) => string;
  label: string;
}) {
  // Every series on one scale: each day's values in series order, then
  // each series' run of days, read back by series and day.
  const all = series.flatMap((one) =>
    days.map((_, index) => one.values[index] ?? 0),
  );
  const heights = barHeights(all);
  const tallest = all.reduce((max, value) => Math.max(max, value), 0);
  const middle = days.length > 2 ? Math.floor((days.length - 1) / 2) : -1;
  return (
    <div className="flex flex-col gap-2 px-4 py-4 sm:px-6">
      <div className="flex items-baseline justify-between gap-4 font-wv-mono text-[10.5px] text-wv-faint">
        <span>{tallest > 0 ? format(tallest) : ""}</span>
        {series.length > 1 ? (
          <span className="flex gap-4">
            {series.map((one) => (
              <span
                key={one.label}
                className="inline-flex items-center gap-[6px]"
              >
                <span
                  className="inline-block size-[8px]"
                  style={{ backgroundColor: one.color }}
                />
                {one.label}
              </span>
            ))}
          </span>
        ) : null}
      </div>
      <div
        role="img"
        aria-label={label}
        className="flex h-[120px] items-end gap-[3px] border-t border-b border-dashed"
        style={{
          borderTopColor: WV.trackDash,
          borderBottomColor: WV.track,
          borderBottomStyle: "solid",
        }}
      >
        {days.map((day, index) => (
          <div
            key={day}
            className="flex h-full min-w-0 flex-1 items-end gap-px"
            style={{ backgroundColor: WV.gridline }}
          >
            {series.map((one, at) => {
              const value = one.values[index] ?? 0;
              const height = heights[at * days.length + index] ?? 0;
              return (
                <div
                  key={one.label}
                  title={`${day} · ${one.label} ${format(value)}`}
                  className="min-w-0 flex-1"
                  style={{
                    height: `${Math.max(height, value > 0 ? 2 : 0)}%`,
                    backgroundColor: one.color,
                  }}
                />
              );
            })}
          </div>
        ))}
      </div>
      <div className="relative flex justify-between font-wv-mono text-[10.5px] text-wv-faint">
        <span>{days[0] ?? ""}</span>
        {middle > 0 ? (
          <span
            className="absolute -translate-x-1/2"
            style={{ left: `${((middle + 0.5) / days.length) * 100}%` }}
          >
            {days[middle]}
          </span>
        ) : null}
        <span>{days.length > 1 ? days[days.length - 1] : ""}</span>
      </div>
    </div>
  );
});

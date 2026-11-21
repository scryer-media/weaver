// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo } from "react";
import { barHeights } from "../../data/dashboards";
import { WV } from "../../data/palette";

/** One bar per day, oldest first, each titled with its date and value. */
export const DailyBars = memo(function DailyBars({
  days,
  values,
  format,
  label,
}: {
  days: readonly string[];
  values: readonly number[];
  format: (value: number) => string;
  label: string;
}) {
  const heights = barHeights(values);
  return (
    <div className="flex flex-col gap-2 px-4 py-4 sm:px-6">
      <div role="img" aria-label={label} className="flex h-[120px] items-end gap-[3px]">
        {heights.map((height, index) => (
          <div
            key={days[index] ?? index}
            title={`${days[index] ?? ""} · ${format(values[index] ?? 0)}`}
            className="min-w-0 flex-1"
            style={{
              height: `${Math.max(height, values[index]! > 0 ? 2 : 0)}%`,
              backgroundColor: WV.accent,
            }}
          />
        ))}
      </div>
      <div className="flex justify-between font-wv-mono text-[10.5px] text-wv-faint">
        <span>{days[0] ?? ""}</span>
        <span>{days[days.length - 1] ?? ""}</span>
      </div>
    </div>
  );
});

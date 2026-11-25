// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { cn } from "@/lib/utils";

/**
 * A transfer rate drawn as the rail and the queue rows draw it: three places,
 * the unit, then the bright arrow of its direction (`6.7 KB/s ↓`). The figure
 * keeps one width at every value and the unit is always four letters, so a
 * column of them holds still as the figures change.
 */
export function RateCounter({
  arrow,
  rate,
  title,
  as: Tag = "span",
  className,
  testId,
}: {
  arrow: "down" | "up";
  rate: number;
  title: string;
  /** `dd` inside a definition list, as the rail's are. */
  as?: "span" | "dd";
  className?: string;
  testId?: string;
}) {
  const { value, unit } = fixedSpeed(rate);
  return (
    <Tag
      className={cn("m-0 inline-flex items-baseline justify-end", className)}
      title={title}
      aria-label={title}
      data-testid={testId}
    >
      <span
        className={cn(
          "inline-block w-[3ch] text-right font-wv-mono text-[12px] font-semibold tabular-nums leading-none",
          rate > 0 ? "text-wv-fg" : "text-wv-muted",
        )}
      >
        {value}
      </span>
      <span className="ml-[2px] font-wv-mono text-[9.5px] text-wv-muted">
        {unit}
      </span>
      <span
        aria-hidden
        className="ml-[4px] font-wv-mono text-[12px] font-semibold leading-none text-wv-fg"
      >
        {arrow === "down" ? "↓" : "↑"}
      </span>
    </Tag>
  );
}

/**
 * A rate in three places and its unit: one decimal below ten (`6.7`), whole
 * above (`58`, `999`). The scale starts at KB/s, so every unit is four letters
 * wide and nothing shifts as it climbs.
 */
export function fixedSpeed(bytesPerSecond: number): {
  value: string;
  unit: string;
} {
  const units = ["KB/s", "MB/s", "GB/s", "TB/s"];
  let scaled =
    Number.isFinite(bytesPerSecond) && bytesPerSecond > 0
      ? bytesPerSecond / 1024
      : 0;
  let index = 0;
  // Past what rounds to 999, the next unit, so the value never needs a fourth place.
  while (scaled >= 999.5 && index < units.length - 1) {
    scaled /= 1024;
    index += 1;
  }
  return {
    value: scaled < 9.95 ? scaled.toFixed(1) : Math.round(scaled).toFixed(0),
    unit: units[index],
  };
}

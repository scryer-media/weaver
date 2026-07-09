import type { ReactNode } from "react";
import { Sparkline } from "@/components/ui/sparkline";
import { cn } from "@/lib/utils";

interface StatTileProps {
  label: ReactNode;
  value: ReactNode;
  unit?: ReactNode;
  /** Color override for the value (e.g. text-status-completed). */
  valueClassName?: string;
  description?: ReactNode;
  /** Optional trailing sparkline. */
  trend?: number[];
  /** Color class for the sparkline (text-*). Defaults to the download accent. */
  trendClassName?: string;
  size?: "sm" | "md";
  className?: string;
}

/**
 * Metric card: uppercase label, large Space Grotesk value, optional unit /
 * description / sparkline. Shared across Monitoring, Job Detail, and the Queue
 * header speed cards.
 */
export function StatTile({
  label,
  value,
  unit,
  valueClassName,
  description,
  trend,
  trendClassName = "text-status-completed",
  size = "md",
  className,
}: StatTileProps) {
  return (
    <div
      className={cn(
        "rounded-card border border-border bg-card",
        size === "sm" ? "p-4" : "p-5",
        className,
      )}
    >
      <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2.5 flex items-baseline gap-1.5">
        <span
          className={cn(
            "font-space-grotesk font-bold leading-none tracking-tight",
            size === "sm" ? "text-lg" : "text-[26px]",
            valueClassName ?? "text-foreground",
          )}
        >
          {value}
        </span>
        {unit ? <span className="text-xs font-medium text-muted-foreground">{unit}</span> : null}
      </div>
      {description ? (
        <div className="mt-2 text-xs leading-5 text-muted-foreground">{description}</div>
      ) : null}
      {trend && trend.length > 0 ? (
        <Sparkline values={trend} className={cn("mt-3", trendClassName)} height={26} />
      ) : null}
    </div>
  );
}

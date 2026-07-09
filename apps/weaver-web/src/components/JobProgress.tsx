import type { ReactNode } from "react";
import { Progress } from "@/components/ui/progress";
import { getDisplayedJobProgress } from "@/lib/job-progress";
import { progressDisplayKind, statusBgClass } from "@/lib/status-tokens";
import { cn } from "@/lib/utils";

export function JobProgress({
  progress,
  status,
  totalBytes,
  downloadedBytes,
  failedBytes,
  showLabel = true,
  compact = false,
  label,
}: {
  progress: number;
  status?: string;
  totalBytes?: number;
  downloadedBytes?: number;
  failedBytes?: number;
  showLabel?: boolean;
  compact?: boolean;
  /** Override the derived label (e.g. "Extracting 42%"). */
  label?: ReactNode;
}) {
  const fraction = getDisplayedJobProgress({
    progress,
    status,
    totalBytes,
    downloadedBytes,
    failedBytes,
  });
  const kind = progressDisplayKind(status, fraction);
  const pct = fraction * 100;
  const barColor = statusBgClass(status);
  const height = compact ? "h-1.5" : "h-2";
  const derivedLabel =
    kind === "empty" ? "—" : kind === "indeterminate" ? "…" : `${pct.toFixed(compact ? 0 : 1)}%`;

  return (
    <div className={cn("flex items-center gap-3", compact && "gap-2")}>
      {kind === "indeterminate" ? (
        <div className={cn("relative w-full overflow-hidden rounded-pill bg-secondary", height)}>
          <div className={cn("progress-indeterminate absolute inset-0 rounded-pill opacity-90", barColor)} />
        </div>
      ) : (
        <Progress
          value={kind === "empty" ? 0 : pct}
          className={cn("rounded-pill bg-secondary", height)}
          indicatorClassName={cn("rounded-pill", barColor)}
        />
      )}
      {showLabel ? (
        <span
          className={cn(
            "text-right text-xs tabular-nums text-muted-foreground",
            compact ? "w-10 text-[10px]" : "w-12",
          )}
        >
          {label ?? derivedLabel}
        </span>
      ) : null}
    </div>
  );
}

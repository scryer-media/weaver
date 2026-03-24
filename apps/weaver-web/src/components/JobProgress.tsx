import { cn } from "@/lib/utils";

function statusColor(status?: string): string {
  switch (status) {
    case "VERIFYING":
      return "bg-amber-500";
    case "REPAIRING":
      return "bg-orange-500";
    case "EXTRACTING":
      return "bg-violet-500";
    case "COMPLETE":
      return "bg-emerald-500";
    case "FAILED":
      return "bg-red-500";
    case "PAUSED":
      return "bg-slate-500";
    default:
      return "bg-primary";
  }
}

export function JobProgress({
  progress,
  status,
  health,
  failedPct,
  showLabel = true,
  compact = false,
}: {
  progress: number;
  status?: string;
  health?: number;
  /** Failed bytes as a fraction of total (0..1) */
  failedPct?: number;
  showLabel?: boolean;
  compact?: boolean;
}) {
  const goodPct = Math.min(100, Math.max(0, progress * 100));
  const badPct = Math.min(100, Math.max(0, (failedPct ?? 0) * 100));
  const isDownloading = status === "DOWNLOADING" || status === "QUEUED";
  const hasFailed = badPct > 0 && isDownloading;

  return (
    <div className={cn("flex items-center gap-3", compact && "gap-2")}>
      <div
        className={cn(
          "relative h-2.5 w-full overflow-hidden rounded-full bg-muted",
          compact && "h-1.5",
        )}
      >
        {/* Good progress (downloaded) */}
        <div
          className={cn("absolute inset-y-0 left-0", statusColor(status))}
          style={{ width: `${goodPct}%` }}
        />
        {/* Failed segments (stacked after good) */}
        {hasFailed ? (
          <div
            className="absolute inset-y-0 bg-red-500"
            style={{ left: `${goodPct}%`, width: `${Math.min(badPct, 100 - goodPct)}%` }}
          />
        ) : null}
      </div>
      {showLabel ? (
        <span
          className={cn(
            "w-12 text-right text-xs tabular-nums text-muted-foreground",
            compact && "w-10 text-[10px]",
          )}
        >
          {goodPct.toFixed(1)}%
        </span>
      ) : null}
    </div>
  );
}

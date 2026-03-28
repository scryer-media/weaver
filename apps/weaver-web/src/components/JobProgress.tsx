import { Progress } from "@/components/ui/progress";
import { getDisplayedJobProgress } from "@/lib/job-progress";
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
  totalBytes,
  downloadedBytes,
  failedBytes,
  showLabel = true,
  compact = false,
}: {
  progress: number;
  status?: string;
  totalBytes?: number;
  downloadedBytes?: number;
  failedBytes?: number;
  showLabel?: boolean;
  compact?: boolean;
}) {
  const displayedProgress =
    getDisplayedJobProgress({ progress, status, totalBytes, downloadedBytes, failedBytes }) * 100;

  return (
    <div className={cn("flex items-center gap-3", compact && "gap-2")}>
      <Progress
        value={displayedProgress}
        className={cn(compact && "h-1.5")}
        indicatorClassName={statusColor(status)}
      />
      {showLabel ? (
        <span
          className={cn(
            "w-12 text-right text-xs tabular-nums text-muted-foreground",
            compact && "w-10 text-[10px]",
          )}
        >
          {displayedProgress.toFixed(1)}%
        </span>
      ) : null}
    </div>
  );
}

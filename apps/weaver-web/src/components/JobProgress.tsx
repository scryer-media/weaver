import { Progress } from "@/components/ui/progress";
import { cn } from "@/lib/utils";

function indicatorClassName(status?: string): string {
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
  showLabel = true,
  compact = false,
}: {
  progress: number;
  status?: string;
  showLabel?: boolean;
  compact?: boolean;
}) {
  const pct = Math.min(100, Math.max(0, progress * 100));

  return (
    <div className={cn("flex items-center gap-3", compact && "gap-2")}>
      <Progress
        value={pct}
        indicatorClassName={indicatorClassName(status)}
        className={cn("flex-1", compact && "h-1.5")}
      />
      {showLabel ? (
        <span
          className={cn(
            "w-12 text-right text-xs tabular-nums text-muted-foreground",
            compact && "w-10 text-[10px]",
          )}
        >
          {pct.toFixed(1)}%
        </span>
      ) : null}
    </div>
  );
}

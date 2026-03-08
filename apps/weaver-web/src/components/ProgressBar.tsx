interface ProgressBarProps {
  progress: number;
  status?: string;
  showLabel?: boolean;
}

function statusColor(status?: string): string {
  switch (status) {
    case "DOWNLOADING":
      return "bg-blue-500";
    case "VERIFYING":
      return "bg-yellow-500";
    case "REPAIRING":
      return "bg-orange-500";
    case "EXTRACTING":
      return "bg-purple-500";
    case "COMPLETE":
      return "bg-green-500";
    case "FAILED":
      return "bg-red-500";
    case "PAUSED":
      return "bg-gray-500";
    default:
      return "bg-blue-500";
  }
}

export function ProgressBar({ progress, status, showLabel = true }: ProgressBarProps) {
  const pct = Math.min(100, Math.max(0, progress * 100));

  return (
    <div className="flex items-center gap-3">
      <div className="h-2 flex-1 rounded-full bg-muted">
        <div
          className={`h-full rounded-full transition-all duration-300 ${statusColor(status)}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      {showLabel && (
        <span className="w-12 text-right text-xs text-muted-foreground">
          {pct.toFixed(1)}%
        </span>
      )}
    </div>
  );
}

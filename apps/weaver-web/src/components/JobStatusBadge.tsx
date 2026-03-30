import type { ComponentProps } from "react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const STATUS_VARIANTS: Record<string, ComponentProps<typeof Badge>["variant"]> = {
  QUEUED: "muted",
  DOWNLOADING: "info",
  CHECKING: "info",
  VERIFYING: "warning",
  QUEUED_REPAIR: "muted",
  REPAIRING: "warning",
  QUEUED_EXTRACT: "muted",
  EXTRACTING: "secondary",
  MOVING: "secondary",
  COMPLETE: "success",
  COMPLETED: "success",
  FAILED: "destructive",
  PAUSED: "outline",
  CANCELLED: "outline",
  FINALIZING: "secondary",
};

const STATUS_LABELS: Record<string, string> = {
  QUEUED: "Queued",
  DOWNLOADING: "Downloading",
  CHECKING: "Checking",
  VERIFYING: "Verifying",
  QUEUED_REPAIR: "Queued",
  REPAIRING: "Repairing",
  QUEUED_EXTRACT: "Queued",
  EXTRACTING: "Extracting",
  MOVING: "Moving",
  COMPLETE: "Complete",
  COMPLETED: "Complete",
  FAILED: "Failed",
  PAUSED: "Paused",
  CANCELLED: "Cancelled",
  FINALIZING: "Finalizing",
};

export function JobStatusBadge({
  status,
  compact = false,
  className,
}: {
  status: string;
  compact?: boolean;
  className?: string;
}) {
  return (
    <Badge
      variant={STATUS_VARIANTS[status] ?? "outline"}
      className={cn(
        "font-medium",
        compact && "px-2 py-0 text-[10px] uppercase tracking-[0.12em]",
        className,
      )}
    >
      {STATUS_LABELS[status] ?? status}
    </Badge>
  );
}

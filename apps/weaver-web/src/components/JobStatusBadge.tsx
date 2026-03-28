import type { ComponentProps } from "react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const STATUS_VARIANTS: Record<string, ComponentProps<typeof Badge>["variant"]> = {
  QUEUED: "muted",
  DOWNLOADING: "info",
  VERIFYING: "warning",
  QUEUED_REPAIR: "muted",
  REPAIRING: "warning",
  QUEUED_EXTRACT: "muted",
  EXTRACTING: "secondary",
  MOVING: "secondary",
  COMPLETE: "success",
  FAILED: "destructive",
  PAUSED: "outline",
  CANCELLED: "outline",
};

const STATUS_LABELS: Record<string, string> = {
  QUEUED: "Queued",
  DOWNLOADING: "Downloading",
  VERIFYING: "Verifying",
  QUEUED_REPAIR: "Queued (Repair)",
  REPAIRING: "Repairing",
  QUEUED_EXTRACT: "Queued (Extract)",
  EXTRACTING: "Extracting",
  MOVING: "Moving",
  COMPLETE: "Complete",
  FAILED: "Failed",
  PAUSED: "Paused",
  CANCELLED: "Cancelled",
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

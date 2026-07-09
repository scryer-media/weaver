import { cn } from "@/lib/utils";
import { useTranslate } from "@/lib/context/translate-context";
import {
  STATUS_SOFT_CLASS,
  STATUS_TEXT_CLASS,
  statusI18nKey,
  statusToken,
} from "@/lib/status-tokens";

interface JobStatusBadgeProps {
  status: string;
  compact?: boolean;
  className?: string;
}

/** A single pipeline-status chip: colored text on a soft tint of the same token. */
export function JobStatusBadge({ status, compact = false, className }: JobStatusBadgeProps) {
  const t = useTranslate();
  const token = statusToken(status);
  return (
    <span
      className={cn(
        "inline-flex items-center justify-center whitespace-nowrap rounded-chip font-bold uppercase tracking-[0.06em]",
        STATUS_TEXT_CLASS[token],
        STATUS_SOFT_CLASS[token],
        compact ? "px-1.5 py-px text-[10px]" : "px-2.5 py-[3px] text-[10.5px]",
        className,
      )}
    >
      {t(statusI18nKey(status))}
    </span>
  );
}

/**
 * Renders one chip per concurrently-active stage. Today `statuses` is usually a
 * single entry; when the backend reports concurrent post-processing stages the
 * dual/triple badges appear automatically (handoff dual-state behavior).
 */
export function JobStatusBadgeGroup({
  statuses,
  compact = false,
  className,
}: {
  statuses: string[];
  compact?: boolean;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-1.5", className)}>
      {statuses.map((status, index) => (
        <JobStatusBadge key={`${status}-${index}`} status={status} compact={compact} />
      ))}
    </div>
  );
}

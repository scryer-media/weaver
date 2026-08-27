import { cn } from "@/lib/utils";
import { useTranslate } from "@/lib/context/translate-context";
import {
  useDebouncedStatus,
  useDebouncedStatuses,
} from "@/lib/hooks/use-debounced-status";
import {
  STATUS_SOFT_CLASS,
  STATUS_TEXT_CLASS,
  statusI18nKey,
  statusToken,
} from "@/lib/status-tokens";

interface JobStatusBadgeProps {
  status: string;
  label?: string;
  compact?: boolean;
  className?: string;
  debounce?: boolean;
}

/** A single pipeline-status chip: colored text on a soft tint of the same token. */
export function JobStatusBadge({
  status,
  label,
  compact = false,
  className,
  debounce = true,
}: JobStatusBadgeProps) {
  const t = useTranslate();
  const visibleStatus = useDebouncedStatus(status, debounce);
  const token = statusToken(visibleStatus);
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
      {label ?? t(statusI18nKey(visibleStatus))}
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
  const visibleStatuses = useDebouncedStatuses(statuses);

  return (
    <div className={cn("flex flex-wrap items-center gap-1.5", className)}>
      {visibleStatuses.map((status, index) => (
        <JobStatusBadge
          key={`${status}-${index}`}
          status={status}
          compact={compact}
          debounce={false}
        />
      ))}
    </div>
  );
}

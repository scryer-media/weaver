import { cn } from "@/lib/utils";
import { PRIORITY_BG_CLASS, priorityToken } from "@/lib/status-tokens";

interface PriorityIndicatorProps {
  priority: string;
  label?: string;
  /** `inline` = dot + label; `chip` = filled pill (used in dense/mobile layouts). */
  variant?: "inline" | "chip";
  className?: string;
}

export function PriorityIndicator({
  priority,
  label,
  variant = "inline",
  className,
}: PriorityIndicatorProps) {
  const token = priorityToken(priority);
  const dot = (
    <span className={cn("size-[7px] shrink-0 rounded-pill", PRIORITY_BG_CLASS[token])} aria-hidden="true" />
  );

  if (variant === "chip") {
    return (
      <span
        className={cn(
          "inline-flex items-center gap-1.5 rounded-chip bg-secondary px-2.5 py-[3px] text-[11px] font-semibold",
          className,
        )}
      >
        {dot}
        {label}
      </span>
    );
  }

  return (
    <span className={cn("inline-flex items-center gap-2 text-[12.5px] font-medium", className)}>
      {dot}
      {label}
    </span>
  );
}

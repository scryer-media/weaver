import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface FilterChipProps {
  label: ReactNode;
  active?: boolean;
  count?: number;
  onClick?: () => void;
  className?: string;
}

/** Quick-filter pill with an optional count badge (Queue / History toolbars). */
export function FilterChip({ label, active = false, count, onClick, className }: FilterChipProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={cn(
        "inline-flex h-8 cursor-pointer items-center gap-2 rounded-[9px] border px-3 text-[13px] font-medium transition-colors",
        active
          ? "border-ring bg-primary/[0.14] text-foreground"
          : "border-border bg-card text-muted-foreground hover:text-foreground",
        className,
      )}
    >
      {label}
      {count != null ? (
        <span
          className={cn(
            "rounded-md bg-secondary px-1.5 py-px text-[11px] font-bold tabular-nums",
            active ? "text-primary" : "text-muted-foreground",
          )}
        >
          {count}
        </span>
      ) : null}
    </button>
  );
}

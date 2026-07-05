import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export interface SegmentedOption<T extends string> {
  value: T;
  label?: ReactNode;
  icon?: ReactNode;
  title?: string;
  disabled?: boolean;
}

interface SegmentedControlProps<T extends string> {
  value: T;
  onValueChange: (value: T) => void;
  options: SegmentedOption<T>[];
  size?: "sm" | "md";
  className?: string;
  ariaLabel?: string;
}

/**
 * Radix-free single-select toggle group. Backs the Queue layout toggle, the Logs
 * level filter, and the Monitoring time-window selector. Active segment fills with
 * `--primary`; inactive segments are muted with a hover affordance.
 */
export function SegmentedControl<T extends string>({
  value,
  onValueChange,
  options,
  size = "md",
  className,
  ariaLabel,
}: SegmentedControlProps<T>) {
  return (
    <div
      role="tablist"
      aria-label={ariaLabel}
      className={cn(
        "inline-flex items-center gap-0.5 rounded-[10px] border border-border bg-card p-1",
        className,
      )}
    >
      {options.map((option) => {
        const active = option.value === value;
        const iconOnly = Boolean(option.icon) && !option.label;
        return (
          <button
            key={option.value}
            type="button"
            role="tab"
            aria-selected={active}
            aria-label={option.title}
            title={option.title}
            disabled={option.disabled}
            onClick={() => onValueChange(option.value)}
            className={cn(
              "inline-flex cursor-pointer items-center justify-center gap-2 rounded-md font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
              size === "sm" ? "h-7 text-xs" : "h-8 text-[13px]",
              iconOnly ? (size === "sm" ? "w-7" : "w-8") : size === "sm" ? "px-2.5" : "px-3.5",
              active
                ? "bg-primary text-primary-foreground shadow-sm"
                : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
            )}
          >
            {option.icon}
            {option.label}
          </button>
        );
      })}
    </div>
  );
}

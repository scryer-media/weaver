import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Icon, type IconName } from "./icons";

/**
 * The 42px bar that appears once rows are ticked.
 *
 * It takes the accent-tinted ground so a selection is unmistakable, and it
 * only ever exists while something is selected — there is no disabled version
 * of it sitting above an untouched list.
 */
export function BulkBar({
  count,
  onClear,
  children,
  className,
}: {
  count: number;
  onClear: () => void;
  /** The actions themselves, as `BulkButton`s. */
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex min-h-[42px] flex-none flex-wrap items-center gap-x-[14px] gap-y-1 border-b border-wv-bulk-line bg-wv-bulk px-4 py-1 sm:px-6 sm:py-0",
        className,
      )}
    >
      <SelectedCount count={count} />
      <div className="ml-auto flex items-center gap-2">
        {children}
        <ClearButton onClear={onClear} />
      </div>
    </div>
  );
}

/**
 * The same count, actions and Clear as a `BulkBar`, inline: for a bar that
 * already exists — the tab row — so selecting rows does not push the list down.
 */
export function BulkCluster({
  count,
  onClear,
  children,
}: {
  count: number;
  onClear: () => void;
  children: ReactNode;
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="pr-[6px]">
        <SelectedCount count={count} />
      </span>
      {children}
      <ClearButton onClear={onClear} />
    </div>
  );
}

function SelectedCount({ count }: { count: number }) {
  return (
    <span className="font-wv-mono text-[11.5px] whitespace-nowrap text-wv-accent">
      {count} selected
    </span>
  );
}

function ClearButton({ onClear }: { onClear: () => void }) {
  return (
    <button
      type="button"
      onClick={onClear}
      className="flex h-[26px] cursor-pointer items-center px-[11px] text-[12.5px] whitespace-nowrap text-wv-muted hover:text-wv-fg"
    >
      Clear
    </button>
  );
}

/** One 26px action inside a `BulkBar`. `tone="danger"` for anything that removes. */
export function BulkButton({
  children,
  icon,
  onClick,
  disabled,
  tone = "default",
}: {
  children: ReactNode;
  /** A leading icon, named by the action. */
  icon?: IconName;
  onClick: () => void;
  disabled?: boolean;
  tone?: "default" | "danger";
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex h-[26px] cursor-pointer items-center border px-[11px] text-[12.5px] whitespace-nowrap",
        tone === "danger"
          ? "border-wv-danger-border bg-wv-danger-bg text-wv-error-text hover:bg-wv-danger-bg-hover"
          : "border-wv-ok-line bg-wv-ok-bg text-wv-secondary hover:bg-wv-ok-hover",
        disabled && "cursor-default opacity-50 hover:bg-transparent",
      )}
    >
      {icon === undefined ? null : <Icon name={icon} size={13} className="-ml-[1px] mr-[6px] flex-none" />}
      {children}
    </button>
  );
}

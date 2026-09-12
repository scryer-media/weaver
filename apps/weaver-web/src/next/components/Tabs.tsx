import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export interface TabDefinition<T extends string> {
  id: T;
  label: string;
  /** Rendered 6px after the label in mono. Omit for tabs that have no count. */
  count?: number;
}

/**
 * The 40px filter bar: tabs on the left, an arbitrary control cluster on the
 * right. The active tab is marked by a 2px underline inset to the 39px row, so
 * the bar's own bottom hairline still reads as continuous.
 */
export function Tabs<T extends string>({
  tabs,
  active,
  onSelect,
  right,
  className,
}: {
  tabs: readonly TabDefinition<T>[];
  active: T;
  onSelect: (id: T) => void;
  right?: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex h-10 flex-none items-center border-b border-wv-hairline bg-wv-app px-4 sm:px-6",
        className,
      )}
    >
      <div className="wv-xscroll flex min-w-0 items-center gap-[14px] sm:gap-[18px]">
        {tabs.map((tab) => {
          const isActive = tab.id === active;
          return (
            <button
              key={tab.id}
              type="button"
              onClick={() => onSelect(tab.id)}
              aria-pressed={isActive}
              className={cn(
                "relative h-[39px] flex-none text-[13px] whitespace-nowrap",
                isActive
                  ? "font-semibold text-wv-strong"
                  : "text-wv-muted hover:text-wv-secondary",
              )}
            >
              {tab.label}
              {tab.count === undefined ? null : (
                <span className="ml-[6px] font-wv-mono text-[11.5px] font-normal text-wv-faint">
                  {tab.count}
                </span>
              )}
              {isActive ? (
                <span className="absolute inset-x-0 bottom-0 h-[2px] bg-wv-fg" />
              ) : null}
            </button>
          );
        })}
      </div>
      {right === undefined ? null : (
        <div className="ml-auto flex flex-none items-center gap-[10px] pl-3">{right}</div>
      )}
    </div>
  );
}

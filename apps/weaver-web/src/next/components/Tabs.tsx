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
 *
 * `center` puts a third cluster in the middle of the bar — a selection's
 * actions, say — without adding a row that would push the list down. It only
 * shows from the `xl` width, where it fits between the tabs and the right-hand
 * controls; the caller covers narrower screens itself. The side columns never
 * shrink below their contents, so a crowded bar moves the middle over rather
 * than letting it overlap the tabs.
 */
export function Tabs<T extends string>({
  tabs,
  active,
  onSelect,
  right,
  center,
  className,
}: {
  tabs: readonly TabDefinition<T>[];
  active: T;
  onSelect: (id: T) => void;
  right?: ReactNode;
  center?: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex h-10 flex-none items-center border-b border-wv-hairline bg-wv-app px-4 sm:px-6",
        center !== undefined &&
          "xl:grid xl:grid-cols-[minmax(max-content,1fr)_auto_minmax(max-content,1fr)] xl:gap-x-4",
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
                  ? "font-medium text-wv-strong"
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
      {center === undefined ? null : (
        <div className="hidden items-center gap-2 xl:flex">{center}</div>
      )}
      {right === undefined && center === undefined ? null : (
        <div className="ml-auto flex flex-none items-center gap-[10px] pl-3">{right}</div>
      )}
    </div>
  );
}

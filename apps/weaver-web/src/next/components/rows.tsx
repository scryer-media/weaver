import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { COLS_CLASS, columnStyle, type Columns } from "./columns";
import { Eyebrow } from "./chrome";

/**
 * The two row shapes the whole UI is built from.
 *
 * `ListRow` is every hairline-separated row that pairs a text block with a
 * right-hand cluster of values — queue rows, provider rows, failure counters,
 * storage volumes, history entries. `FormRow` is the settings equivalent: a
 * label and help text on the left, exactly one control on the right.
 *
 * Both wrap rather than truncate their clusters: the text block carries a
 * `flex-basis`, so the values drop to a second line before a host or a path is
 * cut short.
 */

export function ListRow({
  left,
  right,
  selected = false,
  onClick,
  density = "comfortable",
  /** Draw the 3px leading rail that marks selection. Queue rows use it. */
  markSelection = false,
  className,
  title,
}: {
  left: ReactNode;
  right?: ReactNode;
  selected?: boolean;
  onClick?: () => void;
  density?: "comfortable" | "compact";
  markSelection?: boolean;
  className?: string;
  title?: string;
}) {
  const interactive = typeof onClick === "function";
  const Element = interactive ? "button" : "div";

  return (
    <Element
      {...(interactive
        ? { type: "button" as const, onClick, "aria-pressed": selected }
        : {})}
      title={title}
      className={cn(
        "flex w-full flex-wrap items-center gap-x-5 gap-y-[10px] border-b border-wv-hairline text-left",
        markSelection
          ? cn(
              "border-l-[3px] pr-6 pl-[21px]",
              selected ? "border-l-wv-accent" : "border-l-transparent",
            )
          : "px-4 sm:px-6",
        density === "compact" ? "py-2" : "py-[13px]",
        selected ? "bg-wv-selected" : interactive ? "hover:bg-wv-row-hover" : "hover:bg-wv-cell-hover",
        interactive && "cursor-pointer",
        className,
      )}
    >
      {left}
      {right === undefined ? null : (
        <div className="ml-auto flex flex-none items-center gap-5">{right}</div>
      )}
    </Element>
  );
}

/** Right-aligned mono value inside a `ListRow` cluster. */
export function ValueCell({
  children,
  width = 62,
  className,
}: {
  children: ReactNode;
  width?: number;
  className?: string;
}) {
  return (
    <div
      style={{ minWidth: width }}
      className={cn(
        "whitespace-nowrap text-right font-wv-mono text-[12.5px] text-wv-secondary",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function FormRow({
  label,
  help,
  children,
  htmlFor,
}: {
  label: ReactNode;
  help?: ReactNode;
  children: ReactNode;
  htmlFor?: string;
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-3 border-b border-wv-hairline px-4 sm:px-6 py-[14px] hover:bg-wv-cell-hover">
      <div className="flex min-w-0 flex-[1_1_300px] flex-col gap-1">
        <label
          htmlFor={htmlFor}
          className="text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg"
        >
          {label}
        </label>
        {help === undefined ? null : (
          <div className="text-[12px] leading-[1.45] text-pretty text-wv-muted">{help}</div>
        )}
      </div>
      <div className="ml-auto flex w-full min-w-0 flex-none items-center gap-3 sm:w-auto sm:min-w-[260px] sm:justify-end">
        {children}
      </div>
    </div>
  );
}

/**
 * A hairline grid table.
 *
 * `columns` carries the handoff's exact tracks
 * (`minmax(0,1fr) 92px 110px 150px 44px`), optionally as one set per width.
 * Header cells are eyebrows; body cells truncate.
 */
export function DataTable({
  columns,
  headers,
  children,
}: {
  columns: Columns;
  headers: readonly ReactNode[];
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col">
      <div
        className={cn(
          COLS_CLASS,
          "grid items-center gap-5 border-b border-wv-hairline px-4 py-2 sm:px-6",
        )}
        style={columnStyle(columns)}
      >
        {headers.map((header, index) => (
          <Eyebrow key={index} tone="rail" className="truncate">
            {header}
          </Eyebrow>
        ))}
      </div>
      {children}
    </div>
  );
}

export function DataTableRow({
  columns,
  children,
  onClick,
  className,
}: {
  columns: Columns;
  children: ReactNode;
  onClick?: () => void;
  className?: string;
}) {
  return (
    <div
      {...(onClick ? { role: "button", tabIndex: 0, onClick } : {})}
      onKeyDown={
        onClick
          ? (event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onClick();
              }
            }
          : undefined
      }
      className={cn(
        COLS_CLASS,
        "grid items-center gap-5 border-b border-wv-hairline px-4 py-3 text-[13px] text-wv-fg hover:bg-wv-cell-hover sm:px-6",
        onClick && "cursor-pointer",
        className,
      )}
      style={columnStyle(columns)}
    >
      {children}
    </div>
  );
}

/**
 * A column-ruled list: one `columns` template shared by a header strip and
 * every row under it.
 *
 * `DataTable` above is the settings table — comfortable rows, a light header.
 * This pair is the denser one the list screens use, where the columns are
 * fixed tracks (a 26px checkbox, a 64px size) and the rows are grouped,
 * selectable and clickable. Padding and borders stay with the caller: history
 * rows are separated below, a job's file rows above.
 */
export function GridHeader({
  columns,
  cells,
  cellClassNames,
  gap = 12,
  className,
}: {
  columns: Columns;
  cells: readonly ReactNode[];
  /**
   * Per-cell classes, positional with `cells`. A table whose `columns` drop
   * tracks on a narrow screen hides the matching header cells through this —
   * the wrapper below is the grid child, so a class on the cell's own content
   * would leave an empty track behind.
   */
  cellClassNames?: readonly (string | undefined)[];
  gap?: number;
  className?: string;
}) {
  return (
    <div
      className={cn(
        COLS_CLASS,
        "grid h-[28px] flex-none items-center border-b border-wv-line-strong bg-wv-section px-4 sm:px-[22px]",
        className,
      )}
      style={columnStyle(columns, { gap })}
    >
      {cells.map((cell, index) =>
        cell === null || cell === undefined || cell === "" ? (
          <span key={index} className={cellClassNames?.[index]} />
        ) : (
          <Eyebrow
            key={index}
            className={cn(
              "truncate text-[10.5px] tracking-[0.12em] text-wv-note",
              cellClassNames?.[index],
            )}
          >
            {cell}
          </Eyebrow>
        ),
      )}
    </div>
  );
}

export function GridRow({
  columns,
  gap = 12,
  selected = false,
  onClick,
  title,
  className,
  children,
}: {
  columns: Columns;
  gap?: number;
  selected?: boolean;
  onClick?: () => void;
  title?: string;
  className?: string;
  children: ReactNode;
}) {
  const interactive = typeof onClick === "function";
  return (
    <div
      {...(interactive
        ? {
            role: "button",
            tabIndex: 0,
            onClick,
            onKeyDown: (event: React.KeyboardEvent) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onClick();
              }
            },
          }
        : {})}
      title={title}
      className={cn(
        COLS_CLASS,
        "grid min-w-0 items-center",
        interactive && "cursor-pointer hover:bg-wv-row-hover",
        selected && "bg-wv-row-picked",
        className,
      )}
      style={columnStyle(columns, { gap })}
    >
      {children}
    </div>
  );
}

/** A truncating table cell; `mono` for hosts, paths and every machine value. */
export function Cell({
  children,
  mono = false,
  className,
  title,
}: {
  children: ReactNode;
  mono?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <div
      title={title}
      className={cn(
        "min-w-0 truncate",
        mono ? "font-wv-mono text-[12.5px]" : "text-[13px]",
        className,
      )}
    >
      {children}
    </div>
  );
}

import { Fragment, type MouseEvent as ReactMouseEvent, type ReactNode } from "react";
import { flexRender, type Row, type Table as TanstackTable } from "@tanstack/react-table";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

export type DataTableColumnMeta = {
  headerClassName?: string;
  cellClassName?: string;
};

type DataTableProps<TData> = {
  table: TanstackTable<TData>;
  emptyState: ReactNode;
  renderExpandedRow?: (row: Row<TData>) => ReactNode;
  onRowClick?: (row: Row<TData>, event: ReactMouseEvent<HTMLTableRowElement>) => void;
  rowClassName?: (row: Row<TData>) => string | undefined;
  wrapperClassName?: string;
  tableClassName?: string;
  stickyHeader?: boolean;
};

function shouldIgnoreRowClick(target: EventTarget | null) {
  return target instanceof HTMLElement
    && Boolean(
      target.closest(
        "a, button, input, select, textarea, label, summary, [role='button'], [role='checkbox'], [data-row-click-ignore='true']",
      ),
    );
}

export function DataTable<TData>({
  table,
  emptyState,
  renderExpandedRow,
  onRowClick,
  rowClassName,
  wrapperClassName,
  tableClassName,
  stickyHeader = false,
}: DataTableProps<TData>) {
  const rows = table.getRowModel().rows;
  const columnCount = table.getVisibleLeafColumns().length;

  return (
    <Table className={tableClassName} wrapperClassName={wrapperClassName}>
      <TableHeader>
        {table.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id} className="hover:bg-transparent">
            {headerGroup.headers.map((header) => {
              const meta = header.column.columnDef.meta as DataTableColumnMeta | undefined;
              return (
                <TableHead
                  key={header.id}
                  className={cn(stickyHeader && "sticky top-0 z-10 bg-card", meta?.headerClassName)}
                >
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              );
            })}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {rows.length === 0 ? (
          <TableRow className="hover:bg-transparent">
            <TableCell colSpan={columnCount}>{emptyState}</TableCell>
          </TableRow>
        ) : (
          rows.map((row) => (
            <Fragment key={row.id}>
              <TableRow
                data-state={row.getIsSelected() ? "selected" : undefined}
                className={rowClassName?.(row)}
                onClick={(event) => {
                  if (!onRowClick || shouldIgnoreRowClick(event.target)) {
                    return;
                  }
                  onRowClick(row, event);
                }}
              >
                {row.getVisibleCells().map((cell) => {
                  const meta = cell.column.columnDef.meta as DataTableColumnMeta | undefined;
                  return (
                    <TableCell key={cell.id} className={meta?.cellClassName}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  );
                })}
              </TableRow>
              {renderExpandedRow && row.getIsExpanded() ? (
                <TableRow className="bg-accent/10 hover:bg-accent/10">
                  <TableCell colSpan={columnCount}>{renderExpandedRow(row)}</TableCell>
                </TableRow>
              ) : null}
            </Fragment>
          ))
        )}
      </TableBody>
    </Table>
  );
}

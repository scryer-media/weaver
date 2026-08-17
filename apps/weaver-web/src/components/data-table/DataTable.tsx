import {
  Fragment,
  memo,
  useEffect,
  useRef,
  useState,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
} from "react";
import { flexRender, type Row, type Table as TanstackTable } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
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

type DataTableVirtualization = {
  estimatedRowHeight: number;
  overscan?: number;
  resetKey?: string | number;
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
  virtualization?: DataTableVirtualization;
};

function shouldIgnoreRowClick(target: EventTarget | null) {
  return target instanceof HTMLElement
    && Boolean(
      target.closest(
        "a, button, input, select, textarea, label, summary, [role='button'], [role='checkbox'], [data-row-click-ignore='true']",
      ),
    );
}

type VirtualizedDataTableRowProps<TData> = {
  row: Row<TData>;
  rowIndex: number;
  rowClassName?: (row: Row<TData>) => string | undefined;
  onRowClick?: (row: Row<TData>, event: ReactMouseEvent<HTMLTableRowElement>) => void;
  measureElement: (element: HTMLTableRowElement | null) => void;
};

function VirtualizedDataTableRowInner<TData>({
  row,
  rowIndex,
  rowClassName,
  onRowClick,
  measureElement,
}: VirtualizedDataTableRowProps<TData>) {
  return (
    <TableRow
      ref={measureElement}
      data-index={rowIndex}
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
  );
}

const VirtualizedDataTableRow = memo(VirtualizedDataTableRowInner) as typeof VirtualizedDataTableRowInner;

type VirtualizedDataTableBodyProps<TData> = {
  rows: Row<TData>[];
  columnCount: number;
  emptyState: ReactNode;
  scrollElement: HTMLDivElement | null;
  rowClassName?: (row: Row<TData>) => string | undefined;
  onRowClick?: (row: Row<TData>, event: ReactMouseEvent<HTMLTableRowElement>) => void;
  virtualization: DataTableVirtualization;
};

function VirtualizedDataTableBody<TData>({
  rows,
  columnCount,
  emptyState,
  scrollElement,
  rowClassName,
  onRowClick,
  virtualization,
}: VirtualizedDataTableBodyProps<TData>) {
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollElement,
    getItemKey: (index) => rows[index]?.id ?? index,
    estimateSize: () => virtualization.estimatedRowHeight,
    overscan: virtualization.overscan ?? 8,
    useFlushSync: false,
  });
  const previousResetKeyRef = useRef(virtualization.resetKey);

  useEffect(() => {
    if (previousResetKeyRef.current !== virtualization.resetKey) {
      rowVirtualizer.scrollToOffset(0);
      previousResetKeyRef.current = virtualization.resetKey;
    }
  }, [rowVirtualizer, virtualization.resetKey]);

  if (rows.length === 0) {
    return (
      <TableBody>
        <TableRow className="hover:bg-transparent">
          <TableCell colSpan={columnCount}>{emptyState}</TableCell>
        </TableRow>
      </TableBody>
    );
  }

  const virtualItems = rowVirtualizer.getVirtualItems();
  const totalVirtualSize = rowVirtualizer.getTotalSize();
  const firstVirtualItem = virtualItems[0];
  const lastVirtualItem = virtualItems[virtualItems.length - 1];
  const topSpacerHeight = firstVirtualItem?.start ?? 0;
  const bottomSpacerHeight = lastVirtualItem
    ? Math.max(totalVirtualSize - lastVirtualItem.end, 0)
    : 0;

  return (
    <TableBody>
      {topSpacerHeight > 0 ? (
        <TableRow aria-hidden>
          <TableCell colSpan={columnCount} style={{ height: topSpacerHeight, padding: 0 }} />
        </TableRow>
      ) : null}
      {virtualItems.map((virtualRow) => {
        const row = rows[virtualRow.index];
        return row ? (
          <VirtualizedDataTableRow
            key={virtualRow.key}
            row={row}
            rowIndex={virtualRow.index}
            rowClassName={rowClassName}
            onRowClick={onRowClick}
            measureElement={rowVirtualizer.measureElement}
          />
        ) : null;
      })}
      {bottomSpacerHeight > 0 ? (
        <TableRow aria-hidden>
          <TableCell colSpan={columnCount} style={{ height: bottomSpacerHeight, padding: 0 }} />
        </TableRow>
      ) : null}
    </TableBody>
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
  virtualization,
}: DataTableProps<TData>) {
  const rows = table.getRowModel().rows;
  const columnCount = table.getVisibleLeafColumns().length;
  // A callback ref makes the scroll container a piece of React state. Passing
  // only `ref.current` lets TanStack Virtual initialize before the wrapper is
  // mounted; when the first page then arrives, its range can remain empty.
  const [tableScrollElement, setTableScrollElement] = useState<HTMLDivElement | null>(null);

  return (
    <Table
      ref={virtualization ? setTableScrollElement : undefined}
      className={tableClassName}
      wrapperClassName={wrapperClassName}
    >
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
      {virtualization && !renderExpandedRow ? (
        <VirtualizedDataTableBody
          rows={rows}
          columnCount={columnCount}
          emptyState={emptyState}
          scrollElement={tableScrollElement}
          rowClassName={rowClassName}
          onRowClick={onRowClick}
          virtualization={virtualization}
        />
      ) : (
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
      )}
    </Table>
  );
}

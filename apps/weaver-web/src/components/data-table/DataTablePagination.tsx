import type { Table as TanstackTable } from "@tanstack/react-table";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

type DataTablePaginationProps<TData> = {
  table: TanstackTable<TData>;
  totalCount?: number;
  pageSizeOptions?: number[];
  rowsPerPageLabel: string;
  previousLabel: string;
  nextLabel: string;
};

export function DataTablePagination<TData>({
  table,
  totalCount,
  pageSizeOptions = [25, 50, 100, 500],
  rowsPerPageLabel,
  previousLabel,
  nextLabel,
}: DataTablePaginationProps<TData>) {
  const { pageIndex, pageSize } = table.getState().pagination;
  const currentRows = table.getRowModel().rows.length;
  const resolvedTotal = totalCount ?? table.getFilteredRowModel().rows.length;
  const firstRow = resolvedTotal === 0 ? 0 : pageIndex * pageSize + 1;
  const lastRow = resolvedTotal === 0 ? 0 : pageIndex * pageSize + currentRows;

  return (
    <div className="flex flex-col gap-3 border-t border-border/70 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <span>{rowsPerPageLabel}</span>
        <Select
          value={String(pageSize)}
          onValueChange={(value) => {
            table.setPageSize(Number(value));
            table.setPageIndex(0);
          }}
        >
          <SelectTrigger className="h-8 w-[92px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {pageSizeOptions.map((option) => (
              <SelectItem key={option} value={String(option)}>
                {option}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
        <div className="text-sm text-muted-foreground">
          {firstRow}-{lastRow} / {resolvedTotal}
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
          >
            <ChevronLeft className="size-4" />
            {previousLabel}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
          >
            {nextLabel}
            <ChevronRight className="size-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}

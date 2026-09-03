import { startTransition, type ReactNode } from "react";
import { Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

type DataTableToolbarProps = {
  searchValue: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder: string;
  clearLabel?: string;
  onClear?: () => void;
  className?: string;
  centerContent?: ReactNode;
  searchContainerClassName?: string;
  searchInputClassName?: string;
  centerContainerClassName?: string;
  actionsClassName?: string;
  children?: ReactNode;
};

export function DataTableToolbar({
  searchValue,
  onSearchChange,
  searchPlaceholder,
  clearLabel,
  onClear,
  className,
  centerContent,
  searchContainerClassName,
  searchInputClassName,
  centerContainerClassName,
  actionsClassName,
  children,
}: DataTableToolbarProps) {
  return (
    <div
      className={cn(
        "flex flex-col gap-3 min-[560px]:grid min-[560px]:grid-cols-[auto_minmax(0,1fr)] min-[560px]:items-center min-[560px]:gap-4 xl:grid-cols-[auto_minmax(0,1fr)_auto]",
        className,
      )}
    >
      <div
        className={cn("relative w-full min-[560px]:max-w-[260px]", searchContainerClassName)}
      >
        <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          className={cn("h-9 rounded-inner pl-8", searchInputClassName)}
          value={searchValue}
          onChange={(event) => {
            const value = event.target.value;
            startTransition(() => onSearchChange(value));
          }}
          placeholder={searchPlaceholder}
        />
      </div>
      <div
        className={cn(
          "flex items-center justify-center min-[560px]:col-span-2 xl:hidden",
          centerContainerClassName,
        )}
        aria-hidden={centerContent ? undefined : true}
      >
        {centerContent}
      </div>
      <div
        className={cn(
          "hidden min-w-0 items-center justify-center xl:flex",
          centerContainerClassName,
        )}
        aria-hidden={centerContent ? undefined : true}
      >
        {centerContent}
      </div>
      <div
        className={cn(
          "flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-end min-[560px]:col-start-2 min-[560px]:row-start-1 min-[560px]:justify-self-end xl:col-start-auto xl:row-start-auto",
          actionsClassName,
        )}
      >
        {children}
        {onClear && clearLabel ? (
          <Button variant="ghost" onClick={onClear}>
            {clearLabel}
          </Button>
        ) : null}
      </div>
    </div>
  );
}

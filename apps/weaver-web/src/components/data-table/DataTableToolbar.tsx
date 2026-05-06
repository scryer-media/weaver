import { startTransition, type ReactNode } from "react";
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
    <div className={cn("flex flex-col gap-3 lg:grid lg:grid-cols-[auto_minmax(0,1fr)_auto] lg:items-center lg:gap-4", className)}>
      <div className={cn("max-w-md flex-1", searchContainerClassName)}>
        <Input
          className={searchInputClassName}
          value={searchValue}
          onChange={(event) => {
            const value = event.target.value;
            startTransition(() => onSearchChange(value));
          }}
          placeholder={searchPlaceholder}
        />
      </div>
      <div
        className={cn("flex items-center justify-center lg:hidden", centerContainerClassName)}
        aria-hidden={centerContent ? undefined : true}
      >
        {centerContent}
      </div>
      <div
        className={cn(
          "hidden min-w-0 items-center justify-center lg:flex",
          centerContainerClassName,
        )}
        aria-hidden={centerContent ? undefined : true}
      >
        {centerContent}
      </div>
      <div
        className={cn(
          "flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-end lg:justify-self-end",
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

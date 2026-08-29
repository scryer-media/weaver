import { Skeleton } from "@/components/ui/skeleton";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

/** Row widths written as literals so the Tailwind scanner keeps them in the build. */
const SKELETON_ROWS = ["w-3/5", "w-2/5", "w-1/2", "w-4/6", "w-1/3"] as const;

/**
 * Hydration fallback for the lazy route modules (see `router.tsx`).
 *
 * React Router only renders a data router's tree once the matched route modules
 * resolve; without a `HydrateFallback` on the lazy routes it truncates the match
 * chain to the root route and renders `null`, so `#root` stays empty (and a blank
 * page is shown) for the whole initial load. Attaching this to every lazy route
 * lets the real `Layout` shell - sidebar, nav, header - paint immediately while
 * only the outlet region shows a placeholder.
 *
 * Rendered inside `Layout`'s content container, so it mirrors the standard page
 * shape (`PageHeader` + card) to keep the swap to real content shift-free.
 */
export function RouteFallback() {
  const t = useTranslate();

  return (
    <div
      role="status"
      aria-busy="true"
      data-testid="route-hydrate-fallback"
      className="space-y-6"
    >
      <span className="sr-only">{t("label.loading")}</span>

      {/* PageHeader placeholder. */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 space-y-2.5">
          <Skeleton className="h-8 w-52 sm:h-9" />
          <Skeleton className="h-4 w-full max-w-sm" />
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Skeleton className="h-9 w-28 rounded-inner" />
          <Skeleton className="h-9 w-24 rounded-inner" />
        </div>
      </div>

      {/* Content card placeholder. */}
      <div className="rounded-card border border-border bg-card p-5 sm:p-6">
        <div className="flex items-center justify-between gap-4">
          <Skeleton className="h-5 w-40" />
          <Skeleton className="h-8 w-28 rounded-inner" />
        </div>
        <div className="mt-5 space-y-3.5">
          {SKELETON_ROWS.map((width) => (
            <div key={width} className="flex items-center gap-4">
              <Skeleton className="size-9 shrink-0 rounded-inner" />
              <div className="min-w-0 flex-1 space-y-2">
                <Skeleton className={cn("h-3.5", width)} />
                <Skeleton className="h-3 w-1/4" />
              </div>
              <Skeleton className="hidden h-3.5 w-24 shrink-0 sm:block" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

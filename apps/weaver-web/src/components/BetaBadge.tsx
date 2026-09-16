import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

/** The amber "Beta" chip on every surface whose feature is still settling. */
export function BetaBadge({ className }: { className?: string }) {
  return (
    <Badge variant="warning" className={cn("px-1.5 py-0 text-[10px] uppercase tracking-[0.08em]", className)}>
      Beta
    </Badge>
  );
}

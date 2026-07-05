import { useId, useState, type ReactNode } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

interface SectionCardProps {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  collapsible?: boolean;
  defaultOpen?: boolean;
  /** Controlled open state; pair with `onOpenChange`. */
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  children: ReactNode;
  className?: string;
  contentClassName?: string;
}

/**
 * Panel card with a Space Grotesk header, optional description/actions, and an
 * optional collapse toggle. Shared across Job Detail and Settings.
 */
export function SectionCard({
  title,
  description,
  actions,
  collapsible = false,
  defaultOpen = true,
  open,
  onOpenChange,
  children,
  className,
  contentClassName,
}: SectionCardProps) {
  const [internalOpen, setInternalOpen] = useState(defaultOpen);
  const contentId = useId();
  const isOpen = open ?? internalOpen;

  const toggle = () => {
    const next = !isOpen;
    if (onOpenChange) {
      onOpenChange(next);
    } else {
      setInternalOpen(next);
    }
  };

  const titleBlock = (
    <div className="min-w-0">
      <div className="font-space-grotesk text-lg font-bold leading-tight text-foreground">{title}</div>
      {description ? (
        <div className="mt-1.5 text-[12.5px] leading-5 text-muted-foreground">{description}</div>
      ) : null}
    </div>
  );

  return (
    <section className={cn("rounded-card border border-border bg-card p-5 sm:p-6", className)}>
      <div className="flex items-start justify-between gap-4">
        {collapsible ? (
          <button
            type="button"
            onClick={toggle}
            aria-expanded={isOpen}
            aria-controls={contentId}
            className="flex min-w-0 flex-1 cursor-pointer items-start gap-2.5 text-left"
          >
            {isOpen ? (
              <ChevronDown className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
            ) : (
              <ChevronRight className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
            )}
            {titleBlock}
          </button>
        ) : (
          <div className="flex min-w-0 flex-1 items-start gap-2.5">{titleBlock}</div>
        )}
        {actions ? <div className="flex shrink-0 items-center gap-2">{actions}</div> : null}
      </div>
      {isOpen ? (
        <div id={contentId} className={cn("mt-5", contentClassName)}>
          {children}
        </div>
      ) : null}
    </section>
  );
}

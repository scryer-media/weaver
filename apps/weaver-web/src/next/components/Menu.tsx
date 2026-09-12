import { useEffect, useRef, type ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The one floating surface in the design system.
 *
 * Sort, Storage and every Select share it, which is why it carries the only
 * shadow in the redesign. Positioning is left to the caller through
 * `className` — the menus sit at fixed offsets from their triggers rather than
 * being anchored dynamically, matching the handoff's `top: 44px; left: 24px`
 * and `top: 26px; right: 0`.
 */
export function Menu({
  open,
  onDismiss,
  className,
  children,
  label,
}: {
  open: boolean;
  onDismiss: () => void;
  className?: string;
  children: ReactNode;
  label?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    const onPointerDown = (event: PointerEvent) => {
      const node = ref.current;
      // The trigger lives outside the menu, so a pointerdown on it would both
      // dismiss here and toggle there, leaving the menu shut on every click.
      // Ignoring the trigger's own subtree keeps the toggle authoritative.
      if (node && !node.contains(event.target as Node)
        && !(event.target as HTMLElement).closest?.("[data-wv-menu-trigger]")) {
        onDismiss();
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onDismiss();
      }
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [onDismiss, open]);

  if (!open) {
    return null;
  }

  return (
    <div
      ref={ref}
      role="menu"
      aria-label={label}
      className={cn(
        "absolute z-20 flex flex-col border border-wv-control bg-wv-chrome shadow-wv-menu",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function MenuItem({
  selected,
  onSelect,
  children,
  className,
}: {
  selected?: boolean;
  onSelect: () => void;
  children: ReactNode;
  className?: string;
}) {
  return (
    <button
      type="button"
      role="menuitemradio"
      aria-checked={selected}
      onClick={onSelect}
      className={cn(
        "flex cursor-pointer items-baseline gap-5 px-3 py-[9px] text-left text-[12.5px] hover:bg-wv-menu-hover",
        selected ? "bg-wv-selected text-wv-strong" : "text-wv-secondary",
        className,
      )}
    >
      {children}
    </button>
  );
}

import { useEffect, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { cn } from "@/lib/utils";

/**
 * A modal panel.
 *
 * The handoff designs no dialogs — every surface it draws is in-place — so
 * this is the system applied rather than a fixture copied: the chrome surface,
 * a 56px header identical to the top bar's, hairline-separated content, square
 * corners, and the menu shadow, which is the only shadow the design allows.
 */
export function Dialog({
  open,
  title,
  note,
  onDismiss,
  footer,
  width = 520,
  children,
}: {
  open: boolean;
  title: string;
  note?: ReactNode;
  onDismiss: () => void;
  footer?: ReactNode;
  width?: number;
  children: ReactNode;
}) {
  useEffect(() => {
    if (!open) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onDismiss();
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [onDismiss, open]);

  if (!open) {
    return null;
  }

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-[rgb(0_0_0_/_0.55)] p-6"
      onPointerDown={(event) => {
        if (event.target === event.currentTarget) {
          onDismiss();
        }
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        style={{ width }}
        className={cn(
          "flex max-h-full max-w-full flex-col border border-wv-control bg-wv-chrome text-wv-fg shadow-wv-menu",
        )}
      >
        <header className="flex h-14 flex-none items-baseline gap-[10px] border-b border-wv-line-strong px-4 sm:px-6">
          <h2 className="flex-none font-wv-title text-[15px] font-semibold tracking-[-0.01em]">
            {title}
          </h2>
          {note === undefined ? null : (
            <span className="truncate font-wv-mono text-[11.5px] text-wv-muted">{note}</span>
          )}
        </header>
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">{children}</div>
        {footer === undefined ? null : (
          <div className="flex flex-none items-center justify-end gap-[10px] border-t border-wv-line-strong px-4 sm:px-6 py-4">
            {footer}
          </div>
        )}
      </div>
    </div>,
    document.body,
  );
}

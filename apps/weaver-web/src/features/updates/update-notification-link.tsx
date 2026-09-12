import { CircleArrowUp } from "lucide-react";

import { cn } from "@/lib/utils";

interface UpdateNotificationLinkProps {
  /** Result of `releaseNotification`; `undefined` when there is nothing to advertise. */
  notification: { version: string; url: string } | undefined;
  /**
   * `"desktop"` renders inline in the sidebar footer, in place of the plain
   * version line. `"mobile"` renders a floating pill for viewports where that
   * sidebar is hidden.
   */
  placement: "desktop" | "mobile";
  label: string;
  ariaLabel: string;
}

/**
 * Link to the newest release on GitHub.
 *
 * Renders nothing without a notification, so both placements can be mounted
 * unconditionally — the mobile one is always in the tree and simply yields
 * nothing (its label props are empty strings) until a release turns up.
 *
 * The mobile pill deliberately sits bottom-left: the PWA update banner already
 * occupies bottom-right, and the two can be on screen at once.
 */
export function UpdateNotificationLink({
  notification,
  placement,
  label,
  ariaLabel,
}: UpdateNotificationLinkProps) {
  if (!notification) {
    return null;
  }

  return (
    <a
      href={notification.url}
      target="_blank"
      rel="noreferrer noopener"
      aria-label={ariaLabel}
      className={cn(
        "flex items-center justify-center gap-1.5 font-medium text-primary transition-colors",
        placement === "desktop"
          ? "rounded-[9px] px-2 py-1 text-[11.5px] hover:bg-accent/40 hover:text-primary"
          : "fixed bottom-4 left-4 z-40 rounded-full border border-primary/30 bg-background/95 px-3 py-1.5 text-xs shadow-[0_18px_60px_rgba(8,18,36,0.28)] backdrop-blur-md md:hidden",
      )}
    >
      <CircleArrowUp className="size-3.5" aria-hidden="true" />
      <span>{label}</span>
    </a>
  );
}

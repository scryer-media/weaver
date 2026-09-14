import * as React from "react";
import { ignoredByPasswordManagers, PASSWORD_MANAGER_IGNORE } from "@/lib/password-manager";
import { cn } from "@/lib/utils";

function Input({
  className,
  type,
  secret,
  ...props
}: React.ComponentProps<"input"> & {
  /** Keep password managers out. Defaults to on for a password that is not the Weaver login. */
  secret?: boolean;
}) {
  return (
    <input
      type={type}
      {...(ignoredByPasswordManagers({ secret, type, autoComplete: props.autoComplete })
        ? PASSWORD_MANAGER_IGNORE
        : null)}
      data-slot="input"
      className={cn(
        "file:text-foreground placeholder:text-muted-foreground selection:bg-primary selection:text-primary-foreground bg-field text-foreground border-input h-9 w-full min-w-0 rounded-md border px-3 py-1 text-base shadow-xs transition-[color,box-shadow] outline-none file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
        "focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]",
        "aria-invalid:border-destructive",
        className,
      )}
      {...props}
    />
  );
}

export { Input };

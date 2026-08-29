import { useTheme } from "next-themes";
import { Toaster as Sonner, type ToasterProps } from "sonner";

export function Toaster({ toastOptions, ...props }: ToasterProps) {
  const { resolvedTheme } = useTheme();

  return (
    <Sonner
      theme={resolvedTheme === "light" ? "light" : "dark"}
      toastOptions={{
        className: "border-border bg-popover text-popover-foreground shadow-xl",
        ...toastOptions,
      }}
      {...props}
    />
  );
}

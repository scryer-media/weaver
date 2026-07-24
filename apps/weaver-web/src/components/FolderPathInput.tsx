import { useState, type KeyboardEvent } from "react";
import { FolderOpen } from "lucide-react";
import { DirectoryBrowserDialog } from "@/components/DirectoryBrowserDialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

type FolderPathInputProps = {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  browseLabel?: string;
  className?: string;
  inputClassName?: string;
  inputId?: string;
};

export function FolderPathInput({
  value,
  onChange,
  placeholder,
  disabled = false,
  browseLabel,
  className,
  inputClassName,
  inputId,
}: FolderPathInputProps) {
  const t = useTranslate();
  const [open, setOpen] = useState(false);
  const [browsePath, setBrowsePath] = useState<string | null>(null);

  const openBrowser = () => {
    if (disabled) return;
    setBrowsePath(value.trim() || null);
    setOpen(true);
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    openBrowser();
  };

  return (
    <>
      <div className={cn("flex gap-2", className)}>
        <Input
          id={inputId}
          type="text"
          value={value}
          readOnly
          disabled={disabled}
          placeholder={placeholder}
          onClick={openBrowser}
          onKeyDown={handleKeyDown}
          className={cn("min-w-0 cursor-pointer font-mono", inputClassName)}
        />
        <Button
          type="button"
          variant="outline"
          disabled={disabled}
          onClick={openBrowser}
          className="shrink-0"
        >
          <FolderOpen className="size-4" />
          {browseLabel ?? t("categories.browse")}
        </Button>
      </div>

      <DirectoryBrowserDialog
        open={open}
        path={browsePath}
        onPathChange={setBrowsePath}
        onClose={() => setOpen(false)}
        onChoose={(nextPath) => {
          onChange(nextPath);
          setOpen(false);
        }}
      />
    </>
  );
}

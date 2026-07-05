import { useEffect, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { UploadNzbForm } from "@/features/upload/components/UploadNzbForm";
import { useTranslate } from "@/lib/context/translate-context";

interface UploadModalProps {
  open: boolean;
  onClose: () => void;
}

export function UploadModal({ open, onClose }: UploadModalProps) {
  const t = useTranslate();
  const formRef = useRef<HTMLFormElement | null>(null);
  const wasOpenRef = useRef(false);
  const [openCycle, setOpenCycle] = useState(0);

  const handleDialogKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (
      event.key !== "Enter" ||
      event.defaultPrevented ||
      event.nativeEvent.isComposing ||
      event.metaKey ||
      event.ctrlKey ||
      event.altKey ||
      event.shiftKey
    ) {
      return;
    }

    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const role = target.getAttribute("role");
    if (
      target instanceof HTMLButtonElement ||
      target instanceof HTMLTextAreaElement ||
      target.isContentEditable ||
      role === "button" ||
      role === "combobox" ||
      role === "listbox" ||
      role === "option" ||
      target.closest("[role='listbox']") ||
      target.closest("[data-radix-select-content]")
    ) {
      return;
    }

    if (!formRef.current) {
      return;
    }

    event.preventDefault();
    formRef.current.requestSubmit();
  };

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  useEffect(() => {
    if (open && !wasOpenRef.current) {
      setOpenCycle((current) => current + 1);
    }
    wasOpenRef.current = open;
  }, [open]);

  return (
    <Dialog open={open} onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent
        className="max-h-[90vh] w-[min(96vw,72rem)] overflow-y-auto rounded-card p-4 sm:max-w-5xl sm:p-6 [&>button]:top-6 [&>button]:right-6 [&>button]:size-8 [&>button]:rounded-inner [&>button]:border [&>button]:border-border [&>button]:bg-card [&>button]:opacity-100"
        onKeyDownCapture={handleDialogKeyDown}
      >
        <DialogHeader className="gap-2 text-left">
          <DialogTitle className="font-space-grotesk text-2xl font-bold leading-none tracking-tight">
            {t("upload.title")}
          </DialogTitle>
          <DialogDescription className="text-[13px] text-muted-foreground">
            {t("upload.accepts")}
          </DialogDescription>
        </DialogHeader>
        {open ? (
          <UploadNzbForm
            key={openCycle}
            layout="dialog"
            open={open}
            onSubmitted={onClose}
            formRef={formRef}
          />
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

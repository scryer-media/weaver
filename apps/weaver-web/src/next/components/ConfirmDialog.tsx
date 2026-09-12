import type { ReactNode } from "react";
import { Dialog } from "./Dialog";
import { DangerButton, PrimaryButton, SecondaryButton } from "./controls";

/** One confirmation shape for every destructive action in the Next UI. */
export function ConfirmDialog({
  open,
  title,
  note,
  body,
  confirmLabel = "Remove",
  destructive = true,
  busy = false,
  onConfirm,
  onDismiss,
}: {
  open: boolean;
  title: string;
  note?: ReactNode;
  body: ReactNode;
  confirmLabel?: string;
  destructive?: boolean;
  busy?: boolean;
  onConfirm: () => void;
  onDismiss: () => void;
}) {
  return (
    <Dialog
      open={open}
      title={title}
      note={note}
      width={440}
      onDismiss={onDismiss}
      footer={
        <>
          <SecondaryButton onClick={onDismiss}>Cancel</SecondaryButton>
          {destructive ? (
            <DangerButton onClick={onConfirm} disabled={busy} className="px-[14px]">
              {confirmLabel}
            </DangerButton>
          ) : (
            <PrimaryButton onClick={onConfirm} disabled={busy}>
              {confirmLabel}
            </PrimaryButton>
          )}
        </>
      }
    >
      <div className="px-4 sm:px-6 py-5 text-[13px] leading-[1.55] text-pretty text-wv-secondary">
        {body}
      </div>
    </Dialog>
  );
}

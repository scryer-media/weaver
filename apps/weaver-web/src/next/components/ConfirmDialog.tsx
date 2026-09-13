import type { ReactNode } from "react";
import { Dialog } from "./Dialog";
import { DangerButton, PrimaryButton, SecondaryButton } from "./controls";

/**
 * One confirmation shape for every destructive action in the Next UI.
 *
 * An action that can go two ways — drop the record, or the record and what it
 * points at — passes the milder one as `alternative`: it sits between Cancel
 * and the confirm as an outlined button, and the confirm turns solid red.
 */
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
  alternative,
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
  alternative?: { label: string; onConfirm: () => void };
}) {
  return (
    <Dialog
      open={open}
      title={title}
      note={note}
      width={alternative === undefined ? 440 : 540}
      onDismiss={onDismiss}
      footer={
        <>
          <SecondaryButton onClick={onDismiss}>Cancel</SecondaryButton>
          {alternative === undefined ? null : (
            <DangerButton onClick={alternative.onConfirm} disabled={busy} className="px-[14px]">
              {alternative.label}
            </DangerButton>
          )}
          {destructive ? (
            <DangerButton
              onClick={onConfirm}
              disabled={busy}
              solid={alternative !== undefined}
              className="px-[14px]"
            >
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

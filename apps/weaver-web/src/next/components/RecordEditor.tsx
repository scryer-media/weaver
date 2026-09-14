import type { ReactNode } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { Dialog } from "./Dialog";
import { DangerButton, PrimaryButton, SecondaryButton } from "./controls";
import { SectionHeader } from "./chrome";
import { FieldRows, type FieldSpec } from "@/next/pages/settings/framework";

/**
 * The editor behind every list-shaped settings panel.
 *
 * Servers, categories, proxies, feeds and schedules are all "a table of
 * records, click one to edit it", so they share one dialog: sections of the
 * same declarative fields the panels themselves are built from, an error line,
 * and a footer that is Delete on the left, Cancel and Save on the right.
 */

export interface EditorSection {
  id: string;
  title: string;
  /** A chip beside the title, such as the beta marker. */
  tag?: ReactNode;
  note?: ReactNode;
  fields: FieldSpec[];
}

export function RecordEditor({
  open,
  title,
  note,
  sections,
  error,
  busy = false,
  saveLabel,
  saveDisabled = false,
  onSave,
  onDismiss,
  onDelete,
  deleteLabel,
  extraActions,
  width = 560,
  children,
}: {
  open: boolean;
  title: string;
  note?: ReactNode;
  sections: readonly EditorSection[];
  error?: string | null;
  busy?: boolean;
  saveLabel?: string;
  saveDisabled?: boolean;
  onSave: () => void;
  onDismiss: () => void;
  onDelete?: () => void;
  deleteLabel?: string;
  /** Buttons that belong beside Delete — "Test connection", "Sync now". */
  extraActions?: ReactNode;
  width?: number;
  /** Anything that is not a field: a test result, a warning, a sub-table. */
  children?: ReactNode;
}) {
  const t = useTranslate();
  return (
    <Dialog
      open={open}
      title={title}
      note={note}
      width={width}
      onDismiss={onDismiss}
      footer={
        <>
          {onDelete === undefined ? null : (
            <DangerButton onClick={onDelete} disabled={busy} className="mr-auto px-[14px]">
              {deleteLabel ?? t("action.delete")}
            </DangerButton>
          )}
          {extraActions}
          <SecondaryButton onClick={onDismiss}>{t("action.cancel")}</SecondaryButton>
          <PrimaryButton onClick={onSave} disabled={busy || saveDisabled}>
            {busy ? t("settings.saving") : (saveLabel ?? t("action.save"))}
          </PrimaryButton>
        </>
      }
    >
      {sections.map((section) => (
        <div key={section.id} className="flex flex-none flex-col">
          <SectionHeader label={section.title} tag={section.tag} note={section.note} sticky={false} />
          <FieldRows fields={section.fields} />
        </div>
      ))}
      {children}
      {error ? (
        <div className="flex-none border-t border-wv-hairline px-4 sm:px-6 py-4 text-[12.5px] leading-[1.5] text-wv-error-text">
          {error}
        </div>
      ) : null}
    </Dialog>
  );
}

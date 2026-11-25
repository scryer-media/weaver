// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ReactNode } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { Dialog } from "./Dialog";
import { DangerButton, PrimaryButton, SecondaryButton } from "./controls";
import { SectionHeader } from "./chrome";
import {
  FieldRows,
  Fold,
  type FieldSpec,
} from "@/next/pages/settings/framework";
import { testIds } from "@/next/testids";

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
  /** Rows under `fields` that slide open as one while `open`, such as the rest of a form behind a first choice. */
  folded?: { open: boolean; fields: FieldSpec[] };
  /** Anything that is not a field, drawn the section's full width under its fields. */
  body?: ReactNode;
}

export function RecordEditor({
  open,
  title,
  note,
  sections,
  error,
  errorCode,
  busy = false,
  saveLabel,
  saveDisabled = false,
  onSave,
  onDismiss,
  onDelete,
  deleteLabel,
  extraActions,
  width = 560,
  overlay,
  children,
  testId,
}: {
  open: boolean;
  title: string;
  note?: ReactNode;
  sections: readonly EditorSection[];
  error?: string | null;
  /** The code of the problem the error line shows, when the editor found it itself. */
  errorCode?: string | null;
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
  /** Laid over the whole editor while something slow runs. */
  overlay?: ReactNode;
  /** Anything that is not a field: a test result, a warning, a sub-table. */
  children?: ReactNode;
  testId?: string;
}) {
  const t = useTranslate();
  return (
    <Dialog
      open={open}
      title={title}
      note={note}
      width={width}
      overlay={overlay}
      onDismiss={onDismiss}
      testId={testId}
      footer={
        <>
          {onDelete === undefined ? null : (
            <DangerButton
              icon="remove"
              onClick={onDelete}
              disabled={busy}
              className="mr-auto"
              testId={testIds.editorDelete}
            >
              {deleteLabel ?? t("action.delete")}
            </DangerButton>
          )}
          {extraActions}
          <SecondaryButton onClick={onDismiss} testId={testIds.editorCancel}>
            {t("action.cancel")}
          </SecondaryButton>
          <PrimaryButton
            onClick={onSave}
            disabled={busy || saveDisabled}
            testId={testIds.editorSave}
          >
            {busy ? t("settings.saving") : (saveLabel ?? t("action.save"))}
          </PrimaryButton>
        </>
      }
    >
      {sections.map((section) => (
        <div
          key={section.id}
          data-testid={testIds.section(section.id)}
          className="flex flex-none flex-col"
        >
          <SectionHeader
            label={section.title}
            tag={section.tag}
            note={section.note}
            sticky={false}
          />
          <FieldRows fields={section.fields} />
          {section.folded ? (
            <Fold open={section.folded.open}>
              <FieldRows fields={section.folded.fields} />
            </Fold>
          ) : null}
          {section.body}
        </div>
      ))}
      {children}
      {error ? (
        <div
          data-testid={testIds.editorError}
          data-problem={errorCode ?? undefined}
          className="flex-none border-t border-wv-hairline px-4 sm:px-6 py-4 text-[12.5px] leading-[1.5] text-wv-error-text"
        >
          {error}
        </div>
      ) : null}
    </Dialog>
  );
}

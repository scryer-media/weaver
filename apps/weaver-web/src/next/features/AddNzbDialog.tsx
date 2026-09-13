import { useEffect, useRef, useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { submissionStatusCanForceRetry } from "@/features/duplicates/duplicate-presentation";
import { useUploadNzb, type UploadNzbEntry } from "@/features/upload/hooks/use-upload-nzb";
import { NZB_UPLOAD_ACCEPT } from "@/features/upload/upload-file-types";
import { cn } from "@/lib/utils";
import { ConfirmDialog } from "@/next/components/ConfirmDialog";
import { Dialog } from "@/next/components/Dialog";
import { Eyebrow } from "@/next/components/chrome";
import { FormRow } from "@/next/components/rows";
import { PrimaryButton, SecondaryButton, Select, TextField } from "@/next/components/controls";
import { formatSize } from "@/next/data/format";
import { WV } from "@/next/data/palette";

const NO_CATEGORY_VALUE = "__none__";

const PRIORITIES: { value: string; label: string }[] = [
  { value: "HIGH", label: "High" },
  { value: "NORMAL", label: "Normal" },
  { value: "LOW", label: "Low" },
];

/** Staged, but the duplicate policy turned its submission away; only a forced submit takes it. */
function isBlocked(entry: UploadNzbEntry): boolean {
  return entry.status === "staged" && submissionStatusCanForceRetry(entry.submissionStatus);
}

function entryTone(entry: UploadNzbEntry): string {
  if (entry.status === "failed") return WV.error;
  if (isBlocked(entry)) return WV.warn;
  if (entry.status === "staged" || entry.status === "submitted") return WV.accent;
  return WV.idle;
}

/**
 * "Add NZB", in the Next vocabulary.
 *
 * The whole upload pipeline — staging, duplicate scoring, submission — is the
 * existing `useUploadNzb` hook untouched; this is only a second view of it, so
 * the two interfaces cannot drift in behaviour. Files picked or dropped here
 * are added to the list rather than replacing it, so a second trip through the
 * file browser keeps the first.
 */
export function AddNzbDialog({
  open,
  onClose,
  initialFiles = null,
}: {
  open: boolean;
  onClose: () => void;
  /** Files dropped somewhere else that opened the dialog, staged as it opens. */
  initialFiles?: readonly File[] | null;
}) {
  const t = useTranslate();
  const upload = useUploadNzb({ open, resetOnOpen: true, onSubmitted: onClose });
  const { addFiles } = upload;
  const blocked = upload.entries.filter(isBlocked);
  // Set once a submit has answered, so its verdict is read from the entries it updated.
  const [awaitingVerdict, setAwaitingVerdict] = useState(false);
  const [confirmForce, setConfirmForce] = useState(false);
  if (awaitingVerdict && !upload.fetching) {
    setAwaitingVerdict(false);
    if (blocked.length > 0) {
      setConfirmForce(true);
    }
  }
  if (!open && confirmForce) {
    setConfirmForce(false);
  }

  // Duplicates are not a per-file decision made before submitting: everything
  // goes in, and only what the duplicate policy turned away comes back as one
  // question about adding those anyway.
  const submit = async () => {
    await upload.submit();
    setAwaitingVerdict(true);
  };

  // The hook clears the list as the dialog opens; this runs after that, and
  // only once for a given drop.
  const stagedDrop = useRef<readonly File[] | null>(null);
  useEffect(() => {
    if (!open) {
      stagedDrop.current = null;
      return;
    }
    if (initialFiles && initialFiles.length > 0 && stagedDrop.current !== initialFiles) {
      stagedDrop.current = initialFiles;
      addFiles([...initialFiles]);
    }
  }, [addFiles, initialFiles, open]);

  const categoryOptions = [
    { value: NO_CATEGORY_VALUE, label: "No category" },
    ...(upload.categories as { id: number; name: string }[]).map((entry) => ({
      value: entry.name,
      label: entry.name,
    })),
  ];

  return (
    <Dialog
      open={open}
      title="Add NZB"
      onDismiss={onClose}
      width={620}
      footer={
        <>
          <SecondaryButton onClick={onClose}>Cancel</SecondaryButton>
          <PrimaryButton
            disabled={upload.readyCount === 0 || upload.staging || upload.fetching}
            onClick={() => void submit()}
          >
            {upload.readyCount > 1 ? `Add ${upload.readyCount} downloads` : "Add download"}
          </PrimaryButton>
        </>
      }
    >
      <div className="flex flex-col">
        <button
          type="button"
          onClick={upload.openPicker}
          onDragOver={(event) => {
            event.preventDefault();
            upload.setDragging(true);
          }}
          onDragLeave={() => upload.setDragging(false)}
          onDrop={(event) => {
            event.preventDefault();
            upload.setDragging(false);
            addFiles(Array.from(event.dataTransfer.files));
          }}
          className={cn(
            "flex h-[104px] flex-none cursor-pointer flex-col items-center justify-center gap-2 border border-dashed text-[13px]",
            "mx-6 mt-5",
            upload.dragging
              ? "border-wv-accent bg-wv-selected text-wv-strong"
              : "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover-strong",
          )}
        >
          <input
            ref={upload.fileInputRef}
            type="file"
            accept={NZB_UPLOAD_ACCEPT}
            multiple
            className="hidden"
            onChange={(event) => {
              addFiles(Array.from(event.target.files ?? []));
              event.target.value = "";
            }}
          />
          <span className="font-medium text-wv-fg">Drop NZB files here, or click to choose</span>
          {upload.entries.length === 0 ? null : (
            <span className="font-wv-mono text-[11px] text-wv-faint">
              {`${upload.entries.length} selected · ${formatSize(upload.totalBytes)}`}
            </span>
          )}
        </button>

        {upload.error === null ? null : (
          <div className="mx-6 mt-4 border border-wv-danger-border bg-wv-button px-4 py-3 text-[12.5px] text-wv-error-text">
            {upload.error}
          </div>
        )}

        {upload.entries.length === 0 ? null : (
          <div className="mt-5 flex flex-col">
            <div className="flex h-[30px] flex-none items-center border-y border-wv-hairline bg-wv-section px-4 sm:px-6">
              <Eyebrow>Files</Eyebrow>
            </div>
            {upload.entries.map((entry) => (
              <div
                key={entry.localId}
                className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-wv-hairline px-4 sm:px-6 py-3"
              >
                <div className="flex min-w-0 flex-[1_1_260px] flex-col gap-1">
                  <span title={entry.file.name} className="truncate text-[13px] text-wv-fg">
                    {entry.displayName ?? entry.file.name}
                  </span>
                  {entry.error === undefined ? null : (
                    <span className="text-[11.5px] text-wv-error-text">{entry.error}</span>
                  )}
                </div>
                <span
                  style={{ color: entryTone(entry) }}
                  className="flex-none font-wv-mono text-[10.5px] tracking-[0.1em] uppercase"
                >
                  {isBlocked(entry) ? "blocked" : entry.status}
                </span>
                <span className="w-[72px] flex-none text-right font-wv-mono text-[11.5px] text-wv-muted">
                  {formatSize(entry.file.size)}
                </span>
                <button
                  type="button"
                  onClick={() => upload.removeFile(entry.localId)}
                  aria-label={t("upload.removeFile")}
                  className="flex-none text-[12px] text-wv-error-text hover:text-wv-error"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="mt-2 flex flex-col">
          <FormRow label="Category">
            <Select
              label="Category"
              value={upload.category}
              options={categoryOptions}
              onChange={upload.setCategory}
            />
          </FormRow>
          <FormRow label="Priority">
            <Select
              label="Priority"
              value={upload.priority}
              options={PRIORITIES}
              onChange={upload.setPriority}
            />
          </FormRow>
          <FormRow label="Password">
            <TextField
              label="Password"
              type="password"
              value={upload.password}
              onChange={upload.setPassword}
              placeholder="optional"
              className="w-[268px] max-w-full"
            />
          </FormRow>
        </div>
      </div>
      <ConfirmDialog
        open={confirmForce}
        title="Add duplicates anyway?"
        note={`${blocked.length} blocked`}
        destructive={false}
        busy={upload.fetching}
        body={
          <>
            <span className="block">
              {blocked.length === 1
                ? "This NZB was turned away by the duplicate policy:"
                : `These ${blocked.length} NZBs were turned away by the duplicate policy:`}
            </span>
            <span className="mt-3 flex flex-col gap-1 font-wv-mono text-[11.5px] text-wv-fg">
              {blocked.slice(0, 6).map((entry) => (
                <span key={entry.localId} className="truncate" title={entry.file.name}>
                  {entry.displayName ?? entry.file.name}
                </span>
              ))}
              {blocked.length > 6 ? (
                <span className="text-wv-muted">{`and ${blocked.length - 6} more`}</span>
              ) : null}
            </span>
            <span className="mt-3 block">{t("upload.forceDesc")}</span>
          </>
        }
        confirmLabel={blocked.length === 1 ? "Add anyway" : `Add ${blocked.length} anyway`}
        dismissLabel={blocked.length === 1 ? "Leave it out" : "Leave them out"}
        onConfirm={() => {
          const ids = blocked.map((entry) => entry.localId);
          setConfirmForce(false);
          void upload.submit({ force: true, localIds: ids });
        }}
        onDismiss={() => setConfirmForce(false)}
      />
    </Dialog>
  );
}

import { useState, useCallback, useRef, useEffect } from "react";
import { useMutation } from "urql";
import { SUBMIT_NZB_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

interface UploadModalProps {
  open: boolean;
  onClose: () => void;
}

export function UploadModal({ open, onClose }: UploadModalProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [password, setPassword] = useState("");
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [{ fetching }, submitNzb] = useMutation(SUBMIT_NZB_MUTATION);
  const t = useTranslate();

  // Reset state when modal opens
  useEffect(() => {
    if (open) {
      setFile(null);
      setPassword("");
      setError(null);
      setDragging(false);
    }
  }, [open]);

  const handleFile = useCallback(
    (f: File) => {
      setError(null);
      if (!f.name.endsWith(".nzb")) {
        setError(t("upload.invalidFile"));
        return;
      }
      setFile(f);
    },
    [t],
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      const f = e.dataTransfer.files[0];
      if (f) handleFile(f);
    },
    [handleFile],
  );

  const handleSubmit = useCallback(async () => {
    if (!file) return;
    setError(null);

    try {
      const buffer = await file.arrayBuffer();
      const bytes = new Uint8Array(buffer);
      let binary = "";
      for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
      }
      const nzbBase64 = btoa(binary);

      const result = await submitNzb({
        nzbBase64,
        filename: file.name,
        password: password || null,
      });

      if (result.error) {
        setError(result.error.message);
        return;
      }

      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed");
    }
  }, [file, password, submitNzb, onClose]);

  // Close on Escape, submit on Enter
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      if (e.key === "Enter" && file && !fetching) {
        e.preventDefault();
        handleSubmit();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose, file, fetching, handleSubmit]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="w-full max-w-lg rounded-lg border border-border bg-background p-6 shadow-xl">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-bold text-foreground">
            {t("upload.title")}
          </h2>
          <button
            onClick={onClose}
            className="rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M18 6 6 18" />
              <path d="m6 6 12 12" />
            </svg>
          </button>
        </div>

        <div
          className={`mb-4 flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed p-8 transition-colors ${
            dragging
              ? "border-primary bg-primary/10"
              : "border-border bg-card hover:border-muted-foreground"
          }`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".nzb"
            className="hidden"
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) handleFile(f);
            }}
          />
          {file ? (
            <>
              <div className="text-base font-medium text-foreground">
                {file.name}
              </div>
              <div className="mt-1 text-sm text-muted-foreground">
                {(file.size / 1024).toFixed(1)} KB
              </div>
              <div className="mt-2 text-xs text-muted-foreground">
                {t("upload.replaceHint")}
              </div>
            </>
          ) : (
            <>
              <div className="text-base text-muted-foreground">
                {t("upload.dropzone")}
              </div>
              <div className="mt-1 text-sm text-muted-foreground">
                {t("upload.accepts")}
              </div>
            </>
          )}
        </div>

        <div className="mb-4">
          <label className="mb-1 block text-sm font-medium text-muted-foreground">
            {t("upload.passwordLabel")}
          </label>
          <input
            type="text"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder={t("upload.passwordPlaceholder")}
            className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground placeholder-muted-foreground focus:border-ring focus:outline-none"
          />
        </div>

        {error && (
          <div className="mb-4 rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        )}

        <div className="flex items-center justify-end gap-3">
          <button
            onClick={onClose}
            className="rounded-md px-4 py-2 text-sm text-muted-foreground hover:text-foreground"
          >
            {t("action.cancel")}
          </button>
          <button
            onClick={handleSubmit}
            disabled={!file || fetching}
            className="rounded-md bg-primary px-6 py-2 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {fetching ? t("action.uploading") : t("action.submit")}
          </button>
        </div>
      </div>
    </div>
  );
}

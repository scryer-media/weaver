import { useState, useCallback, useRef } from "react";
import { useNavigate } from "react-router";
import { useMutation } from "urql";
import { SUBMIT_NZB_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

export function Upload() {
  const navigate = useNavigate();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [file, setFile] = useState<File | null>(null);
  const [password, setPassword] = useState("");
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [{ fetching }, submitNzb] = useMutation(SUBMIT_NZB_MUTATION);

  const t = useTranslate();

  const handleFile = useCallback((f: File) => {
    setError(null);
    if (!f.name.endsWith(".nzb")) {
      setError(t("upload.invalidFile"));
      return;
    }
    setFile(f);
  }, [t]);

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

      navigate("/");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed");
    }
  }, [file, password, submitNzb, navigate]);

  return (
    <div className="p-4 sm:p-6">
      <h1 className="mb-4 text-xl font-bold text-foreground sm:mb-6 sm:text-2xl">{t("upload.title")}</h1>

      <div
        className={`mb-4 flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed p-8 transition-colors sm:mb-6 sm:p-12 ${
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
            <div className="text-lg font-medium text-foreground">{file.name}</div>
            <div className="mt-1 text-sm text-muted-foreground">
              {(file.size / 1024).toFixed(1)} KB
            </div>
            <div className="mt-2 text-xs text-muted-foreground">{t("upload.replaceHint")}</div>
          </>
        ) : (
          <>
            <div className="text-lg text-muted-foreground">
              {t("upload.dropzone")}
            </div>
            <div className="mt-1 text-sm text-muted-foreground">{t("upload.accepts")}</div>
          </>
        )}
      </div>

      <div className="mb-4 sm:mb-6">
        <label className="mb-1 block text-sm font-medium text-muted-foreground">
          {t("upload.passwordLabel")}
        </label>
        <input
          type="text"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder={t("upload.passwordPlaceholder")}
          className="w-full max-w-md rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground placeholder-muted-foreground focus:border-ring focus:outline-none"
        />
      </div>

      {error && (
        <div className="mb-4 rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <button
        onClick={handleSubmit}
        disabled={!file || fetching}
        className="rounded-md bg-primary px-6 py-2 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
      >
        {fetching ? t("action.uploading") : t("action.submit")}
      </button>
    </div>
  );
}

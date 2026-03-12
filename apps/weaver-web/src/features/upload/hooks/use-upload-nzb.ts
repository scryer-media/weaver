import { useCallback, useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { useMutation, useQuery } from "urql";
import { CATEGORIES_QUERY, SUBMIT_NZB_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

const NO_CATEGORY_VALUE = "__none__";

async function toBase64(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let index = 0; index < bytes.length; index += 1) {
    binary += String.fromCharCode(bytes[index]);
  }
  return btoa(binary);
}

export function useUploadNzb(options?: {
  resetOnOpen?: boolean;
  open?: boolean;
  onSubmitted?: () => void;
}) {
  const t = useTranslate();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [files, setFiles] = useState<File[]>([]);
  const [password, setPassword] = useState("");
  const [category, setCategory] = useState(NO_CATEGORY_VALUE);
  const [priority, setPriority] = useState("NORMAL");
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [{ fetching }, submitNzb] = useMutation(SUBMIT_NZB_MUTATION);
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });

  useEffect(() => {
    if (options?.resetOnOpen && options.open) {
      setFiles([]);
      setPassword("");
      setCategory(NO_CATEGORY_VALUE);
      setPriority("NORMAL");
      setDragging(false);
      setError(null);
      setSubmitting(false);
    }
  }, [options?.open, options?.resetOnOpen]);

  const handleFiles = useCallback(
    (nextFiles: File[]) => {
      setError(null);
      if (nextFiles.length === 0) {
        setFiles([]);
        return;
      }
      if (nextFiles.some((file) => !file.name.toLowerCase().endsWith(".nzb"))) {
        setError(t("upload.invalidFiles"));
        return;
      }
      setFiles(nextFiles);
    },
    [t],
  );

  const handleDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault();
      setDragging(false);
      handleFiles(Array.from(event.dataTransfer.files));
    },
    [handleFiles],
  );

  const submit = useCallback(async () => {
    if (files.length === 0) {
      return false;
    }

    setError(null);
    setSubmitting(true);

    try {
      let submittedCount = 0;
      for (const file of files) {
        const result = await submitNzb({
          nzbBase64: await toBase64(file),
          filename: file.name,
          password: password.trim() || null,
          category: category.trim() && category !== NO_CATEGORY_VALUE ? category : null,
          metadata: [{ key: "priority", value: priority }],
        });

        if (result.error) {
          const prefix =
            submittedCount > 0
              ? t("upload.partialFailure", {
                  submitted: submittedCount,
                  total: files.length,
                  name: file.name,
                })
              : t("upload.batchFailure", { name: file.name });
          setError(`${prefix} ${result.error.message}`);
          return false;
        }

        submittedCount += 1;
      }

      setFiles([]);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      options?.onSubmitted?.();
      return true;
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "Upload failed",
      );
      return false;
    } finally {
      setSubmitting(false);
    }
  }, [category, files, options, password, priority, submitNzb, t]);

  return {
    categories: categoryData?.categories ?? [],
    dragging,
    error,
    files,
    fileInputRef,
    fetching: submitting || fetching,
    password,
    priority,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    handleDrop,
    handleFiles,
    openPicker: () => fileInputRef.current?.click(),
    submit,
    onFileInputChange: (event: ChangeEvent<HTMLInputElement>) => {
      handleFiles(Array.from(event.target.files ?? []));
      event.target.value = "";
    },
  };
}

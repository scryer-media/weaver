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
  const [file, setFile] = useState<File | null>(null);
  const [password, setPassword] = useState("");
  const [category, setCategory] = useState(NO_CATEGORY_VALUE);
  const [priority, setPriority] = useState("NORMAL");
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [{ fetching }, submitNzb] = useMutation(SUBMIT_NZB_MUTATION);
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });

  useEffect(() => {
    if (options?.resetOnOpen && options.open) {
      setFile(null);
      setPassword("");
      setCategory(NO_CATEGORY_VALUE);
      setPriority("NORMAL");
      setDragging(false);
      setError(null);
    }
  }, [options?.open, options?.resetOnOpen]);

  const handleFile = useCallback(
    (nextFile: File) => {
      setError(null);
      if (!nextFile.name.toLowerCase().endsWith(".nzb")) {
        setError(t("upload.invalidFile"));
        return;
      }
      setFile(nextFile);
    },
    [t],
  );

  const handleDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault();
      setDragging(false);
      const droppedFile = event.dataTransfer.files[0];
      if (droppedFile) {
        handleFile(droppedFile);
      }
    },
    [handleFile],
  );

  const submit = useCallback(async () => {
    if (!file) {
      return false;
    }

    setError(null);

    try {
      const result = await submitNzb({
        nzbBase64: await toBase64(file),
        filename: file.name,
        password: password.trim() || null,
        category: category.trim() && category !== NO_CATEGORY_VALUE ? category : null,
        metadata: [{ key: "priority", value: priority }],
      });

      if (result.error) {
        setError(result.error.message);
        return false;
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
    }
  }, [category, file, options, password, priority, submitNzb]);

  return {
    categories: categoryData?.categories ?? [],
    dragging,
    error,
    file,
    fileInputRef,
    fetching,
    password,
    priority,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    handleDrop,
    handleFile,
    openPicker: () => fileInputRef.current?.click(),
    submit,
    onFileInputChange: (event: ChangeEvent<HTMLInputElement>) => {
      const nextFile = event.target.files?.[0];
      if (nextFile) {
        handleFile(nextFile);
      }
    },
  };
}

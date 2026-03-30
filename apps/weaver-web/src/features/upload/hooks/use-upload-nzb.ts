import { useCallback, useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { CATEGORIES_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

const NO_CATEGORY_VALUE = "__none__";

const SUBMIT_NZB_UPLOAD_OPERATION = `
  mutation SubmitNzb($input: SubmitNzbInput!) {
    submitNzb(input: $input) {
      accepted
      clientRequestId
      item {
        id
        error
      }
    }
  }
`;

type SubmitNzbUploadResponse = {
  data?: {
    submitNzb?: {
      accepted: boolean;
      clientRequestId?: string | null;
      item?: {
        id: number;
        error?: string | null;
      } | null;
    } | null;
  };
  errors?: Array<{ message: string }>;
};

async function submitUploadedNzb(input: {
  file: File;
  password: string;
  category: string;
  priority: string;
}): Promise<SubmitNzbUploadResponse> {
  const form = new FormData();
  form.append(
    "operations",
    JSON.stringify({
      query: SUBMIT_NZB_UPLOAD_OPERATION,
      variables: {
        input: {
          nzbUpload: null,
          filename: input.file.name,
          password: input.password.trim() || null,
          category:
            input.category.trim() && input.category !== NO_CATEGORY_VALUE
              ? input.category
              : null,
          attributes: [{ key: "priority", value: input.priority }],
        },
      },
    }),
  );
  form.append("map", JSON.stringify({ "0": ["variables.input.nzbUpload"] }));
  form.append("0", input.file, input.file.name);

  const response = await fetch(new URL("graphql", document.baseURI).href, {
    method: "POST",
    headers: authHeaders(),
    body: form,
    credentials: "same-origin",
  });

  const payload = (await response.json()) as SubmitNzbUploadResponse;
  if (!response.ok && (!payload.errors || payload.errors.length === 0)) {
    throw new Error(`Upload failed with status ${response.status}`);
  }
  return payload;
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

  const removeFile = useCallback((targetFile: File) => {
    setError(null);
    setFiles((currentFiles) =>
      currentFiles.filter(
        (file) =>
          !(
            file.name === targetFile.name &&
            file.size === targetFile.size &&
            file.lastModified === targetFile.lastModified
          ),
      ),
    );
  }, []);

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
        const result = await submitUploadedNzb({
          file,
          password,
          category,
          priority,
        });

        if (result.errors?.length) {
          const prefix =
            submittedCount > 0
              ? t("upload.partialFailure", {
                  submitted: submittedCount,
                  total: files.length,
                  name: file.name,
                })
              : t("upload.batchFailure", { name: file.name });
          setError(`${prefix} ${result.errors[0]?.message ?? t("upload.rejected")}`);
          return false;
        }

        if (!result.data?.submitNzb?.accepted) {
          const prefix =
            submittedCount > 0
              ? t("upload.partialFailure", {
                  submitted: submittedCount,
                  total: files.length,
                  name: file.name,
                })
              : t("upload.batchFailure", { name: file.name });
          const rejectionMessage =
            result.data?.submitNzb?.item?.error ?? t("upload.rejected");
          setError(`${prefix} ${rejectionMessage}`);
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
  }, [category, files, options, password, priority, t]);

  return {
    categories: categoryData?.categories ?? [],
    dragging,
    error,
    files,
    fileInputRef,
    fetching: submitting,
    password,
    priority,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    handleDrop,
    handleFiles,
    removeFile,
    openPicker: () => fileInputRef.current?.click(),
    submit,
    onFileInputChange: (event: ChangeEvent<HTMLInputElement>) => {
      handleFiles(Array.from(event.target.files ?? []));
      event.target.value = "";
    },
  };
}

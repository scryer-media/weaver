import { useCallback, useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { CATEGORIES_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

const NO_CATEGORY_VALUE = "__none__";

const STAGE_NZB_UPLOAD_OPERATION = `
  mutation StageNzbUpload($input: StageNzbUploadInput!) {
    stageNzbUpload(input: $input) {
      accepted
      stagedUploadId
      filename
      displayName
      totalFiles
      totalBytes
      error
    }
  }
`;

const SUBMIT_STAGED_NZBS_OPERATION = `
  mutation SubmitStagedNzbs($input: SubmitStagedNzbsInput!) {
    submitStagedNzbs(input: $input) {
      acceptedCount
      clientRequestId
      results {
        stagedUploadId
        accepted
        retained
        error
        item {
          id
        }
      }
    }
  }
`;

const DISCARD_STAGED_NZBS_OPERATION = `
  mutation DiscardStagedNzbs($ids: [String!]!) {
    discardStagedNzbs(ids: $ids)
  }
`;

export type UploadNzbEntryStatus =
  | "queued"
  | "staging"
  | "staged"
  | "failed"
  | "submitting"
  | "submitted";

export type UploadNzbEntry = {
  localId: string;
  file: File;
  status: UploadNzbEntryStatus;
  stagedUploadId?: string;
  error?: string;
  displayName?: string;
  totalFiles?: number;
  totalBytes?: number;
};

type StageNzbUploadResponse = {
  data?: {
    stageNzbUpload?: {
      accepted: boolean;
      stagedUploadId?: string | null;
      filename?: string | null;
      displayName?: string | null;
      totalFiles?: number | null;
      totalBytes?: number | null;
      error?: string | null;
    } | null;
  };
  errors?: Array<{ message: string }>;
};

type SubmitStagedNzbsResponse = {
  data?: {
    submitStagedNzbs?: {
      acceptedCount: number;
      clientRequestId?: string | null;
      results?: Array<{
        stagedUploadId: string;
        accepted: boolean;
        retained: boolean;
        error?: string | null;
        item?: {
          id: number;
        } | null;
      }> | null;
    } | null;
  };
  errors?: Array<{ message: string }>;
};

function graphqlUrl(): string {
  return new URL("graphql", document.baseURI).href;
}

function graphqlErrorMessage(
  payload: { errors?: Array<{ message: string }> } | undefined,
): string | null {
  return payload?.errors?.find((entry) => entry.message)?.message ?? null;
}

async function stageNzbUpload(
  file: File,
  signal: AbortSignal,
): Promise<StageNzbUploadResponse> {
  const form = new FormData();
  form.append(
    "operations",
    JSON.stringify({
      query: STAGE_NZB_UPLOAD_OPERATION,
      variables: {
        input: {
          nzbUpload: null,
          filename: file.name,
        },
      },
    }),
  );
  form.append("map", JSON.stringify({ "0": ["variables.input.nzbUpload"] }));
  form.append("0", file, file.name);

  const response = await fetch(graphqlUrl(), {
    method: "POST",
    headers: authHeaders(),
    body: form,
    credentials: "same-origin",
    signal,
  });

  const payload = (await response.json()) as StageNzbUploadResponse;
  if (!response.ok && (!payload.errors || payload.errors.length === 0)) {
    throw new Error(`Upload failed with status ${response.status}`);
  }
  return payload;
}

async function submitStagedNzbs(input: {
  stagedUploadIds: string[];
  password: string;
  category: string;
  priority: string;
}): Promise<SubmitStagedNzbsResponse> {
  const response = await fetch(graphqlUrl(), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...authHeaders(),
    },
    credentials: "same-origin",
    body: JSON.stringify({
      query: SUBMIT_STAGED_NZBS_OPERATION,
      variables: {
        input: {
          stagedUploadIds: input.stagedUploadIds,
          password: input.password.trim() || null,
          category:
            input.category.trim() && input.category !== NO_CATEGORY_VALUE
              ? input.category
              : null,
          attributes: [{ key: "priority", value: input.priority }],
        },
      },
    }),
  });

  const payload = (await response.json()) as SubmitStagedNzbsResponse;
  if (!response.ok && (!payload.errors || payload.errors.length === 0)) {
    throw new Error(`Submit failed with status ${response.status}`);
  }
  return payload;
}

async function discardStagedNzbs(
  ids: string[],
  options?: { keepalive?: boolean },
): Promise<void> {
  if (ids.length === 0) {
    return;
  }

  const response = await fetch(graphqlUrl(), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...authHeaders(),
    },
    credentials: "same-origin",
    keepalive: options?.keepalive,
    body: JSON.stringify({
      query: DISCARD_STAGED_NZBS_OPERATION,
      variables: { ids },
    }),
  });

  if (!response.ok) {
    throw new Error(`Discard failed with status ${response.status}`);
  }
}

function makeLocalId(): string {
  return globalThis.crypto?.randomUUID?.() ?? `upload-${Date.now()}-${Math.random()}`;
}

export function useUploadNzb(options?: {
  resetOnOpen?: boolean;
  open?: boolean;
  onSubmitted?: () => void;
}) {
  const t = useTranslate();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const controllerRef = useRef(new Map<string, AbortController>());
  const generationRef = useRef(0);
  const mountedRef = useRef(true);
  const entriesRef = useRef<UploadNzbEntry[]>([]);
  const [entries, setEntries] = useState<UploadNzbEntry[]>([]);
  const [password, setPassword] = useState("");
  const [category, setCategory] = useState(NO_CATEGORY_VALUE);
  const [priority, setPriority] = useState("NORMAL");
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });

  const setEntriesState = useCallback(
    (updater: UploadNzbEntry[] | ((current: UploadNzbEntry[]) => UploadNzbEntry[])) => {
      const next =
        typeof updater === "function"
          ? (updater as (current: UploadNzbEntry[]) => UploadNzbEntry[])(entriesRef.current)
          : updater;
      entriesRef.current = next;
      setEntries(next);
    },
    [],
  );

  const discardKnownStageIds = useCallback((ids: string[], keepalive = false) => {
    if (ids.length === 0) {
      return;
    }
    void discardStagedNzbs(ids, { keepalive }).catch(() => {});
  }, []);

  const cleanupEntries = useCallback(
    (currentEntries: UploadNzbEntry[], keepalive = false) => {
      for (const entry of currentEntries) {
        controllerRef.current.get(entry.localId)?.abort();
        controllerRef.current.delete(entry.localId);
      }
      discardKnownStageIds(
        currentEntries
          .map((entry) => entry.stagedUploadId)
          .filter((value): value is string => !!value),
        keepalive,
      );
    },
    [discardKnownStageIds],
  );

  const isEntryCurrent = useCallback((localId: string, generation: number) => {
    return (
      mountedRef.current &&
      generationRef.current === generation &&
      entriesRef.current.some((entry) => entry.localId === localId)
    );
  }, []);

  const stageEntry = useCallback(
    async (entry: UploadNzbEntry, generation: number) => {
      const controller = new AbortController();
      controllerRef.current.set(entry.localId, controller);
      setEntriesState((current) =>
        current.map((candidate) =>
          candidate.localId === entry.localId
            ? { ...candidate, status: "staging", error: undefined }
            : candidate,
        ),
      );

      try {
        const payload = await stageNzbUpload(entry.file, controller.signal);
        const stageError =
          graphqlErrorMessage(payload) ??
          payload.data?.stageNzbUpload?.error ??
          (!payload.data?.stageNzbUpload?.accepted ? t("upload.rejected") : null);

        const stagedUploadId = payload.data?.stageNzbUpload?.stagedUploadId ?? undefined;
        if (!isEntryCurrent(entry.localId, generation)) {
          if (stagedUploadId) {
            discardKnownStageIds([stagedUploadId], !mountedRef.current);
          }
          return;
        }

        if (stageError || !stagedUploadId) {
          setEntriesState((current) =>
            current.map((candidate) =>
              candidate.localId === entry.localId
                ? {
                    ...candidate,
                    status: "failed",
                    stagedUploadId: undefined,
                    error: stageError ?? t("upload.rejected"),
                  }
                : candidate,
            ),
          );
          return;
        }

        setEntriesState((current) =>
          current.map((candidate) =>
            candidate.localId === entry.localId
              ? {
                  ...candidate,
                  status: "staged",
                  stagedUploadId,
                  error: undefined,
                  displayName:
                    payload.data?.stageNzbUpload?.displayName ?? candidate.displayName,
                  totalFiles: payload.data?.stageNzbUpload?.totalFiles ?? undefined,
                  totalBytes: payload.data?.stageNzbUpload?.totalBytes ?? undefined,
                }
              : candidate,
          ),
        );
      } catch (stageError) {
        if (!isEntryCurrent(entry.localId, generation)) {
          return;
        }
        if (
          stageError instanceof DOMException &&
          stageError.name === "AbortError"
        ) {
          return;
        }
        setEntriesState((current) =>
          current.map((candidate) =>
            candidate.localId === entry.localId
              ? {
                  ...candidate,
                  status: "failed",
                  stagedUploadId: undefined,
                  error:
                    stageError instanceof Error ? stageError.message : t("upload.rejected"),
                }
              : candidate,
          ),
        );
      } finally {
        if (controllerRef.current.get(entry.localId) === controller) {
          controllerRef.current.delete(entry.localId);
        }
      }
    },
    [discardKnownStageIds, isEntryCurrent, setEntriesState, t],
  );

  const replaceEntries = useCallback(
    (nextFiles: File[]) => {
      const nextGeneration = generationRef.current + 1;
      generationRef.current = nextGeneration;
      cleanupEntries(entriesRef.current);

      if (nextFiles.length === 0) {
        setEntriesState([]);
        return;
      }

      const nextEntries = nextFiles.map<UploadNzbEntry>((file) => ({
        localId: makeLocalId(),
        file,
        status: "queued",
      }));
      setEntriesState(nextEntries);
      nextEntries.forEach((entry) => {
        void stageEntry(entry, nextGeneration);
      });
    },
    [cleanupEntries, setEntriesState, stageEntry],
  );

  useEffect(() => {
    if (options?.resetOnOpen && options.open) {
      replaceEntries([]);
      setPassword("");
      setCategory(NO_CATEGORY_VALUE);
      setPriority("NORMAL");
      setDragging(false);
      setError(null);
    }
  }, [options?.open, options?.resetOnOpen, replaceEntries]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      generationRef.current += 1;
      cleanupEntries(entriesRef.current, true);
    };
  }, [cleanupEntries]);

  const handleFiles = useCallback(
    (nextFiles: File[]) => {
      setError(null);
      if (nextFiles.length > 0 && nextFiles.some((file) => !file.name.toLowerCase().endsWith(".nzb"))) {
        setError(t("upload.invalidFiles"));
        return;
      }
      replaceEntries(nextFiles);
    },
    [replaceEntries, t],
  );

  const removeFile = useCallback(
    (localId: string) => {
      setError(null);
      const target = entriesRef.current.find((entry) => entry.localId === localId);
      if (!target) {
        return;
      }
      controllerRef.current.get(localId)?.abort();
      controllerRef.current.delete(localId);
      if (target.stagedUploadId) {
        discardKnownStageIds([target.stagedUploadId]);
      }
      setEntriesState((current) => current.filter((entry) => entry.localId !== localId));
    },
    [discardKnownStageIds, setEntriesState],
  );

  const retryFile = useCallback(
    (localId: string) => {
      const existing = entriesRef.current.find((entry) => entry.localId === localId);
      if (!existing) {
        return;
      }

      controllerRef.current.get(localId)?.abort();
      controllerRef.current.delete(localId);
      if (existing.stagedUploadId) {
        discardKnownStageIds([existing.stagedUploadId]);
      }

      const generation = generationRef.current;
      const retryEntry: UploadNzbEntry = {
        ...existing,
        status: "queued",
        stagedUploadId: undefined,
        error: undefined,
      };
      setEntriesState((current) =>
        current.map((entry) => (entry.localId === localId ? retryEntry : entry)),
      );
      void stageEntry(retryEntry, generation);
    },
    [discardKnownStageIds, setEntriesState, stageEntry],
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
    const readyEntries = entriesRef.current.filter((entry) => entry.status === "staged" && entry.stagedUploadId);
    const hasPendingStages = entriesRef.current.some(
      (entry) => entry.status === "queued" || entry.status === "staging",
    );
    if (readyEntries.length === 0 || hasPendingStages) {
      return false;
    }

    setError(null);
    const readyIds = new Set(readyEntries.map((entry) => entry.localId));
    setEntriesState((current) =>
      current.map((entry) =>
        readyIds.has(entry.localId)
          ? { ...entry, status: "submitting", error: undefined }
          : entry,
      ),
    );

    try {
      const payload = await submitStagedNzbs({
        stagedUploadIds: readyEntries
          .map((entry) => entry.stagedUploadId)
          .filter((value): value is string => !!value),
        password,
        category,
        priority,
      });

      const submitError = graphqlErrorMessage(payload);
      if (submitError) {
        setError(submitError);
        setEntriesState((current) =>
          current.map((entry) =>
            entry.status === "submitting"
              ? { ...entry, status: "staged", error: submitError }
              : entry,
          ),
        );
        return false;
      }

      const resultMap = new Map(
        (payload.data?.submitStagedNzbs?.results ?? []).map((result) => [result.stagedUploadId, result]),
      );

      let shouldClose = false;
      setEntriesState((current) => {
        const nextEntries: UploadNzbEntry[] = [];
        for (const entry of current) {
          if (entry.status !== "submitting") {
            nextEntries.push(entry);
            continue;
          }

          const stagedUploadId = entry.stagedUploadId;
          if (!stagedUploadId) {
            nextEntries.push({ ...entry, status: "failed", error: t("upload.stageExpired") });
            continue;
          }

          const result = resultMap.get(stagedUploadId);
          if (!result) {
            nextEntries.push({
              ...entry,
              status: "staged",
              error: t("upload.rejected"),
            });
            continue;
          }

          if (result.accepted) {
            continue;
          }

          if (result.retained) {
            nextEntries.push({
              ...entry,
              status: "staged",
              error: result.error ?? t("upload.rejected"),
            });
            continue;
          }

          nextEntries.push({
            ...entry,
            status: "failed",
            stagedUploadId: undefined,
            error: result.error ?? t("upload.stageExpired"),
          });
        }

        shouldClose = nextEntries.length === 0;
        return nextEntries;
      });

      if (shouldClose) {
        if (fileInputRef.current) {
          fileInputRef.current.value = "";
        }
        options?.onSubmitted?.();
      }

      return (payload.data?.submitStagedNzbs?.acceptedCount ?? 0) > 0;
    } catch (submissionError) {
      const message =
        submissionError instanceof Error ? submissionError.message : t("upload.rejected");
      setError(message);
      setEntriesState((current) =>
        current.map((entry) =>
          entry.status === "submitting"
            ? {
                ...entry,
                status: entry.stagedUploadId ? "staged" : "failed",
                error: message,
              }
            : entry,
        ),
      );
      return false;
    }
  }, [category, options, password, priority, setEntriesState, t]);

  const staging = entries.some((entry) => entry.status === "queued" || entry.status === "staging");
  const fetching = entries.some((entry) => entry.status === "submitting");
  const readyCount = entries.filter((entry) => entry.status === "staged").length;
  const failedCount = entries.filter((entry) => entry.status === "failed").length;
  const totalBytes = entries.reduce((sum, entry) => sum + entry.file.size, 0);

  return {
    categories: categoryData?.categories ?? [],
    dragging,
    entries,
    error,
    fileInputRef,
    fetching,
    staging,
    readyCount,
    failedCount,
    totalBytes,
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
    retryFile,
    openPicker: () => fileInputRef.current?.click(),
    submit,
    onFileInputChange: (event: ChangeEvent<HTMLInputElement>) => {
      handleFiles(Array.from(event.target.files ?? []));
      event.target.value = "";
    },
  };
}

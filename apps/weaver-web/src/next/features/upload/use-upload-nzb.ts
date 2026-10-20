// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { useQuery } from "urql";
import { authHeaders, fetchWithSessionRetry } from "@/graphql/client";
import { CATEGORIES_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { shouldCloseAfterSubmit } from "@/next/features/upload/close-decision";
import {
  duplicateSubmissionInput,
  type DuplicateSubmissionMode,
} from "@/next/features/upload/duplicate-submission";
import { submissionStatusIsDurable } from "@/next/features/duplicates/duplicate-presentation";

const NO_CATEGORY_VALUE = "__none__";

const STAGE_NZB_UPLOAD_OPERATION = `
  mutation StageNzbUpload($input: StageNzbUploadInput!) {
    stageNzbUpload(input: $input) {
      kind
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
        status
        semanticDuplicate {
          groupId
          score
          state
          supersededJobId
        }
        error
        item {
          id
        }
      }
    }
  }
`;

const SUBMIT_OPERATION = `
  mutation Submit($input: SubmitInput!) {
    submit(input: $input) {
      accepted
      status
      message
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

/** What the daemon read a file as. */
export type SubmissionKind = "NZB" | "TORRENT";

export type UploadNzbEntry = {
  localId: string;
  file: File;
  status: UploadNzbEntryStatus;
  /** Set once staging has read the file; a torrent is not staged, it is sent whole on submit. */
  kind?: SubmissionKind;
  stagedUploadId?: string;
  error?: string;
  displayName?: string;
  totalFiles?: number;
  totalBytes?: number;
  submissionStatus?: string;
  semanticDuplicate?: {
    groupId: number;
    score: number;
    state: string;
    supersededJobId?: number | null;
  };
};

type StageNzbUploadResponse = {
  data?: {
    stageNzbUpload?: {
      kind?: SubmissionKind;
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
        status?: string | null;
        semanticDuplicate?: {
          groupId: number;
          score: number;
          state: string;
          supersededJobId?: number | null;
        } | null;
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

async function parseJsonResponse<T>(response: Response): Promise<T | undefined> {
  const text = await response.text();
  if (!text.trim()) {
    return undefined;
  }

  return JSON.parse(text) as T;
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

  const response = await fetchWithSessionRetry(graphqlUrl(), {
    method: "POST",
    headers: authHeaders(),
    body: form,
    credentials: "same-origin",
    signal,
  });

  const payload = await parseJsonResponse<StageNzbUploadResponse>(response);
  if (!response.ok) {
    throw new Error(graphqlErrorMessage(payload) ?? `Upload failed with status ${response.status}`);
  }
  if (!payload) {
    throw new Error("Upload failed: empty response body");
  }
  return payload;
}

async function submitStagedNzbs(input: {
  stagedUploadIds: string[];
  password: string;
  category: string;
  priority: string;
  paused: boolean;
  duplicate: ReturnType<typeof duplicateSubmissionInput>;
}): Promise<SubmitStagedNzbsResponse> {
  const response = await fetchWithSessionRetry(graphqlUrl(), {
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
          paused: input.paused,
          ...input.duplicate,
        },
      },
    }),
  });

  const payload = await parseJsonResponse<SubmitStagedNzbsResponse>(response);
  if (!response.ok) {
    throw new Error(graphqlErrorMessage(payload) ?? `Submit failed with status ${response.status}`);
  }
  if (!payload) {
    throw new Error("Submit failed: empty response body");
  }
  return payload;
}

type SubmitResponse = {
  data?: {
    submit?: { accepted: boolean; status: string; message?: string | null } | null;
  };
  errors?: Array<{ message: string }>;
};

/**
 * Sends one text or file through `submit`, which reads it for what it is.
 * Resolves to the daemon's refusal in its own words, or null once it is in.
 */
async function submitOne(input: {
  text?: string;
  file?: File;
  category: string;
  paused: boolean;
  password: string;
}): Promise<string | null> {
  const operations = {
    query: SUBMIT_OPERATION,
    variables: {
      input: {
        text: input.text ?? null,
        upload: null,
        category:
          input.category.trim() && input.category !== NO_CATEGORY_VALUE ? input.category : null,
        paused: input.paused,
        password: input.password.trim() || null,
      },
    },
  };
  let request: RequestInit;
  if (input.file) {
    const form = new FormData();
    form.append("operations", JSON.stringify(operations));
    form.append("map", JSON.stringify({ "0": ["variables.input.upload"] }));
    form.append("0", input.file, input.file.name);
    request = { method: "POST", headers: authHeaders(), body: form };
  } else {
    request = {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify(operations),
    };
  }
  const response = await fetchWithSessionRetry(graphqlUrl(), { ...request, credentials: "same-origin" });
  const payload = await parseJsonResponse<SubmitResponse>(response);
  const refusal = graphqlErrorMessage(payload);
  if (refusal) return refusal;
  const result = payload?.data?.submit;
  if (!response.ok || !result) return `Submit failed with status ${response.status}`;
  return result.accepted ? null : (result.message ?? result.status);
}

async function discardStagedNzbs(
  ids: string[],
  options?: { keepalive?: boolean },
): Promise<void> {
  if (ids.length === 0) {
    return;
  }

  const response = await fetchWithSessionRetry(graphqlUrl(), {
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

function fileIdentity(file: File): string {
  return `${file.name}\u0000${file.size}\u0000${file.lastModified}`;
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
  const [paused, setPaused] = useState(false);
  // One NZB or torrent URL, magnet link or infohash, sent as it is on submit.
  const [text, setTextState] = useState("");
  const [textError, setTextError] = useState<string | null>(null);
  const [force, setForce] = useState(false);
  const [duplicateMode, setDuplicateMode] = useState<DuplicateSubmissionMode>("ENFORCE");
  const [duplicateKey, setDuplicateKey] = useState("");
  const [duplicateScore, setDuplicateScore] = useState("");
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
        if (payload.data?.stageNzbUpload?.kind === "TORRENT" && !graphqlErrorMessage(payload)) {
          if (isEntryCurrent(entry.localId, generation)) {
            setEntriesState((current) =>
              current.map((candidate) =>
                candidate.localId === entry.localId
                  ? { ...candidate, status: "staged", kind: "TORRENT", error: undefined }
                  : candidate,
              ),
            );
          }
          return;
        }
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
                  kind: "NZB",
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
      setPaused(false);
      setTextState("");
      setTextError(null);
      setForce(false);
      setDuplicateMode("ENFORCE");
      setDuplicateKey("");
      setDuplicateScore("");
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
      replaceEntries(nextFiles);
    },
    [replaceEntries],
  );

  const addFiles = useCallback(
    (nextFiles: File[]) => {
      setError(null);
      // Picking the same file a second time adds nothing: it is already listed.
      // What a file is, the daemon reads from its bytes as it stages it.
      const listed = new Set(entriesRef.current.map((entry) => fileIdentity(entry.file)));
      const added = nextFiles
        .filter((file) => !listed.has(fileIdentity(file)))
        .map<UploadNzbEntry>((file) => ({ localId: makeLocalId(), file, status: "queued" }));
      if (added.length === 0) {
        return;
      }
      const generation = generationRef.current;
      setEntriesState((current) => [...current, ...added]);
      added.forEach((entry) => {
        void stageEntry(entry, generation);
      });
    },
    [setEntriesState, stageEntry],
  );

  const setText = useCallback((value: string) => {
    setTextState(value);
    setTextError(null);
  }, []);

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

  const submit = useCallback(async (submitOptions?: { force?: boolean; localIds?: string[] }) => {
    const selectedIds = submitOptions?.localIds ? new Set(submitOptions.localIds) : null;
    const readyEntries = entriesRef.current.filter(
      (entry) =>
        entry.status === "staged" &&
        (entry.stagedUploadId || entry.kind === "TORRENT") &&
        (!selectedIds || selectedIds.has(entry.localId)),
    );
    const nzbEntries = readyEntries.filter((entry) => entry.stagedUploadId);
    const torrentEntries = readyEntries.filter((entry) => entry.kind === "TORRENT");
    // A forced resubmit of chosen files leaves the text alone.
    const sentText = selectedIds ? "" : text.trim();
    const hasPendingStages = entriesRef.current.some(
      (entry) => entry.status === "queued" || entry.status === "staging",
    );
    if ((readyEntries.length === 0 && sentText === "") || hasPendingStages) {
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

    // The text and each torrent go on their own; a refusal stays on its row for another try.
    const shared = { category, paused, password };
    let anyAccepted = false;
    const textOutcome: UploadNzbEntryStatus[] = [];
    if (sentText !== "") {
      const refusal = await submitOne({ ...shared, text: sentText }).catch((cause: unknown) =>
        cause instanceof Error ? cause.message : t("upload.rejected"),
      );
      if (refusal === null) {
        anyAccepted = true;
        setTextState("");
      } else {
        setTextError(refusal);
      }
      textOutcome.push(refusal === null ? "submitted" : "failed");
    }
    for (const entry of torrentEntries) {
      const refusal = await submitOne({ ...shared, file: entry.file }).catch((cause: unknown) =>
        cause instanceof Error ? cause.message : t("upload.rejected"),
      );
      anyAccepted ||= refusal === null;
      setEntriesState((current) =>
        current.map((candidate) =>
          candidate.localId === entry.localId
            ? refusal === null
              ? { ...candidate, status: "submitted", error: undefined }
              : { ...candidate, status: "staged", error: refusal }
            : candidate,
        ),
      );
    }

    const closeIfDone = () => {
      // The text counts as one more item: the dialog closes once everything sent is in.
      const statuses = [...entriesRef.current.map((entry) => entry.status), ...textOutcome];
      if (shouldCloseAfterSubmit(statuses)) {
        if (fileInputRef.current) {
          fileInputRef.current.value = "";
        }
        options?.onSubmitted?.();
      }
    };
    if (nzbEntries.length === 0) {
      closeIfDone();
      return anyAccepted;
    }

    try {
      const payload = await submitStagedNzbs({
        stagedUploadIds: nzbEntries
          .map((entry) => entry.stagedUploadId)
          .filter((value): value is string => !!value),
        password,
        category,
        priority,
        paused,
        duplicate: duplicateSubmissionInput({
          force: submitOptions?.force || force,
          mode: duplicateMode,
          key: duplicateKey,
          score: duplicateScore,
        }),
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

          const submissionStatus = result.status ?? (result.accepted ? "ACCEPTED" : undefined);
          const outcome = {
            submissionStatus,
            semanticDuplicate: result.semanticDuplicate ?? undefined,
          };

          if (submissionStatusIsDurable(submissionStatus) || result.accepted) {
            nextEntries.push({ ...entry, status: "submitted", error: undefined, ...outcome });
            continue;
          }

          if (result.retained) {
            nextEntries.push({
              ...entry,
              status: "staged",
              error: result.error ?? undefined,
              ...outcome,
            });
            continue;
          }

          nextEntries.push({
            ...entry,
            status: "failed",
            stagedUploadId: undefined,
            error: result.error ?? undefined,
            ...outcome,
          });
        }
        return nextEntries;
      });

      // Close once every entry reached a successful terminal state (see
      // shouldCloseAfterSubmit). Submitted entries are retained in the list so
      // the queue-duplicate "accepted with warning" outcome can render.
      closeIfDone();

      return anyAccepted || (payload.data?.submitStagedNzbs?.acceptedCount ?? 0) > 0;
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
  }, [
    category,
    duplicateKey,
    duplicateMode,
    duplicateScore,
    force,
    options,
    password,
    paused,
    priority,
    setEntriesState,
    t,
    text,
  ]);

  const staging = entries.some((entry) => entry.status === "queued" || entry.status === "staging");
  const fetching = entries.some((entry) => entry.status === "submitting");
  const readyCount =
    entries.filter((entry) => entry.status === "staged").length + (text.trim() === "" ? 0 : 1);
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
    paused,
    text,
    textError,
    force,
    duplicateMode,
    duplicateKey,
    duplicateScore,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    setPaused,
    setText,
    setForce,
    setDuplicateMode,
    setDuplicateKey,
    setDuplicateScore,
    handleDrop,
    handleFiles,
    addFiles,
    removeFile,
    retryFile,
    forceSubmitFile: (localId: string) => submit({ force: true, localIds: [localId] }),
    openPicker: () => fileInputRef.current?.click(),
    submit,
    onFileInputChange: (event: ChangeEvent<HTMLInputElement>) => {
      handleFiles(Array.from(event.target.files ?? []));
      event.target.value = "";
    },
  };
}

import {
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type RowSelectionState,
  type SortingState,
} from "@tanstack/react-table";
import { Bug, Download, RefreshCcw, Trash2 } from "lucide-react";
import { useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import { Link } from "react-router";
import { useClient, useMutation, useQuery, useSubscription } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { DataTable } from "@/components/data-table/DataTable";
import type { DataTableColumnMeta } from "@/components/data-table/DataTable";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { DataTablePagination } from "@/components/data-table/DataTablePagination";
import { DataTableToolbar } from "@/components/data-table/DataTableToolbar";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { formatBytes } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";
import { useTablePreferences } from "@/lib/hooks/use-table-preferences";
import {
  formatJobReleaseName,
  normalizeJobData,
  type DeleteOperationData,
  type GraphqlJobData,
  type JobData,
} from "@/lib/job-types";
import { cn } from "@/lib/utils";
import {
  ACCEPT_HISTORY_DELETE_MUTATION,
  HISTORY_FACADE_EVENTS_SUBSCRIPTION,
  HISTORY_DELETE_OPERATIONS_QUERY,
  HISTORY_PAGE_QUERY,
  REDOWNLOAD_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  START_DIAGNOSTIC_REDOWNLOAD_MUTATION,
} from "@/graphql/queries";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";

type HistoryJob = JobData;
type HistoryFilter = "all" | "success" | "failure";
type FacadeHistoryJob = GraphqlJobData;
type HistoryPageCounts = {
  all: number;
  success: number;
  failure: number;
};
type HistoryPageResponse = {
  historyPage: {
    items: FacadeHistoryJob[];
    totalCount: number;
    counts: HistoryPageCounts;
  };
};
type HistoryDeleteAcceptanceResponse = {
  acceptHistoryDelete: {
    operationId: number;
    acceptedIds: number[];
    totalTargets: number;
  };
};
type HistoryDeleteOperationSummary = {
  id: number;
  state: "QUEUED" | "RUNNING" | "COMPLETED" | "COMPLETED_WITH_ERRORS";
  deleteFiles: boolean;
  totalTargets: number;
  queuedTargets: number;
  runningTargets: number;
  completedTargets: number;
  failedTargets: number;
  requestedAt: string;
};
type HistoryDeleteOperationsResponse = {
  historyDeleteOperations: HistoryDeleteOperationSummary[];
};
type HistoryTablePreferences = {
  pageSize: number;
  search: string;
  status: HistoryFilter;
  sorting: SortingState;
};
type LocalDeleteLock = {
  operationId: number;
  deleteFiles: boolean;
};

const HISTORY_PAGE_SIZE_OPTIONS = [25, 50, 100, 500] as const;
const DEFAULT_HISTORY_SORTING: SortingState = [{ id: "completedAt", desc: true }];
const DEFAULT_HISTORY_PREFERENCES: HistoryTablePreferences = {
  pageSize: 100,
  search: "",
  status: "all",
  sorting: DEFAULT_HISTORY_SORTING,
};

function normalizeHistoryJob(job: FacadeHistoryJob): HistoryJob {
  return normalizeJobData(job);
}

function historyStatusToGraphql(filter: HistoryFilter): "ALL" | "SUCCESS" | "FAILURE" {
  switch (filter) {
    case "success":
      return "SUCCESS";
    case "failure":
      return "FAILURE";
    default:
      return "ALL";
  }
}

function historySortingToGraphql(sorting: SortingState) {
  const current = sorting[0];
  switch (current?.id) {
    case "name":
      return { sortField: "NAME" as const, sortDirection: current.desc ? "DESC" : "ASC" };
    case "status":
      return { sortField: "STATE" as const, sortDirection: current.desc ? "DESC" : "ASC" };
    case "health":
      return { sortField: "HEALTH" as const, sortDirection: current.desc ? "DESC" : "ASC" };
    case "size":
      return { sortField: "SIZE" as const, sortDirection: current.desc ? "DESC" : "ASC" };
    case "category":
      return { sortField: "CATEGORY" as const, sortDirection: current.desc ? "DESC" : "ASC" };
    default:
      return { sortField: "COMPLETED_AT" as const, sortDirection: current?.desc === false ? "ASC" : "DESC" };
  }
}

function buildHistoryPageInput(
  preferences: HistoryTablePreferences,
  search: string,
  pageIndex: number,
) {
  return {
    pageIndex,
    pageSize: preferences.pageSize,
    search: search.length > 0 ? search : undefined,
    status: historyStatusToGraphql(preferences.status),
    ...historySortingToGraphql(preferences.sorting),
  };
}

function removeRowSelectionIds(
  selection: RowSelectionState,
  ids: number[],
): RowSelectionState {
  const next = { ...selection };
  for (const id of ids) {
    delete next[String(id)];
  }
  return next;
}

function isDefaultHistorySorting(sorting: SortingState) {
  return sorting.length === 1
    && sorting[0]?.id === DEFAULT_HISTORY_SORTING[0]?.id
    && sorting[0]?.desc === DEFAULT_HISTORY_SORTING[0]?.desc;
}

function localDeleteOperation(lock: LocalDeleteLock): DeleteOperationData {
  return {
    operationId: lock.operationId,
    state: "QUEUED",
    locked: true,
    deleteFiles: lock.deleteFiles,
    errorMessage: null,
  };
}

function isDiagnosticRunActive(
  diagnosticRun: HistoryJob["diagnosticRun"],
) {
  return diagnosticRun != null
    && ["QUEUED", "RUNNING", "COLLECTING", "UPLOADING"].includes(diagnosticRun.stage);
}

function diagnosticStageLabel(
  t: ReturnType<typeof useTranslate>,
  stage: NonNullable<HistoryJob["diagnosticRun"]>["stage"],
) {
  switch (stage) {
    case "QUEUED":
      return t("history.diagnosticQueued");
    case "RUNNING":
      return t("history.diagnosticRunning");
    case "COLLECTING":
      return t("history.diagnosticCollecting");
    case "UPLOADING":
      return t("history.diagnosticUploading");
    case "COMPLETE":
      return t("history.diagnosticComplete");
    case "FAILED":
      return t("history.diagnosticFailed");
    default:
      return stage;
  }
}

function formatDiagnosticSummary(
  t: ReturnType<typeof useTranslate>,
  job: HistoryJob,
) {
  const diagnosticRun = job.diagnosticRun;
  if (diagnosticRun) {
    const stageLabel = diagnosticStageLabel(t, diagnosticRun.stage);
    if (diagnosticRun.diagnosticId) {
      return t("history.diagnosticWithId", {
        stage: stageLabel,
        id: diagnosticRun.diagnosticId,
      });
    }
    if (diagnosticRun.errorMessage) {
      return `${stageLabel}: ${diagnosticRun.errorMessage}`;
    }
    return stageLabel;
  }
  if (job.lastDiagnosticId) {
    return t("history.lastDiagnosticId", { id: job.lastDiagnosticId });
  }
  return "";
}

export function History() {
  const t = useTranslate();
  const client = useClient();
  const [historyPreferences, setHistoryPreferences] = useTablePreferences(
    "weaver.history.table.preferences",
    DEFAULT_HISTORY_PREFERENCES,
  );
  const [pageIndex, setPageIndex] = useState(0);
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [redownloadConfirmId, setRedownloadConfirmId] = useState<number | null>(null);
  const [diagnosticConfirm, setDiagnosticConfirm] = useState<{
    id: number;
    includeServerHostnames: boolean;
  } | null>(null);
  const [deleteBatchConfirm, setDeleteBatchConfirm] = useState(false);
  const [deleteAllConfirm, setDeleteAllConfirm] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [deleteAcceptError, setDeleteAcceptError] = useState<string | null>(null);
  const [diagnosticAcceptError, setDiagnosticAcceptError] = useState<string | null>(null);
  const [acceptedDeleteLocks, setAcceptedDeleteLocks] = useState<Record<number, LocalDeleteLock>>(
    {},
  );
  const [hadActiveDeleteOperations, setHadActiveDeleteOperations] = useState(false);

  const deferredSearch = useDeferredValue(historyPreferences.search.trim());
  const historyPageInput = useMemo(
    () => buildHistoryPageInput(historyPreferences, deferredSearch, pageIndex),
    [deferredSearch, historyPreferences, pageIndex],
  );

  const [{ data, fetching }, reexecuteHistoryPage] = useQuery<HistoryPageResponse>({
    query: HISTORY_PAGE_QUERY,
    variables: { input: historyPageInput },
  });
  const [{ data: deleteOperationsData }, reexecuteHistoryDeleteOperations] =
    useQuery<HistoryDeleteOperationsResponse>({
      query: HISTORY_DELETE_OPERATIONS_QUERY,
      variables: { activeOnly: true },
    });

  const [acceptDeleteState, acceptHistoryDelete] =
    useMutation<HistoryDeleteAcceptanceResponse>(ACCEPT_HISTORY_DELETE_MUTATION);
  const [reprocessState, reprocessJob] = useMutation(REPROCESS_JOB_MUTATION);
  const [redownloadState, redownloadJob] = useMutation(REDOWNLOAD_JOB_MUTATION);
  const [diagnosticStartState, startDiagnosticRedownload] = useMutation(
    START_DIAGNOSTIC_REDOWNLOAD_MUTATION,
  );

  const rawJobs = useMemo(
    () => ((data?.historyPage.items ?? []) as FacadeHistoryJob[]).map(normalizeHistoryJob),
    [data?.historyPage.items],
  );
  const jobs = useMemo(
    () =>
      rawJobs.map((job) => ({
        ...job,
        deleteOperation:
          job.deleteOperation ?? (acceptedDeleteLocks[job.id]
            ? localDeleteOperation(acceptedDeleteLocks[job.id]!)
            : null),
      })),
    [acceptedDeleteLocks, rawJobs],
  );
  const deleteOperations = useMemo(
    () => deleteOperationsData?.historyDeleteOperations ?? [],
    [deleteOperationsData?.historyDeleteOperations],
  );
  const counts = data?.historyPage.counts ?? { all: 0, success: 0, failure: 0 };
  const totalCount = data?.historyPage.totalCount ?? 0;
  const pageCount = Math.max(1, Math.ceil(totalCount / historyPreferences.pageSize));
  const lockedRowIds = useMemo(
    () => new Set(jobs.filter((job) => job.deleteOperation?.locked).map((job) => job.id)),
    [jobs],
  );
  const selectedIds = useMemo(
    () => Object.entries(rowSelection)
      .filter(([, selected]) => selected)
      .map(([id]) => Number(id)),
    [rowSelection],
  );
  const selectedActionIds = useMemo(
    () => selectedIds.filter((id) => !lockedRowIds.has(id)),
    [lockedRowIds, selectedIds],
  );
  const selectedCount = selectedActionIds.length;
  const hasActiveDeleteOperations =
    deleteOperations.length > 0
    || jobs.some((job) => job.deleteOperation?.locked)
    || Object.keys(acceptedDeleteLocks).length > 0;
  const deleteProgress = useMemo(
    () => {
      const summary = deleteOperations.reduce(
        (current, operation) => ({
          totalTargets: current.totalTargets + operation.totalTargets,
          completedTargets: current.completedTargets + operation.completedTargets,
          failedTargets: current.failedTargets + operation.failedTargets,
          runningTargets: current.runningTargets + operation.runningTargets,
          queuedTargets: current.queuedTargets + operation.queuedTargets,
        }),
        {
          totalTargets: 0,
          completedTargets: 0,
          failedTargets: 0,
          runningTargets: 0,
          queuedTargets: 0,
        },
      );
      if (summary.totalTargets > 0) {
        return summary;
      }

      const pendingTargets = Object.keys(acceptedDeleteLocks).length;
      return {
        totalTargets: pendingTargets,
        completedTargets: 0,
        failedTargets: 0,
        runningTargets: 0,
        queuedTargets: pendingTargets,
      };
    },
    [acceptedDeleteLocks, deleteOperations],
  );
  const actionsBusy =
    acceptDeleteState.fetching
    || reprocessState.fetching
    || redownloadState.fetching
    || diagnosticStartState.fetching;
  const hasActiveDiagnosticRuns = useMemo(
    () => jobs.some((job) => isDiagnosticRunActive(job.diagnosticRun)),
    [jobs],
  );

  useEffect(() => {
    if (pageIndex >= pageCount && pageIndex > 0) {
      setPageIndex(pageCount - 1);
    }
  }, [pageCount, pageIndex]);

  useEffect(() => {
    setRowSelection({});
  }, [pageIndex, historyPreferences.pageSize, historyPreferences.search, historyPreferences.sorting, historyPreferences.status]);

  useEffect(() => {
    if (lockedRowIds.size === 0) {
      return;
    }
    setRowSelection((current) => removeRowSelectionIds(current, [...lockedRowIds]));
  }, [lockedRowIds]);

  useEffect(() => {
    setAcceptedDeleteLocks((current) => {
      let changed = false;
      const next = { ...current };
      const rawJobsById = new Map(rawJobs.map((job) => [job.id, job]));

      for (const [id, lock] of Object.entries(current)) {
        const numericId = Number(id);
        const job = rawJobsById.get(numericId);
        if (!job) {
          delete next[numericId];
          changed = true;
          continue;
        }
        if (job.deleteOperation == null) {
          continue;
        }
        if (job.deleteOperation.operationId !== lock.operationId && job.deleteOperation.locked) {
          delete next[numericId];
          changed = true;
          continue;
        }
        delete next[numericId];
        changed = true;
      }

      return changed ? next : current;
    });
  }, [rawJobs]);

  useEffect(() => {
    if (!hasActiveDeleteOperations && !hasActiveDiagnosticRuns) {
      return;
    }

    const intervalId = window.setInterval(() => {
      void reexecuteHistoryDeleteOperations({ requestPolicy: "network-only" });
      void reexecuteHistoryPage({ requestPolicy: "network-only" });
    }, 1000);

    return () => window.clearInterval(intervalId);
  }, [
    hasActiveDeleteOperations,
    hasActiveDiagnosticRuns,
    reexecuteHistoryDeleteOperations,
    reexecuteHistoryPage,
  ]);

  useEffect(() => {
    if (hasActiveDeleteOperations) {
      if (!hadActiveDeleteOperations) {
        setHadActiveDeleteOperations(true);
      }
      return;
    }

    if (!hadActiveDeleteOperations) {
      return;
    }

    setHadActiveDeleteOperations(false);
    void reexecuteHistoryDeleteOperations({ requestPolicy: "network-only" });
    void reexecuteHistoryPage({ requestPolicy: "network-only" });
  }, [
    hadActiveDeleteOperations,
    hasActiveDeleteOperations,
    reexecuteHistoryDeleteOperations,
    reexecuteHistoryPage,
  ]);

  const refetchHistoryPage = useCallback(async (preferredPageIndex: number) => {
    const probeInput = buildHistoryPageInput(historyPreferences, deferredSearch, preferredPageIndex);
    const probe = await client
      .query(HISTORY_PAGE_QUERY, { input: probeInput }, { requestPolicy: "network-only" })
      .toPromise();
    const nextTotal = probe.data?.historyPage.totalCount ?? 0;
    const nextPageCount = Math.max(1, Math.ceil(nextTotal / historyPreferences.pageSize));
    const nextPageIndex = Math.min(preferredPageIndex, nextPageCount - 1);

    if (nextPageIndex !== pageIndex) {
      setPageIndex(nextPageIndex);
      return;
    }

    void reexecuteHistoryPage({ requestPolicy: "network-only" });
  }, [client, deferredSearch, historyPreferences, pageIndex, reexecuteHistoryPage]);

  const handleSubscription = useCallback(
    (
      previous: unknown,
      response: { queueEvents: { kind: string; itemId: number | null; state: string | null } },
    ) => {
      const event = response.queueEvents;
      if (
        (event.kind === "ITEM_COMPLETED"
          || (event.kind === "ITEM_STATE_CHANGED" && event.state === "FAILED")
          || event.kind === "ITEM_REMOVED")
        && event.itemId != null
      ) {
        void refetchHistoryPage(pageIndex === 0 ? 0 : pageIndex);
        void reexecuteHistoryDeleteOperations({ requestPolicy: "network-only" });
      }
      return previous;
    },
    [pageIndex, refetchHistoryPage, reexecuteHistoryDeleteOperations],
  );
  useSubscription({ query: HISTORY_FACADE_EVENTS_SUBSCRIPTION }, handleSubscription);

  const timestampFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      }),
    [],
  );

  const handleReprocess = useCallback(
    async (jobId: number) => {
      const result = await reprocessJob({ id: jobId });
      if (!result.error) {
        setRowSelection((current) => removeRowSelectionIds(current, [jobId]));
        await refetchHistoryPage(pageIndex);
      }
    },
    [pageIndex, refetchHistoryPage, reprocessJob],
  );

  const handleDiagnosticRedownload = useCallback(
    async (jobId: number, includeServerHostnames: boolean) => {
      setDiagnosticAcceptError(null);
      const result = await startDiagnosticRedownload({
        id: jobId,
        includeServerHostnames,
      });
      if (result.error) {
        setDiagnosticAcceptError(result.error.message ?? "Unable to start diagnostic rerun.");
        return false;
      }
      setRowSelection((current) => removeRowSelectionIds(current, [jobId]));
      await refetchHistoryPage(pageIndex);
      void reexecuteHistoryPage({ requestPolicy: "network-only" });
      return true;
    },
    [pageIndex, refetchHistoryPage, reexecuteHistoryPage, startDiagnosticRedownload],
  );

  const isJobLocked = useCallback(
    (job: HistoryJob) => Boolean(job.deleteOperation?.locked || isDiagnosticRunActive(job.diagnosticRun)),
    [],
  );

  const acceptDelete = useCallback(
    async (
      input:
        | { mode: "IDS"; ids: number[]; deleteFiles: boolean }
        | { mode: "ALL_HISTORY"; ids?: number[]; deleteFiles: boolean },
    ) => {
      setDeleteAcceptError(null);
      const result = await acceptHistoryDelete({ input });
      const acceptance = result.data?.acceptHistoryDelete;
      if (result.error || !acceptance) {
        setDeleteAcceptError(result.error?.message ?? "Unable to queue delete request.");
        return false;
      }

      setAcceptedDeleteLocks((current) => {
        const next = { ...current };
        for (const id of acceptance.acceptedIds) {
          next[id] = {
            operationId: acceptance.operationId,
            deleteFiles: input.deleteFiles,
          };
        }
        return next;
      });
      setRowSelection((current) => removeRowSelectionIds(current, acceptance.acceptedIds));
      void reexecuteHistoryDeleteOperations({ requestPolicy: "network-only" });
      void reexecuteHistoryPage({ requestPolicy: "network-only" });
      return true;
    },
    [acceptHistoryDelete, reexecuteHistoryDeleteOperations, reexecuteHistoryPage],
  );

  const renderActions = useCallback(
    (job: HistoryJob, buttonSizeClassName: string, iconSizeClassName: string) => {
      const isRestartable = job.status === "FAILED" || job.status === "COMPLETE";
      const locked = isJobLocked(job);
      const hasActiveDiagnostic = isDiagnosticRunActive(job.diagnosticRun);

      return (
        <div
          className="flex h-full w-full items-center justify-end gap-1 px-2 py-1.5"
          data-row-click-ignore="true"
        >
          {isRestartable ? (
            <>
              <Button
                variant="ghost"
                size="icon"
                title={t("action.reprocess")}
                aria-label={t("action.reprocess")}
                className={`${buttonSizeClassName} text-muted-foreground hover:bg-transparent hover:text-foreground`}
                disabled={actionsBusy || locked}
                onClick={() => {
                  void handleReprocess(job.id);
                }}
              >
                <RefreshCcw className={iconSizeClassName} />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                title={t("action.diagnosticRedownload")}
                aria-label={t("action.diagnosticRedownload")}
                className={`${buttonSizeClassName} text-muted-foreground hover:bg-transparent hover:text-foreground`}
                disabled={actionsBusy || locked || hasActiveDiagnostic}
                onClick={() => {
                  setDiagnosticAcceptError(null);
                  setDiagnosticConfirm({
                    id: job.id,
                    includeServerHostnames: true,
                  });
                }}
              >
                <Bug className={iconSizeClassName} />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                title={t("action.redownload")}
                aria-label={t("action.redownload")}
                className={`${buttonSizeClassName} text-muted-foreground hover:bg-transparent hover:text-foreground`}
                disabled={actionsBusy || locked}
                onClick={() => setRedownloadConfirmId(job.id)}
              >
                <Download className={iconSizeClassName} />
              </Button>
            </>
          ) : null}
          <Button
            variant="ghost"
            size="icon"
            title={t("action.delete")}
            aria-label={t("action.delete")}
            className={`${buttonSizeClassName} text-muted-foreground hover:bg-transparent hover:text-foreground`}
            disabled={actionsBusy || locked}
            onClick={() => {
              setDeleteAcceptError(null);
              setDeleteConfirmId(job.id);
            }}
          >
            <Trash2 className={iconSizeClassName} />
          </Button>
        </div>
      );
    },
    [actionsBusy, handleReprocess, isJobLocked, t],
  );

  const columns = useMemo<ColumnDef<HistoryJob>[]>(
    () => [
      {
        id: "select",
        enableSorting: false,
        enableHiding: false,
        header: ({ table }) => (
          <div className="flex justify-center">
            <Checkbox
              checked={(() => {
                const selectableRows = table
                  .getRowModel()
                  .rows
                  .filter((row) => !isJobLocked(row.original));
                if (selectableRows.length === 0) {
                  return false;
                }
                const selectedRows = selectableRows.filter((row) => row.getIsSelected());
                return selectedRows.length === selectableRows.length
                  ? true
                  : selectedRows.length > 0
                    ? "indeterminate"
                    : false;
              })()}
              disabled={table.getRowModel().rows.every((row) => isJobLocked(row.original))}
              onCheckedChange={(value) => {
                const shouldSelect = value === true;
                setRowSelection((current) => {
                  const next = { ...current };
                  for (const row of table.getRowModel().rows) {
                    if (isJobLocked(row.original)) {
                      delete next[row.id];
                      continue;
                    }
                    if (shouldSelect) {
                      next[row.id] = true;
                    } else {
                      delete next[row.id];
                    }
                  }
                  return next;
                });
              }}
              aria-label="Select page"
            />
          </div>
        ),
        cell: ({ row }) => (
          <div
            className="flex h-full w-full items-center justify-center px-2 py-1.5"
            data-row-click-ignore="true"
          >
            <Checkbox
              checked={row.getIsSelected()}
              disabled={isJobLocked(row.original)}
              onCheckedChange={(value) => row.toggleSelected(value === true)}
              aria-label={`Select ${formatJobReleaseName(row.original)}`}
            />
          </div>
        ),
        meta: {
          headerClassName: "h-7 w-[52px] px-2 text-center",
          cellClassName: "p-0 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        accessorKey: "name",
        header: ({ column }) => <DataTableColumnHeader column={column} title={t("table.name")} />,
        cell: ({ row }) => {
          const displayName = formatJobReleaseName(row.original);
          const deleteOperation = row.original.deleteOperation;
          return (
            <div className="min-w-0">
              <Link
                to={`/jobs/${row.original.id}`}
                className="flex min-h-6 w-full items-center truncate text-[11px] font-medium leading-tight text-foreground"
              >
                {displayName}
              </Link>
              {deleteOperation?.locked ? (
                <span className="text-[9px] text-amber-500">Deleting…</span>
              ) : row.original.diagnosticRun ? (
                <span
                  className={cn(
                    "block truncate text-[9px]",
                    isDiagnosticRunActive(row.original.diagnosticRun)
                      ? "text-sky-600"
                      : row.original.diagnosticRun.stage === "FAILED"
                        ? "text-destructive"
                        : "text-muted-foreground",
                  )}
                  title={formatDiagnosticSummary(t, row.original)}
                >
                  {formatDiagnosticSummary(t, row.original)}
                </span>
              ) : row.original.lastDiagnosticId ? (
                <span className="block truncate text-[9px] text-muted-foreground">
                  {t("history.lastDiagnosticId", { id: row.original.lastDiagnosticId })}
                </span>
              ) : deleteOperation?.state === "FAILED" ? (
                <span
                  className="block truncate text-[9px] text-destructive"
                  title={deleteOperation.errorMessage ?? "Delete failed"}
                >
                  {deleteOperation.errorMessage ?? "Delete failed"}
                </span>
              ) : null}
            </div>
          );
        },
        meta: {
          headerClassName: "h-7 min-w-[260px] px-2 text-left",
          cellClassName: "min-w-[260px] px-2 py-1.5 text-left",
        } satisfies DataTableColumnMeta,
      },
      {
        accessorKey: "status",
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.status")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <div className="space-y-1 text-center">
            <div className="flex justify-center">
              <JobStatusBadge status={row.original.status} compact className="px-1.5" />
            </div>
            {row.original.deleteOperation?.locked ? (
              <div className="text-[9px] text-amber-500">Locked</div>
            ) : row.original.diagnosticRun ? (
              <div className="text-[9px] text-sky-600">
                {diagnosticStageLabel(t, row.original.diagnosticRun.stage)}
              </div>
            ) : null}
          </div>
        ),
        meta: {
          headerClassName: "h-7 w-[120px] px-2 text-center",
          cellClassName: "px-2 py-1.5 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "completedAt",
        accessorFn: (job) => job.completedAt ?? 0,
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.time")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <div
            className="text-center text-[10px] text-muted-foreground"
            title={formatHistoryTimestamp(row.original.completedAt ?? null)}
          >
            {formatHistoryTimestamp(row.original.completedAt ?? null, timestampFormatter)}
          </div>
        ),
        meta: {
          headerClassName: "h-7 min-w-[180px] px-2 text-center",
          cellClassName: "min-w-[180px] px-2 py-1.5 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        accessorKey: "health",
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.health")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <div className="text-center text-[10px] text-muted-foreground">
            {(row.original.health / 10).toFixed(1)}%
          </div>
        ),
        meta: {
          headerClassName: "h-7 w-[96px] px-2 text-center",
          cellClassName: "w-[96px] px-2 py-1.5 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "size",
        accessorFn: (job) => job.totalBytes,
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.size")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <div className="text-center text-[10px] text-muted-foreground">
            {formatBytes(row.original.totalBytes)}
          </div>
        ),
        meta: {
          headerClassName: "h-7 w-[120px] px-2 text-center",
          cellClassName: "w-[120px] px-2 py-1.5 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        accessorKey: "category",
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.category")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <div
            className="truncate text-center text-[10px] text-muted-foreground"
            title={row.original.category ?? "\u2014"}
          >
            {row.original.category ?? "\u2014"}
          </div>
        ),
        meta: {
          headerClassName: "h-7 min-w-[120px] px-2 text-center",
          cellClassName: "min-w-[120px] px-2 py-1.5 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "actions",
        enableSorting: false,
        header: () => <div className="text-right">{t("table.actions")}</div>,
        cell: ({ row }) => renderActions(row.original, "size-8", "size-4"),
        meta: {
          headerClassName: "h-7 w-[184px] px-2 text-right",
          cellClassName: "w-[184px] p-0 text-right",
        } satisfies DataTableColumnMeta,
      },
    ],
    [isJobLocked, setRowSelection, t, timestampFormatter, renderActions],
  );

  const historyTable = useReactTable({
    data: jobs,
    columns,
    getRowId: (row) => String(row.id),
    manualPagination: true,
    manualSorting: true,
    enableRowSelection: (row) => !isJobLocked(row.original),
    pageCount,
    getCoreRowModel: getCoreRowModel(),
    state: {
      pagination: {
        pageIndex,
        pageSize: historyPreferences.pageSize,
      },
      rowSelection,
      sorting: historyPreferences.sorting,
    },
    onRowSelectionChange: setRowSelection,
    onPaginationChange: (updater) => {
      const next =
        typeof updater === "function"
          ? updater({
            pageIndex,
            pageSize: historyPreferences.pageSize,
          })
          : updater;

      if (next.pageSize !== historyPreferences.pageSize) {
        setHistoryPreferences((current) => ({
          ...current,
          pageSize: next.pageSize,
        }));
        setPageIndex(0);
        return;
      }

      setPageIndex(next.pageIndex);
    },
    onSortingChange: (updater) => {
      const next =
        typeof updater === "function"
          ? updater(historyPreferences.sorting)
          : updater;
      setHistoryPreferences((current) => ({
        ...current,
        sorting: next,
      }));
      setPageIndex(0);
    },
  });

  function resetHistoryView() {
    setHistoryPreferences((current) => ({
      ...current,
      search: "",
      status: "all",
      sorting: DEFAULT_HISTORY_SORTING,
    }));
    setPageIndex(0);
  }

  async function handleRedownload(jobId: number) {
    const result = await redownloadJob({ id: jobId });
    if (!result.error) {
      setRowSelection((current) => removeRowSelectionIds(current, [jobId]));
      await refetchHistoryPage(pageIndex);
    }
  }

  async function handleSingleDelete(jobId: number) {
    const accepted = await acceptDelete({
      mode: "IDS",
      ids: [jobId],
      deleteFiles,
    });
    if (!accepted) {
      return;
    }

    setDeleteConfirmId(null);
    setDeleteFiles(false);
  }

  async function handleBatchDelete() {
    if (selectedActionIds.length === 0) {
      return;
    }

    const accepted = await acceptDelete({
      mode: "IDS",
      ids: selectedActionIds,
      deleteFiles,
    });
    if (!accepted) {
      return;
    }

    setDeleteBatchConfirm(false);
    setDeleteFiles(false);
  }

  async function handleDeleteAll() {
    const accepted = await acceptDelete({
      mode: "ALL_HISTORY",
      deleteFiles,
    });
    if (!accepted) {
      return;
    }

    setDeleteAllConfirm(false);
    setDeleteFiles(false);
    setPageIndex(0);
  }

  const showFilterCard = counts.all > 0 || historyPreferences.search.length > 0 || historyPreferences.status !== "all";
  const showEmptyState = !fetching && counts.all === 0 && totalCount === 0 && historyPreferences.search.length === 0 && historyPreferences.status === "all";

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("history.title")}
        description={t("history.empty")}
        actions={
          counts.all > 0 ? (
            <div className="flex gap-2">
              {selectedCount > 0 ? (
                <Button
                  variant="destructive"
                  disabled={acceptDeleteState.fetching}
                  onClick={() => {
                    setDeleteAcceptError(null);
                    setDeleteBatchConfirm(true);
                  }}
                >
                  {t("action.delete")} ({selectedCount})
                </Button>
              ) : null}
              <Button
                variant="outline"
                disabled={hasActiveDeleteOperations || acceptDeleteState.fetching}
                onClick={() => {
                  setDeleteAcceptError(null);
                  setDeleteAllConfirm(true);
                }}
              >
                {t("action.deleteAll")}
              </Button>
            </div>
          ) : undefined
        }
      />

      {hasActiveDeleteOperations ? (
        <Card className="sticky top-4 z-10 border-amber-500/30 bg-amber-500/5">
          <CardContent className="flex flex-wrap items-center justify-between gap-3 py-4">
            <div className="space-y-1">
              <div className="text-sm font-medium text-foreground">Deleting history items</div>
              <div className="text-xs text-muted-foreground">
                {deleteProgress.totalTargets}
                {" "}tracked
                {deleteProgress.runningTargets > 0
                  ? ` • ${deleteProgress.runningTargets} running`
                  : ""}
                {deleteProgress.queuedTargets > 0
                  ? ` • ${deleteProgress.queuedTargets} queued`
                  : ""}
                {deleteProgress.failedTargets > 0
                  ? ` • ${deleteProgress.failedTargets} failed`
                  : ""}
              </div>
            </div>
            <div className="text-sm font-medium text-amber-600">
              Rows stay visible until each delete finishes
            </div>
          </CardContent>
        </Card>
      ) : null}

      {showFilterCard ? (
        <Card>
          <CardContent className="space-y-4 pt-6">
            <div className="flex flex-wrap gap-2">
              <FilterButton
                active={historyPreferences.status === "all"}
                label={t("history.filterAll")}
                count={counts.all}
                onClick={() => {
                  setHistoryPreferences((current) => ({ ...current, status: "all" }));
                  setPageIndex(0);
                }}
              />
              <FilterButton
                active={historyPreferences.status === "success"}
                label={t("history.filterSuccess")}
                count={counts.success}
                onClick={() => {
                  setHistoryPreferences((current) => ({ ...current, status: "success" }));
                  setPageIndex(0);
                }}
              />
              <FilterButton
                active={historyPreferences.status === "failure"}
                label={t("history.filterFailure")}
                count={counts.failure}
                onClick={() => {
                  setHistoryPreferences((current) => ({ ...current, status: "failure" }));
                  setPageIndex(0);
                }}
              />
            </div>

            <DataTableToolbar
              searchValue={historyPreferences.search}
              onSearchChange={(value) => {
                setHistoryPreferences((current) => ({
                  ...current,
                  search: value,
                }));
                setPageIndex(0);
              }}
              searchPlaceholder={t("history.searchPlaceholder")}
              clearLabel={
                historyPreferences.search || historyPreferences.status !== "all" || !isDefaultHistorySorting(historyPreferences.sorting)
                  ? t("action.clearFilters")
                  : undefined
              }
              onClear={
                historyPreferences.search || historyPreferences.status !== "all" || !isDefaultHistorySorting(historyPreferences.sorting)
                  ? resetHistoryView
                  : undefined
              }
            />
          </CardContent>
        </Card>
      ) : null}

      {fetching && !data ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
      ) : showEmptyState ? (
        <EmptyState title={t("history.title")} description={t("history.empty")} />
      ) : (
        <Card>
          <CardContent className="px-0 pb-0">
            <DataTable
              table={historyTable}
              tableClassName="table-fixed"
              wrapperClassName="max-h-[70vh]"
              rowClassName={(row) => cn(
                "text-[11px]",
                row.getIsSelected() && "bg-muted/50",
                row.original.deleteOperation?.locked && "bg-amber-500/5 opacity-75",
              )}
              emptyState={
                <div className="space-y-3 py-12 text-center">
                  <div className="text-sm text-muted-foreground">{t("history.noMatches")}</div>
                  <div>
                    <Button variant="outline" onClick={resetHistoryView}>
                      {t("action.clearFilters")}
                    </Button>
                  </div>
                </div>
              }
            />
            <DataTablePagination
              table={historyTable}
              totalCount={totalCount}
              pageSizeOptions={[...HISTORY_PAGE_SIZE_OPTIONS]}
              rowsPerPageLabel={t("table.rowsPerPage")}
              previousLabel={t("action.previous")}
              nextLabel={t("action.next")}
            />
          </CardContent>
        </Card>
      )}

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteHistory")}
        message={t("confirm.deleteHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        confirmDisabled={acceptDeleteState.fetching}
        cancelDisabled={acceptDeleteState.fetching}
        onConfirm={() => {
          if (deleteConfirmId != null) {
            void handleSingleDelete(deleteConfirmId);
          }
        }}
        onCancel={() => {
          setDeleteAcceptError(null);
          setDeleteConfirmId(null);
          setDeleteFiles(false);
        }}
      >
        <label className="flex items-center gap-2">
          <Checkbox
            checked={deleteFiles}
            disabled={acceptDeleteState.fetching}
            onCheckedChange={(value) => setDeleteFiles(value === true)}
          />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
        {deleteAcceptError ? (
          <p className="text-sm text-destructive">{deleteAcceptError}</p>
        ) : null}
      </ConfirmDialog>

      <ConfirmDialog
        open={redownloadConfirmId != null}
        title={t("confirm.redownloadJob")}
        message={t("confirm.redownloadJobMessage")}
        confirmLabel={t("confirm.redownloadJobConfirm")}
        cancelLabel={t("confirm.redownloadJobDismiss")}
        onConfirm={() => {
          if (redownloadConfirmId != null) {
            void handleRedownload(redownloadConfirmId);
          }
          setRedownloadConfirmId(null);
        }}
        onCancel={() => setRedownloadConfirmId(null)}
      />

      <ConfirmDialog
        open={diagnosticConfirm != null}
        title={t("confirm.diagnosticRedownload")}
        message={t("confirm.diagnosticRedownloadMessage")}
        confirmLabel={t("confirm.diagnosticRedownloadConfirm")}
        cancelLabel={t("confirm.diagnosticRedownloadDismiss")}
        confirmDisabled={diagnosticStartState.fetching}
        cancelDisabled={diagnosticStartState.fetching}
        onConfirm={() => {
          if (!diagnosticConfirm) {
            return;
          }
          void handleDiagnosticRedownload(
            diagnosticConfirm.id,
            diagnosticConfirm.includeServerHostnames,
          ).then((accepted) => {
            if (accepted) {
              setDiagnosticConfirm(null);
            }
          });
        }}
        onCancel={() => {
          setDiagnosticAcceptError(null);
          setDiagnosticConfirm(null);
        }}
      >
        <label className="flex items-center gap-2">
          <Checkbox
            checked={diagnosticConfirm?.includeServerHostnames ?? true}
            disabled={diagnosticStartState.fetching}
            onCheckedChange={(value) => {
              setDiagnosticConfirm((current) => (current ? {
                ...current,
                includeServerHostnames: value === true,
              } : current));
            }}
          />
          <span className="text-sm">{t("confirm.includeServerHostnames")}</span>
        </label>
        {diagnosticAcceptError ? (
          <p className="text-sm text-destructive">{diagnosticAcceptError}</p>
        ) : null}
      </ConfirmDialog>

      <ConfirmDialog
        open={deleteBatchConfirm}
        title={t("confirm.deleteHistoryBatch", { count: selectedCount })}
        message={t("confirm.deleteHistoryBatchMessage", { count: selectedCount })}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        confirmDisabled={acceptDeleteState.fetching || selectedCount === 0}
        cancelDisabled={acceptDeleteState.fetching}
        onConfirm={() => {
          void handleBatchDelete();
        }}
        onCancel={() => {
          setDeleteAcceptError(null);
          setDeleteBatchConfirm(false);
          setDeleteFiles(false);
        }}
      >
        <label className="flex items-center gap-2">
          <Checkbox
            checked={deleteFiles}
            disabled={acceptDeleteState.fetching}
            onCheckedChange={(value) => setDeleteFiles(value === true)}
          />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
        {deleteAcceptError ? (
          <p className="text-sm text-destructive">{deleteAcceptError}</p>
        ) : null}
      </ConfirmDialog>

      <ConfirmDialog
        open={deleteAllConfirm}
        title={t("confirm.deleteAllHistory")}
        message={t("confirm.deleteAllHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        confirmDisabled={acceptDeleteState.fetching}
        cancelDisabled={acceptDeleteState.fetching}
        onConfirm={() => {
          void handleDeleteAll();
        }}
        onCancel={() => {
          setDeleteAcceptError(null);
          setDeleteAllConfirm(false);
          setDeleteFiles(false);
        }}
      >
        <label className="flex items-center gap-2">
          <Checkbox
            checked={deleteFiles}
            disabled={acceptDeleteState.fetching}
            onCheckedChange={(value) => setDeleteFiles(value === true)}
          />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
        {deleteAcceptError ? (
          <p className="text-sm text-destructive">{deleteAcceptError}</p>
        ) : null}
      </ConfirmDialog>
    </div>
  );
}

function formatHistoryTimestamp(
  epochMs: number | null,
  formatter = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }),
) {
  if (!epochMs) {
    return "\u2014";
  }

  return formatter.format(epochMs);
}

type FilterButtonProps = {
  active: boolean;
  label: string;
  count: number;
  onClick: () => void;
};

function FilterButton({ active, label, count, onClick }: FilterButtonProps) {
  return (
    <Button
      variant={active ? "default" : "outline"}
      className="rounded-full"
      onClick={onClick}
    >
      {label}
      <span className="rounded-full bg-background/20 px-2 py-0.5 text-xs">
        {count}
      </span>
    </Button>
  );
}

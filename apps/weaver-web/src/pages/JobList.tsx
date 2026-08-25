import {
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type RowSelectionState,
  type SortingState,
} from "@tanstack/react-table";
import {
  ChevronDown,
  ListFilter,
  Pause,
  Pencil,
  Play,
  Rows3,
  Table as TableIcon,
  X,
} from "lucide-react";
import {
  memo,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";
import { Link } from "react-router";
import { useClient, useMutation, useQuery, useSubscription } from "urql";
import { BulkEditModal } from "@/components/BulkEditModal";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { DataTable } from "@/components/data-table/DataTable";
import type { DataTableColumnMeta } from "@/components/data-table/DataTable";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { DataTablePagination } from "@/components/data-table/DataTablePagination";
import { DataTableToolbar } from "@/components/data-table/DataTableToolbar";
import { EmptyState } from "@/components/EmptyState";
import { FilterChip } from "@/components/FilterChip";
import { JobPhaseProgressBars } from "@/components/JobPhaseProgressBars";
import { JobStatusBadgeGroup } from "@/components/JobStatusBadge";
import { PageHeader } from "@/components/PageHeader";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { formatBytes, formatSpeed } from "@/components/SpeedDisplay";
import { UploadModal } from "@/components/UploadModal";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  CATEGORIES_QUERY,
  CANCEL_JOB_MUTATION,
  HAS_CONFIGURED_SERVERS_QUERY,
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
  QUEUE_EVENTS_SUBSCRIPTION,
  QUEUE_PAGE_QUERY,
  SET_SPEED_LIMIT_MUTATION,
  UPDATE_JOBS_MUTATION,
} from "@/graphql/queries";
import { executeAliasedIdMutation } from "@/graphql/aliased-mutations";
import { useGraphqlConnectionState } from "@/graphql/client";
import {
  useLiveDownloadBlock,
  useLivePaused,
  useLiveSpeed,
} from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { useTablePreferences } from "@/lib/hooks/use-table-preferences";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { getDisplayedJobProgress } from "@/lib/job-progress";
import { getJobStages } from "@/lib/job-stages";
import { isActiveStatus, STATUS_BG_CLASS, statusToken } from "@/lib/status-tokens";
import { useStableQueueEta } from "@/lib/hooks/use-stable-queue-eta";
import {
  formatJobReleaseName,
  normalizeJobData,
  type GraphqlJobData,
  type JobData,
} from "@/lib/job-types";
import { cn } from "@/lib/utils";

type QueueStatusFilter =
  | "QUEUED"
  | "DOWNLOADING"
  | "PAUSED"
  | "VERIFYING"
  | "REPAIRING"
  | "EXTRACTING"
  | "POST_PROCESSING"
  | "MOVING";

type QueuePriorityFilter = "LOW" | "NORMAL" | "HIGH";
type PendingQueueJobUpdate = {
  category?: string | null;
  priority?: QueuePriorityFilter;
};
type QueueSelectOption = {
  value: string;
  label: string;
};
type QueueTablePreferences = {
  pageSize: number;
  search: string;
  statuses: QueueStatusFilter[];
  priorities: QueuePriorityFilter[];
  categories: string[];
  sorting: SortingState;
};

type QueueRowData = JobData & {
  displayName: string;
  statusLabel: string;
  priorityValue: QueuePriorityFilter;
  priorityLabel: string;
  priorityRank: number;
  categoryValue: string | null;
  categoryLabel: string;
  blockedByGlobalPause: boolean;
  blockedByIspCap: boolean;
  etaDisplay: string;
};

type QueuePageSummary = {
  totalItems: number;
  queuedItems: number;
  activeItems: number;
  pausedItems: number;
};

type QueuePageResponse = {
  queuePage: {
    items: GraphqlJobData[];
    totalCount: number;
    summary: QueuePageSummary;
    categories: string[];
    latestCursor: string;
  };
};

type QueuePageData = QueuePageResponse["queuePage"];

type QueueEventPayload = {
  cursor: string;
  kind: "ITEM_CREATED" | "ITEM_STATE_CHANGED" | "ITEM_PROGRESS" | "ITEM_ATTENTION" | "ITEM_COMPLETED" | "ITEM_REMOVED" | "GLOBAL_STATE_CHANGED";
  itemId: number | null;
  item: GraphqlJobData | null;
};

type QueueItemEventOverlay = {
  item: GraphqlJobData;
  cursor: bigint;
};

type PolledQueuePage = {
  queryKey: string;
  page: QueuePageData;
};

const QUEUE_PAGE_SIZE_OPTIONS = [25, 50, 100, 500] as const;
const QUEUE_EVENT_REFRESH_INTERVAL_MS = 2_000;
const EMPTY_QUEUE_PAGE_ITEMS: GraphqlJobData[] = [];
const EMPTY_QUEUE_CATEGORIES: string[] = [];
const DEFAULT_QUEUE_PREFERENCES: QueueTablePreferences = {
  pageSize: 50,
  search: "",
  statuses: [],
  priorities: [],
  categories: [],
  sorting: [],
};
const QUEUE_TABLE_PREFERENCES_KEY = "weaver.queue.table.preferences.v5";
const QUEUE_STATUS_OPTIONS: QueueStatusFilter[] = [
  "QUEUED",
  "DOWNLOADING",
  "PAUSED",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "POST_PROCESSING",
  "MOVING",
];
const QUEUE_PRIORITY_OPTIONS: QueuePriorityFilter[] = ["HIGH", "NORMAL", "LOW"];
const QUEUE_ACTIVE_STATUSES: QueueStatusFilter[] = [
  "DOWNLOADING",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "POST_PROCESSING",
  "MOVING",
];
const NO_CATEGORY_SELECT_VALUE = "__no_category__";

type QueueLayout = "table" | "compact";

function queueStatusToGraphql(status: QueueStatusFilter): string {
  return status === "MOVING" ? "FINALIZING" : status;
}

function queueSortingToGraphql(sorting: SortingState) {
  const current = sorting[0];
  if (!current) {
    return {};
  }
  const sortField = (() => {
    switch (current.id) {
      case "name":
        return "NAME";
      case "status":
        return "STATE";
      case "priority":
        return "PRIORITY";
      case "category":
        return "CATEGORY";
      case "size":
        return "SIZE";
      default:
        return "PROGRESS";
    }
  })();
  return {
    sortField,
    sortDirection: current.desc === false ? "ASC" : "DESC",
  };
}

function buildQueuePageInput(
  preferences: QueueTablePreferences,
  search: string,
  pageIndex: number,
) {
  return {
    pageIndex,
    pageSize: preferences.pageSize,
    search: search.length > 0 ? search : undefined,
    states: preferences.statuses.length > 0
      ? preferences.statuses.map(queueStatusToGraphql)
      : undefined,
    priorities: preferences.priorities.length > 0 ? preferences.priorities : undefined,
    categories: preferences.categories.length > 0 ? preferences.categories : undefined,
    ...queueSortingToGraphql(preferences.sorting),
  };
}

function decodeQueueEventCursor(cursor: string): bigint | null {
  try {
    const base64 = cursor.replace(/-/g, "+").replace(/_/g, "/");
    const padded = `${base64}${"=".repeat((4 - (base64.length % 4)) % 4)}`;
    const decoded = atob(padded);
    if (!decoded.startsWith("evt:")) {
      return null;
    }
    return BigInt(decoded.slice(4));
  } catch {
    return null;
  }
}

function sameStatusSet(current: readonly string[], preset: readonly string[]): boolean {
  return current.length === preset.length && preset.every((value) => current.includes(value));
}

type QueueActionButtonsProps = {
  jobId: number;
  status: JobData["status"];
  pauseLabel: string;
  resumeLabel: string;
  cancelLabel: string;
  onPause: (id: number) => void;
  onResume: (id: number) => void;
  onCancel: (id: number) => void;
};

const QueueActionButtons = memo(function QueueActionButtons({
  jobId,
  status,
  pauseLabel,
  resumeLabel,
  cancelLabel,
  onPause,
  onResume,
  onCancel,
}: QueueActionButtonsProps) {
  return (
    <div
      className="flex h-full w-full items-center justify-end gap-1 px-2 py-1.5"
      data-row-click-ignore="true"
    >
      {status === "PAUSED" ? (
        <Button
          variant="ghost"
          size="icon"
          title={resumeLabel}
          aria-label={resumeLabel}
          className="size-8 shrink-0 text-muted-foreground hover:bg-transparent hover:text-foreground"
          onClick={() => onResume(jobId)}
        >
          <Play className="size-4" />
        </Button>
      ) : (
        <Button
          variant="ghost"
          size="icon"
          title={pauseLabel}
          aria-label={pauseLabel}
          className="size-8 shrink-0 text-muted-foreground hover:bg-transparent hover:text-foreground"
          onClick={() => onPause(jobId)}
        >
          <Pause className="size-4" />
        </Button>
      )}
      <Button
        variant="ghost"
        size="icon"
        title={cancelLabel}
        aria-label={cancelLabel}
        className="size-8 shrink-0 text-muted-foreground hover:bg-transparent hover:text-foreground"
        onClick={() => onCancel(jobId)}
      >
        <X className="size-4" />
      </Button>
    </div>
  );
});

const QueueCellSelect = memo(function QueueCellSelect({
  jobId,
  value,
  options,
  ariaLabel,
  disabled,
  onValueChange,
  className,
}: {
  jobId: number;
  value: string;
  options: QueueSelectOption[];
  ariaLabel: string;
  disabled?: boolean;
  onValueChange: (jobId: number, value: string) => void;
  className?: string;
}) {
  const handleValueChange = useCallback((nextValue: string) => {
    onValueChange(jobId, nextValue);
  }, [jobId, onValueChange]);

  return (
    <div className="flex justify-center" data-row-click-ignore="true">
      <Select
        value={value}
        onValueChange={handleValueChange}
        disabled={disabled}
      >
        <SelectTrigger
          size="sm"
          aria-label={ariaLabel}
          className={cn(
            "h-8 min-w-0 border-0 bg-transparent px-2 text-[11px] shadow-none transition-none hover:bg-accent/40 focus-visible:ring-2",
            "justify-center gap-1.5 text-center",
            className,
          )}
        >
          <SelectValue className="truncate" />
        </SelectTrigger>
        <SelectContent>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
});

const QueueNameCell = memo(function QueueNameCell({
  jobId,
  displayName,
}: {
  jobId: number;
  displayName: string;
}) {
  return (
    <div className="min-w-0">
      <Link
        to={`/jobs/${jobId}`}
        className="block whitespace-normal break-words text-xs font-medium leading-snug text-foreground"
      >
        {displayName}
      </Link>
    </div>
  );
});

const QueueStatusCell = memo(function QueueStatusCell({
  status,
  blockedByIspCap,
  bandwidthCapLabel,
}: {
  status: JobData["status"];
  blockedByIspCap: boolean;
  bandwidthCapLabel: string;
}) {
  return (
    <div className="flex flex-col items-center gap-1 text-center">
      <JobStatusBadgeGroup statuses={getJobStages({ status })} compact className="justify-center" />
      {blockedByIspCap ? (
        <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-status-paused">
          {bandwidthCapLabel}
        </span>
      ) : null}
    </div>
  );
});

const QueueProgressCell = memo(function QueueProgressCell({
  phaseProgress,
}: {
  phaseProgress: JobData["phaseProgress"];
}) {
  return (
    <div className="flex justify-center">
      <div className="w-full max-w-[176px]">
        <JobPhaseProgressBars phaseProgress={phaseProgress} compact />
      </div>
    </div>
  );
});

const QueueSizeCell = memo(function QueueSizeCell({
  totalBytes,
}: {
  totalBytes: number;
}) {
  return (
    <div className="text-center text-[11px] text-muted-foreground">
      {formatBytes(totalBytes)}
    </div>
  );
});

function toggleMultiSelectValue<T extends string>(current: readonly T[], value: T) {
  return current.includes(value)
    ? current.filter((item) => item !== value)
    : [...current, value];
}

function countActiveQueueFilters(preferences: QueueTablePreferences) {
  return preferences.statuses.length + preferences.priorities.length + preferences.categories.length;
}

function handleFilterOptionKeyDown(
  event: KeyboardEvent<HTMLDivElement>,
  onToggle: () => void,
) {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    onToggle();
  }
}

function getJobPriority(job: { metadata: { key: string; value: string }[] }): "LOW" | "NORMAL" | "HIGH" {
  const rawPriority = job.metadata.find((entry) => entry.key === "priority")?.value?.toUpperCase();
  if (rawPriority === "LOW" || rawPriority === "HIGH") {
    return rawPriority;
  }
  return "NORMAL";
}

function formatJobPriority(priority: "LOW" | "NORMAL" | "HIGH") {
  if (priority === "LOW") return "Low";
  if (priority === "HIGH") return "High";
  return "Normal";
}

function hasOwnPendingField<TKey extends keyof PendingQueueJobUpdate>(
  pending: PendingQueueJobUpdate | undefined,
  key: TKey,
): pending is PendingQueueJobUpdate & Required<Pick<PendingQueueJobUpdate, TKey>> {
  return Object.prototype.hasOwnProperty.call(pending ?? {}, key);
}

function resolveJobPriority(
  job: { metadata: { key: string; value: string }[] },
  pending: PendingQueueJobUpdate | undefined,
): QueuePriorityFilter {
  return hasOwnPendingField(pending, "priority")
    ? pending.priority
    : getJobPriority(job);
}

function resolveJobCategory(
  job: { category?: string | null },
  pending: PendingQueueJobUpdate | undefined,
): string | null {
  return hasOwnPendingField(pending, "category")
    ? pending.category
    : (job.category ?? null);
}

function isBlockedByGlobalPause(job: { status: string }, isPaused: boolean) {
  return isPaused && (job.status === "DOWNLOADING" || job.status === "QUEUED");
}

function isBlockedByDownloadPolicy(
  job: { status: string },
  downloadBlock: { kind: string },
) {
  return (
    (downloadBlock.kind === "ISP_CAP" || downloadBlock.kind === "SERVER_QUOTA")
    && (job.status === "DOWNLOADING" || job.status === "QUEUED")
  );
}

function formatResetAt(epochMs?: number | null) {
  if (!epochMs) return "\u2014";
  return new Date(epochMs).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function queueStatusLabel(status: QueueStatusFilter, t: ReturnType<typeof useTranslate>) {
  switch (status) {
    case "QUEUED":
      return t("status.queued");
    case "DOWNLOADING":
      return t("status.downloading");
    case "PAUSED":
      return t("status.paused");
    case "VERIFYING":
      return t("status.verifying");
    case "REPAIRING":
      return t("status.repairing");
    case "EXTRACTING":
      return t("status.extracting");
    case "POST_PROCESSING":
      return t("status.postProcessing");
    case "MOVING":
      return t("status.moving");
    default:
      return status;
  }
}

export function JobList() {
  const client = useClient();
  const graphqlConnection = useGraphqlConnectionState();
  const [serversResult] = useQuery({ query: HAS_CONFIGURED_SERVERS_QUERY });
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });
  const hasNoServers = serversResult.data?.hasConfiguredServers === false;
  const t = useTranslate();
  const [queuePreferences, setQueuePreferences] = useTablePreferences(
    QUEUE_TABLE_PREFERENCES_KEY,
    DEFAULT_QUEUE_PREFERENCES,
  );
  const [pageIndex, setPageIndex] = useState(0);
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});
  const [pendingJobUpdates, setPendingJobUpdates] = useState<Record<number, PendingQueueJobUpdate>>({});
  const [savingQueueFields, setSavingQueueFields] = useState<Record<string, boolean>>({});
  const [queueLayout, setQueueLayout] = useState<QueueLayout>("table");

  const speed = useLiveSpeed();
  const isPaused = useLivePaused();
  const downloadBlock = useLiveDownloadBlock();
  const deferredSearch = useDeferredValue(queuePreferences.search.trim());
  const queuePageInput = useMemo(
    () => buildQueuePageInput(queuePreferences, deferredSearch, pageIndex),
    [deferredSearch, pageIndex, queuePreferences],
  );
  const queuePageVariables = useMemo(() => ({ input: queuePageInput }), [queuePageInput]);
  const queueQueryKey = useMemo(() => JSON.stringify(queuePageInput), [queuePageInput]);
  const queueTableVirtualization = useMemo(
    () => ({
      estimatedRowHeight: 56,
      overscan: 8,
      resetKey: queueQueryKey,
    }),
    [queueQueryKey],
  );
  const queueRowClassName = useCallback(() => "text-xs", []);
  const [{ data: queuePageData, error: queuePageError }, reexecuteQueuePage] =
    useQuery<QueuePageResponse>({
      query: QUEUE_PAGE_QUERY,
      variables: queuePageVariables,
    });
  const [polledQueuePage, setPolledQueuePage] = useState<PolledQueuePage>();
  useReconnectPolling<QueuePageResponse>({
    enabled: graphqlConnection.status === "disconnected",
    query: QUEUE_PAGE_QUERY,
    variables: queuePageVariables,
    onData: (data) => setPolledQueuePage({ queryKey: queueQueryKey, page: data.queuePage }),
  });
  const queuePage = useMemo(
    () => {
      const currentPolledPage =
        polledQueuePage?.queryKey === queueQueryKey ? polledQueuePage.page : undefined;
      if (graphqlConnection.status === "disconnected" && currentPolledPage) {
        return currentPolledPage;
      }
      return queuePageData?.queuePage;
    },
    [graphqlConnection.status, polledQueuePage, queuePageData?.queuePage, queueQueryKey],
  );
  const [{ data: queueEventData, error: queueEventError }] = useSubscription<{
    queueEvents: QueueEventPayload;
  }>({
    query: QUEUE_EVENTS_SUBSCRIPTION,
    variables: { after: queuePage?.latestCursor },
    pause: !queuePage?.latestCursor,
  });
  const [eventItems, setEventItems] = useState<Record<number, QueueItemEventOverlay>>({});
  const [optimisticallyRemovedJobIds, setOptimisticallyRemovedJobIds] = useState<Set<number>>(
    () => new Set(),
  );
  const queueRefreshTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastQueueRefreshAtRef = useRef(0);
  const lastQueueEventSequenceRef = useRef<bigint | null>(null);
  const lastQueueEventErrorRef = useRef<string | null>(null);
  const lastQueueConnectionAtRef = useRef<number | null | undefined>(undefined);
  const rawQueuePageItems = queuePage?.items ?? EMPTY_QUEUE_PAGE_ITEMS;
  const queuePageItems = useMemo(
    () =>
      optimisticallyRemovedJobIds.size === 0
        ? rawQueuePageItems
        : rawQueuePageItems.filter((job) => !optimisticallyRemovedJobIds.has(job.id)),
    [optimisticallyRemovedJobIds, rawQueuePageItems],
  );
  const hideQueueJobs = useCallback((ids: readonly number[]) => {
    if (ids.length === 0) {
      return;
    }
    const idsToHide = new Set(ids);
    setOptimisticallyRemovedJobIds((current) => {
      const next = new Set(current);
      let changed = false;
      for (const id of idsToHide) {
        if (!next.has(id)) {
          next.add(id);
          changed = true;
        }
      }
      return changed ? next : current;
    });
    setEventItems((current) => {
      let changed = false;
      const next = { ...current };
      for (const id of idsToHide) {
        if (id in next) {
          delete next[id];
          changed = true;
        }
      }
      return changed ? next : current;
    });
    setRowSelection((current) => {
      let changed = false;
      const next = { ...current };
      for (const id of idsToHide) {
        if (next[id]) {
          delete next[id];
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, []);
  const restoreQueueJobs = useCallback((ids: readonly number[]) => {
    if (ids.length === 0) {
      return;
    }
    setOptimisticallyRemovedJobIds((current) => {
      let changed = false;
      const next = new Set(current);
      for (const id of ids) {
        if (next.delete(id)) {
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, []);
  const [hasBootstrappedQueue, setHasBootstrappedQueue] = useState(false);
  const queueInitialFetchPending = queuePage === undefined && !queuePageError;
  const serverConfigurationPending = serversResult.data === undefined && !serversResult.error;
  const isQueueBootstrapPending =
    !hasBootstrappedQueue && (queueInitialFetchPending || serverConfigurationPending);
  const jobs = useMemo(
    () => queuePageItems.map((job) => normalizeJobData(eventItems[job.id]?.item ?? job)),
    [eventItems, queuePageItems],
  );
  const policyBlockedJobs = jobs.filter((job) => isBlockedByDownloadPolicy(job, downloadBlock)).length;
  const capResetAt = formatResetAt(downloadBlock.windowEndsAtEpochMs);

  useEffect(() => {
    setRowSelection({});
  }, [queueQueryKey]);

  useEffect(() => {
    setEventItems({});
    setPolledQueuePage(undefined);
    if (queueRefreshTimeoutRef.current) {
      clearTimeout(queueRefreshTimeoutRef.current);
      queueRefreshTimeoutRef.current = null;
    }
  }, [queueQueryKey]);

  useEffect(() => {
    const pageCursor = queuePage && decodeQueueEventCursor(queuePage.latestCursor);
    if (pageCursor === null || pageCursor === undefined) {
      return;
    }
    setEventItems((current) => {
      let changed = false;
      const next = { ...current };
      for (const [jobId, overlay] of Object.entries(current)) {
        if (overlay.cursor <= pageCursor) {
          delete next[Number(jobId)];
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [queuePage]);

  useEffect(() => {
    setOptimisticallyRemovedJobIds((current) => {
      if (current.size === 0) {
        return current;
      }
      const visibleJobIds = new Set(rawQueuePageItems.map((job) => job.id));
      const next = new Set(current);
      let changed = false;
      for (const id of current) {
        if (!visibleJobIds.has(id)) {
          next.delete(id);
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [rawQueuePageItems]);

  useEffect(() => {
    if (!isQueueBootstrapPending) {
      setHasBootstrappedQueue(true);
    }
  }, [isQueueBootstrapPending]);

  useEffect(() => {
    if (graphqlConnection.status === "disconnected") {
      setEventItems({});
    }
  }, [graphqlConnection.status]);

  const refreshQueuePageNow = useCallback(() => {
    if (queueRefreshTimeoutRef.current) {
      clearTimeout(queueRefreshTimeoutRef.current);
      queueRefreshTimeoutRef.current = null;
    }
    lastQueueRefreshAtRef.current = Date.now();
    void reexecuteQueuePage({ requestPolicy: "network-only" });
  }, [reexecuteQueuePage]);

  const scheduleQueuePageRefresh = useCallback(() => {
    if (queueRefreshTimeoutRef.current) {
      return;
    }
    const elapsed = Date.now() - lastQueueRefreshAtRef.current;
    const delay = Math.max(0, QUEUE_EVENT_REFRESH_INTERVAL_MS - elapsed);
    queueRefreshTimeoutRef.current = setTimeout(() => {
      queueRefreshTimeoutRef.current = null;
      lastQueueRefreshAtRef.current = Date.now();
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }, delay);
  }, [reexecuteQueuePage]);

  useEffect(() => {
    if (graphqlConnection.status !== "connected" || graphqlConnection.lastConnectedAt === null) {
      return;
    }
    if (lastQueueConnectionAtRef.current === undefined) {
      lastQueueConnectionAtRef.current = graphqlConnection.lastConnectedAt;
      return;
    }
    if (lastQueueConnectionAtRef.current === graphqlConnection.lastConnectedAt) {
      return;
    }
    lastQueueConnectionAtRef.current = graphqlConnection.lastConnectedAt;
    lastQueueEventSequenceRef.current = null;
    setEventItems({});
    setPolledQueuePage(undefined);
    refreshQueuePageNow();
  }, [graphqlConnection.lastConnectedAt, graphqlConnection.status, refreshQueuePageNow]);

  useEffect(() => {
    if (queueEventError) {
      const errorKey = queueEventError.message;
      if (lastQueueEventErrorRef.current !== errorKey) {
        lastQueueEventErrorRef.current = errorKey;
        refreshQueuePageNow();
      }
    } else {
      lastQueueEventErrorRef.current = null;
    }
    const event = queueEventData?.queueEvents;
    if (!event) {
      return;
    }
    const eventCursor = decodeQueueEventCursor(event.cursor);
    if (eventCursor === null) {
      refreshQueuePageNow();
      return;
    }
    if (lastQueueEventSequenceRef.current !== null && eventCursor <= lastQueueEventSequenceRef.current) {
      return;
    }
    lastQueueEventSequenceRef.current = eventCursor;
    if (event.kind === "ITEM_REMOVED" && event.itemId != null) {
      hideQueueJobs([event.itemId]);
    }
    const eventItemIsVisible =
      event.item !== null && queuePageItems.some((item) => item.id === event.item!.id);
    if (event.item && eventItemIsVisible) {
      setEventItems((current) => {
        const currentOverlay = current[event.item!.id];
        if (currentOverlay && currentOverlay.cursor >= eventCursor) {
          return current;
        }
        return { ...current, [event.item!.id]: { item: event.item!, cursor: eventCursor } };
      });
    }
    if (event.item && !eventItemIsVisible) {
      refreshQueuePageNow();
      return;
    }
    if (event.kind === "ITEM_PROGRESS") {
      if (!event.item) {
        scheduleQueuePageRefresh();
        return;
      }
      if (queuePreferences.sorting.length > 0 && queuePreferences.sorting[0]?.id !== "progress") {
        return;
      }
    }
    scheduleQueuePageRefresh();
  }, [
    hideQueueJobs,
    queueEventData,
    queueEventError,
    queuePageItems,
    queuePreferences.sorting,
    refreshQueuePageNow,
    scheduleQueuePageRefresh,
  ]);

  useEffect(() => () => {
    if (queueRefreshTimeoutRef.current) {
      clearTimeout(queueRefreshTimeoutRef.current);
    }
  }, []);

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation<{ cancelJob: boolean }>(CANCEL_JOB_MUTATION);
  const [, setSpeedLimit] = useMutation(SET_SPEED_LIMIT_MUTATION);
  const [, updateJobs] = useMutation(UPDATE_JOBS_MUTATION);

  const [uploadOpen, setUploadOpen] = useState(false);
  const [speedLimitOpen, setSpeedLimitOpen] = useState(false);
  const [speedLimitInput, setSpeedLimitInput] = useState("");
  const [speedLimitIsUnlimited, setSpeedLimitIsUnlimited] = useState(true);

  const [effectiveSpeedLimit, setEffectiveSpeedLimit] = useState(0);
  const lastScheduledRef = useRef(0);
  useEffect(() => {
    const scheduled = downloadBlock.scheduledSpeedLimit ?? 0;
    if (scheduled !== lastScheduledRef.current) {
      lastScheduledRef.current = scheduled;
      setEffectiveSpeedLimit(scheduled);
    }
  }, [downloadBlock.scheduledSpeedLimit]);

  const openSpeedLimitDialog = () => {
    const unlimited = effectiveSpeedLimit === 0;
    setSpeedLimitIsUnlimited(unlimited);
    setSpeedLimitInput(unlimited ? "" : String(effectiveSpeedLimit / (1024 * 1024)));
    setSpeedLimitOpen(true);
  };

  const applySpeedLimit = () => {
    const bytes = speedLimitIsUnlimited
      ? 0
      : Math.max(0, parseFloat(speedLimitInput) || 0) * 1024 * 1024;
    setEffectiveSpeedLimit(bytes);
    void setSpeedLimit({ bytesPerSec: Math.round(bytes) });
    setSpeedLimitOpen(false);
  };

  const [cancelConfirmId, setCancelConfirmId] = useState<number | null>(null);
  const [bulkEditOpen, setBulkEditOpen] = useState(false);
  const [cancelSelectedConfirm, setCancelSelectedConfirm] = useState(false);

  const handlePauseJob = useCallback((id: number) => {
    void pauseJob({ id });
  }, [pauseJob]);

  const handleResumeJob = useCallback((id: number) => {
    void resumeJob({ id });
  }, [resumeJob]);

  const handleCancelJob = useCallback((id: number) => {
    setCancelConfirmId(id);
  }, []);

  const handleConfirmCancelJob = useCallback(async () => {
    const id = cancelConfirmId;
    setCancelConfirmId(null);
    if (id == null) {
      return;
    }

    hideQueueJobs([id]);
    const result = await cancelJob({ id });
    if (result.error || result.data?.cancelJob !== true) {
      restoreQueueJobs([id]);
    }
    void reexecuteQueuePage({ requestPolicy: "network-only" });
  }, [cancelConfirmId, cancelJob, hideQueueJobs, reexecuteQueuePage, restoreQueueJobs]);

  const selectedIds = useMemo(
    () => Object.entries(rowSelection)
      .filter(([, selected]) => selected)
      .map(([id]) => Number(id)),
    [rowSelection],
  );

  const queueCategories = queuePage?.categories ?? EMPTY_QUEUE_CATEGORIES;
  const editableCategoryOptions = useMemo(
    () => {
      const next = Array.from(
        new Set([
          ...(((categoryData?.categories as { id: number; name: string }[] | undefined) ?? [])
            .map((entry) => entry.name)
            .filter((name): name is string => Boolean(name))),
          ...queueCategories,
        ]),
      ).sort((left, right) => left.localeCompare(right));
      return next;
    },
    [categoryData?.categories, queueCategories],
  );

  const prioritySelectOptions = useMemo<QueueSelectOption[]>(
    () => [
      { value: "HIGH", label: t("upload.priorityHigh") },
      { value: "NORMAL", label: t("upload.priorityNormal") },
      { value: "LOW", label: t("upload.priorityLow") },
    ],
    [t],
  );

  const categorySelectOptions = useMemo<QueueSelectOption[]>(
    () => [
      { value: NO_CATEGORY_SELECT_VALUE, label: t("upload.noCategory") },
      ...editableCategoryOptions.map((category) => ({ value: category, label: category })),
    ],
    [editableCategoryOptions, t],
  );

  useEffect(() => {
    setPendingJobUpdates((current) => {
      const entries = Object.entries(current);
      if (entries.length === 0) {
        return current;
      }

      const jobsById = new Map(jobs.map((job) => [job.id, job]));
      let changed = false;
      const next: Record<number, PendingQueueJobUpdate> = {};

      for (const [rawId, update] of entries) {
        const id = Number(rawId);
        const job = jobsById.get(id);
        if (!job) {
          changed = true;
          continue;
        }

        const remaining: PendingQueueJobUpdate = {};
        if (hasOwnPendingField(update, "category")) {
          if (resolveJobCategory(job, undefined) !== update.category) {
            remaining.category = update.category;
          } else {
            changed = true;
          }
        }
        if (hasOwnPendingField(update, "priority")) {
          if (resolveJobPriority(job, undefined) !== update.priority) {
            remaining.priority = update.priority;
          } else {
            changed = true;
          }
        }

        if (Object.keys(remaining).length > 0) {
          next[id] = remaining;
        } else {
          changed = true;
        }
      }

      return changed ? next : current;
    });
  }, [jobs]);

  const queueEtaById = useStableQueueEta(jobs, speed);
  const queueTableRows = useMemo<QueueRowData[]>(
    () =>
      jobs.map((job) => {
        const pending = pendingJobUpdates[job.id];
        const priorityValue = resolveJobPriority(job, pending);
        const categoryValue = resolveJobCategory(job, pending);
        const blockedByIspCap = isBlockedByDownloadPolicy(job, downloadBlock);
        const blockedByGlobalPause = isBlockedByGlobalPause(job, isPaused);
        return {
          ...job,
          displayName: formatJobReleaseName(job),
          statusLabel: queueStatusLabel(job.status as QueueStatusFilter, t),
          priorityValue,
          priorityLabel: formatJobPriority(priorityValue),
          priorityRank: priorityValue === "HIGH" ? 3 : priorityValue === "LOW" ? 1 : 2,
          categoryValue,
          categoryLabel: categoryValue ?? "\u2014",
          blockedByGlobalPause,
          blockedByIspCap,
          etaDisplay: blockedByIspCap
            ? downloadBlock.kind === "SERVER_QUOTA"
              ? t("jobs.serverQuotaEta")
              : t("jobs.bandwidthCapEta", { resetAt: capResetAt })
            : blockedByGlobalPause
              ? t("status.paused")
              : (queueEtaById.get(job.id) ?? "\u2014"),
        };
      }),
    [capResetAt, downloadBlock, isPaused, jobs, pendingJobUpdates, queueEtaById, t],
  );
  const totalCount = Math.max(
    0,
    (queuePage?.totalCount ?? 0)
      - rawQueuePageItems.filter((job) => optimisticallyRemovedJobIds.has(job.id)).length,
  );
  const hasUnfilteredQueueItems = (queuePage?.summary.totalItems ?? 0) > 0;
  const pageCount = Math.max(1, Math.ceil(totalCount / queuePreferences.pageSize));

  useEffect(() => {
    if (pageIndex >= pageCount && pageIndex > 0) {
      setPageIndex(pageCount - 1);
    }
  }, [pageCount, pageIndex]);

  const setQueueFieldSaving = useCallback((fieldKey: string, saving: boolean) => {
    setSavingQueueFields((current) => {
      if (saving) {
        if (current[fieldKey]) {
          return current;
        }
        return {
          ...current,
          [fieldKey]: true,
        };
      }
      if (!current[fieldKey]) {
        return current;
      }
      const next = { ...current };
      delete next[fieldKey];
      return next;
    });
  }, []);

  const handleInlinePriorityChange = useCallback(async (jobId: number, value: QueuePriorityFilter) => {
    const fieldKey = `${jobId}:priority`;
    let previousUpdate: PendingQueueJobUpdate | undefined;
    setPendingJobUpdates((current) => {
      previousUpdate = current[jobId];
      return {
        ...current,
        [jobId]: {
          ...current[jobId],
          priority: value,
        },
      };
    });
    setQueueFieldSaving(fieldKey, true);
    const result = await updateJobs({ ids: [jobId], priority: value });
    setQueueFieldSaving(fieldKey, false);
    if (result.error) {
      setPendingJobUpdates((current) => {
        if (!Object.prototype.hasOwnProperty.call(current, jobId)) {
          return current;
        }
        if (!previousUpdate) {
          const next = { ...current };
          delete next[jobId];
          return next;
        }
        return {
          ...current,
          [jobId]: previousUpdate,
        };
      });
    } else {
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }
  }, [reexecuteQueuePage, setQueueFieldSaving, updateJobs]);

  const handleInlineCategoryChange = useCallback(async (jobId: number, value: string) => {
    const nextCategory = value === NO_CATEGORY_SELECT_VALUE ? null : value;
    const fieldKey = `${jobId}:category`;
    let previousUpdate: PendingQueueJobUpdate | undefined;
    setPendingJobUpdates((current) => {
      previousUpdate = current[jobId];
      return {
        ...current,
        [jobId]: {
          ...current[jobId],
          category: nextCategory,
        },
      };
    });
    setQueueFieldSaving(fieldKey, true);
    const result = await updateJobs({
      ids: [jobId],
      category: nextCategory ?? "",
    });
    setQueueFieldSaving(fieldKey, false);
    if (result.error) {
      setPendingJobUpdates((current) => {
        if (!Object.prototype.hasOwnProperty.call(current, jobId)) {
          return current;
        }
        if (!previousUpdate) {
          const next = { ...current };
          delete next[jobId];
          return next;
        }
        return {
          ...current,
          [jobId]: previousUpdate,
        };
      });
    } else {
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }
  }, [reexecuteQueuePage, setQueueFieldSaving, updateJobs]);

  const handlePrioritySelectValueChange = useCallback((jobId: number, value: string) => {
    void handleInlinePriorityChange(jobId, value as QueuePriorityFilter);
  }, [handleInlinePriorityChange]);

  const handleCategorySelectValueChange = useCallback((jobId: number, value: string) => {
    void handleInlineCategoryChange(jobId, value);
  }, [handleInlineCategoryChange]);

  const columns = useMemo<ColumnDef<QueueRowData>[]>(
    () => [
      {
        id: "select",
        enableSorting: false,
        enableHiding: false,
        header: ({ table }) => (
          <div className="flex justify-center">
            <Checkbox
              checked={
                table.getIsAllPageRowsSelected()
                  ? true
                  : table.getIsSomePageRowsSelected()
                    ? "indeterminate"
                    : false
              }
              onCheckedChange={(value) => table.toggleAllPageRowsSelected(value === true)}
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
              onCheckedChange={(value) => row.toggleSelected(value === true)}
            />
          </div>
        ),
        meta: {
          headerClassName: "h-7 w-[52px] px-2 text-center",
          cellClassName: "p-0 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "name",
        accessorKey: "displayName",
        header: ({ column }) => <DataTableColumnHeader column={column} title={t("table.name")} />,
        cell: ({ row }) => (
          <QueueNameCell jobId={row.original.id} displayName={row.original.displayName} />
        ),
        meta: {
          headerClassName: "h-7 w-[34%] px-2 text-left",
          cellClassName: "w-[34%] px-2 py-1.5 text-left align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "status",
        accessorKey: "status",
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.status")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <QueueStatusCell
            status={row.original.status}
            blockedByIspCap={row.original.blockedByIspCap}
            bandwidthCapLabel={t("jobs.bandwidthCapShort")}
          />
        ),
        meta: {
          headerClassName: "h-7 w-[104px] px-2 text-center",
          cellClassName: "w-[104px] px-2 py-1.5 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "priority",
        accessorFn: (job) => job.priorityRank,
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.priority")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <QueueCellSelect
            jobId={row.original.id}
            value={row.original.priorityValue}
            options={prioritySelectOptions}
            ariaLabel={`${t("upload.priorityLabel")} ${row.original.displayName}`}
            disabled={Boolean(savingQueueFields[`${row.original.id}:priority`])}
            onValueChange={handlePrioritySelectValueChange}
            className="w-[108px]"
          />
        ),
        meta: {
          headerClassName: "h-7 w-[124px] px-2 text-center",
          cellClassName: "w-[124px] px-2 py-1.5 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        accessorKey: "categoryLabel",
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.category")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <QueueCellSelect
            jobId={row.original.id}
            value={row.original.categoryValue ?? NO_CATEGORY_SELECT_VALUE}
            options={categorySelectOptions}
            ariaLabel={`${t("table.category")} ${row.original.displayName}`}
            disabled={Boolean(savingQueueFields[`${row.original.id}:category`])}
            onValueChange={handleCategorySelectValueChange}
            className="w-[136px]"
          />
        ),
        meta: {
          headerClassName: "h-7 w-[152px] px-2 text-center",
          cellClassName: "w-[152px] px-2 py-1.5 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "progress",
        accessorFn: (job) =>
          getDisplayedJobProgress({
            progress: job.progress,
            status: job.status,
            totalBytes: job.totalBytes,
            downloadedBytes: job.downloadedBytes,
            failedBytes: job.failedBytes,
          }),
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title={t("table.progress")}
            className="justify-center text-center"
          />
        ),
        cell: ({ row }) => (
          <QueueProgressCell phaseProgress={row.original.phaseProgress} />
        ),
        meta: {
          headerClassName: "h-7 w-[188px] px-2 text-center",
          cellClassName: "w-[188px] px-2 py-1.5 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "eta",
        enableSorting: false,
        header: () => <div className="text-center">{t("table.eta")}</div>,
        cell: ({ row }) => (
          <span className="tabular-nums text-muted-foreground">{row.original.etaDisplay}</span>
        ),
        meta: {
          headerClassName: "h-7 w-[96px] px-2 text-center",
          cellClassName: "w-[96px] px-2 py-1.5 text-center align-middle",
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
        cell: ({ row }) => <QueueSizeCell totalBytes={row.original.totalBytes} />,
        meta: {
          headerClassName: "h-7 w-[132px] px-2 text-center",
          cellClassName: "w-[132px] px-2 py-1.5 text-center align-middle",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "actions",
        enableSorting: false,
        header: () => <div className="text-right">{t("table.actions")}</div>,
        cell: ({ row }) => (
          <QueueActionButtons
            jobId={row.original.id}
            status={row.original.status}
            pauseLabel={t("action.pause")}
            resumeLabel={t("action.resume")}
            cancelLabel={t("action.cancel")}
            onPause={handlePauseJob}
            onResume={handleResumeJob}
            onCancel={handleCancelJob}
          />
        ),
        meta: {
          headerClassName: "h-7 w-[116px] px-2 text-right",
          cellClassName: "w-[116px] p-0 text-right align-middle",
        } satisfies DataTableColumnMeta,
      },
    ],
    [
      categorySelectOptions,
      handleCancelJob,
      handleCategorySelectValueChange,
      handlePauseJob,
      handlePrioritySelectValueChange,
      handleResumeJob,
      prioritySelectOptions,
      savingQueueFields,
      t,
    ],
  );

  const queueTable = useReactTable({
    data: queueTableRows,
    columns,
    getRowId: (row) => String(row.id),
    enableRowSelection: true,
    manualPagination: true,
    manualSorting: true,
    pageCount,
    getCoreRowModel: getCoreRowModel(),
    state: {
      pagination: {
        pageIndex,
        pageSize: queuePreferences.pageSize,
      },
      rowSelection,
      sorting: queuePreferences.sorting,
    },
    onRowSelectionChange: setRowSelection,
    onSortingChange: (updater) => {
      const next =
        typeof updater === "function"
          ? updater(queuePreferences.sorting)
          : updater;
      setQueuePreferences((current) => ({
        ...current,
        sorting: next,
      }));
      setPageIndex(0);
    },
    onPaginationChange: (updater) => {
      const next =
        typeof updater === "function"
          ? updater({
            pageIndex,
            pageSize: queuePreferences.pageSize,
          })
          : updater;

      if (next.pageSize !== queuePreferences.pageSize) {
        setQueuePreferences((current) => ({
          ...current,
          pageSize: next.pageSize,
        }));
        setPageIndex(0);
        return;
      }

      setPageIndex(next.pageIndex);
    },
  });

  const handleBulkEdit = async (category: string | null, priority: string | null) => {
    if (selectedIds.length === 0) {
      return;
    }

    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids: selectedIds,
      operationName: "UpdateSelectedJobs",
      aliasPrefix: "updateJob",
      fieldName: "updateJobs",
      sharedVariables: {
        category: { type: "String", value: category },
        priority: { type: "String", value: priority },
      },
      buildFieldArguments: (idVariable) =>
        `ids: [${idVariable}], category: $category, priority: $priority`,
    });
    if (!result.error) {
      setRowSelection({});
      setBulkEditOpen(false);
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }
  };

  const handleBulkPause = async () => {
    if (selectedIds.length === 0) {
      return;
    }

    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids: selectedIds,
      operationName: "PauseSelectedJobs",
      aliasPrefix: "pauseJob",
      fieldName: "pauseJob",
    });
    if (!result.error) {
      setRowSelection({});
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }
  };

  const handleBulkResume = async () => {
    if (selectedIds.length === 0) {
      return;
    }

    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids: selectedIds,
      operationName: "ResumeSelectedJobs",
      aliasPrefix: "resumeJob",
      fieldName: "resumeJob",
    });
    if (!result.error) {
      setRowSelection({});
      void reexecuteQueuePage({ requestPolicy: "network-only" });
    }
  };

  const handleBulkCancel = async () => {
    if (selectedIds.length === 0) {
      return;
    }

    hideQueueJobs(selectedIds);
    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids: selectedIds,
      operationName: "CancelSelectedJobs",
      aliasPrefix: "cancelJob",
      fieldName: "cancelJob",
    });
    const cancelledIds = selectedIds.filter(
      (_id, index) => result.data?.[`cancelJob${index}`] === true,
    );
    const failedIds = selectedIds.filter((id) => !cancelledIds.includes(id));
    if (failedIds.length > 0) {
      restoreQueueJobs(failedIds);
    }
    void reexecuteQueuePage({ requestPolicy: "network-only" });
    setCancelSelectedConfirm(false);
  };

  const activeQueueFilterCount = countActiveQueueFilters(queuePreferences);

  const applyStatusChip = useCallback(
    (statuses: QueueStatusFilter[]) => {
      setQueuePreferences((current) => ({ ...current, statuses }));
      setPageIndex(0);
    },
    [setQueuePreferences],
  );
  const statusChips: {
    key: string;
    label: string;
    count: number;
    statuses: QueueStatusFilter[];
  }[] = [
    { key: "all", label: t("history.filterAll"), count: queuePage?.summary.totalItems ?? 0, statuses: [] },
    {
      key: "active",
      label: t("queue.filterActive"),
      count: queuePage?.summary.activeItems ?? 0,
      statuses: QUEUE_ACTIVE_STATUSES,
    },
    {
      key: "queued",
      label: t("status.queued"),
      count: queuePage?.summary.queuedItems ?? 0,
      statuses: ["QUEUED"],
    },
    {
      key: "stalled",
      label: t("queue.filterStalled"),
      count: queuePage?.summary.pausedItems ?? 0,
      statuses: ["PAUSED"],
    },
  ];

  function resetQueueView() {
    setQueuePreferences((current) => ({
      ...current,
      search: "",
      statuses: [],
      priorities: [],
      categories: [],
      sorting: [],
    }));
    setPageIndex(0);
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("jobs.title")}
        actions={
          <>
            {hasNoServers ? (
              <Card className="flex h-[66px] shrink-0 items-center rounded-inner border-destructive/40 bg-destructive/8 shadow-none">
                <div className="flex h-full w-full items-center gap-2 px-4">
                  <Badge variant="destructive" className="px-2 py-0.5 text-[10px] uppercase tracking-[0.12em]">
                    {t("jobs.noServersBadge")}
                  </Badge>
                  <Button asChild variant="outline" size="sm" className="h-7 px-2 text-xs">
                    <Link to="/settings/servers">{t("jobs.noServersAction")}</Link>
                  </Button>
                </div>
              </Card>
            ) : null}
            <div className="flex overflow-hidden rounded-inner border border-border bg-card">
              <div className="border-r border-border px-4 py-2.5">
                <div className="text-[9.5px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                  {t("label.downloadSpeed")}
                </div>
                <div className="mt-0.5 font-space-grotesk text-lg font-bold text-foreground">
                  {formatSpeed(speed)}
                </div>
                {isPaused ? (
                  <div className="text-[10px] font-medium uppercase tracking-[0.14em] text-status-paused">
                    {t("jobs.downloadsPaused")}
                  </div>
                ) : null}
              </div>
              <button
                type="button"
                onClick={openSpeedLimitDialog}
                className="px-4 py-2.5 text-left transition-colors hover:bg-accent/40"
              >
                <div className="text-[9.5px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                  {t("settings.speedLimit")}
                </div>
                <div
                  className={cn(
                    "mt-0.5 font-space-grotesk text-lg font-bold",
                    effectiveSpeedLimit === 0 ? "text-status-completed" : "text-status-paused",
                  )}
                >
                  {effectiveSpeedLimit === 0 ? t("settings.unlimited") : formatSpeed(effectiveSpeedLimit)}
                </div>
              </button>
            </div>
            <Button
              variant={isPaused ? "default" : "outline"}
              onClick={() => void (isPaused ? resumeAll({}) : pauseAll({}))}
            >
              {isPaused ? <Play className="size-4" /> : <Pause className="size-4" />}
              {isPaused ? t("action.resumeAll") : t("action.pauseAll")}
            </Button>
            <Button onClick={() => setUploadOpen(true)}>{t("nav.upload")}</Button>
          </>
        }
      />

      {(downloadBlock.kind === "ISP_CAP" || downloadBlock.kind === "SERVER_QUOTA")
      && policyBlockedJobs > 0 ? (
        <Card className="border-orange-500/40 bg-orange-500/8">
          <CardContent className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="warning">
                  {downloadBlock.kind === "SERVER_QUOTA"
                    ? t("jobs.serverQuotaBadge")
                    : t("jobs.bandwidthCapBadge")}
                </Badge>
                <span className="text-sm font-medium text-foreground">
                  {downloadBlock.kind === "SERVER_QUOTA"
                    ? t("jobs.serverQuotaTitle")
                    : t("jobs.bandwidthCapTitle")}
                </span>
              </div>
              <div className="text-sm text-muted-foreground">
                {downloadBlock.kind === "SERVER_QUOTA"
                  ? t("jobs.serverQuotaBody")
                  : t("jobs.bandwidthCapBody", { resetAt: capResetAt })}
              </div>
              {isPaused && downloadBlock.kind === "ISP_CAP" ? (
                <div className="text-xs uppercase tracking-[0.14em] text-orange-700 dark:text-orange-300">
                  {t("jobs.bandwidthCapManualPauseNote")}
                </div>
              ) : null}
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <Button asChild variant="outline">
                <Link to={downloadBlock.kind === "SERVER_QUOTA" ? "/settings/servers" : "/settings/general"}>
                  {downloadBlock.kind === "SERVER_QUOTA"
                    ? t("jobs.serverQuotaOpenSettings")
                    : t("jobs.bandwidthCapOpenSettings")}
                </Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}

      {isQueueBootstrapPending ? (
        <div role="status" className="py-8 text-center text-sm text-muted-foreground">
          {t("label.loading")}
        </div>
      ) : totalCount === 0 ? (
        <EmptyState
          title={hasUnfilteredQueueItems ? t("queue.noMatches") : t("jobs.empty")}
          description={hasUnfilteredQueueItems ? undefined : t("jobs.emptyHint")}
          actionLabel={hasUnfilteredQueueItems ? t("action.clearFilters") : t("jobs.emptyAction")}
          onAction={hasUnfilteredQueueItems ? resetQueueView : () => setUploadOpen(true)}
        />
      ) : (
        <Card>
          <CardContent className="space-y-4 px-0 pb-0 pt-6">
            <div className="px-6">
              <DataTableToolbar
                className="lg:min-h-11"
                searchValue={queuePreferences.search}
                onSearchChange={(value) => {
                  setQueuePreferences((current) => ({
                    ...current,
                    search: value,
                  }));
                  setPageIndex(0);
                }}
                searchPlaceholder={t("jobs.searchPlaceholder")}
                centerContainerClassName="min-h-10"
                centerContent={selectedIds.length > 0 ? (
                  <div className="inline-flex h-10 min-w-0 items-center justify-center gap-1.5 rounded-md border border-border/70 bg-muted/20 px-2">
                    <span className="shrink-0 px-1 text-xs font-medium text-muted-foreground">
                      {t("bulk.selected", { count: selectedIds.length })}
                    </span>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="size-8"
                      aria-label={t("bulk.editSelected")}
                      title={t("bulk.editSelected")}
                      onClick={() => setBulkEditOpen(true)}
                    >
                      <Pencil className="size-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="size-8"
                      aria-label={t("action.resume")}
                      title={t("action.resume")}
                      onClick={() => void handleBulkResume()}
                    >
                      <Play className="size-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="size-8"
                      aria-label={t("bulk.pauseSelected")}
                      title={t("bulk.pauseSelected")}
                      onClick={() => void handleBulkPause()}
                    >
                      <Pause className="size-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="size-8 text-destructive hover:text-destructive"
                      aria-label={t("bulk.cancelSelected")}
                      title={t("bulk.cancelSelected")}
                      onClick={() => setCancelSelectedConfirm(true)}
                    >
                      <X className="size-4" />
                    </Button>
                  </div>
                ) : (
                  <div className="flex flex-wrap items-center justify-center gap-2">
                    {statusChips.map((chip) => (
                      <FilterChip
                        key={chip.key}
                        label={chip.label}
                        count={chip.count}
                        active={
                          chip.statuses.length === 0
                            ? queuePreferences.statuses.length === 0
                            : sameStatusSet(queuePreferences.statuses, chip.statuses)
                        }
                        onClick={() => applyStatusChip(chip.statuses)}
                      />
                    ))}
                  </div>
                )}
              >
                <SegmentedControl
                  size="sm"
                  ariaLabel={t("queue.layoutTable")}
                  value={queueLayout}
                  onValueChange={setQueueLayout}
                  options={[
                    {
                      value: "table",
                      icon: <TableIcon className="size-4" />,
                      title: t("queue.layoutTable"),
                    },
                    {
                      value: "compact",
                      icon: <Rows3 className="size-4" />,
                      title: t("queue.layoutCompact"),
                    },
                  ]}
                />
                <Popover>
                  <PopoverTrigger asChild>
                    <Button variant="outline" className="h-10 w-full justify-between gap-3 sm:w-[176px]">
                      <span className="inline-flex items-center gap-2">
                        <ListFilter className="size-4 text-muted-foreground" />
                        <span>{t("table.filters")}</span>
                      </span>
                      <span className="inline-flex items-center gap-2">
                        {activeQueueFilterCount > 0 ? (
                          <span className="rounded-full bg-muted px-2 py-0.5 text-[11px] font-medium text-foreground">
                            {activeQueueFilterCount}
                          </span>
                        ) : (
                          <span className="text-[11px] text-muted-foreground">
                            {t("history.filterAll")}
                          </span>
                        )}
                        <ChevronDown className="size-4 text-muted-foreground" />
                      </span>
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-[288px] p-0">
                    <div className="space-y-4 p-4">
                      <div className="space-y-2">
                        <div className="px-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                          {t("table.status")}
                        </div>
                        <div
                          role="button"
                          tabIndex={0}
                          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/40"
                          onClick={() => {
                            setQueuePreferences((current) => ({
                              ...current,
                              statuses: [],
                            }));
                            setPageIndex(0);
                          }}
                          onKeyDown={(event) => {
                            handleFilterOptionKeyDown(event, () => {
                              setQueuePreferences((current) => ({
                                ...current,
                                statuses: [],
                              }));
                              setPageIndex(0);
                            });
                          }}
                        >
                          <Checkbox
                            className="pointer-events-none"
                            tabIndex={-1}
                            aria-hidden="true"
                            checked={queuePreferences.statuses.length === 0}
                          />
                          <span className="text-sm">{t("history.filterAll")}</span>
                        </div>
                        {QUEUE_STATUS_OPTIONS.map((status) => (
                          <div
                            key={status}
                            role="button"
                            tabIndex={0}
                            className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/40"
                            onClick={() => {
                              setQueuePreferences((current) => ({
                                ...current,
                                statuses: toggleMultiSelectValue(current.statuses, status),
                              }));
                              setPageIndex(0);
                            }}
                            onKeyDown={(event) => {
                              handleFilterOptionKeyDown(event, () => {
                                setQueuePreferences((current) => ({
                                  ...current,
                                  statuses: toggleMultiSelectValue(current.statuses, status),
                                }));
                                setPageIndex(0);
                              });
                            }}
                          >
                            <Checkbox
                              className="pointer-events-none"
                              tabIndex={-1}
                              aria-hidden="true"
                              checked={queuePreferences.statuses.includes(status)}
                            />
                            <span className="text-sm">{queueStatusLabel(status, t)}</span>
                          </div>
                        ))}
                      </div>

                      <div className="border-t border-border/70 pt-4">
                        <div className="space-y-2">
                        <div className="px-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                          {t("table.priority")}
                        </div>
                        <div
                          role="button"
                          tabIndex={0}
                          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/40"
                          onClick={() => {
                            setQueuePreferences((current) => ({
                              ...current,
                              priorities: [],
                            }));
                            setPageIndex(0);
                          }}
                          onKeyDown={(event) => {
                            handleFilterOptionKeyDown(event, () => {
                              setQueuePreferences((current) => ({
                                ...current,
                                priorities: [],
                              }));
                              setPageIndex(0);
                            });
                          }}
                        >
                            <Checkbox
                              className="pointer-events-none"
                              tabIndex={-1}
                              aria-hidden="true"
                              checked={queuePreferences.priorities.length === 0}
                            />
                            <span className="text-sm">{t("history.filterAll")}</span>
                        </div>
                          {QUEUE_PRIORITY_OPTIONS.map((priority) => (
                            <div
                              key={priority}
                              role="button"
                              tabIndex={0}
                              className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/40"
                              onClick={() => {
                                setQueuePreferences((current) => ({
                                  ...current,
                                  priorities: toggleMultiSelectValue(current.priorities, priority),
                                }));
                                setPageIndex(0);
                              }}
                              onKeyDown={(event) => {
                                handleFilterOptionKeyDown(event, () => {
                                  setQueuePreferences((current) => ({
                                    ...current,
                                    priorities: toggleMultiSelectValue(current.priorities, priority),
                                  }));
                                  setPageIndex(0);
                                });
                              }}
                            >
                              <Checkbox
                                className="pointer-events-none"
                                tabIndex={-1}
                                aria-hidden="true"
                                checked={queuePreferences.priorities.includes(priority)}
                              />
                              <span className="text-sm">{formatJobPriority(priority)}</span>
                            </div>
                          ))}
                        </div>
                      </div>

                      <div className="border-t border-border/70 pt-4">
                        <div className="space-y-2">
                        <div className="px-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                          {t("table.category")}
                        </div>
                        <div
                          role="button"
                          tabIndex={queueCategories.length === 0 ? -1 : 0}
                          aria-disabled={queueCategories.length === 0}
                          onClick={() => {
                            if (queueCategories.length === 0) {
                              return;
                            }
                            setQueuePreferences((current) => ({
                              ...current,
                              categories: [],
                            }));
                            setPageIndex(0);
                          }}
                          onKeyDown={(event) => {
                            if (queueCategories.length === 0) {
                              return;
                            }
                            handleFilterOptionKeyDown(event, () => {
                              setQueuePreferences((current) => ({
                                ...current,
                                categories: [],
                              }));
                              setPageIndex(0);
                            });
                          }}
                          className={cn(
                            "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left",
                            queueCategories.length === 0
                              ? "cursor-default text-muted-foreground"
                              : "hover:bg-accent/40",
                          )}
                        >
                          <Checkbox
                            className="pointer-events-none"
                            tabIndex={-1}
                            aria-hidden="true"
                            checked={queuePreferences.categories.length === 0}
                            disabled={queueCategories.length === 0}
                          />
                          <span className="text-sm">{t("history.filterAll")}</span>
                        </div>
                          {queueCategories.map((category) => (
                            <div
                              key={category}
                              role="button"
                              tabIndex={0}
                              className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/40"
                              onClick={() => {
                                setQueuePreferences((current) => ({
                                  ...current,
                                  categories: toggleMultiSelectValue(current.categories, category),
                                }));
                                setPageIndex(0);
                              }}
                              onKeyDown={(event) => {
                                handleFilterOptionKeyDown(event, () => {
                                  setQueuePreferences((current) => ({
                                    ...current,
                                    categories: toggleMultiSelectValue(current.categories, category),
                                  }));
                                  setPageIndex(0);
                                });
                              }}
                            >
                              <Checkbox
                                className="pointer-events-none"
                                tabIndex={-1}
                                aria-hidden="true"
                                checked={queuePreferences.categories.includes(category)}
                              />
                              <span className="truncate text-sm">{category}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </PopoverContent>
                </Popover>
              </DataTableToolbar>
            </div>

            {queueLayout === "table" ? (
              <DataTable
                table={queueTable}
                wrapperClassName="max-h-[70vh]"
                rowClassName={queueRowClassName}
                virtualization={queueTableVirtualization}
                emptyState={
                  <div className="space-y-3 py-12 text-center">
                    <div className="text-sm text-muted-foreground">{t("queue.noMatches")}</div>
                    <div>
                      <Button variant="outline" onClick={resetQueueView}>
                        {t("action.clearFilters")}
                      </Button>
                    </div>
                  </div>
                }
              />
            ) : queueTable.getRowModel().rows.length === 0 ? (
              <div className="space-y-3 py-12 text-center">
                <div className="text-sm text-muted-foreground">{t("queue.noMatches")}</div>
                <div>
                  <Button variant="outline" onClick={resetQueueView}>
                    {t("action.clearFilters")}
                  </Button>
                </div>
              </div>
            ) : (
              <div className="max-h-[70vh] overflow-y-auto border-t border-border">
                {queueTable.getRowModel().rows.map((row) => {
                  const job = row.original;
                  const stages = getJobStages({ status: job.status });
                  return (
                    <div
                      key={row.id}
                      data-state={row.getIsSelected() ? "selected" : undefined}
                      className="group/row flex items-center gap-3 border-b border-border px-6 py-2.5 transition-colors last:border-0 hover:bg-accent/20 data-[state=selected]:bg-primary/[0.06]"
                    >
                      <div data-row-click-ignore="true" className="shrink-0">
                        <Checkbox
                          checked={row.getIsSelected()}
                          onCheckedChange={(value) => row.toggleSelected(value === true)}
                        />
                      </div>
                      <span className="flex shrink-0 items-center gap-0.5">
                        {stages.map((stage, index) => (
                          <span
                            key={`${stage}-${index}`}
                            title={index === 0 ? job.statusLabel : undefined}
                            className={cn(
                              "size-2 rounded-pill",
                              STATUS_BG_CLASS[statusToken(stage)],
                              isActiveStatus(stage) && "animate-status-pulse",
                            )}
                          />
                        ))}
                      </span>
                      <div className="min-w-0 flex-[1.6]">
                        <Link
                          to={`/jobs/${job.id}`}
                          title={job.displayName}
                          className="block truncate text-[13px] font-medium text-foreground"
                        >
                          {job.displayName}
                        </Link>
                      </div>
                      <div className="hidden min-w-[130px] flex-1 sm:block">
                        <JobPhaseProgressBars compact phaseProgress={job.phaseProgress} />
                      </div>
                      <span className="hidden w-16 shrink-0 text-right text-[12px] tabular-nums text-muted-foreground md:block">
                        {job.etaDisplay}
                      </span>
                      <span className="w-16 shrink-0 text-right text-[12px] tabular-nums text-muted-foreground">
                        {formatBytes(job.totalBytes)}
                      </span>
                      <div
                        data-row-click-ignore="true"
                        className="flex shrink-0 items-center gap-1 opacity-40 transition-opacity group-hover/row:opacity-100"
                      >
                        {job.status === "PAUSED" ? (
                          <Button
                            variant="ghost"
                            size="icon"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            title={t("action.resume")}
                            aria-label={t("action.resume")}
                            onClick={() => handleResumeJob(job.id)}
                          >
                            <Play className="size-3.5" />
                          </Button>
                        ) : (
                          <Button
                            variant="ghost"
                            size="icon"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            title={t("action.pause")}
                            aria-label={t("action.pause")}
                            onClick={() => handlePauseJob(job.id)}
                          >
                            <Pause className="size-3.5" />
                          </Button>
                        )}
                        <Button
                          variant="ghost"
                          size="icon"
                          className="size-7 text-muted-foreground hover:text-foreground"
                          title={t("action.cancel")}
                          aria-label={t("action.cancel")}
                          onClick={() => handleCancelJob(job.id)}
                        >
                          <X className="size-3.5" />
                        </Button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
            <DataTablePagination
              table={queueTable}
              totalCount={totalCount}
              pageSizeOptions={[...QUEUE_PAGE_SIZE_OPTIONS]}
              rowsPerPageLabel={t("table.rowsPerPage")}
              previousLabel={t("action.previous")}
              nextLabel={t("action.next")}
            />
          </CardContent>
        </Card>
      )}

      <ConfirmDialog
        open={cancelConfirmId != null}
        title={t("confirm.cancelJob")}
        message={t("confirm.cancelJobMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => void handleConfirmCancelJob()}
        onCancel={() => setCancelConfirmId(null)}
      />

      <ConfirmDialog
        open={cancelSelectedConfirm}
        title={t("confirm.cancelSelected", { count: selectedIds.length })}
        message={t("confirm.cancelSelectedMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => void handleBulkCancel()}
        onCancel={() => setCancelSelectedConfirm(false)}
      />

      <BulkEditModal
        open={bulkEditOpen}
        selectedCount={selectedIds.length}
        onClose={() => setBulkEditOpen(false)}
        onApply={handleBulkEdit}
      />

      <UploadModal
        open={uploadOpen}
        onClose={() => setUploadOpen(false)}
        onSubmitted={() => void reexecuteQueuePage({ requestPolicy: "network-only" })}
      />

      <Dialog open={speedLimitOpen} onOpenChange={setSpeedLimitOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t("settings.speedLimit")}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Checkbox
                checked={speedLimitIsUnlimited}
                onCheckedChange={(checked) => setSpeedLimitIsUnlimited(checked === true)}
              />
              <Label>{t("settings.unlimited")}</Label>
            </div>
            {!speedLimitIsUnlimited ? (
              <div className="space-y-2">
                <Label htmlFor="speed-limit-input">MB/s</Label>
                <Input
                  id="speed-limit-input"
                  type="number"
                  min="0"
                  step="0.1"
                  value={speedLimitInput}
                  onChange={(event) => setSpeedLimitInput(event.target.value)}
                />
              </div>
            ) : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSpeedLimitOpen(false)}>
              {t("action.cancel")}
            </Button>
            <Button onClick={applySpeedLimit}>{t("action.save")}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

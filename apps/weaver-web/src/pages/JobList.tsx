import {
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type RowSelectionState,
  type SortingState,
} from "@tanstack/react-table";
import { ChevronDown, ListFilter, Pause, Pencil, Play, X } from "lucide-react";
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
import { useClient, useMutation, useQuery } from "urql";
import { BulkEditModal } from "@/components/BulkEditModal";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { DataTable } from "@/components/data-table/DataTable";
import type { DataTableColumnMeta } from "@/components/data-table/DataTable";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { DataTablePagination } from "@/components/data-table/DataTablePagination";
import { DataTableToolbar } from "@/components/data-table/DataTableToolbar";
import { EmptyState } from "@/components/EmptyState";
import { JobProgress } from "@/components/JobProgress";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { PageHeader } from "@/components/PageHeader";
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
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
  SERVERS_QUERY,
  SET_SPEED_LIMIT_MUTATION,
  UPDATE_JOBS_MUTATION,
} from "@/graphql/queries";
import { executeAliasedIdMutation } from "@/graphql/aliased-mutations";
import {
  useLiveDownloadBlock,
  useLiveJobs,
  useLivePaused,
  useLiveSpeed,
} from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { useTablePreferences } from "@/lib/hooks/use-table-preferences";
import { getDisplayedJobProgress } from "@/lib/job-progress";
import { useStableQueueEta } from "@/lib/hooks/use-stable-queue-eta";
import { formatJobReleaseName, type JobData } from "@/lib/job-types";
import { cn } from "@/lib/utils";

type QueueStatusFilter =
  | "QUEUED"
  | "DOWNLOADING"
  | "PAUSED"
  | "VERIFYING"
  | "REPAIRING"
  | "EXTRACTING"
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
type OpenQueueCellSelect = {
  field: "priority" | "category";
  jobId: number;
} | null;
type QueueCellSelectField = NonNullable<OpenQueueCellSelect>["field"];

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

const QUEUE_PAGE_SIZE_OPTIONS = [25, 50, 100, 500] as const;
const DEFAULT_QUEUE_PREFERENCES: QueueTablePreferences = {
  pageSize: 50,
  search: "",
  statuses: [],
  priorities: [],
  categories: [],
  sorting: [],
};
const QUEUE_TABLE_PREFERENCES_KEY = "weaver.queue.table.preferences.v2";
const QUEUE_STATUS_OPTIONS: QueueStatusFilter[] = [
  "QUEUED",
  "DOWNLOADING",
  "PAUSED",
  "VERIFYING",
  "REPAIRING",
  "EXTRACTING",
  "MOVING",
];
const QUEUE_PRIORITY_OPTIONS: QueuePriorityFilter[] = ["HIGH", "NORMAL", "LOW"];
const NO_CATEGORY_SELECT_VALUE = "__no_category__";

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
  field,
  value,
  options,
  ariaLabel,
  disabled,
  open,
  onOpenChange,
  onValueChange,
  className,
}: {
  jobId: number;
  field: QueueCellSelectField;
  value: string;
  options: QueueSelectOption[];
  ariaLabel: string;
  disabled?: boolean;
  open?: boolean;
  onOpenChange?: (jobId: number, field: QueueCellSelectField, open: boolean) => void;
  onValueChange: (jobId: number, value: string) => void;
  className?: string;
}) {
  const handleOpenChange = useCallback((nextOpen: boolean) => {
    onOpenChange?.(jobId, field, nextOpen);
  }, [field, jobId, onOpenChange]);

  const handleValueChange = useCallback((nextValue: string) => {
    onValueChange(jobId, nextValue);
  }, [jobId, onValueChange]);

  return (
    <div className="flex justify-center" data-row-click-ignore="true">
      <Select
        value={value}
        open={open}
        onOpenChange={handleOpenChange}
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
        className="block min-h-6 whitespace-normal break-words text-xs font-medium leading-snug text-foreground"
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
      <JobStatusBadge status={status} compact className="px-1.5" />
      {blockedByIspCap ? (
        <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-orange-600 dark:text-orange-300">
          {bandwidthCapLabel}
        </span>
      ) : null}
    </div>
  );
});

const QueueProgressCell = memo(function QueueProgressCell({
  etaDisplay,
  progress,
  status,
  totalBytes,
  downloadedBytes,
  failedBytes,
}: {
  etaDisplay: string;
  progress: number;
  status: JobData["status"];
  totalBytes: number;
  downloadedBytes: number;
  failedBytes: number;
}) {
  return (
    <div className="flex justify-center">
      <div className="w-full max-w-[176px]">
        <div className="mb-1 text-right text-[10px] font-medium tabular-nums text-muted-foreground">
          {etaDisplay}
        </div>
        <JobProgress
          progress={progress}
          status={status}
          totalBytes={totalBytes}
          downloadedBytes={downloadedBytes}
          failedBytes={failedBytes}
          compact
          showLabel={false}
        />
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

function isBlockedByIspCap(
  job: { status: string },
  downloadBlock: { kind: string },
) {
  return (
    downloadBlock.kind === "ISP_CAP"
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

function pruneBooleanRecord<T extends Record<string, boolean>>(
  current: T,
  validIds: Set<string>,
): T {
  const entries = Object.entries(current);
  if (entries.length === 0) {
    return current;
  }

  let changed = false;
  const nextEntries: Array<[string, boolean]> = [];

  for (const [id, enabled] of entries) {
    if (enabled && validIds.has(id)) {
      nextEntries.push([id, enabled]);
      continue;
    }

    changed = true;
  }

  if (!changed && nextEntries.length === entries.length) {
    return current;
  }

  return Object.fromEntries(nextEntries) as T;
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
    case "MOVING":
      return t("status.moving");
    default:
      return status;
  }
}

function sameStringArray(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

export function JobList() {
  const client = useClient();
  const [serversResult] = useQuery({ query: SERVERS_QUERY });
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });
  const hasNoServers = (serversResult.data?.servers?.length ?? 1) === 0;
  const t = useTranslate();
  const [queuePreferences, setQueuePreferences] = useTablePreferences(
    QUEUE_TABLE_PREFERENCES_KEY,
    DEFAULT_QUEUE_PREFERENCES,
  );
  const [pageIndex, setPageIndex] = useState(0);
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});
  const [pendingJobUpdates, setPendingJobUpdates] = useState<Record<number, PendingQueueJobUpdate>>({});
  const [savingQueueFields, setSavingQueueFields] = useState<Record<string, boolean>>({});
  const [openQueueCellSelect, setOpenQueueCellSelect] = useState<OpenQueueCellSelect>(null);

  const allJobs = useLiveJobs();
  const speed = useLiveSpeed();
  const isPaused = useLivePaused();
  const downloadBlock = useLiveDownloadBlock();
  const jobs = allJobs.filter((job) => job.status !== "COMPLETE" && job.status !== "FAILED");
  const capBlockedJobs = jobs.filter((job) => isBlockedByIspCap(job, downloadBlock)).length;
  const capResetAt = formatResetAt(downloadBlock.windowEndsAtEpochMs);

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
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

  const selectedIds = useMemo(
    () => Object.entries(rowSelection)
      .filter(([, selected]) => selected)
      .map(([id]) => Number(id)),
    [rowSelection],
  );

  const queueCategoriesRef = useRef<string[]>([]);
  const queueCategories = useMemo(
    () => {
      const next = Array.from(
        new Set(
          jobs
            .map((job) => resolveJobCategory(job, pendingJobUpdates[job.id]))
            .filter((category): category is string => Boolean(category)),
        ),
      ).sort((left, right) => left.localeCompare(right));

      if (sameStringArray(queueCategoriesRef.current, next)) {
        return queueCategoriesRef.current;
      }

      queueCategoriesRef.current = next;
      return next;
    },
    [jobs, pendingJobUpdates],
  );

  const editableCategoryOptionsRef = useRef<string[]>([]);
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

      if (sameStringArray(editableCategoryOptionsRef.current, next)) {
        return editableCategoryOptionsRef.current;
      }

      editableCategoryOptionsRef.current = next;
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
    const nextCategories = queuePreferences.categories
      .filter((category) => queueCategories.includes(category));
    if (nextCategories.length === queuePreferences.categories.length) {
      return;
    }

    setQueuePreferences((current) => ({
      ...current,
      categories: current.categories.filter((category) => queueCategories.includes(category)),
    }));
  }, [queueCategories, queuePreferences.categories, setQueuePreferences]);

  useEffect(() => {
    const validIds = new Set(jobs.map((job) => String(job.id)));
    setRowSelection((current) => pruneBooleanRecord(current, validIds));
  }, [jobs]);

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

  const queueRows = useMemo(
    () =>
      jobs.filter((job) => {
        const pending = pendingJobUpdates[job.id];
        const matchesStatus =
          queuePreferences.statuses.length === 0
          || queuePreferences.statuses.includes(job.status as QueueStatusFilter);
        const priority = resolveJobPriority(job, pending);
        const matchesPriority =
          queuePreferences.priorities.length === 0
          || queuePreferences.priorities.includes(priority);
        const category = resolveJobCategory(job, pending);
        const matchesCategory =
          queuePreferences.categories.length === 0
          || (category != null && queuePreferences.categories.includes(category));
        return matchesStatus && matchesPriority && matchesCategory;
      }),
    [jobs, pendingJobUpdates, queuePreferences.categories, queuePreferences.priorities, queuePreferences.statuses],
  );

  const deferredSearch = useDeferredValue(queuePreferences.search.trim().toLowerCase());
  const queueEtaById = useStableQueueEta(jobs, speed);
  const queueTableRows = useMemo<QueueRowData[]>(
    () =>
      queueRows.map((job) => {
        const pending = pendingJobUpdates[job.id];
        const priorityValue = resolveJobPriority(job, pending);
        const categoryValue = resolveJobCategory(job, pending);
        const blockedByIspCap = isBlockedByIspCap(job, downloadBlock);
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
            ? t("jobs.bandwidthCapEta", { resetAt: capResetAt })
            : blockedByGlobalPause
              ? t("status.paused")
              : (queueEtaById.get(job.id) ?? "\u2014"),
        };
      }),
    [capResetAt, downloadBlock, isPaused, pendingJobUpdates, queueEtaById, queueRows, t],
  );
  const queueSearchIndex = useMemo(
    () =>
      new Map(
        queueTableRows.map((job) => [
          String(job.id),
          job.displayName.toLowerCase(),
        ]),
      ),
    [queueTableRows],
  );
  const pageCount = Math.max(1, Math.ceil(queueTableRows.length / queuePreferences.pageSize));

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
    }
  }, [setQueueFieldSaving, updateJobs]);

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
    }
  }, [setQueueFieldSaving, updateJobs]);

  const handleQueueCellSelectOpenChange = useCallback(
    (jobId: number, field: NonNullable<OpenQueueCellSelect>["field"], open: boolean) => {
      setOpenQueueCellSelect((current) => {
        if (!open) {
          return current?.jobId === jobId && current.field === field ? null : current;
        }
        if (current?.jobId === jobId && current.field === field) {
          return current;
        }
        return { jobId, field };
      });
    },
    [],
  );

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
          cellClassName: "p-0 text-center",
        } satisfies DataTableColumnMeta,
      },
      {
        id: "name",
        accessorKey: "displayName",
        header: ({ column }) => <DataTableColumnHeader column={column} title={t("table.name")} />,
        cell: ({ row }) => <QueueNameCell jobId={row.original.id} displayName={row.original.displayName} />,
        meta: {
          headerClassName: "h-7 w-[34%] px-2 text-left",
          cellClassName: "w-[34%] px-2 py-1.5 text-left",
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
          cellClassName: "w-[104px] px-2 py-1.5 text-center",
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
            field="priority"
            value={row.original.priorityValue}
            options={prioritySelectOptions}
            ariaLabel={`${t("upload.priorityLabel")} ${row.original.displayName}`}
            disabled={Boolean(savingQueueFields[`${row.original.id}:priority`])}
            open={openQueueCellSelect?.jobId === row.original.id && openQueueCellSelect.field === "priority"}
            onOpenChange={handleQueueCellSelectOpenChange}
            onValueChange={handlePrioritySelectValueChange}
            className="w-[108px]"
          />
        ),
        meta: {
          headerClassName: "h-7 w-[124px] px-2 text-center",
          cellClassName: "w-[124px] px-2 py-1.5 text-center",
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
            field="category"
            value={row.original.categoryValue ?? NO_CATEGORY_SELECT_VALUE}
            options={categorySelectOptions}
            ariaLabel={`${t("table.category")} ${row.original.displayName}`}
            disabled={Boolean(savingQueueFields[`${row.original.id}:category`])}
            open={openQueueCellSelect?.jobId === row.original.id && openQueueCellSelect.field === "category"}
            onOpenChange={handleQueueCellSelectOpenChange}
            onValueChange={handleCategorySelectValueChange}
            className="w-[136px]"
          />
        ),
        meta: {
          headerClassName: "h-7 w-[152px] px-2 text-center",
          cellClassName: "w-[152px] px-2 py-1.5 text-center",
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
          <QueueProgressCell
            etaDisplay={row.original.etaDisplay}
            progress={row.original.progress}
            status={row.original.status}
            totalBytes={row.original.totalBytes}
            downloadedBytes={row.original.downloadedBytes}
            failedBytes={row.original.failedBytes}
          />
        ),
        meta: {
          headerClassName: "h-7 w-[188px] px-2 text-center",
          cellClassName: "w-[188px] px-2 py-1.5 text-center",
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
          cellClassName: "w-[132px] px-2 py-1.5 text-center",
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
          cellClassName: "w-[116px] p-0 text-right",
        } satisfies DataTableColumnMeta,
      },
    ],
    [
      categorySelectOptions,
      handleCancelJob,
      handleCategorySelectValueChange,
      handleQueueCellSelectOpenChange,
      handlePauseJob,
      handlePrioritySelectValueChange,
      handleResumeJob,
      openQueueCellSelect,
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
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    globalFilterFn: (row, _columnId, filterValue) => {
      if (typeof filterValue !== "string" || filterValue.length === 0) {
        return true;
      }
      return queueSearchIndex.get(row.id)?.includes(filterValue) ?? false;
    },
    state: {
      globalFilter: deferredSearch,
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
    }
  };

  const handleBulkCancel = async () => {
    if (selectedIds.length === 0) {
      return;
    }

    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids: selectedIds,
      operationName: "CancelSelectedJobs",
      aliasPrefix: "cancelJob",
      fieldName: "cancelJob",
    });
    if (!result.error) {
      setRowSelection({});
    }
    setCancelSelectedConfirm(false);
  };

  const activeQueueFilterCount = countActiveQueueFilters(queuePreferences);

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
            <div className="min-w-[120px] rounded-xl border border-border/70 bg-background/70 px-4 py-2">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {t("label.downloadSpeed")}
              </div>
              <div className="text-base font-semibold text-foreground">
                {formatSpeed(speed)}
              </div>
              {isPaused ? (
                <div className="mt-1 text-[11px] font-medium uppercase tracking-[0.16em] text-amber-600 dark:text-amber-300">
                  {t("jobs.downloadsPaused")}
                </div>
              ) : null}
            </div>
            <button
              type="button"
              onClick={openSpeedLimitDialog}
              className="min-w-[120px] rounded-xl border border-border/70 bg-background/70 px-4 py-2 text-left transition hover:bg-accent/40"
            >
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {t("settings.speedLimit")}
              </div>
              <div className={`text-base font-semibold ${effectiveSpeedLimit === 0 ? "text-emerald-600 dark:text-emerald-400" : "text-amber-600 dark:text-amber-300"}`}>
                {effectiveSpeedLimit === 0 ? t("settings.unlimited") : formatSpeed(effectiveSpeedLimit)}
              </div>
            </button>
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

      {downloadBlock.kind === "ISP_CAP" && capBlockedJobs > 0 ? (
        <Card className="border-orange-500/40 bg-orange-500/8">
          <CardContent className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="warning">{t("jobs.bandwidthCapBadge")}</Badge>
                <span className="text-sm font-medium text-foreground">
                  {t("jobs.bandwidthCapTitle")}
                </span>
              </div>
              <div className="text-sm text-muted-foreground">
                {t("jobs.bandwidthCapBody", { resetAt: capResetAt })}
              </div>
              {isPaused ? (
                <div className="text-xs uppercase tracking-[0.14em] text-orange-700 dark:text-orange-300">
                  {t("jobs.bandwidthCapManualPauseNote")}
                </div>
              ) : null}
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <Button asChild variant="outline">
                <Link to="/settings/general">{t("jobs.bandwidthCapOpenSettings")}</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}

      {hasNoServers ? (
        <Card className="border-destructive/40 bg-destructive/8">
          <CardContent className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="destructive">{t("jobs.noServersBadge")}</Badge>
                <span className="text-sm font-medium text-foreground">
                  {t("jobs.noServersTitle")}
                </span>
              </div>
              <div className="text-sm text-muted-foreground">
                {t("jobs.noServersBody")}
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <Button asChild variant="outline">
                <Link to="/settings/servers">{t("jobs.noServersAction")}</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}

      {jobs.length === 0 ? (
        <EmptyState
          title={t("jobs.empty")}
          description={t("jobs.emptyHint")}
          actionLabel={t("jobs.emptyAction")}
          onAction={() => setUploadOpen(true)}
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
                searchContainerClassName="max-w-[280px]"
                searchInputClassName="h-10"
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
                ) : null}
              >
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

            <DataTable
              table={queueTable}
              wrapperClassName="max-h-[70vh]"
              rowClassName={() => "text-xs"}
              emptyState={
                <div className="space-y-3 py-12 text-center">
                  <div className="text-sm text-muted-foreground">{t("history.noMatches")}</div>
                  <div>
                    <Button variant="outline" onClick={resetQueueView}>
                      {t("action.clearFilters")}
                    </Button>
                  </div>
                </div>
              }
            />
            <DataTablePagination
              table={queueTable}
              totalCount={queueTable.getFilteredRowModel().rows.length}
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
        onConfirm={() => {
          if (cancelConfirmId != null) {
            void cancelJob({ id: cancelConfirmId });
          }
          setCancelConfirmId(null);
        }}
        onCancel={() => setCancelConfirmId(null)}
      />

      <ConfirmDialog
        open={cancelSelectedConfirm}
        title={t("confirm.cancelJobBatch")}
        message={t("confirm.cancelJobBatchMessage", { count: selectedIds.length })}
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

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />

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

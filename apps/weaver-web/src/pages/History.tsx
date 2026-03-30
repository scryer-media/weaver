import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Trash2 } from "lucide-react";
import { Link } from "react-router";
import { useClient, useMutation, useQuery, useSubscription } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  DELETE_ALL_HISTORY_MUTATION,
  DELETE_HISTORY_BATCH_MUTATION,
  DELETE_HISTORY_MUTATION,
  HISTORY_JOBS_COUNT_QUERY,
  HISTORY_FACADE_EVENTS_SUBSCRIPTION,
  HISTORY_JOBS_QUERY,
} from "@/graphql/queries";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

type HistoryJob = JobData;

type HistoryFilter = "all" | "success" | "failure";

const PAGE_SIZE = 50;

type FacadeHistoryJob = GraphqlJobData;

type HistoryCountResponse = {
  all: number;
  success: number;
  failure: number;
};

function normalizeHistoryJob(job: FacadeHistoryJob): HistoryJob {
  return normalizeJobData(job);
}

function encodeOffsetCursor(offset: number): string {
  return btoa(`off:${offset}`).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function historyFilterVariables(filter: HistoryFilter) {
  switch (filter) {
    case "success":
      return { states: ["COMPLETED"] };
    case "failure":
      return { states: ["FAILED"] };
    default:
      return null;
  }
}

export function History() {
  const t = useTranslate();
  const client = useClient();
  const [deleteState, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);
  const [deleteBatchState, deleteHistoryBatch] = useMutation(DELETE_HISTORY_BATCH_MUTATION);
  const [deleteAllState, deleteAllHistory] = useMutation(DELETE_ALL_HISTORY_MUTATION);

  const [jobs, setJobs] = useState<HistoryJob[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [initialFetching, setInitialFetching] = useState(true);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [deleteBatchConfirm, setDeleteBatchConfirm] = useState(false);
  const [deleteAllConfirm, setDeleteAllConfirm] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [filter, setFilter] = useState<HistoryFilter>("all");
  const [search, setSearch] = useState("");

  const desktopSentinelRef = useRef<HTMLDivElement>(null);
  const mobileSentinelRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const queryFilter = useMemo(() => historyFilterVariables(filter), [filter]);

  // Initial page + count
  const [{ data: initialData }] = useQuery({
    query: HISTORY_JOBS_QUERY,
    variables: { first: PAGE_SIZE, after: null, filter: queryFilter },
  });
  const [{ data: countData }, refetchCount] = useQuery<{ all: number; success: number; failure: number }>({
    query: HISTORY_JOBS_COUNT_QUERY,
  });

  useEffect(() => {
    setJobs([]);
    setTotalCount(0);
    setHasMore(true);
    setLoadingMore(false);
    setInitialFetching(true);
    setSelectedIds(new Set());
  }, [filter]);

  useEffect(() => {
    if (initialData?.historyItems) {
      const nextJobs = (initialData.historyItems as FacadeHistoryJob[]).map(normalizeHistoryJob);
      setJobs(nextJobs);
      setHasMore(nextJobs.length >= PAGE_SIZE);
      setInitialFetching(false);
    }
  }, [initialData?.historyItems]);

  useEffect(() => {
    if (!countData) {
      return;
    }
    const counts = countData as HistoryCountResponse;
    if (filter === "success") {
      setTotalCount(counts.success ?? 0);
      return;
    }
    if (filter === "failure") {
      setTotalCount(counts.failure ?? 0);
      return;
    }
    setTotalCount(counts.all ?? 0);
  }, [countData, filter]);

  // Load next page
  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    setLoadingMore(true);
    try {
      const result = await client
        .query(HISTORY_JOBS_QUERY, {
          first: PAGE_SIZE,
          after: encodeOffsetCursor(jobs.length),
          filter: queryFilter,
        })
        .toPromise();
      const nextJobs: HistoryJob[] = ((result.data?.historyItems ?? []) as FacadeHistoryJob[])
        .map(normalizeHistoryJob);
      if (nextJobs.length > 0) {
        setJobs((prev) => {
          const existingIds = new Set(prev.map((j) => j.id));
          const deduped = nextJobs.filter((j) => !existingIds.has(j.id));
          return [...prev, ...deduped];
        });
      }
      setHasMore(nextJobs.length >= PAGE_SIZE);
    } finally {
      setLoadingMore(false);
    }
  }, [loadingMore, hasMore, jobs.length, client, queryFilter]);

  // Desktop: sentinel is inside the scrollable Table container
  useEffect(() => {
    const el = desktopSentinelRef.current;
    const root = scrollRef.current;
    if (!el || !root || !hasMore) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry?.isIntersecting) void loadMore(); },
      { root, rootMargin: "200px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [hasMore, loadMore]);

  // Mobile: sentinel is in page flow, observed against viewport
  useEffect(() => {
    const el = mobileSentinelRef.current;
    if (!el || !hasMore) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry?.isIntersecting) void loadMore(); },
      { rootMargin: "200px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [hasMore, loadMore]);

  // Delete mutations update local state
  useEffect(() => {
    if (deleteState.data?.deleteHistory) {
      setJobs((deleteState.data.deleteHistory as FacadeHistoryJob[]).map(normalizeHistoryJob));
      void refetchCount();
    }
  }, [deleteState.data, refetchCount]);

  useEffect(() => {
    if (deleteBatchState.data?.deleteHistoryBatch) {
      setJobs(
        (deleteBatchState.data.deleteHistoryBatch as FacadeHistoryJob[]).map(normalizeHistoryJob),
      );
      setSelectedIds(new Set());
      void refetchCount();
    }
  }, [deleteBatchState.data, refetchCount]);

  useEffect(() => {
    if (deleteAllState.data?.deleteAllHistory) {
      setJobs(
        (deleteAllState.data.deleteAllHistory as FacadeHistoryJob[]).map(normalizeHistoryJob),
      );
      setSelectedIds(new Set());
      setTotalCount(0);
      setHasMore(false);
    }
  }, [deleteAllState.data]);

  // Prepend newly completed/failed jobs from event stream
  const handleSubscription = useCallback(
    (
      _prev: unknown,
      response: { queueEvents: { kind: string; itemId: number | null; state: string | null } },
    ) => {
      const event = response.queueEvents;
      if (
        (event.kind === "ITEM_COMPLETED"
          || (event.kind === "ITEM_STATE_CHANGED" && event.state === "FAILED")) &&
        event.itemId != null
      ) {
        void client
          .query(HISTORY_JOBS_QUERY, { first: 1, after: null, filter: queryFilter })
          .toPromise()
          .then((result) => {
            const newJobs: HistoryJob[] = ((result.data?.historyItems ?? []) as FacadeHistoryJob[])
              .map(normalizeHistoryJob);
            if (newJobs.length > 0) {
              setJobs((prev) => {
                const existingIds = new Set(prev.map((j) => j.id));
                const fresh = newJobs.filter((j) => !existingIds.has(j.id));
                return [...fresh, ...prev];
              });
              setTotalCount((c) => c + newJobs.length);
            }
          });
      }
      return [];
    },
    [client, queryFilter],
  );
  useSubscription({ query: HISTORY_FACADE_EVENTS_SUBSCRIPTION }, handleSubscription);

  const counts = useMemo(
    () => ({
      all: (countData as HistoryCountResponse | undefined)?.all ?? totalCount,
      success: (countData as HistoryCountResponse | undefined)?.success ?? 0,
      failure: (countData as HistoryCountResponse | undefined)?.failure ?? 0,
    }),
    [countData, totalCount],
  );
  const sortedJobs = useMemo(
    () =>
      [...jobs].sort(
        (left, right) =>
          (right.createdAt ?? 0) - (left.createdAt ?? 0) || right.id - left.id,
      ),
    [jobs],
  );
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

  const normalizedSearch = search.trim().toLowerCase();
  const filteredJobs = useMemo(
    () =>
      sortedJobs.filter((job) => {
        if (!normalizedSearch) {
          return true;
        }
        return [job.name, job.category ?? "", job.outputDir ?? ""]
          .join(" ")
          .toLowerCase()
          .includes(normalizedSearch);
      }),
    [normalizedSearch, sortedJobs],
  );

  const filteredIds = useMemo(() => new Set(filteredJobs.map((j) => j.id)), [filteredJobs]);
  const visibleSelectedCount = useMemo(
    () => [...selectedIds].filter((id) => filteredIds.has(id)).length,
    [selectedIds, filteredIds],
  );
  const allVisibleSelected = filteredJobs.length > 0 && visibleSelectedCount === filteredJobs.length;

  function toggleSelect(id: number) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }

  function toggleSelectAll() {
    if (allVisibleSelected) {
      setSelectedIds((prev) => {
        const next = new Set(prev);
        for (const id of filteredIds) {
          next.delete(id);
        }
        return next;
      });
    } else {
      setSelectedIds((prev) => {
        const next = new Set(prev);
        for (const id of filteredIds) {
          next.add(id);
        }
        return next;
      });
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("history.title")}
        description={t("history.empty")}
        actions={
          jobs.length > 0 ? (
            <div className="flex gap-2">
              {visibleSelectedCount > 0 ? (
                <Button variant="destructive" onClick={() => setDeleteBatchConfirm(true)}>
                  {t("action.delete")} ({visibleSelectedCount})
                </Button>
              ) : null}
              <Button variant="outline" onClick={() => setDeleteAllConfirm(true)}>
                {t("action.deleteAll")}
              </Button>
            </div>
          ) : undefined
        }
      />

      {jobs.length > 0 ? (
        <Card>
          <CardContent className="space-y-4 pt-6">
            <div className="flex flex-wrap gap-2">
              <FilterButton
                active={filter === "all"}
                label={t("history.filterAll")}
                count={counts.all}
                onClick={() => setFilter("all")}
              />
              <FilterButton
                active={filter === "success"}
                label={t("history.filterSuccess")}
                count={counts.success}
                onClick={() => setFilter("success")}
              />
              <FilterButton
                active={filter === "failure"}
                label={t("history.filterFailure")}
                count={counts.failure}
                onClick={() => setFilter("failure")}
              />
            </div>

            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="max-w-md flex-1">
                <Input
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder={t("history.searchPlaceholder")}
                />
              </div>
              {search || filter !== "all" ? (
                <Button
                  variant="ghost"
                  onClick={() => {
                    setFilter("all");
                    setSearch("");
                  }}
                >
                  {t("action.clearFilters")}
                </Button>
              ) : null}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {initialFetching && jobs.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
      ) : jobs.length === 0 ? (
        <EmptyState title={t("history.title")} description={t("history.empty")} />
      ) : filteredJobs.length === 0 ? (
        <Card>
          <CardContent className="space-y-3 py-12 text-center">
            <div className="text-sm text-muted-foreground">{t("history.noMatches")}</div>
            <div>
              <Button
                variant="outline"
                onClick={() => {
                  setFilter("all");
                  setSearch("");
                }}
              >
                {t("action.clearFilters")}
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <>
          <Card className="hidden lg:block">
            <CardContent className="px-0 pb-0">
              <Table ref={scrollRef} className="table-fixed" wrapperClassName="max-h-[1700px]">
                <TableHeader className="sticky top-0 z-10 bg-card">
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-7 w-[4%] px-2">
                      <Checkbox
                        checked={allVisibleSelected}
                        onCheckedChange={toggleSelectAll}
                        aria-label="Select all"
                      />
                    </TableHead>
                    <TableHead className="h-7 w-[32%] px-2 text-[9px]">{t("table.name")}</TableHead>
                    <TableHead className="h-7 w-[10%] px-2 text-[9px]">{t("table.status")}</TableHead>
                    <TableHead className="h-7 w-[16%] px-2 text-[9px]">{t("table.time")}</TableHead>
                    <TableHead className="h-7 w-[8%] px-2 text-right text-[9px]">{t("table.health")}</TableHead>
                    <TableHead className="h-7 w-[12%] px-2 text-right text-[9px]">{t("table.size")}</TableHead>
                    <TableHead className="h-7 w-[10%] px-2 text-[9px]">{t("table.category")}</TableHead>
                    <TableHead className="h-7 w-[6%] px-2 text-right text-[9px]">{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredJobs.map((job) => (
                    <TableRow
                      key={job.id}
                      className={cn("text-[11px]", selectedIds.has(job.id) && "bg-muted/50")}
                    >
                      <TableCell className="px-2 py-1.5">
                        <Checkbox
                          checked={selectedIds.has(job.id)}
                          onCheckedChange={() => toggleSelect(job.id)}
                          aria-label={`Select ${job.displayTitle}`}
                        />
                      </TableCell>
                      <TableCell className="min-w-0 px-2 py-1.5">
                        <Link
                          to={`/jobs/${job.id}`}
                          title={job.originalTitle}
                          className="block min-w-0 truncate text-[11px] font-medium leading-tight text-foreground transition hover:text-primary"
                        >
                          {job.displayTitle}
                        </Link>
                        {job.hasPassword ? (
                          <span className="ml-2 shrink-0 text-[9px] text-amber-500">
                            {t("jobs.passwordProtected")}
                          </span>
                        ) : null}
                      </TableCell>
                      <TableCell className="overflow-hidden px-2 py-1.5">
                        <JobStatusBadge status={job.status} compact className="px-1.5" />
                      </TableCell>
                      <TableCell
                        className="px-2 py-1.5 text-[10px] text-muted-foreground"
                        title={formatHistoryTimestamp(job.createdAt ?? null)}
                      >
                        {formatHistoryTimestamp(job.createdAt ?? null, timestampFormatter)}
                      </TableCell>
                      <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                        {(job.health / 10).toFixed(1)}%
                      </TableCell>
                      <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                        {formatBytes(job.totalBytes)}
                      </TableCell>
                      <TableCell className="truncate px-2 py-1.5 text-[10px] text-muted-foreground" title={job.category ?? "\u2014"}>
                        {job.category ?? "\u2014"}
                      </TableCell>
                      <TableCell className="px-2 py-1.5">
                        <div className="flex justify-end gap-0.5">
                          <Button
                            variant="ghost"
                            size="icon"
                            title={t("action.delete")}
                            aria-label={t("action.delete")}
                            className="size-6"
                            onClick={() => setDeleteConfirmId(job.id)}
                          >
                            <Trash2 className="size-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                  {hasMore ? (
                    <tr>
                      <td colSpan={8}>
                        <div ref={desktopSentinelRef} className="flex justify-center py-3 text-xs text-muted-foreground">
                          {loadingMore ? t("label.loading") : null}
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="space-y-3 lg:hidden">
            {filteredJobs.map((job) => (
              <Card key={job.id} className={cn(selectedIds.has(job.id) && "ring-1 ring-primary/40")}>
                <CardContent className="space-y-3">
                  <div className="flex items-start gap-3">
                    <Checkbox
                      checked={selectedIds.has(job.id)}
                      onCheckedChange={() => toggleSelect(job.id)}
                      className="mt-1"
                    />
                    <div className="min-w-0 flex-1">
                      <div className="flex items-start justify-between gap-3">
                        <Link
                          to={`/jobs/${job.id}`}
                          className="block truncate font-medium text-foreground transition hover:text-primary"
                        >
                          {job.displayTitle}
                        </Link>
                        <JobStatusBadge status={job.status} compact />
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                        <span>{formatHistoryTimestamp(job.createdAt ?? null, timestampFormatter)}</span>
                        <span>{formatBytes(job.totalBytes)}</span>
                        <span>Health {(job.health / 10).toFixed(1)}%</span>
                        {job.category ? <span>{job.category}</span> : null}
                      </div>
                    </div>
                  </div>
                  <div className="flex justify-end">
                    <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(job.id)}>
                      {t("action.delete")}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
            {hasMore ? (
              <div ref={mobileSentinelRef} className="flex justify-center py-3 text-xs text-muted-foreground">
                {loadingMore ? t("label.loading") : null}
              </div>
            ) : null}
          </div>
        </>
      )}

      {/* Single delete confirm */}
      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteHistory")}
        message={t("confirm.deleteHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => {
          if (deleteConfirmId != null) {
            void deleteHistory({ id: deleteConfirmId, deleteFiles });
            setSelectedIds((prev) => {
              const next = new Set(prev);
              next.delete(deleteConfirmId);
              return next;
            });
          }
          setDeleteConfirmId(null);
          setDeleteFiles(false);
        }}
        onCancel={() => { setDeleteConfirmId(null); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>

      {/* Batch delete confirm */}
      <ConfirmDialog
        open={deleteBatchConfirm}
        title={t("confirm.deleteHistory")}
        message={`Delete ${visibleSelectedCount} selected item${visibleSelectedCount === 1 ? "" : "s"} from history?`}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => {
          const ids = [...selectedIds].filter((id) => filteredIds.has(id));
          if (ids.length > 0) {
            void deleteHistoryBatch({ ids, deleteFiles });
          }
          setDeleteBatchConfirm(false);
          setDeleteFiles(false);
        }}
        onCancel={() => { setDeleteBatchConfirm(false); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>

      {/* Delete all confirm */}
      <ConfirmDialog
        open={deleteAllConfirm}
        title={t("confirm.deleteAllHistory")}
        message={t("confirm.deleteAllHistoryMessage")}
        confirmLabel={t("confirm.deleteAllHistoryConfirm")}
        cancelLabel={t("confirm.deleteAllHistoryDismiss")}
        onConfirm={() => {
          void deleteAllHistory({ deleteFiles });
          setDeleteAllConfirm(false);
          setDeleteFiles(false);
        }}
        onCancel={() => { setDeleteAllConfirm(false); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>
    </div>
  );
}

function formatHistoryTimestamp(
  timestamp: number | null,
  formatter = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }),
) {
  if (!timestamp) {
    return "-";
  }

  return formatter.format(new Date(timestamp));
}

function FilterButton({
  active,
  label,
  count,
  onClick,
}: {
  active: boolean;
  label: string;
  count: number;
  onClick: () => void;
}) {
  return (
    <Button variant={active ? "default" : "outline"} size="sm" onClick={onClick}>
      <span>{label}</span>
      <span
        className={cn(
          "ml-1.5 rounded-full px-1.5 py-0.5 text-[11px] leading-none",
          active
            ? "bg-primary-foreground/18 text-primary-foreground"
            : "bg-muted text-muted-foreground",
        )}
      >
        {count}
      </span>
    </Button>
  );
}

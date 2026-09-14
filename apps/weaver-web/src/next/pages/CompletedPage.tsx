import { useCallback, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  REDOWNLOAD_JOB_MUTATION,
  RERUN_POST_PROCESSING_MUTATION,
  SYSTEM_INFO_QUERY,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { formatJobReleaseName, normalizeGraphqlTimestamp } from "@/lib/job-types";
import { saveBlobAsDownload } from "@/lib/download";
import { cn } from "@/lib/utils";
import { statusToken } from "@/lib/status-tokens";
import { EmptyState, MetricCell, MetricStrip, SectionHeader, Square } from "../components/chrome";
import { BulkBar, BulkButton, BulkCluster } from "../components/BulkBar";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { Pagination } from "../components/Pagination";
import { CheckBox, SecondaryButton, TextField } from "../components/controls";
import { Icon } from "../components/icons";
import { Menu, MenuItem } from "../components/Menu";
import { GridHeader, GridRow } from "../components/rows";
import { StorageMounts, storageMounts, type StorageVolume } from "../components/storage";
import { Tabs } from "../components/Tabs";
import { useStartOfToday } from "../data/clock";
import { NEXT_HISTORY_PAGE_QUERY } from "../data/queries";
import {
  EM_DASH,
  formatClock,
  formatCount,
  formatDayLabel,
  formatElapsed,
  formatSize,
  splitSpeed,
  startOfDay,
} from "../data/format";
import { WV } from "../data/palette";
import { countLabel } from "../i18n/labels";
import { useStatusLabel } from "../data/status";
import { NextShell, RailBlock } from "../shell/NextShell";
import { CategoryListBlock } from "../shell/rail-blocks";
import {
  categoryFacets,
  facetsToCategories,
  NO_FACETS,
  toggleFacet,
} from "../data/categories";
import { useNextData } from "../data/next-data";
import { describeDeleteProgress } from "../data/history-deletes";
import { useHistoryDeletes } from "../data/use-history-deletes";
import { useHistoryLiveRefresh } from "../data/use-history-live-refresh";

/**
 * Completed — the archive of finished work.
 *
 * Everything the screen filters, sorts and pages by is server-side: history is
 * the one list weaver never holds in the client, so the search box, the tab
 * bar, the sort trigger and the pagination bar all feed `historyPage` rather
 * than a local filter chain. Day groups are built from the page that comes
 * back, which is why a group disappears when its rows are not on this page.
 *
 * Two outcomes only, per the handoff: Complete and Failed. A repaired job is
 * Complete — what par2 had to do about it belongs in the job's own event log,
 * not in a status someone has to interpret from a list.
 */

/** The row tracks, shared by the column header and every row. */
// Release and outcome are what this screen is for, so they are the two that
// never drop; size joins them once there is room, and the finished-at stack
// last, at the width the handoff was drawn for.
const COLUMNS = {
  base: "26px minmax(0, 1fr) 76px 14px",
  sm: "26px minmax(0, 2fr) minmax(96px, 1fr) 64px 14px",
  lg: "26px minmax(0, 2.4fr) minmax(112px, 1.1fr) 64px 84px 14px",
};
const PAGE_SIZES = [25, 50, 100] as const;
/** Deep enough to cover a busy day, cheap enough to ask for on every visit. */
const SAMPLE_SIZE = 200;

type TabId = "all" | "success" | "failure";
type SortId = "newest" | "largest" | "integrity";

const SORT_OPTIONS: {
  value: SortId;
  /** Translation key. */
  label: string;
  field: string;
  direction: "ASC" | "DESC";
}[] = [
  { value: "newest", label: "next.completed.sort.newest", field: "COMPLETED_AT", direction: "DESC" },
  { value: "largest", label: "next.completed.sort.largest", field: "SIZE", direction: "DESC" },
  { value: "integrity", label: "next.completed.sort.integrity", field: "HEALTH", direction: "ASC" },
];

const TAB_STATUS: Record<TabId, "ALL" | "SUCCESS" | "FAILURE"> = {
  all: "ALL",
  success: "SUCCESS",
  failure: "FAILURE",
};

interface HistoryRow {
  id: number;
  name: string;
  displayTitle: string;
  originalTitle: string;
  status: string;
  error: string | null;
  totalBytes: number;
  downloadedBytes: number;
  health: number;
  category: string | null;
  createdAt: string | number | null;
  completedAt: string | number | null;
  deleteOperation: { state: string; locked: boolean } | null;
}

interface HistoryPageResponse {
  historyPage: {
    items: HistoryRow[];
    totalCount: number;
    counts: { all: number; success: number; failure: number };
  };
}

interface DayGroup {
  key: number;
  label: string;
  rows: HistoryRow[];
  bytes: number;
}

function millis(value: string | number | null): number | null {
  return normalizeGraphqlTimestamp(value);
}

/** How long the job took, start to finish, or null when either end is missing. */
function elapsedMs(row: HistoryRow): number | null {
  const started = millis(row.createdAt);
  const finished = millis(row.completedAt);
  if (started === null || finished === null || finished < started) {
    return null;
  }
  return finished - started;
}

function groupByDay(t: Translate, rows: readonly HistoryRow[]): DayGroup[] {
  const groups = new Map<number, DayGroup>();
  for (const row of rows) {
    const finished = millis(row.completedAt);
    const key = finished === null ? 0 : startOfDay(finished);
    const group = groups.get(key);
    if (group) {
      group.rows.push(row);
      group.bytes += row.totalBytes;
    } else {
      groups.set(key, {
        key,
        label: finished === null ? t("next.day.undated") : formatDayLabel(t, finished),
        rows: [row],
        bytes: row.totalBytes,
      });
    }
  }
  return [...groups.values()];
}

function csvCell(value: string | number): string {
  const text = String(value);
  return /["\n,]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function CompletedPage() {
  const t = useTranslate();
  const navigate = useNavigate();
  const statusLabel = useStatusLabel();
  const { categories: configured, refreshHistoryCount } = useNextData();

  const [tab, setTab] = useState<TabId>("all");
  const [sort, setSort] = useState<SortId>("newest");
  const [sortOpen, setSortOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState<number>(50);
  const [picked, setPicked] = useState<ReadonlySet<number>>(() => new Set());
  const [facets, setFacets] = useState<ReadonlySet<string>>(NO_FACETS);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [report, setReport] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const midnight = useStartOfToday();

  const sortOption = SORT_OPTIONS.find((option) => option.value === sort)!;
  const categories = useMemo(() => facetsToCategories(facets), [facets]);
  const input = useMemo(
    () => ({
      pageIndex,
      pageSize,
      search: query.trim() === "" ? undefined : query.trim(),
      status: TAB_STATUS[tab],
      categories,
      sortField: sortOption.field,
      sortDirection: sortOption.direction,
    }),
    [categories, pageIndex, pageSize, query, sortOption.direction, sortOption.field, tab],
  );

  const [{ data, fetching }, reexecute] = useQuery<HistoryPageResponse>({
    query: NEXT_HISTORY_PAGE_QUERY,
    variables: { input },
  });
  // The strip's own numbers — today's jobs and what they averaged — need rows
  // the current page may not hold, so they come from a shallow newest-first
  // window rather than from whatever happens to be on screen.
  const [{ data: sampleData }, reexecuteSample] = useQuery<HistoryPageResponse>({
    query: NEXT_HISTORY_PAGE_QUERY,
    variables: {
      input: {
        pageIndex: 0,
        pageSize: SAMPLE_SIZE,
        status: "ALL",
        categories,
        sortField: "COMPLETED_AT",
        sortDirection: "DESC",
      },
    },
  });
  const [{ data: systemInfo }] = useQuery<{
    systemInfo: { configuredStorage: StorageVolume[] };
  }>({ query: SYSTEM_INFO_QUERY });

  const [, redownloadJob] = useMutation(REDOWNLOAD_JOB_MUTATION);
  const [, rerunPostProcessing] = useMutation(RERUN_POST_PROCESSING_MUTATION);

  const page = data?.historyPage;
  const pageRows = useMemo(() => page?.items ?? [], [page?.items]);
  const counts = page?.counts ?? { all: 0, success: 0, failure: 0 };
  const totalCount = page?.totalCount ?? 0;
  const pageCount = Math.max(1, Math.ceil(totalCount / pageSize));

  // A filter that shrinks the set can leave the page index past the end.
  const clampedPage = Math.min(pageIndex, pageCount - 1);
  if (clampedPage !== pageIndex) {
    setPageIndex(clampedPage);
  }

  // The rail counts the same history, so whatever made this page read again
  // (a drained delete, a bulk action) has changed its count too.
  const refresh = useCallback(() => {
    void reexecute({ requestPolicy: "network-only" });
    void reexecuteSample({ requestPolicy: "network-only" });
    refreshHistoryCount();
  }, [reexecute, reexecuteSample, refreshHistoryCount]);

  // Deletes run as background operations: rows handed to one stay on the page,
  // locked, until the last operation drains and the page is fetched again.
  const deletes = useHistoryDeletes({ rows: pageRows, onDrained: refresh });
  const refreshDeletes = deletes.refresh;
  const refreshLive = useCallback(() => {
    refresh();
    refreshDeletes();
  }, [refresh, refreshDeletes]);
  useHistoryLiveRefresh({ refresh: refreshLive, deletesActive: deletes.active });
  const rows = deletes.rows;
  const lockedIds = useMemo(
    () => new Set(rows.filter((row) => row.deleteOperation?.locked).map((row) => row.id)),
    [rows],
  );
  if ([...picked].some((id) => lockedIds.has(id))) {
    setPicked(new Set([...picked].filter((id) => !lockedIds.has(id))));
  }

  const days = useMemo(() => groupByDay(t, rows), [rows, t]);

  const reset = (change: () => void) => {
    change();
    setPageIndex(0);
  };

  const sample = useMemo(
    () => sampleData?.historyPage?.items ?? [],
    [sampleData?.historyPage?.items],
  );
  const metrics = useMemo(() => {
    const today = sample.filter((row) => (millis(row.completedAt) ?? 0) >= midnight);
    const timed = (today.length > 0 ? today : sample).filter((row) => elapsedMs(row) !== null);
    const bytes = timed.reduce((total, row) => total + (row.downloadedBytes || row.totalBytes), 0);
    const seconds = timed.reduce((total, row) => total + (elapsedMs(row) ?? 0), 0) / 1000;
    return {
      todayCount: today.length,
      todayBytes: today.reduce((total, row) => total + row.totalBytes, 0),
      rate: seconds > 0 ? bytes / seconds : 0,
      rateScope: today.length > 0 ? "today" : "recent",
      timedCount: timed.length,
      capped: sample.length >= SAMPLE_SIZE,
    };
  }, [midnight, sample]);

  // No counts here: history is paginated on the server, so the only number
  // this page could put beside a facet is "how many on the page you are
  // looking at", which is not what a number there would be read as.
  const facetItems = useMemo(
    () =>
      categoryFacets({
        configured,
        extras: rows.map((row) => row.category).filter((name): name is string => !!name),
      }),
    [configured, rows],
  );

  const volumes = systemInfo?.systemInfo?.configuredStorage ?? [];
  const mounts = storageMounts(volumes);
  const freeAcross = mounts.reduce(
    (total, mount) => total + (mount.capacity?.freeBytes ?? 0),
    0,
  );

  // A row already being deleted cannot be picked, so select-all passes over it.
  const selectable = rows.filter((row) => !lockedIds.has(row.id));
  const pickedOnPage = selectable.filter((row) => picked.has(row.id));
  const allPicked = selectable.length > 0 && pickedOnPage.length === selectable.length;

  const togglePicked = (id: number) => {
    setPicked((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  // Select-all is scoped to the page you are looking at, both ways.
  const toggleAll = () => {
    setPicked((current) => {
      const next = new Set(current);
      for (const row of selectable) {
        if (allPicked) {
          next.delete(row.id);
        } else {
          next.add(row.id);
        }
      }
      return next;
    });
  };

  const runOnPicked = async (
    kind: "requeued" | "rerun",
    run: (id: number) => Promise<{ error?: unknown }>,
  ) => {
    const ids = [...picked];
    setBusy(true);
    const results = await Promise.all(ids.map((id) => run(id)));
    const failures = results.filter((result) => result.error).length;
    setBusy(false);
    setPicked(new Set());
    setReport(
      failures === 0
        ? countLabel(t, `next.completed.bulk.${kind}`, ids.length, { count: formatCount(ids.length) })
        : t(`next.completed.bulk.${kind}Partial`, {
            done: formatCount(ids.length - failures),
            total: formatCount(ids.length),
            refused: formatCount(failures),
          }),
    );
    refresh();
  };

  const deletePicked = async (deleteFiles: boolean) => {
    const ids = [...picked];
    setDeleteError(null);
    const error = await deletes.accept(ids, deleteFiles);
    if (error !== null) {
      setDeleteError(error);
      return;
    }
    setConfirmDelete(false);
    setReport(null);
    setPicked((current) => new Set([...current].filter((id) => !ids.includes(id))));
  };

  const actionsBusy = busy || deletes.accepting;

  // One set of actions, shown in the tab row where it fits and in its own bar where it does not.
  const bulkActions = (
    <>
      <BulkButton
        icon="redownload"
        disabled={actionsBusy}
        onClick={() => {
          void runOnPicked("requeued", (id) => redownloadJob({ id }));
        }}
      >
        {t("next.completed.redownload")}
      </BulkButton>
      <BulkButton
        icon="postProcessing"
        disabled={actionsBusy}
        onClick={() => {
          void runOnPicked("rerun", (id) => rerunPostProcessing({ jobId: id }));
        }}
      >
        {t("next.completed.rerunScripts")}
      </BulkButton>
      <BulkButton
        icon="remove"
        tone="danger"
        disabled={actionsBusy}
        onClick={() => {
          setDeleteError(null);
          setConfirmDelete(true);
        }}
      >
        {t("action.delete")}
      </BulkButton>
    </>
  );

  const exportList = () => {
    const header = [
      "release",
      "outcome",
      "category",
      "size_bytes",
      "integrity_percent",
      "finished",
      "elapsed_seconds",
      "error",
    ];
    const lines = rows.map((row) => {
      const finished = millis(row.completedAt);
      const elapsed = elapsedMs(row);
      return [
        row.name,
        statusToken(row.status) === "failed" ? "failed" : "complete",
        row.category ?? "",
        row.totalBytes,
        (row.health / 10).toFixed(1),
        finished === null ? "" : new Date(finished).toISOString(),
        elapsed === null ? "" : Math.round(elapsed / 1000),
        row.error ?? "",
      ]
        .map(csvCell)
        .join(",");
    });
    const stamp = new Date().toISOString().slice(0, 10);
    saveBlobAsDownload(
      new Blob([[header.join(","), ...lines].join("\n")], { type: "text/csv;charset=utf-8" }),
      `weaver-history-${stamp}.csv`,
    );
    setReport(countLabel(t, "next.completed.exported", rows.length, { count: formatCount(rows.length) }));
  };

  const rate = splitSpeed(metrics.rate);

  return (
    <NextShell
      title={t("next.nav.completed")}
      note={countLabel(t, "next.completed.kept", counts.all, { count: formatCount(counts.all) })}
      controls={
        <>
          <TextField
            label={t("next.completed.filterLabel")}
            placeholder={t("next.completed.filterPlaceholder")}
            mono={false}
            value={query}
            onChange={(next) => reset(() => setQuery(next))}
            className="w-[118px] min-w-[80px] sm:w-[172px] sm:min-w-[96px]"
          />
          <SecondaryButton icon="downloadFile" onClick={exportList} disabled={rows.length === 0}>
            {t("next.completed.export")}
          </SecondaryButton>
        </>
      }
      railMiddle={
        <CategoryListBlock
          items={facetItems}
          selected={facets}
          onToggle={(key) => reset(() => setFacets((current) => toggleFacet(current, key)))}
          onClear={() => reset(() => setFacets(NO_FACETS))}
        />
      }
      railFooter={
        <RailBlock eyebrow={t("next.completed.archive")}>
          <StorageMounts volumes={volumes} layout="stack" />
        </RailBlock>
      }
      beforeContent={
        <>
          <MetricStrip>
            <MetricCell
              variant="strip"
              eyebrow={t("next.day.today")}
              value={metrics.capped && metrics.todayCount >= SAMPLE_SIZE
                ? `${SAMPLE_SIZE}+`
                : formatCount(metrics.todayCount)}
              unit={countLabel(t, "next.completed.jobsUnit", metrics.todayCount)}
              note={t("next.completed.downloaded", { size: formatSize(metrics.todayBytes) })}
            />
            <MetricCell
              variant="strip"
              eyebrow={t("next.completed.complete")}
              value={formatCount(counts.success)}
              unit={t("next.completed.ofTotal", { total: formatCount(counts.all) })}
            />
            <MetricCell
              variant="strip"
              eyebrow={t("status.failed")}
              value={formatCount(counts.failure)}
              unit={t("next.completed.ofTotal", { total: formatCount(counts.all) })}
              valueClassName={counts.failure > 0 ? "text-wv-error" : undefined}
            />
            <MetricCell
              variant="strip"
              eyebrow={t("next.completed.avgThroughput")}
              value={metrics.rate > 0 ? rate.value : EM_DASH}
              unit={metrics.rate > 0 ? rate.unit : undefined}
              note={
                metrics.rate <= 0
                  ? t("next.completed.nothingFinished")
                  : metrics.rateScope === "today"
                    ? t("next.completed.rateToday")
                    : countLabel(t, "next.completed.rateRecent", metrics.timedCount)
              }
            />
          </MetricStrip>

          <Tabs
            tabs={[
              { id: "all", label: t("history.filterAll"), count: counts.all },
              { id: "success", label: t("next.completed.complete"), count: counts.success },
              { id: "failure", label: t("status.failed"), count: counts.failure },
            ]}
            active={tab}
            onSelect={(next) => reset(() => setTab(next))}
            center={
              picked.size === 0 ? undefined : (
                <BulkCluster count={picked.size} onClear={() => setPicked(new Set())}>
                  {bulkActions}
                </BulkCluster>
              )
            }
            right={
              <div className="relative">
                <button
                  type="button"
                  data-wv-menu-trigger=""
                  aria-haspopup="menu"
                  aria-expanded={sortOpen}
                  onClick={() => setSortOpen((previous) => !previous)}
                  className="flex cursor-pointer items-center gap-[7px] font-wv-mono text-[11px] text-wv-muted hover:text-wv-fg"
                >
                  {t(sortOption.label)}
                  <Icon name="dropdown" size={12} className="text-wv-disabled" />
                </button>
                <Menu
                  open={sortOpen}
                  onDismiss={() => setSortOpen(false)}
                  label={t("next.completed.sortMenu")}
                  className="top-[26px] right-0 w-[196px]"
                >
                  {SORT_OPTIONS.map((option) => (
                    <MenuItem
                      key={option.value}
                      selected={option.value === sort}
                      onSelect={() => {
                        reset(() => setSort(option.value));
                        setSortOpen(false);
                      }}
                    >
                      {t(option.label)}
                    </MenuItem>
                  ))}
                </Menu>
              </div>
            }
          />

          {/* From xl the actions sit in the tab row instead; below it there is no room. */}
          {picked.size === 0 ? null : (
            <BulkBar
              count={picked.size}
              onClear={() => setPicked(new Set())}
              className="xl:hidden"
            >
              {bulkActions}
            </BulkBar>
          )}

          <GridHeader
            columns={COLUMNS}
            cells={[
              <CheckBox
                key="all"
                label={t("next.completed.selectPage")}
                checked={allPicked}
                onChange={toggleAll}
              />,
              t("next.completed.release"),
              t("next.completed.outcome"),
              <span key="size" className="block text-right">
                {t("table.size")}
              </span>,
              <span key="done" className="block text-right">
                {t("next.completed.done")}
              </span>,
              "",
            ]}
            cellClassNames={[undefined, undefined, undefined, "hidden sm:block", "hidden lg:block"]}
          />
        </>
      }
      afterContent={
        <Pagination
          pageIndex={clampedPage}
          pageCount={pageCount}
          onPage={(next) => setPageIndex(Math.max(0, Math.min(pageCount - 1, next)))}
          pageSize={pageSize}
          pageSizes={PAGE_SIZES}
          onPageSize={(next) => reset(() => setPageSize(next))}
          total={totalCount}
        />
      }
      statusNote={
        deletes.active ? describeDeleteProgress(t, deletes.progress) : (report ?? undefined)
      }
      statusRight={
        mounts.length === 0
          ? undefined
          : mounts.length === 1
            ? t("next.completed.freeOn", { size: formatSize(freeAcross), volume: mounts[0].label })
            : t("next.completed.freeAcross", { size: formatSize(freeAcross), count: mounts.length })
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {rows.length === 0 ? (
          <EmptyState
            loading={fetching}
            title={fetching ? t("next.common.loading") : t("next.downloads.noMatchTitle")}
            body={fetching ? t("next.completed.loadingBody") : t("next.completed.noMatchBody")}
          />
        ) : (
          days.map((day) => (
            <section key={day.key} className="flex flex-none flex-col">
              <SectionHeader
                label={day.label}
                count={countLabel(t, "next.completed.items", day.rows.length)}
                note={t("next.completed.archived", { size: formatSize(day.bytes) })}
              />
              {day.rows.map((row) => {
                const token = statusToken(row.status);
                const failed = token === "failed";
                const removing = row.deleteOperation !== null;
                const locked = lockedIds.has(row.id);
                const finished = millis(row.completedAt);
                const elapsed = elapsedMs(row);
                return (
                  <GridRow
                    key={row.id}
                    columns={COLUMNS}
                    selected={picked.has(row.id)}
                    onClick={() => navigate(`/jobs/${row.id}`)}
                    title={
                      row.error
                        ? `${formatJobReleaseName(row)}\n${row.error}`
                        : formatJobReleaseName(row)
                    }
                    className={cn(
                      "border-b border-wv-hairline px-4 sm:px-[22px] py-[11px]",
                      locked && "opacity-60",
                    )}
                  >
                    <CheckBox
                      label={t("next.common.selectItem", { name: formatJobReleaseName(row) })}
                      checked={picked.has(row.id)}
                      disabled={locked}
                      onChange={() => togglePicked(row.id)}
                    />
                    <div className="min-w-0 truncate font-wv-mono text-[12.5px] text-wv-fg">
                      {formatJobReleaseName(row)}
                    </div>
                    <div className="flex min-w-0 items-center gap-[7px]">
                      <Square
                        color={removing ? WV.idle : failed ? WV.error : WV.accent}
                      />
                      <span className="truncate text-[12.5px] text-wv-secondary">
                        {removing
                          ? t("next.completed.deleting")
                          : token === "completed"
                            ? t("next.completed.complete")
                            : failed
                              ? t("status.failed")
                              : statusLabel(row.status)}
                      </span>
                    </div>
                    <div className="hidden text-right font-wv-mono text-[12.5px] text-wv-secondary sm:block">
                      {formatSize(row.totalBytes)}
                    </div>
                    <div className="hidden min-w-0 flex-col items-end gap-1 lg:flex">
                      <span className="font-wv-mono text-[11.5px] whitespace-nowrap text-wv-secondary">
                        {formatClock(finished)}
                      </span>
                      <span className="font-wv-mono text-[10.5px] whitespace-nowrap text-wv-faint">
                        {elapsed === null ? EM_DASH : t("next.completed.took", { span: formatElapsed(elapsed) })}
                      </span>
                    </div>
                    <div className="flex justify-end text-wv-dim">
                      <Icon name="open" size={14} />
                    </div>
                  </GridRow>
                );
              })}
            </section>
          ))
        )}
      </div>

      <ConfirmDialog
        open={confirmDelete}
        title={t("action.delete")}
        note={t("bulk.selected", { count: picked.size })}
        busy={actionsBusy}
        body={
          <>
            {t("next.completed.deleteBody")}
            {deleteError === null ? null : (
              <span className="mt-3 block text-wv-error-text">{deleteError}</span>
            )}
          </>
        }
        alternative={{ label: t("next.job.deleteSaveFiles"), onConfirm: () => void deletePicked(false) }}
        confirmLabel={t("action.delete")}
        onConfirm={() => void deletePicked(true)}
        onDismiss={() => setConfirmDelete(false)}
      />
    </NextShell>
  );
}

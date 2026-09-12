import { useCallback, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  ACCEPT_HISTORY_DELETE_MUTATION,
  REDOWNLOAD_JOB_MUTATION,
  RERUN_POST_PROCESSING_MUTATION,
  SYSTEM_INFO_QUERY,
} from "@/graphql/queries";
import { normalizeGraphqlTimestamp } from "@/lib/job-types";
import { saveBlobAsDownload } from "@/lib/download";
import { statusToken } from "@/lib/status-tokens";
import { EmptyState, MetricCell, MetricStrip, SectionHeader, Square } from "../components/chrome";
import { BulkBar, BulkButton } from "../components/BulkBar";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { Pagination } from "../components/Pagination";
import { CheckBox, SecondaryButton, TextField } from "../components/controls";
import { Menu, MenuItem } from "../components/Menu";
import { GridHeader, GridRow } from "../components/rows";
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
import { useStatusLabel } from "../data/status";
import { NextShell, RailBlock } from "../shell/NextShell";

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
  label: string;
  field: string;
  direction: "ASC" | "DESC";
}[] = [
  { value: "newest", label: "Newest first", field: "COMPLETED_AT", direction: "DESC" },
  { value: "largest", label: "Largest first", field: "SIZE", direction: "DESC" },
  { value: "integrity", label: "Lowest integrity first", field: "HEALTH", direction: "ASC" },
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

interface StorageVolume {
  labels: string[];
  path: string;
  capacity: { totalBytes: number; usedBytes: number; freeBytes: number } | null;
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

function groupByDay(rows: readonly HistoryRow[]): DayGroup[] {
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
        label: finished === null ? "Undated" : formatDayLabel(finished),
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
  const navigate = useNavigate();
  const statusLabel = useStatusLabel();

  const [tab, setTab] = useState<TabId>("all");
  const [sort, setSort] = useState<SortId>("newest");
  const [sortOpen, setSortOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState<number>(50);
  const [picked, setPicked] = useState<ReadonlySet<number>>(() => new Set());
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [report, setReport] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const midnight = useStartOfToday();

  const sortOption = SORT_OPTIONS.find((option) => option.value === sort)!;
  const input = useMemo(
    () => ({
      pageIndex,
      pageSize,
      search: query.trim() === "" ? undefined : query.trim(),
      status: TAB_STATUS[tab],
      sortField: sortOption.field,
      sortDirection: sortOption.direction,
    }),
    [pageIndex, pageSize, query, sortOption.direction, sortOption.field, tab],
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
  const [, acceptHistoryDelete] = useMutation(ACCEPT_HISTORY_DELETE_MUTATION);

  const page = data?.historyPage;
  const rows = useMemo(() => page?.items ?? [], [page?.items]);
  const counts = page?.counts ?? { all: 0, success: 0, failure: 0 };
  const totalCount = page?.totalCount ?? 0;
  const pageCount = Math.max(1, Math.ceil(totalCount / pageSize));
  const days = useMemo(() => groupByDay(rows), [rows]);

  // A filter that shrinks the set can leave the page index past the end.
  const clampedPage = Math.min(pageIndex, pageCount - 1);
  if (clampedPage !== pageIndex) {
    setPageIndex(clampedPage);
  }

  const refresh = useCallback(() => {
    void reexecute({ requestPolicy: "network-only" });
    void reexecuteSample({ requestPolicy: "network-only" });
  }, [reexecute, reexecuteSample]);

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
      rateScope: today.length > 0 ? "end to end, today" : `end to end, last ${timed.length} jobs`,
      capped: sample.length >= SAMPLE_SIZE,
    };
  }, [midnight, sample]);

  const volumes = systemInfo?.systemInfo?.configuredStorage ?? [];
  const library =
    volumes.find((volume) => volume.labels.some((label) => label.startsWith("Complete")))
    ?? volumes[0]
    ?? null;

  const pickedOnPage = rows.filter((row) => picked.has(row.id));
  const allPicked = rows.length > 0 && pickedOnPage.length === rows.length;

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
      for (const row of rows) {
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
    label: string,
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
        ? `${label} ${formatCount(ids.length)} ${ids.length === 1 ? "entry" : "entries"}`
        : `${label} ${formatCount(ids.length - failures)} of ${formatCount(ids.length)} — ${formatCount(failures)} refused`,
    );
    refresh();
  };

  const deletePicked = async () => {
    const ids = [...picked];
    setBusy(true);
    const result = await acceptHistoryDelete({
      input: { mode: "IDS", ids, deleteFiles: false },
    });
    setBusy(false);
    setConfirmDelete(false);
    setPicked(new Set());
    setReport(
      result.error
        ? result.error.message
        : `Removing ${formatCount(ids.length)} ${ids.length === 1 ? "entry" : "entries"} from history`,
    );
    refresh();
  };

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
    setReport(`Exported this page — ${formatCount(rows.length)} entries`);
  };

  const rate = splitSpeed(metrics.rate);

  return (
    <NextShell
      title="Completed"
      note={`${formatCount(counts.all)} jobs kept`}
      controls={
        <>
          <TextField
            label="Filter completed by name"
            placeholder="Filter by name"
            mono={false}
            value={query}
            onChange={(next) => reset(() => setQuery(next))}
            className="w-[118px] min-w-[80px] sm:w-[172px] sm:min-w-[96px]"
          />
          <SecondaryButton onClick={exportList} disabled={rows.length === 0}>
            Export list
          </SecondaryButton>
        </>
      }
      railFooter={
        <RailBlock eyebrow="Archive">
          <div className="flex items-baseline justify-between gap-3 text-[12.5px] text-wv-tertiary">
            <span>Used</span>
            <span className="font-wv-mono text-[11px] text-wv-muted">
              {library?.capacity ? formatSize(library.capacity.usedBytes) : EM_DASH}
            </span>
          </div>
          <div className="flex items-baseline justify-between gap-3 text-[12.5px] text-wv-tertiary">
            <span>Free</span>
            <span className="font-wv-mono text-[11px] text-wv-muted">
              {library?.capacity ? formatSize(library.capacity.freeBytes) : EM_DASH}
            </span>
          </div>
          <div className="font-wv-mono text-[10.5px] break-all text-wv-faint">
            {library?.path ?? "no library folder configured"}
          </div>
        </RailBlock>
      }
      beforeContent={
        <>
          <MetricStrip>
            <MetricCell
              variant="strip"
              eyebrow="Today"
              value={metrics.capped && metrics.todayCount >= SAMPLE_SIZE
                ? `${SAMPLE_SIZE}+`
                : formatCount(metrics.todayCount)}
              unit={metrics.todayCount === 1 ? "job" : "jobs"}
              note={`${formatSize(metrics.todayBytes)} downloaded`}
            />
            <MetricCell
              variant="strip"
              eyebrow="Complete"
              value={formatCount(counts.success)}
              unit={`of ${formatCount(counts.all)}`}
            />
            <MetricCell
              variant="strip"
              eyebrow="Failed"
              value={formatCount(counts.failure)}
              unit={`of ${formatCount(counts.all)}`}
              valueClassName={counts.failure > 0 ? "text-wv-error" : undefined}
            />
            <MetricCell
              variant="strip"
              eyebrow="Avg throughput"
              value={metrics.rate > 0 ? rate.value : EM_DASH}
              unit={metrics.rate > 0 ? rate.unit : undefined}
              note={metrics.rate > 0 ? metrics.rateScope : "nothing finished yet"}
            />
          </MetricStrip>

          <Tabs
            tabs={[
              { id: "all", label: "All", count: counts.all },
              { id: "success", label: "Complete", count: counts.success },
              { id: "failure", label: "Failed", count: counts.failure },
            ]}
            active={tab}
            onSelect={(next) => reset(() => setTab(next))}
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
                  {sortOption.label}
                  <span aria-hidden="true" className="text-[8px] text-wv-disabled">
                    &#9660;
                  </span>
                </button>
                <Menu
                  open={sortOpen}
                  onDismiss={() => setSortOpen(false)}
                  label="Sort completed"
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
                      {option.label}
                    </MenuItem>
                  ))}
                </Menu>
              </div>
            }
          />

          {picked.size === 0 ? null : (
            <BulkBar count={picked.size} onClear={() => setPicked(new Set())}>
              <BulkButton
                disabled={busy}
                onClick={() => {
                  void runOnPicked("Re-queued", (id) => redownloadJob({ id }));
                }}
              >
                Re-download
              </BulkButton>
              <BulkButton
                disabled={busy}
                onClick={() => {
                  void runOnPicked("Re-ran scripts for", (id) => rerunPostProcessing({ jobId: id }));
                }}
              >
                Re-run scripts
              </BulkButton>
              <BulkButton tone="danger" disabled={busy} onClick={() => setConfirmDelete(true)}>
                Delete from history
              </BulkButton>
            </BulkBar>
          )}

          <GridHeader
            columns={COLUMNS}
            cells={[
              <CheckBox
                key="all"
                label="Select every entry on this page"
                checked={allPicked}
                onChange={toggleAll}
              />,
              "Release",
              "Outcome",
              <span key="size" className="block text-right">
                Size
              </span>,
              <span key="done" className="block text-right">
                Done
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
        report
          ?? `${formatCount(totalCount)} of ${formatCount(counts.all)} entries match · history is kept until an entry is deleted`
      }
      statusRight={
        library?.capacity
          ? `${formatSize(library.capacity.freeBytes)} free on ${library.path}`
          : undefined
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {rows.length === 0 ? (
          <EmptyState
            title={fetching ? "Loading" : "Nothing matches this view"}
            body={
              fetching
                ? "Fetching the history page."
                : "Clear the filter or pick another outcome."
            }
          />
        ) : (
          days.map((day) => (
            <section key={day.key} className="flex flex-none flex-col">
              <SectionHeader
                label={day.label}
                count={`${day.rows.length} ${day.rows.length === 1 ? "item" : "items"}`}
                note={`${formatSize(day.bytes)} archived`}
              />
              {day.rows.map((row) => {
                const token = statusToken(row.status);
                const failed = token === "failed";
                const removing = row.deleteOperation !== null;
                const finished = millis(row.completedAt);
                const elapsed = elapsedMs(row);
                return (
                  <GridRow
                    key={row.id}
                    columns={COLUMNS}
                    selected={picked.has(row.id)}
                    onClick={() => navigate(`/jobs/${row.id}`)}
                    title={row.error ? `${row.name}\n${row.error}` : row.name}
                    className="border-b border-wv-hairline px-4 sm:px-[22px] py-[11px]"
                  >
                    <CheckBox
                      label={`Select ${row.displayTitle || row.name}`}
                      checked={picked.has(row.id)}
                      onChange={() => togglePicked(row.id)}
                    />
                    <div className="min-w-0 truncate font-wv-mono text-[12.5px] text-wv-fg">
                      {row.name}
                    </div>
                    <div className="flex min-w-0 items-center gap-[7px]">
                      <Square
                        color={removing ? WV.idle : failed ? WV.error : WV.accent}
                      />
                      <span className="truncate text-[12.5px] text-wv-secondary">
                        {removing
                          ? "Removing"
                          : token === "completed"
                            ? "Complete"
                            : failed
                              ? "Failed"
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
                        {elapsed === null ? EM_DASH : `took ${formatElapsed(elapsed)}`}
                      </span>
                    </div>
                    <div aria-hidden="true" className="text-right text-[13px] text-wv-dim">
                      &rsaquo;
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
        title="Delete from history"
        note={`${picked.size} selected`}
        busy={busy}
        confirmLabel="Delete from history"
        body="The entries leave history and their files stay on disk. Anything still being removed finishes in the background."
        onConfirm={() => void deletePicked()}
        onDismiss={() => setConfirmDelete(false)}
      />
    </NextShell>
  );
}

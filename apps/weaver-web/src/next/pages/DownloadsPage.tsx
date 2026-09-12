import { useCallback, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  PAUSE_ALL_MUTATION,
  RESUME_ALL_MUTATION,
  SYSTEM_INFO_QUERY,
} from "@/graphql/queries";
import { formatJobReleaseName, type JobData } from "@/lib/job-types";
import {
  formatEtaFromRemainingBytes,
  useStableEtaSpeed,
  useStableQueueEta,
} from "@/lib/hooks/use-stable-queue-eta";
import { statusToken } from "@/lib/status-tokens";
import { Bar, EmptyState, MetricCell, SectionHeader, Square } from "../components/chrome";
import { PrimaryButton, SecondaryButton, TextField } from "../components/controls";
import { Menu, MenuItem } from "../components/Menu";
import { StorageMounts, type StorageVolume } from "../components/storage";
import { ListRow, ValueCell } from "../components/rows";
import { Tabs } from "../components/Tabs";
import { useNextData } from "../data/next-data";
import { EM_DASH, formatSize } from "../data/format";
import { categoryColor, statusColor, UNCATEGORISED_COLOR } from "../data/palette";
import {
  DOWNLOAD_GROUP_LABEL,
  DOWNLOAD_GROUP_NOTE,
  DOWNLOAD_GROUP_ORDER,
  currentPhase,
  downloadGroup,
  useStatusLabel,
  type DownloadGroup,
} from "../data/status";
import { NextShell } from "../shell/NextShell";
import {
  CategoryListBlock,
  ProvidersBlock,
  ThroughputBlock,
  type CategoryEntry,
} from "../shell/rail-blocks";
import { AddNzbDialog } from "../features/AddNzbDialog";
import { DownloadInspector } from "./downloads/DownloadInspector";

type TabId = "all" | "active" | "queued" | "paused";
type SortId = "priority" | "name" | "size" | "progress" | "eta";

const SORT_OPTIONS: { value: SortId; label: string }[] = [
  { value: "priority", label: "Priority" },
  { value: "name", label: "Name" },
  { value: "size", label: "Size" },
  { value: "progress", label: "Progress" },
  { value: "eta", label: "Time left" },
];

const UNCATEGORISED = "uncategorised";

/** Group → the tab that shows it. "All" shows everything. */
const TAB_FOR_GROUP: Record<DownloadGroup, TabId> = {
  active: "active",
  paused: "paused",
  queued: "queued",
  attention: "all",
};

function sortJobs(jobs: JobData[], sort: SortId, etaById: Map<number, string>): JobData[] {
  if (sort === "priority") {
    // Server order already is priority order; leave it alone.
    return jobs;
  }
  const sorted = [...jobs];
  switch (sort) {
    case "name":
      sorted.sort((left, right) =>
        formatJobReleaseName(left).localeCompare(formatJobReleaseName(right)),
      );
      break;
    case "size":
      sorted.sort((left, right) => right.totalBytes - left.totalBytes);
      break;
    case "progress":
      sorted.sort((left, right) => right.progress - left.progress);
      break;
    case "eta":
      // Rows without an estimate sort last rather than first.
      sorted.sort((left, right) => {
        const leftHas = etaById.has(left.id) ? 0 : 1;
        const rightHas = etaById.has(right.id) ? 0 : 1;
        if (leftHas !== rightHas) return leftHas - rightHas;
        const leftRemaining = Math.max(left.totalBytes - left.downloadedBytes, 0);
        const rightRemaining = Math.max(right.totalBytes - right.downloadedBytes, 0);
        return leftRemaining - rightRemaining;
      });
      break;
  }
  return sorted;
}

export function DownloadsPage() {
  const { queue, speed, isPaused } = useNextData();
  const statusLabel = useStatusLabel();

  const [category, setCategory] = useState<string | null>(null);
  const [tab, setTab] = useState<TabId>("all");
  const [sort, setSort] = useState<SortId>("priority");
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [sortOpen, setSortOpen] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);
  // Cancelling is not instant; hide the row until the refetch confirms it.
  const [removedIds, setRemovedIds] = useState<ReadonlySet<number>>(() => new Set());

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [{ data: systemInfo }] = useQuery<{
    systemInfo: { configuredStorage: StorageVolume[] };
  }>({ query: SYSTEM_INFO_QUERY });

  const jobs = useMemo(
    () => queue.jobs.filter((job) => !removedIds.has(job.id)),
    [queue.jobs, removedIds],
  );

  const etaSpeed = useStableEtaSpeed(jobs, speed);
  const etaById = useStableQueueEta(jobs, speed);

  // Search narrows everything, so the category counts answer "how many rows
  // would I see if I clicked this" rather than "how many exist somewhere".
  const searched = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (needle === "") {
      return jobs;
    }
    return jobs.filter(
      (job) =>
        job.name.toLowerCase().includes(needle)
        || job.displayTitle.toLowerCase().includes(needle)
        || formatJobReleaseName(job).toLowerCase().includes(needle),
    );
  }, [jobs, query]);

  const categories = useMemo<CategoryEntry[]>(() => {
    const counts = new Map<string, number>();
    for (const job of searched) {
      const key = job.category || UNCATEGORISED;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    const names = new Set<string>([...queue.categories, ...counts.keys()]);
    const entries: CategoryEntry[] = [
      { key: null, label: "All categories", color: UNCATEGORISED_COLOR, count: searched.length },
    ];
    for (const name of [...names].sort((left, right) => left.localeCompare(right))) {
      entries.push({
        key: name,
        label: name,
        color: name === UNCATEGORISED ? UNCATEGORISED_COLOR : categoryColor(name),
        count: counts.get(name) ?? 0,
      });
    }
    return entries;
  }, [queue.categories, searched]);

  const inCategory = useMemo(() => {
    if (category === null) {
      return searched;
    }
    return searched.filter((job) => (job.category || UNCATEGORISED) === category);
  }, [category, searched]);

  const grouped = useMemo(() => {
    const buckets = new Map<DownloadGroup, JobData[]>();
    for (const group of DOWNLOAD_GROUP_ORDER) {
      buckets.set(group, []);
    }
    for (const job of inCategory) {
      buckets.get(downloadGroup(job.status))!.push(job);
    }
    return buckets;
  }, [inCategory]);

  const tabCounts = useMemo(
    () => ({
      all: inCategory.length,
      active: grouped.get("active")!.length,
      queued: grouped.get("queued")!.length,
      paused: grouped.get("paused")!.length,
    }),
    [grouped, inCategory.length],
  );

  const visibleGroups = DOWNLOAD_GROUP_ORDER.filter((group) => {
    const rows = grouped.get(group)!;
    if (rows.length === 0) {
      return false;
    }
    return tab === "all" || TAB_FOR_GROUP[group] === tab;
  });

  const selected = jobs.find((job) => job.id === selectedId) ?? null;

  const remainingBytes = useMemo(
    () =>
      jobs.reduce((total, job) => {
        const token = statusToken(job.status);
        if (token !== "downloading" && token !== "queued") {
          return total;
        }
        return total + Math.max(job.totalBytes - job.downloadedBytes, 0);
      }, 0),
    [jobs],
  );

  const volumes = systemInfo?.systemInfo?.configuredStorage ?? [];

  const handleRemoved = useCallback((id: number) => {
    setRemovedIds((current) => new Set(current).add(id));
    setSelectedId((current) => (current === id ? null : current));
  }, []);

  const trailingValue = useCallback(
    (job: JobData): string => {
      const token = statusToken(job.status);
      if (token === "paused") return "paused";
      const eta = etaById.get(job.id);
      if (eta) return eta;
      const phase = currentPhase(job);
      if (token !== "queued" && phase && phase.totalBytes > 0) {
        return `${Math.round(phase.progressPercent)}%`;
      }
      return EM_DASH;
    },
    [etaById],
  );

  return (
    <NextShell
      title="Downloads"
      controls={
        <>
          <TextField
            label="Search downloads"
            placeholder="Search downloads"
            mono={false}
            value={query}
            onChange={setQuery}
            className="w-[132px] sm:w-[224px]"
          />
          <SecondaryButton
            onClick={() => {
              void (isPaused ? resumeAll({}) : pauseAll({}));
            }}
          >
            {isPaused ? "Resume all" : "Pause all"}
          </SecondaryButton>
          <PrimaryButton onClick={() => setUploadOpen(true)}>Add NZB</PrimaryButton>
        </>
      }
      railMiddle={
        <CategoryListBlock
          items={categories}
          active={category}
          onSelect={(next) => setCategory(next)}
        />
      }
      railFooter={
        <>
          <ThroughputBlock />
          <ProvidersBlock />
        </>
      }
      beforeContent={
        <>
          <div className="flex flex-none border-b border-wv-hairline bg-wv-app">
            <MetricCell
              eyebrow="Complete in"
              value={
                remainingBytes > 0
                  ? formatEtaFromRemainingBytes(remainingBytes, etaSpeed)
                  : EM_DASH
              }
              note={
                remainingBytes > 0 ? `${formatSize(remainingBytes)} left to fetch` : "queue is clear"
              }
            />
            <StorageMounts
              volumes={volumes}
              className="flex-1 border-t border-wv-hairline px-4 py-4 sm:px-6 lg:border-t-0"
            />
          </div>

          <Tabs
            tabs={[
              { id: "all", label: "All", count: tabCounts.all },
              { id: "active", label: "Active", count: tabCounts.active },
              { id: "queued", label: "Queued", count: tabCounts.queued },
              { id: "paused", label: "Paused", count: tabCounts.paused },
            ]}
            active={tab}
            onSelect={setTab}
            right={
              <div className="relative">
                <button
                  type="button"
                  data-wv-menu-trigger=""
                  aria-haspopup="menu"
                  aria-expanded={sortOpen}
                  onClick={() => setSortOpen((previous) => !previous)}
                  className="flex items-center gap-[6px] font-wv-mono text-[11.5px] text-wv-muted hover:text-wv-fg"
                >
                  Sort by
                  <span className="font-semibold text-wv-fg">
                    {SORT_OPTIONS.find((option) => option.value === sort)!.label}
                  </span>
                  <span aria-hidden="true" className="text-[8px]">
                    &#9660;
                  </span>
                </button>
                <Menu
                  open={sortOpen}
                  onDismiss={() => setSortOpen(false)}
                  label="Sort downloads"
                  className="top-[26px] right-0 w-[164px]"
                >
                  {SORT_OPTIONS.map((option) => (
                    <MenuItem
                      key={option.value}
                      selected={option.value === sort}
                      onSelect={() => {
                        setSort(option.value);
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
        </>
      }
      statusRight={
        queue.totalCount > queue.jobs.length
          ? `showing ${queue.jobs.length} of ${queue.totalCount} downloads`
          : `${inCategory.length} of ${jobs.length} downloads shown`
      }
      // The list and the inspector are side by side only where both fit; below
      // that the inspector becomes a panel under the list rather than squeezing
      // the release names it exists to explain.
      contentClassName="flex-col xl:flex-row"
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {visibleGroups.length === 0 ? (
          <EmptyState
            title="Nothing matches this view"
            body="Clear the search or pick another filter."
          />
        ) : (
          visibleGroups.map((group) => {
            const rows = sortJobs(grouped.get(group)!, sort, etaById);
            return (
              <section key={group} className="flex flex-none flex-col">
                <SectionHeader
                  label={DOWNLOAD_GROUP_LABEL[group]}
                  count={rows.length}
                  note={DOWNLOAD_GROUP_NOTE[group] || undefined}
                />
                {rows.map((job) => {
                  const color = statusColor(job.status);
                  const percent = Math.round(job.progress * 100);
                  return (
                    <ListRow
                      key={job.id}
                      markSelection
                      selected={job.id === selectedId}
                      onClick={() => setSelectedId(job.id)}
                      title={formatJobReleaseName(job)}
                      left={
                        <div className="flex min-w-0 flex-[1_1_240px] flex-col gap-[9px]">
                          <span className="truncate text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg">
                            {formatJobReleaseName(job)}
                          </span>
                          <div className="flex items-center gap-3">
                            <Bar
                              percent={percent}
                              color={color}
                              height={10}
                              className="max-w-[196px] min-w-[56px] flex-1"
                            />
                            <span className="w-[34px] flex-none font-wv-mono text-[11.5px] text-wv-muted">
                              {percent}%
                            </span>
                          </div>
                        </div>
                      }
                      right={
                        <>
                          <div className="flex min-w-[96px] items-center gap-[9px]">
                            <Square color={color} />
                            <span className="truncate text-[12.5px] text-wv-secondary">
                              {statusLabel(job.status)}
                            </span>
                          </div>
                          <ValueCell>{formatSize(job.totalBytes)}</ValueCell>
                          <ValueCell>{trailingValue(job)}</ValueCell>
                        </>
                      }
                    />
                  );
                })}
              </section>
            );
          })
        )}
      </div>

      {selected === null ? null : (
        <DownloadInspector
          job={selected}
          eta={trailingValue(selected)}
          rate={currentPhase(selected)?.rateBps ?? 0}
          onRemoved={handleRemoved}
        />
      )}

      <AddNzbDialog open={uploadOpen} onClose={() => setUploadOpen(false)} />
    </NextShell>
  );
}

import { useCallback, useMemo, useRef, useState, type DragEvent } from "react";
import { useClient, useMutation, useQuery } from "urql";
import { executeAliasedIdMutation } from "@/graphql/aliased-mutations";
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
import { useTranslate } from "@/lib/context/translate-context";
import { statusToken } from "@/lib/status-tokens";
import { BulkBar, BulkButton, BulkCluster, BulkMenu } from "../components/BulkBar";
import { EmptyState, MetricCell, SectionHeader } from "../components/chrome";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { CheckBox, PrimaryButton, SecondaryButton, TextField } from "../components/controls";
import { Icon } from "../components/icons";
import { Menu, MenuItem } from "../components/Menu";
import { StorageMounts, type StorageVolume } from "../components/storage";
import { Tabs } from "../components/Tabs";
import { useNextData } from "../data/next-data";
import { EM_DASH, formatDayClock, formatSize } from "../data/format";
import { ratePhase } from "../data/phase-bars";
import {
  categoryFacets,
  facetKey,
  NO_FACETS,
  toggleFacet,
  type CategoryEntry,
} from "../data/categories";
import {
  DOWNLOAD_GROUP_LABEL,
  DOWNLOAD_GROUP_NOTE,
  DOWNLOAD_GROUP_ORDER,
  downloadGroup,
  useStatusLabel,
  type DownloadGroup,
} from "../data/status";
import { countLabel } from "../i18n/labels";
import { NextShell } from "../shell/NextShell";
import { CategoryListBlock, ProvidersBlock } from "../shell/rail-blocks";
import { AddNzbDialog } from "../features/AddNzbDialog";
import { SpeedLimitControl } from "../features/SpeedLimitDialog";
import { DownloadInspector } from "./downloads/DownloadInspector";
import { DownloadRow } from "./downloads/DownloadRow";

type TabId = "all" | "active" | "queued" | "paused";
type SortId = "priority" | "name" | "size" | "progress" | "eta";

/** Each sort and the key of its label. */
const SORT_OPTIONS: { value: SortId; label: string }[] = [
  { value: "priority", label: "table.priority" },
  { value: "name", label: "table.name" },
  { value: "size", label: "table.size" },
  { value: "progress", label: "table.progress" },
  { value: "eta", label: "next.common.timeLeft" },
];

/** The bulk edits a report names, each with its `.one`/`.other` and `Partial` keys. */
type BulkReport = "paused" | "resumed" | "priority" | "category" | "cancelled";

/** The statuses a global pause or a download block holds back. */
const HELD_BACK_STATUSES = new Set(["DOWNLOADING", "QUEUED", "PROPAGATING"]);

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
    case "eta": {
      // A job's time left counts everything queued ahead of it, so estimates
      // grow in queue order and that order is the ranking; a small job behind a
      // large one finishes after it. Rows without an estimate sort last.
      const rank = new Map([...etaById.keys()].map((id, index) => [id, index]));
      sorted.sort(
        (left, right) =>
          (rank.get(left.id) ?? Number.MAX_SAFE_INTEGER) -
          (rank.get(right.id) ?? Number.MAX_SAFE_INTEGER),
      );
      break;
    }
  }
  return sorted;
}

export function DownloadsPage() {
  const { queue, categories: configured, speed, isPaused, downloadBlock } = useNextData();
  const statusLabel = useStatusLabel();
  const t = useTranslate();

  const [facets, setFacets] = useState<ReadonlySet<string>>(NO_FACETS);
  const [tab, setTab] = useState<TabId>("all");
  const [sort, setSort] = useState<SortId>("priority");
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [sortOpen, setSortOpen] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);
  // NZBs dropped on the list, handed to the dialog so it opens with them staged.
  const [droppedFiles, setDroppedFiles] = useState<readonly File[] | null>(null);
  const [dropping, setDropping] = useState(false);
  // dragenter and dragleave fire for every child the pointer crosses, so the
  // highlight tracks how deep inside the pane the drag is, not the last event.
  const dragDepth = useRef(0);
  // Cancelling is not instant; hide the row until the refetch confirms it.
  const [removedIds, setRemovedIds] = useState<ReadonlySet<number>>(() => new Set());
  // Ticked for a bulk action; separate from the one row the inspector shows.
  const [picked, setPicked] = useState<ReadonlySet<number>>(() => new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(false);
  const [report, setReport] = useState<string | null>(null);

  const client = useClient();

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

  const categories = useMemo<CategoryEntry[]>(
    () => categoryFacets({ configured, rows: searched, extras: queue.categories }),
    [configured, queue.categories, searched],
  );

  // Facets union: two of them asks for both, which is the only reading that
  // lets a second click widen the view rather than empty it.
  const inCategory = useMemo(() => {
    if (facets.size === 0) {
      return searched;
    }
    return searched.filter((job) => facets.has(facetKey(job)));
  }, [facets, searched]);

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

  // A ticked download that finishes or is removed leaves the queue, and with
  // it the selection.
  if ([...picked].some((id) => !jobs.some((job) => job.id === id))) {
    setPicked(new Set([...picked].filter((id) => jobs.some((job) => job.id === id))));
  }

  const togglePicked = useCallback((id: number) => {
    setPicked((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  /** Tick or untick a whole section, both ways, like select-all on a page. */
  const toggleGroup = (rows: readonly JobData[]) => {
    const allPicked = rows.every((job) => picked.has(job.id));
    setPicked((current) => {
      const next = new Set(current);
      for (const job of rows) {
        if (allPicked) {
          next.delete(job.id);
        } else {
          next.add(job.id);
        }
      }
      return next;
    });
  };

  /**
   * One aliased mutation for the whole selection, so a hundred ticked rows are
   * one request; each alias answers for its own id.
   */
  const bulkReport = (kind: BulkReport, total: number, refused: number) =>
    refused === 0
      ? countLabel(t, `next.downloads.bulk.${kind}`, total)
      : t(`next.downloads.bulk.${kind}Partial`, { done: total - refused, total, refused });

  const runOnPicked = async (
    kind: BulkReport,
    definition: Omit<Parameters<typeof executeAliasedIdMutation>[0], "client" | "ids">,
  ) => {
    const ids = [...picked];
    setBulkBusy(true);
    const result = await executeAliasedIdMutation<boolean>({ client, ids, ...definition });
    setBulkBusy(false);
    if (result.error) {
      setReport(t("next.downloads.bulk.failed", { message: result.error.message }));
      return;
    }
    const refused = ids.filter((_id, index) => result.data?.[`${definition.aliasPrefix}${index}`] !== true);
    setPicked(new Set());
    setReport(bulkReport(kind, ids.length, refused.length));
    queue.refresh();
  };

  const pausePicked = () =>
    runOnPicked("paused", { operationName: "PauseSelectedJobs", aliasPrefix: "pauseJob", fieldName: "pauseJob" });

  const resumePicked = () =>
    runOnPicked("resumed", { operationName: "ResumeSelectedJobs", aliasPrefix: "resumeJob", fieldName: "resumeJob" });

  const editPicked = (kind: BulkReport, category: string | null, priority: string | null) =>
    runOnPicked(kind, {
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

  // Rows leave the list the moment the cancel is sent, and any the daemon
  // refuses come back once it answers.
  const cancelPicked = async () => {
    const ids = [...picked];
    setBulkBusy(true);
    setRemovedIds((current) => new Set([...current, ...ids]));
    setSelectedId((current) => (current !== null && ids.includes(current) ? null : current));
    const result = await executeAliasedIdMutation<boolean>({
      client,
      ids,
      operationName: "CancelSelectedJobs",
      aliasPrefix: "cancelJob",
      fieldName: "cancelJob",
    });
    const cancelled = ids.filter((_id, index) => result.data?.[`cancelJob${index}`] === true);
    const failed = ids.filter((id) => !cancelled.includes(id));
    if (failed.length > 0) {
      setRemovedIds((current) => new Set([...current].filter((id) => !failed.includes(id))));
    }
    setBulkBusy(false);
    setConfirmCancel(false);
    setPicked(new Set());
    setReport(bulkReport("cancelled", ids.length, failed.length));
    queue.refresh();
  };

  const priorityOptions = [
    { value: "HIGH", label: t("upload.priorityHigh") },
    { value: "NORMAL", label: t("upload.priorityNormal") },
    { value: "LOW", label: t("upload.priorityLow") },
  ];

  const categoryOptions = [
    { value: "", label: t("next.categories.uncategorised") },
    ...configured.map((category) => ({ value: category.name, label: category.name })),
  ];

  // One set of actions, shown in the tab row where it fits and in its own bar where it does not.
  const bulkActions = (
    <>
      <BulkButton icon="pause" disabled={bulkBusy} onClick={() => void pausePicked()}>
        {t("action.pause")}
      </BulkButton>
      <BulkButton icon="resume" disabled={bulkBusy} onClick={() => void resumePicked()}>
        {t("action.resume")}
      </BulkButton>
      <BulkMenu
        icon="priority"
        label={t("next.downloads.setPriority")}
        disabled={bulkBusy}
        options={priorityOptions}
        onSelect={(value) => void editPicked("priority", null, value)}
      >
        {t("table.priority")}
      </BulkMenu>
      <BulkMenu
        icon="categories"
        label={t("next.downloads.setCategory")}
        disabled={bulkBusy}
        options={categoryOptions}
        onSelect={(value) => void editPicked("category", value, null)}
      >
        {t("table.category")}
      </BulkMenu>
      <BulkButton icon="cancelDownload" tone="danger" disabled={bulkBusy} onClick={() => setConfirmCancel(true)}>
        {t("action.cancel")}
      </BulkButton>
    </>
  );

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

  // A cap or a provider quota stops every download that would otherwise be
  // fetching, whatever its own status says; so does pausing everything.
  const blocked = downloadBlock.kind === "ISP_CAP" || downloadBlock.kind === "SERVER_QUOTA";
  const blockEta =
    downloadBlock.kind === "SERVER_QUOTA"
      ? t("jobs.serverQuotaEta")
      : t("jobs.bandwidthCapEta", { resetAt: formatDayClock(downloadBlock.windowEndsAtEpochMs) });
  const blockLabel =
    downloadBlock.kind === "SERVER_QUOTA" ? t("jobs.serverQuotaBadge") : t("jobs.bandwidthCapShort");

  /** What stands in for time left: a hold, or an estimate; null when there is neither. */
  const waitValue = useCallback(
    (job: JobData): string | null => {
      if (statusToken(job.status) === "paused") return t("next.downloads.waitPaused");
      if (HELD_BACK_STATUSES.has(job.status)) {
        if (blocked) return blockEta;
        if (isPaused) return t("next.downloads.waitPaused");
      }
      return etaById.get(job.id) ?? null;
    },
    [blockEta, blocked, etaById, isPaused, t],
  );

  return (
    <NextShell
      title={t("next.nav.downloads")}
      controls={
        <>
          <TextField
            label={t("next.downloads.search")}
            placeholder={t("next.downloads.search")}
            mono={false}
            value={query}
            onChange={setQuery}
            className="w-[132px] sm:w-[224px]"
          />
          <SecondaryButton
            icon={isPaused ? "resume" : "pause"}
            onClick={() => {
              void (isPaused ? resumeAll({}) : pauseAll({}));
            }}
          >
            {isPaused ? t("next.downloads.resumeAll") : t("next.downloads.pauseAll")}
          </SecondaryButton>
          <SpeedLimitControl />
          <PrimaryButton icon="add" onClick={() => setUploadOpen(true)}>{t("next.addNzb.title")}</PrimaryButton>
        </>
      }
      railMiddle={
        <CategoryListBlock
          items={categories}
          selected={facets}
          onToggle={(key) => setFacets((current) => toggleFacet(current, key))}
          onClear={() => setFacets(NO_FACETS)}
        />
      }
      railFooter={<ProvidersBlock />}
      beforeContent={
        <>
          <div className="flex flex-none border-b border-wv-hairline bg-wv-app">
            <MetricCell
              eyebrow={t("next.downloads.completeIn")}
              value={
                remainingBytes > 0
                  ? formatEtaFromRemainingBytes(remainingBytes, etaSpeed)
                  : EM_DASH
              }
              note={
                remainingBytes > 0
                  ? t("next.downloads.leftToFetch", { size: formatSize(remainingBytes) })
                  : queue.isLoading
                    ? t("next.downloads.fetchingQueue")
                    : t("next.downloads.queueClear")
              }
            />
            <StorageMounts
              volumes={volumes}
              className="flex-1 border-t border-wv-hairline px-4 py-4 sm:px-6 lg:border-t-0"
            />
          </div>

          <Tabs
            tabs={[
              { id: "all", label: t("history.filterAll"), count: tabCounts.all },
              { id: "active", label: t("next.downloads.tabActive"), count: tabCounts.active },
              { id: "queued", label: t("status.queued"), count: tabCounts.queued },
              { id: "paused", label: t("status.paused"), count: tabCounts.paused },
            ]}
            active={tab}
            onSelect={setTab}
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
                  className="flex items-center gap-[6px] font-wv-mono text-[11.5px] text-wv-muted hover:text-wv-fg"
                >
                  {t("next.downloads.sortBy")}
                  <span className="font-semibold text-wv-fg">
                    {t(SORT_OPTIONS.find((option) => option.value === sort)!.label)}
                  </span>
                  <Icon name="dropdown" size={12} />
                </button>
                <Menu
                  open={sortOpen}
                  onDismiss={() => setSortOpen(false)}
                  label={t("next.downloads.sortMenu")}
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
                      {t(option.label)}
                    </MenuItem>
                  ))}
                </Menu>
              </div>
            }
          />

          {/* From xl the actions sit in the tab row instead; below it there is no room. */}
          {picked.size === 0 ? null : (
            <BulkBar count={picked.size} onClear={() => setPicked(new Set())} className="xl:hidden">
              {bulkActions}
            </BulkBar>
          )}
        </>
      }
      statusNote={report ?? undefined}
      statusRight={
        queue.totalCount > queue.jobs.length
          ? t("next.downloads.showingPage", { shown: queue.jobs.length, total: queue.totalCount })
          : t("next.downloads.shown", { shown: inCategory.length, total: jobs.length })
      }
      // The list and the inspector are side by side only where both fit; below
      // that the inspector becomes a panel under the list rather than squeezing
      // the release names it exists to explain.
      contentClassName="flex-col xl:flex-row"
    >
      <div
        className="relative flex min-h-0 flex-1 flex-col"
        onDragEnter={(event) => {
          if (!carriesFiles(event)) return;
          event.preventDefault();
          dragDepth.current += 1;
          setDropping(true);
        }}
        onDragOver={(event) => {
          if (!carriesFiles(event)) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = "copy";
        }}
        onDragLeave={(event) => {
          if (!carriesFiles(event)) return;
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (dragDepth.current === 0) setDropping(false);
        }}
        onDrop={(event) => {
          if (!carriesFiles(event)) return;
          event.preventDefault();
          dragDepth.current = 0;
          setDropping(false);
          const files = Array.from(event.dataTransfer.files);
          if (files.length === 0) return;
          setDroppedFiles(files);
          setUploadOpen(true);
        }}
      >
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
          {queue.isLoading ? (
            <EmptyState loading title={t("next.common.loading")} body={t("next.downloads.loadingBody")} />
          ) : jobs.length === 0 ? (
            <EmptyState
              centered
              title={t("next.downloads.emptyTitle")}
              body={t("next.downloads.emptyBody")}
              action={<PrimaryButton icon="add" onClick={() => setUploadOpen(true)}>{t("next.addNzb.title")}</PrimaryButton>}
            />
          ) : visibleGroups.length === 0 ? (
            <EmptyState
              title={t("next.downloads.noMatchTitle")}
              body={t("next.downloads.noMatchBody")}
            />
          ) : (
            visibleGroups.map((group) => {
              const rows = sortJobs(grouped.get(group)!, sort, etaById);
              return (
                <section key={group} className="flex flex-none flex-col">
                  <SectionHeader
                    lead={
                      <CheckBox
                        label={t("next.downloads.selectGroup", { group: t(DOWNLOAD_GROUP_LABEL[group]) })}
                        checked={rows.every((job) => picked.has(job.id))}
                        onChange={() => toggleGroup(rows)}
                      />
                    }
                    label={t(DOWNLOAD_GROUP_LABEL[group])}
                    count={rows.length}
                    note={DOWNLOAD_GROUP_NOTE[group] === null ? undefined : t(DOWNLOAD_GROUP_NOTE[group])}
                  />
                  {rows.map((job) => (
                    <DownloadRow
                      key={job.id}
                      job={job}
                      selected={job.id === selectedId}
                      onSelect={setSelectedId}
                      picked={picked.has(job.id)}
                      onPick={togglePicked}
                      statusLabel={statusLabel}
                      wait={waitValue(job)}
                      hold={blocked && HELD_BACK_STATUSES.has(job.status) ? blockLabel : null}
                      statusTitle={
                        job.status === "PROPAGATING" && job.downloadRetryAtEpochMs != null
                          ? t("next.status.propagationUntil", {
                              time: new Date(job.downloadRetryAtEpochMs).toLocaleString(),
                            })
                          : undefined
                      }
                    />
                  ))}
                </section>
              );
            })
          )}
        </div>
        {dropping ? (
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center border border-dashed border-wv-accent bg-wv-selected">
            <span className="text-[13px] font-medium text-wv-strong">{t("next.downloads.dropHint")}</span>
          </div>
        ) : null}
      </div>

      {selected === null ? null : (
        <DownloadInspector
          key={selected.id}
          job={selected}
          eta={waitValue(selected) ?? EM_DASH}
          rate={ratePhase(selected.phaseProgress)?.rateBps ?? 0}
          onRemoved={handleRemoved}
        />
      )}

      <ConfirmDialog
        open={confirmCancel}
        title={t("next.downloads.cancelTitle")}
        note={t("bulk.selected", { count: picked.size })}
        busy={bulkBusy}
        destructive
        body={t("confirm.cancelSelectedMessage")}
        confirmLabel={t("next.downloads.cancelTitle")}
        dismissLabel={t("next.downloads.keepDownloading")}
        onConfirm={() => void cancelPicked()}
        onDismiss={() => setConfirmCancel(false)}
      />

      <AddNzbDialog
        open={uploadOpen}
        initialFiles={droppedFiles}
        onClose={() => {
          setUploadOpen(false);
          setDroppedFiles(null);
        }}
      />
    </NextShell>
  );
}

/** Whether a drag is carrying files from the desktop, not text or a link from the page. */
function carriesFiles(event: DragEvent): boolean {
  return Array.from(event.dataTransfer.types).includes("Files");
}

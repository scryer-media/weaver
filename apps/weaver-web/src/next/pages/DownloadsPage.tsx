// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, useCallback, useLayoutEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { gql, useClient, useMutation, useQuery } from "urql";
import { ALIASED_GROUP_SIZE, executeAliasedIdMutation } from "@/graphql/aliased-mutations";
import {
  CANCEL_JOB_MUTATION,
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
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
import { sameElements, sameEntries, sameItems, shallowEqual, useSettled } from "../data/settled";
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
import { AttentionBlock, CategoryListBlock, ProvidersBlock } from "../shell/rail-blocks";
import { AddNzbDialog } from "../features/AddNzbDialog";
import { SpeedLimitControl } from "../features/SpeedLimitDialog";
import { DownloadInspector } from "./downloads/DownloadInspector";
import { DownloadRow } from "./downloads/DownloadRow";
import { QueueEtaContext } from "./downloads/queue-eta";

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
const NO_ETAS: ReadonlyMap<number, string> = new Map();

/** Group → the tab that shows it. "All" shows everything. */
const TAB_FOR_GROUP: Record<DownloadGroup, TabId> = {
  active: "active",
  paused: "paused",
  queued: "queued",
  attention: "all",
};

function sortJobs(
  jobs: readonly JobData[],
  sort: SortId,
  etaById: ReadonlyMap<number, string> | null,
): readonly JobData[] {
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
      const rank = new Map([...(etaById ?? NO_ETAS).keys()].map((id, index) => [id, index]));
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

const CANCEL_JOBS_MUTATION = gql`
  mutation CancelSelectedJobs($ids: [Int!]!) {
    cancelJobs(ids: $ids) {
      id
      cancelled
    }
  }
`;

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
  // The one row whose cancel button is waiting on confirmation.
  const [confirmRowCancel, setConfirmRowCancel] = useState<number | null>(null);
  const [report, setReport] = useState<string | null>(null);

  const client = useClient();

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation<{ cancelJob: boolean }>(CANCEL_JOB_MUTATION);
  const [{ data: systemInfo }] = useQuery<{
    systemInfo: { configuredStorage: StorageVolume[] };
  }>({ query: SYSTEM_INFO_QUERY });

  const jobs = useMemo(
    () => queue.jobs.filter((job) => !removedIds.has(job.id)),
    [queue.jobs, removedIds],
  );

  const etaSpeed = useStableEtaSpeed(jobs, speed);
  const estimates = useStableQueueEta(jobs, speed);
  // Kept while every estimate reads the same, so the rows' time-left text is
  // drawn again only when some of it changes.
  const etaById = useSettled(estimates, sameEntries);

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

  const categories = useSettled(
    useMemo<CategoryEntry[]>(
      () => categoryFacets({ configured, rows: searched, extras: queue.categories }),
      [configured, queue.categories, searched],
    ),
    sameItems,
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

  const tabCounts = useSettled(
    useMemo(
      () => ({
        all: inCategory.length,
        active: grouped.get("active")!.length,
        queued: grouped.get("queued")!.length,
        paused: grouped.get("paused")!.length,
      }),
      [grouped, inCategory.length],
    ),
    shallowEqual,
  );
  // The section tick boxes read the groups and the ticks on screen through
  // these, so a box keeps its handler while the rows under it move.
  const groupedRef = useRef(grouped);
  const pickedRef = useRef(picked);
  useLayoutEffect(() => {
    groupedRef.current = grouped;
    pickedRef.current = picked;
  });

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
  const toggleGroup = useCallback((group: DownloadGroup) => {
    const rows = groupedRef.current.get(group) ?? [];
    const allPicked = rows.every((job) => pickedRef.current.has(job.id));
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
  }, []);

  const refreshQueue = queue.refresh;

  /**
   * One aliased mutation for the whole selection, so a hundred ticked rows are
   * one request; each alias answers for its own id.
   */
  const bulkReport = (kind: BulkReport, total: number, refused: number) =>
    refused === 0
      ? countLabel(t, `next.downloads.bulk.${kind}`, total)
      : t(`next.downloads.bulk.${kind}Partial`, { done: total - refused, total, refused });

  /**
   * Settle a bulk action once every group has answered: the ids it carried
   * out leave the selection, the rest stay ticked, and the report says how
   * many went through. A request error is named only when nothing did.
   */
  const settleBulk = (kind: BulkReport, ids: number[], done: Set<number>, error?: { message: string }) => {
    setPicked((current) => new Set([...current].filter((id) => !done.has(id))));
    setReport(
      error && done.size === 0
        ? t("next.downloads.bulk.failed", { message: error.message })
        : bulkReport(kind, ids.length, ids.length - done.size),
    );
    refreshQueue();
  };

  const runOnPicked = async (
    kind: BulkReport,
    definition: Omit<Parameters<typeof executeAliasedIdMutation>[0], "client" | "ids">,
  ) => {
    const ids = [...picked];
    setBulkBusy(true);
    const result = await executeAliasedIdMutation<boolean>({ client, ids, ...definition });
    setBulkBusy(false);
    const done = new Set(ids.filter((_id, index) => result.data[`${definition.aliasPrefix}${index}`] === true));
    settleBulk(kind, ids, done, result.error);
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
  // refuses come back once it answers. Each selection group is one cancel
  // the daemon carries out as a batch.
  const cancelPicked = async () => {
    const ids = [...picked];
    setBulkBusy(true);
    setRemovedIds((current) => new Set([...current, ...ids]));
    setSelectedId((current) => (current !== null && ids.includes(current) ? null : current));
    const cancelled = new Set<number>();
    let error: { message: string } | undefined;
    for (let start = 0; start < ids.length; start += ALIASED_GROUP_SIZE) {
      const result = await client
        .mutation<{ cancelJobs: { id: number; cancelled: boolean }[] }>(CANCEL_JOBS_MUTATION, {
          ids: ids.slice(start, start + ALIASED_GROUP_SIZE),
        })
        .toPromise();
      error ??= result.error;
      for (const row of result.data?.cancelJobs ?? []) {
        if (row.cancelled) cancelled.add(row.id);
      }
    }
    const failed = new Set(ids.filter((id) => !cancelled.has(id)));
    if (failed.size > 0) {
      setRemovedIds((current) => new Set([...current].filter((id) => !failed.has(id))));
    }
    setBulkBusy(false);
    setConfirmCancel(false);
    settleBulk("cancelled", ids, cancelled, error);
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

  const pauseRow = useCallback(
    (id: number, paused: boolean) => {
      void (paused ? pauseJob({ id }) : resumeJob({ id }));
    },
    [pauseJob, resumeJob],
  );

  // Like the bulk cancel: the row leaves the moment the cancel is sent and
  // comes back if the daemon refuses it.
  const cancelRow = async () => {
    const id = confirmRowCancel;
    setConfirmRowCancel(null);
    if (id === null) {
      return;
    }
    handleRemoved(id);
    const result = await cancelJob({ id });
    if (result.error || result.data?.cancelJob !== true) {
      setRemovedIds((current) => {
        const next = new Set(current);
        next.delete(id);
        return next;
      });
      setReport(result.error?.message ?? t("next.inspector.removeFailed"));
    }
    refreshQueue();
  };
  const rowCancelJob =
    confirmRowCancel === null ? null : (jobs.find((job) => job.id === confirmRowCancel) ?? null);

  // An egress or a provider quota stops every download that would otherwise
  // be fetching, whatever its own status says; so does pausing everything.
  const blocked = downloadBlock.kind === "EGRESS_QUOTA" || downloadBlock.kind === "SERVER_QUOTA";
  const blockEta =
    downloadBlock.kind === "SERVER_QUOTA"
      ? t("jobs.serverQuotaEta")
      : t("jobs.egressQuotaEta", { resetAt: formatDayClock(downloadBlock.windowEndsAtEpochMs) });
  const blockLabel =
    downloadBlock.kind === "SERVER_QUOTA" ? t("jobs.serverQuotaBadge") : t("jobs.egressQuotaBadge");

  /** What stands in for time left while a download is held; null when it is not. */
  const heldValue = useCallback(
    (job: JobData): string | null => {
      if (statusToken(job.status) === "paused") return t("next.downloads.waitPaused");
      if (HELD_BACK_STATUSES.has(job.status)) {
        if (blocked) return blockEta;
        if (isPaused) return t("next.downloads.waitPaused");
      }
      return null;
    },
    [blockEta, blocked, isPaused, t],
  );
  const holdValue = useCallback(
    (job: JobData): string | null => (blocked && HELD_BACK_STATUSES.has(job.status) ? blockLabel : null),
    [blockLabel, blocked],
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
        <>
          {/* A provider the daemon is holding back is the usual reason a queue
              looks stalled, so Downloads says so rather than leaving it to the
              diagnostic screens. The block draws nothing when all is well. */}
          <AttentionBlock />
          <CategoryListBlock
            items={categories}
            selected={facets}
            onToggle={(key) => setFacets((current) => toggleFacet(current, key))}
            onClear={() => setFacets(NO_FACETS)}
          />
        </>
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
            <QueueEtaContext value={etaById}>
              {visibleGroups.map((group) => (
                <QueueGroup
                  key={group}
                  group={group}
                  rows={grouped.get(group)!}
                  sort={sort}
                  etaById={sort === "eta" ? etaById : null}
                  selectedId={selectedId}
                  picked={picked}
                  onToggleGroup={toggleGroup}
                  onSelect={setSelectedId}
                  onPick={togglePicked}
                  statusLabel={statusLabel}
                  onPause={pauseRow}
                  onCancel={setConfirmRowCancel}
                  heldOf={heldValue}
                  holdOf={holdValue}
                />
              ))}
            </QueueEtaContext>
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
          eta={heldValue(selected) ?? etaById.get(selected.id) ?? EM_DASH}
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

      <ConfirmDialog
        open={confirmRowCancel !== null}
        title={t("next.job.cancelTitle")}
        note={rowCancelJob ? formatJobReleaseName(rowCancelJob) : undefined}
        destructive
        body={t("next.job.cancelBody")}
        confirmLabel={t("next.job.cancelTitle")}
        dismissLabel={t("next.downloads.keepDownloading")}
        onConfirm={() => void cancelRow()}
        onDismiss={() => setConfirmRowCancel(null)}
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


/** Whether every prop is the same, the rows compared one by one. */
function sameGroupProps(left: QueueGroupProps, right: QueueGroupProps): boolean {
  return (Object.keys(right) as (keyof QueueGroupProps)[]).every((key) =>
    key === "rows" ? sameElements(left.rows, right.rows) : Object.is(left[key], right[key]),
  );
}

interface QueueGroupProps {
  group: DownloadGroup;
  /** In queue order; sorted here. */
  rows: readonly JobData[];
  sort: SortId;
  /** Time left by id, in the order the sort ranks by, while that is the sort. */
  etaById: ReadonlyMap<number, string> | null;
  selectedId: number | null;
  picked: ReadonlySet<number>;
  onToggleGroup: (group: DownloadGroup) => void;
  onSelect: (id: number) => void;
  onPick: (id: number) => void;
  statusLabel: (status: string) => string;
  onPause: (id: number, paused: boolean) => void;
  onCancel: (id: number) => void;
  heldOf: (job: JobData) => string | null;
  holdOf: (job: JobData) => string | null;
}

/**
 * One section of the queue. A progress event replaces one job's object and
 * leaves the rest as they were, so a section whose rows are all the same
 * objects in the same order is not drawn again.
 */
const QueueGroup = memo(function QueueGroup({
  group,
  rows,
  sort,
  etaById,
  selectedId,
  picked,
  onToggleGroup,
  onSelect,
  onPick,
  statusLabel,
  onPause,
  onCancel,
  heldOf,
  holdOf,
}: QueueGroupProps) {
  const t = useTranslate();
  const sorted = sortJobs(rows, sort, etaById);
  return (
    <section className="flex flex-none flex-col">
      <SectionHeader
        lead={
          <CheckBox
            label={t("next.downloads.selectGroup", { group: t(DOWNLOAD_GROUP_LABEL[group]) })}
            checked={rows.every((job) => picked.has(job.id))}
            onChange={() => onToggleGroup(group)}
          />
        }
        label={t(DOWNLOAD_GROUP_LABEL[group])}
        count={rows.length}
        note={DOWNLOAD_GROUP_NOTE[group] === null ? undefined : t(DOWNLOAD_GROUP_NOTE[group])}
      />
      {sorted.map((job) => (
        <DownloadRow
          key={job.id}
          job={job}
          selected={job.id === selectedId}
          onSelect={onSelect}
          picked={picked.has(job.id)}
          onPick={onPick}
          statusLabel={statusLabel}
          onPause={onPause}
          onCancel={onCancel}
          held={heldOf(job)}
          hold={holdOf(job)}
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
}, sameGroupProps);

/** Whether a drag is carrying files from the desktop, not text or a link from the page. */
function carriesFiles(event: DragEvent): boolean {
  return Array.from(event.dataTransfer.types).includes("Files");
}

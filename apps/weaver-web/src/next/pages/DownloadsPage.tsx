// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { useClient, useMutation, useQuery, useSubscription, type TypedDocumentNode } from "urql";
import {
  CANCEL_JOB_MUTATION,
  CANCEL_QUEUE_ITEMS_MUTATION,
  HISTORY_FACADE_EVENTS_SUBSCRIPTION,
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  PAUSE_QUEUE_ITEMS_MUTATION,
  PAUSE_TORRENTS_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
  RESUME_QUEUE_ITEMS_MUTATION,
  RESUME_TORRENTS_MUTATION,
  SET_QUEUE_ITEMS_CATEGORY_MUTATION,
  SET_QUEUE_ITEMS_PRIORITY_MUTATION,
  SYSTEM_INFO_QUERY,
} from "@/graphql/queries";
import { formatJobReleaseName, type JobData, type JobProtocol } from "@/lib/job-types";
import { cn } from "@/lib/utils";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import { useTranslate } from "@/lib/context/translate-context";
import { statusToken } from "@/lib/status-tokens";
import { showsSeeding, torrentErrorText, type TorrentSummaryData } from "@/lib/torrents";
import { BulkBar, BulkButton, BulkCluster, BulkMenu } from "../components/BulkBar";
import { EmptyState, MetricCell, SectionHeader } from "../components/chrome";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { CheckBox, PrimaryButton, SecondaryButton, TextField } from "../components/controls";
import { Icon } from "../components/icons";
import { Menu, MenuItem } from "../components/Menu";
import { Pagination } from "../components/Pagination";
import { StorageMounts, type StorageVolume } from "../components/storage";
import { Tabs } from "../components/Tabs";
import { useNextData } from "../data/next-data";
import { sameElements, sameEntries, sameItems, shallowEqual, useSettled } from "../data/settled";
import {
  batchTarget,
  conflictCount,
  departedId,
  etaById as etaByIdFrom,
  groupCounts as groupCountsFrom,
  queuePageInput,
  queueSelection,
  tabCounts as tabCountsFrom,
  withoutIds,
  type QueueBatchTarget,
  type QueueEventSummary,
  type QueueSort,
  type QueueTab,
} from "../data/queue-page";
import { useQueuePage } from "../data/use-live-queue";
import { EM_DASH, formatDayClock, formatSize } from "../data/format";
import { ratePhase } from "../data/phase-bars";
import {
  categoryFacets,
  countsByFacet,
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
import {
  AttentionBlock,
  CategoryListBlock,
  ProtocolFilterBlock,
  ProvidersBlock,
  type ProtocolFacet,
} from "../shell/rail-blocks";
import { AddNzbDialog } from "../features/AddNzbDialog";
import { ScheduleSeedingNote } from "../features/torrents/ScheduleSeedingNote";
import { TorrentRemoveDialog } from "../features/torrents/TorrentRemoveDialog";
import { useTorrentSummaries } from "../data/use-torrents";
import { SpeedLimitControl, useSpeedLimitInForce } from "../features/SpeedLimitDialog";
import { DownloadInspector } from "./downloads/DownloadInspector";
import { DownloadRow } from "./downloads/DownloadRow";
import { QueueEtaContext } from "./downloads/queue-eta";

type TabId = QueueTab;
type SortId = QueueSort;

/** Each sort and the key of its label. */
const SORT_OPTIONS: { value: SortId; label: string }[] = [
  { value: "priority", label: "table.priority" },
  { value: "name", label: "table.name" },
  { value: "size", label: "table.size" },
  { value: "progress", label: "table.progress" },
  { value: "eta", label: "next.common.timeLeft" },
];

const PAGE_SIZES = [25, 50, 100] as const;

/** The bulk edits a report names, each with its `.one`/`.other` and `Partial` keys. */
type BulkReport = "paused" | "resumed" | "priority" | "category" | "cancelled";

/** The statuses a global pause or a download block holds back. */
const HELD_BACK_STATUSES = new Set(["DOWNLOADING", "QUEUED", "PROPAGATING"]);
const NO_ETAS: ReadonlyMap<number, string> = new Map();

/** What a batch mutation answers. */
interface BatchResult {
  matched: number;
  applied: number;
  refused: { id: number; code: string; message: string }[];
}

function sortJobs(
  jobs: readonly JobData[],
  sort: SortId,
  etaById: ReadonlyMap<number, string> | null,
): readonly JobData[] {
  // The daemon sorts the page; within a section only time left needs ranking,
  // and its order is queue order, which the page already holds.
  if (sort !== "eta") {
    return jobs;
  }
  // Rows without an estimate sort last.
  const rank = new Map([...(etaById ?? NO_ETAS).keys()].map((id, index) => [id, index]));
  return [...jobs].sort(
    (left, right) =>
      (rank.get(left.id) ?? Number.MAX_SAFE_INTEGER) - (rank.get(right.id) ?? Number.MAX_SAFE_INTEGER),
  );
}

const PAUSE_FOR_OPTIONS: { seconds: number; label: string }[] = [
  { seconds: 30 * 60, label: "next.downloads.pauseFor30m" },
  { seconds: 60 * 60, label: "next.downloads.pauseFor1h" },
  { seconds: 2 * 60 * 60, label: "next.downloads.pauseFor2h" },
  { seconds: 8 * 60 * 60, label: "next.downloads.pauseFor8h" },
];

/** Pause all, split with a menu of timed pauses that resume on their own. */
function PauseAllControl({ paused, resumeAt }: { paused: boolean; resumeAt: number | null }) {
  const t = useTranslate();
  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [open, setOpen] = useState(false);

  if (paused && resumeAt !== null) {
    return <TimedResumeButton resumeAt={resumeAt} onResume={() => void resumeAll({})} />;
  }
  if (paused) {
    return (
      <button type="button" onClick={() => void resumeAll({})} className={PAUSED_BUTTON}>
        <Icon name="resume" size={14} className="-ml-[2px] mr-[7px] flex-none" />
        {t("next.downloads.resumeAll")}
      </button>
    );
  }

  return (
    <div className="relative flex">
      <SecondaryButton icon="pause" onClick={() => void pauseAll({})}>
        {t("next.downloads.pauseAll")}
      </SecondaryButton>
      <button
        type="button"
        data-wv-menu-trigger=""
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={t("next.downloads.pauseForMenu")}
        onClick={() => setOpen((previous) => !previous)}
        className="-ml-px flex h-[34px] cursor-pointer items-center border border-wv-control bg-wv-button px-[9px] text-wv-fg hover:border-wv-control-hover hover:bg-wv-button-hover"
      >
        <Icon name="dropdown" size={14} />
      </button>
      <Menu
        open={open}
        onDismiss={() => setOpen(false)}
        label={t("next.downloads.pauseForMenu")}
        className="top-[38px] right-0 w-[164px]"
      >
        {PAUSE_FOR_OPTIONS.map((option) => (
          <MenuItem
            key={option.seconds}
            onSelect={() => {
              setOpen(false);
              void pauseAll({ resumeAfterSecs: option.seconds });
            }}
          >
            {t(option.label)}
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}

/** A paused queue's resume button: warning yellow, with the ping the update notice uses. */
const PAUSED_BUTTON =
  "wv-ping flex h-[34px] cursor-pointer items-center whitespace-nowrap bg-wv-warn px-[14px] text-[13px] font-medium text-wv-on-warn [--wv-ping-color:var(--color-wv-warn)] hover:bg-wv-warn-hover";

/** Whole minutes left as HH:MM, rounded up so it never reads 00:00 while still paused. */
function formatResumeCountdown(msLeft: number): string {
  const minutes = Math.max(0, Math.ceil(msLeft / 60_000));
  return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
}

/**
 * A timed pause: the countdown to the automatic resume, which reads
 * "Resume now" while hovered or focused. Both labels share one grid cell so
 * the button keeps its width when the text swaps.
 */
function TimedResumeButton({ resumeAt, onResume }: { resumeAt: number; onResume: () => void }) {
  const t = useTranslate();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  const countdown = t("next.downloads.resumingIn", { time: formatResumeCountdown(resumeAt - now) });
  const resumeNow = t("next.downloads.resumeNow");

  return (
    <button
      type="button"
      onClick={onResume}
      aria-label={`${resumeNow} (${countdown})`}
      className={cn("group", PAUSED_BUTTON)}
    >
      <Icon name="resume" size={14} className="-ml-[2px] mr-[7px] flex-none" />
      <span className="grid">
        <span className="col-start-1 row-start-1 tabular-nums group-hover:invisible group-focus-visible:invisible">
          {countdown}
        </span>
        <span className="invisible col-start-1 row-start-1 group-hover:visible group-focus-visible:visible">
          {resumeNow}
        </span>
      </span>
    </button>
  );
}

export function DownloadsPage() {
  const { queue, categories: configured, speed, isPaused, torrentsPaused, resumeAt, downloadBlock } = useNextData();
  // The ETA's cap; 0 is none.
  const speedLimit = useSpeedLimitInForce().inForce ?? 0;
  const statusLabel = useStatusLabel();
  const t = useTranslate();

  const [facets, setFacets] = useState<ReadonlySet<string>>(NO_FACETS);
  const [protocol, setProtocol] = useState<ProtocolFacet>(null);
  const [tab, setTab] = useState<TabId>("all");
  const [sort, setSort] = useState<SortId>("priority");
  const [query, setQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState<number>(50);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [sortOpen, setSortOpen] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);
  // NZBs dropped on the list, handed to the dialog so it opens with them staged.
  const [droppedFiles, setDroppedFiles] = useState<readonly File[] | null>(null);
  const [dropping, setDropping] = useState(false);
  // dragenter and dragleave fire for every child the pointer crosses, so the
  // highlight tracks how deep inside the pane the drag is, not the last event.
  const dragDepth = useRef(0);
  // Cancelling is not instant; hide the row until the daemon's next page drops it.
  const [removedIds, setRemovedIds] = useState<ReadonlySet<number>>(() => new Set());
  // Ticked for a bulk action; separate from the one row the inspector shows.
  const [picked, setPicked] = useState<ReadonlySet<number>>(() => new Set());
  // Every job the filters match, on every page, rather than the ticked rows.
  const [allMatching, setAllMatching] = useState(false);
  const [bulkBusy, setBulkBusy] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(false);
  // The one row whose cancel button is waiting on confirmation.
  const [confirmRowCancel, setConfirmRowCancel] = useState<number | null>(null);
  // The one torrent whose remove is waiting on confirmation.
  const [removeTorrentId, setRemoveTorrentId] = useState<number | null>(null);
  // A selection batch the daemon refused because the queue moved since the
  // operator confirmed it: the count it matches now, and the batch to run again.
  const [reconfirm, setReconfirm] = useState<{ matched: number; run: () => void } | null>(null);
  const [report, setReport] = useState<string | null>(null);

  const client = useClient();

  const [, pauseTorrents] = useMutation(PAUSE_TORRENTS_MUTATION);
  const [, resumeTorrents] = useMutation(RESUME_TORRENTS_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation<{ cancelJob: boolean }>(CANCEL_JOB_MUTATION);
  const [{ data: systemInfo }] = useQuery<{
    systemInfo: { configuredStorage: StorageVolume[] };
  }>({ query: SYSTEM_INFO_QUERY });

  const selection = useMemo(
    () => queueSelection({ tab, facets, query, protocol }),
    [facets, protocol, query, tab],
  );
  const input = useMemo(
    () => queuePageInput(selection, sort, pageIndex, pageSize),
    [pageIndex, pageSize, selection, sort],
  );
  const page = useQueuePage(input);
  const pageCount = Math.max(1, Math.ceil(page.totalCount / pageSize));

  // A queue that shrinks can leave the page index past the end. The page
  // shown is the last one there is, judged only on the asked page's own
  // count; the index follows after the render, never during it.
  const settled = !page.isLoading && !page.stale;
  const clampedPage = settled ? Math.min(pageIndex, pageCount - 1) : pageIndex;
  useEffect(() => {
    if (clampedPage !== pageIndex) setPageIndex(clampedPage);
  }, [clampedPage, pageIndex]);
  const showingStale = page.stale || clampedPage !== pageIndex;

  /** A new filter, sort or page size starts again at the first page, with nothing ticked. */
  const reset = (change: () => void) => {
    change();
    setPageIndex(0);
    setPicked(new Set());
    setAllMatching(false);
  };

  const jobs = useMemo(
    () => page.jobs.filter((job) => !removedIds.has(job.id)),
    [page.jobs, removedIds],
  );

  const torrents = useTorrentSummaries(jobs.some((job) => job.protocol === "TORRENT"));
  const seedingColumn = [...torrents.values()].some((torrent) => showsSeeding(torrent.state));
  // The row handlers read a job's protocol by id, so they keep their identity while the rows move.
  const jobsRef = useRef(jobs);
  useLayoutEffect(() => {
    jobsRef.current = jobs;
  });
  const isTorrent = useCallback((id: number) => jobsRef.current.find((job) => job.id === id)?.protocol === "TORRENT", []);


  // The time-left speed samples the bytes the estimate counts across the whole
  // queue, which the daemon totals; the page holds only its own rows.
  const etaSample = useMemo(
    () =>
      page.etaTotalBytes > page.etaDownloadedBytes
        ? [
            {
              id: 0,
              status: "DOWNLOADING",
              totalBytes: page.etaTotalBytes,
              downloadedBytes: page.etaDownloadedBytes,
              remainingBytes: page.etaTotalBytes - page.etaDownloadedBytes,
            },
          ]
        : [],
    [page.etaDownloadedBytes, page.etaTotalBytes],
  );
  const etaConditions = useMemo(() => ({ paused: isPaused, speedLimit }), [isPaused, speedLimit]);
  const etaSpeed = useStableEtaSpeed(etaSample, speed, etaConditions);
  const estimates = useMemo(
    () => etaByIdFrom(page.etaThrough, etaSpeed, formatEtaFromRemainingBytes),
    [etaSpeed, page.etaThrough],
  );
  // Kept while every estimate reads the same, so the rows' time-left text is
  // drawn again only when some of it changes.
  const etaById = useSettled(estimates, sameEntries);

  const categories = useSettled(
    useMemo<CategoryEntry[]>(
      () =>
        categoryFacets({
          configured,
          counts: countsByFacet(page.categoryCounts),
          extras: queue.categories,
        }),
      [configured, page.categoryCounts, queue.categories],
    ),
    sameItems,
  );

  const protocolCounts = useSettled(
    useMemo(() => {
      const count = (protocol: JobProtocol) =>
        page.protocolCounts.find((entry) => entry.protocol === protocol)?.count ?? 0;
      const torrents = count("TORRENT");
      const usenet = count("NZB");
      return { ALL: torrents + usenet, TORRENT: torrents, NZB: usenet };
    }, [page.protocolCounts]),
    shallowEqual,
  );

  const grouped = useMemo(() => {
    const buckets = new Map<DownloadGroup, JobData[]>();
    for (const group of DOWNLOAD_GROUP_ORDER) {
      buckets.set(group, []);
    }
    for (const job of jobs) {
      buckets.get(downloadGroup(job.status))!.push(job);
    }
    return buckets;
  }, [jobs]);

  const tabCounts = useSettled(
    useMemo(() => tabCountsFrom(page.stateCounts), [page.stateCounts]),
    shallowEqual,
  );
  // Each section's header counts its jobs across every page.
  const groupCounts = useSettled(
    useMemo(() => groupCountsFrom(page.stateCounts), [page.stateCounts]),
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

  const visibleGroups = DOWNLOAD_GROUP_ORDER.filter((group) => grouped.get(group)!.length > 0);

  const selected = jobs.find((job) => job.id === selectedId) ?? null;

  // Ticks hold across pages. A ticked download leaves the selection only when
  // it leaves the queue: finished, failed or removed.
  const dropDeparted = useCallback(
    (previous: unknown, response: { queueEvents: QueueEventSummary }) => {
      const id = departedId(response.queueEvents);
      if (id !== null) setPicked((current) => withoutIds(current, [id]));
      return previous;
    },
    [],
  );
  useSubscription({ query: HISTORY_FACADE_EVENTS_SUBSCRIPTION }, dropDeparted);

  const togglePicked = useCallback((id: number) => {
    setAllMatching(false);
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
    setAllMatching(false);
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

  const clearPicked = () => {
    setPicked(new Set());
    setAllMatching(false);
  };

  const refreshQueue = page.refresh;
  const pickedCount = allMatching ? page.totalCount : picked.size;
  // Every row on the page is ticked and more match than are ticked.
  const canSelectAllMatching =
    !allMatching
    && !showingStale
    && jobs.length > 0
    && jobs.every((job) => picked.has(job.id))
    && page.totalCount > picked.size;

  const bulkReport = (kind: BulkReport, total: number, refused: number) =>
    refused === 0
      ? countLabel(t, `next.downloads.bulk.${kind}`, total)
      : t(`next.downloads.bulk.${kind}Partial`, { done: total - refused, total, refused });

  /**
   * One batch for the whole selection: the ticked rows by id, or every job
   * the filters match by the filters themselves, with the count the operator
   * saw. The ids it carried out leave the selection, and so do those the
   * queue no longer holds; the rest stay ticked, and the report says how many
   * went through. A request error is named only when nothing did. When the
   * queue moved under a selection, nothing is done and the operator is asked
   * again with the count it matches now.
   */
  const runBatch = async (
    kind: BulkReport,
    mutation: TypedDocumentNode<Record<string, BatchResult>, Record<string, unknown>>,
    field: string,
    extra: Record<string, unknown> = {},
    before?: (target: QueueBatchTarget) => void,
    confirmed: number = page.totalCount,
  ): Promise<ReadonlySet<number> | null> => {
    const target = batchTarget(allMatching, picked, selection, confirmed);
    setBulkBusy(true);
    before?.(target);
    const result = await client.mutation(mutation, { target, ...extra }).toPromise();
    setBulkBusy(false);
    const answer = result.data?.[field];
    if (!answer) {
      const matched = "selection" in target ? conflictCount(result.error) : null;
      if (matched !== null) {
        setReconfirm({
          matched,
          run: () => void runBatch(kind, mutation, field, extra, before, matched),
        });
        refreshQueue();
        return null;
      }
      setReport(t("next.downloads.bulk.failed", { message: result.error?.message ?? "" }));
      refreshQueue();
      return null;
    }
    const refused = new Set(
      answer.refused.filter((entry) => entry.code !== "NOT_FOUND").map((entry) => entry.id),
    );
    if ("ids" in target) {
      setPicked((current) => new Set([...current].filter((id) => refused.has(id))));
    } else {
      setPicked(new Set());
    }
    setAllMatching(false);
    setReport(bulkReport(kind, answer.matched, answer.refused.length));
    refreshQueue();
    return refused;
  };

  const pausePicked = () => runBatch("paused", PAUSE_QUEUE_ITEMS_MUTATION, "pauseQueueItems");
  const resumePicked = () => runBatch("resumed", RESUME_QUEUE_ITEMS_MUTATION, "resumeQueueItems");
  const setPickedPriority = (priority: string) =>
    runBatch("priority", SET_QUEUE_ITEMS_PRIORITY_MUTATION, "setQueueItemsPriority", { priority });
  const setPickedCategory = (category: string) =>
    runBatch("category", SET_QUEUE_ITEMS_CATEGORY_MUTATION, "setQueueItemsCategory", { category });

  // Ticked rows leave the list the moment the cancel is sent, and any the
  // daemon refuses come back once it answers.
  const cancelPicked = async () => {
    let hidden: number[] = [];
    const refused = await runBatch("cancelled", CANCEL_QUEUE_ITEMS_MUTATION, "cancelQueueItems", {}, (target) => {
      if ("ids" in target) {
        hidden = target.ids;
        setRemovedIds((current) => new Set([...current, ...target.ids]));
        setSelectedId((current) => (current !== null && target.ids.includes(current) ? null : current));
      }
    });
    const back = refused === null ? new Set(hidden) : refused;
    if (back.size > 0) {
      setRemovedIds((current) => new Set([...current].filter((id) => !back.has(id))));
    }
    setConfirmCancel(false);
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
      {canSelectAllMatching ? (
        <BulkButton disabled={bulkBusy} onClick={() => setAllMatching(true)}>
          {t("next.downloads.selectAllMatching", { count: page.totalCount })}
        </BulkButton>
      ) : null}
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
        onSelect={(value) => void setPickedPriority(value)}
      >
        {t("table.priority")}
      </BulkMenu>
      <BulkMenu
        icon="categories"
        label={t("next.downloads.setCategory")}
        disabled={bulkBusy}
        options={categoryOptions}
        onSelect={(value) => void setPickedCategory(value)}
      >
        {t("table.category")}
      </BulkMenu>
      <BulkButton icon="cancelDownload" tone="danger" disabled={bulkBusy} onClick={() => setConfirmCancel(true)}>
        {t("action.cancel")}
      </BulkButton>
    </>
  );

  const remainingBytes = page.remainingBytes;

  const volumes = systemInfo?.systemInfo?.configuredStorage ?? [];

  const handleRemoved = useCallback((id: number) => {
    setRemovedIds((current) => new Set(current).add(id));
    setSelectedId((current) => (current === id ? null : current));
  }, []);

  const pauseRow = useCallback(
    (id: number, paused: boolean) => {
      void (paused ? pauseJob({ id }) : resumeJob({ id })).then((result) => {
        if (result.error) setReport(torrentErrorText(result.error));
        refreshQueue();
      });
    },
    [pauseJob, refreshQueue, resumeJob],
  );
  const askCancelRow = useCallback(
    (id: number) => (isTorrent(id) ? setRemoveTorrentId(id) : setConfirmRowCancel(id)),
    [isTorrent],
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

  // A speed test holds the queue on its own, apart from a pause.
  const heldBy =
    downloadBlock.kind === "SPEED_TEST" ? t("jobs.speedTestBadge") : t("next.downloads.waitPaused");

  /** What stands in for time left while a download is held; null when it is not. */
  const heldValue = useCallback(
    (job: JobData): string | null => {
      if (statusToken(job.status) === "paused") return t("next.downloads.waitPaused");
      if (HELD_BACK_STATUSES.has(job.status)) {
        if (blocked) return blockEta;
        if (isPaused) return heldBy;
      }
      return null;
    },
    [blockEta, blocked, heldBy, isPaused, t],
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
            onChange={(next) => reset(() => setQuery(next))}
            className="w-[132px] sm:w-[224px]"
          />
          <PauseAllControl paused={isPaused} resumeAt={resumeAt} />
          <ScheduleSeedingNote torrents={torrents} />
          <SecondaryButton
            icon={torrentsPaused ? "resume" : "pause"}
            onClick={() => {
              void (torrentsPaused ? resumeTorrents({}) : pauseTorrents({}));
            }}
          >
            {torrentsPaused ? t("next.downloads.resumeTorrents") : t("next.downloads.pauseTorrents")}
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
          <ProtocolFilterBlock
            counts={protocolCounts}
            selected={protocol}
            onSelect={(facet) => reset(() => setProtocol(facet))}
          />
          <CategoryListBlock
            items={categories}
            selected={facets}
            onToggle={(key) => reset(() => setFacets((current) => toggleFacet(current, key)))}
            onClear={() => reset(() => setFacets(NO_FACETS))}
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
                  : page.isLoading
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
            onSelect={(next) => reset(() => setTab(next))}
            center={
              pickedCount === 0 ? undefined : (
                <BulkCluster count={pickedCount} onClear={clearPicked}>
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
          {pickedCount === 0 ? null : (
            <BulkBar count={pickedCount} onClear={clearPicked} className="xl:hidden">
              {bulkActions}
            </BulkBar>
          )}
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
          total={page.totalCount}
        />
      }
      statusNote={report ?? undefined}
      statusRight={
        pageCount > 1
          ? t("next.downloads.showingPage", { shown: jobs.length, total: page.totalCount })
          : t("next.downloads.shown", { shown: jobs.length, total: queue.summary.totalItems })
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
        {/* The rows lay out by the list's own width, which the inspector beside it takes from. */}
        <div
          aria-busy={showingStale}
          className={`@container flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list${showingStale ? " opacity-60" : ""}`}
        >
          {page.isLoading ? (
            <EmptyState loading title={t("next.common.loading")} body={t("next.downloads.loadingBody")} />
          ) : queue.summary.totalItems === 0 && jobs.length === 0 ? (
            <EmptyState
              centered
              title={t("next.downloads.emptyTitle")}
              body={t("next.downloads.emptyBody")}
              action={<PrimaryButton icon="add" onClick={() => setUploadOpen(true)}>{t("next.addNzb.title")}</PrimaryButton>}
            />
          ) : jobs.length === 0 ? (
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
                  count={groupCounts[group]}
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
                  onCancel={askCancelRow}
                  torrents={torrents}
                  seedingColumn={seedingColumn}
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
          torrent={torrents.get(selected.id) ?? null}
          onRemoved={handleRemoved}
        />
      )}

      <ConfirmDialog
        open={confirmCancel}
        title={t("next.downloads.cancelTitle")}
        note={t("bulk.selected", { count: pickedCount })}
        busy={bulkBusy}
        destructive
        body={t("confirm.cancelSelectedMessage")}
        confirmLabel={t("next.downloads.cancelTitle")}
        dismissLabel={t("next.downloads.keepDownloading")}
        onConfirm={() => void cancelPicked()}
        onDismiss={() => setConfirmCancel(false)}
      />

      <ConfirmDialog
        open={reconfirm !== null}
        title={t("next.downloads.selectionChanged")}
        busy={bulkBusy}
        body={t("bulk.selected", { count: reconfirm?.matched ?? 0 })}
        confirmLabel={t("action.apply")}
        onConfirm={() => {
          const run = reconfirm?.run;
          setReconfirm(null);
          run?.();
        }}
        onDismiss={() => setReconfirm(null)}
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

      <TorrentRemoveDialog
        id={removeTorrentId}
        name={(() => {
          const job = jobs.find((candidate) => candidate.id === removeTorrentId);
          return job ? formatJobReleaseName(job) : undefined;
        })()}
        onRemoved={(id) => {
          setRemoveTorrentId(null);
          handleRemoved(id);
          refreshQueue();
        }}
        onFailed={(message) => {
          setRemoveTorrentId(null);
          setReport(message);
        }}
        onDismiss={() => setRemoveTorrentId(null)}
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
  /** The section's jobs across every page. */
  count: number;
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
  torrents: ReadonlyMap<number, TorrentSummaryData>;
  seedingColumn: boolean;
}

/**
 * One section of the queue. A progress event replaces one job's object and
 * leaves the rest as they were, so a section whose rows are all the same
 * objects in the same order is not drawn again.
 */
const QueueGroup = memo(function QueueGroup({
  group,
  count,
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
  torrents,
  seedingColumn,
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
        count={count}
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
          hold={
            torrents.get(job.id)?.state === "NEEDS_CERTIFICATE" ? t("next.torrent.needsCertificate") : holdOf(job)
          }
          seeding={seedingOf(torrents.get(job.id))}
          seedingColumn={seedingColumn}
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

/** A torrent's figures for the seeding column, while it seeds or has stopped. */
function seedingOf(torrent: TorrentSummaryData | undefined): TorrentSummaryData | null {
  return torrent && showsSeeding(torrent.state) ? torrent : null;
}

/** Whether a drag is carrying files from the desktop, not text or a link from the page. */
function carriesFiles(event: DragEvent): boolean {
  return Array.from(event.dataTransfer.types).includes("Files");
}

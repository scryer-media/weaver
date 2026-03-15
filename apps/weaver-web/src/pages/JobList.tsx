import { Fragment, useRef, useState } from "react";
import { Link } from "react-router";
import { ChevronDown, ChevronRight, Pause, Pencil, Play, X } from "lucide-react";
import { useMutation, useQuery } from "urql";
import {
  CANCEL_JOB_MUTATION,
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
  SERVERS_QUERY,
  SET_SPEED_LIMIT_MUTATION,
  UPDATE_JOBS_MUTATION,
} from "@/graphql/queries";
import { BulkEditModal } from "@/components/BulkEditModal";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { JobProgress } from "@/components/JobProgress";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { PageHeader } from "@/components/PageHeader";
import { ParsedReleaseDetails } from "@/components/ParsedReleaseDetails";
import { SpeedDisplay, formatBytes } from "@/components/SpeedDisplay";
import { UploadModal } from "@/components/UploadModal";
import { useLiveData } from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const SPEED_LIMIT_PRESETS = [
  { label: "Unlimited", value: 0 },
  { label: "1 MB/s", value: 1024 * 1024 },
  { label: "5 MB/s", value: 5 * 1024 * 1024 },
  { label: "10 MB/s", value: 10 * 1024 * 1024 },
  { label: "25 MB/s", value: 25 * 1024 * 1024 },
  { label: "50 MB/s", value: 50 * 1024 * 1024 },
  { label: "100 MB/s", value: 100 * 1024 * 1024 },
];

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

function formatEta(remainingBytes: number, speed: number): string {
  if (speed <= 0 || remainingBytes <= 0) return "\u2014";
  const secs = Math.ceil(remainingBytes / speed);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

const ETA_UPDATE_INTERVAL_MS = 2500;

function buildQueueEtaById(
  jobs: {
    id: number;
    status: string;
    totalBytes: number;
    downloadedBytes: number;
  }[],
  speed: number,
): Map<number, string> {
  const etaById = new Map<number, string>();
  if (speed <= 0) {
    return etaById;
  }

  let bytesAhead = 0;
  for (const job of jobs) {
    if (job.status !== "DOWNLOADING" && job.status !== "QUEUED") {
      continue;
    }
    const remaining = Math.max(job.totalBytes - job.downloadedBytes, 0);
    bytesAhead += remaining;
    etaById.set(job.id, formatEta(bytesAhead, speed));
  }

  return etaById;
}

function useThrottledQueueEta(
  jobs: {
    id: number;
    status: string;
    totalBytes: number;
    downloadedBytes: number;
  }[],
  speed: number,
) {
  const cache = useRef({
    at: 0,
    signature: "",
    etaById: new Map<number, string>(),
  });

  const signature = jobs
    .filter((job) => job.status === "DOWNLOADING" || job.status === "QUEUED")
    .map((job) => `${job.id}:${job.status}:${job.totalBytes}`)
    .join("|");

  // Signature changed or speed dropped to zero — always refresh.
  // Otherwise, throttle by checking elapsed time via the ref.
  const signatureChanged = speed <= 0 || cache.current.signature !== signature;
  // eslint-disable-next-line react-hooks/purity -- Date.now() is intentional for throttling
  const stale = !signatureChanged && Date.now() - cache.current.at >= ETA_UPDATE_INTERVAL_MS;

  if (signatureChanged || stale) {
    cache.current = {
      at: Date.now(), // eslint-disable-line react-hooks/purity
      signature,
      etaById: buildQueueEtaById(jobs, speed),
    };
  }

  return cache.current.etaById;
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

export function JobList() {
  const [serversResult] = useQuery({ query: SERVERS_QUERY });
  const hasNoServers = (serversResult.data?.servers?.length ?? 1) === 0;

  const { jobs: allJobs, speed, isPaused, downloadBlock } = useLiveData();
  const jobs = allJobs.filter((job) => job.status !== "COMPLETE" && job.status !== "FAILED");
  const blockedJobs = jobs.filter((job) => isBlockedByGlobalPause(job, isPaused)).length;
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
  const [speedLimitValue, setSpeedLimitValue] = useState("0");
  const [cancelConfirmId, setCancelConfirmId] = useState<number | null>(null);
  const [expandedJobIds, setExpandedJobIds] = useState<Set<number>>(new Set());
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [bulkEditOpen, setBulkEditOpen] = useState(false);
  const [cancelSelectedConfirm, setCancelSelectedConfirm] = useState(false);
  const t = useTranslate();

  const toggleExpanded = (jobId: number) => {
    setExpandedJobIds((current) => {
      const next = new Set(current);
      if (next.has(jobId)) {
        next.delete(jobId);
      } else {
        next.add(jobId);
      }
      return next;
    });
  };

  const toggleSelected = (id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selectedIds.size === jobs.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(jobs.map((j) => j.id)));
    }
  };

  const handleBulkEdit = async (category: string | null, priority: string | null) => {
    const ids = Array.from(selectedIds);
    await updateJobs({ ids, category, priority });
    setSelectedIds(new Set());
    setBulkEditOpen(false);
  };

  const handleBulkPause = async () => {
    for (const id of selectedIds) {
      await pauseJob({ id });
    }
    setSelectedIds(new Set());
  };

  const handleBulkCancel = async () => {
    for (const id of selectedIds) {
      await cancelJob({ id });
    }
    setSelectedIds(new Set());
    setCancelSelectedConfirm(false);
  };

  const queueEtaById = useThrottledQueueEta(jobs, speed);

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("jobs.title")}
        description={
          downloadBlock.kind === "ISP_CAP" && capBlockedJobs > 0
            ? t("jobs.bandwidthCapHeaderHint", { resetAt: capResetAt })
            : isPaused && blockedJobs > 0
            ? t("jobs.pausedHeaderHint")
            : jobs.length === 0
              ? t("jobs.emptyHint")
              : undefined
        }
        actions={
          <>
            <div className="rounded-xl border border-border/70 bg-background/70 px-4 py-2">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {t("label.downloadSpeed")}
              </div>
              <SpeedDisplay bytesPerSec={speed} className="text-lg font-semibold text-foreground" />
              {isPaused ? (
                <div className="mt-1 text-[11px] font-medium uppercase tracking-[0.16em] text-amber-600 dark:text-amber-300">
                  {t("jobs.downloadsPaused")}
                </div>
              ) : null}
            </div>
            <Select
              value={speedLimitValue}
              onValueChange={(value) => {
                setSpeedLimitValue(value);
                void setSpeedLimit({ bytesPerSec: Number(value) });
              }}
            >
              <SelectTrigger className="w-36">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {SPEED_LIMIT_PRESETS.map((preset) => (
                  <SelectItem key={preset.value} value={String(preset.value)}>
                    {preset.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
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
      ) : isPaused && blockedJobs > 0 ? (
        <Card className="border-amber-500/40 bg-amber-500/8">
          <CardContent className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="warning">{t("jobs.globalPauseBadge")}</Badge>
                <span className="text-sm font-medium text-foreground">
                  {t("jobs.downloadsPausedTitle")}
                </span>
              </div>
              <div className="text-sm text-muted-foreground">
                {t("jobs.downloadsPausedBody")}
              </div>
              <div className="text-xs uppercase tracking-[0.14em] text-amber-700 dark:text-amber-300">
                {t("jobs.pauseAffectedCount", { count: blockedJobs })}
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <Button onClick={() => void resumeAll({})}>
                <Play className="size-4" />
                {t("action.resumeAll")}
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

      {selectedIds.size > 0 ? (
        <Card className="border-primary/40 bg-primary/5">
          <CardContent className="flex items-center justify-between py-3">
            <span className="text-sm font-medium">
              {t("bulk.selected", { count: selectedIds.size })}
            </span>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={() => setBulkEditOpen(true)}>
                <Pencil className="size-3.5" />
                {t("bulk.editSelected")}
              </Button>
              <Button variant="outline" size="sm" onClick={() => void handleBulkPause()}>
                <Pause className="size-3.5" />
                {t("bulk.pauseSelected")}
              </Button>
              <Button variant="destructive" size="sm" onClick={() => setCancelSelectedConfirm(true)}>
                <X className="size-3.5" />
                {t("bulk.cancelSelected")}
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
        <>
          <Card className="hidden lg:block">
            <CardContent className="px-0 pb-0">
              <Table className="table-fixed">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-7 w-[4%] px-2">
                      <Checkbox
                        checked={selectedIds.size === jobs.length && jobs.length > 0 ? true : selectedIds.size > 0 ? "indeterminate" : false}
                        onCheckedChange={toggleSelectAll}
                      />
                    </TableHead>
                    <TableHead className="h-7 w-[30%] px-2 text-[13px]">{t("table.name")}</TableHead>
                    <TableHead className="h-7 w-[11%] px-2 text-[13px]">{t("table.status")}</TableHead>
                    <TableHead className="h-7 w-[7%] px-2 text-[13px]">{t("table.priority")}</TableHead>
                    <TableHead className="h-7 w-[8%] px-2 text-[13px]">{t("table.category")}</TableHead>
                    <TableHead className="h-7 w-[15%] px-2 text-[13px]">{t("table.progress")}</TableHead>
                    <TableHead className="h-7 w-[12%] px-2 text-right text-[13px]">{t("table.size")}</TableHead>
                    <TableHead className="h-7 w-[6%] px-2 text-right text-[13px]">{t("table.eta")}</TableHead>
                    <TableHead className="h-7 w-[7%] px-2 text-right text-[13px]">{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => {
                    const priority = getJobPriority(job);
                    const displayName = job.displayTitle;
                    const expanded = expandedJobIds.has(job.id);
                    return (
                      <Fragment key={job.id}>
                        <TableRow key={job.id} className="text-xs">
                          <TableCell className="px-2 py-1.5">
                            <Checkbox
                              checked={selectedIds.has(job.id)}
                              onCheckedChange={() => toggleSelected(job.id)}
                            />
                          </TableCell>
                          <TableCell className="min-w-0 px-2 py-1.5">
                            <div className="flex min-w-0 items-center gap-1.5">
                              <Button
                                variant="ghost"
                                size="icon"
                                className="size-5 shrink-0"
                                title={expanded ? "Collapse details" : "Expand details"}
                                aria-label={expanded ? "Collapse details" : "Expand details"}
                                onClick={() => toggleExpanded(job.id)}
                              >
                                {expanded ? (
                                  <ChevronDown className="size-3.5" />
                                ) : (
                                  <ChevronRight className="size-3.5" />
                                )}
                              </Button>
                              <Link
                                to={`/jobs/${job.id}`}
                                title={job.originalTitle}
                                className="block min-w-0 truncate text-xs font-medium leading-tight transition hover:text-primary"
                              >
                                {displayName}
                              </Link>
                              {job.hasPassword ? (
                                <span className="shrink-0 text-[10px] text-amber-500">{t("jobs.passwordProtected")}</span>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell className="overflow-hidden px-2 py-1.5">
                            <div className="flex flex-col items-start gap-1">
                              <JobStatusBadge status={job.status} compact className="px-1.5" />
                              {isBlockedByIspCap(job, downloadBlock) ? (
                                <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-orange-600 dark:text-orange-300">
                                  {t("jobs.bandwidthCapShort")}
                                </span>
                              ) : isBlockedByGlobalPause(job, isPaused) ? (
                                <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-amber-600 dark:text-amber-300">
                                  {t("jobs.globalPauseShort")}
                                </span>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell className="truncate px-2 py-1.5 text-[11px]" title={formatJobPriority(priority)}>
                            {formatJobPriority(priority)}
                          </TableCell>
                          <TableCell className="truncate px-2 py-1.5 text-[11px]" title={job.category ?? "\u2014"}>
                            {job.category ?? "\u2014"}
                          </TableCell>
                          <TableCell className="min-w-0 px-2 py-1.5" title={`${(job.progress * 100).toFixed(1)}%`}>
                            <JobProgress progress={job.progress} status={job.status} compact showLabel={false} />
                          </TableCell>
                          <TableCell className="px-2 py-1.5 text-right text-[11px] text-muted-foreground">
                            {formatBytes(job.downloadedBytes)} / {formatBytes(job.totalBytes)}
                          </TableCell>
                          <TableCell className="px-2 py-1.5 text-right text-[11px] text-muted-foreground">
                            {isBlockedByIspCap(job, downloadBlock)
                              ? t("jobs.bandwidthCapEta", { resetAt: capResetAt })
                              : isBlockedByGlobalPause(job, isPaused)
                              ? t("status.paused")
                              : (queueEtaById.get(job.id) ?? "\u2014")}
                          </TableCell>
                          <TableCell className="px-2 py-1.5">
                            <div className="flex justify-end gap-0.5">
                              {job.status === "PAUSED" ? (
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  title={t("action.resume")}
                                  aria-label={t("action.resume")}
                                  className="size-6"
                                  onClick={() => void resumeJob({ id: job.id })}
                                >
                                  <Play className="size-3.5" />
                                </Button>
                              ) : (
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  title={t("action.pause")}
                                  aria-label={t("action.pause")}
                                  className="size-6"
                                  onClick={() => void pauseJob({ id: job.id })}
                                >
                                  <Pause className="size-3.5" />
                                </Button>
                              )}
                              <Button
                                variant="ghost"
                                size="icon"
                                title={t("action.cancel")}
                                aria-label={t("action.cancel")}
                                className="size-6"
                                onClick={() => setCancelConfirmId(job.id)}
                              >
                                <X className="size-3.5" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                        {expanded ? (
                          <TableRow className="bg-accent/10 hover:bg-accent/10">
                            <TableCell colSpan={9} className="px-4 py-4">
                              <ParsedReleaseDetails
                                originalTitle={job.originalTitle}
                                parsedRelease={job.parsedRelease}
                                compact
                              />
                            </TableCell>
                          </TableRow>
                        ) : null}
                      </Fragment>
                    );
                  })}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="space-y-3 lg:hidden">
            {jobs.map((job) => {
              const priority = getJobPriority(job);
              const displayName = job.displayTitle;
              const expanded = expandedJobIds.has(job.id);
              return (
                <Card key={job.id}>
                  <CardContent className="space-y-3">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <Checkbox
                            checked={selectedIds.has(job.id)}
                            onCheckedChange={() => toggleSelected(job.id)}
                          />
                          <Button
                            variant="ghost"
                            size="icon"
                            className="size-7 shrink-0"
                            onClick={() => toggleExpanded(job.id)}
                          >
                            {expanded ? (
                              <ChevronDown className="size-4" />
                            ) : (
                              <ChevronRight className="size-4" />
                            )}
                          </Button>
                          <Link
                            to={`/jobs/${job.id}`}
                            title={job.originalTitle}
                            className="block min-w-0 truncate font-medium transition hover:text-primary"
                          >
                            {displayName}
                          </Link>
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                          <span>{formatJobPriority(priority)}</span>
                          <span>{job.category ?? "\u2014"}</span>
                          <span>{formatBytes(job.downloadedBytes)} / {formatBytes(job.totalBytes)}</span>
                          <span>
                            {isBlockedByIspCap(job, downloadBlock)
                              ? t("jobs.bandwidthCapEta", { resetAt: capResetAt })
                              : isBlockedByGlobalPause(job, isPaused)
                              ? t("status.paused")
                              : (queueEtaById.get(job.id) ?? "\u2014")}
                          </span>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <JobStatusBadge status={job.status} compact />
                        {isBlockedByIspCap(job, downloadBlock) ? (
                          <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-orange-600 dark:text-orange-300">
                            {t("jobs.bandwidthCapShort")}
                          </span>
                        ) : isBlockedByGlobalPause(job, isPaused) ? (
                          <span className="text-[10px] font-medium uppercase tracking-[0.14em] text-amber-600 dark:text-amber-300">
                            {t("jobs.globalPauseShort")}
                          </span>
                        ) : null}
                      </div>
                    </div>
                    <JobProgress progress={job.progress} status={job.status} compact />
                    {expanded ? (
                      <ParsedReleaseDetails
                        originalTitle={job.originalTitle}
                        parsedRelease={job.parsedRelease}
                        compact
                      />
                    ) : null}
                    <div className="flex justify-end gap-2">
                      {job.status === "PAUSED" ? (
                        <Button variant="ghost" size="sm" onClick={() => void resumeJob({ id: job.id })}>
                          {t("action.resume")}
                        </Button>
                      ) : (
                        <Button variant="ghost" size="sm" onClick={() => void pauseJob({ id: job.id })}>
                          {t("action.pause")}
                        </Button>
                      )}
                      <Button variant="ghost" size="sm" onClick={() => setCancelConfirmId(job.id)}>
                        {t("action.cancel")}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </>
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
        title={t("confirm.cancelSelected", { count: selectedIds.size })}
        message={t("confirm.cancelSelectedMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => void handleBulkCancel()}
        onCancel={() => setCancelSelectedConfirm(false)}
      />

      <BulkEditModal
        open={bulkEditOpen}
        selectedCount={selectedIds.size}
        onClose={() => setBulkEditOpen(false)}
        onApply={(category, priority) => void handleBulkEdit(category, priority)}
      />

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
    </div>
  );
}

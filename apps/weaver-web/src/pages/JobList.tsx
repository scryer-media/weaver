import { Fragment, useRef, useState } from "react";
import { Link } from "react-router";
import { ChevronDown, ChevronRight, Pause, Play, X } from "lucide-react";
import { useMutation } from "urql";
import {
  CANCEL_JOB_MUTATION,
  PAUSE_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_ALL_MUTATION,
  RESUME_JOB_MUTATION,
  SET_SPEED_LIMIT_MUTATION,
} from "@/graphql/queries";
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
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
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

function useThrottledEta() {
  const cache = useRef(new Map<number, { eta: string; at: number }>());
  return (jobId: number, remainingBytes: number, jobSpeed: number): string => {
    const now = Date.now();
    const entry = cache.current.get(jobId);
    if (entry && now - entry.at < ETA_UPDATE_INTERVAL_MS) {
      return entry.eta;
    }
    const eta = formatEta(remainingBytes, jobSpeed);
    cache.current.set(jobId, { eta, at: now });
    return eta;
  };
}

export function JobList() {
  const { jobs: allJobs, speed, isPaused } = useLiveData();
  const jobs = allJobs.filter((job) => job.status !== "COMPLETE" && job.status !== "FAILED");

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, setSpeedLimit] = useMutation(SET_SPEED_LIMIT_MUTATION);

  const [uploadOpen, setUploadOpen] = useState(false);
  const [speedLimitValue, setSpeedLimitValue] = useState("0");
  const [cancelConfirmId, setCancelConfirmId] = useState<number | null>(null);
  const [expandedJobIds, setExpandedJobIds] = useState<Set<number>>(new Set());
  const getEta = useThrottledEta();
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

  const totalRemaining = jobs.reduce(
    (sum, job) => (job.status === "DOWNLOADING" ? sum + (job.totalBytes - job.downloadedBytes) : sum),
    0,
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("jobs.title")}
        description={jobs.length === 0 ? t("jobs.emptyHint") : undefined}
        actions={
          <>
            <div className="rounded-xl border border-border/70 bg-background/70 px-4 py-2">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {t("label.downloadSpeed")}
              </div>
              <SpeedDisplay bytesPerSec={speed} className="text-lg font-semibold text-foreground" />
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
            <Button variant="outline" onClick={() => void (isPaused ? resumeAll({}) : pauseAll({}))}>
              {isPaused ? t("action.resumeAll") : t("action.pauseAll")}
            </Button>
            <Button onClick={() => setUploadOpen(true)}>{t("nav.upload")}</Button>
          </>
        }
      />

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
                    <TableHead className="h-7 w-[36%] px-2 text-[9px]">{t("table.name")}</TableHead>
                    <TableHead className="h-7 w-[9%] px-2 text-[9px]">{t("table.status")}</TableHead>
                    <TableHead className="h-7 w-[7%] px-2 text-[9px]">{t("table.priority")}</TableHead>
                    <TableHead className="h-7 w-[8%] px-2 text-[9px]">{t("table.category")}</TableHead>
                    <TableHead className="h-7 w-[15%] px-2 text-[9px]">{t("table.progress")}</TableHead>
                    <TableHead className="h-7 w-[12%] px-2 text-right text-[9px]">{t("table.size")}</TableHead>
                    <TableHead className="h-7 w-[6%] px-2 text-right text-[9px]">{t("table.eta")}</TableHead>
                    <TableHead className="h-7 w-[7%] px-2 text-right text-[9px]">{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => {
                    const remaining = job.totalBytes - job.downloadedBytes;
                    const priority = getJobPriority(job);
                    const displayName = job.displayTitle;
                    const expanded = expandedJobIds.has(job.id);
                    const jobSpeed =
                      job.status === "DOWNLOADING" && totalRemaining > 0
                        ? (remaining / totalRemaining) * speed
                        : 0;
                    return (
                      <Fragment key={job.id}>
                        <TableRow key={job.id} className="text-[11px]">
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
                                className="block min-w-0 truncate text-[11px] font-medium leading-tight transition hover:text-primary"
                              >
                                {displayName}
                              </Link>
                              {job.hasPassword ? (
                                <span className="shrink-0 text-[9px] text-amber-500">{t("jobs.passwordProtected")}</span>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell className="overflow-hidden px-2 py-1.5">
                            <JobStatusBadge status={job.status} compact className="px-1.5" />
                          </TableCell>
                          <TableCell className="truncate px-2 py-1.5 text-[10px]" title={formatJobPriority(priority)}>
                            {formatJobPriority(priority)}
                          </TableCell>
                          <TableCell className="truncate px-2 py-1.5 text-[10px]" title={job.category ?? "\u2014"}>
                            {job.category ?? "\u2014"}
                          </TableCell>
                          <TableCell className="min-w-0 px-2 py-1.5" title={`${(job.progress * 100).toFixed(1)}%`}>
                            <JobProgress progress={job.progress} status={job.status} compact showLabel={false} />
                          </TableCell>
                          <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                            {formatBytes(job.downloadedBytes)} / {formatBytes(job.totalBytes)}
                          </TableCell>
                          <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                            {job.status === "DOWNLOADING"
                              ? getEta(job.id, remaining, jobSpeed)
                              : "\u2014"}
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
                            <TableCell colSpan={8} className="px-4 py-4">
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
              const remaining = job.totalBytes - job.downloadedBytes;
              const priority = getJobPriority(job);
              const displayName = job.displayTitle;
              const expanded = expandedJobIds.has(job.id);
              const jobSpeed =
                job.status === "DOWNLOADING" && totalRemaining > 0
                  ? (remaining / totalRemaining) * speed
                  : 0;
              return (
                <Card key={job.id}>
                  <CardContent className="space-y-3">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
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
                          <span>{job.status === "DOWNLOADING" ? getEta(job.id, remaining, jobSpeed) : "\u2014"}</span>
                        </div>
                      </div>
                      <JobStatusBadge status={job.status} compact />
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

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
    </div>
  );
}

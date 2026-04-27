import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { ChevronRight } from "lucide-react";
import { useMutation, useQuery, useSubscription } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { JobProgress } from "@/components/JobProgress";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { PageHeader } from "@/components/PageHeader";
import { PipelineTimelineCard } from "@/components/PipelineTimelineCard";
import type { JobTimelineData } from "@/components/PipelineTimelineCard";
import { ParsedReleaseDetails } from "@/components/ParsedReleaseDetails";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  CANCEL_JOB_MUTATION,
  DELETE_HISTORY_MUTATION,
  JOB_OUTPUT_FILES_QUERY,
  JOB_DETAIL_UPDATES_SUBSCRIPTION,
  JOB_QUERY,
  PAUSE_JOB_MUTATION,
  REDOWNLOAD_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  RESUME_JOB_MUTATION,
} from "@/graphql/queries";
import { requestGraphqlClientRestart } from "@/graphql/client";
import { useLiveData } from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import { cn } from "@/lib/utils";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { getDisplayedJobProgress } from "@/lib/job-progress";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";

interface JobDetailSnapshotData {
  queueItem?: GraphqlJobData | null;
  historyItem?: GraphqlJobData | null;
  jobTimeline?: JobTimelineData | null;
  jobEvents?: Array<{
    kind: string;
    jobId: number;
    fileId: string | null;
    message: string;
    timestamp: number;
  }>;
}

interface JobDetailQueryData {
  jobDetailSnapshot?: JobDetailSnapshotData | null;
}

export function JobDetail() {
  const t = useTranslate();
  const { id } = useParams();
  const jobId = Number(id);
  const navigate = useNavigate();
  const { jobs: liveJobs, speed, connection } = useLiveData();
  const queryVariables = useMemo(() => ({ id: jobId }), [jobId]);

  const [{ data, fetching }, reexecuteJobQuery] = useQuery<JobDetailQueryData>({
    query: JOB_QUERY,
    variables: queryVariables,
  });
  const [{ data: subscriptionData }] = useSubscription<{
    jobDetailUpdates: JobDetailSnapshotData;
  }>({
    query: JOB_DETAIL_UPDATES_SUBSCRIPTION,
    variables: queryVariables,
    pause: connection.isDisconnected || !Number.isFinite(jobId),
  });
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, reprocessJob] = useMutation(REPROCESS_JOB_MUTATION);
  const [, redownloadJob] = useMutation(REDOWNLOAD_JOB_MUTATION);
  const [, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);

  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [showRedownloadConfirm, setShowRedownloadConfirm] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [lastLiveJob, setLastLiveJob] = useState<JobData | null>(null);
  const [polledData, setPolledData] = useState<JobDetailQueryData | undefined>();
  const liveJob = liveJobs.find((candidate) => candidate.id === jobId) ?? null;
  const jobQueryData =
    polledData?.jobDetailSnapshot
    ?? subscriptionData?.jobDetailUpdates
    ?? data?.jobDetailSnapshot
    ?? null;
  const queryJob = useMemo(() => {
    const rawJob = jobQueryData?.queueItem ?? jobQueryData?.historyItem;
    return rawJob ? normalizeJobData(rawJob) : null;
  }, [jobQueryData?.historyItem, jobQueryData?.queueItem]);
  const events = useMemo(
    () =>
      (jobQueryData?.jobEvents ?? [])
        .map((event) => ({
          kind: event.kind,
          jobId: event.jobId,
          fileId: event.fileId,
          message: event.message,
          timestamp: event.timestamp,
        }))
        .reverse(),
    [jobQueryData?.jobEvents],
  );

  useEffect(() => {
    setLastLiveJob(null);
    setPolledData(undefined);
  }, [jobId]);

  useEffect(() => {
    if (!connection.isDisconnected) {
      setPolledData(undefined);
    }
  }, [connection.isDisconnected]);

  useEffect(() => {
    if (connection.status === "connected" && subscriptionData?.jobDetailUpdates) {
      setPolledData(undefined);
    }
  }, [connection.status, subscriptionData]);

  useEffect(() => {
    if (liveJob) {
      setLastLiveJob(liveJob);
    }
  }, [liveJob]);

  useReconnectPolling<JobDetailQueryData>({
    enabled: connection.isDisconnected && Number.isFinite(jobId),
    query: JOB_QUERY,
    variables: queryVariables,
    onData: (nextData) => {
      setPolledData(nextData);
      requestGraphqlClientRestart();
    },
  });

  const queryJobIsTerminal = queryJob?.status === "COMPLETE" || queryJob?.status === "FAILED";
  const job = liveJob ?? (queryJobIsTerminal ? queryJob : lastLiveJob ?? queryJob);
  const timeline = jobQueryData?.jobTimeline ?? null;
  const etaSpeed = useStableEtaSpeed(job ? [job] : [], speed);
  const eta = job
    ? formatEtaFromRemainingBytes(
        Math.max(job.totalBytes - job.downloadedBytes, 0),
        etaSpeed,
      )
    : "\u2014";
  const showEta = job?.status === "DOWNLOADING" || job?.status === "QUEUED";

  if (fetching && !job) {
    return <div className="text-muted-foreground">{t("label.loading")}</div>;
  }

  if (!job) {
    return (
      <Card>
        <CardContent className="space-y-3 py-8">
          <p className="text-muted-foreground">{t("job.notFound")}</p>
          <Button asChild variant="outline">
            <Link to="/">{t("action.backToJobs")}</Link>
          </Button>
        </CardContent>
      </Card>
    );
  }

  const optionalRecoveryBytes = job.optionalRecoveryBytes ?? 0;
  const optionalRecoveryDownloadedBytes = job.optionalRecoveryDownloadedBytes ?? 0;
  const savedBandwidthBytes = Math.max(
    optionalRecoveryBytes - optionalRecoveryDownloadedBytes,
    0,
  );
  const displayProgress = getDisplayedJobProgress({
    progress: job.progress,
    status: job.status,
    totalBytes: job.totalBytes,
    downloadedBytes: job.downloadedBytes,
    failedBytes: job.failedBytes,
  });
  const showSavedBandwidthTile = job.status === "COMPLETE" && optionalRecoveryBytes > 0;
  const showLegacySavedBandwidthNote =
    job.status === "COMPLETE"
    && job.downloadedBytes < job.totalBytes
    && optionalRecoveryBytes === 0
    && optionalRecoveryDownloadedBytes === 0;
  const savedBandwidthDetail =
    savedBandwidthBytes > 0
      ? t("job.savedBandwidthSkipped")
      : t("job.savedBandwidthUsedAll");

  return (
    <div className="space-y-6">
      <div>
        <Button asChild variant="ghost" size="sm" className="-ml-3">
          <Link to="/">{t("action.backToJobs")}</Link>
        </Button>
      </div>

      <PageHeader
        title={job.displayTitle}
        description={
          job.originalTitle !== job.displayTitle ? `Original NZB title: ${job.originalTitle}` : undefined
        }
        actions={
          <>
            {job.status === "PAUSED" ? (
              <Button onClick={() => void resumeJob({ id: job.id })}>{t("action.resume")}</Button>
            ) : job.status !== "COMPLETE" && job.status !== "FAILED" ? (
              <Button variant="outline" onClick={() => void pauseJob({ id: job.id })}>
                {t("action.pause")}
              </Button>
            ) : null}
            {job.status === "FAILED" ? (
              <>
                <Button
                  variant="outline"
                  onClick={() => {
                    void reprocessJob({ id: job.id }).then(() => {
                      void reexecuteJobQuery({ requestPolicy: "network-only" });
                    });
                  }}
                >
                  {t("action.reprocess")}
                </Button>
                <Button variant="outline" onClick={() => setShowRedownloadConfirm(true)}>
                  {t("action.redownload")}
                </Button>
              </>
            ) : null}
            {job.status !== "COMPLETE" && job.status !== "FAILED" ? (
              <Button variant="destructive" onClick={() => setShowCancelConfirm(true)}>
                {t("action.cancel")}
              </Button>
            ) : (
              <Button variant="destructive" onClick={() => setShowDeleteConfirm(true)}>
                {t("action.delete")}
              </Button>
            )}
          </>
        }
      />

      {/* Progress & stats */}
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center gap-3">
            <JobStatusBadge status={job.status} />
            {job.hasPassword ? (
              <span className="text-xs text-amber-500">{t("job.passwordProtected")}</span>
            ) : null}
          </div>
        </CardHeader>
        <CardContent className="space-y-5">
          {showEta ? (
            <div className="flex justify-end">
              <div className="text-right">
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  {t("table.eta")}
                </div>
                <div className="mt-1 text-2xl font-semibold tracking-tight text-foreground sm:text-3xl">
                  {eta}
                </div>
              </div>
            </div>
          ) : null}
          <JobProgress
            progress={job.progress}
            status={job.status}
            totalBytes={job.totalBytes}
            downloadedBytes={job.downloadedBytes}
            failedBytes={job.failedBytes}
          />
          <div
            className={`grid grid-cols-2 gap-4 ${showSavedBandwidthTile ? "sm:grid-cols-5" : "sm:grid-cols-4"}`}
          >
            <MetricTile label={t("label.downloaded")} value={formatBytes(job.downloadedBytes)} />
            <MetricTile label={t("label.totalSize")} value={formatBytes(job.totalBytes)} />
            <MetricTile label={t("label.progress")} value={`${(displayProgress * 100).toFixed(1)}%`} />
            <MetricTile
              label={t("table.health")}
              value={`${(job.health / 10).toFixed(1)}%`}
              className={healthColor(job.health)}
            />
            {showSavedBandwidthTile ? (
              <MetricTile
                label={t("label.savedBandwidth")}
                value={formatBytes(savedBandwidthBytes)}
                detail={savedBandwidthDetail}
                className={savedBandwidthBytes > 0 ? "text-emerald-600 dark:text-emerald-300" : undefined}
              />
            ) : null}
          </div>
          {showLegacySavedBandwidthNote ? (
            <p className="text-sm text-muted-foreground">{t("job.savedBandwidthLegacy")}</p>
          ) : null}
        </CardContent>
      </Card>

      {/* Output files */}
      <JobOutputFilesCard jobId={job.id} status={job.status} />

      {/* Timeline */}
      <PipelineTimelineCard timeline={timeline} />

      {/* Release details (collapsed) */}
      <CollapsibleCard title="Release Details">
        <ParsedReleaseDetails
          originalTitle={job.originalTitle}
          parsedRelease={job.parsedRelease}
          category={job.category}
        />
      </CollapsibleCard>

      {/* Metadata (collapsed) */}
      <CollapsibleCard title={t("job.metadata")} noPadding hidden={job.metadata.length === 0}>
        <Table>
          <TableBody>
            {job.metadata.map((entry: { key: string; value: string }) => (
              <TableRow key={entry.key}>
                <TableCell className="w-1/3 text-xs font-medium text-muted-foreground">
                  {formatMetadataKey(entry.key)}
                </TableCell>
                <TableCell className="text-sm">{entry.value}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CollapsibleCard>

      {/* Event log (collapsed) */}
      <CollapsibleCard title={t("job.eventLog")} noPadding>
        {events.length === 0 ? (
          <div className="px-6 pb-6 text-sm text-muted-foreground">{t("job.waitingForEvents")}</div>
        ) : (
          <div className="max-h-[150lh] overflow-y-auto">
            <div className="hidden sm:block">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>{t("table.time")}</TableHead>
                    <TableHead>{t("table.kind")}</TableHead>
                    <TableHead>{t("table.message")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {events.map((event, index) => (
                    <TableRow key={`${event.timestamp}-${index}`}>
                      <TableCell className="font-mono text-xs text-muted-foreground">
                        {new Date(event.timestamp).toLocaleTimeString()}
                      </TableCell>
                      <TableCell className="text-xs">{event.kind}</TableCell>
                      <TableCell>{event.message}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            <div className="divide-y divide-border/50 sm:hidden">
              {events.map((event, index) => (
                <div key={`${event.timestamp}-${index}`} className="px-6 py-3">
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span className="font-mono">{new Date(event.timestamp).toLocaleTimeString()}</span>
                    <span className="text-foreground">{event.kind}</span>
                  </div>
                  <div className="mt-1 text-sm text-foreground">{event.message}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </CollapsibleCard>

      <ConfirmDialog
        open={showCancelConfirm}
        title={t("confirm.cancelJob")}
        message={t("confirm.cancelJobMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => {
          void cancelJob({ id: job.id }).then(() => navigate("/"));
          setShowCancelConfirm(false);
        }}
        onCancel={() => setShowCancelConfirm(false)}
      />

      <ConfirmDialog
        open={showRedownloadConfirm}
        title={t("confirm.redownloadJob")}
        message={t("confirm.redownloadJobMessage")}
        confirmLabel={t("confirm.redownloadJobConfirm")}
        cancelLabel={t("confirm.redownloadJobDismiss")}
        onConfirm={() => {
          void redownloadJob({ id: job.id }).then(() => {
            void reexecuteJobQuery({ requestPolicy: "network-only" });
          });
          setShowRedownloadConfirm(false);
        }}
        onCancel={() => setShowRedownloadConfirm(false)}
      />

      <ConfirmDialog
        open={showDeleteConfirm}
        title={t("confirm.deleteHistory")}
        message={t("confirm.deleteHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => {
          void deleteHistory({ id: job.id, deleteFiles }).then(() => navigate("/history"));
          setShowDeleteConfirm(false);
          setDeleteFiles(false);
        }}
        onCancel={() => { setShowDeleteConfirm(false); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>
    </div>
  );
}

function MetricTile({
  label,
  value,
  detail,
  className,
}: {
  label: string;
  value: string;
  detail?: string;
  className?: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">{label}</div>
      <div className={`mt-2 text-lg font-semibold ${className ?? "text-foreground"}`}>{value}</div>
      {detail ? <div className="mt-1 text-xs text-muted-foreground">{detail}</div> : null}
    </div>
  );
}

function healthColor(health: number): string {
  if (health >= 980) return "text-emerald-600 dark:text-emerald-300";
  if (health >= 950) return "text-amber-600 dark:text-amber-300";
  return "text-red-600 dark:text-red-300";
}

function CollapsibleCard({
  title,
  noPadding,
  hidden,
  children,
}: {
  title: string;
  noPadding?: boolean;
  hidden?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  if (hidden) return null;
  return (
    <Card>
      <CardHeader>
        <button
          type="button"
          className="flex w-full items-center gap-2 text-left"
          onClick={() => setOpen((v) => !v)}
        >
          <ChevronRight
            className={cn(
              "size-4 text-muted-foreground transition-transform",
              open && "rotate-90",
            )}
          />
          <CardTitle>{title}</CardTitle>
        </button>
      </CardHeader>
      {open ? (
        <CardContent className={noPadding ? "px-0 pb-0" : undefined}>
          {children}
        </CardContent>
      ) : null}
    </Card>
  );
}

/** Strip vendor prefixes (e.g. `*scryer_`) and humanize key names. */
function formatMetadataKey(key: string): string {
  const stripped = key.replace(/^\*\w+_/, "");
  return stripped.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

type OutputFile = { name: string; path: string; sizeBytes: number };
type OutputResult = { outputDir: string; files: OutputFile[]; totalBytes: number };

function JobOutputFilesCard({ jobId, status }: { jobId: number; status: string }) {
  const t = useTranslate();
  const isTerminal = status === "COMPLETE" || status === "FAILED";
  const [{ data, fetching }] = useQuery<{ jobOutputFiles: OutputResult | null }>({
    query: JOB_OUTPUT_FILES_QUERY,
    variables: { jobId },
    pause: !isTerminal,
  });

  if (!isTerminal) return null;

  const result = data?.jobOutputFiles;

  if (fetching) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Output Files</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground">{t("label.loading")}</div>
        </CardContent>
      </Card>
    );
  }

  if (!result || result.files.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Output Files</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground">
            No output files found
            {result?.outputDir ? (
              <span className="ml-1 font-mono text-xs">({result.outputDir})</span>
            ) : null}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Output Files</CardTitle>
          <span className="text-xs text-muted-foreground">
            {result.files.length} file{result.files.length !== 1 ? "s" : ""} &middot; {formatBytes(result.totalBytes)}
          </span>
        </div>
        <div className="font-mono text-xs text-muted-foreground">{result.outputDir}</div>
      </CardHeader>
      <CardContent className="px-0 pb-0">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="text-xs">Name</TableHead>
              <TableHead className="w-[120px] text-right text-xs">Size</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.files.map((file) => (
              <TableRow key={file.path}>
                <TableCell className="font-mono text-xs">{file.name}</TableCell>
                <TableCell className="text-right text-xs text-muted-foreground">
                  {formatBytes(file.sizeBytes)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

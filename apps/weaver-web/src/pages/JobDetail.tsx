import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { ChevronRight, Download } from "lucide-react";
import { useMutation, useQuery, useSubscription } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import {
  DuplicateSnapshotPanel,
  type DuplicateSnapshot,
} from "@/features/duplicates/DuplicateSnapshotPanel";
import { JobProgress } from "@/components/JobProgress";
import { JobStatusBadgeGroup } from "@/components/JobStatusBadge";
import { getJobStages } from "@/lib/job-stages";
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
  ACCEPT_HISTORY_DELETE_MUTATION,
  CANCEL_JOB_MUTATION,
  DUPLICATE_SNAPSHOT_QUERY,
  FORGET_DUPLICATE_IDENTITY_MUTATION,
  JOB_OUTPUT_FILES_QUERY,
  JOB_DETAIL_UPDATES_SUBSCRIPTION,
  JOB_QUERY,
  MARK_DUPLICATE_BAD_MUTATION,
  MARK_DUPLICATE_GOOD_MUTATION,
  PAUSE_JOB_MUTATION,
  PROMOTE_DUPLICATE_CANDIDATE_MUTATION,
  REDOWNLOAD_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  RESUME_JOB_MUTATION,
} from "@/graphql/queries";
import { authHeaders, requestGraphqlClientRestart } from "@/graphql/client";
import {
  useLiveConnection,
  useLiveJob,
  useLiveSpeed,
} from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { formatEtaFromRemainingBytes, useStableEtaSpeed } from "@/lib/hooks/use-stable-queue-eta";
import { cn } from "@/lib/utils";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";
import { getDisplayedJobProgress } from "@/lib/job-progress";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";
import {
  readDownloadErrorMessage,
  saveResponseAsDownload,
  submitFormAsDownload,
} from "@/lib/download";

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

interface DuplicateSnapshotQueryData {
  duplicateSnapshot?: DuplicateSnapshot | null;
}

type DuplicateAction = "good" | "bad" | "promote" | "forget";

export function JobDetail() {
  const t = useTranslate();
  const { id } = useParams();
  const jobId = Number(id);
  const navigate = useNavigate();
  const liveJob = useLiveJob(jobId);
  const speed = useLiveSpeed();
  const connection = useLiveConnection();
  const queryVariables = useMemo(() => ({ id: jobId }), [jobId]);

  const [{ data, fetching }, reexecuteJobQuery] = useQuery<JobDetailQueryData>({
    query: JOB_QUERY,
    variables: queryVariables,
  });
  const [{ data: duplicateData }, reexecuteDuplicateSnapshot] = useQuery<DuplicateSnapshotQueryData>({
    query: DUPLICATE_SNAPSHOT_QUERY,
    variables: queryVariables,
    pause: !Number.isFinite(jobId),
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
  const [acceptDeleteState, acceptHistoryDelete] = useMutation(ACCEPT_HISTORY_DELETE_MUTATION);
  const [markDuplicateGoodState, markDuplicateGood] = useMutation(MARK_DUPLICATE_GOOD_MUTATION);
  const [markDuplicateBadState, markDuplicateBad] = useMutation(MARK_DUPLICATE_BAD_MUTATION);
  const [promoteDuplicateState, promoteDuplicate] = useMutation(PROMOTE_DUPLICATE_CANDIDATE_MUTATION);
  const [forgetDuplicateState, forgetDuplicateIdentity] = useMutation(FORGET_DUPLICATE_IDENTITY_MUTATION);

  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [showRedownloadConfirm, setShowRedownloadConfirm] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [deleteAcceptError, setDeleteAcceptError] = useState<string | null>(null);
  const [lastLiveJob, setLastLiveJob] = useState<JobData | null>(null);
  const [polledData, setPolledData] = useState<JobDetailQueryData | undefined>();
  const [isDownloadingNzb, setIsDownloadingNzb] = useState(false);
  const [nzbDownloadError, setNzbDownloadError] = useState<string | null>(null);
  const [duplicateAction, setDuplicateAction] = useState<DuplicateAction | null>(null);
  const [duplicateActionError, setDuplicateActionError] = useState<string | null>(null);
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
    setNzbDownloadError(null);
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
  const canRestart = job.status === "COMPLETE" || job.status === "FAILED";
  const deleteLocked = Boolean(job.deleteOperation?.locked);
  const savedBandwidthDetail =
    savedBandwidthBytes > 0
      ? t("job.savedBandwidthSkipped")
      : t("job.savedBandwidthUsedAll");
  const originalNzbTitle = job.originalTitle.trim() || job.name;
  const defaultNzbFilename = `${originalNzbTitle || job.displayTitle}.nzb`;
  const jobNzbDownloadHref = new URL(`api/jobs/${job.id}/nzb`, document.baseURI).href;
  const duplicateSnapshot = duplicateData?.duplicateSnapshot ?? null;
  const duplicateActionBusy =
    markDuplicateGoodState.fetching
    || markDuplicateBadState.fetching
    || promoteDuplicateState.fetching
    || forgetDuplicateState.fetching;
  const duplicateConfirm = duplicateAction
    ? {
        good: {
          title: "confirm.duplicateGood",
          message: "confirm.duplicateGoodMessage",
          confirm: "confirm.duplicateGoodConfirm",
        },
        bad: {
          title: "confirm.duplicateBad",
          message: "confirm.duplicateBadMessage",
          confirm: "confirm.duplicateBadConfirm",
        },
        promote: {
          title: "confirm.duplicatePromote",
          message: "confirm.duplicatePromoteMessage",
          confirm: "confirm.duplicatePromoteConfirm",
        },
        forget: {
          title: "confirm.duplicateForget",
          message: "confirm.duplicateForgetMessage",
          confirm: "confirm.duplicateForgetConfirm",
        },
      }[duplicateAction]
    : null;

  async function runDuplicateAction() {
    if (!duplicateAction) {
      return;
    }
    setDuplicateActionError(null);
    let accepted = false;
    let message: string | null | undefined;

    if (duplicateAction === "good") {
      const result = await markDuplicateGood({ id: job.id });
      accepted = result.data?.markDuplicateGood === true;
      message = result.error?.message;
    } else if (duplicateAction === "bad") {
      const result = await markDuplicateBad({ id: job.id });
      accepted = result.data?.markDuplicateBad?.accepted === true;
      message = result.error?.message ?? result.data?.markDuplicateBad?.message;
    } else if (duplicateAction === "promote") {
      const result = await promoteDuplicate({ id: job.id });
      accepted = result.data?.promoteDuplicateCandidate?.accepted === true;
      message = result.error?.message ?? result.data?.promoteDuplicateCandidate?.message;
    } else {
      const result = await forgetDuplicateIdentity({ id: job.id });
      accepted = result.data?.forgetDuplicateIdentity === true;
      message = result.error?.message;
    }

    if (!accepted) {
      setDuplicateActionError(message ?? t("duplicate.actionFailed"));
      return;
    }
    setDuplicateAction(null);
    void reexecuteDuplicateSnapshot({ requestPolicy: "network-only" });
    void reexecuteJobQuery({ requestPolicy: "network-only" });
  }

  async function downloadNzb() {
    setIsDownloadingNzb(true);
    setNzbDownloadError(null);

    try {
      const response = await fetch(jobNzbDownloadHref, {
        headers: authHeaders(),
        credentials: "same-origin",
      });
      if (!response.ok) {
        throw new Error(await readDownloadErrorMessage(response, "Failed to download NZB."));
      }

      await saveResponseAsDownload(response, defaultNzbFilename);
    } catch (error) {
      setNzbDownloadError(error instanceof Error ? error.message : "Failed to download NZB.");
    } finally {
      setIsDownloadingNzb(false);
    }
  }

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
          <div className="space-y-1.5">
            <div className="flex min-w-0 items-center gap-1.5">
              <span className="truncate">{originalNzbTitle}</span>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-8 shrink-0 text-muted-foreground hover:text-foreground"
                onClick={() => void downloadNzb()}
                disabled={isDownloadingNzb}
                aria-label={`Download NZB for ${originalNzbTitle}`}
                title={isDownloadingNzb ? "Preparing NZB..." : "Download NZB"}
              >
                <Download className="size-4" />
              </Button>
            </div>
            {nzbDownloadError ? (
              <p className="text-xs text-destructive">{nzbDownloadError}</p>
            ) : null}
          </div>
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
            {canRestart ? (
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
              <Button
                variant="destructive"
                disabled={deleteLocked || acceptDeleteState.fetching}
                onClick={() => {
                  setDeleteAcceptError(null);
                  setShowDeleteConfirm(true);
                }}
              >
                {t("action.delete")}
              </Button>
            )}
          </>
        }
      />

      {deleteLocked ? (
        <Card className="border-status-paused/30 bg-status-paused/5">
          <CardContent className="py-4 text-sm text-status-paused">
            This history item is already being deleted in the background. It will disappear when the delete completes.
          </CardContent>
        </Card>
      ) : null}

      {/* Progress & stats */}
      <Card>
        <CardHeader>
          <div className="flex items-start justify-between gap-4">
            <div className="flex min-w-0 flex-1 flex-wrap items-center gap-3">
              <JobStatusBadgeGroup statuses={getJobStages(job)} />
              {job.hasPassword ? (
                <span className="text-xs text-status-paused">{t("job.passwordProtected")}</span>
              ) : null}
            </div>
            {showEta ? (
              <div className="min-w-[7.5rem] shrink-0 text-right tabular-nums sm:min-w-[8.5rem]">
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  {t("table.eta")}
                </div>
                <div className="mt-1 text-2xl font-semibold tracking-tight text-foreground tabular-nums sm:text-3xl">
                  {eta}
                </div>
              </div>
            ) : null}
          </div>
        </CardHeader>
        <CardContent className="space-y-5">
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
                className={savedBandwidthBytes > 0 ? "text-status-completed" : undefined}
              />
            ) : null}
          </div>
          {showLegacySavedBandwidthNote ? (
            <p className="text-sm text-muted-foreground">{t("job.savedBandwidthLegacy")}</p>
          ) : null}
        </CardContent>
      </Card>

      {duplicateSnapshot ? (
        <DuplicateSnapshotPanel
          snapshot={duplicateSnapshot}
          busy={duplicateActionBusy}
          error={duplicateActionError}
          onMarkGood={() => setDuplicateAction("good")}
          onMarkBad={() => setDuplicateAction("bad")}
          onPromote={() => setDuplicateAction("promote")}
          onForget={() => setDuplicateAction("forget")}
        />
      ) : null}

      {/* Output files */}
      <JobOutputFilesCard jobId={job.id} status={job.status} />

      {/* Timeline */}
      <PipelineTimelineCard timeline={timeline} />

      {/* Release details */}
      <CollapsibleCard title="Release Details" defaultOpen>
        <ParsedReleaseDetails
          originalTitle={job.originalTitle}
          parsedRelease={job.parsedRelease}
          category={job.category}
        />
      </CollapsibleCard>

      {/* Metadata */}
      <CollapsibleCard title={t("job.metadata")} defaultOpen noPadding hidden={job.metadata.length === 0}>
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
        open={duplicateAction !== null}
        title={t(duplicateConfirm?.title ?? "duplicate.panelTitle")}
        message={t(duplicateConfirm?.message ?? "duplicate.actionFailed")}
        confirmLabel={t(duplicateConfirm?.confirm ?? "action.cancel")}
        cancelLabel={t("action.cancel")}
        confirmDisabled={duplicateActionBusy}
        cancelDisabled={duplicateActionBusy}
        onConfirm={() => void runDuplicateAction()}
        onCancel={() => {
          setDuplicateActionError(null);
          setDuplicateAction(null);
        }}
      >
        {duplicateActionError ? <p className="text-sm text-destructive">{duplicateActionError}</p> : null}
      </ConfirmDialog>

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
        confirmDisabled={acceptDeleteState.fetching}
        cancelDisabled={acceptDeleteState.fetching}
        onConfirm={() => {
          void (async () => {
            setDeleteAcceptError(null);
            const result = await acceptHistoryDelete({
              input: {
                mode: "IDS",
                ids: [job.id],
                deleteFiles,
              },
            });
            if (result.error) {
              setDeleteAcceptError(result.error.message);
              return;
            }

            setShowDeleteConfirm(false);
            setDeleteFiles(false);
            navigate("/history");
          })();
        }}
        onCancel={() => {
          setDeleteAcceptError(null);
          setShowDeleteConfirm(false);
          setDeleteFiles(false);
        }}
      >
        <label className="flex items-center gap-2">
          <Checkbox
            checked={deleteFiles}
            disabled={acceptDeleteState.fetching}
            onCheckedChange={(v) => setDeleteFiles(v === true)}
          />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
        {deleteAcceptError ? (
          <p className="text-sm text-destructive">{deleteAcceptError}</p>
        ) : null}
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
    <div className="rounded-inner border border-border bg-background/40 p-4">
      <div className="text-[10.5px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
        {label}
      </div>
      <div
        className={cn(
          "mt-2 font-space-grotesk text-[22px] font-bold leading-none tracking-tight tabular-nums",
          className ?? "text-foreground",
        )}
      >
        {value}
      </div>
      {detail ? <div className="mt-1.5 text-xs text-muted-foreground">{detail}</div> : null}
    </div>
  );
}

function healthColor(health: number): string {
  if (health >= 980) return "text-status-completed";
  if (health >= 950) return "text-status-paused";
  return "text-status-failed";
}

function CollapsibleCard({
  title,
  noPadding,
  hidden,
  defaultOpen = false,
  children,
}: {
  title: string;
  noPadding?: boolean;
  hidden?: boolean;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
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
  const [downloadingPath, setDownloadingPath] = useState<string | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [filesOpenOverride, setFilesOpenOverride] = useState<boolean | null>(null);
  const [{ data, fetching }] = useQuery<{ jobOutputFiles: OutputResult | null }>({
    query: JOB_OUTPUT_FILES_QUERY,
    variables: { jobId },
    pause: !isTerminal,
  });

  useEffect(() => {
    setFilesOpenOverride(null);
  }, [jobId]);

  if (!isTerminal) return null;

  const result = data?.jobOutputFiles;
  const outputFileDownloadHref = new URL(`api/jobs/${jobId}/output-file`, document.baseURI).href;

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

  async function downloadOutputFile(file: OutputFile) {
    setDownloadingPath(file.path);
    setDownloadError(null);

    submitFormAsDownload(outputFileDownloadHref, { path: file.path });
    window.setTimeout(() => {
      setDownloadingPath((current) => (current === file.path ? null : current));
    }, 1000);
  }

  const filesOpen = filesOpenOverride ?? (result.files.length <= 10);
  const outputFilesRegionId = `job-${jobId}-output-files`;
  const outputFileCountLabel = `${result.files.length} file${result.files.length !== 1 ? "s" : ""}`;

  return (
    <Card>
      <CardHeader>
        <button
          type="button"
          className="flex w-full items-start justify-between gap-3 text-left"
          aria-expanded={filesOpen}
          aria-controls={outputFilesRegionId}
          title={filesOpen ? "Collapse output files" : "Expand output files"}
          onClick={() => setFilesOpenOverride(!filesOpen)}
        >
          <div className="flex min-w-0 items-center gap-2">
            <ChevronRight
              className={cn(
                "size-4 shrink-0 text-muted-foreground transition-transform",
                filesOpen && "rotate-90",
              )}
            />
            <CardTitle>Output Files</CardTitle>
          </div>
          <span className="text-xs text-muted-foreground">
            {outputFileCountLabel} &middot; {formatBytes(result.totalBytes)}
          </span>
        </button>
        <div className="font-mono text-xs text-muted-foreground">{result.outputDir}</div>
        {downloadError ? <p className="text-sm text-destructive">{downloadError}</p> : null}
      </CardHeader>
      {filesOpen ? (
        <CardContent id={outputFilesRegionId} className="px-0 pb-0">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="text-xs">Name</TableHead>
                <TableHead className="w-[120px] text-right text-xs">Size</TableHead>
                <TableHead className="w-[52px] text-right text-xs">&nbsp;</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {result.files.map((file) => (
                <TableRow key={file.path}>
                  <TableCell className="font-mono text-xs">{file.name}</TableCell>
                  <TableCell className="text-right text-xs text-muted-foreground">
                    {formatBytes(file.sizeBytes)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="size-8 text-muted-foreground hover:text-foreground"
                      title={downloadingPath === file.path ? "Preparing download..." : `Download ${file.name}`}
                      aria-label={`Download ${file.name}`}
                      disabled={downloadingPath === file.path}
                      onClick={() => {
                        void downloadOutputFile(file);
                      }}
                    >
                      <Download className="size-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      ) : null}
    </Card>
  );
}

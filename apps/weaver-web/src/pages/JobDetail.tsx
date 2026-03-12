import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useMutation, useQuery, useSubscription } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { JobProgress } from "@/components/JobProgress";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { PageHeader } from "@/components/PageHeader";
import { PipelineTimelineCard } from "@/components/PipelineTimelineCard";
import { ParsedReleaseDetails } from "@/components/ParsedReleaseDetails";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
  EVENTS_SUBSCRIPTION,
  JOB_QUERY,
  PAUSE_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  RESUME_JOB_MUTATION,
} from "@/graphql/queries";
import { requestGraphqlClientRestart } from "@/graphql/client";
import { useLiveData } from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { useReconnectPolling } from "@/lib/hooks/use-reconnect-polling";

interface EventEntry {
  kind: string;
  jobId: number | null;
  fileId: string | null;
  message: string;
  timestamp: number;
}

export function JobDetail() {
  const t = useTranslate();
  const { id } = useParams();
  const jobId = Number(id);
  const navigate = useNavigate();
  const { jobs: liveJobs, connection } = useLiveData();
  const queryVariables = useMemo(() => ({ id: jobId }), [jobId]);

  const [{ data, fetching }, reexecuteJobQuery] = useQuery({
    query: JOB_QUERY,
    variables: queryVariables,
  });
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, reprocessJob] = useMutation(REPROCESS_JOB_MUTATION);
  const [, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);

  const [events, setEvents] = useState<EventEntry[]>([]);
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [lastLiveJob, setLastLiveJob] = useState<(typeof liveJobs)[number] | null>(null);
  const [polledData, setPolledData] = useState<typeof data>();
  const seededRef = useRef(false);
  const lastTimelineRefreshRef = useRef(0);
  const liveJob = liveJobs.find((candidate) => candidate.id === jobId) ?? null;
  const jobQueryData = polledData ?? data;

  useEffect(() => {
    setLastLiveJob(null);
    seededRef.current = false;
    setEvents([]);
    setPolledData(undefined);
  }, [jobId]);

  useEffect(() => {
    if (!connection.isDisconnected) {
      setPolledData(undefined);
    }
  }, [connection.isDisconnected]);

  useEffect(() => {
    if (liveJob) {
      setLastLiveJob(liveJob);
    }
  }, [liveJob]);

  useEffect(() => {
    if (jobQueryData?.job?.id !== jobId || !jobQueryData.jobEvents) {
      return;
    }

    if (!seededRef.current || connection.isDisconnected) {
      seededRef.current = true;
      setEvents(
        jobQueryData.jobEvents
          .map(
            (event: {
              kind: string;
              jobId: number;
              fileId: string | null;
              message: string;
              timestamp: number;
            }) => ({
              kind: event.kind,
              jobId: event.jobId,
              fileId: event.fileId,
              message: event.message,
              timestamp: event.timestamp,
            }),
          )
          .reverse(),
      );
    }
  }, [connection.isDisconnected, jobId, jobQueryData?.job?.id, jobQueryData?.jobEvents]);

  const handleSubscription = useCallback(
    (
      _prev: EventEntry[] | undefined,
      result: {
        events: { kind: string; jobId: number | null; fileId: string | null; message: string };
      },
    ) => {
      const event = result.events;
      const noisy = new Set([
        "ARTICLE_DOWNLOADED",
        "ARTICLE_NOT_FOUND",
        "SEGMENT_QUEUED",
        "SEGMENT_DECODED",
        "SEGMENT_DECODE_FAILED",
        "SEGMENT_COMMITTED",
        "SEGMENT_RETRY_SCHEDULED",
        "SEGMENT_FAILED_PERMANENT",
        "FILE_COMPLETE",
        "FILE_CLASSIFIED",
        "VERIFICATION_STARTED",
        "VERIFICATION_COMPLETE",
        "EXTRACTION_PROGRESS",
        "REPAIR_CONFIDENCE_UPDATED",
      ]);
      const timelineRelevant = new Set([
        "JOB_PAUSED",
        "JOB_RESUMED",
        "DOWNLOAD_STARTED",
        "DOWNLOAD_FINISHED",
        "JOB_VERIFICATION_STARTED",
        "JOB_VERIFICATION_COMPLETE",
        "REPAIR_STARTED",
        "REPAIR_COMPLETE",
        "REPAIR_FAILED",
        "EXTRACTION_MEMBER_STARTED",
        "EXTRACTION_MEMBER_WAITING_STARTED",
        "EXTRACTION_MEMBER_WAITING_FINISHED",
        "EXTRACTION_MEMBER_APPEND_STARTED",
        "EXTRACTION_MEMBER_APPEND_FINISHED",
        "EXTRACTION_MEMBER_FINISHED",
        "EXTRACTION_MEMBER_FAILED",
        "MOVE_TO_COMPLETE_STARTED",
        "MOVE_TO_COMPLETE_FINISHED",
        "JOB_COMPLETED",
        "JOB_FAILED",
      ]);

      if ((event.jobId === jobId || event.jobId === null) && !noisy.has(event.kind)) {
        setEvents((current) => [{ ...event, timestamp: Date.now() }, ...current].slice(0, 500));
        if (event.jobId === jobId && timelineRelevant.has(event.kind)) {
          const now = Date.now();
          if (now - lastTimelineRefreshRef.current > 250) {
            lastTimelineRefreshRef.current = now;
            void reexecuteJobQuery({ requestPolicy: "network-only" });
          }
        }
      }

      return [];
    },
    [jobId, reexecuteJobQuery],
  );

  useSubscription({ query: EVENTS_SUBSCRIPTION }, handleSubscription);

  useReconnectPolling({
    enabled: connection.isDisconnected && Number.isFinite(jobId),
    query: JOB_QUERY,
    variables: queryVariables,
    onData: (nextData) => {
      setPolledData(nextData);
      requestGraphqlClientRestart();
    },
  });

  const queryJob = jobQueryData?.job?.id === jobId ? jobQueryData.job : null;
  const job = liveJob ?? lastLiveJob ?? queryJob;
  const timeline = jobQueryData?.jobTimeline ?? null;

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
  const displayProgress = job.status === "COMPLETE" ? 1 : job.progress;
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
              <Button variant="outline" onClick={() => void reprocessJob({ id: job.id })}>
                {t("action.reprocess")}
              </Button>
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
          <JobProgress progress={displayProgress} status={job.status} />
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

      <Card>
        <CardHeader>
          <CardTitle>Release Details</CardTitle>
        </CardHeader>
        <CardContent>
          <ParsedReleaseDetails
            originalTitle={job.originalTitle}
            parsedRelease={job.parsedRelease}
          />
        </CardContent>
      </Card>

      <PipelineTimelineCard timeline={timeline} />

      <Card>
        <CardHeader>
          <CardTitle>{t("job.eventLog")}</CardTitle>
        </CardHeader>
        <CardContent className="px-0 pb-0">
          {events.length === 0 ? (
            <div className="px-6 pb-6 text-sm text-muted-foreground">{t("job.waitingForEvents")}</div>
          ) : (
            <>
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
            </>
          )}
        </CardContent>
      </Card>

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
        open={showDeleteConfirm}
        title={t("confirm.deleteHistory")}
        message={t("confirm.deleteHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => {
          void deleteHistory({ id: job.id }).then(() => navigate("/history"));
          setShowDeleteConfirm(false);
        }}
        onCancel={() => setShowDeleteConfirm(false)}
      />
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

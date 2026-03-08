import { useParams, Link } from "react-router";
import { useQuery, useMutation, useSubscription } from "urql";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  JOB_QUERY,
  PAUSE_JOB_MUTATION,
  RESUME_JOB_MUTATION,
  CANCEL_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  EVENTS_SUBSCRIPTION,
} from "@/graphql/queries";
import { ProgressBar } from "@/components/ProgressBar";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { formatBytes } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";

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

  const [{ data, fetching }] = useQuery({
    query: JOB_QUERY,
    variables: { id: jobId },
    requestPolicy: "network-only",
  });

  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, reprocessJob] = useMutation(REPROCESS_JOB_MUTATION);

  const [events, setEvents] = useState<EventEntry[]>([]);
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const seededRef = useRef(false);

  // Seed events from historical query data when it first loads.
  useEffect(() => {
    if (data?.jobEvents && !seededRef.current) {
      seededRef.current = true;
      const historical: EventEntry[] = data.jobEvents.map(
        (e: { kind: string; jobId: number; fileId: string | null; message: string; timestamp: number }) => ({
          kind: e.kind,
          jobId: e.jobId,
          fileId: e.fileId,
          message: e.message,
          timestamp: e.timestamp,
        }),
      );
      // Historical events are oldest-first from the DB; reverse for newest-first display.
      setEvents(historical.reverse());
    }
  }, [data?.jobEvents]);

  const handleSubscription = useCallback(
    (_prev: EventEntry[] | undefined, data: { events: { kind: string; jobId: number | null; fileId: string | null; message: string } }) => {
      const event = data.events;
      // Only show meaningful job-level events, not per-segment noise.
      const noisy = new Set([
        "ARTICLE_DOWNLOADED",
        "SEGMENT_DECODED",
        "SEGMENT_COMMITTED",
      ]);
      if (
        (event.jobId === jobId || event.jobId === null) &&
        !noisy.has(event.kind)
      ) {
        const entry: EventEntry = { ...event, timestamp: Date.now() };
        setEvents((prev) => [entry, ...prev].slice(0, 500));
      }
      return [];
    },
    [jobId],
  );

  useSubscription({ query: EVENTS_SUBSCRIPTION }, handleSubscription);

  const job = data?.job;

  if (fetching && !job) {
    return <div className="p-6 text-muted-foreground">{t("label.loading")}</div>;
  }

  if (!job) {
    return (
      <div className="p-6">
        <p className="text-muted-foreground">{t("job.notFound")}</p>
        <Link to="/" className="mt-2 inline-block text-primary hover:underline">
          {t("action.backToJobs")}
        </Link>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-6">
      <div className="mb-4 sm:mb-6">
        <Link to="/" className="text-sm text-muted-foreground hover:text-foreground">
          {t("action.backToJobs")}
        </Link>
      </div>

      <div className="mb-4 flex flex-col gap-3 sm:mb-6 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <h1 className="truncate text-xl font-bold text-foreground sm:text-2xl">{job.name}</h1>
          <div className="mt-1 flex items-center gap-3">
            <span
              className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${statusColor(job.status)}`}
            >
              {job.status}
            </span>
            {job.hasPassword && (
              <span className="text-xs text-yellow-500">{t("job.passwordProtected")}</span>
            )}
          </div>
        </div>
        <div className="flex gap-2">
          {job.status === "PAUSED" ? (
            <button
              onClick={() => resumeJob({ id: job.id })}
              className="rounded-md bg-green-600 px-3 py-2 text-sm font-medium text-white hover:bg-green-500 sm:px-4"
            >
              {t("action.resume")}
            </button>
          ) : job.status !== "COMPLETE" && job.status !== "FAILED" ? (
            <button
              onClick={() => pauseJob({ id: job.id })}
              className="rounded-md bg-yellow-600 px-3 py-2 text-sm font-medium text-white hover:bg-yellow-500 sm:px-4"
            >
              {t("action.pause")}
            </button>
          ) : null}
          {job.status === "FAILED" && (
            <button
              onClick={() => reprocessJob({ id: job.id })}
              className="rounded-md bg-blue-600 px-3 py-2 text-sm font-medium text-white hover:bg-blue-500 sm:px-4"
            >
              {t("action.reprocess")}
            </button>
          )}
          {job.status !== "COMPLETE" && (
            <button
              onClick={() => setShowCancelConfirm(true)}
              className="rounded-md bg-destructive px-3 py-2 text-sm font-medium text-destructive-foreground hover:bg-destructive/90 sm:px-4"
            >
              {t("action.cancel")}
            </button>
          )}
        </div>
      </div>

      <div className="mb-4 rounded-lg border border-border bg-card p-4 sm:mb-6 sm:p-5">
        <div className="mb-4">
          <ProgressBar progress={job.progress} status={job.status} />
        </div>
        <div className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
          <div>
            <div className="text-muted-foreground">{t("label.downloaded")}</div>
            <div className="text-foreground">{formatBytes(job.downloadedBytes)}</div>
          </div>
          <div>
            <div className="text-muted-foreground">{t("label.totalSize")}</div>
            <div className="text-foreground">{formatBytes(job.totalBytes)}</div>
          </div>
          <div>
            <div className="text-muted-foreground">{t("label.progress")}</div>
            <div className="text-foreground">{(job.progress * 100).toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-muted-foreground">Health</div>
            <div className={healthColor(job.health)}>{(job.health / 10).toFixed(1)}%</div>
          </div>
        </div>
      </div>

      <div>
        <h2 className="mb-3 text-lg font-semibold text-foreground">{t("job.eventLog")}</h2>
        <div className="max-h-96 overflow-auto rounded-lg border border-border bg-card">
          {events.length === 0 ? (
            <div className="p-4 text-center text-sm text-muted-foreground">
              {t("job.waitingForEvents")}
            </div>
          ) : (
            <>
              {/* Desktop event table */}
              <table className="hidden w-full text-sm sm:table">
                <thead>
                  <tr className="border-b border-border text-left text-xs text-muted-foreground">
                    <th className="px-4 py-2">{t("table.time")}</th>
                    <th className="px-4 py-2">{t("table.kind")}</th>
                    <th className="px-4 py-2">{t("table.message")}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {events.map((event, i) => (
                    <tr key={i} className="text-foreground">
                      <td className="whitespace-nowrap px-4 py-1.5 font-mono text-xs text-muted-foreground">
                        {new Date(event.timestamp).toLocaleTimeString()}
                      </td>
                      <td className="px-4 py-1.5 text-xs">{event.kind}</td>
                      <td className="px-4 py-1.5">{event.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {/* Mobile event list */}
              <div className="divide-y divide-border/50 sm:hidden">
                {events.map((event, i) => (
                  <div key={i} className="px-3 py-2">
                    <div className="flex items-center gap-2 text-xs text-muted-foreground">
                      <span className="font-mono">{new Date(event.timestamp).toLocaleTimeString()}</span>
                      <span className="text-foreground">{event.kind}</span>
                    </div>
                    <div className="mt-0.5 text-sm text-foreground">{event.message}</div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      </div>

      <ConfirmDialog
        open={showCancelConfirm}
        title={t("confirm.cancelJob")}
        message={t("confirm.cancelJobMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => {
          cancelJob({ id: job.id });
          setShowCancelConfirm(false);
        }}
        onCancel={() => setShowCancelConfirm(false)}
      />
    </div>
  );
}

function statusColor(status: string): string {
  const colors: Record<string, string> = {
    QUEUED: "bg-zinc-600 text-zinc-200",
    DOWNLOADING: "bg-blue-600 text-blue-100",
    VERIFYING: "bg-yellow-600 text-yellow-100",
    REPAIRING: "bg-orange-600 text-orange-100",
    EXTRACTING: "bg-purple-600 text-purple-100",
    COMPLETE: "bg-green-600 text-green-100",
    FAILED: "bg-red-600 text-red-100",
    PAUSED: "bg-zinc-600 text-zinc-300",
  };
  return colors[status] ?? "bg-zinc-700 text-zinc-300";
}

function healthColor(health: number): string {
  if (health >= 950) return "text-green-400";
  if (health >= 850) return "text-yellow-400";
  return "text-red-400";
}

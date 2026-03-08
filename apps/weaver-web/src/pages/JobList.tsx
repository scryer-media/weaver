import { useRef, useState } from "react";
import { Link } from "react-router";
import { useMutation } from "urql";
import {
  PAUSE_ALL_MUTATION,
  RESUME_ALL_MUTATION,
  PAUSE_JOB_MUTATION,
  RESUME_JOB_MUTATION,
  CANCEL_JOB_MUTATION,
  SET_SPEED_LIMIT_MUTATION,
} from "@/graphql/queries";
import { ProgressBar } from "@/components/ProgressBar";
import { StatusBadge } from "@/components/StatusBadge";
import { SpeedDisplay, formatBytes } from "@/components/SpeedDisplay";
import { UploadModal } from "@/components/UploadModal";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { useTranslate } from "@/lib/context/translate-context";
import { useLiveData } from "@/lib/context/live-data-context";

const SPEED_LIMIT_PRESETS = [
  { label: "Unlimited", value: 0 },
  { label: "1 MB/s", value: 1024 * 1024 },
  { label: "5 MB/s", value: 5 * 1024 * 1024 },
  { label: "10 MB/s", value: 10 * 1024 * 1024 },
  { label: "25 MB/s", value: 25 * 1024 * 1024 },
  { label: "50 MB/s", value: 50 * 1024 * 1024 },
  { label: "100 MB/s", value: 100 * 1024 * 1024 },
];

function formatEta(remainingBytes: number, speed: number): string {
  if (speed <= 0 || remainingBytes <= 0) return "—";
  const secs = Math.ceil(remainingBytes / speed);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) {
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return `${m}m ${s}s`;
  }
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  return `${h}h ${m}m`;
}

const ETA_UPDATE_INTERVAL_MS = 2500;

/** Returns a stable ETA string that only recomputes every 2.5s per job. */
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
  const jobs = allJobs.filter(
    (j) => j.status !== "COMPLETE" && j.status !== "FAILED",
  );

  const [, pauseAll] = useMutation(PAUSE_ALL_MUTATION);
  const [, resumeAll] = useMutation(RESUME_ALL_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, setSpeedLimit] = useMutation(SET_SPEED_LIMIT_MUTATION);

  const [uploadOpen, setUploadOpen] = useState(false);
  const [speedLimitValue, setSpeedLimitValue] = useState(0);
  const [cancelConfirmId, setCancelConfirmId] = useState<number | null>(null);
  const getEta = useThrottledEta();

  const t = useTranslate();

  // Compute per-job speed proportionally based on remaining bytes.
  const totalRemaining = jobs.reduce(
    (sum, j) =>
      j.status === "DOWNLOADING"
        ? sum + (j.totalBytes - j.downloadedBytes)
        : sum,
    0,
  );

  return (
    <div className="p-4 sm:p-6">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3 sm:mb-6">
        <h1 className="text-xl font-bold text-foreground sm:text-2xl">{t("jobs.title")}</h1>
        <div className="flex flex-wrap items-center gap-2 sm:gap-3">
          <SpeedDisplay bytesPerSec={speed} className="text-lg font-bold text-foreground sm:text-2xl" />
          <select
            value={speedLimitValue}
            onChange={(e) => {
              const v = Number(e.target.value);
              setSpeedLimitValue(v);
              setSpeedLimit({ bytesPerSec: v });
            }}
            className="rounded-md border border-border bg-card px-2 py-1.5 text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
          >
            {SPEED_LIMIT_PRESETS.map((p) => (
              <option key={p.value} value={p.value}>{p.label}</option>
            ))}
          </select>
          <button
            onClick={() => setUploadOpen(true)}
            className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 sm:px-4"
          >
            {t("nav.upload")}
          </button>
          <button
            onClick={() => (isPaused ? resumeAll() : pauseAll())}
            className={`rounded-md px-3 py-2 text-sm font-medium transition-colors sm:px-4 ${
              isPaused
                ? "bg-green-600 text-white hover:bg-green-500"
                : "bg-yellow-600 text-white hover:bg-yellow-500"
            }`}
          >
            {isPaused ? t("action.resumeAll") : t("action.pauseAll")}
          </button>
        </div>
      </div>

      {jobs.length === 0 ? (
        <div className="py-12 text-center text-muted-foreground">
          {t("jobs.empty")}{" "}
          <button onClick={() => setUploadOpen(true)} className="text-primary hover:underline">
            {t("jobs.emptyAction")}
          </button>{" "}
          {t("jobs.emptyHint")}
        </div>
      ) : (
        <>
          {/* Desktop table */}
          <div className="hidden overflow-hidden rounded-lg border border-border lg:block">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-card text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  <th className="max-w-48 px-4 py-3">{t("table.name")}</th>
                  <th className="w-28 px-4 py-3 text-center">{t("table.status")}</th>
                  <th className="w-28 px-4 py-3">{t("table.category")}</th>
                  <th className="w-64 px-4 py-3 text-center">{t("table.progress")}</th>
                  <th className="w-48 px-4 py-3 text-center">{t("table.size")}</th>
                  <th className="w-24 px-4 py-3 text-center">{t("table.eta")}</th>
                  <th className="w-40 px-4 py-3 text-right">{t("table.actions")}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {jobs.map((job) => {
                  const remaining = job.totalBytes - job.downloadedBytes;
                  const jobSpeed =
                    job.status === "DOWNLOADING" && totalRemaining > 0
                      ? (remaining / totalRemaining) * speed
                      : 0;
                  return (
                    <tr key={job.id} className="hover:bg-accent/30">
                      <td className="max-w-48 truncate px-4 py-3">
                        <Link
                          to={`/jobs/${job.id}`}
                          className="text-sm font-medium text-foreground hover:text-primary hover:underline"
                        >
                          {job.name}
                        </Link>
                        {job.health < 1000 && (
                          <span
                            className={`ml-2 inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
                              job.health >= 950
                                ? "bg-yellow-600/20 text-yellow-400"
                                : "bg-red-600/20 text-red-400"
                            }`}
                          >
                            {(job.health / 10).toFixed(1)}%
                          </span>
                        )}
                        {job.hasPassword && (
                          <span className="ml-2 text-xs text-yellow-500" title={t("job.passwordProtected")}>
                            {t("jobs.passwordProtected")}
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-center">
                        <StatusBadge status={job.status} />
                      </td>
                      <td className="px-4 py-3 text-sm text-muted-foreground">
                        {job.category ?? "—"}
                      </td>
                      <td className="px-4 py-3">
                        <ProgressBar progress={job.progress} status={job.status} />
                      </td>
                      <td className="px-4 py-3 text-center text-sm text-muted-foreground">
                        {formatBytes(job.downloadedBytes)} / {formatBytes(job.totalBytes)}
                      </td>
                      <td className="px-4 py-3 text-center text-sm tabular-nums text-muted-foreground">
                        {job.status === "DOWNLOADING"
                          ? getEta(job.id, remaining, jobSpeed)
                          : "—"}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <div className="flex items-center justify-end gap-1">
                          {job.status === "PAUSED" ? (
                            <button
                              onClick={() => resumeJob({ id: job.id })}
                              className="rounded px-2 py-1 text-xs text-green-400 hover:bg-accent"
                            >
                              {t("action.resume")}
                            </button>
                          ) : job.status !== "COMPLETE" && job.status !== "FAILED" ? (
                            <button
                              onClick={() => pauseJob({ id: job.id })}
                              className="rounded px-2 py-1 text-xs text-yellow-400 hover:bg-accent"
                            >
                              {t("action.pause")}
                            </button>
                          ) : null}
                          {job.status !== "COMPLETE" && (
                            <button
                              onClick={() => setCancelConfirmId(job.id)}
                              className="rounded px-2 py-1 text-xs text-destructive hover:bg-accent"
                            >
                              {t("action.cancel")}
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Mobile card layout */}
          <div className="space-y-3 lg:hidden">
            {jobs.map((job) => {
              const remaining = job.totalBytes - job.downloadedBytes;
              const jobSpeed =
                job.status === "DOWNLOADING" && totalRemaining > 0
                  ? (remaining / totalRemaining) * speed
                  : 0;
              return (
                <div key={job.id} className="rounded-lg border border-border bg-card p-4">
                  <div className="mb-2 flex items-start justify-between gap-2">
                    <Link
                      to={`/jobs/${job.id}`}
                      className="min-w-0 flex-1 truncate text-sm font-medium text-foreground hover:text-primary hover:underline"
                    >
                      {job.name}
                    </Link>
                    <StatusBadge status={job.status} />
                  </div>
                  {job.health < 1000 && (
                    <span
                      className={`mb-2 inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
                        job.health >= 950
                          ? "bg-yellow-600/20 text-yellow-400"
                          : "bg-red-600/20 text-red-400"
                      }`}
                    >
                      {(job.health / 10).toFixed(1)}%
                    </span>
                  )}
                  <div className="mb-3">
                    <ProgressBar progress={job.progress} status={job.status} />
                  </div>
                  <div className="mb-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                    <span>{formatBytes(job.downloadedBytes)} / {formatBytes(job.totalBytes)}</span>
                    {job.status === "DOWNLOADING" && (
                      <span className="tabular-nums">ETA: {getEta(job.id, remaining, jobSpeed)}</span>
                    )}
                    {job.category && <span>{job.category}</span>}
                    {job.hasPassword && (
                      <span className="text-yellow-500">{t("jobs.passwordProtected")}</span>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {job.status === "PAUSED" ? (
                      <button
                        onClick={() => resumeJob({ id: job.id })}
                        className="rounded-md px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-accent"
                      >
                        {t("action.resume")}
                      </button>
                    ) : job.status !== "COMPLETE" && job.status !== "FAILED" ? (
                      <button
                        onClick={() => pauseJob({ id: job.id })}
                        className="rounded-md px-3 py-1.5 text-xs font-medium text-yellow-400 hover:bg-accent"
                      >
                        {t("action.pause")}
                      </button>
                    ) : null}
                    {job.status !== "COMPLETE" && (
                      <button
                        onClick={() => setCancelConfirmId(job.id)}
                        className="rounded-md px-3 py-1.5 text-xs font-medium text-destructive hover:bg-accent"
                      >
                        {t("action.cancel")}
                      </button>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      <UploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />

      <ConfirmDialog
        open={cancelConfirmId != null}
        title={t("confirm.cancelJob")}
        message={t("confirm.cancelJobMessage")}
        confirmLabel={t("confirm.cancelJobConfirm")}
        cancelLabel={t("confirm.cancelJobDismiss")}
        onConfirm={() => {
          if (cancelConfirmId != null) cancelJob({ id: cancelConfirmId });
          setCancelConfirmId(null);
        }}
        onCancel={() => setCancelConfirmId(null)}
      />
    </div>
  );
}

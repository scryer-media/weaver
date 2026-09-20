import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useMutation, useQuery, useSubscription } from "urql";
import {
  ACCEPT_HISTORY_DELETE_MUTATION,
  CANCEL_JOB_MUTATION,
  CANCEL_JOB_POST_PROCESSING_MUTATION,
  DUPLICATE_SNAPSHOT_QUERY,
  FORGET_DUPLICATE_IDENTITY_MUTATION,
  JOB_DETAIL_UPDATES_SUBSCRIPTION,
  JOB_OUTPUT_FILES_QUERY,
  JOB_QUERY,
  PAUSE_JOB_MUTATION,
  REDOWNLOAD_JOB_MUTATION,
  REPROCESS_JOB_MUTATION,
  RERUN_POST_PROCESSING_MUTATION,
  RESUME_JOB_MUTATION,
} from "@/graphql/queries";
import { authHeaders } from "@/graphql/client";
import { useTranslate } from "@/lib/context/translate-context";
import { openUrlAsDownload, readDownloadErrorMessage, saveResponseAsDownload } from "@/lib/download";
import { normalizeJobData, type GraphqlJobData, type JobData } from "@/lib/job-types";
import { statusToken } from "@/lib/status-tokens";
import {
  Bar,
  DetailBlock,
  EmptyState,
  Field,
  MetricCell,
  MetricStrip,
  PanelGrid,
  Square,
  StateChip,
  Tag,
} from "../components/chrome";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { DangerButton, SecondaryButton } from "../components/controls";
import { Icon } from "../components/icons";
import { PhaseBars, useJobProgress } from "../components/PhaseBars";
import { GridRow } from "../components/rows";
import { Waterfall } from "../components/Waterfall";
import {
  EM_DASH,
  formatClockSeconds,
  formatDuration,
  formatRate,
  formatSize,
  formatSpan,
} from "../data/format";
import { useNow } from "../data/clock";
import { statusColor, WV } from "../data/palette";
import { ratePhase } from "../data/phase-bars";
import { releaseFields, releaseFlags } from "../data/release";
import { eventTone, useStatusLabel } from "../data/status";
import { buildTimelineView, type JobTimelineData } from "../data/timeline";
import { useNextData } from "../data/next-data";
import { countLabel } from "../i18n/labels";
import { NextShell, RailBlock } from "../shell/NextShell";

/**
 * One job, end to end.
 *
 * The screen is a single scroller of blocks — what happened, how long each
 * stage took, what came out, what the engine said, and what weaver understood
 * about the release — because a job is a story with an order, and tabs would
 * let you miss the part that explains the part you are looking at.
 *
 * It reads `jobDetailSnapshot`, which answers for a queued job and a finished
 * one alike, so the same screen serves a download in flight and an entry in
 * history without branching into two pages.
 */

// The relative-size bar is the one cell here carrying no value of its own, so
// it is the one that goes when a phone cannot hold four tracks.
const FILE_COLUMNS = {
  base: "minmax(0, 1fr) 62px 74px 46px",
  sm: "minmax(0, 1fr) 68px 82px 54px",
};

// A log needs its clock and its message; the machine-readable kind is what a
// narrow screen can do without, so its tone moves onto the message instead.
const EVENT_COLUMNS = {
  base: "56px minmax(0, 1fr)",
  sm: "64px minmax(120px, 210px) minmax(0, 1fr)",
};

const EVENT_TONE_CLASS = {
  bad: "text-wv-error",
  good: "text-wv-accent",
  note: "text-wv-info",
  plain: "text-wv-muted",
} as const;

/**
 * Provider bars by rank, not by identity: the same hues the category rotation
 * uses, in contribution order. A provider keeps its colour for as long as it
 * keeps its place, which is what the eye actually tracks down a short list.
 */
const PROVIDER_COLORS = [WV.accent, WV.info, WV.violet, WV.green, WV.gold] as const;

interface JobEvent {
  kind: string;
  fileId: string | null;
  message: string;
  timestamp: number;
}

/** One provider's share of a job, as the engine attributed it. */
interface ServerContribution {
  serverId: number;
  /** Null once the server has been removed from the configuration. */
  serverHost: string | null;
  articles: number;
  wireBytes: number;
}

interface JobSnapshot {
  queueItem?: GraphqlJobData | null;
  historyItem?: GraphqlJobData | null;
  jobTimeline?: JobTimelineData | null;
  jobEvents?: JobEvent[];
  serverAttribution?: ServerContribution[];
}

interface OutputFile {
  name: string;
  path: string;
  sizeBytes: number;
}

interface DuplicateSnapshot {
  lifecycle: string | null;
  normalizedName: string | null;
  semantic: {
    groupId: number | null;
    normalizedKey: string | null;
    score: number | null;
    state: string | null;
    terminalCause: string | null;
    promotionState: string | null;
  } | null;
}

/** What the progress clock holds before the snapshot arrives. */
const NO_JOB: Pick<JobData, "id" | "status" | "phaseProgress"> = {
  id: -1,
  status: "",
  phaseProgress: [],
};

/** `SOME_EVENT_KIND` is the engine's word; the log shows it as it is. */
function sentenceCase(value: string): string {
  const text = value.replace(/_/g, " ").toLowerCase();
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export function JobDetailPage() {
  const t = useTranslate();
  const { id } = useParams();
  const jobId = Number(id);
  const navigate = useNavigate();
  const statusLabel = useStatusLabel();
  const { connection } = useNextData();
  const variables = useMemo(() => ({ id: jobId }), [jobId]);

  const [{ data, fetching }, refetch] = useQuery<{ jobDetailSnapshot: JobSnapshot | null }>({
    query: JOB_QUERY,
    variables,
    pause: !Number.isFinite(jobId),
  });
  const [{ data: live }] = useSubscription<{ jobDetailUpdates: JobSnapshot }>({
    query: JOB_DETAIL_UPDATES_SUBSCRIPTION,
    variables,
    pause: connection.isDisconnected || !Number.isFinite(jobId),
  });
  const [{ data: filesData }, refetchFiles] = useQuery<{
    jobOutputFiles: { outputDir: string | null; files: OutputFile[]; totalBytes: number } | null;
  }>({ query: JOB_OUTPUT_FILES_QUERY, variables: { jobId }, pause: !Number.isFinite(jobId) });
  const [{ data: duplicateData }, refetchDuplicate] = useQuery<{
    duplicateSnapshot: DuplicateSnapshot | null;
  }>({ query: DUPLICATE_SNAPSHOT_QUERY, variables, pause: !Number.isFinite(jobId) });

  const [, redownloadJob] = useMutation(REDOWNLOAD_JOB_MUTATION);
  const [, rerunPostProcessing] = useMutation(RERUN_POST_PROCESSING_MUTATION);
  const [, reprocessJob] = useMutation(REPROCESS_JOB_MUTATION);
  const [, acceptHistoryDelete] = useMutation(ACCEPT_HISTORY_DELETE_MUTATION);
  const [, forgetDuplicateIdentity] = useMutation(FORGET_DUPLICATE_IDENTITY_MUTATION);
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, cancelPostProcessing] = useMutation(CANCEL_JOB_POST_PROCESSING_MUTATION);

  const [confirm, setConfirm] = useState<"delete" | "deleteAll" | "cancel" | "forget" | null>(null);
  const [report, setReport] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const snapshot = live?.jobDetailUpdates ?? data?.jobDetailSnapshot ?? null;
  const inQueue = Boolean(snapshot?.queueItem);
  const raw = snapshot?.queueItem ?? snapshot?.historyItem ?? null;
  const job = useMemo(() => (raw ? normalizeJobData(raw) : null), [raw]);
  const progress = useJobProgress(job ?? NO_JOB);
  // The output folder fills in as the job moves through post-processing and
  // settles when it finishes, so the listing is re-read on each status change
  // rather than kept from the page's first render.
  const jobStatus = job?.status;
  const listedStatus = useRef(jobStatus);
  useEffect(() => {
    const previous = listedStatus.current;
    listedStatus.current = jobStatus;
    // The first status is the one the initial listing was read under.
    if (previous !== undefined && jobStatus !== undefined && previous !== jobStatus) {
      refetchFiles({ requestPolicy: "network-only" });
    }
  }, [jobStatus, refetchFiles]);
  // The engine appends; the log reads newest first, like every other log here.
  const events = useMemo(() => [...(snapshot?.jobEvents ?? [])].reverse(), [snapshot?.jobEvents]);
  // Shares are of the payload that *was* attributed, not of the job's bytes:
  // an article whose serving provider could not be named is left out of the
  // ledger rather than charged to the wrong one, so these add to 100% of a
  // total that can be smaller than the download. Ordered and measured by
  // payload, which is the question the panel answers.
  const providers = useMemo(() => {
    const credited = (snapshot?.serverAttribution ?? []).filter((entry) => entry.articles > 0);
    const attributed = credited.reduce((sum, entry) => sum + entry.wireBytes, 0);
    return credited
      .slice()
      .sort((left, right) => right.wireBytes - left.wireBytes || left.serverId - right.serverId)
      .map((entry) => ({
        ...entry,
        share: attributed > 0 ? (entry.wireBytes / attributed) * 100 : 0,
      }));
  }, [snapshot?.serverAttribution]);
  // A running job's window keeps growing, so the axis has to move with it; a
  // finished one is fixed and needs no clock at all.
  const now = useNow(1000, inQueue);
  const timeline = useMemo(
    () => buildTimelineView(t, snapshot?.jobTimeline, now, job?.status === "PROPAGATING"),
    [job?.status, now, snapshot?.jobTimeline, t],
  );

  // The two dot-files weaver drops to mark a job's own directory are
  // bookkeeping, not output: listing them offers a download the server refuses.
  const files = (filesData?.jobOutputFiles?.files ?? []).filter(
    (file) => file.name !== ".weaver-job-dir" && file.name !== ".weaver-output-dir",
  );
  const outputDir = filesData?.jobOutputFiles?.outputDir || job?.outputDir || null;
  const duplicate = duplicateData?.duplicateSnapshot ?? null;

  if (!Number.isFinite(jobId) || (!job && !fetching)) {
    return (
      <NextShell title={t("next.job.title")} note={`#${id}`}>
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
          <EmptyState title={t("next.job.missingTitle")} body={t("next.job.missingBody")} />
          <div className="px-4 sm:px-6">
            <SecondaryButton icon="back" onClick={() => navigate("/history")}>{t("next.job.backToCompleted")}</SecondaryButton>
          </div>
        </div>
      </NextShell>
    );
  }

  if (!job) {
    return (
      <NextShell title={t("next.job.title")} note={`#${id}`}>
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
          <EmptyState loading title={t("next.common.loading")} body={t("next.job.loadingBody")} />
        </div>
      </NextShell>
    );
  }

  const token = statusToken(job.status);
  const failed = token === "failed";
  const done = token === "completed";
  // Weaver holds a finished job in its queue for a while before history takes
  // it, so whether the job is over comes from its status, not from where the
  // snapshot found it: pausing or cancelling a finished job means nothing.
  const terminal = done || failed;
  const color = statusColor(progress.status);
  const percent = job.progress * 100;
  const phase = ratePhase(job.phaseProgress);
  const stageIds = new Set(timeline?.stages.map((stage) => stage.id) ?? []);
  const verified = stageIds.has("VERIFYING");
  const repaired = stageIds.has("REPAIRING");
  const elapsedMs =
    job.createdAt && job.completedAt && job.completedAt >= job.createdAt
      ? job.completedAt - job.createdAt
      : null;
  const savedBytes = Math.max(
    0,
    job.optionalRecoveryBytes - job.optionalRecoveryDownloadedBytes,
  );

  const outcome = failed
    ? t("next.job.outcomeFailed")
    : done
      ? repaired
        ? t("next.job.outcomeRepaired")
        : t("next.job.outcomeVerified")
      : t("next.job.outcomeRunning", { status: statusLabel(progress.status), percent: Math.round(percent) });

  const statusSentence = failed
    ? t("next.job.stopped", {
        time: formatClockSeconds(job.completedAt),
        reason: job.error || t("next.job.stoppedReason"),
      })
    : done
      ? [
          t("next.job.finished", { time: formatClockSeconds(job.completedAt) }),
          elapsedMs === null ? null : t("next.job.endToEnd", { span: formatSpan(elapsedMs) }),
          repaired ? t("next.job.repairedBeforeMoving") : verified ? t("next.job.verifiedWithoutRepair") : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : [
          t("next.job.started", { time: formatClockSeconds(job.createdAt) }),
          phase?.rateBps ? formatRate(phase.rateBps) : null,
          phase?.estimatedRemainingMs
            ? t("next.job.remaining", { duration: formatDuration(phase.estimatedRemainingMs / 1000) })
            : null,
        ]
          .filter(Boolean)
          .join(" · ");

  const run = async (label: string, action: () => Promise<{ error?: unknown }>) => {
    setBusy(true);
    const result = await action();
    setBusy(false);
    setReport(
      result.error
        ? String((result.error as { message?: string }).message ?? result.error)
        : label,
    );
    void refetch({ requestPolicy: "network-only" });
  };

  /**
   * The job's own NZB, fetched rather than linked: the endpoint wants the
   * session's auth header, which a plain anchor cannot carry.
   */
  const downloadNzb = async () => {
    if (!job) {
      return;
    }
    const title = job.originalTitle.trim() || job.name || job.displayTitle;
    setBusy(true);
    try {
      const response = await fetch(new URL(`api/jobs/${job.id}/nzb`, document.baseURI).href, {
        headers: authHeaders(),
        credentials: "same-origin",
      });
      if (!response.ok) {
        throw new Error(await readDownloadErrorMessage(response, t("next.job.nzbFailed")));
      }
      await saveResponseAsDownload(response, `${title}.nzb`);
      setReport(t("next.job.nzbSaved", { name: title }));
    } catch (error) {
      setReport(error instanceof Error ? error.message : t("next.job.nzbFailed"));
    } finally {
      setBusy(false);
    }
  };

  /**
   * An output file, fetched by navigation so the browser streams it straight
   * to disk: these run to gigabytes, which is no size to hold in a blob.
   */
  const downloadOutputFile = (file: OutputFile) => {
    const url = new URL(`api/jobs/${jobId}/output-file`, document.baseURI);
    url.searchParams.set("path", file.path);
    openUrlAsDownload(url.href);
    setReport(t("next.job.fileSaving", { name: file.name }));
  };

  const copyLog = () => {
    const text = events
      .map(
        (event) =>
          `${formatClockSeconds(event.timestamp)}  ${event.kind.padEnd(32)}  ${event.message}`,
      )
      .join("\n");
    void navigator.clipboard?.writeText(text);
    setReport(countLabel(t, "next.job.copiedEvents", events.length));
  };

  return (
    <NextShell
      header={
        <header className="flex flex-none flex-wrap items-center gap-[14px] border-b border-wv-line-strong bg-wv-chrome py-[11px] pr-4 pl-12 sm:pr-[22px] lg:pl-[22px]">
          <div className="flex min-w-0 flex-[1_1_260px] items-center gap-[11px]">
            <Link
              to={inQueue ? "/" : "/history"}
              className="flex flex-none items-center gap-[5px] font-wv-mono text-[11.5px] text-wv-muted hover:text-wv-fg"
            >
              <Icon name="back" size={13} />
              {inQueue ? t("next.nav.downloads") : t("next.nav.completed")}
            </Link>
            <span aria-hidden="true" className="flex-none text-wv-dim">
              /
            </span>
            <h1 className="min-w-0 truncate font-wv-title text-[15px] font-semibold tracking-[-0.01em]">
              {job.displayTitle || job.name}
            </h1>
            <StateChip label={statusLabel(progress.status)} tone={failed ? "bad" : "ok"} />
          </div>
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <SecondaryButton
              icon="downloadFile"
              size="compact"
              disabled={busy}
              onClick={() => {
                void downloadNzb();
              }}
            >
              {t("next.job.downloadNzb")}
            </SecondaryButton>
            {/* Only while scripts run: a job waiting for a script slot reports itself as queued. */}
            {job.status === "POST_PROCESSING" ? (
              <SecondaryButton
                icon="stopScripts"
                size="compact"
                disabled={busy}
                onClick={() => {
                  void run(t("next.job.report.stoppedScripts"), () => cancelPostProcessing({ jobId: job.id }));
                }}
              >
                {t("next.inspector.stopScripts")}
              </SecondaryButton>
            ) : null}
            {inQueue && !terminal ? (
              <>
                <SecondaryButton
                  icon={token === "paused" ? "resume" : "pause"}
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(
                      token === "paused" ? t("next.job.report.resumed") : t("next.job.report.paused"),
                      () => (token === "paused" ? resumeJob({ id: job.id }) : pauseJob({ id: job.id })),
                    );
                  }}
                >
                  {token === "paused" ? t("action.resume") : t("action.pause")}
                </SecondaryButton>
                <SecondaryButton
                  icon="postProcessing"
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(t("next.job.report.rerunning"), () =>
                      rerunPostProcessing({ jobId: job.id }),
                    );
                  }}
                >
                  {t("next.completed.rerunScripts")}
                </SecondaryButton>
                <DangerButton icon="cancelDownload" size="compact" disabled={busy} onClick={() => setConfirm("cancel")}>
                  {t("action.cancel")}
                </DangerButton>
              </>
            ) : (
              <>
                <SecondaryButton
                  icon="redownload"
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(t("next.job.report.requeued"), () => redownloadJob({ id: job.id }));
                  }}
                >
                  {t("next.completed.redownload")}
                </SecondaryButton>
                <SecondaryButton
                  icon="postProcessing"
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(t("next.job.report.rerunning"), () =>
                      rerunPostProcessing({ jobId: job.id }),
                    );
                  }}
                >
                  {t("next.completed.rerunScripts")}
                </SecondaryButton>
                <SecondaryButton
                  icon="reprocess"
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(t("next.job.report.reprocessing"), () =>
                      reprocessJob({ id: job.id }),
                    );
                  }}
                >
                  {t("next.job.reprocess")}
                </SecondaryButton>
                <DangerButton icon="remove" size="compact" disabled={busy} onClick={() => setConfirm("delete")}>
                  {t("next.job.deleteSaveFiles")}
                </DangerButton>
                <DangerButton icon="remove" size="compact" solid disabled={busy} onClick={() => setConfirm("deleteAll")}>
                  {t("action.delete")}
                </DangerButton>
              </>
            )}
          </div>
        </header>
      }
      railFooter={
        <RailBlock eyebrow={t("next.job.savedBandwidth")}>
          <div className="text-[20px] font-semibold tracking-[-0.02em] text-wv-accent">
            {job.optionalRecoveryBytes > 0 ? formatSize(savedBytes) : EM_DASH}
          </div>
          <div className="font-wv-mono text-[10.5px] leading-[1.5] text-wv-faint">
            {job.optionalRecoveryBytes === 0
              ? t("next.job.saved.noSet")
              : job.optionalRecoveryDownloadedBytes === 0
                ? t("next.job.saved.untouched")
                : savedBytes === 0
                  ? t("next.job.saved.wholeSet")
                  : t("next.job.saved.partSet")}
          </div>
        </RailBlock>
      }
      statusNote={
        report ?? (
          <span className="flex min-w-0 items-center gap-[7px]">
            <Square color={color} size={6} />
            <span className="truncate">{outcome}</span>
            {outputDir ? (
              <>
                <span aria-hidden="true" className="flex-none text-wv-inert">
                  |
                </span>
                <span className="min-w-0 truncate">{outputDir}</span>
              </>
            ) : null}
          </span>
        )
      }
      statusRight={t("next.job.statusId", { id: job.id })}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <div className="flex flex-none flex-col gap-[14px] border-b border-wv-line-strong px-4 sm:px-[22px] pt-[18px] pb-5">
          <div className="font-wv-mono text-[11.5px] leading-[1.5] break-all text-wv-muted">
            {job.name}
          </div>
          <PhaseBars
            job={job}
            view={progress}
            height={16}
            decimals={1}
            barClassName="min-w-0 flex-1"
            percentClassName="text-[13px] font-medium text-wv-fg"
          />
          <div className="font-wv-mono text-[11.5px] text-wv-muted">{statusSentence}</div>
        </div>

        <MetricStrip min={168}>
          <MetricCell
            variant="stat"
            eyebrow={t("next.job.downloaded")}
            value={formatSize(job.downloadedBytes)}
            note={t("next.job.ofPosted", { size: formatSize(job.totalBytes) })}
          />
          <MetricCell
            variant="stat"
            eyebrow={t("next.job.integrity")}
            value={verified || done || failed ? `${(job.health / 10).toFixed(1)}%` : EM_DASH}
            valueClassName={
              !verified && !done && !failed
                ? "text-wv-muted"
                : job.health >= 1000
                  ? "text-wv-accent"
                  : "text-wv-error"
            }
            note={
              verified || done || failed
                ? job.failedBytes > 0
                  ? t("next.job.neverRecovered", { size: formatSize(job.failedBytes) })
                  : t("next.job.noDamage")
                : t("next.job.verifiesAfter")
            }
          />
          <MetricCell
            variant="stat"
            eyebrow={inQueue ? t("next.job.currentRate") : t("next.job.averageRate")}
            value={
              inQueue
                ? phase?.rateBps
                  ? formatRate(phase.rateBps)
                  : EM_DASH
                : elapsedMs && elapsedMs > 0
                  ? formatRate(job.downloadedBytes / (elapsedMs / 1000))
                  : EM_DASH
            }
            note={
              inQueue
                ? phase
                  ? t(`next.job.while.${phase.phase}`)
                  : t("next.job.thisPhase")
                : t("next.job.endToEndWithScripts")
            }
          />
          <MetricCell
            variant="stat"
            eyebrow={job.optionalRecoveryDownloadedBytes > 0 ? t("next.job.recovery") : t("next.job.saved")}
            value={
              job.optionalRecoveryBytes === 0
                ? EM_DASH
                : job.optionalRecoveryDownloadedBytes > 0
                  ? formatSize(job.optionalRecoveryDownloadedBytes)
                  : formatSize(savedBytes)
            }
            valueClassName={
              job.optionalRecoveryBytes === 0
                ? "text-wv-muted"
                : job.optionalRecoveryDownloadedBytes > 0
                  ? "text-wv-warn"
                  : "text-wv-accent"
            }
            note={
              job.optionalRecoveryBytes === 0
                ? t("next.job.noSetPosted")
                : job.optionalRecoveryDownloadedBytes > 0
                  ? t("next.job.ofFetched", { size: formatSize(job.optionalRecoveryBytes) })
                  : t("next.job.setSkipped")
            }
          />
        </MetricStrip>

        <DetailBlock id="pipeline" title={t("next.job.pipeline")} note={timeline?.note} bodyClassName="gap-0">
          {timeline ? (
            <Waterfall
              stages={timeline.stages}
              members={timeline.members}
              ticks={timeline.ticks}
              window={timeline.window}
              total={timeline.total}
            />
          ) : (
            <span className="text-[12.5px] text-wv-muted">
              {t("next.job.noStages")}
            </span>
          )}
        </DetailBlock>

        <DetailBlock
          id="files"
          title={t("next.job.outputFiles")}
          note={
            files.length === 0
              ? t("next.job.nothingWritten")
              : `${countLabel(t, "next.job.fileCount", files.length)} · ${formatSize(filesData?.jobOutputFiles?.totalBytes ?? 0)}`
          }
          right={outputDir ?? undefined}
        >
          {files.length === 0 ? (
            <span className="text-[12.5px] text-wv-muted">
              {t("next.job.filesAppear")}
            </span>
          ) : (
            files.map((file) => (
              <GridRow
                key={file.path}
                columns={FILE_COLUMNS}
                gap={14}
                className="h-[38px] border-t border-wv-hairline"
                title={file.path}
              >
                <div className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                  {file.name}
                </div>
                <div className="text-right font-wv-mono text-[12px] text-wv-muted">
                  {formatSize(file.sizeBytes)}
                </div>
                <button
                  type="button"
                  title={t("next.job.downloadFileTitle", { name: file.name })}
                  onClick={() => downloadOutputFile(file)}
                  className="cursor-pointer text-right text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-fg"
                >
                  {t("next.job.download")}
                </button>
                <button
                  type="button"
                  title={t("next.job.copyPathTitle")}
                  onClick={() => {
                    void navigator.clipboard?.writeText(file.path);
                    setReport(t("next.job.copiedPath", { name: file.name }));
                  }}
                  className="cursor-pointer text-right text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-fg"
                >
                  {t("next.job.copy")}
                </button>
              </GridRow>
            ))
          )}
        </DetailBlock>

        <DetailBlock
          id="log"
          title={t("next.job.eventLog")}
          note={countLabel(t, "next.job.eventCount", events.length)}
          right={
            events.length === 0 ? undefined : (
              <button
                type="button"
                onClick={copyLog}
                className="cursor-pointer text-wv-dim hover:text-wv-fg"
              >
                {t("next.job.copyAsText")}
              </button>
            )
          }
        >
          {events.length === 0 ? (
            <span className="text-[12.5px] text-wv-muted">
              {t("next.job.noEvents")}
            </span>
          ) : (
            events.map((event) => (
              <GridRow
                key={`${event.timestamp}-${event.kind}-${event.fileId ?? ""}-${event.message}`}
                columns={EVENT_COLUMNS}
                className="items-baseline border-t border-wv-hairline py-[7px]"
              >
                <span className="font-wv-mono text-[11px] text-wv-disabled">
                  {formatClockSeconds(event.timestamp)}
                </span>
                <span
                  className={`hidden truncate font-wv-mono text-[11px] sm:block ${EVENT_TONE_CLASS[eventTone(event.kind)]}`}
                >
                  {event.kind}
                </span>
                <span
                  className={`min-w-0 text-[12.5px] sm:text-wv-tertiary ${EVENT_TONE_CLASS[eventTone(event.kind)]}`}
                >
                  {event.message}
                </span>
              </GridRow>
            ))
          )}
        </DetailBlock>

        <PanelGrid>
          <DetailBlock id="release" title={t("next.completed.release")} tone="panel" bodyClassName="gap-[13px]">
            <div
              className="grid gap-x-4 gap-y-[13px]"
              style={{ gridTemplateColumns: "repeat(auto-fit, minmax(104px, 1fr))" }}
            >
              {releaseFields(t, job.parsedRelease, job.category).map((field) => (
                <Field
                  key={field.label}
                  label={field.label}
                  value={field.value}
                  title={field.value}
                />
              ))}
            </div>
            {releaseFlags(t, job.parsedRelease).length === 0 ? null : (
              <div className="flex flex-wrap gap-1.5">
                {releaseFlags(t, job.parsedRelease).map((flag) => (
                  <Tag key={flag}>{flag}</Tag>
                ))}
              </div>
            )}
          </DetailBlock>

          <DetailBlock
            id="identity"
            title={t("next.job.duplicateIdentity")}
            tone="panel"
            bodyClassName="gap-[9px]"
          >
            <Field
              variant="inline"
              label={t("next.job.lifecycle")}
              value={duplicate?.lifecycle ? sentenceCase(duplicate.lifecycle) : EM_DASH}
            />
            <Field
              variant="inline"
              label={t("next.job.normalised")}
              value={duplicate?.normalizedName || EM_DASH}
              title={duplicate?.normalizedName ?? undefined}
            />
            <Field
              variant="inline"
              label={t("next.job.group")}
              value={
                duplicate?.semantic?.state
                  ? `${sentenceCase(duplicate.semantic.state)}${
                      duplicate.semantic.score === null
                        ? ""
                        : ` · ${Math.round(duplicate.semantic.score * 100)}%`
                    }`
                  : t("next.job.noGroup")
              }
            />
            <DangerButton
              icon="forget"
              className="mt-1 h-[30px] w-full"
              disabled={busy || !duplicate}
              onClick={() => setConfirm("forget")}
            >
              {t("next.job.forgetIdentity")}
            </DangerButton>
          </DetailBlock>

          <DetailBlock id="metadata" title={t("next.job.metadata")} tone="panel" bodyClassName="gap-[11px]">
            <Field variant="stacked" label={t("next.job.jobId")} value={String(job.id)} />
            <Field
              variant="stacked"
              label={t("next.job.originalTitle")}
              value={job.originalTitle || job.name}
            />
            <Field
              variant="stacked"
              label={t("table.category")}
              value={job.category || t("next.categories.uncategorised")}
            />
            {job.metadata.map((entry) => (
              <Field
                key={entry.key}
                variant="stacked"
                label={sentenceCase(entry.key)}
                value={entry.value}
              />
            ))}
          </DetailBlock>

          {/*
            The prototype's last panel. It lists only the providers that served
            something: the ledger omits a provider that contributed nothing
            rather than carrying it at zero, so "this backup went untouched" is
            read from its absence here, not from a row.
          */}
          <DetailBlock
            id="providers"
            title={t("next.job.providersUsed")}
            tone="panel"
            note={providers.length === 0 ? undefined : t("next.job.providersNote")}
            bodyClassName="gap-[11px]"
          >
            {providers.length === 0 ? (
              <div className="text-[12.5px] leading-[1.5] text-wv-muted">
                {t("next.job.providersEmpty")}
              </div>
            ) : (
              providers.map((provider, index) => (
                <ShareRow
                  key={provider.serverId}
                  label={provider.serverHost ?? t("next.job.serverId", { id: provider.serverId })}
                  title={countLabel(t, "next.job.articles", provider.articles, {
                    count: provider.articles.toLocaleString(),
                  })}
                  note={`${formatSize(provider.wireBytes)} · ${Math.round(provider.share)}%`}
                  percent={provider.share}
                  color={PROVIDER_COLORS[index % PROVIDER_COLORS.length]}
                />
              ))
            )}
          </DetailBlock>

          <DetailBlock id="recovery" title={t("next.job.bytes")} tone="panel" bodyClassName="gap-[11px]">
            <ShareRow
              label={t("next.job.payload")}
              note={t("next.job.sizeOf", { done: formatSize(job.downloadedBytes), total: formatSize(job.totalBytes) })}
              percent={job.totalBytes > 0 ? (job.downloadedBytes / job.totalBytes) * 100 : 0}
              color={color}
            />
            <ShareRow
              label={t("next.job.recoverySet")}
              note={
                job.optionalRecoveryBytes === 0
                  ? t("next.job.nonePosted")
                  : t("next.job.sizeOf", {
                      done: formatSize(job.optionalRecoveryDownloadedBytes),
                      total: formatSize(job.optionalRecoveryBytes),
                    })
              }
              percent={
                job.optionalRecoveryBytes > 0
                  ? (job.optionalRecoveryDownloadedBytes / job.optionalRecoveryBytes) * 100
                  : 0
              }
              color={WV.info}
            />
            <ShareRow
              label={t("next.job.unrecovered")}
              note={job.failedBytes > 0 ? formatSize(job.failedBytes) : t("next.job.none")}
              percent={job.totalBytes > 0 ? (job.failedBytes / job.totalBytes) * 100 : 0}
              color={WV.error}
            />
          </DetailBlock>
        </PanelGrid>
      </div>

      {(["delete", "deleteAll"] as const).map((kind) => {
        const deleteFiles = kind === "deleteAll";
        const title = deleteFiles ? t("next.job.deleteAllTitle") : t("next.job.deleteTitle");
        return (
          <ConfirmDialog
            key={kind}
            open={confirm === kind}
            title={title}
            note={job.displayTitle || job.name}
            busy={busy}
            confirmLabel={deleteFiles ? t("action.delete") : t("next.job.deleteSaveFiles")}
            solid={deleteFiles}
            body={deleteFiles ? t("next.job.deleteAllBody") : t("next.job.deleteBody")}
            onDismiss={() => setConfirm(null)}
            onConfirm={() => {
              setConfirm(null);
              void run(t("next.job.report.removed"), async () => {
                const result = await acceptHistoryDelete({
                  input: { mode: "IDS", ids: [job.id], deleteFiles },
                });
                if (!result.error) {
                  navigate("/history");
                }
                return result;
              });
            }}
          />
        );
      })}

      <ConfirmDialog
        open={confirm === "cancel"}
        title={t("next.job.cancelTitle")}
        note={job.displayTitle || job.name}
        busy={busy}
        confirmLabel={t("next.job.cancelTitle")}
        body={t("next.job.cancelBody")}
        onDismiss={() => setConfirm(null)}
        onConfirm={() => {
          setConfirm(null);
          void run(t("next.job.report.cancelled"), () => cancelJob({ id: job.id }));
        }}
      />

      <ConfirmDialog
        open={confirm === "forget"}
        title={t("next.job.forgetIdentity")}
        note={duplicate?.normalizedName ?? undefined}
        busy={busy}
        confirmLabel={t("next.job.forgetIdentity")}
        body={t("next.job.forgetBody")}
        onDismiss={() => setConfirm(null)}
        onConfirm={() => {
          setConfirm(null);
          void run(t("next.job.report.forgotten"), async () => {
            const result = await forgetDuplicateIdentity({ id: job.id });
            void refetchDuplicate({ requestPolicy: "network-only" });
            return result;
          });
        }}
      />
    </NextShell>
  );
}

/** One labelled bar in the bytes panel: a name, what it came to, and its share. */
function ShareRow({
  label,
  note,
  percent,
  color,
  title,
}: {
  label: string;
  note: string;
  percent: number;
  color: string;
  /** Hover detail for a label that stands for more than it can show. */
  title?: string;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-[5px]">
      <div className="flex items-baseline justify-between gap-[10px] text-[12.5px]">
        <span className="truncate text-wv-tertiary" title={title ?? label}>
          {label}
        </span>
        <span className="flex-none font-wv-mono text-[11px] text-wv-muted">{note}</span>
      </div>
      <Bar percent={percent} color={color} />
    </div>
  );
}

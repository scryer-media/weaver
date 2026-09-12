import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useMutation, useQuery, useSubscription } from "urql";
import {
  ACCEPT_HISTORY_DELETE_MUTATION,
  CANCEL_JOB_MUTATION,
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
import { normalizeJobData, type GraphqlJobData } from "@/lib/job-types";
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
import { fileColor, statusColor, WV } from "../data/palette";
import { releaseFields, releaseFlags } from "../data/release";
import { currentPhase, eventTone, useStatusLabel } from "../data/status";
import { buildTimelineView, type JobTimelineData } from "../data/timeline";
import { useNextData } from "../data/next-data";
import { NextShell, RailBlock } from "../shell/NextShell";
import { JumpListBlock } from "../shell/rail-blocks";

/**
 * One job, end to end.
 *
 * The screen is a single scroller of blocks — what happened, how long each
 * stage took, what came out, what the engine said, and what weaver understood
 * about the release — because a job is a story with an order, and tabs would
 * let you miss the part that explains the part you are looking at.
 *
 * It reads `jobDetailSnapshot`, which answers for a queued job and a finished
 * one alike, so the same screen serves a transfer in flight and an entry in
 * history without branching into two pages.
 */

// The relative-size bar is the one cell here carrying no value of its own, so
// it is the one that goes when a phone cannot hold four tracks.
const FILE_COLUMNS = {
  base: "minmax(0, 1fr) 62px 46px",
  sm: "minmax(0, 2fr) minmax(56px, 1fr) 68px 54px",
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

interface JobEvent {
  kind: string;
  fileId: string | null;
  message: string;
  timestamp: number;
}

interface JobSnapshot {
  queueItem?: GraphqlJobData | null;
  historyItem?: GraphqlJobData | null;
  jobTimeline?: JobTimelineData | null;
  jobEvents?: JobEvent[];
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

/** `SOME_EVENT_KIND` is the engine's word; the log shows it as it is. */
function sentenceCase(value: string): string {
  const text = value.replace(/_/g, " ").toLowerCase();
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export function JobDetailPage() {
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
  const [{ data: filesData }] = useQuery<{
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

  const [confirm, setConfirm] = useState<"delete" | "cancel" | "forget" | null>(null);
  const [report, setReport] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const snapshot = live?.jobDetailUpdates ?? data?.jobDetailSnapshot ?? null;
  const inQueue = Boolean(snapshot?.queueItem);
  const raw = snapshot?.queueItem ?? snapshot?.historyItem ?? null;
  const job = useMemo(() => (raw ? normalizeJobData(raw) : null), [raw]);
  // The engine appends; the log reads newest first, like every other log here.
  const events = useMemo(() => [...(snapshot?.jobEvents ?? [])].reverse(), [snapshot?.jobEvents]);
  // A running job's window keeps growing, so the axis has to move with it; a
  // finished one is fixed and needs no clock at all.
  const now = useNow(1000, inQueue);
  const timeline = useMemo(
    () => buildTimelineView(snapshot?.jobTimeline, now),
    [now, snapshot?.jobTimeline],
  );

  const files = filesData?.jobOutputFiles?.files ?? [];
  const outputDir = filesData?.jobOutputFiles?.outputDir || job?.outputDir || null;
  const duplicate = duplicateData?.duplicateSnapshot ?? null;

  if (!Number.isFinite(jobId) || (!job && !fetching)) {
    return (
      <NextShell title="Job" note={`#${id}`}>
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
          <EmptyState
            title="No such job"
            body="It was deleted from history, or the link points at an id weaver does not hold."
          />
          <div className="px-4 sm:px-6">
            <SecondaryButton onClick={() => navigate("/history")}>Back to Completed</SecondaryButton>
          </div>
        </div>
      </NextShell>
    );
  }

  if (!job) {
    return (
      <NextShell title="Job" note={`#${id}`}>
        <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
          <EmptyState title="Loading" body="Fetching this job's snapshot." />
        </div>
      </NextShell>
    );
  }

  const token = statusToken(job.status);
  const failed = token === "failed";
  const done = token === "completed";
  const color = statusColor(job.status);
  const percent = job.progress * 100;
  const phase = currentPhase(job);
  const largest = files.reduce((max, file) => Math.max(max, file.sizeBytes), 0);
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
    ? "Failed — incomplete, nothing moved"
    : done
      ? repaired
        ? "Complete — repaired, moved, scripts run"
        : "Complete — verified, moved, scripts run"
      : `${statusLabel(job.status)} — ${Math.round(percent)}% of this job`;

  const statusSentence = failed
    ? `Stopped ${formatClockSeconds(job.completedAt)} · ${job.error || "the pipeline could not finish this job"}`
    : done
      ? [
          `Finished ${formatClockSeconds(job.completedAt)}`,
          elapsedMs === null ? null : `${formatSpan(elapsedMs)} end to end`,
          repaired ? "repaired before moving" : verified ? "verified without repair" : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : [
          `Started ${formatClockSeconds(job.createdAt)}`,
          phase?.rateBps ? formatRate(phase.rateBps) : null,
          phase?.estimatedRemainingMs
            ? `${formatDuration(phase.estimatedRemainingMs / 1000)} remaining`
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

  const copyLog = () => {
    const text = events
      .map(
        (event) =>
          `${formatClockSeconds(event.timestamp)}  ${event.kind.padEnd(32)}  ${event.message}`,
      )
      .join("\n");
    void navigator.clipboard?.writeText(text);
    setReport(`Copied ${events.length} events to the clipboard`);
  };

  return (
    <NextShell
      header={
        <header className="flex flex-none flex-wrap items-center gap-[14px] border-b border-wv-line-strong bg-wv-chrome py-[11px] pr-4 pl-12 sm:pr-[22px] lg:pl-[22px]">
          <div className="flex min-w-0 flex-[1_1_260px] items-center gap-[11px]">
            <Link
              to={inQueue ? "/" : "/history"}
              className="flex-none font-wv-mono text-[11.5px] text-wv-muted hover:text-wv-fg"
            >
              {inQueue ? "‹ Transfers" : "‹ Completed"}
            </Link>
            <span aria-hidden="true" className="flex-none text-wv-dim">
              /
            </span>
            <h1 className="min-w-0 truncate text-[15px] font-semibold tracking-[-0.01em]">
              {job.displayTitle || job.name}
            </h1>
            <StateChip label={statusLabel(job.status)} tone={failed ? "bad" : "ok"} />
          </div>
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            {inQueue ? (
              <>
                <SecondaryButton
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run(
                      token === "paused" ? "Resumed" : "Paused",
                      () => (token === "paused" ? resumeJob({ id: job.id }) : pauseJob({ id: job.id })),
                    );
                  }}
                >
                  {token === "paused" ? "Resume" : "Pause"}
                </SecondaryButton>
                <SecondaryButton
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run("Re-running post-processing", () =>
                      rerunPostProcessing({ jobId: job.id }),
                    );
                  }}
                >
                  Re-run scripts
                </SecondaryButton>
                <DangerButton size="compact" disabled={busy} onClick={() => setConfirm("cancel")}>
                  Cancel
                </DangerButton>
              </>
            ) : (
              <>
                <SecondaryButton
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run("Queued again for download", () => redownloadJob({ id: job.id }));
                  }}
                >
                  Re-download
                </SecondaryButton>
                <SecondaryButton
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run("Re-running post-processing", () =>
                      rerunPostProcessing({ jobId: job.id }),
                    );
                  }}
                >
                  Re-run scripts
                </SecondaryButton>
                <SecondaryButton
                  size="compact"
                  disabled={busy}
                  onClick={() => {
                    void run("Reprocessing from what is on disk", () =>
                      reprocessJob({ id: job.id }),
                    );
                  }}
                >
                  Reprocess
                </SecondaryButton>
                <DangerButton size="compact" disabled={busy} onClick={() => setConfirm("delete")}>
                  Delete
                </DangerButton>
              </>
            )}
          </div>
        </header>
      }
      railMiddle={
        <JumpListBlock
          eyebrow="This job"
          items={[
            { id: "pipeline", label: "Pipeline", meta: timeline?.stages.length ?? 0 },
            { id: "files", label: "Output files", meta: files.length },
            { id: "log", label: "Event log", meta: events.length },
            { id: "release", label: "Release details" },
            { id: "metadata", label: "Metadata" },
          ]}
        />
      }
      railFooter={
        <RailBlock eyebrow="Saved bandwidth">
          <div className="text-[20px] font-semibold tracking-[-0.02em] text-wv-accent">
            {job.optionalRecoveryBytes > 0 ? formatSize(savedBytes) : EM_DASH}
          </div>
          <div className="font-wv-mono text-[10.5px] leading-[1.5] text-wv-faint">
            {job.optionalRecoveryBytes === 0
              ? "No recovery set was posted with this release."
              : job.optionalRecoveryDownloadedBytes === 0
                ? "par2 recovery set never downloaded — every article arrived intact."
                : savedBytes === 0
                  ? "The whole recovery set had to be fetched."
                  : "Part of the recovery set was enough — the rest was never fetched."}
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
      statusRight={`job #${job.id}`}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <div className="flex flex-none flex-col gap-[14px] border-b border-wv-line-strong px-4 sm:px-[22px] pt-[18px] pb-5">
          <div className="font-wv-mono text-[11.5px] leading-[1.5] break-all text-wv-muted">
            {job.name}
          </div>
          <div className="flex items-center gap-[14px]">
            <Bar percent={percent} color={color} height={12} className="min-w-0 flex-1" />
            <span className="flex-none font-wv-mono text-[13px] font-medium text-wv-fg">
              {percent.toFixed(1)}%
            </span>
          </div>
          <div className="font-wv-mono text-[11.5px] text-wv-muted">{statusSentence}</div>
        </div>

        <MetricStrip min={168}>
          <MetricCell
            variant="stat"
            eyebrow="Downloaded"
            value={formatSize(job.downloadedBytes)}
            note={`of ${formatSize(job.totalBytes)} posted`}
          />
          <MetricCell
            variant="stat"
            eyebrow="Integrity"
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
                  ? `${formatSize(job.failedBytes)} never recovered`
                  : "no damaged bytes"
                : "verifies after download"
            }
          />
          <MetricCell
            variant="stat"
            eyebrow={inQueue ? "Current rate" : "Average rate"}
            value={
              inQueue
                ? phase?.rateBps
                  ? formatRate(phase.rateBps)
                  : EM_DASH
                : elapsedMs && elapsedMs > 0
                  ? formatRate(job.downloadedBytes / (elapsedMs / 1000))
                  : EM_DASH
            }
            note={inQueue ? "this phase" : "end to end, including post-processing"}
          />
          <MetricCell
            variant="stat"
            eyebrow={job.optionalRecoveryDownloadedBytes > 0 ? "Recovery" : "Saved"}
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
                ? "no recovery set posted"
                : job.optionalRecoveryDownloadedBytes > 0
                  ? `of ${formatSize(job.optionalRecoveryBytes)} fetched`
                  : "par2 set skipped"
            }
          />
        </MetricStrip>

        <DetailBlock id="pipeline" title="Pipeline" note={timeline?.note} bodyClassName="gap-0">
          {timeline ? (
            <Waterfall
              stages={timeline.stages}
              ticks={timeline.ticks}
              window={timeline.window}
              total={timeline.total}
            />
          ) : (
            <span className="text-[12.5px] text-wv-muted">
              No stage has started yet — the pipeline appears once the job is picked up.
            </span>
          )}
        </DetailBlock>

        <DetailBlock
          id="files"
          title="Output files"
          note={
            files.length === 0
              ? "nothing written yet"
              : `${files.length} ${files.length === 1 ? "file" : "files"} · ${formatSize(filesData?.jobOutputFiles?.totalBytes ?? 0)}`
          }
          right={outputDir ?? undefined}
        >
          {files.length === 0 ? (
            <span className="text-[12.5px] text-wv-muted">
              Files appear here once the job reaches its destination.
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
                <Bar
                  percent={largest > 0 ? (file.sizeBytes / largest) * 100 : 0}
                  color={fileColor(file.name, color)}
                  className="hidden min-w-0 sm:block"
                />
                <div className="text-right font-wv-mono text-[12px] text-wv-muted">
                  {formatSize(file.sizeBytes)}
                </div>
                <button
                  type="button"
                  title="Copy the full path"
                  onClick={() => {
                    void navigator.clipboard?.writeText(file.path);
                    setReport(`Copied ${file.name}'s path`);
                  }}
                  className="cursor-pointer text-right text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-fg"
                >
                  Copy
                </button>
              </GridRow>
            ))
          )}
        </DetailBlock>

        <DetailBlock
          id="log"
          title="Event log"
          note={`${events.length} ${events.length === 1 ? "event" : "events"}`}
          right={
            events.length === 0 ? undefined : (
              <button
                type="button"
                onClick={copyLog}
                className="cursor-pointer text-wv-dim hover:text-wv-fg"
              >
                Copy as text
              </button>
            )
          }
        >
          {events.length === 0 ? (
            <span className="text-[12.5px] text-wv-muted">
              The engine has not recorded anything for this job.
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
          <DetailBlock id="release" title="Release" tone="panel" bodyClassName="gap-[13px]">
            <div
              className="grid gap-x-4 gap-y-[13px]"
              style={{ gridTemplateColumns: "repeat(auto-fit, minmax(104px, 1fr))" }}
            >
              {releaseFields(job.parsedRelease, job.category).map((field) => (
                <Field
                  key={field.label}
                  label={field.label}
                  value={field.value}
                  title={field.value}
                />
              ))}
            </div>
            {releaseFlags(job.parsedRelease).length === 0 ? null : (
              <div className="flex flex-wrap gap-1.5">
                {releaseFlags(job.parsedRelease).map((flag) => (
                  <Tag key={flag}>{flag}</Tag>
                ))}
              </div>
            )}
          </DetailBlock>

          <DetailBlock
            id="identity"
            title="Duplicate identity"
            tone="panel"
            bodyClassName="gap-[9px]"
          >
            <Field
              variant="inline"
              label="Lifecycle"
              value={duplicate?.lifecycle ? sentenceCase(duplicate.lifecycle) : EM_DASH}
            />
            <Field
              variant="inline"
              label="Normalised"
              value={duplicate?.normalizedName || EM_DASH}
              title={duplicate?.normalizedName ?? undefined}
            />
            <Field
              variant="inline"
              label="Group"
              value={
                duplicate?.semantic?.state
                  ? `${sentenceCase(duplicate.semantic.state)}${
                      duplicate.semantic.score === null
                        ? ""
                        : ` · ${Math.round(duplicate.semantic.score * 100)}%`
                    }`
                  : "no semantic group"
              }
            />
            <DangerButton
              className="mt-1 h-[30px] w-full"
              disabled={busy || !duplicate}
              onClick={() => setConfirm("forget")}
            >
              Forget identity
            </DangerButton>
          </DetailBlock>

          <DetailBlock id="metadata" title="Metadata" tone="panel" bodyClassName="gap-[11px]">
            <Field variant="stacked" label="Job id" value={String(job.id)} />
            <Field
              variant="stacked"
              label="Original NZB title"
              value={job.originalTitle || job.name}
            />
            <Field variant="stacked" label="Category" value={job.category || "uncategorised"} />
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
            The prototype ends on "Providers used". weaver does not persist which
            provider served which article — the attempt events are transient — so
            this panel says what the job's bytes actually were instead of
            inventing an attribution.
          */}
          <DetailBlock id="recovery" title="Bytes" tone="panel" bodyClassName="gap-[11px]">
            <ShareRow
              label="Payload"
              note={`${formatSize(job.downloadedBytes)} of ${formatSize(job.totalBytes)}`}
              percent={job.totalBytes > 0 ? (job.downloadedBytes / job.totalBytes) * 100 : 0}
              color={color}
            />
            <ShareRow
              label="Recovery set"
              note={
                job.optionalRecoveryBytes === 0
                  ? "none posted"
                  : `${formatSize(job.optionalRecoveryDownloadedBytes)} of ${formatSize(job.optionalRecoveryBytes)}`
              }
              percent={
                job.optionalRecoveryBytes > 0
                  ? (job.optionalRecoveryDownloadedBytes / job.optionalRecoveryBytes) * 100
                  : 0
              }
              color={WV.info}
            />
            <ShareRow
              label="Unrecovered"
              note={job.failedBytes > 0 ? formatSize(job.failedBytes) : "none"}
              percent={job.totalBytes > 0 ? (job.failedBytes / job.totalBytes) * 100 : 0}
              color={WV.error}
            />
          </DetailBlock>
        </PanelGrid>
      </div>

      <ConfirmDialog
        open={confirm === "delete"}
        title="Delete from history"
        note={job.displayTitle || job.name}
        busy={busy}
        confirmLabel="Delete from history"
        body="The entry leaves history and its files stay on disk."
        onDismiss={() => setConfirm(null)}
        onConfirm={() => {
          setConfirm(null);
          void run("Removed from history", async () => {
            const result = await acceptHistoryDelete({
              input: { mode: "IDS", ids: [job.id], deleteFiles: false },
            });
            if (!result.error) {
              navigate("/history");
            }
            return result;
          });
        }}
      />

      <ConfirmDialog
        open={confirm === "cancel"}
        title="Cancel transfer"
        note={job.displayTitle || job.name}
        busy={busy}
        confirmLabel="Cancel transfer"
        body="The transfer stops and leaves the queue. Partial data is cleaned up as usual."
        onDismiss={() => setConfirm(null)}
        onConfirm={() => {
          setConfirm(null);
          void run("Transfer cancelled", () => cancelJob({ id: job.id }));
        }}
      />

      <ConfirmDialog
        open={confirm === "forget"}
        title="Forget identity"
        note={duplicate?.normalizedName ?? undefined}
        busy={busy}
        confirmLabel="Forget identity"
        body="weaver stops recognising this release as a duplicate of anything it has seen. A future grab of the same release will be treated as new."
        onDismiss={() => setConfirm(null)}
        onConfirm={() => {
          setConfirm(null);
          void run("Identity forgotten", async () => {
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
}: {
  label: string;
  note: string;
  percent: number;
  color: string;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-[5px]">
      <div className="flex items-baseline justify-between gap-[10px] text-[12.5px]">
        <span className="truncate text-wv-tertiary">{label}</span>
        <span className="flex-none font-wv-mono text-[11px] text-wv-muted">{note}</span>
      </div>
      <Bar percent={percent} color={color} />
    </div>
  );
}

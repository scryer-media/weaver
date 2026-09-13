import { useState, type ReactNode } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  CANCEL_JOB_MUTATION,
  CANCEL_JOB_POST_PROCESSING_MUTATION,
  JOB_OUTPUT_FILES_QUERY,
  PAUSE_JOB_MUTATION,
  RESUME_JOB_MUTATION,
  UPDATE_JOBS_MUTATION,
} from "@/graphql/queries";
import type { JobData } from "@/lib/job-types";
import { statusToken } from "@/lib/status-tokens";
import { Eyebrow } from "@/next/components/chrome";
import { DangerButton, SecondaryButton, Select } from "@/next/components/controls";
import { Icon } from "@/next/components/icons";
import { PhaseBars, useJobProgress } from "@/next/components/PhaseBars";
import { EM_DASH, formatRate, formatSize } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { statusDetail, useStatusLabel } from "@/next/data/status";

interface OutputFile {
  name: string;
  path: string;
  sizeBytes: number;
}

interface OutputFilesResponse {
  jobOutputFiles: {
    outputDir: string | null;
    files: OutputFile[];
    totalBytes: number;
  } | null;
}

type Priority = "HIGH" | "NORMAL" | "LOW";

const PRIORITY_OPTIONS: { value: Priority; label: string }[] = [
  { value: "HIGH", label: "High" },
  { value: "NORMAL", label: "Normal" },
  { value: "LOW", label: "Low" },
];

/** Priority rides in the job's metadata; anything unset or unknown is Normal. */
function jobPriority(job: JobData): Priority {
  const raw = job.metadata.find((entry) => entry.key === "priority")?.value?.toUpperCase();
  return raw === "HIGH" || raw === "LOW" ? raw : "NORMAL";
}

function EditField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex items-center gap-3">
      <span className="w-[104px] flex-none text-[12.5px] text-wv-muted">{label}</span>
      <div className="min-w-0 flex-1">{children}</div>
    </div>
  );
}

function Field({ label, value, title }: { label: string; value: string; title?: string }) {
  return (
    <div className="flex gap-3">
      <span className="w-[104px] flex-none text-[12.5px] text-wv-muted">{label}</span>
      <span title={title ?? value} className="min-w-0 flex-1 truncate text-[12.5px] text-wv-fg">
        {value}
      </span>
    </div>
  );
}

/**
 * The 344px inspector.
 *
 * It scrolls on its own and pins the action block below that scroller, so the
 * three buttons stay reachable however long the file list gets.
 */
export function DownloadInspector({
  job,
  eta,
  rate,
  onRemoved,
}: {
  job: JobData;
  eta: string;
  rate: number;
  onRemoved: (id: number) => void;
}) {
  const statusLabel = useStatusLabel();
  const [{ data }] = useQuery<OutputFilesResponse>({
    query: JOB_OUTPUT_FILES_QUERY,
    variables: { jobId: job.id },
  });
  const [, pauseJob] = useMutation(PAUSE_JOB_MUTATION);
  const [, resumeJob] = useMutation(RESUME_JOB_MUTATION);
  const [, cancelJob] = useMutation(CANCEL_JOB_MUTATION);
  const [, updateJobs] = useMutation(UPDATE_JOBS_MUTATION);
  const [stopState, cancelPostProcessing] = useMutation(CANCEL_JOB_POST_PROCESSING_MUTATION);
  const { categories, queue } = useNextData();
  // What was just picked, shown until the queue carries it back; the daemon's
  // answer can take a refresh to arrive, and a select that snaps back to the
  // old value in the meantime reads as a refusal.
  const [pending, setPending] = useState<{ priority?: Priority; category?: string }>({});
  const [failure, setFailure] = useState<string | null>(null);

  const priority = pending.priority ?? jobPriority(job);
  const category = pending.category ?? (job.category || "");
  if (pending.priority !== undefined && pending.priority === jobPriority(job)) {
    setPending(({ priority: _settled, ...rest }) => rest);
  }
  if (pending.category !== undefined && pending.category === (job.category || "")) {
    setPending(({ category: _settled, ...rest }) => rest);
  }

  const edit = (change: { priority?: Priority; category?: string }) => {
    setPending((current) => ({ ...current, ...change }));
    setFailure(null);
    void updateJobs({ ids: [job.id], ...change }).then((result) => {
      if (result.error) {
        setPending((current) => {
          const next = { ...current };
          for (const key of Object.keys(change) as (keyof typeof change)[]) {
            delete next[key];
          }
          return next;
        });
        setFailure(result.error.message);
        return;
      }
      queue.refresh();
    });
  };

  const categoryOptions = [
    { value: "", label: "Uncategorised" },
    ...categories.map((entry) => ({ value: entry.name, label: entry.name })),
    // A category that has since been removed from settings is still the job's.
    ...(category !== "" && !categories.some((entry) => entry.name === category)
      ? [{ value: category, label: category }]
      : []),
  ];

  const token = statusToken(job.status);
  const progress = useJobProgress(job);
  const files = data?.jobOutputFiles?.files ?? [];
  // Only a phase with a bar is one the inspector says the job is in.
  const detail = statusDetail(
    job,
    progress.bars.find((bar) => bar.phase === progress.status) ?? progress.bars.at(-1) ?? null,
    progress.status,
  );
  const isPaused = token === "paused";

  return (
    <aside className="flex max-h-[46vh] min-h-0 w-full flex-none flex-col border-t border-wv-line-strong bg-wv-input xl:max-h-none xl:w-[344px] xl:border-t-0 xl:border-l">
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">
        <div className="flex flex-none flex-col gap-3 border-b border-wv-hairline px-5 py-[22px]">
          <Eyebrow tone="rail">Selected download</Eyebrow>
          <div className="font-wv-title text-[15px] leading-[1.35] font-semibold tracking-[-0.01em] text-wv-strong">
            {job.displayTitle || job.name}
          </div>
          <div className="font-wv-mono text-[11px] leading-[1.5] break-all text-wv-faint">
            {job.name}
          </div>
          <PhaseBars
            job={job}
            view={progress}
            height={16}
            className="mt-1"
            barClassName="flex-1"
            percentClassName="w-[34px] text-right text-[11.5px] text-wv-secondary"
          />
          {/* The inspector is a summary; the whole story lives on the job's own screen. */}
          <Link
            to={`/jobs/${job.id}`}
            className="flex items-center gap-[3px] font-wv-mono text-[11.5px] text-wv-accent hover:text-wv-accent-hover"
          >
            Open job detail
            <Icon name="open" size={13} />
          </Link>
        </div>

        <div className="flex flex-none flex-col gap-[9px] border-b border-wv-hairline px-5 py-[18px]">
          <Field
            label="Status"
            value={
              detail ? `${statusLabel(progress.status)} — ${detail}` : statusLabel(progress.status)
            }
          />
          <EditField label="Category">
            <Select
              label="Category"
              value={category}
              options={categoryOptions}
              onChange={(next) => edit({ category: next })}
              className="h-[30px] w-full min-w-0 text-[12.5px]"
            />
          </EditField>
          <EditField label="Priority">
            <Select
              label="Priority"
              value={priority}
              options={PRIORITY_OPTIONS}
              onChange={(next) => edit({ priority: next })}
              className="h-[30px] w-full min-w-0 text-[12.5px]"
            />
          </EditField>
          {failure === null ? null : (
            <div className="text-[12px] text-wv-error-text">{failure}</div>
          )}
          <Field label="Total size" value={formatSize(job.totalBytes)} />
          <Field label="Rate" value={rate > 0 ? formatRate(rate) : EM_DASH} />
          <Field label="Time left" value={eta} />
          <Field label="Destination" value={data?.jobOutputFiles?.outputDir || EM_DASH} />
        </div>

        <div className="flex flex-none flex-col gap-[13px] px-5 py-[18px]">
          <Eyebrow tone="rail">Files</Eyebrow>
          {files.length === 0 ? (
            <div className="text-[12.5px] text-wv-muted">
              Nothing written yet — files appear once the download reaches its destination.
            </div>
          ) : (
            files.map((file) => (
              <div key={file.path} className="flex items-baseline gap-3">
                <span
                  title={file.path}
                  className="min-w-0 flex-1 truncate text-[12.5px] text-wv-secondary"
                >
                  {file.name}
                </span>
                <span className="flex-none font-wv-mono text-[11.5px] text-wv-muted">
                  {formatSize(file.sizeBytes)}
                </span>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="flex flex-none flex-col gap-[10px] border-t border-wv-hairline px-5 py-[18px]">
        <div className="flex gap-[10px]">
          <SecondaryButton
            icon={isPaused ? "resume" : "pause"}
            className="flex-1 justify-center"
            onClick={() => {
              void (isPaused ? resumeJob({ id: job.id }) : pauseJob({ id: job.id }));
            }}
          >
            {isPaused ? "Resume" : "Pause"}
          </SecondaryButton>
          <SecondaryButton
            icon="topOfQueue"
            className="flex-1 justify-center"
            onClick={() => {
              void updateJobs({ ids: [job.id], priority: "HIGH" });
            }}
          >
            Top of queue
          </SecondaryButton>
        </div>
        {/* Only while scripts run: a job waiting for a script slot reports itself as queued. */}
        {job.status === "POST_PROCESSING" ? (
          <SecondaryButton
            icon="stopScripts"
            className="w-full justify-center"
            disabled={stopState.fetching}
            onClick={() => {
              setFailure(null);
              void cancelPostProcessing({ jobId: job.id }).then((result) => {
                if (result.error) {
                  setFailure(result.error.message);
                }
              });
            }}
          >
            Stop scripts
          </SecondaryButton>
        ) : null}
        <DangerButton
          icon="cancelDownload"
          className="w-full justify-center"
          onClick={() => {
            onRemoved(job.id);
            void cancelJob({ id: job.id });
          }}
        >
          Remove download
        </DangerButton>
      </div>
    </aside>
  );
}

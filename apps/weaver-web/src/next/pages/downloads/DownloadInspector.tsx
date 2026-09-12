import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  CANCEL_JOB_MUTATION,
  JOB_OUTPUT_FILES_QUERY,
  PAUSE_JOB_MUTATION,
  RESUME_JOB_MUTATION,
  UPDATE_JOBS_MUTATION,
} from "@/graphql/queries";
import type { JobData } from "@/lib/job-types";
import { statusToken } from "@/lib/status-tokens";
import { Bar, Eyebrow } from "@/next/components/chrome";
import { DangerButton, SecondaryButton } from "@/next/components/controls";
import { EM_DASH, formatRate, formatSize } from "@/next/data/format";
import { statusColor } from "@/next/data/palette";
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

  const token = statusToken(job.status);
  const color = statusColor(job.status);
  const percent = Math.round(job.progress * 100);
  const files = data?.jobOutputFiles?.files ?? [];
  const detail = statusDetail(job);
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
          <div className="mt-1 flex items-center gap-3">
            <Bar percent={percent} color={color} height={16} className="flex-1" />
            <span className="w-[34px] flex-none text-right font-wv-mono text-[11.5px] text-wv-secondary">
              {percent}%
            </span>
          </div>
          {/* The inspector is a summary; the whole story lives on the job's own screen. */}
          <Link
            to={`/jobs/${job.id}`}
            className="font-wv-mono text-[11.5px] text-wv-accent hover:text-wv-accent-hover"
          >
            Open job detail &rsaquo;
          </Link>
        </div>

        <div className="flex flex-none flex-col gap-[9px] border-b border-wv-hairline px-5 py-[18px]">
          <Field
            label="Status"
            value={detail ? `${statusLabel(job.status)} — ${detail}` : statusLabel(job.status)}
          />
          <Field label="Category" value={job.category || "uncategorised"} />
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
            className="flex-1 justify-center"
            onClick={() => {
              void (isPaused ? resumeJob({ id: job.id }) : pauseJob({ id: job.id }));
            }}
          >
            {isPaused ? "Resume" : "Pause"}
          </SecondaryButton>
          <SecondaryButton
            className="flex-1 justify-center"
            onClick={() => {
              void updateJobs({ ids: [job.id], priority: "HIGH" });
            }}
          >
            Top of queue
          </SecondaryButton>
        </div>
        <DangerButton
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

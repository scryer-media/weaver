import { memo } from "react";
import { formatJobReleaseName, type JobData } from "@/lib/job-types";
import { Square } from "@/next/components/chrome";
import { CheckBox } from "@/next/components/controls";
import { PhaseBars, useJobProgress } from "@/next/components/PhaseBars";
import { ListRow, ValueCell } from "@/next/components/rows";
import { EM_DASH, formatSize } from "@/next/data/format";
import { statusColor, WV } from "@/next/data/palette";

/**
 * One queue row.
 *
 * Its own component because a row keeps time: the phase bars and the status
 * label each wait out a brief change before they show it, and that clock
 * belongs to the row rather than to the page that lists them.
 */
export const DownloadRow = memo(function DownloadRow({
  job,
  selected,
  onSelect,
  picked,
  onPick,
  statusLabel,
  wait,
  hold,
  statusTitle,
}: {
  job: JobData;
  selected: boolean;
  onSelect: (id: number) => void;
  /** Ticked for a bulk action. */
  picked: boolean;
  onPick: (id: number) => void;
  statusLabel: (status: string) => string;
  /** A hold or an estimate for the last column; without one it shows the phase's progress. */
  wait: string | null;
  /** Why the download is held back despite its status, in a word: `ISP cap`. */
  hold: string | null;
  /** A longer account of the wait, shown on hover. */
  statusTitle?: string;
}) {
  const progress = useJobProgress(job);
  const name = formatJobReleaseName(job);
  // The phase the label names, or failing that the last one with a bar; a
  // phase still settling is not what the row says it is doing.
  const phase =
    progress.bars.find((bar) => bar.phase === progress.status) ?? progress.bars.at(-1) ?? null;
  const trailing =
    wait ?? (phase && progress.status !== "QUEUED" ? `${Math.round(phase.progressPercent)}%` : EM_DASH);

  return (
    <ListRow
      markSelection
      selected={selected}
      picked={picked}
      onClick={() => onSelect(job.id)}
      title={name}
      lead={
        <CheckBox label={`Select ${name}`} checked={picked} onChange={() => onPick(job.id)} />
      }
      left={
        <div className="flex min-w-0 flex-[1_1_240px] flex-col gap-[9px]">
          <span className="truncate text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg">
            {name}
          </span>
          <PhaseBars
            job={job}
            view={progress}
            height={10}
            // 49 whole cells of the 7px period a 10px bar takes; a cap off that
            // multiple leaves dead track before the percentage.
            barClassName="max-w-[343px] min-w-[56px] flex-1"
            percentClassName="w-[34px] text-[11.5px] text-wv-muted"
          />
        </div>
      }
      right={
        <>
          <div className="flex min-w-[96px] items-center gap-[9px]" title={statusTitle}>
            <Square color={statusColor(progress.status)} />
            <span className="flex min-w-0 flex-col">
              <span className="truncate text-[12.5px] text-wv-secondary">
                {statusLabel(progress.status)}
              </span>
              {hold === null ? null : (
                <span
                  className="truncate font-wv-mono text-[10px] tracking-[0.08em] uppercase"
                  style={{ color: WV.warn }}
                >
                  {hold}
                </span>
              )}
            </span>
          </div>
          <ValueCell>{formatSize(job.totalBytes)}</ValueCell>
          <ValueCell>{trailing}</ValueCell>
        </>
      }
    />
  );
});

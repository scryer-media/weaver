import { useMemo } from "react";
import { Progress } from "@/components/ui/progress";
import { formatSpeed } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";
import type { JobPhase, JobPhaseProgressData } from "@/lib/job-types";
import { useExtractionVisibility } from "@/lib/hooks/use-extraction-visibility";
import { cn } from "@/lib/utils";

const PHASE_PRIORITY: Record<JobPhase, number> = {
  MOVING: 0,
  REPAIRING: 1,
  EXTRACTING: 2,
  DOWNLOADING: 3,
};

const PHASE_COLOR: Record<JobPhase, string> = {
  DOWNLOADING: "bg-primary",
  REPAIRING: "bg-orange-500",
  EXTRACTING: "bg-violet-500",
  MOVING: "bg-cyan-500",
};

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.min(100, Math.max(0, value));
}

function phaseLabelKey(phase: JobPhase): string {
  switch (phase) {
    case "DOWNLOADING":
      return "phase.downloading";
    case "REPAIRING":
      return "phase.repairing";
    case "EXTRACTING":
      return "phase.extracting";
    case "MOVING":
      return "phase.moving";
  }
}

export function JobPhaseProgressBars({
  phaseProgress,
  status,
  progress,
  compact = false,
}: {
  phaseProgress?: JobPhaseProgressData[] | null;
  status?: string | null;
  /** Normalized job progress in the 0–1 range. */
  progress?: number | null;
  compact?: boolean;
}) {
  const t = useTranslate();
  const phases = useMemo(() => phaseProgress ?? [], [phaseProgress]);
  const extractionVisible = useExtractionVisibility(phases);

  const visible = phases
    .filter(
      (phase) =>
        phase.totalBytes > 0 &&
        (phase.phase !== "EXTRACTING" || extractionVisible),
    )
    .sort((left, right) => PHASE_PRIORITY[left.phase] - PHASE_PRIORITY[right.phase])
    .slice(0, 2);
  const progressClassName = cn("rounded-pill bg-secondary", compact ? "h-1.5" : "h-2");

  if (visible.length === 0) {
    if (status === "FINALIZING_DOWNLOAD") {
      const pct = clampPercent((progress ?? 0) * 100);
      return (
        <div className={cn("space-y-1.5", compact && "space-y-1")}>
          <div className="flex items-center justify-between gap-2 text-[10px] font-medium text-muted-foreground">
            <span className="truncate">{t("timeline.finalizingDownload")}</span>
            <span className="shrink-0 tabular-nums">{pct.toFixed(0)}%</span>
          </div>
          <Progress value={pct} className={progressClassName} />
        </div>
      );
    }
    return <Progress value={0} className={progressClassName} />;
  }

  return (
    <div className={cn("space-y-1.5", compact && "space-y-1")}>
      {visible.map((phase) => {
        const pct = clampPercent(phase.progressPercent);
        const label = t(phaseLabelKey(phase.phase));
        const rate = phase.rateBps && phase.rateBps > 0 ? formatSpeed(phase.rateBps) : null;
        return (
          <div key={phase.phase} className="space-y-1">
            <div className="flex items-center justify-between gap-2 text-[10px] font-medium text-muted-foreground">
              <span className="truncate">{rate ? `${label} · ${rate}` : label}</span>
              <span className="shrink-0 tabular-nums">{pct.toFixed(0)}%</span>
            </div>
            <Progress
              value={pct}
              className={progressClassName}
              indicatorClassName={cn("rounded-pill", PHASE_COLOR[phase.phase])}
            />
          </div>
        );
      })}
    </div>
  );
}

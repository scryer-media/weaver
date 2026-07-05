import { useEffect, useMemo, useState } from "react";
import { Progress } from "@/components/ui/progress";
import { formatSpeed } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";
import type { JobPhase, JobPhaseProgressData } from "@/lib/job-types";
import { cn } from "@/lib/utils";

const REVEAL_AFTER_MS = 5_000;
const MIN_REMAINING_MS = 5_000;

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
  compact = false,
}: {
  phaseProgress?: JobPhaseProgressData[] | null;
  compact?: boolean;
}) {
  const t = useTranslate();
  const phases = useMemo(() => phaseProgress ?? [], [phaseProgress]);
  const [now, setNow] = useState(() => Date.now());
  const [revealed, setRevealed] = useState<Set<JobPhase>>(() => new Set());
  const phaseKey = phases.map((phase) => phase.phase).sort().join("|");

  useEffect(() => {
    if (phases.length === 0) {
      return;
    }
    const id = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(id);
  }, [phases.length]);

  useEffect(() => {
    const live = new Set(phases.map((phase) => phase.phase));
    setRevealed((previous) => {
      const next = new Set<JobPhase>();
      for (const phase of previous) {
        if (live.has(phase)) {
          next.add(phase);
        }
      }
      for (const phase of phases) {
        const remaining = phase.estimatedRemainingMs ?? 0;
        if (
          phase.totalBytes > 0 &&
          now - phase.startedAtEpochMs >= REVEAL_AFTER_MS &&
          remaining >= MIN_REMAINING_MS
        ) {
          next.add(phase.phase);
        }
      }
      return next;
    });
  }, [now, phaseKey, phases]);

  const visible = phases
    .filter((phase) => revealed.has(phase.phase) && phase.totalBytes > 0)
    .sort((left, right) => PHASE_PRIORITY[left.phase] - PHASE_PRIORITY[right.phase])
    .slice(0, 2);

  if (visible.length === 0) {
    return null;
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
              className={cn("rounded-pill bg-secondary", compact ? "h-1.5" : "h-2")}
              indicatorClassName={cn("rounded-pill", PHASE_COLOR[phase.phase])}
            />
          </div>
        );
      })}
    </div>
  );
}

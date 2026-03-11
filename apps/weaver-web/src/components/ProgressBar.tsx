import { JobProgress } from "@/components/JobProgress";

interface ProgressBarProps {
  progress: number;
  status?: string;
  showLabel?: boolean;
}

export function ProgressBar({ progress, status, showLabel = true }: ProgressBarProps) {
  return <JobProgress progress={progress} status={status} showLabel={showLabel} />;
}

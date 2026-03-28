import { JobProgress } from "@/components/JobProgress";

interface ProgressBarProps {
  progress: number;
  status?: string;
  totalBytes?: number;
  downloadedBytes?: number;
  failedBytes?: number;
  showLabel?: boolean;
}

export function ProgressBar({
  progress,
  status,
  totalBytes,
  downloadedBytes,
  failedBytes,
  showLabel = true,
}: ProgressBarProps) {
  return (
    <JobProgress
      progress={progress}
      status={status}
      totalBytes={totalBytes}
      downloadedBytes={downloadedBytes}
      failedBytes={failedBytes}
      showLabel={showLabel}
    />
  );
}

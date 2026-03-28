function clampFraction(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.min(1, Math.max(0, value));
}

export function getDisplayedJobProgress({
  progress,
  totalBytes,
  downloadedBytes,
  failedBytes,
  status,
}: {
  progress: number;
  totalBytes?: number;
  downloadedBytes?: number;
  failedBytes?: number;
  status?: string;
}): number {
  if (status === "COMPLETE") {
    return 1;
  }

  const rawProgress = clampFraction(progress);
  if (!totalBytes || totalBytes <= 0) {
    return rawProgress;
  }

  const processedBytes = Math.min(
    totalBytes,
    Math.max(0, (downloadedBytes ?? 0) + (failedBytes ?? 0)),
  );
  return Math.max(rawProgress, clampFraction(processedBytes / totalBytes));
}


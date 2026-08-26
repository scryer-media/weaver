import { useEffect, useState } from "react";
import type { JobPhaseProgressData } from "@/lib/job-types";

const EXTRACTION_VISIBILITY_DELAY_MS = 3_000;

function hasIncompleteDownload(phases: readonly JobPhaseProgressData[]): boolean {
  return phases.some(
    (phase) =>
      phase.phase === "DOWNLOADING"
      && phase.totalBytes > 0
      && phase.completedBytes < phase.totalBytes,
  );
}

export function extractionVisibilityIdentity(
  phaseProgress?: readonly JobPhaseProgressData[] | null,
): number | null {
  const phases = phaseProgress ?? [];
  const extraction = phases.find(
    (phase) => phase.phase === "EXTRACTING" && phase.totalBytes > 0,
  );
  if (!extraction) {
    return null;
  }

  // Streaming extraction can sit at 100% for the bytes currently available
  // while it waits for another archive volume. Keep showing the live download
  // instead of presenting that wait as active extraction.
  if (
    extraction.completedBytes >= extraction.totalBytes
    && hasIncompleteDownload(phases)
  ) {
    return null;
  }

  return extraction.startedAtEpochMs;
}

export function extractionPresentationStatus(
  status: string,
  phaseProgress: readonly JobPhaseProgressData[] | null | undefined,
  extractionVisible: boolean,
): string {
  if (
    status === "EXTRACTING"
    && !extractionVisible
    && hasIncompleteDownload(phaseProgress ?? [])
  ) {
    return "DOWNLOADING";
  }
  return status;
}

export function useExtractionVisibility(
  phaseProgress?: readonly JobPhaseProgressData[] | null,
): boolean {
  const identity = extractionVisibilityIdentity(phaseProgress);
  const [revealedIdentity, setRevealedIdentity] = useState<number | null>(null);

  useEffect(() => {
    if (identity == null) {
      setRevealedIdentity(null);
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setRevealedIdentity(identity);
    }, EXTRACTION_VISIBILITY_DELAY_MS);
    return () => window.clearTimeout(timeoutId);
  }, [identity]);

  return identity != null && identity === revealedIdentity;
}

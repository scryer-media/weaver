/**
 * Concurrently-active pipeline stages for a job, in display order.
 *
 * The backend currently reports a single `status`. When it begins emitting
 * concurrent post-processing stages (e.g. DOWNLOADING + VERIFYING, shifting to
 * EXTRACTING as parts finish), this is the single place to surface them — every
 * status chip, compact dot, and detail badge lights up automatically. Callers
 * should treat the first entry as the primary (progress-owning) stage.
 */
export function getJobStages(job: {
  status: string;
  concurrentStatuses?: readonly string[] | null;
}): string[] {
  const extras = (job.concurrentStatuses ?? []).filter(
    (stage): stage is string => Boolean(stage) && stage !== job.status,
  );
  return [job.status, ...extras];
}

-- A slot belongs to an NZB until its terminal work has drained.
ALTER TABLE active_jobs ADD COLUMN nzb_admitted INTEGER NOT NULL DEFAULT 0
    CHECK (nzb_admitted IN (0, 1));
ALTER TABLE active_jobs ADD COLUMN nzb_release_pending INTEGER NOT NULL DEFAULT 0
    CHECK (nzb_release_pending IN (0, 1));

-- Grandfather existing started jobs; untouched and explicitly paused jobs wait.
UPDATE active_jobs SET nzb_admitted = 1
WHERE status NOT IN ('paused', 'complete', 'partial', 'failed', 'cancelled')
  AND COALESCE(run_state, '') != 'paused'
  AND (
    status NOT IN ('queued', 'propagating')
    OR EXISTS (SELECT 1 FROM active_file_progress p WHERE p.job_id = active_jobs.job_id)
    OR EXISTS (SELECT 1 FROM active_files f WHERE f.job_id = active_jobs.job_id)
  );

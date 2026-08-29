-- Earlier history deletion removed visible history but retained duplicate
-- identity. Permanently forget snapshots with no surviving job record, while
-- retaining a tombstone so duplicate backfill cannot recreate them.
INSERT INTO forgotten_duplicate_identities (job_id, forgotten_at)
SELECT snapshot.job_id, strftime('%s', 'now')
FROM duplicate_job_snapshots AS snapshot
WHERE NOT EXISTS (
    SELECT 1 FROM active_jobs AS active WHERE active.job_id = snapshot.job_id
)
  AND NOT EXISTS (
    SELECT 1 FROM job_history AS history WHERE history.job_id = snapshot.job_id
)
ON CONFLICT(job_id) DO NOTHING;

DELETE FROM duplicate_job_snapshots
WHERE NOT EXISTS (
    SELECT 1 FROM active_jobs AS active WHERE active.job_id = duplicate_job_snapshots.job_id
)
  AND NOT EXISTS (
    SELECT 1 FROM job_history AS history WHERE history.job_id = duplicate_job_snapshots.job_id
);

DELETE FROM semantic_duplicate_groups
WHERE NOT EXISTS (
    SELECT 1 FROM semantic_duplicate_candidates AS candidate
    WHERE candidate.group_id = semantic_duplicate_groups.group_id
);

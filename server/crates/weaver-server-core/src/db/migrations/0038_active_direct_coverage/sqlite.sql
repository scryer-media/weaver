-- Direct-store coverage checkpoint (plan 135, D6).
--
-- Exactly one row per archive set, transactionally replaced at every barrier.
-- No history and no per-volume rows: a 2 000-volume set at ~400 barriers would
-- be ~800 000 row writes if normalized, which is the article-proportional cost
-- the direct-store design exists to avoid. The whole checkpoint -- schema
-- version, generation, layout-plan digest, destination claims and every
-- per-volume floor -- rides in one encoded blob.
CREATE TABLE active_direct_coverage (
    job_id    INTEGER NOT NULL,
    set_name  TEXT NOT NULL,
    snapshot  BLOB NOT NULL,
    PRIMARY KEY (job_id, set_name)
) WITHOUT ROWID;
